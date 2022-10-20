import threading
import time
import json
from enum import Enum
from datetime import datetime

from opcua import Client, ua


class ReturnCodes(Enum):
    OK = 0
    NOK = -1
    TIMEOUT = -2
    NO_CONNECTION = -3

class RobobarOpcClient(Client):
    connected = False

    def __init__(self, url):
        super().__init__(url)

    def _init_nodes(self):
        self._queue_items_node = self.get_node('ns=3;s="Drink_DB"."drinkQueue"."items"')
        self._queue_start_index = self.get_node('ns=3;s="Drink_DB"."drinkQueue"."firstItemIndex"')
        self._queue_end_index = self.get_node('ns=3;s="Drink_DB"."drinkQueue"."lastItemIndex"')
        self._current_queue_length_node = self.get_node('ns=3;s="Drink_DB"."drinkQueue"."currentQueueLength"')
        self._queue_read_index_node = self.get_node('ns=3;s="Drink_DB"."drinkQueue"."readIndex"')
        self._drink_types_node = self.get_node('ns=3;s="Drink_DB"."drinkTypes"')
        self._pickup_drinks_node = self.get_node('ns=3;s="Drink_DB"."pickUpDrinks"')
        self.prep_drink_nodes = [
            self.get_node('ns=3;s="Drink_DB"."leftPrepDrink"'),
            self.get_node('ns=3;s="Drink_DB"."rightPrepDrink"'),
        ]
        self.prep_drink_prepStartAt_nodes = [
            self.get_node('ns=3;s="Drink_DB"."leftPrepDrink"."prepStartAt"'),
            self.get_node('ns=3;s="Drink_DB"."rightPrepDrink"."prepStartAt"'),
        ]
        self.prep_drink_prepDoneAt_nodes = [
            self.get_node('ns=3;s="Drink_DB"."leftPrepDrink"."prepDoneAt"'),
            self.get_node('ns=3;s="Drink_DB"."rightPrepDrink"."prepDoneAt"'),
        ]
        self._server_state_node = self.get_node('i=2259')
        self._plc_time_node = self.get_node('ns=3;s="Queue_Handle_DB"."currentTime"')

        self._push_new_order_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."pushNewOrderToQueue"')
        self._new_order_use_ice_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."newOrderUseIce"')
        self._new_order_drink_size_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."newOrderDrinkSizeId"')
        self._new_drink_type_id_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."newOrderDrinkTypeId"')
        self._order_pushed_successfully_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Input"."orderPushedSuccessfully"')
        self._success_order_number_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Input"."successOrderNumber"')

    @staticmethod
    def get_ua_integer_object(integer_number):
        if type(integer_number) is not int:
            raise TypeError(f'Parameter integer_number is not an integer (passed { type(integer_number) }).')
        ua_integer = ua.DataValue(ua.Variant(integer_number, ua.VariantType.Int16))
        ua_integer.ServerTimestamp = None
        ua_integer.SourceTimestamp = None

        return ua_integer

    @staticmethod
    def get_ua_boolean_object(boolean_value):
        if type(boolean_value) is not bool:
            raise TypeError(f'Parameter boolean_value is not boolean (passed { type(boolean_value)} ).')
        ua_boolean = ua.DataValue(ua.Variant(boolean_value, ua.VariantType.Boolean))
        ua_boolean.ServerTimestamp = None
        ua_boolean.SourceTimestamp = None

        return ua_boolean
    
    @staticmethod
    def get_items_from_circular_buffer(
        buffer, index_of_first_item, queue_length, index_of_buffer_start, index_of_buffer_end):
        """Get currently pushed items in the circular buffer drink queue.

        Args:
            buffer (List): buffer array in the plc containing all the drink orders
            index_of_first_item (int): 
                (aka readIndex) equals the index in buffer of first item on the stack (first to pop)
            queue_length (int): equals the number of currently pushed items
            index_of_buffer_start (int): 
                equals the index of first buffer item (since PLC can start indexing from any integer) in the array
            index_of_buffer_end (int): equals the index of last buffer item in the array

        Returns:
            List: list containing only the currently pushed items (without the empty buffer slots)
        """
        ret_items = []
        
        for queue_index in range(queue_length):
            buffer_index = index_of_first_item + queue_index

            if buffer_index > index_of_buffer_end:
                buffer_index = (buffer_index - index_of_buffer_end - 1) + index_of_buffer_start

            ret_items.append(buffer[buffer_index])
        
        return ret_items

    @staticmethod
    def get_datetime_string(year, month, day, hours, minutes, seconds):
        return f'{year:04d}-{month:02d}-{day:02d}-{hours:02d}-{minutes:02d}-{seconds:02d}'

    @staticmethod
    def get_datetime_dict_from_byte_array(byte_array):
        """Converts PLC's Date_and_Time variable to dictionary.

        Args:
            byte_array (List): defined in 
                https://support.industry.siemens.com/cs/document/36479/date_and_time-format-for-s7-?dti=0&lc=en-WW

        Returns:
            dict: keys are taken from VALUE_NAMES
        """

        VALUE_NAMES = ('year', 'month', 'day', 'hours', 'minutes', 'seconds')
        date_dict = {}

        for byte_index, dt_byte in enumerate(byte_array):
            if byte_index > 5:
                break
            high = dt_byte >> 4;
            low = dt_byte & 0xF;
            number = 10 * high + low;
            if VALUE_NAMES[byte_index] == 'year':
                number += 2000
            date_dict[VALUE_NAMES[byte_index]] = number

        return date_dict

    def get_queue_drinks_json(self):
        """Returns an object with drinks in queue, that is ready to be the json response of api.

        First, items are loaded from circulare buffer using get_tiems_from_circular_buffer and then
        parsed in a way to return json ready object/dictionary structure.

        Returns:
            dict: json ready dictionary structure
        """
        if self.connected is False:
            print('ERROR @ get_queue_drinks_json: Server is not connected. Please, try later.')
            return ReturnCodes.NO_CONNECTION, None

        try:
            queue_items_array = self._queue_items_node.get_value()
            queue_start_index = self._queue_start_index.get_value()
            queue_end_index = self._queue_end_index.get_value()
            current_queue_length = self._current_queue_length_node.get_value()
            queue_read_index = self._queue_read_index_node.get_value()
        except Exception as e:
            print('Exception message: {0}\nTry getting queue drinks later.'.format(e))

            return ReturnCodes.NOK, None

        queue_drinks = RobobarOpcClient.get_items_from_circular_buffer(
            queue_items_array,
            queue_read_index,
            current_queue_length,
            queue_start_index,
            queue_end_index
        )

        queue_drinks_obj = {
            "queueDrinks": [
                {
                    "drinkOrderId": drink.orderId,
                    "drinkTypeId": drink.drinkTypeId,
                    "prepStartedAt": drink.prepStartAt,
                } for ii, drink in enumerate(queue_drinks)
            ]
        }

        return ReturnCodes.OK, queue_drinks_obj

    def get_drink_types_json(self):
        """Returns all drink types from the PLC DrinkDB in a json ready object/dictionary structure.

        Returns:
            dict: json ready dictionary structure
        """
        if self.connected is False:
            print('ERROR @ get_drink_types_json: Server is not connected. Please, try later.')
            return ReturnCodes.NO_CONNECTION, None

        try:
            drink_types = self._drink_types_node.get_value()
        except Exception as e:
            print('Exception message: {0}\nTry getting drink types later.'.format(e))

            return ReturnCodes.NOK, None

        drink_types_obj = {
            "drinkTypes": [],
        }

        for ii, drink_type in enumerate(drink_types):
            drink_types_obj["drinkTypes"].append({
                "id": ii, 
                "name": drink_type.drinkName,
                "enabled": drink_type.drinkEnabled,
                "drinkGroups": {
                    "soft": drink_type.postmixDrink != "" and drink_type.conveyorDrink == "",
                    "alcohol": drink_type.conveyorDrink != "",
                    "coffee": drink_type.coffeeDrink != "",
                },
                "iceOption": drink_type.iceOption,
                "volumeOption": drink_type.volumeOption,
                "parameters": {
                    "showParameters": drink_type.parameters.showParameters,
                    "coffeeStrength": drink_type.parameters.coffeeStrength,
                    "volumeInMl": drink_type.parameters.volumeInMl,
                    "milkPercentage": drink_type.parameters.milkPercentage,
                },
                "prepTimeInSeconds": drink_type.preparationTime / 1000,
            })
        
        return ReturnCodes.OK, drink_types_obj

    def get_pickup_drinks_json(self):
        """Returns all prepared drinks that have not been taken by the customers yet.

        Returns:
            dict: json ready dictionary structure
        """
        if self.connected is False:
            print('ERROR @ get_pickup_drinks_json: Server is not connected. Please, try later.')
            return ReturnCodes.NO_CONNECTION, None

        try:
            pickup_drinks = self._pickup_drinks_node.get_value()
        except Exception as e:
            print('Exception message: {0}\nTry getting pickup drinks later.'.format(e))
            return ReturnCodes.NOK, None

        pickup_drinks_obj = {
            "pickUpDrinks": {}
        }

        for ii, drink in enumerate(pickup_drinks):

            if drink.orderId != 0 and not drink.pickedUp:
                pickup_drinks_obj['pickUpDrinks'][ii] = {
                    "drinkOrderId": drink.orderId,
                    "drinkTypeId": drink.drinkTypeId,
                    "prepStartedAt": drink.prepStartAt,
                }
                
        return ReturnCodes.OK, pickup_drinks_obj
    
    def get_current_plc_time(self):
        """Returns PLC local time as a string in format YYYY-MM-dd-hh-mm-ss.

        Returns:
            str: string containg PLC local time in format YYYY-MM-dd-hh-mm-ss
        """
        if self.connected is False:
            print('ERROR @ get_current_plc_time: Server is not connected. Please, try later.')
            return ReturnCodes.NO_CONNECTION, None

        try:
            current_plc_time_byte_array = self._plc_time_node.get_value()
        except Exception as e:
            print('Exception message: {0}\nTry plc current time later.'.format(e))
            return ReturnCodes.NOK, None

        current_plc_time = self.get_datetime_dict_from_byte_array(
            current_plc_time_byte_array
        )
        
        current_plc_time_string = RobobarOpcClient.get_datetime_string(
            current_plc_time['year'],
            current_plc_time['month'],
            current_plc_time['day'],
            current_plc_time['hours'],
            current_plc_time['minutes'],
            current_plc_time['seconds'],
        )
        
        return ReturnCodes.OK, current_plc_time_string

    def get_prep_drink_json(self, side=0):
        """Returns drink currently being prepared by IIWA1 (the left one from the perspective of the customer).

        Returns:
            dict: json ready dictionary structure
        """
        if self.connected is False:
            print('ERROR @ get_prep_drink_json: Server is not connected. Please, try later.')
            return ReturnCodes.NO_CONNECTION, None

        try:
            prep_drink = self.prep_drink_nodes[side].get_value()
            prep_startAt = self.prep_drink_prepStartAt_nodes[side].get_value()
            prep_doneAt = self.prep_drink_prepDoneAt_nodes[side].get_value()
        except Exception as e:
            print('Exception message: {0}\nTry getting prep drink later.'.format(e))
            return ReturnCodes.NOK, None

        prep_startAt = self.get_datetime_dict_from_byte_array(prep_startAt)
        prep_doneAt = self.get_datetime_dict_from_byte_array(prep_doneAt)

        prep_drink_obj = {
            "drinkInProgress": {
                "drinkOrderId": prep_drink.orderId,
                "drinkTypeId": prep_drink.drinkTypeId,
                "prepStartedAt": RobobarOpcClient.get_datetime_string(
                    prep_startAt['year'],
                    prep_startAt['month'],
                    prep_startAt['day'],
                    prep_startAt['hours'],
                    prep_startAt['minutes'],
                    prep_startAt['seconds'],
                ),
                "prepDoneAt": RobobarOpcClient.get_datetime_string(
                    prep_doneAt['year'],
                    prep_doneAt['month'],
                    prep_doneAt['day'],
                    prep_doneAt['hours'],
                    prep_doneAt['minutes'],
                    prep_doneAt['seconds'],
                ),
            }
        }

        return ReturnCodes.OK, prep_drink_obj

    def push_new_drink(self, drink_type_id, new_order_use_ice=False, drink_size=1):
        try:
            self._new_order_use_ice_node.set_value(RobobarOpcClient.get_ua_boolean_object(new_order_use_ice))
            self._new_order_drink_size_node.set_value(RobobarOpcClient.get_ua_integer_object(drink_size))
            self._new_drink_type_id_node.set_value(RobobarOpcClient.get_ua_integer_object(drink_type_id))
            self._push_new_order_node.set_value(RobobarOpcClient.get_ua_boolean_object(True))
        except Exception as e:
            print('Exception message: {0}\nTry pushing new drink into queue later.'.format(e))
            return ReturnCodes.NOK, None

        time.sleep(0.1)
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < 5:
            if self._push_new_order_node.get_value() is False:
                return self.get_new_order_status()
            time.sleep(0.5)

        return ReturnCodes.TIMEOUT, None

    def get_new_order_status(self):
        if self.connected is False:
            print('ERROR @ get_new_order_status: Server is not connected. Please, try later.')
            return ReturnCodes.NO_CONNECTION, None

        try:
            order_pushed_successfully = self._order_pushed_successfully_node.get_value()
            success_order_number = self._success_order_number_node.get_value()
        except Exception as e:
            print('Exception message: {0}\nTry getting prep drink later.'.format(e))
            return ReturnCodes.NOK, None
        
        new_order_status = {
            "newOrderStatus": {
                "orderPushedSuccessfully": order_pushed_successfully,
                "pushedOrderNumber": success_order_number,
            }
        }

        return ReturnCodes.OK, new_order_status

    def create_and_maintain_connection(self):
        """Creates an OPC connection and maintains it. If connection is lost, it tries to reconnect each second."""
        while True:
            # connection to server
            try:
                self.connect()
                # required to read structures (otherwise, ExtensionObject byte array returned)
                self.load_type_definitions()
                self._init_nodes()
                print('INFO: New connection created.')
                self.connected = True
            except Exception as e:
                self.connected = False
                print(e)
                print('ERROR: Connection could not be created.')
                time.sleep(1)
                continue  # try to connect again
            
            # checking connection status
            try:
                while True:
                    self._server_state_node.get_value()
                    time.sleep(1)
            except Exception as e:
                print(e)
                print('ERROR: Could not get value of server_state_node.')

            # try to disconnect
            try:
                self.disconnect()
                print('INFO: Connection was successfully closed.')
            except Exception as e:
                print(e)
                print('ERROR: Connection could not be closed.')
            finally:
                self.connected = False


if __name__ == "__main__":
    print('opc main')
    test_client = RobobarOpcClient('opc.tpc://10.35.91.101:4840')
    conn_thread = threading.Thread(target=test_client.create_and_maintain_connection)
    conn_thread.start()
