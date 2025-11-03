"""OPC UA client for Robobar system."""

import threading
import time
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import asyncua.ua.uatypes as ua
from asyncua.sync import Client

from custom_types.drink import Drink, DrinkGroups, DrinkParameters
from custom_types.order import NewOrderResponse, Order
from utilities import logger
from utilities.datetime_functions import get_datetime_dict_from_byte_array, get_datetime_string

TIMEOUT_TIME_SECONDS = 5


class ReturnCodes(Enum):
    """Return codes for OPC UA client methods."""

    OK = 0
    NOK = -1
    TIMEOUT = -2
    NO_CONNECTION = -3


class RobobarOpcClient(Client):
    """OPC UA client for Robobar system."""

    connected = False
    exit = False

    def __init__(self, url: str) -> None:
        """Initialize RobobarOpcClient."""
        super().__init__(url)

    def _init_nodes(self) -> None:
        """Initialize all required nodes."""
        self._queue_items_node = self.get_node('ns=3;s="Drink_DB"."drinkQueue"."items"')
        self._queue_start_index = self.get_node(
            'ns=3;s="Drink_DB"."drinkQueue"."firstItemIndex"',
        )
        self._queue_end_index = self.get_node(
            'ns=3;s="Drink_DB"."drinkQueue"."lastItemIndex"',
        )
        self._current_queue_length_node = self.get_node(
            'ns=3;s="Drink_DB"."drinkQueue"."currentQueueLength"',
        )
        self._queue_read_index_node = self.get_node(
            'ns=3;s="Drink_DB"."drinkQueue"."readIndex"',
        )
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
        self._server_state_node = self.get_node("i=2259")
        self._plc_time_node = self.get_node('ns=3;s="Queue_Handle_DB"."currentTime"')

        self._push_new_order_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."pushNewOrderToQueue"',
        )
        self._new_order_use_ice_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."newOrderUseIce"',
        )
        self._new_order_drink_size_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."newOrderDrinkSizeId"',
        )
        self._new_drink_type_id_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Output"."newOrderDrinkTypeId"',
        )
        self._order_pushed_successfully_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Input"."orderPushedSuccessfully"',
        )
        self._success_order_number_node = self.get_node(
            'ns=3;s="Web_Terminal_Communication"."Terminal_Input"."successOrderNumber"',
        )

    @staticmethod
    def get_ua_integer_object(integer_number: int) -> ua.DataValue:
        """Get OPC UA integer object from python integer value."""
        return ua.DataValue(Value=ua.Variant(integer_number, ua.VariantType.Int16))

    @staticmethod
    def get_ua_boolean_object(boolean_value: bool) -> ua.DataValue:  # noqa: FBT001
        """Get OPC UA boolean object from python boolean value."""
        return ua.DataValue(Value=ua.Variant(boolean_value, ua.VariantType.Boolean))

    @staticmethod
    def get_items_from_circular_buffer(
        buffer: list[Any],
        index_of_first_item: int,
        queue_length: int,
        index_of_buffer_start: int,
        index_of_buffer_end: int,
    ) -> list[Any]:
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
        ret_items: list[Any] = []

        for queue_index in range(queue_length):
            buffer_index = index_of_first_item + queue_index

            if buffer_index > index_of_buffer_end:
                buffer_index = (buffer_index - index_of_buffer_end - 1) + index_of_buffer_start

            ret_items.append(buffer[buffer_index])

        return ret_items

    def get_queue_drinks_json(self) -> tuple[ReturnCodes, dict[str, list[Order]] | None]:
        """Return an object with drinks in queue, that is ready to be the json response of api.

        First, items are loaded from circulare buffer using get_tiems_from_circular_buffer and then
        parsed in a way to return json ready object/dictionary structure.

        Returns:
            dict: json ready dictionary structure

        """
        if self.connected is False:
            logger.error(
                "Server is not connected. Please, try later.",
            )
            return ReturnCodes.NO_CONNECTION, None

        try:
            values = self.read_values(
                [
                    self._queue_items_node,
                    self._queue_read_index_node,
                    self._current_queue_length_node,
                    self._queue_start_index,
                    self._queue_end_index,
                ],
            )
        except Exception as e:
            logger.error(f"Exception message: {e}\nTry getting queue drinks later.")

            return ReturnCodes.NOK, None

        queue_drinks: list[Order] = list(
            map(Order.model_validate_without_prep_times, RobobarOpcClient.get_items_from_circular_buffer(*values)),
        )

        queue_drinks_obj = {
            "queueDrinks": queue_drinks,
        }

        return ReturnCodes.OK, queue_drinks_obj

    def get_drink_types_json(self) -> tuple[ReturnCodes, dict[str, list[Drink]] | None]:
        """Return all drink types from the PLC DrinkDB in a json ready object/dictionary structure.

        Returns:
            dict: json ready dictionary structure

        """
        if self.connected is False:
            logger.error(
                "Server is not connected. Please, try later.",
            )
            return ReturnCodes.NO_CONNECTION, None

        try:
            drink_types_value = self._drink_types_node.get_value()
            (
                beer_enabled,
                coffee_enabled,
                postmix_enabled,
                conveyor_enabled,
                _,
            ) = self.read_values(
                [
                    self.get_node('ns=3;s="Drink_DB"."beerEnabled"'),
                    self.get_node('ns=3;s="Drink_DB"."coffeeEnabled"'),
                    self.get_node('ns=3;s="Drink_DB"."postmixEnabled"'),
                    self.get_node('ns=3;s="Drink_DB"."conveyorEnabled"'),
                    self.get_node('ns=3;s="Drink_DB"."iceEnabled"'),
                ],
            )
        except Exception as e:
            logger.error(f"Exception message: {e}\nTry getting drink types later.")

            return ReturnCodes.NOK, None

        drink_types: list[Drink] = []

        for ii, drink_type in enumerate(drink_types_value):
            is_soft = drink_type.postmixDrink != "" and drink_type.conveyorDrink == ""
            is_alco = drink_type.conveyorDrink != ""
            is_coffee = drink_type.coffeeDrink != ""
            is_beer = drink_type.recipe.robotBeer.useStep
            is_enabled = (
                (is_soft and postmix_enabled)
                or (is_alco and conveyor_enabled)
                or (is_coffee and coffee_enabled)
                or (is_beer and beer_enabled)
            )

            drink = Drink(
                id=ii,
                name=drink_type.drinkName,
                enabled=is_enabled,
                drink_groups=DrinkGroups(
                    soft=is_soft,
                    alcohol=is_alco or is_beer,
                    coffee=is_coffee,
                ),
                ice_option=False,
                volume_option=is_soft,
                parameters=DrinkParameters(
                    show_parameters=drink_type.parameters.showParameters,
                    coffee_strength=drink_type.parameters.coffeeStrength,
                    volume_in_ml=drink_type.parameters.volumeInMl,
                    milk_percentage=drink_type.parameters.milkPercentage,
                ),
                prep_time_in_seconds=drink_type.preparationTime / 1000,
            )
            drink_types.append(drink)

        drink_types_obj = {"drinkTypes": drink_types}
        return ReturnCodes.OK, drink_types_obj

    def get_pickup_drinks_json(self) -> tuple[ReturnCodes, dict[str, dict[int, Order]] | None]:
        """Return all prepared drinks that have not been taken by the customers yet.

        Returns:
            dict: json ready dictionary structure

        """
        if self.connected is False:
            logger.error(
                "Server is not connected. Please, try later.",
            )
            return ReturnCodes.NO_CONNECTION, None

        try:
            pickup_drinks_value = self._pickup_drinks_node.get_value()
        except Exception as e:
            logger.error(f"Exception message: {e}\nTry getting pickup drinks later.")
            return ReturnCodes.NOK, None

        pickup_drinks: dict[int, Order] = {}

        for ii, drink in enumerate(pickup_drinks_value):
            if drink.orderId != 0 and not drink.pickedUp:
                pickup_drinks[ii] = Order(
                    drink_order_id=drink.orderId,
                    drink_type_id=drink.drinkTypeId,
                )

        pickup_drinks_obj: dict[str, dict[int, Order]] = {"pickUpDrinks": pickup_drinks}
        return ReturnCodes.OK, pickup_drinks_obj

    def get_current_plc_time(self) -> tuple[ReturnCodes, str | None]:
        """Return PLC local time as a string in format YYYY-MM-dd-hh-mm-ss.

        Returns:
            str: string containg PLC local time in format YYYY-MM-dd-hh-mm-ss

        """
        if self.connected is False:
            logger.error(
                "Server is not connected. Please, try later.",
            )
            return ReturnCodes.NO_CONNECTION, None

        try:
            current_plc_time_byte_array = self._plc_time_node.get_value()
        except Exception as e:
            logger.error(f"Exception message: {e}\nTry plc current time later.")
            return ReturnCodes.NOK, None

        current_plc_time = get_datetime_dict_from_byte_array(
            current_plc_time_byte_array,
        )

        current_plc_time_string = get_datetime_string(**current_plc_time)

        return ReturnCodes.OK, current_plc_time_string

    def get_prep_drink_json(self, side: int = 0) -> tuple[ReturnCodes, dict[str, Order] | None]:
        """Return drink currently being prepared by IIWA1 (the left one from the perspective of the customer).

        Returns:
            dict: json ready dictionary structure

        """
        if self.connected is False:
            logger.error(
                "ERROR @ get_prep_drink_json: Server is not connected. Please, try later.",
            )
            return ReturnCodes.NO_CONNECTION, None

        try:
            prep_drink_value = self.prep_drink_nodes[side].get_value()
            prep_start_at = self.prep_drink_prepStartAt_nodes[side].get_value()
            prep_done_at = self.prep_drink_prepDoneAt_nodes[side].get_value()
        except Exception as e:
            logger.error(f"Exception message: {e}\nTry getting prep drink later.")
            return ReturnCodes.NOK, None

        prep_start_at = get_datetime_dict_from_byte_array(prep_start_at)
        prep_done_at = get_datetime_dict_from_byte_array(prep_done_at)

        prep_drink = Order(
            drink_order_id=prep_drink_value.orderId,
            drink_type_id=prep_drink_value.drinkTypeId,
            prep_started_at=get_datetime_string(
                **prep_start_at,
            ),
            prep_done_at=get_datetime_string(
                **prep_done_at,
            ),
        )

        prep_drink_obj = {
            "drinkInProgress": prep_drink,
        }

        return ReturnCodes.OK, prep_drink_obj

    def push_new_drink(
        self,
        *,
        drink_type_id: int,
        new_order_use_ice: bool = False,
        drink_size: int = 1,
    ) -> tuple[ReturnCodes, dict[str, NewOrderResponse] | None]:
        """Push a new drink order into the drink queue."""
        try:
            self._new_order_use_ice_node.set_value(
                RobobarOpcClient.get_ua_boolean_object(new_order_use_ice),
            )
            self._new_order_drink_size_node.set_value(
                RobobarOpcClient.get_ua_integer_object(drink_size),
            )
            self._new_drink_type_id_node.set_value(
                RobobarOpcClient.get_ua_integer_object(drink_type_id),
            )
            self._push_new_order_node.set_value(
                RobobarOpcClient.get_ua_boolean_object(True),  # noqa: FBT003
            )
        except Exception as e:
            logger.error(
                f"Exception message: {e}\nTry pushing new drink into queue later.",
            )
            return ReturnCodes.NOK, None

        time.sleep(0.1)
        start_time = datetime.now(tz=UTC)
        while (datetime.now(tz=UTC) - start_time).total_seconds() < TIMEOUT_TIME_SECONDS:
            if self._push_new_order_node.get_value() is False:
                return self.get_new_order_status()
            time.sleep(0.5)

        return ReturnCodes.TIMEOUT, None

    def get_new_order_status(self) -> tuple[ReturnCodes, dict[str, NewOrderResponse] | None]:
        """Return status of the last pushed drink order."""
        if self.connected is False:
            logger.error(
                "Server is not connected. Please, try later.",
            )
            return ReturnCodes.NO_CONNECTION, None

        try:
            order_pushed_successfully = self._order_pushed_successfully_node.get_value()
            success_order_number = self._success_order_number_node.get_value()
        except Exception as e:
            logger.error(f"Exception message: {e}\nTry getting prep drink later.")
            return ReturnCodes.NOK, None

        new_order_status: dict[str, NewOrderResponse] = {
            "newOrderStatus": NewOrderResponse(
                pushed_successfully=order_pushed_successfully,
                drink_order_id=success_order_number if order_pushed_successfully else None,
            ),
        }

        return ReturnCodes.OK, new_order_status

    def create_and_maintain_connection(self) -> None:
        """Create an OPC connection and maintains it. If connection is lost, it tries to reconnect each second."""
        while True:
            if self.exit:
                break
            # connection to server
            try:
                self.connect()
                # required to read structures (otherwise, ExtensionObject byte array returned)
                self.load_type_definitions()
                self._init_nodes()
                logger.info("New connection created.")
                self.connected = True
            except Exception as e:
                self.connected = False
                logger.error(e)
                logger.error("Connection could not be created.")
                time.sleep(1)
                continue  # try to connect again

            # checking connection status
            try:
                while True:
                    self._server_state_node.get_value()
                    if self.exit:
                        break
                    time.sleep(1)
            except Exception as e:
                logger.error(e)
                logger.error("Could not get value of server_state_node.")

            # try to disconnect
            try:
                self.disconnect()
                logger.info("Connection was successfully closed.")
            except Exception as e:
                logger.error(e)
                logger.error("Connection could not be closed.")
            finally:
                self.connected = False


opc_client_instance: RobobarOpcClient | None


if __name__ == "__main__":
    test_client = RobobarOpcClient("opc.tpc://10.35.91.101:4840")
    conn_thread = threading.Thread(target=test_client.create_and_maintain_connection)
    conn_thread.start()
