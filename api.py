from datetime import datetime

from flask import Flask, request
from flask_cors import CORS
from flask_restful import Api, Resource, reqparse
from enum import Enum

import opc_client
import threading

app = Flask(__name__)
api = Api(app)
CORS(app)

opc_client_instance = opc_client.RobobarOpcClient('opc.tpc://10.35.91.101:4840')

DRINK_TYPES_JSON_EXAMPLE = {
    "drinkTypes": [
        {
            "id": 0,
            "name": "Coffee",
            "prepTimeInSeconds": 10,
        },
        {
            "id": 1,
            "name": "Beer",
            "prepTimeInSeconds": 30,
        },
        {
            "id": 2,
            "name": "Whiskey",
            "prepTimeInSeconds": 60,
        }
    ]
}

QUEUE_STATE_JSON_EXAMPLE = {
    "queueDrinks": [
        {
            "drinkOrderId": 11,
            "drinkTypeId": 1,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 12,
            "drinkTypeId": 2,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 13,
            "drinkTypeId": 1,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 14,
            "drinkTypeId": 2,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 15,
            "drinkTypeId": 1,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 16,
            "drinkTypeId": 2,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 17,
            "drinkTypeId": 1,
            "prepStartedAt": None,
        },
        {
            "drinkOrderId": 18,
            "drinkTypeId": 2,
            "prepStartedAt": None,
        },
    ],
}

PICKUP_DRINKS_JSON_EXAMPLE = {
    "pickUpDrinks": {
        1: {
            "drinkOrderId": 1,
            "drinkTypeId": 1,
            "prepStartedAt": None,
        },
        11: {
            "drinkOrderId": 11,
            "drinkTypeId": 2,
            "prepStartedAt": None,
        },
    },
}

DRINK_IN_PROGRESS_JSON_EXAMPLE = {
    "drinkInProgress": {
        "drinkOrderId": 10,
        "drinkTypeId": 0,
        "prepStartedAt": "2022-05-12-15-10-00",
    }
}

class DrinkTypes(Resource):
    def get(self):
        return_code, drink_types = opc_client_instance.get_drink_types_json()

        message = {
            'statusCode': return_code.value,
            'data': drink_types,
        }

        return message

class QueueState(Resource):
    def get(self):
        return_code, queue_state = opc_client_instance.get_queue_drinks_json()

        message = {
            'statusCode': return_code.value,
            'data': queue_state,
        }

        return message

class PlcCurrentTime(Resource):
    def get(self):
        return_code, current_plc_time = opc_client_instance.get_current_plc_time()

        message = {
            'statusCode': return_code.value,
            'data': current_plc_time,
        }

        return message

class PickUpDrinksState(Resource):
    def get(self):
        return_code, pickup_drinks = opc_client_instance.get_pickup_drinks_json()

        message = {
            'statusCode': return_code.value,
            'data': pickup_drinks,
        }

        return message

class DrinkInProgress(Resource):
    def get(self, side=0):
        return_code, prep_drink = opc_client_instance.get_prep_drink_json(side)

        message = {
            'statusCode': return_code.value,
            'data': prep_drink,
        }

        return message

class NewDrinkInQueue(Resource):
    def post(self):
        json_body = request.get_json(force=True)

        return_code, new_drink_status = opc_client_instance.push_new_drink(
            drink_type_id=json_body['drinkId'],
            new_order_use_ice=True if json_body['useIce'] else False,
            drink_size=2 if json_body['useLargeGlass'] else 1
        )

        message = {
            'statusCode': return_code.value,
            'data': new_drink_status,
        }

        return message


api.add_resource(DrinkTypes, '/DrinkTypes/')
api.add_resource(QueueState, '/QueueState/')
api.add_resource(PickUpDrinksState, '/PickUpDrinksState/')
api.add_resource(DrinkInProgress, '/DrinkInProgress/<int:side>/')
api.add_resource(PlcCurrentTime, '/PlcCurrentTime/')
api.add_resource(NewDrinkInQueue, '/NewDrinkInQueue/')

if __name__ == '__main__':
    conn_thread = threading.Thread(target=opc_client_instance.create_and_maintain_connection)
    conn_thread.daemon = True
    conn_thread.start()
    app.run(host='0.0.0.0', debug=True)
