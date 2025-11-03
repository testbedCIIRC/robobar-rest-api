from typing import Any

from fastapi.routing import APIRouter

from custom_types.order import NewOrderRequest
from opc_ua.opc_client_manager import opc_manager_instance

root_router = APIRouter(prefix="")


@root_router.get("/DrinkTypes")
def get_drinks() -> dict[str, Any]:
    """Get available drink types."""
    return_code, drink_types = opc_manager_instance.client_instance.get_drink_types_json()

    message = {
        "statusCode": return_code.value,
        "data": drink_types,
    }

    return message


@root_router.get("/QueueState")
def get_queue() -> dict[str, Any]:
    """Get current queue state."""
    return_code, queue_state = opc_manager_instance.client_instance.get_queue_drinks_json()

    message = {
        "statusCode": return_code.value,
        "data": queue_state,
    }

    return message


@root_router.get("/PlcCurrentTime")
def get_plc_current_time() -> dict[str, Any]:
    """Get current PLC time."""
    return_code, current_plc_time = opc_manager_instance.client_instance.get_current_plc_time()

    message = {
        "statusCode": return_code.value,
        "data": current_plc_time,
    }

    return message


@root_router.get("/PickUpDrinksState")
def get_served_drinks() -> dict[str, Any]:
    """Get current served drinks state."""
    return_code, pickup_drinks = opc_manager_instance.client_instance.get_pickup_drinks_json()

    message = {
        "statusCode": return_code.value,
        "data": pickup_drinks,
    }

    return message


@root_router.get("/DrinkInProgress")
def get_drink_in_progress(side: int = 0) -> dict[str, Any]:
    """Get current drink in progress for a given side."""
    return_code, prep_drink = opc_manager_instance.client_instance.get_prep_drink_json(side)

    message = {
        "statusCode": return_code.value,
        "data": prep_drink,
    }

    return message


@root_router.post("/NewDrinkInQueue")
def add_order_to_queue(drink: NewOrderRequest) -> dict[str, Any]:
    """Add a new drink order to the queue."""
    return_code, new_drink_status = opc_manager_instance.client_instance.push_new_drink(
        **drink.model_dump(by_alias=True),
    )

    message = {
        "statusCode": return_code.value,
        "data": new_drink_status,
    }

    return message
