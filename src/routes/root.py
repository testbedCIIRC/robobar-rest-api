"""Module defining the root API routes for the Robobar REST API."""

from typing import Annotated, Any

from fastapi import Depends
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field

from custom_types.order import NewOrderRequest
from opc_ua.opc_client import ReturnCodes, RobobarOpcClient
from opc_ua.opc_client_manager import get_client_instance

root_router = APIRouter(prefix="")


class RootResponseModel(BaseModel):
    """Model representing a standard root response."""

    status_code: ReturnCodes = Field(serialization_alias="statusCode")
    data: Any


@root_router.get("/DrinkTypes")
def get_drinks(opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)]) -> RootResponseModel:
    """Get available drink types."""
    return_code, drink_types = opc_client_instance.get_drink_types_json()

    return RootResponseModel(
        status_code=return_code,
        data=drink_types,
    )


@root_router.get("/QueueState")
def get_queue(opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)]) -> RootResponseModel:
    """Get current queue state."""
    return_code, queue_state = opc_client_instance.get_queue_drinks_json()

    return RootResponseModel(
        status_code=return_code,
        data=queue_state,
    )


@root_router.get("/PlcCurrentTime")
def get_plc_current_time(
    opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)],
) -> RootResponseModel:
    """Get current PLC time."""
    return_code, current_plc_time = opc_client_instance.get_current_plc_time()

    return RootResponseModel(
        status_code=return_code,
        data=current_plc_time,
    )


@root_router.get("/PickUpDrinksState")
def get_served_drinks(
    opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)],
) -> RootResponseModel:
    """Get current served drinks state."""
    return_code, pickup_drinks = opc_client_instance.get_pickup_drinks_json()

    return RootResponseModel(
        status_code=return_code,
        data=pickup_drinks,
    )


@root_router.get("/DrinkInProgress")
def get_drink_in_progress(
    opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)],
    side: int = 0,
) -> RootResponseModel:
    """Get current drink in progress for a given side."""
    return_code, prep_drink = opc_client_instance.get_prep_drink_json(side)

    return RootResponseModel(
        status_code=return_code,
        data=prep_drink,
    )


@root_router.post("/NewDrinkInQueue")
def add_order_to_queue(
    drink: NewOrderRequest,
    opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)],
) -> RootResponseModel:
    """Add a new drink order to the queue."""
    return_code, new_drink_status = opc_client_instance.push_new_drink(
        **drink.model_dump(by_alias=True),
    )

    return RootResponseModel(
        status_code=return_code,
        data=new_drink_status,
    )
