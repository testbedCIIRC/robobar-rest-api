from typing import Annotated, Any

from fastapi import Depends
from fastapi.routing import APIRouter

from opc_ua.opc_client import RobobarOpcClient
from opc_ua.opc_client_manager import get_client_instance

drink_router = APIRouter(prefix="drink")


@drink_router.get("/")
def get_drink(
    opc_client_instance: Annotated[RobobarOpcClient, Depends(get_client_instance)],
) -> dict[str, Any]:
    """Get available drink types."""
    return_code, drink_types = opc_client_instance.get_drink_types_json()

    return {
        "statusCode": return_code,
        "data": drink_types,
    }
