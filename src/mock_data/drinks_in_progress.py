"""Mock data for drinks in progress."""

from typing import Literal

from custom_types.order import Order

drinks_in_progress: dict[Literal["drinkInProgress"], Order] = {
    "drinkInProgress": Order(drink_order_id=10, drink_type_id=0, prep_started_at=1625244872),
}
