"""Mock data for served drinks."""

from typing import Literal

from custom_types.order import Order

served_drinks: dict[Literal["pickUpDrinks"], dict[int, Order]] = {
    "pickUpDrinks": {
        1: Order(drink_order_id=1, drink_type_id=1, prep_started_at=None),
        11: Order(drink_order_id=11, drink_type_id=2, prep_started_at=None),
    },
}
