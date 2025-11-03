"""Mock data for queue state."""

from typing import Literal

from custom_types import Order

queue: dict[Literal["queueDrinks"], list[Order]] = {
    "queueDrinks": [
        Order(drink_order_id=11, drink_type_id=1, prep_started_at=None),
        Order(drink_order_id=12, drink_type_id=2, prep_started_at=None),
        Order(drink_order_id=13, drink_type_id=1, prep_started_at=None),
        Order(drink_order_id=14, drink_type_id=2, prep_started_at=None),
        Order(drink_order_id=15, drink_type_id=1, prep_started_at=None),
        Order(drink_order_id=16, drink_type_id=2, prep_started_at=None),
    ],
}
