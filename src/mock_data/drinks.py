"""Mock data for drinks."""

from typing import Literal

from custom_types.drink import Drink

drinks: dict[Literal["drinkTypes"], list[Drink]] = {
    "drinkTypes": [
        Drink(id=0, name="Coffee", prep_time_in_seconds=10),
        Drink(id=1, name="Beer", prep_time_in_seconds=30),
        Drink(id=2, name="Postmix", prep_time_in_seconds=60),
    ],
}
