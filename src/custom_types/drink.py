"""Definition of DrinkType model."""

from pydantic import BaseModel, Field


class DrinkGroups(BaseModel):
    """Model representing drink groups."""

    soft: bool
    alcohol: bool
    coffee: bool


class DrinkParameters(BaseModel):
    """Model representing drink parameters."""

    show_parameters: bool = Field(serialization_alias="showParameters")
    coffee_strength: bool = Field(serialization_alias="coffeeStrength")
    volume_in_ml: bool = Field(serialization_alias="volumeInMl")
    milk_percentage: bool = Field(serialization_alias="milkPercentage")


class DrinkCategory(BaseModel):
    """Model representing a drink category."""

    id: int
    name: str
    icon: str
    compound_category: bool = False


class DrinkVolume(BaseModel):
    """Model representing a drink volume option."""

    id: int
    name: str
    image: str


class Drink(BaseModel):
    """Model representing a type of drink."""

    id: int
    name: str
    enabled: bool
    drink_groups: DrinkGroups = Field(serialization_alias="drinkGroups", deprecated=True)
    ice_option: bool = Field(serialization_alias="iceOption")
    volume_option: bool = Field(serialization_alias="volumeOption")
    parameters: DrinkParameters
    prep_time_in_seconds: int = Field(serialization_alias="prepTimeInSeconds")


class DrinkV2(BaseModel):
    """Model representing a type of drink with category."""

    id: int
    name: str
    category: DrinkCategory
    enabled: bool = False
    image: str
    volumes: list[DrinkVolume] | None = None
    brand_image: str | None = None
    parameters: DrinkParameters | None = None
