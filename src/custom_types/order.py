"""Definition of Order model."""

from typing import Annotated, Any

from pydantic import AfterValidator, AliasChoices, BaseModel, Field

from utilities.datetime_functions import get_datetime_dict_from_byte_array, get_datetime_string


def datetime_int_validator(value: bytes | str | None) -> int | str | None:
    """Validate that the value is either an integer, a string, or None."""
    if value is None or isinstance(value, str):
        return value
    return get_datetime_string(
        **get_datetime_dict_from_byte_array(value),
    )


class Order(BaseModel):
    """Model representing a drink order in the queue."""

    drink_order_id: int = Field(
        validation_alias=AliasChoices("orderId", "drink_order_id"),
        serialization_alias="drinkOrderId",
    )
    drink_type_id: int = Field(
        validation_alias=AliasChoices("drinkTypeId", "drink_type_id"),
        serialization_alias="drinkTypeId",
    )
    prep_started_at: Annotated[bytes | str | None, AfterValidator(datetime_int_validator)] = Field(
        default=None,
        validation_alias=AliasChoices("prepStartAt", "prep_started_at"),
        serialization_alias="prepStartedAt",
    )
    prep_done_at: Annotated[bytes | str | None, AfterValidator(datetime_int_validator)] = Field(
        default=None,
        validation_alias=AliasChoices("prepDoneAt", "prep_done_at"),
        serialization_alias="prepDoneAt",
    )

    @classmethod
    def model_validate_without_prep_times(cls, obj: Any) -> "Order":
        """Validate the model without prep time fields."""
        prep_time_attributes = ["prep_started_at", "prep_done_at", "prepStartAt", "prepDoneAt"]
        for attr in prep_time_attributes:
            try:
                getattr(obj, attr)
                delattr(obj, attr)
            except AttributeError:
                pass
        return cls.model_validate(
            obj.__dict__,  # type: ignore[attr-defined]
        )


class NewOrderRequest(BaseModel):
    """Model representing a new drink order request."""

    drink_type_id: int = Field(
        validation_alias=AliasChoices("drinkId", "drink_type_id"),
    )
    use_ice: bool = Field(
        validation_alias=AliasChoices("useIce", "use_ice"),
        serialization_alias="new_order_use_ice",
    )
    use_large_glass: bool = Field(
        validation_alias=AliasChoices("useLargeGlass", "use_large_glass"),
        exclude=True,
    )

    @property
    def drink_size(self) -> int:
        """Get drink size based on whether a large glass is used."""
        return 2 if self.use_large_glass else 1


class NewOrderResponse(BaseModel):
    """Model representing the response after pushing a new drink order."""

    pushed_successfully: bool = Field(serialization_alias="orderPushedSuccessfully")
    drink_order_id: int | None = Field(serialization_alias="pushedOrderNumber")
