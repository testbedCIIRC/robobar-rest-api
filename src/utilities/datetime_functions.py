"""Date and time utility functions."""

TIME_VALUE_NAMES = ("year", "month", "day", "hours", "minutes", "seconds")


def get_datetime_string(year: int, month: int, day: int, hours: int, minutes: int, seconds: int) -> str:  # noqa: PLR0913
    """Format date and time values into a string."""
    return f"{year:04d}-{month:02d}-{day:02d}-{hours:02d}-{minutes:02d}-{seconds:02d}"


def get_datetime_dict_from_byte_array(byte_array: bytes) -> dict[str, int]:
    """Convert PLC's Date_and_Time variable to dictionary.

    Args:
        byte_array (List): defined in
            https://support.industry.siemens.com/cs/document/36479/date_and_time-format-for-s7-?dti=0&lc=en-WW

    Returns:
        dict: keys are taken from VALUE_NAMES

    """
    date_dict: dict[str, int] = {}

    for byte_index, dt_byte in enumerate(byte_array):
        if byte_index >= len(TIME_VALUE_NAMES):
            break
        high: int = dt_byte >> 4
        low: int = dt_byte & 0xF
        number: int = 10 * high + low
        if TIME_VALUE_NAMES[byte_index] == "year":
            number += 2000
        date_dict[TIME_VALUE_NAMES[byte_index]] = number

    return date_dict
