"""Utility functions for Cook County scraper"""


def make_snake_case(s: str) -> str:
    """Converts a given string to snake_case
    Parameters:
       s (str): A string value
    Returns:
       A string in snake_case
    """
    s = str(s)
    s = [char if char != " " else "_" for char in s]
    return "".join(s).lower()


def remove_duplicates(list_of_strings: list[str]) -> list[str]:
    """
    Remove duplicate values from a list, preserving order.

    Parameters:
        list_of_strings (List[str]): A list of strings.

    Returns:
        A list of unique strings.
    """
    return list(dict.fromkeys(list_of_strings))


def clean_pin(pin: str) -> str:
    """
    Cleans a Property Identification Number (PIN) value

    Parameters:
        pin (str): A formatted PIN

    Returns:
        A PIN without hyphens.
    """
    assert isinstance(pin, str), "pin must be of type str"
    pin = "".join(filter(str.isdigit, pin))
    assert len(pin) == 14, (
        f"pin value must evaluate to a string of  digits of length 14, instead have {pin}"
    )
    return pin