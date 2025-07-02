from typing import List, Dict

def make_snake_case(s: str) -> str:
    '''Converts a given string to snake_case
    
    Parameters: 
        s (str): An input string
    
    Returns: 
        A string in snake_case
    '''
    s = [char if char != " " else "_" for char in s]
    return "".join(s).lower()

def clean_pin(pin: str) -> str:
    '''Cleans a Property Identification Number (PIN) value so that it can be used in the base URLs.

    Parameters:
        pin (str): An input PIN

    Returns:
        A PIN without hyphens.
    '''
    assert isinstance(pin, str), "pin must be of type str"
    pin = "".join(filter(str.isdigit, pin))
    assert len(pin) == 14, "pin value must evaluate to a string of numeric digits of length 14"
    return pin

def remove_duplicates(urls: List[str]) -> List[str]:
    '''Removes duplicate values from a list object.

    Parameters:
        urls (List[str]): A list of strings.

    Returns:
        A list of unique strings.
    '''
    urls = list(set(urls))
    return urls