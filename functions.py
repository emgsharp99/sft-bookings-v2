import re
from datetime import datetime

def parse_phone_number(number: str) -> str:
    """
    Normalizes a UK phone number to local format starting with '0'.

    This function removes all non-digit characters from the input,
    strips the UK country code prefix ('44') if present, and ensures
    the resulting number starts with a leading zero.

    Args:
        number (str): The raw phone number (e.g. '+44 7802 312241').

    Returns:
        str: Normalized UK phone number (e.g. '07802312241').
    """
    number = ''.join(filter(str.isdigit, number))
    if number.startswith("44"):
        number = number[2:]
    if not number.startswith("0"):
        number = "0" + number
    return number


def parse_long_form_date(date_str: str) -> str:
    """
    Converts a string like 'Sunday 25th January 2026' into '25/01/2026'.

    Args:
        date_str (str): Date in the format 'DayOfWeek DDth Month YYYY'

    Returns:
        str: Date in 'dd/mm/yyyy' format.
    """
    # Remove the day of the week and ordinal suffix
    # E.g., 'Sunday 25th January 2026' -> '25 January 2026'
    cleaned = re.sub(r'\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b', '', date_str)
    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', cleaned).strip()

    # Parse to datetime object
    dt = datetime.strptime(cleaned, "%d %B %Y")

    # Format to dd/mm/yyyy
    return dt.strftime("%d/%m/%Y")

