"""
Date utilities for the NASA NeoWs Data Pipeline.

Provides helpers to parse ISO date strings and validate a date window.

Typical usage example:
    from src.utils.dates import validate_date_range
    start, end = validate_date_range("2025-10-01", "2025-10-07")
"""
# We validate the date range here to ensure separation of concerns and consistent date handling across the codebase.

from __future__ import annotations # Allows the program to use newer type hint syntax in older Python versions

from datetime import datetime # Imports the datetime class for date manipulation
from typing import Tuple # Allows use of Tuple in type hints


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string in ISO format (YYYY-MM-DD) into a datetime object.

    Args:
        date_str (str): Date string in the form "YYYY-MM-DD".

    Returns:
        datetime: A datetime object set to midnight of the provided date.

    Raises:
        ValueError: If the input does not match "YYYY-MM-DD".
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d") # Parses the date string into a datetime object
    except ValueError as e: 
        raise ValueError(f"Invalid date format: {date_str}. Expected 'YYYY-MM-DD'.") from e # Raises a value error with a custom message if parsing fails; from e preserves the original exception context
    

def validate_date_range(start_str: str, end_str: str) -> Tuple[str, str]: # Takes two date strings (user input) as parameters and returns a Tuple with two strings
    """
    Validate and normalize a date window.

    Ensures both inputs are valid ISO dates and that start <= end. Returns
    normalized "YYYY-MM-DD" strings suitable for API requests and storage.

    Args:
        start_str (str): Start date string (inclusive), "YYYY-MM-DD".
        end_str (str): End date string (inclusive), "YYYY-MM-DD".

    Returns:
        Tuple[str, str]: A pair of normalized ISO date strings (start, end).

    Raises:
        ValueError: If either date is invalid or if start > end.
    """
    start_date = parse_date(start_str) # Calls parse_date to convert start_str to a datetime object
    end_date = parse_date(end_str) # Calls parse_date to convert end_str to a datetime object

    if start_date > end_date: 
        raise ValueError(f"Start date {start_str} cannot be after end date {end_str}.") # If the start date is after the end date, raises a ValueError with a custom message

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d") # Returns the dates formatted back to "YYYY-MM-DD" strings so that further functions can rely on consistent formatting