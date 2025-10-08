"""
Date utilities for the NASA NeoWs Data Pipeline.

Provides helpers to parse ISO date strings and validate a date window.

Typical usage example:
    from src.utils.dates import validate_date_range
    start, end = validate_date_range("2025-10-01", "2025-10-07")
"""

from __future__ import annotations

from datetime import datetime
from typing import Tuple


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
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Expected 'YYYY-MM-DD'.") from e
    

def validate_date_range(start_str: str, end_str: str) -> Tuple[str, str]:
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
    start_date = parse_date(start_str)
    end_date = parse_date(end_str)

    if start_date > end_date:
        raise ValueError(f"Start date {start_str} cannot be after end date {end_str}.")

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")