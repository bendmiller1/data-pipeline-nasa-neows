"""
Unit tests for date related utilities in utils/dates.py.
"""

from __future__ import annotations
from datetime import datetime
import pytest
from src.utils.dates import parse_date, validate_date_range

class TestParseDate:

    @pytest.mark.parametrize("input_date, expected_year, expected_month, expected_day", [
        ("2025-01-15", 2025, 1, 15),  # Standard valid date
        ("2023-12-31", 2023, 12, 31),  # Standard valid date at year end
        ("2024-02-29", 2024, 2, 29),  # Leap year date
    ])
    
    def test_valid_dates(self, input_date, expected_year, expected_month, expected_day):
        """
        Test that parse_date correctly converts valid ISO date strings to datetime objects.
        
        Verifies the function:
        - Accepts properly formatted "YYYY-MM-DD" date strings
        - Returns datetime objects with correct year, month, and day values
        - Handles edge cases like leap year dates (2024-02-29)
        - Maintains data integrity during string-to-datetime conversion
        
        This validates the core happy path functionality that enables reliable
        date parsing for user inputs and API request formatting.
        """
        valid_date_result = parse_date(input_date)
        assert valid_date_result.year == expected_year
        assert valid_date_result.month == expected_month
        assert valid_date_result.day == expected_day
    
    @pytest.mark.parametrize("invalid_date", [
        "2025-01-32",  # Invalid day
        "2025-13-01",  # Invalid month
        "3025-02-29",  # Invalid year
        "2025/01/15",  # Wrong format
        "not-a-date",  # Non-date string
        "",            # Empty string
    ])

    def test_invalid_dates(self, invalid_date):
        """
        Test that parse_date raises ValueError for invalid date strings.
        
        Verifies the function:
        - Rejects improperly formatted date strings (wrong separators, etc.)
        - Handles impossible dates (invalid days, months, years)
        - Raises ValueError with descriptive error messages
        - Provides consistent error handling for edge cases like empty strings
        
        This validates robust error handling that prevents the system from
        processing malformed date inputs and provides clear feedback to users.
        """
        with pytest.raises(ValueError):
            parse_date(invalid_date)