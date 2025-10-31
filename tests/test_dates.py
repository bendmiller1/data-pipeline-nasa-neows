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
        with pytest.raises(ValueError):
            parse_date(invalid_date)