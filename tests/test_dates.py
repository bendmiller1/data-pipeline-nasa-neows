"""
Comprehensive unit tests for the dates utility module.

This test suite provides complete coverage for date handling functions
that support the NASA NEOWs data pipeline with reliable date parsing
and validation capabilities for user inputs and API interactions.

Test Classes:
    TestParseDate: Tests ISO date string parsing and datetime conversion
    TestValidateDateRange: Tests date range validation and normalization

Coverage:
    - Valid date string parsing with edge cases (leap years, boundaries)
    - Invalid date string handling with comprehensive error scenarios
    - Date range validation for API request formatting
    - Logical date range validation (start <= end)
    - Error handling for malformed date inputs and impossible ranges

The test suite uses pytest parametrize decorators for efficient testing
of multiple scenarios while maintaining clear, readable test organization
that validates reliable date handling across the data pipeline.
"""

from __future__ import annotations
from datetime import datetime
import pytest
from src.utils.dates import parse_date, validate_date_range

class TestParseDate:
    """
    Unit tests for the parse_date function.
    
    Tests date string parsing logic that converts ISO format date strings
    into datetime objects for reliable date handling across the application.
    """

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


class TestValidateDateRange:
    """
    Unit tests for the validate_date_range function.
    
    Tests date range validation logic that ensures date ranges are properly
    formatted and logically valid for API requests and data processing.
    """

    @pytest.mark.parametrize("start_str, end_str, expected_start_str, expected_end_str", [
        ("2025-01-01", "2025-01-31", "2025-01-01", "2025-01-31"),  # 1 month range
        ("2025-01-15", "2025-01-22", "2025-01-15", "2025-01-22"),  # 1 week range
        ("2025-12-31", "2026-01-01", "2025-12-31", "2026-01-01"),  # Year boundary
        ("2025-06-01", "2025-06-01", "2025-06-01", "2025-06-01"),  # Same day
    ])

    def test_valid_date_ranges(self, start_str, end_str, expected_start_str, expected_end_str):
        """
        Test that validate_date_range correctly validates and returns valid date ranges.
        
        Verifies the function:
        - Accepts valid start and end date strings in "YYYY-MM-DD" format
        - Returns datetime objects with correct year, month, and day values
        - Handles edge cases like same-day ranges and year boundaries
        - Maintains data integrity during string-to-datetime conversion
        
        This validates the core happy path functionality that enables reliable
        date range validation for user inputs and API request formatting.
        """
        start_date, end_date = validate_date_range(start_str, end_str)
        assert start_date == expected_start_str
        assert end_date == expected_end_str

    
    @pytest.mark.parametrize("start_str, end_str", [
        ("2025-01-31", "2025-01-01"),  # Start date after end date
        ("2025-02-30", "2025-03-01"),  # Invalid start date
        ("2025-01-01", "2025-13-01"),  # Invalid end date
        ("not-a-date", "2025-01-15"),  # Non-date start string
        ("2025-01-15", "also-not-a-date"),  # Non-date end string
    ])

    def test_invalid_date_ranges(self, start_str, end_str):
        """
        Test that validate_date_range raises ValueError for invalid date ranges.
        
        Verifies the function:
        - Rejects ranges where start date is after end date
        - Handles improperly formatted date strings
        - Raises ValueError with descriptive error messages
        - Provides consistent error handling for edge cases
        
        This validates robust error handling that prevents the system from
        processing malformed date ranges and provides clear feedback to users.
        """
        with pytest.raises(ValueError):
            validate_date_range(start_str, end_str)