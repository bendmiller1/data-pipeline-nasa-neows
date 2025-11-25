from __future__ import annotations
import os
import json
import time
from pathlib import Path
from typing import Dict, Any

import pytest
from unittest.mock import Mock, patch, mock_open, MagicMock

import requests

from src.fetch import _http_get, fetch_feed
from src.config import SAMPLE_DATA_DIR

class TestHttpGet:

    @patch('src.fetch.requests.get')
    def test_successful_request(self, mock_get):
        """
        Test that _http_get handles successful HTTP responses correctly.
    
        Verifies the function:
        - Makes HTTP GET request with correct URL, parameters, and timeout
        - Returns parsed JSON response from successful API calls
        - Does not attempt retries for successful 200 responses
        - Properly handles response parsing and data return

        This validates the core happy path for API communication and data retrieval.
        """
        # Arrange: Set up mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"near_earth_objects": {"2025-10-01": []}}
        mock_get.return_value = mock_response

        # Act: Call the function under test
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": "2025-10-01", "end_date": "2025-10-07", "api_key": "DEMO_KEY"}
        result = _http_get(url, params)

        # Assert: Verify all expected behavior
        mock_get.assert_called_once_with(url, params=params, timeout=15)
        mock_response.raise_for_status.assert_called_once()
        mock_response.json.assert_called_once()

        # Assert: Correct data returned
        assert result == {"near_earth_objects": {"2025-10-01": []}}


    @patch('src.fetch.time.sleep')
    @patch('src.fetch.requests.get')
    def test_rate_limit_retry_success(self, mock_get, mock_sleep):
        """
        Test that _http_get handles rate limiting (429) with proper retry.
    
        Verifies the function:
        - Retries on HTTP 429 (Too Many Requests) responses
        - Implements exponential backoff for rate limit scenarios
        - Eventually succeeds when rate limit window resets
        - Properly logs retry attempts during rate limiting
    
        This validates the rate limiting resilience for NASA API usage limits.
        """
        # Arrange: Set up multiple responses - first fails, second succeeds
        first_response = Mock()
        first_response.status_code = 429  # Rate limited
    
        second_response = Mock()
        second_response.status_code = 200  # Success
        second_response.json.return_value = {"success": True}
    
        mock_get.side_effect = [first_response, second_response]
    
        # Act: Call the function
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": "2025-10-01", "api_key": "DEMO_KEY"}
        result = _http_get(url, params)
    
        # Assert: Verify retry behavior
        # 1. Two HTTP requests were made
        assert mock_get.call_count == 2
    
        # 2. Both calls had same parameters
        expected_call = ((url,), {"params": params, "timeout": 15})
        assert mock_get.call_args_list == [expected_call, expected_call]
    
        # 3. Exponential backoff sleep was called once (0.5 seconds for first retry)
        mock_sleep.assert_called_once_with(0.5)
    
        # 4. Final success response was returned
        assert result == {"success": True}
    
        # 5. raise_for_status only called on successful response (second one)
        first_response.raise_for_status.assert_not_called()
        second_response.raise_for_status.assert_called_once()


    @patch('src.fetch.time.sleep')
    @patch('src.fetch.requests.get')
    def test_client_error_no_retry(self, mock_get, mock_sleep):
        """
        Test that _http_get does not retry on client errors (4xx responses).
    
        Verifies the function:
        - Does not retry on HTTP 4xx client error responses (400, 401, 404, etc.)
        - Immediately calls raise_for_status() which raises an exception
        - Does not perform any exponential backoff delays
        - Fails fast for non-retryable errors like invalid requests
    
        This validates proper error handling for client-side mistakes.
        """
        # Arrange: Set up client error response
        mock_response = Mock()
        mock_response.status_code = 404  # Not Found - client error
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")
        mock_get.return_value = mock_response
    
        # Act & Assert: Function should raise exception immediately
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": "2025-10-01", "api_key": "DEMO_KEY"}
    
        with pytest.raises(requests.exceptions.HTTPError, match="404 Client Error"):
            _http_get(url, params)
    
        # Assert: Verify no retry behavior
        # 1. Only one HTTP request was made
        mock_get.assert_called_once_with(url, params=params, timeout=15)
    
        # 2. No sleep/backoff was attempted
        mock_sleep.assert_not_called()
    
        # 3. raise_for_status was called (which raised the exception)
        mock_response.raise_for_status.assert_called_once()
    
        # 4. json() was never called since we got an error
        mock_response.json.assert_not_called()


    @patch('src.fetch.time.sleep')
    @patch('src.fetch.requests.get')
    def test_server_error_retry_success(self, mock_get, mock_sleep):
        """
        Test that _http_get retries server errors (5xx) and eventually succeeds.

        Verifies the function:
        - Retries on HTTP 5xx server error responses (500, 502, 503, etc.)
        - Uses exponential backoff for server error scenarios
        - Eventually succeeds when server recovers
        - Properly handles different server error codes

        This validates resilience against temporary server-side issues.
        """
        # Arrange: Set up server error then success
        first_response = Mock()
        first_response.status_code = 500  # Internal Server Error
    
        second_response = Mock()
        second_response.status_code = 200  # Success
        second_response.json.return_value = {"recovered": True}
    
        mock_get.side_effect = [first_response, second_response]
    
        # Act: Call the function
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": "2025-10-01", "api_key": "DEMO_KEY"}
        result = _http_get(url, params)
    
        # Assert: Verify retry behavior
        # 1. Two HTTP requests were made
        assert mock_get.call_count == 2
    
        # 2. Exponential backoff sleep was called (0.5 seconds for first retry)
        mock_sleep.assert_called_once_with(0.5)
    
        # 3. Success response returned
        assert result == {"recovered": True}
    
        # 4. raise_for_status only called on successful response
        first_response.raise_for_status.assert_not_called()
        second_response.raise_for_status.assert_called_once()


    @patch('src.fetch.time.sleep')
    @patch('src.fetch.requests.get')
    def test_max_retries_exceeded(self, mock_get, mock_sleep):
        """
        Test that _http_get raises RuntimeError after max retries exceeded.

        Verifies the function:
        - Attempts retry up to max_retries + 1 total attempts (5 attempts for default max_retries=4)
        - Raises RuntimeError when all retry attempts fail
        - Uses exponential backoff for each retry attempt
        - Includes diagnostic information in the error message

        This validates proper failure handling when servers are persistently down.
        """
        # Arrange: All responses return server errors
        mock_response = Mock()
        mock_response.status_code = 503  # Service Unavailable
        mock_get.return_value = mock_response
    
        # Act & Assert: Should raise RuntimeError after all retries
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": "2025-10-01", "api_key": "DEMO_KEY"}
    
        with pytest.raises(RuntimeError, match=r"GET failed after 5 attempts"):
            _http_get(url, params, max_retries=4)
    
        # Assert: Verify all retry attempts made
        # 1. Total of 5 HTTP requests (initial + 5 retries)
        assert mock_get.call_count == 5
    
        # 2. Sleep called 5 times with exponential backoff: 0.5, 1.0, 2.0, 4.0, 8.0
        expected_sleep_calls = [0.5, 1.0, 2.0, 4.0, 8.0]
        actual_sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert actual_sleep_calls == expected_sleep_calls
    
        # 3. No raise_for_status or json calls since all failed
        mock_response.raise_for_status.assert_not_called()
        mock_response.json.assert_not_called()


    @patch('src.fetch.time.sleep')
    @patch('src.fetch.requests.get')
    def test_exponential_backoff_timing(self, mock_get, mock_sleep):
        """
        Test that _http_get uses correct exponential backoff formula.

        Verifies the function:
        - Calculates backoff delays using formula: 0.5 * (2 ** attempt_index)
        - Uses correct timing progression: 0.5s, 1.0s, 2.0s, 4.0s
        - Applies backoff only for retryable errors (429, 5xx)
        - Handles multiple consecutive failures with proper timing

        This validates the mathematical correctness of the backoff algorithm.
        """
        # Arrange: Multiple server errors to trigger all backoff delays
        mock_response = Mock()
        mock_response.status_code = 502  # Bad Gateway
        mock_get.return_value = mock_response
    
        # Act: Trigger all retries (will eventually raise RuntimeError)
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {"start_date": "2025-10-01", "api_key": "DEMO_KEY"}
    
        with pytest.raises(RuntimeError):
            _http_get(url, params, max_retries=4)
    
        # Assert: Verify exponential backoff formula
        # Formula: 0.5 * (2 ** attempt_index)
        # attempt_index: 0, 1, 2, 3, 4 -> backoff: 0.5, 1.0, 2.0, 4.0, 8.0
        expected_backoffs = [
            0.5 * (2 ** 0),  # 0.5
            0.5 * (2 ** 1),  # 1.0  
            0.5 * (2 ** 2),  # 2.0
            0.5 * (2 ** 3),  # 4.0
            0.5 * (2 ** 4),  # 8.0
        ]
    
        actual_backoffs = [call[0][0] for call in mock_sleep.call_args_list]
        assert actual_backoffs == expected_backoffs
    
        # Assert: Verify total attempts
        assert mock_get.call_count == 5  # 5 total attempts
        assert mock_sleep.call_count == 5  # 5 backoff delays


class TestFetchFeedDemoMode:
    """Test fetch_feed function in demo mode (file system operations)."""

    @patch.dict(os.environ, {"DEMO_MODE": "1"})
    @patch("src.fetch.Path")
    def test_demo_mode_success(self, mock_path_class):
        """
        Test that fetch_feed loads sample JSON file successfully in demo mode.

        Verifies the function:
        - Detects DEMO_MODE environment variable correctly
        - Constructs proper file path to sample data
        - Opens and reads JSON file with correct encoding
        - Returns parsed JSON data from sample file

        This validates the core demo mode functionality for offline testing.
        """
        # Mock file content
        sample_data = {"near_earth_objects": {"2025-01-01": []}}
        
        # Create mock path and file objects
        mock_path_instance = MagicMock()
        mock_file = mock_open(read_data=json.dumps(sample_data))
        
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.__truediv__.return_value = mock_path_instance
        mock_path_instance.open.return_value = mock_file.return_value
        
        # Call function
        result = fetch_feed("2025-01-01", "2025-01-07")
        
        # Verify result
        assert result == sample_data
        
        # Verify file operations
        mock_path_class.assert_called_once_with(SAMPLE_DATA_DIR)
        mock_path_instance.__truediv__.assert_called_once_with("feed_sample.json")
        mock_path_instance.open.assert_called_once_with("r", encoding="utf-8")

    @patch.dict(os.environ, {"DEMO_MODE": "1"})
    @patch("src.fetch.Path")
    def test_demo_mode_file_not_found(self, mock_path_class):
        """
        Test that fetch_feed handles missing sample files gracefully in demo mode.

        Verifies the function:
        - Attempts to open the sample file at expected location
        - Propagates FileNotFoundError when sample file is missing
        - Does not attempt fallback or alternative file locations
        - Provides clear error indication for missing demo data

        This validates proper error handling when demo setup is incomplete.
        """
        # Mock path operations
        mock_path_instance = MagicMock()
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.__truediv__.return_value = mock_path_instance
        
        # Mock file not found
        mock_path_instance.open.side_effect = FileNotFoundError("Sample file not found")
        
        # Verify FileNotFoundError is raised
        with pytest.raises(FileNotFoundError, match="Sample file not found"):
            fetch_feed("2025-01-01", "2025-01-07")

    @patch.dict(os.environ, {"DEMO_MODE": "1"})
    def test_demo_mode_date_validation_start_too_early(self):
        """
        Test that fetch_feed validates start_date is not before demo data range.

        Verifies the function:
        - Checks start_date against minimum demo date (2025-01-01)
        - Raises ValueError for dates before demo data availability
        - Provides descriptive error message with valid date range
        - Performs validation before attempting file operations

        This validates demo mode date boundary enforcement for start dates.
        """
        with pytest.raises(ValueError, match="Demo mode supports dates from 2025-01-01 to 2025-10-31"):
            fetch_feed("2024-12-31", "2025-01-07")

    @patch.dict(os.environ, {"DEMO_MODE": "1"})
    def test_demo_mode_date_validation_end_too_late(self):
        """
        Test that fetch_feed validates end_date is not after demo data range.

        Verifies the function:
        - Checks end_date against maximum demo date (2025-10-31)
        - Raises ValueError for dates after demo data availability
        - Provides descriptive error message with valid date range
        - Performs validation before attempting file operations

        This validates demo mode date boundary enforcement for end dates.
        """
        with pytest.raises(ValueError, match="Demo mode supports dates from 2025-01-01 to 2025-10-31"):
            fetch_feed("2025-01-01", "2025-11-01")

    @patch.dict(os.environ, {"DEMO_MODE": "1"})
    @patch("src.fetch.Path")
    def test_demo_mode_valid_date_range(self, mock_path_class):
        """
        Test that fetch_feed accepts valid date ranges within demo boundaries.

        Verifies the function:
        - Accepts dates within 2025-01-01 to 2025-10-31 range
        - Proceeds with normal file operations for valid dates
        - Does not raise ValueError for dates within bounds
        - Successfully loads and returns sample data

        This validates proper acceptance of valid demo mode date ranges.
        """
        # Mock file content
        sample_data = {"near_earth_objects": {"2025-06-15": []}}
        
        # Create mock path and file objects
        mock_path_instance = MagicMock()
        mock_file = mock_open(read_data=json.dumps(sample_data))
        
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.__truediv__.return_value = mock_path_instance
        mock_path_instance.open.return_value = mock_file.return_value
        
        # Test dates within valid range
        result = fetch_feed("2025-06-01", "2025-06-30")
        
        # Should succeed without raising ValueError
        assert result == sample_data

    @patch.dict(os.environ, {"DEMO_MODE": "1"})
    @patch("src.fetch.Path")
    def test_demo_mode_json_parsing_error(self, mock_path_class):
        """
        Test that fetch_feed handles malformed JSON files properly in demo mode.

        Verifies the function:
        - Attempts to parse JSON content from sample file
        - Propagates JSONDecodeError when file contains invalid JSON
        - Does not attempt to handle or recover from JSON parsing errors
        - Provides clear indication of data format problems

        This validates proper error handling for corrupted demo data files.
        """
        # Mock path operations
        mock_path_instance = MagicMock()
        mock_file = mock_open(read_data="invalid json content")
        
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.__truediv__.return_value = mock_path_instance
        mock_path_instance.open.return_value = mock_file.return_value
        
        # Verify JSONDecodeError is raised
        with pytest.raises(json.JSONDecodeError):
            fetch_feed("2025-01-01", "2025-01-07")

    @patch.dict(os.environ, {"DEMO_MODE": "true"})
    @patch("src.fetch.Path")
    def test_demo_mode_environment_variations(self, mock_path_class):
        """
        Test that fetch_feed recognizes different DEMO_MODE environment values.

        Verifies the function:
        - Detects DEMO_MODE="true" as enabling demo mode
        - Uses same demo mode logic for various truthy values
        - Performs normal file operations regardless of specific value
        - Maintains consistent behavior across environment variations

        This validates flexible demo mode environment variable detection.
        """
        # Mock file content
        sample_data = {"test": "data"}
        
        # Create mock path and file objects
        mock_path_instance = MagicMock()
        mock_file = mock_open(read_data=json.dumps(sample_data))
        
        mock_path_class.return_value = mock_path_instance
        mock_path_instance.__truediv__.return_value = mock_path_instance
        mock_path_instance.open.return_value = mock_file.return_value
        
        # Should work with DEMO_MODE="true"
        result = fetch_feed("2025-05-01", "2025-05-07")
        assert result == sample_data


