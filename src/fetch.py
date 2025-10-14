"""
Data fetch layer for the NASA NeoWs Data Pipeline.

This module retrieves Near-Earth Object (NEO) data from NASA's NeoWs API.
It supports two execution modes:

1. DEMO_MODE (default): Loads a cached local JSON sample for offline testing.
2. LIVE_MODE: Performs authenticated API requests to the NASA NeoWs endpoint.

Typical usage example:
    python -m src.fetch

Environment variables (configured in .env):
    - DEMO_MODE: "1" or "true" to use sample data instead of API calls.
    - NASA_API_KEY: Optional; defaults to "DEMO_KEY".
"""
# This module handles data retrieval, either from a local sample data file (DEMO_MODE) or via HTTP requests to the NASA NeoWs API (LIVE_MODE).

import json # Allows the program to parse JSON data from files and API responses
import os # Allows the program to check environment variables dynamically
import time # Allows the program to implement delays for retry/backoff logic
from pathlib import Path # Allows the program to work with file system path objects in a platform-independent way
from typing import Dict, Any # Provides type hinting for dictionaries with string keys and any-type values

import requests # Allows the program to make HTTP requests to external APIs (LIVE_MODE)


from .config import ( # Imports these configuration variables from the config module
    DEMO_MODE, # Boolean flag indicating whether to use demo mode (True = local sample data) or live mode (False = API calls)
    NASA_API_KEY, # The NASA API key to use for authenticated requests (defaults to NASA's free "DEMO_KEY" if not set)
    NASA_API_BASE_URL, # The base URL for the NASA NeoWs API
    SAMPLE_DATA_DIR, # Path to the directory containing local sample data files
)
# Endpoint for fetching NEO data: /neo/rest/v1/feed
FEED_URL = f"{NASA_API_BASE_URL}/feed" 


def _http_get( # Internal helper function to perform HTTP GET with retries and back off for LIVE_MODE API calls
        url: str, # The full endpoint URL to send the GET request to
        params: Dict[str, Any], # A dictionary of user-provided query parameters to include in the request
        max_retries: int = 4, # Maximum number of retry attempts for transient errors (default is 4)
        timeout_seconds: int = 15) -> Dict[str, Any]: # Timeout for each request in seconds (default is 15); returns the parsed JSON response as a dictionary
    """
    Perform an HTTP GET request with exponential backoff for rate-limit (429)
    and transient 5xx server errors.

    Args:
        url (str): The full endpoint URL.
        params (Dict[str, Any]): Query parameters to include in the request.
        max_retries (int, optional): Maximum number of retry attempts.
            Defaults to 4.
        timeout_seconds (int, optional): Timeout for each request in seconds.
            Defaults to 15.

    Returns:
        Dict[str, Any]: Parsed JSON response from the server.

    Raises:
        RuntimeError: If all retry attempts fail or no valid response is received.
        requests.exceptions.RequestException: For non-retryable HTTP errors.
    """
    for attempt_index in range(max_retries + 1): # Loop to attempt the request up to max_retries + 1 times
        response = requests.get(url, params=params, timeout=timeout_seconds) # Sends the GET request with the provided URL, parameters, and timeout

        status_code = response.status_code # Gets the HTTP status code from the response (e.g., 200, 404, 500) 
        retryable = status_code == 429 or 500 <= status_code < 600 # Determines if the error is retryable (429 = rate limit, 5xx = server errors)

        if retryable: # If the error is retryable, prepare to retry
            backoff_seconds = 0.5 * (2 ** attempt_index) # Exponential backoff calculation (0.5s, 1s, 2s, 4s, etc.)
            print( # Print the retry attempt with status code and backoff time
                f"[retry] status={status_code} backoff={backoff_seconds:.1f}s" 
                f"(attempt {attempt_index + 1}/{max_retries + 1})"
            ) 
            time.sleep(backoff_seconds) # Waits for the calculated backoff time before retrying
            continue # Continues to the next iteration of the loop to retry the request

        response.raise_for_status() # Raises an exception for non-retryable HTTP errors (4xx client errors, or 5xx errors not already handled by retry logic)
        # If we reach here, the request was successful (status code 200)
        return response.json() # Parses and returns the JSON response as a dictionary
    
    raise RuntimeError(
        f"GET failed after {max_retries + 1} attempts to {url} with params {params}" # Raises a RuntimeError if all retry attempts fail
    )


def fetch_feed(start_date: str, end_date: str) -> Dict[str, Any]: # Main function to fetch NEO feed data for a given date range (takes user-provided start and end dates as strings)
    """
    Retrieve Near-Earth Object data for a specified date range.

    This function loads sample data in DEMO_MODE or fetches live data
    from NASA's NeoWs API in LIVE_MODE. Results contain detailed asteroid
    information grouped by date.

    Args:
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.

    Returns:
        Dict[str, Any]: The full feed JSON response from NeoWs containing
        asteroid data grouped by date.

    Raises:
        FileNotFoundError: If DEMO_MODE is enabled but the sample file is missing.
        requests.exceptions.RequestException: If a network or API error occurs.
    """
    # Check DEMO_MODE dynamically to allow runtime override
    demo_mode = os.environ.get("DEMO_MODE", "0").lower() in ("1", "true", "yes")
    
    if demo_mode: # If DEMO_MODE is True, load data from the local sample file
        # Validate requested dates are within sample range
        if start_date < "2025-01-01" or end_date > "2025-10-31":
            raise ValueError("Demo mode supports dates from 2025-01-01 to 2025-10-31")
            
        sample_path = Path(SAMPLE_DATA_DIR) / "feed_sample.json" # Constructs the full path to the sample JSON file
        print(f"[DEMO_MODE] Loading cached sample from {sample_path}") 
        with sample_path.open("r", encoding="utf-8") as sample_file: # Opens the sample file for reading with UTF-8 encoding (to handle any special or non-ASCII characters in the JSON)
            sample_json: Dict[str, Any] = json.load(sample_file) # Parses the JSON content into a dictionary
            return sample_json # Returns the parsed local sample JSON data
        
    params = {
        "start_date": start_date, # User-provided start date for the API request
        "end_date": end_date, # User-provided end date for the API request
        "api_key": NASA_API_KEY, # API key for authentication
    }
    print(f"[LIVE_MODE] GET {FEED_URL} params={params}") 
    return _http_get(FEED_URL, params=params) # Calls the internal _http_get function to perform the API request and return the JSON response (if in LIVE_MODE)


# Verifies functionality when running this file directly
if __name__ == "__main__":
    """
    Script entry point for manual testing.

    DEMO_MODE=1 -> Loads local JSON file.
    DEMO_MODE=0 -> Fetches live data from NASA using DEMO_KEY or user API key.
    """
    feed_json = fetch_feed("2025-10-01", "2025-10-07")
    print("Top-level keys:", list(feed_json.keys()))