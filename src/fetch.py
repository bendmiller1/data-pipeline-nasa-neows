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

import json
import time
from pathlib import Path
from typing import Dict, Any

import requests


from .config import (
    DEMO_MODE,
    NASA_API_KEY,
    NASA_API_BASE_URL,
    SAMPLE_DATA_DIR,
)
# Endpoint for fetching NEO data: /neo/rest/v1/feed
FEED_URL = f"{NASA_API_BASE_URL}/feed"


def _http_get(
        url: str, 
        params: Dict[str, Any], 
        max_retries: int = 4, 
        timeout_seconds: int = 15) -> Dict[str, Any]:
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
    for attempt_index in range(max_retries + 1):
        response = requests.get(url, params=params, timeout=timeout_seconds)

        status_code = response.status_code
        retryable = status_code == 429 or 500 <= status_code < 600

        if retryable:
            backoff_seconds = 0.5 * (2 ** attempt_index)
            print(
                f"[retry] status={status_code} backoff={backoff_seconds:.1f}s"
                f"(attempt {attempt_index + 1}/{max_retries + 1})"
            )
            time.sleep(backoff_seconds)
            continue

        response.raise_for_status()
        return response.json()
    
    raise RuntimeError(
        f"GET failed after {max_retries + 1} attempts to {url} with params {params}"
    )


def fetch_feed(start_date: str, end_date: str) -> Dict[str, Any]:
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
    if DEMO_MODE:
        sample_path = Path(SAMPLE_DATA_DIR) / "feed_sample.json"
        print(f"[DEMO_MODE] Loading cached sample from {sample_path}")
        with sample_path.open("r", encoding="utf-8") as sample_file:
            sample_json: Dict[str, Any] = json.load(sample_file)
            return sample_json
        
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "api_key": NASA_API_KEY,
    }
    print(f"[LIVE_MODE] GET {FEED_URL} params={params}")
    return _http_get(FEED_URL, params=params)

if __name__ == "__main__":
    """
    Script entry point for manual testing.

    DEMO_MODE=1 -> Loads local JSON file.
    DEMO_MODE=0 -> Fetches live data from NASA using DEMO_KEY or user API key.
    """
    feed_json = fetch_feed("2025-10-01", "2025-10-07")
    print("Top-level keys:", list(feed_json.keys()))