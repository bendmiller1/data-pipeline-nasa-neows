"""
Data fetch layer for NASA NeoWs Data Pipeline.

- In DEMO_MODE: loads a local JSON sample (no network).
- In LIVE_MODE: calls the official NASA NeoWs API using DEMO_KEY or 
user-provided API key.
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
    Perform an HTTP GET with exponential backoff on rate-limit (429) 
    and 5xx errors.
    Returns parsed JSON on success; raises on failure after retries.
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
    Fetch NeoWs 'feed' for a date range (YYYY-MM-DD to YYYY-MM-DD).

    DEMO_MODE:
      - Reads local file: sample_data/feed_sample.json
    LIVE:
      - Calls: /feed?start_date=...&end_date=...&api_key=...
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
    # Quick manual test:
    # DEMO_MODE=1 -> loads local JSON
    # DEMO_MODE=0 -> makes a real API call using DEMO_KEY or your configured key
    feed_json = fetch_feed("2025-10-01", "2025-10-07")
    print("Top-level keys:", list(feed_json.keys()))