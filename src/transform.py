"""
Transform layer for the NASA NeoWs Data Pipeline.

This module flattens nested NeoWs JSON data into a tabular format suitable
for CSV export and database insertion.

The transformation process:
1. Loads the raw feed JSON structure.
2. Iterates through all near-Earth objects grouped by date.
3. Extracts each close-approach event as an individual record.
4. Converts the flattened data into a pandas DataFrame.
5. Writes the resulting dataset to CSV.
"""

from typing import Dict, List, Any
from pathlib import Path
import pandas as pd

from .config import CSV_OUTPUT, SAMPLE_DATA_DIR
from .fetch import fetch_feed


def extract_close_approaches(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten the nested near_earth_objects structure into a list of dictionaries.

    Each record in the returned list represents a single close-approach event
    for an asteroid, combining general asteroid properties with approach details.

    Args:
        raw_data (Dict[str, Any]): The raw NeoWs JSON structure containing
            nested data under "near_earth_objects".

    Returns:
        List[Dict[str, Any]]: A list of flattened records where each element
        corresponds to one asteroid approach event.
    """
    flat_records: List[Dict[str, Any]] = []

    near_earth_objects = raw_data.get("near_earth_objects", {})
    for date_key, asteroid_list in near_earth_objects.items():
        for asteroid in asteroid_list:
            asteroid_id = asteroid.get("id")
            asteroid_name = asteroid.get("name")
            absolute_magnitude = asteroid.get("absolute_magnitude_h")
            is_potentially_hazardous = asteroid.get("is_potentially_hazardous_asteroid")

            diameter_data = asteroid.get("estimated_diameter", {}).get("kilometers", {})
            diameter_min_km = diameter_data.get("estimated_diameter_min")
            diameter_max_km = diameter_data.get("estimated_diameter_max")

            close_approach_list = asteroid.get("close_approach_data", [])
            for approach in close_approach_list:
                approach_date = approach.get("close_approach_date")
                relative_velocity_kps = float(
                    approach.get("relative_velocity", {}).get("kilometers_per_second", 0)
                )
                miss_distance_km = float(
                    approach.get("miss_distance", {}).get("kilometers", 0)
                )
                orbiting_body = approach.get("orbiting_body", "Unknown")

                record = {
                    "id" : asteroid_id,
                    "name": asteroid_name,
                    "close_approach_date": approach_date,
                    "absolute_magnitude_h": absolute_magnitude,
                    "diameter_min_km": diameter_min_km,
                    "diameter_max_km": diameter_max_km,
                    "is_potentially_hazardous": is_potentially_hazardous,
                    "relative_velocity_kps": relative_velocity_kps,
                    "miss_distance_km": miss_distance_km,
                    "orbiting_body": orbiting_body,
                }

                flat_records.append(record)

    return flat_records


def transform_to_dataframe(raw_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert the raw NeoWs JSON data into a flattened pandas DataFrame.

    Args:
        raw_data (Dict[str, Any]): Parsed JSON data containing the full feed.

    Returns:
        pd.DataFrame: A DataFrame where each row represents one close-approach
        event, with all numeric and categorical fields flattened.
    """
    flat_records = extract_close_approaches(raw_data)
    dataframe = pd.DataFrame(flat_records)
    dataframe.sort_values(by=["close_approach_date"], inplace=True)
    return dataframe


def save_dataframe_to_csv(dataframe: pd.DataFrame, output_path: Path = CSV_OUTPUT) -> None:
    """
    Write the transformed DataFrame to a CSV file.

    The output directory will be created automatically if it does not exist.

    Args:
        dataframe (pd.DataFrame): The transformed NeoWs dataset.
        output_path (Path, optional): Target file path for CSV output.
            Defaults to CSV_OUTPUT defined in config.py.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)
    print(f"[transform] CSV saved to: {output_path}")


if __name__ == "__main__":
    """
    Script entry point for manual testing.

    Fetches sample data (from DEMO_MODE) or live data (if configured),
    transforms it, prints a preview, and writes the CSV output.
    """
    raw_feed_data = fetch_feed("2025-10-01", "2025-10-03")
    transformed_dataframe = transform_to_dataframe(raw_feed_data)
    print(transformed_dataframe.head())
    save_dataframe_to_csv(transformed_dataframe)