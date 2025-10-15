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
# This module uses pandas to transform the nested JSON structure from the NeoWs API into a flat table format suitable for CSV and database storage.

from typing import Dict, List, Any # Provides type hinting for dictionaries and lists with string keys and any-type values
from pathlib import Path # Allows the program to work with file system path objects in a platform-independent way
import pandas as pd # Provides useful "database-like" data structures (Series - one column with rows, DataFrame - multiple columns with rows) and data manipulation functions

from .config import CSV_OUTPUT, SAMPLE_DATA_DIR # Imports the CSV output path and sample data directory from the config module
from .fetch import fetch_feed # imports the fetch_feed function from the fetch module to retrieve raw data


def extract_close_approaches(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]: # Function to flatten the nested near_earth_objects JSON structure into a list of dictionaries (each representing one close-approach event)
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
    flat_records: List[Dict[str, Any]] = [] # Initializes an empty List of Dictionaries to hold the flattened NEO records

    near_earth_objects = raw_data.get("near_earth_objects", {}) # Extracts the nested near_earth_objects dictionary from the raw JSON data (keyed by user-provided date strings)
    for date_key, asteroid_list in near_earth_objects.items(): # Loop #1: Iterate over each date key in the near_earth_objects dictionary
        for asteroid in asteroid_list: # Loop #2: Iterate over each asteroid entry associated with a date key
            asteroid_id = asteroid.get("id") # Extracts the asteroid's unique ID
            asteroid_name = asteroid.get("name") # Extracts the asteroid's name
            absolute_magnitude = asteroid.get("absolute_magnitude_h") # Extracts the asteroid's absolute magnitude (brightness)
            is_potentially_hazardous = asteroid.get("is_potentially_hazardous_asteroid") # Extracts the hazardous flag (boolean)

            diameter_data = asteroid.get("estimated_diameter", {}).get("kilometers", {}) # Chained .get() looks for "estimated_diameter" key and then "kilometers" subkey; returns "kilometers" subkey dictionary which is itself a dictionary with min/max keys
            diameter_min_km = diameter_data.get("estimated_diameter_min") # Extracts estimated minimum diameter in kilometers from the "kilometers" sub-dictionary
            diameter_max_km = diameter_data.get("estimated_diameter_max") # Extracts estimated maximum diameter in kilometers from the "kilometers" sub-dictionary

            close_approach_list = asteroid.get("close_approach_data", []) # Extracts the list of close-approach events for this asteroid (each event is a dictionary with date, velocity, distance, etc.)
            for approach in close_approach_list: # Loop #3: Iterate over each close-approach event for the current asteroid
                approach_date = approach.get("close_approach_date") # Extracts the close-approach date for this event
                relative_velocity_kps = float(
                    approach.get("relative_velocity", {}).get("kilometers_per_second", 0) # Extracts the relative velocity in kilometers per second from the nested "relative_velocity" dictionary and converts it to float; defaults to 0 if not found
                )
                miss_distance_km = float(
                    approach.get("miss_distance", {}).get("kilometers", 0) # Extracts the miss distance in kilometers from the nested "miss_distance" dictionary and converts it to float; defaults to 0 if not found
                )
                orbiting_body = approach.get("orbiting_body", "Unknown") # Extracts the orbiting body (e.g., Earth, Mars) for this close-approach event; defaults to "Unknown" if not found

                record = { # Contstructs a flattened dictionary record combining asteroid properties with this specific close-approach event's details
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

                flat_records.append(record) # Appends the flattened close approach event record dictionary to the list of all close approach event records

    return flat_records # Returns the complete list of flattened close-approach event record dictionaries


def transform_to_dataframe(raw_data: Dict[str, Any]) -> pd.DataFrame: # Main function to convert the raw feed JSON data into a pandas DataFrame (uses extract_close_approaches internally)
    """
    Convert the raw NeoWs JSON data into a flattened pandas DataFrame.

    Args:
        raw_data (Dict[str, Any]): Parsed JSON data containing the full feed.

    Returns:
        pd.DataFrame: A DataFrame where each row represents one close-approach
        event, with all numeric and categorical fields flattened.
    """
    flat_records = extract_close_approaches(raw_data) # Calls the extract_close_approaches() function to get the List of flattened close approach event dictionaries
    dataframe = pd.DataFrame(flat_records) # converts the List of dictionaries into a pandas DataFrame (each dictionary becomes one row, with keys as column names)
    if not dataframe.empty: # Only sort if DataFrame has data (avoids KeyError on empty DataFrames)
        dataframe.sort_values(by=["close_approach_date"], inplace=True) # Sorts the DataFrame in place based on the "close_approach_date" column in ascending order (earliest dates first)
    return dataframe # Returns the sorted DataFrame


def save_dataframe_to_csv(dataframe: pd.DataFrame, output_path: Path = CSV_OUTPUT) -> None: # Function to write the transformed DataFrame to a CSV file (creates parent directories if they do not exist yet)
    """
    Write the transformed DataFrame to a CSV file.

    The output directory will be created automatically if it does not exist.

    Args:
        dataframe (pd.DataFrame): The transformed NeoWs dataset.
        output_path (Path, optional): Target file path for CSV output.
            Defaults to CSV_OUTPUT defined in config.py.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True) # Ensures that the parent directory for the output CSV file exists (creates it if not)
    dataframe.to_csv(output_path, index=False) # Writes the DataFrame to a CSV file at the specified path without the DataFrame index column
    print(f"[transform] CSV saved to: {output_path}") # Prints a confirmation message with the output path


# Verifies transformation when running this file directly
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