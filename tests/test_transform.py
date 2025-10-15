"""
Unit tests for transform.py
"""

import pytest
import test
from src.transform import extract_close_approaches, transform_to_dataframe, save_dataframe_to_csv


def test_extract_close_approaches_single_asteroid_single_event():
    """
    Test that extract_close_approaches correctly flattens a single asteroid with one close approach event.
    
    Verifies the function:
    - Returns exactly one flattened record for one input event
    - Correctly extracts all asteroid properties (id, name, magnitude, diameter, hazard status)
    - Properly converts nested approach data (date, velocity, distance, orbiting body)
    - Handles data type conversions (strings to floats where appropriate)
    
    This validates the core ETL transformation logic that converts nested JSON
    structure into flat tabular records suitable for CSV/database storage.
    """
    # Test data for a single asteroid with one close approach event
    test_data = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "12345",
                    "name": "Test Asteroid",
                    "absolute_magnitude_h": 22.1,
                    "is_potentially_hazardous_asteroid": True,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.1,
                            "estimated_diameter_max": 0.3
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                "kilometers_per_second": "5.5"
                            },
                            "miss_distance": {
                                "kilometers": "750000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                }
            ]
        }
    }
    
    test_result = extract_close_approaches(test_data)
    assert len(test_result) == 1
    test_record = test_result[0]
    assert test_record["id"] == "12345"
    assert test_record["name"] == "Test Asteroid"
    assert test_record["close_approach_date"] == "2025-01-01"
    assert test_record["absolute_magnitude_h"] == 22.1
    assert test_record["diameter_min_km"] == 0.1
    assert test_record["diameter_max_km"] == 0.3
    assert test_record["is_potentially_hazardous"] is True
    assert test_record["relative_velocity_kps"] == 5.5
    assert test_record["miss_distance_km"] == 750000.0
    assert test_record["orbiting_body"] == "Earth"


def test_extract_close_approaches_multiple_asteroids_multiple_events():
    """
    Test that extract_close_approaches correctly flattens multiple asteroids with multiple close approach events.
    
    Verifies the function:
    - Returns the correct number of flattened records for multiple input events
    - Accurately extracts and combines asteroid properties with each approach event
    - Handles multiple asteroids appearing on the same date
    - Correctly processes asteroids with multiple close approach events
    
    This validates the function's ability to handle more complex nested structures
    and ensures all relevant data is captured in the flattened output.
    """
    # Test data
    test_data = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "12345",
                    "name": "Test Asteroid 1",
                    "absolute_magnitude_h": 22.1,
                    "is_potentially_hazardous_asteroid": True,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.1,
                            "estimated_diameter_max": 0.3
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                "kilometers_per_second": "5.5"
                            },
                            "miss_distance": {
                                "kilometers": "750000"
                            },
                            "orbiting_body": "Earth"
                        },
                        {
                            "close_approach_date": "2025-01-21",
                            "relative_velocity": {
                                "kilometers_per_second": "6.0"
                            },
                            "miss_distance": {
                                "kilometers": "800000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                },
                {
                    "id": "67890",
                    "name": "Test Asteroid 2",
                    "absolute_magnitude_h": 19.5,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.5,
                            "estimated_diameter_max": 1.2
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-03",
                            "relative_velocity": {
                                "kilometers_per_second": "12.3"
                            },
                            "miss_distance": {
                                "kilometers": "1500000"
                            },
                            "orbiting_body": "Mars"
                        },
                        {
                            "close_approach_date": "2025-01-15",
                            "relative_velocity": {
                                "kilometers_per_second": "11.8"
                            },
                            "miss_distance": {
                                "kilometers": "1400000"
                            },
                            "orbiting_body": "Mars"
                        }
                    ]
                },
                {
                    "id": "11223",
                    "name": "Test Asteroid 3",
                    "absolute_magnitude_h": 25.0,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.05,
                            "estimated_diameter_max": 0.1
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-26",
                            "relative_velocity": {
                                "kilometers_per_second": "3.2"
                            },
                            "miss_distance": {
                                "kilometers": "500000"
                            },
                            "orbiting_body": "Earth"
                        },
                        {
                            "close_approach_date": "2025-02-10",
                            "relative_velocity": {
                                "kilometers_per_second": "3.5"
                            },
                            "miss_distance": {
                                "kilometers": "450000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                }
            ]
        }
    }

    test_result = extract_close_approaches(test_data)
    assert len(test_result) == 6  # 3 asteroids with a total of 6 approach events

    # Verify asteroid ids are correctly extracted
    test_asteroid_ids = [record["id"] for record in test_result]
    assert "12345" in test_asteroid_ids
    assert "67890" in test_asteroid_ids
    assert "11223" in test_asteroid_ids

    # Verify approach dates are correctly extracted
    test_approach_dates = [record["close_approach_date"] for record in test_result]
    assert "2025-01-01" in test_approach_dates
    assert "2025-01-21" in test_approach_dates
    assert "2025-01-03" in test_approach_dates
    assert "2025-01-15" in test_approach_dates
    assert "2025-01-26" in test_approach_dates
    assert "2025-02-10" in test_approach_dates


def test_close_approaches_with_empty_data():
    """
    Test that extract_close_approaches handles empty input data gracefully.
    
    Verifies the function:
    - Returns an empty list when no near_earth_objects are present
    - Does not raise exceptions or errors on empty input
    
    This ensures robustness of the ETL transformation logic when faced with
    edge cases of missing or empty data structures.
    """
    
    # Test Case 1: Empty near_earth_objects dictionary
    test_empty_dates = {"near_earth_objects": {}}
    result_empty_dates = extract_close_approaches(test_empty_dates)
    assert result_empty_dates == []

    # Test Case 2: Date exists but no asteroids
    test_no_asteroids = {"near_earth_objects": {"2025-01-01": []}}
    result_no_asteroids = extract_close_approaches(test_no_asteroids)
    assert result_no_asteroids == []

    # Test Case 3: Missing near_earth_objects key entirely
    test_missing_key = {}
    result_missing_key = extract_close_approaches(test_missing_key)
    assert result_missing_key == []


def test_extract_close_approaches_with_incomplete_data():
    """
    Test that extract_close_approaches handles missing fields gracefully.
    
    Verifies the function:
    - Returns empty list when close_approach_data is missing entirely
    - Sets None for missing asteroid fields (id, name, magnitude, diameter, hazard status)
    - Uses default values (0.0) for missing numeric approach fields (velocity, distance)
    - Maintains data integrity when only partial field sets are available
    
    This ensures the ETL process is resilient to incomplete API responses
    and malformed data while providing predictable default behaviors.
    """

    # Test data for missing close_approach_data field
    test_data_missing_approach = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "12345",
                    "name": "Test Asteroid",
                    "absolute_magnitude_h": 25.0,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.05,
                            "estimated_diameter_max": 0.1
                        }
                    },
                    # "close_approach_data" key is intentionally missing
                }
            ]
        }
    }
    test_result_missing_approach = extract_close_approaches(test_data_missing_approach)
    assert test_result_missing_approach == [] # No records should be returned since there are no approach events

    # Test data for missing individual asteroid fields
    test_data_missing_fields_asteroid = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    # "id" key is missing
                    "name": "Test Asteroid",
                    # "absolute_magnitude_h" key is missing
                    # "is_potentially_hazardous_asteroid" key is missing
                    "estimated_diameter": {
                        "kilometers": {
                            # "estimated_diameter_min" key is missing
                            "estimated_diameter_max": 0.1
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                "kilometers_per_second": "5.5"
                            },
                            "miss_distance": {
                                "kilometers": "750000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                }
            ]
        }
    }
    test_result_missing_fields_asteroid = extract_close_approaches(test_data_missing_fields_asteroid)
    assert len(test_result_missing_fields_asteroid) == 1
    test_record_missing_fields_asteroid = test_result_missing_fields_asteroid[0]
    assert test_record_missing_fields_asteroid["id"] is None
    assert test_record_missing_fields_asteroid["name"] == "Test Asteroid"
    assert test_record_missing_fields_asteroid["absolute_magnitude_h"] is None
    assert test_record_missing_fields_asteroid["is_potentially_hazardous"] is None
    assert test_record_missing_fields_asteroid["diameter_min_km"] is None
    assert test_record_missing_fields_asteroid["diameter_max_km"] == 0.1

    # Test data for missing individual approach fields
    test_data_missing_fields_approach = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "12345",
                    "name": "Test Asteroid",
                    "absolute_magnitude_h": 25.0,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.05,
                            "estimated_diameter_max": 0.1
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                # "kilometers_per_second" key is missing
                            },
                            "miss_distance": {
                                # "kilometers" key is missing
                            },
                            # "orbiting_body" key is missing
                        }
                    ]
                }
            ]
        }
    }
    test_result_missing_fields_approach = extract_close_approaches(test_data_missing_fields_approach)
    assert len(test_result_missing_fields_approach) == 1
    test_record_missing_fields_approach = test_result_missing_fields_approach[0]
    assert test_record_missing_fields_approach["id"] == "12345"
    assert test_record_missing_fields_approach["name"] == "Test Asteroid"
    assert test_record_missing_fields_approach["absolute_magnitude_h"] == 25.0
    assert test_record_missing_fields_approach["is_potentially_hazardous"] is False
    assert test_record_missing_fields_approach["diameter_min_km"] == 0.05
    assert test_record_missing_fields_approach["diameter_max_km"] == 0.1
    assert test_record_missing_fields_approach["close_approach_date"] == "2025-01-01"
    assert test_record_missing_fields_approach["relative_velocity_kps"] == 0.0  # Defaulted to 0.0
    assert test_record_missing_fields_approach["miss_distance_km"] == 0.0  # Defaulted to 0.0
    assert test_record_missing_fields_approach["orbiting_body"] == "Unknown"  # Defaulted to "Unknown" 


def test_transform_to_dataframe_basic_functionality():
    """
    Test that transform_to_dataframe correctly converts extracted records to DataFrame.
    
    Verifies the function:
    - Returns a proper pandas DataFrame object
    - Creates correct DataFrame structure (shape, columns, column order)
    - Preserves all data integrity during list-to-DataFrame conversion
    - Maintains correct data types for all fields
    
    This validates the core DataFrame conversion logic that transforms
    flattened record dictionaries into structured tabular data suitable
    for analysis and CSV export.
    """
    import pandas as pd

    # Test data for a single asteroid with one close approach event
    test_data = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "12345",
                    "name": "Test Asteroid",
                    "absolute_magnitude_h": 22.1,
                    "is_potentially_hazardous_asteroid": True,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.1,
                            "estimated_diameter_max": 0.3
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                "kilometers_per_second": "5.5"
                            },
                            "miss_distance": {
                                "kilometers": "750000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                }
            ]
        }
    }
    test_dataframe = transform_to_dataframe(test_data)
    assert isinstance(test_dataframe, pd.DataFrame) # Check that the result is a DataFrame
    assert test_dataframe.shape == (1, 10)  # Check for correct shape: 1 record, 10 columns
    assert list(test_dataframe.columns) == [ # Check for correct columns
        "id",
        "name",
        "close_approach_date",
        "absolute_magnitude_h",
        "diameter_min_km",
        "diameter_max_km",
        "is_potentially_hazardous",
        "relative_velocity_kps",
        "miss_distance_km",
        "orbiting_body"
    ]

    test_record = test_dataframe.iloc[0] # Get the first (and only) record and check values
    assert test_record["id"] == "12345"
    assert test_record["name"] == "Test Asteroid"
    assert test_record["close_approach_date"] == "2025-01-01"
    assert test_record["absolute_magnitude_h"] == 22.1
    assert test_record["diameter_min_km"] == 0.1
    assert test_record["diameter_max_km"] == 0.3
    assert test_record["is_potentially_hazardous"] == True
    assert test_record["relative_velocity_kps"] == 5.5
    assert test_record["miss_distance_km"] == 750000.0
    assert test_record["orbiting_body"] == "Earth"


def test_transform_to_dataframe_sorting():
    """
    Test that transform_to_dataframe correctly sorts DataFrame by close_approach_date.
    
    Verifies the function:
    - Sorts DataFrame rows in ascending chronological order (earliest dates first)
    - Maintains data integrity during the sorting process
    - Handles multiple dates across multiple asteroids correctly
    - Produces consistent ordering regardless of input data arrangement
    
    This validates the sorting logic that ensures chronological data presentation
    for time-series analysis and consistent CSV output formatting.
    """
    import pandas as pd

    # Test data with multiple asteroids and approach dates in unsorted order
    test_data = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "11223",
                    "name": "Test Asteroid 3",
                    "absolute_magnitude_h": 25.0,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.05,
                            "estimated_diameter_max": 0.1
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-26",
                            "relative_velocity": {
                                "kilometers_per_second": "3.2"
                            },
                            "miss_distance": {
                                "kilometers": "500000"
                            },
                            "orbiting_body": "Earth"
                        },
                        {
                            "close_approach_date": "2025-02-10",
                            "relative_velocity": {
                                "kilometers_per_second": "3.5"
                            },
                            "miss_distance": {
                                "kilometers": "450000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                },
                {
                    "id": "12345",
                    "name": "Test Asteroid 1",
                    "absolute_magnitude_h": 22.1,
                    "is_potentially_hazardous_asteroid": True,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.1,
                            "estimated_diameter_max": 0.3
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                "kilometers_per_second": "5.5"
                            },
                            "miss_distance": {
                                "kilometers": "750000"
                            },
                            "orbiting_body": "Earth"
                        },
                        {
                            "close_approach_date": "2025-01-21",
                            "relative_velocity": {
                                "kilometers_per_second": "6.0"
                            },
                            "miss_distance": {
                                "kilometers": "800000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                },
                {
                    "id": "67890",
                    "name": "Test Asteroid 2",
                    "absolute_magnitude_h": 19.5,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.5,
                            "estimated_diameter_max": 1.2
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-03",
                            "relative_velocity": {
                                "kilometers_per_second": "12.3"
                            },
                            "miss_distance": {
                                "kilometers": "1500000"
                            },
                            "orbiting_body": "Mars"
                        },
                        {
                            "close_approach_date": "2025-01-15",
                            "relative_velocity": {
                                "kilometers_per_second": "11.8"
                            },
                            "miss_distance": {
                                "kilometers": "1400000"
                            },
                            "orbiting_body": "Mars"
                        }
                    ]
                }
            ]
        }
    }
    test_dataframe = transform_to_dataframe(test_data)
    assert isinstance(test_dataframe, pd.DataFrame)
    assert test_dataframe.shape == (6, 10)  # 6 records, 10 columns
    expected_dates_order = [ # Expected sorted order of approach dates from test data
        "2025-01-01",
        "2025-01-03",
        "2025-01-15",
        "2025-01-21",
        "2025-01-26",
        "2025-02-10"
    ]
    actual_dates_order = test_dataframe["close_approach_date"].tolist() # Extract actual order of approach dates from DataFrame
    assert actual_dates_order == expected_dates_order


def test_transform_to_dataframe_edge_cases():
    """
    Test that transform_to_dataframe handles edge cases and malformed data gracefully.
    
    Verifies the function:
    - Returns proper DataFrame structure (0, 10) for empty input data
    - Maintains consistent column schema even with no records
    - Handles missing asteroid fields by setting None values appropriately
    - Processes missing approach fields using correct default values (0.0, "Unknown")
    - Preserves data integrity for fields that are present
    
    This validates DataFrame conversion resilience to incomplete API responses
    and ensures consistent tabular structure regardless of input data quality.
    """
    import pandas as pd

    # Test Case 1: Empty data
    test_data_empty = {"near_earth_objects": {}}
    test_dataframe_empty = transform_to_dataframe(test_data_empty)
    assert isinstance(test_dataframe_empty, pd.DataFrame)
    assert test_dataframe_empty.empty  # DataFrame should be empty
    assert test_dataframe_empty.shape == (0, 10)  # Should have 0 rows and 10 columns
    assert list(test_dataframe_empty.columns) == [
        "id",
        "name",
        "close_approach_date",
        "absolute_magnitude_h",
        "diameter_min_km",
        "diameter_max_km",
        "is_potentially_hazardous",
        "relative_velocity_kps",
        "miss_distance_km",
        "orbiting_body"
    ]

    # Test Case 2: Data with missing asteroid fields
    test_data_missing_fields_asteroid = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    # "id" key is missing
                    "name": "Test Asteroid",
                    # "absolute_magnitude_h" key is missing
                    # "is_potentially_hazardous_asteroid" key is missing
                    "estimated_diameter": {
                        "kilometers": {
                            # "estimated_diameter_min" key is missing
                            "estimated_diameter_max": 0.1
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                "kilometers_per_second": "5.5"
                            },
                            "miss_distance": {
                                "kilometers": "750000"
                            },
                            "orbiting_body": "Earth"
                        }
                    ]
                }
            ]
        }
    }
    test_dataframe_missing_fields_asteroid = transform_to_dataframe(test_data_missing_fields_asteroid)
    assert isinstance(test_dataframe_missing_fields_asteroid, pd.DataFrame)
    assert test_dataframe_missing_fields_asteroid.shape == (1, 10)  # 1 record, 10 columns
    test_record_missing_fields_asteroid = test_dataframe_missing_fields_asteroid.iloc[0]
    assert test_record_missing_fields_asteroid["id"] == None
    assert test_record_missing_fields_asteroid["name"] == "Test Asteroid"
    assert test_record_missing_fields_asteroid["absolute_magnitude_h"] == None
    assert test_record_missing_fields_asteroid["is_potentially_hazardous"] == None
    assert test_record_missing_fields_asteroid["diameter_min_km"] == None
    assert test_record_missing_fields_asteroid["diameter_max_km"] == 0.1
    assert test_record_missing_fields_asteroid["close_approach_date"] == "2025-01-01"
    assert test_record_missing_fields_asteroid["relative_velocity_kps"] == 5.5
    assert test_record_missing_fields_asteroid["miss_distance_km"] == 750000.0
    assert test_record_missing_fields_asteroid["orbiting_body"] == "Earth"

    # Test Case 3: Data with missing approach fields
    test_data_missing_fields_approach = {
        "near_earth_objects": {
            "2025-01-01": [
                {
                    "id": "12345",
                    "name": "Test Asteroid",
                    "absolute_magnitude_h": 25.0,
                    "is_potentially_hazardous_asteroid": False,
                    "estimated_diameter": {
                        "kilometers": {
                            "estimated_diameter_min": 0.05,
                            "estimated_diameter_max": 0.1
                        }
                    },
                    "close_approach_data": [
                        {
                            "close_approach_date": "2025-01-01",
                            "relative_velocity": {
                                # "kilometers_per_second" key is missing
                            },
                            "miss_distance": {
                                # "kilometers" key is missing
                            },
                            # "orbiting_body" key is missing
                        }
                    ]
                }
            ]
        }
    }
    test_dataframe_missing_fields_approach = transform_to_dataframe(test_data_missing_fields_approach)
    assert isinstance(test_dataframe_missing_fields_approach, pd.DataFrame)
    assert test_dataframe_missing_fields_approach.shape == (1, 10) # 1 record, 10 columns
    test_record_missing_fields_approach = test_dataframe_missing_fields_approach.iloc[0]
    assert test_record_missing_fields_approach["id"] == "12345"
    assert test_record_missing_fields_approach["name"] == "Test Asteroid"
    assert test_record_missing_fields_approach["absolute_magnitude_h"] == 25.0
    assert test_record_missing_fields_approach["is_potentially_hazardous"] == False
    assert test_record_missing_fields_approach["diameter_min_km"] == 0.05
    assert test_record_missing_fields_approach["diameter_max_km"] == 0.1
    assert test_record_missing_fields_approach["close_approach_date"] == "2025-01-01"
    assert test_record_missing_fields_approach["relative_velocity_kps"] == 0.0 # Defaulted to 0.0
    assert test_record_missing_fields_approach["miss_distance_km"] == 0.0 # Defaulted to 0.0
    assert test_record_missing_fields_approach["orbiting_body"] == "Unknown" # Defaulted to "Unknown"


