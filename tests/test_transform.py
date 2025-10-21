"""
Unit tests for transform.py
"""

from os import read
from numpy import save
import pytest
import test
import tempfile
from pathlib import Path
import pandas as pd
from src.transform import extract_close_approaches, transform_to_dataframe, save_dataframe_to_csv


class TestExtractCloseApproaches:
    """
    Unit tests for the extract_close_approaches function.
    
    Tests the core ETL transformation logic that flattens nested JSON
    structure into tabular records suitable for CSV/database storage.
    """

    def test_single_asteroid_single_event(self):
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

    def test_multiple_asteroids_multiple_events(self):
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

    def test_empty_data(self):
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

    def test_incomplete_data(self):
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


class TestTransformToDataframe:
    """
    Unit tests for the transform_to_dataframe function.
    
    Tests DataFrame conversion logic that transforms flattened record
    dictionaries into structured tabular data suitable for analysis and CSV export.
    """

    def test_basic_functionality(self):
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

    def test_sorting(self):
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

    def test_edge_cases(self):
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


class TestSaveDataframeToCSV:

    def setup_method(self):
        """
        Set up temporary directory and test DataFrame before each test method.
       Creates:
       - Temporary directory for isolated file I/O testing
       - Test CSV file path within the temporary directory
       - Sample DataFrame with realistic asteroid data structure

       This ensures each test has a clean environment and consistent test data
       while preventing interference between test methods and avoiding file system pollution.
        """
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.test_dir.name) / "test_output.csv"
        self.test_data = {
            "id": ["12345", "67890", "11223", "44556"],
            "name": ["Asteroid A", "Asteroid B", "Asteroid C", "Asteroid D"],
            "close_approach_date": ["2025-01-01", "2025-01-05", "2025-01-10", "2025-01-15"],
            "absolute_magnitude_h": [22.1, 19.5, 25.0, 21.3],
            "diameter_min_km": [0.1, 0.5, 0.05, 0.2],
            "diameter_max_km": [0.3, 1.2, 0.1, 0.4],
            "is_potentially_hazardous": [True, True, False, False],
            "relative_velocity_kps": [5.5, 12.3, 3.2, 7.8],
            "miss_distance_km": [600000.0, 453000.0, 800000.0, 740000.0],
            "orbiting_body": ["Earth", "Earth", "Mars", "Venus"]
        }
        self.test_dataframe = pd.DataFrame(self.test_data)

    def teardown_method(self):
        """
        Clean up temporary directory and files after each test method.
        Automatically removes:
        - All temporary files created during test execution
        - Temporary directory structure and contents
        - Any residual file system artifacts from testing

        This ensures complete test isolation and prevents accumulation of test files
        that could interfere with subsequent test runs or consume system resources.
        """
        self.test_dir.cleanup()

    def test_basic_functionality(self):
        """
        Test that save_dataframe_to_csv correctly writes DataFrame to CSV file.
    
        Verifies the function:
        - Creates CSV file at the specified path location
        - Ensures parent directories exist before writing
        - Writes DataFrame content without index column
        - Preserves data integrity during CSV serialization process
    
        This validates the core file I/O functionality that enables data export
        for analysis tools and external consumption of processed asteroid data.
        """
        save_dataframe_to_csv(self.test_dataframe, self.test_path)

        assert self.test_path.exists()
        assert self.test_path.parent.exists()
        assert self.test_path.is_file()

        read_back_df = pd.read_csv(self.test_path, dtype={"id": "object"})
        pd.testing.assert_frame_equal(self.test_dataframe, read_back_df)

    def test_directory_creation(self):
        """
        Test that save_dataframe_to_csv creates parent directories when they don't exist.
    
        Verifies the function:
        - Creates parent directory structure using pathlib.Path.mkdir(parents=True)
        - Handles nested directory paths that don't already exist
        - Successfully writes file after creating required parent directories
        - Only creates necessary parent directories (not arbitrary deep structures)
    
        This ensures the function can handle output paths in non-existent directories
        without requiring manual directory creation by calling code.
        """
        nested_path = self.test_path.parent / "subdir" / "output.csv"
        assert not nested_path.parent.exists()

        save_dataframe_to_csv(self.test_dataframe, nested_path)

        assert nested_path.parent.exists()
        assert nested_path.exists()
        assert nested_path.is_file()

        read_back_df = pd.read_csv(nested_path, dtype={"id": "object"})
        pd.testing.assert_frame_equal(self.test_dataframe, read_back_df)

    
    def test_edge_cases(self):
        """
        Test edge cases for save_dataframe_to_csv function.
    
        Verifies the function:
        - Handles empty DataFrames gracefully without raising exceptions
        - Creates CSV files with correct headers even when no data is present
        - Overwrites existing files correctly when called multiple times
        - Handles DataFrames with unusual data types (None, NaN values)
        - Maintains consistent file structure for downstream processing tools
        - Provides predictable behavior for edge conditions in the ETL pipeline

        This ensures the function is robust across various real-world scenarios
        that could occur during ETL processing.
        """

        # Test Case 1: Empty DataFrame with correct column structure
        empty_columns = [
            "id", "name", "close_approach_date", "absolute_magnitude_h",
            "diameter_min_km", "diameter_max_km", "is_potentially_hazardous",
            "relative_velocity_kps", "miss_distance_km", "orbiting_body"
        ]
        empty_dataframe = pd.DataFrame(columns=empty_columns)
        empty_csv_path = self.test_path.parent / "empty_output.csv"

        save_dataframe_to_csv(empty_dataframe, empty_csv_path)

        assert empty_csv_path.exists()
        assert empty_csv_path.is_file()

        read_back_empty_df = pd.read_csv(empty_csv_path, dtype={"id": "object"})
        assert list(read_back_empty_df.columns) == empty_columns
        assert len(read_back_empty_df) == 0
        assert read_back_empty_df.empty


        # Test Case 2: Overwriting existing file
        overwrite_path = self.test_path.parent / "overwrite_output.csv"

        # First call to create the file
        save_dataframe_to_csv(self.test_dataframe, overwrite_path)
        first_size = overwrite_path.stat().st_size

        # Second call to overwrite the file
        overwrite_data = {
            "id": ["1", "2", "3"],
            "name": ["Asteroid A", "Asteroid B", "Asteroid C"],
            "close_approach_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "absolute_magnitude_h": [22.5, 23.1, 21.8],
            "diameter_min_km": [0.5, 0.6, 0.4],
            "diameter_max_km": [1.0, 1.2, 0.8],
            "is_potentially_hazardous": [False, True, False],
            "relative_velocity_kps": [12.3, 15.4, 10.1],
            "miss_distance_km": [100000.0, 200000.0, 150000.0],
            "orbiting_body": ["Earth", "Mars", "Venus"]
        }
        overwrite_dataframe = pd.DataFrame(overwrite_data)
        save_dataframe_to_csv(overwrite_dataframe, overwrite_path)

        second_size = overwrite_path.stat().st_size
        assert second_size != first_size  # File size should change after overwrite

        # Verify content matches the new DataFrame
        read_back_overwrite_df = pd.read_csv(overwrite_path, dtype={"id": "object"})
        pd.testing.assert_frame_equal(overwrite_dataframe, read_back_overwrite_df)


        # Test Case 3: DataFrame with None/NaN values
        import numpy as np
        mixed_missing_data = {
            "id": ["12345", None, "67890", "11223"],
            "name": ["Asteroid A", "Asteroid B", None, "Asteroid D"],
            "close_approach_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"],
            "absolute_magnitude_h": [22.1, None, np.nan, 25.0],
            "diameter_min_km": [0.1, 0.2, None, np.nan],
            "diameter_max_km": [0.3, np.nan, 0.5, None],
            "is_potentially_hazardous": [True, False, None, True],
            "relative_velocity_kps": [5.5, None, np.nan, 7.8],
            "miss_distance_km": [600000.0, np.nan, None, 800000.0],
            "orbiting_body": ["Earth", None, "Earth", "Mars"]
        }
        mixed_missing_dataframe = pd.DataFrame(mixed_missing_data)
        mixed_missing_csv_path = self.test_path.parent / "mixed_missing_output.csv"

        # Should handle both None and NaN values gracefully
        save_dataframe_to_csv(mixed_missing_dataframe, mixed_missing_csv_path)

        assert mixed_missing_csv_path.exists()
        assert mixed_missing_csv_path.is_file()

        # Verify data structure is maintained (None and NaN become empty strings or handled by pandas)
        read_back_mixed_df = pd.read_csv(mixed_missing_csv_path, dtype={"id": "object"})
        assert len(read_back_mixed_df) == 4 # Should have 4 records
        assert read_back_mixed_df.shape[1] == 10 # Should have 10 columns

        # Verify that the file can be read back successfully (main requirement)
        assert list(read_back_mixed_df.columns) == list(mixed_missing_dataframe.columns)