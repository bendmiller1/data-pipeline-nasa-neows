"""
Load layer for the NASA NeoWs Data Pipeline.

This module persists transformed NeoWs data into a local SQLite database.
It supports creating the database (and table) if missing and inserting
records from a pandas DataFrame or from a CSV produced by the transform step.

It also supports idempotent reloads for a specific date window:
before inserting, it can delete rows in the [start_date, end_date] range
to avoid UNIQUE violations under the composite PK (close_approach_date, id).

Typical usage examples:
    # As a module targeting the default CSV created by transform.py
    python -m src.load

    # Programmatic usage:
    from pathlib import Path
    from src.load import read_csv_to_dataframe, load_dataframe_to_sqlite
    df = read_csv_to_dataframe(Path("data/processed/neows_latest.csv"))
    load_dataframe_to_sqlite(df, delete_range_before_insert=True)
"""
# This module handles loading the transformed NeoWs CSV data into a SQLite database

from __future__ import annotations # Allows the program to use newer type hint syntax in older Python versions

from pathlib import Path # Allows the program to work with file system path objects in a platform-independent way
from typing import Optional # Provides type hinting for optional parameters

import sqlite3 # Provides the interface for interacting with SQLite databases
import pandas as pd # Provides useful "database-like" data structures (Series - one column with rows, DataFrame - multiple columns with rows) and data manipulation functions

from .config import( # Import configuration variables from config.py
    DB_PATH,
    CSV_OUTPUT,
    WAREHOUSE_DIR,
)

# -----------------------------------------------------------------------------
# Default schema: date-first composite PK for efficient date-range queries,
# plus a separate index on id for fast asteroid lookups (for future browse mode).
# -----------------------------------------------------------------------------

# Default SQL schema to create the neows table if it does not exist
# (suitable for the current transform output in both DEMO and LIVE modes)
DEFAULT_SCHEMA_SQL = """ 
CREATE TABLE IF NOT EXISTS neows (
    id TEXT,
    name TEXT,
    close_approach_date TEXT,
    absolute_magnitude_h REAL,
    diameter_min_km REAL,
    diameter_max_km REAL,
    is_potentially_hazardous INTEGER,
    relative_velocity_kps REAL,
    miss_distance_km REAL,
    orbiting_body TEXT,
    PRIMARY KEY (close_approach_date, id)
);

CREATE INDEX IF NOT EXISTS idx_neows_date ON neows (id);
"""


def ensure_database_ready( # Function to ensure the SQLite database and neows table exist (creates them if not)
        database_path: Path = DB_PATH, # Path to the SQLite database file (defaults to the configured DB_PATH)
        schema_sql_path: Optional[Path] = None, # Optional path to a .sql file containing DDL statements (if None, uses the DEFAULT_SCHEMA_SQL)
) -> None:
    """
    Create the SQLite database (and neows table) if they do not exist.

    If a schema file path is provided (e.g., sql/schema.sql), it will be used.
    Otherwise, a minimal default schema suitable for the current transform
    output will be applied.

    Args:
        database_path (Path): Target SQLite database file path.
        schema_sql_path (Optional[Path]): Optional path to a .sql file containing
            DDL statements.

    Raises:
        sqlite3.Error: If the database or schema creation fails.
    """
    database_path.parent.mkdir(parents=True, exist_ok=True) # Ensures that the parent directory for the database file exists (creates it if not)

    if schema_sql_path and schema_sql_path.exists(): # If a schema file path is provided and the file exists
        ddl_sql = schema_sql_path.read_text(encoding="utf-8") # Reads the SQL DDL statements from the file
    else:
        ddl_sql = DEFAULT_SCHEMA_SQL # Else, uses the default schema defined in this module
    
    connection = sqlite3.connect(database_path) # Opens a connection to the SQLite database (creates the file if it does not exist)
    try:
        connection.executescript(ddl_sql) # Executes the DDL SQL script to create the neows table and any indexes (either from file or default)
        connection.commit() # Commits the changes to the database
    finally:
        connection.close() # ensures the database connection is closed


def read_csv_to_dataframe(csv_path: Path = CSV_OUTPUT,) -> pd.DataFrame: # Function to read the transformed CSV into a pandas DataFrame for loading into SQLite
    """
    Load a CSV (produced by transform.py) into a pandas DataFrame.

    Args:
        csv_path (Path): Path to the CSV file to read. Defaults to the
            configured CSV_OUTPUT.

    Returns:
        pd.DataFrame: DataFrame containing NeoWs records.

    Raises:
        FileNotFoundError: If the provided CSV path does not exist.
        ValueError: If the CSV is empty or cannot be parsed into a DataFrame.
    """
    if not csv_path.exists(): 
        raise FileNotFoundError(f"CSV file not found: {csv_path}") # Raises an error if the specified CSV file does not exist

    dataframe = pd.read_csv(csv_path) # Reads the CSV file into a pandas DataFrame
    if dataframe.empty:
        raise ValueError(f"CSV file is empty or could not be parsed: {csv_path}") # Raises an error if the DataFrame is empty (no data)
    
    return dataframe # Returns the populated DataFrame


def delete_date_range( # Function to delete rows in the requested date range from the NEoWs table (to enable idempotent reloads)
        database_path: Path, # Path to the SQLite file
        table_name: str, # Name of the table to delete the range from (e.g., "neows")
        start_date: str, # Start date of the range to delete (inclusive, in "YYYY-MM-DD" format)
        end_date: str, # End date of the range to delete (inclusive, in "YYYY-MM-DD" format)
) -> int:
    """
    Delete rows in [start_date, end_date] (inclusive) from the target table 
    (Enables reruns of the same range of dates).

    Args:
        database_path (Path): SQLite DB path.
        table_name (str): Table to delete from (e.g., "neows").
        start_date (str): "YYYY-MM-DD".
        end_date (str): "YYYY-MM-DD".

    Returns:
        int: Number of rows deleted.
    """
    with sqlite3.connect(database_path) as connection: # Opens a connection to the SQLite database (ensures it is closed after the block)
        cursor = connection.cursor() # Creates a cursor object to execute SQL commands 
        cursor.execute( # Executes a DELETE SQL command to remove rows in the specified date range
            f"""
            DELETE FROM {table_name}
            WHERE close_approach_date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )
        deleted_rows = cursor.rowcount if cursor.rowcount is not None else 0 # Gets the count of rows deleted (if rowcount is None, defaults to 0)
        connection.commit() # Commits the changes to the database
        return deleted_rows # Returns the count of rows that were deleted
    

def load_dataframe_to_sqlite( # Main function to load a pandas DataFrame into the SQLite database (with optional pre-delete for idempotency)
        dataframe: pd.DataFrame,
        database_path: Path = DB_PATH,
        table_name: str = "neows",
        if_exists: str = "append",
        chunk_size: Optional[int] = None,
        delete_range_before_insert: bool = True,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> int:
    """
    Insert a DataFrame into a SQLite table, creating the DB if needed.

    Optionally deletes an existing date window to keep re-runs idempotent
    under the composite PK (close_approach_date, id).

    Args:
        dataframe (pd.DataFrame): Transformed NeoWs records to persist.
        database_path (Path): Path to the SQLite database file.
        table_name (str): Destination table name. Defaults to "neows".
        if_exists (str): Behavior if the table exists. One of {"fail","replace","append"}.
            Defaults to "append".
        chunk_size (Optional[int]): Optional number of rows per batch insert.
        delete_range_before_insert (bool): If True, delete rows in the target
            date window before inserting. Defaults to True.
        start_date (Optional[str]): Start of date window ("YYYY-MM-DD"). If None,
            inferred from the DataFrame.
        end_date (Optional[str]): End of date window ("YYYY-MM-DD"). If None,
            inferred from the DataFrame.

    Returns:
        int: Number of rows written.

    Raises:
        sqlite3.Error: If insertion fails.
        ValueError: If the DataFrame is empty or required columns are missing.
    """
    if dataframe is None or dataframe.empty:
        raise ValueError("No data to load: the provided DataFrame is empty.") # Immediately checks if the DataFrame is empty and raises a ValueError if so

    if delete_range_before_insert and ("close_approach_date" not in dataframe.columns): # Ensures the DataFrame has the required column if pre-delete is requested and raises an ValueError if not
        raise ValueError(
            "DataFrame must contain 'close_approach_date' column to delete date range." 
        )
    
    #Infer date range from DataFrame if not provided
    if delete_range_before_insert and (start_date is None or end_date is None): # If pre-delete is requested but start_date or end_date is not provided, infers them from the DataFrame (for testing the module by itself)
        start_date = str(dataframe["close_approach_date"].min()) # Infers the start date from the minimum close_approach_date in the DataFrame
        end_date = str(dataframe["close_approach_date"].max()) # Infers the end date from the maximum close_approach_date in the DataFrame

    ensure_database_ready(database_path) # Calls ensure_database_ready to create the database and neows table if they do not exist

    # Optional pre-delete to keep re-runs idempotent
    if delete_range_before_insert and start_date and end_date: # if pre-delete is requested and both start_date and end_date are provided (either by the user or inferred from the DataFrame)
        deleted_rows = delete_date_range(database_path, table_name, start_date, end_date) # Calls delete_date_range to remove existing rows in the specified date range
        print(f"[load] Pre-delete: removed {deleted_rows} rows in [{start_date} .. {end_date}]") # Prints a message indicating how many rows were deleted in the specified date range

    with sqlite3.connect(database_path) as connection: # Opens a connection to the SQLite database ('with' ensures it is closed after the block)
        dataframe.to_sql( 
            name = table_name, # Name of the target table (the table that will receive and store the data)
            con = connection, # The active SQLite database connection
            if_exists = if_exists, # Specifies behaviour if the table already exists (defaults to "append" to add new rows)
            index = False, # Do not include the DataFrame index column as a database column
            chunksize = chunk_size, # Optional number of rows to insert per batch (if None, inserts all at once)
            method = None, # Use default executemany
        )
        connection.commit() # Commits the changes to the database

    return int(len(dataframe)) # Returns the number of rows written to the database (the length of the DataFrame)

# Verifies functionality when running this file directly
if __name__ == "__main__":
    """
    Script entry point for manual testing.

    Reads the default CSV produced by transform.py (CSV_OUTPUT),
    ensures the SQLite database exists, deletes the CSV's date window,
    and appends rows to the "neows" table. Prints a confirmation with
    row count and target DB path.
    """
    try:
        print(f"[load] Reading CSV from: {CSV_OUTPUT}")
        records_dataframe = read_csv_to_dataframe(CSV_OUTPUT)

        min_date = str(records_dataframe["close_approach_date"].min())
        max_date = str(records_dataframe["close_approach_date"].max())

        print(f"[load] Ensuring database at: {DB_PATH}")
        written_rows = load_dataframe_to_sqlite(
            dataframe = records_dataframe,
            database_path = DB_PATH,
            table_name = "neows",
            if_exists = "append",
            delete_range_before_insert = True,
            start_date = min_date,
            end_date = max_date,
        )

        print(f"[load] Wrote {written_rows} rows to SQLite database at: {DB_PATH}")
        print(f"[load] Warehouse directory: {WAREHOUSE_DIR}")

    except Exception as e:
        print(f"[load] [ERROR] {type(e).__name__}: {e}")
        raise