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

from __future__ import annotations

from pathlib import Path
from typing import Optional

import sqlite3
import pandas as pd

from .config import(
    DB_PATH,
    CSV_OUTPUT,
    WAREHOUSE_DIR,
)

# -----------------------------------------------------------------------------
# Default schema: date-first composite PK for efficient date-range queries,
# plus a separate index on id for fast asteroid lookups.
# -----------------------------------------------------------------------------

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

CREATE INDEX IF NOT EXISTS idx_neows_date ON neows (close_approach_date);
"""


def ensure_database_ready(
        database_path: Path = DB_PATH,
        schema_sql_path: Optional[Path] = None,
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
    database_path.parent.mkdir(parents=True, exist_ok=True)

    if schema_sql_path and schema_sql_path.exists():
        ddl_sql = schema_sql_path.read_text(encoding="utf-8")
    else:
        ddl_sql = DEFAULT_SCHEMA_SQL
    
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(ddl_sql)
        connection.commit()
    finally:
        connection.close()


def read_csv_to_dataframe(csv_path: Path = CSV_OUTPUT,) -> pd.DataFrame:
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
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    dataframe = pd.read_csv(csv_path)
    if dataframe.empty:
        raise ValueError(f"CSV file is empty or could not be parsed: {csv_path}")
    
    return dataframe


def delete_date_range(
        database_path: Path,
        table_name: str,
        start_date: str,
        end_date: str,
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
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            DELETE FROM {table_name}
            WHERE close_approach_date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )
        deleted_rows = cursor.rowcount if cursor.rowcount is not None else 0
        connection.commit()
        return deleted_rows
    

def load_dataframe_to_sqlite(
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
        raise ValueError("No data to load: the provided DataFrame is empty.")
    
    if delete_range_before_insert and ("close_approach_date" not in dataframe.columns):
        raise ValueError(
            "DataFrame must contain 'close_approach_date' column to delete date range."
        )
    
    #Infer date range from DataFrame if not provided
    if delete_range_before_insert and (start_date is None or end_date is None):
        start_date = str(dataframe["close_approach_date"].min())
        end_date = str(dataframe["close_approach_date"].max())

    ensure_database_ready(database_path)

    # Optional pre-delete to keep re-runs idempotent
    if delete_range_before_insert and start_date and end_date:
        deleted_rows = delete_date_range(database_path, table_name, start_date, end_date)
        print(f"[load] Pre-delete: removed {deleted_rows} rows in [{start_date} .. {end_date}]")

    with sqlite3.connect(database_path) as connection:
        dataframe.to_sql(
            name = table_name,
            con = connection,
            if_exists = if_exists,
            index = False,
            chunksize = chunk_size,
            method = None, # Use default executemany
        )
        connection.commit()

    return int(len(dataframe))


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