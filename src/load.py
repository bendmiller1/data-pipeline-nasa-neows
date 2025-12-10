"""
Load layer for the NASA NeoWs Data Pipeline.

This module provides database persistence for transformed NeoWs data with support
for both SQLite and PostgreSQL backends. It includes database-agnostic connection
management, schema handling, and data loading operations with automatic PostgreSQL
data type optimization.

Key Features:
- Dual database support: SQLite (development/demo) and PostgreSQL (production)
- Database-agnostic operations through SQLAlchemy
- Automatic PostgreSQL data type optimization for enhanced performance
- Idempotent data loading with date range pre-deletion
- Connection pooling for PostgreSQL
- Automatic schema creation and management
- CSV to database pipeline integration

Database Support:
- SQLite: File-based storage for development, testing, and demos
- PostgreSQL: Production-ready with connection pooling and type optimization

The module supports creating databases and tables if missing, inserting records
from pandas DataFrames or CSV files, and idempotent reloads for specific date
windows to avoid UNIQUE violations under the composite PK (close_approach_date, id).

Typical usage examples:
    # As a module targeting the default CSV created by transform.py
    python -m src.load

    # Programmatic usage with database-agnostic approach:
    from pathlib import Path
    from src.load import read_csv_to_dataframe, load_dataframe_to_database
    df = read_csv_to_dataframe(Path("data/processed/neows_latest.csv"))
    # Uses configured DATABASE_URL (SQLite or PostgreSQL)
    load_dataframe_to_database(df, delete_range_before_insert=True)

    # Modern usage with DatabaseManager (dual database support):
    from src.config import DATABASE_URL
    from src.load import DatabaseManager
    db = DatabaseManager(DATABASE_URL)
    schema_sql = db.get_schema_sql()
    db.execute_sql(schema_sql)
"""
# This module handles loading the transformed NeoWs CSV data into both SQLite and PostgreSQL databases

from __future__ import annotations # Allows the program to use newer type hint syntax in older Python versions

from pathlib import Path # Allows the program to work with file system path objects in a platform-independent way
from typing import Optional, Literal, Dict, Any # Provides type hinting for optional parameters and literal types
from sqlalchemy import create_engine, text, event # Provides the create_engine function to establish database connections and text for executing raw SQL
from sqlalchemy.engine import Engine # Provides the Engine type for type hinting database engine objects
from sqlalchemy.pool import QueuePool # Import QueuePool for explicit pool configuration
import time # For timing connection operations
import logging # Provides logging capabilities for tracking events that happen when some software runs
import pandas as pd # Provides useful "database-like" data structures (Series - one column with rows, DataFrame - multiple columns with rows) and data manipulation functions

from .config import( # Import configuration variables from config.py
    DB_PATH,
    CSV_OUTPUT,
    WAREHOUSE_DIR,
    DATABASE_URL,
    # PostgreSQL connection pool configuration
    POSTGRES_POOL_SIZE,
    POSTGRES_MAX_OVERFLOW,
    POSTGRES_POOL_TIMEOUT,
    POSTGRES_POOL_RECYCLE,
    POSTGRES_POOL_PRE_PING
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

CREATE INDEX IF NOT EXISTS idx_neows_id ON neows (id);
"""

# -----------------------------------------------------------------------------
# PostgreSQL schema: proper data types and basic indexing for production use.
# Uses DATE/BOOLEAN types for better type safety, VARCHAR constraints for data
# validation, and essential indexes for common query patterns (date ranges,
# asteroid lookups, hazard filtering, size sorting).
# -----------------------------------------------------------------------------

# PostgreSQL schema with proper data types and basic indexing
# (suitable for production deployments with moderate query loads)
POSTGRES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS neows (
    id VARCHAR(50),
    name VARCHAR(200),
    close_approach_date DATE,
    absolute_magnitude_h REAL,
    diameter_min_km REAL,
    diameter_max_km REAL,
    is_potentially_hazardous BOOLEAN,
    relative_velocity_kps REAL,
    miss_distance_km REAL,
    orbiting_body VARCHAR(50),
    PRIMARY KEY (close_approach_date, id)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_neows_id ON neows (id);
CREATE INDEX IF NOT EXISTS idx_neows_date ON neows (close_approach_date);
CREATE INDEX IF NOT EXISTS idx_neows_hazardous ON neows (is_potentially_hazardous);
CREATE INDEX IF NOT EXISTS idx_neows_size ON neows (diameter_max_km);
"""

class DatabaseManager:
    """
    Database connection manager supporting both SQLite and PostgreSQL backends.

    This class provides a unified interface for database operations across different
    database types. It automatically configures engine settings based on the database
    URL and provides methods for connection management, query execution, and schema
    handling.

    Key features:
    - Automatic database type detection from connection URL
    - PostgreSQL: Connection pooling and connection recycling
    - SQLite: Thread-safe configuration for concurrent access
    - Parameterized query execution with SQL injection protection
    - Database-specific schema selection

    Typical usage:
        # SQLite (development/demo)
        db = DatabaseManager("sqlite:///data/warehouse/neows_data.db")
        
        # PostgreSQL (production)
        db = DatabaseManager("postgresql://user:pass@localhost:5432/nasa_neows")
        
        # Execute queries
        result = db.execute_sql("SELECT COUNT(*) FROM neows")
        
        # Get appropriate schema
        schema_sql = db.get_schema_sql()

    Attributes:
        database_url (str): Original database connection string
        is_postgres (bool): True if using PostgreSQL, False if SQLite
        engine (sqlalchemy.engine.Engine): SQLAlchemy database engine
    """

    def __init__(self, database_url: str):
        """
        Initialize database manager with appropriate engine configuration.

        Creates a SQLAlchemy engine with database-specific settings:
        - PostgreSQL: Connection pooling with ping validation and recycling
        - SQLite: Thread safety configuration

        Args:
            database_url (str): Database connection string. Format:
                - SQLite: "sqlite:///path/to/database.db"
                - PostgreSQL: "postgresql://user:password@host:port/database"

        Raises:
            sqlalchemy.exc.ArgumentError: If the database URL is invalid.
            sqlalchemy.exc.NoSuchModuleError: If required database driver is missing.
        """
        self.database_url = database_url
        self.is_postgres = database_url.startswith("postgresql")
        self._connection_stats = {
            "connections_created": 0,
            "connections_closed": 0,
            "pool_timeouts": 0,
            "total_queries": 0
        }

        if self.is_postgres:
            # Advanced PostgreSQL connection pool configuration
            self.engine = create_engine(
                database_url,
                # Core pool settings
                poolclass=QueuePool,
                pool_size=POSTGRES_POOL_SIZE,
                max_overflow=POSTGRES_MAX_OVERFLOW,
                pool_timeout=POSTGRES_POOL_TIMEOUT,
                pool_recycle=POSTGRES_POOL_RECYCLE,
                pool_pre_ping=POSTGRES_POOL_PRE_PING,
                
                # Advanced pool behavior
                pool_reset_on_return="commit",
                pool_use_lifo=True,  # Use LIFO for better cache locality
                
                # Connection settings
                connect_args={
                    "connect_timeout": 10,  # Connection timeout in seconds
                    "options": "-c statement_timeout=60000"  # Query timeout in milliseconds
                },
                
                # Engine behavior settings
                echo=False,  # Set to True for SQL query logging in development
                future=True,  # Use SQLAlchemy 2.0 style
            )
            
            # Set up connection pool event listeners for monitoring
            self._setup_pool_listeners()
        else:
            self.engine = create_engine(
                database_url,
                connect_args={"check_same_thread": False},
            )

    def _setup_pool_listeners(self):
        """Set up SQLAlchemy event listeners for connection pool monitoring."""
        if not self.is_postgres:
            return
            
        @event.listens_for(self.engine, "connect")
        def on_connect(dbapi_conn, connection_record):
            """Track new connections created."""
            self._connection_stats["connections_created"] += 1
            
        @event.listens_for(self.engine, "close")
        def on_close(dbapi_conn, connection_record):
            """Track connections closed."""
            self._connection_stats["connections_closed"] += 1
            
        @event.listens_for(self.engine, "invalidate")
        def on_invalidate(dbapi_conn, connection_record, exception):
            """Log connection invalidations for debugging."""
            logging.warning(f"[load] Connection invalidated: {exception}")

    def get_pool_status(self) -> dict:
        """
        Get current connection pool status and statistics.
        
        Returns:
            dict: Pool statistics and configuration information.
        """
        if not self.is_postgres:
            return {"pool_type": "sqlite", "status": "N/A - SQLite doesn't use pooling"}
            
        return {
            "pool_type": "postgresql",
            "engine_url": str(self.engine.url).replace(self.engine.url.password or "", "***"),
            "connection_stats": self._connection_stats.copy(),
            "pool_config": {
                "base_pool_size": POSTGRES_POOL_SIZE,
                "max_overflow": POSTGRES_MAX_OVERFLOW,
                "timeout": POSTGRES_POOL_TIMEOUT,
                "recycle_time": POSTGRES_POOL_RECYCLE,
                "pre_ping": POSTGRES_POOL_PRE_PING
            }
        }

    def warm_up_pool(self, num_connections: Optional[int] = None) -> None:
        """
        Pre-populate the connection pool to avoid cold start delays.
        
        Args:
            num_connections (int): Number of connections to create. Defaults to pool_size.
        """
        if not self.is_postgres:
            print("[load] Pool warm-up skipped: SQLite doesn't use connection pooling")
            return
            
        if num_connections is None:
            num_connections = POSTGRES_POOL_SIZE
            
        print(f"[load] Warming up connection pool with {num_connections} connections...")
        connections = []
        
        try:
            # Create connections to warm up the pool
            for i in range(min(num_connections, POSTGRES_POOL_SIZE)):
                conn = self.get_connection()
                # Test the connection with a simple query
                conn.execute(text("SELECT 1"))
                connections.append(conn)
                
            print(f"[load] Pool warmed up with {len(connections)} connections")
            
        except Exception as e:
            print(f"[load] Warning: Pool warmup failed: {e}")
            
        finally:
            # Return all warmup connections to the pool
            for conn in connections:
                try:
                    conn.close()
                except:
                    pass

    def test_connection_health(self, max_retries: int = 3, timeout_seconds: float = 5.0) -> Dict[str, Any]:
        """
        Test the health of database connections with timing and retry logic.
        
        Args:
            max_retries: Maximum number of retry attempts for failed connections
            timeout_seconds: Maximum time to wait for each connection test
            
        Returns:
            Dictionary containing health test results and performance metrics
        """
        import time
        from sqlalchemy import text
        
        health_results = {
            'healthy': False,
            'total_time': 0.0,
            'connection_time': 0.0,
            'query_time': 0.0,
            'retries_used': 0,
            'pool_status': {},
            'errors': [],
            'test_timestamp': time.time()
        }
        
        start_time = time.time()
        
        for attempt in range(max_retries + 1):
            try:
                print(f"[load] Testing connection health (attempt {attempt + 1}/{max_retries + 1})...")
                
                # Test connection acquisition
                conn_start = time.time()
                with self.get_connection() as conn:
                    conn_time = time.time() - conn_start
                    health_results['connection_time'] = conn_time
                    
                    # Test simple query execution
                    query_start = time.time()
                    if self.is_postgres:
                        result = conn.execute(text("SELECT 1 as health_check, current_timestamp, version()"))
                    else:
                        result = conn.execute(text("SELECT 1 as health_check"))
                    
                    row = result.fetchone()
                    query_time = time.time() - query_start
                    health_results['query_time'] = query_time
                    
                    # Verify query result
                    if row and row[0] == 1:
                        health_results['healthy'] = True
                        health_results['retries_used'] = attempt
                        print(f"[load] Connection health test passed (conn: {conn_time:.3f}s, query: {query_time:.3f}s)")
                        break
                    else:
                        raise Exception("Health check query returned unexpected result")
                        
            except Exception as e:
                error_msg = f"Connection health test failed on attempt {attempt + 1}: {str(e)}"
                print(f"[load] Warning: {error_msg}")
                health_results['errors'].append(error_msg)
                health_results['retries_used'] = attempt + 1
                
                if attempt < max_retries:
                    # Wait briefly before retry
                    time.sleep(0.5)
                else:
                    print(f"[load] Error: Connection health test failed after {max_retries + 1} attempts")
        
        # Calculate total test time
        health_results['total_time'] = time.time() - start_time
        
        # Get current pool status
        try:
            health_results['pool_status'] = self.get_pool_status()
        except Exception as e:
            health_results['errors'].append(f"Could not get pool status: {str(e)}")
        
        return health_results

    def get_connection(self):
        """
        Get a database connection from the engine pool.

        Returns:
            sqlalchemy.engine.Connection: Database connection object that should
                be used with context manager (with statement) for automatic cleanup.

        Raises:
            sqlalchemy.exc.OperationalError: If connection to database fails.
        """
        return self.engine.connect()
    
    def execute_sql(self, sql_query: str, parameters: Optional[dict] = None):
        """Execute SQL query(s) - handles multiple statements for schema creation."""
        with self.get_connection() as conn:
            result = None
            
            # Split multiple statements and execute separately  
            if ";" in sql_query and parameters is None:  # Schema creation case
                statements = [stmt.strip() for stmt in sql_query.split(';') if stmt.strip()]
                for statement in statements:
                    result = conn.execute(text(statement))
                # If no statements were executed, create a dummy result
                if result is None:
                    result = conn.execute(text("SELECT 1"))
            else:
                # Single parameterized query
                result = conn.execute(text(sql_query), parameters or {})
            
            conn.commit()
            return result

    def get_schema_sql(self) -> str:
        """
        Get the appropriate CREATE TABLE schema for the current database type.

        Returns database-specific DDL statements with proper data types:
        - PostgreSQL: Uses DATE, BOOLEAN, VARCHAR types with length constraints
        - SQLite: Uses TEXT, INTEGER, REAL types with simpler syntax

        Returns:
            str: SQL DDL statements to create the neows table and indexes.

        Raises:
            NameError: If POSTGRES_SCHEMA_SQL is not defined when using PostgreSQL.
        """
        if self.is_postgres:
            return POSTGRES_SCHEMA_SQL
        else:
            return DEFAULT_SCHEMA_SQL

    

def ensure_database_ready( # Function to ensure the database and neows table exist (creates them if not)
        database_url: Optional[str] = None, # Optional database connection string (if None, uses the default DATABASE_URL from config.py)
        schema_sql_path: Optional[Path] = None, # Optional path to a .sql file containing DDL statements (if None, uses the DEFAULT_SCHEMA_SQL)
) -> None:
    """
    Create the database and neows table if they do not exist.
    
    Works with both SQLite and PostgreSQL backends. For SQLite, ensures
    the directory structure exists. For both databases, applies the
    appropriate schema.

    Args:
        database_url (Optional[str]): Database connection string. If None,
            uses the configured DATABASE_URL.
        schema_sql_path (Optional[Path]): Optional path to a .sql file containing
            DDL statements. If provided, overrides the default schema.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If database or schema creation fails.
    """
    # Use configured DATABASE_URL if none provided
    if database_url is None:
        database_url = DATABASE_URL
    
    # Create DatabaseManager instance
    db = DatabaseManager(database_url)

    # For SQLite, ensure the directory structure exists
    if not db.is_postgres and database_url.startswith("sqlite:///"):
        # Extract file path from SQLite URL (removes "sqlite:///" prefix)
        db_file_path = Path(database_url[10:])
        db_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Get appropriate schema SQL
    if schema_sql_path and schema_sql_path.exists():
        ddl_sql = schema_sql_path.read_text(encoding="utf-8")
    else:
        ddl_sql = db.get_schema_sql()

    # Execute schema creation
    db.execute_sql(ddl_sql)
   

def read_csv_to_dataframe(csv_path: Path = CSV_OUTPUT,) -> pd.DataFrame: # Function to read the transformed CSV into a pandas DataFrame for database loading
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
        start_date: str, # Start date of the range to delete (inclusive, in "YYYY-MM-DD" format)
        end_date: str, # End date of the range to delete (inclusive, in "YYYY-MM-DD" format)
        database_url: Optional[str] = None, # Optional database connection string (if None, uses the default DATABASE_URL from config.py)
        table_name: str = "neows", # Name of the table to delete the range from (e.g., "neows")
) -> int:
    """
    Delete rows in [start_date, end_date] (inclusive) from the target table.
    
    Works with both SQLite and PostgreSQL backends. Enables reruns of the same
    range of dates by clearing existing data before new inserts.

    Args:
        start_date (str): Start date in "YYYY-MM-DD" format (inclusive).
        end_date (str): End date in "YYYY-MM-DD" format (inclusive).
        database_url (Optional[str]): Database connection string. If None,
            uses the configured DATABASE_URL.
        table_name (str): Table to delete from. Defaults to "neows".

    Returns:
        int: Number of rows deleted.
        
    Raises:
        sqlalchemy.exc.SQLAlchemyError: If the delete operation fails.
    """
    # Use configured DATABASE_URL if none provided
    if database_url is None:
        database_url = DATABASE_URL

    # Create DatabaseManager instance
    db = DatabaseManager(database_url)

    # Execute DELETE with parameterized query (safe from SQL injection)
    result = db.execute_sql(
        f"DELETE FROM {table_name} WHERE close_approach_date BETWEEN :start_date AND :end_date",
        {"start_date": start_date, "end_date": end_date},
    )

    # Get the count of rows deleted from the result
    deleted_rows = result.rowcount if result.rowcount is not None else 0
    return deleted_rows
    

def prepare_dataframe_for_postgres(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame data types for PostgreSQL insertion.
    
    Converts pandas data types to PostgreSQL-optimized formats:
    - Date columns to datetime64[ns]
    - Integer columns with boolean-like values (0/1) to boolean
    - String columns to appropriate text types with validation
    
    Args:
        dataframe (pd.DataFrame): Input DataFrame with mixed data types
        
    Returns:
        pd.DataFrame: DataFrame with PostgreSQL-optimized data types
        
    Raises:
        ValueError: If data conversion fails or contains invalid values
    """
    if dataframe is None or dataframe.empty:
        return dataframe
    
    # Create a copy to avoid modifying the original DataFrame
    optimized_df = dataframe.copy()
    
    # Convert date columns to datetime
    date_columns = ['close_approach_date', 'epoch_date_close_approach', 'orbital_period_days']
    for col in date_columns:
        if col in optimized_df.columns:
            try:
                # Handle different date formats
                if col == 'close_approach_date':
                    # Expected format: YYYY-MM-DD
                    optimized_df[col] = pd.to_datetime(optimized_df[col], format='%Y-%m-%d', errors='coerce')
                elif col == 'epoch_date_close_approach':
                    # Expected format: Unix timestamp or ISO format
                    optimized_df[col] = pd.to_datetime(optimized_df[col], errors='coerce')
                elif col == 'orbital_period_days':
                    # Convert to numeric first, then handle as days
                    optimized_df[col] = pd.to_numeric(optimized_df[col], errors='coerce')
            except Exception as e:
                print(f"[load] Warning: Could not convert {col} to datetime: {e}")
    
    # Convert boolean-like integer columns
    boolean_columns = ['is_potentially_hazardous', 'is_sentry_object']
    for col in boolean_columns:
        if col in optimized_df.columns:
            try:
                # Convert 0/1 integers to boolean, handle NaN values
                unique_values = optimized_df[col].dropna().unique()
                if set(unique_values).issubset({0, 1, True, False}):
                    optimized_df[col] = optimized_df[col].astype(bool)
            except Exception as e:
                print(f"[load] Warning: Could not convert {col} to boolean: {e}")
    
    # Optimize numeric columns for PostgreSQL
    numeric_columns = [
        'diameter_min_km', 'diameter_max_km',  # Actual column names in CSV
        'relative_velocity_kps', 'miss_distance_km',  # Actual column names in CSV
        'absolute_magnitude_h', 'orbital_period_days'
    ]
    for col in numeric_columns:
        if col in optimized_df.columns:
            try:
                # Convert to float64 for precision, handle NaN values
                optimized_df[col] = pd.to_numeric(optimized_df[col], errors='coerce')
                # Use float32 for less critical measurements to save space
                if col in ['relative_velocity_kps', 'miss_distance_km']:
                    optimized_df[col] = optimized_df[col].astype('float32')
            except Exception as e:
                print(f"[load] Warning: Could not optimize numeric column {col}: {e}")
    
    # Validate string columns and limit length for VARCHAR efficiency
    string_columns = ['id', 'name', 'nasa_jpl_url', 'orbiting_body']  # Updated column names
    for col in string_columns:
        if col in optimized_df.columns:
            try:
                # Ensure string type and handle nulls
                optimized_df[col] = optimized_df[col].astype(str)
                # Replace 'nan' strings with actual NaN
                optimized_df[col] = optimized_df[col].replace('nan', pd.NA)
                # Truncate very long strings to reasonable limits for PostgreSQL VARCHAR
                if col == 'nasa_jpl_url':
                    optimized_df[col] = optimized_df[col].str[:500]  # URLs can be long
                else:
                    optimized_df[col] = optimized_df[col].str[:255]  # Standard VARCHAR limit
            except Exception as e:
                print(f"[load] Warning: Could not optimize string column {col}: {e}")
    
    return optimized_df


def load_dataframe_to_database( # Main function to load a pandas DataFrame into the database (with optional pre-delete for idempotency)
        dataframe: pd.DataFrame,
        database_url: Optional[str] = None,
        table_name: str = "neows",
        if_exists: Literal["fail", "replace", "append"] = "append",
        chunk_size: Optional[int] = None,
        delete_range_before_insert: bool = True,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> int:
    """
    Insert a DataFrame into the database, creating the DB/table if needed.

    Works with both SQLite and PostgreSQL backends. Optionally deletes an 
    existing date window to keep re-runs idempotent under the composite 
    PK (close_approach_date, id).

    Args:
        dataframe (pd.DataFrame): Transformed NeoWs records to persist.
        database_url (Optional[str]): Database connection string. If None,
            uses the configured DATABASE_URL.
        table_name (str): Destination table name. Defaults to "neows".
        if_exists (str): Behavior if the table exists. One of {"fail","replace","append"}.
            Defaults to "append".
        chunk_size (Optional[int]): Optional number of rows per batch insert.
        delete_range_before_insert (bool): If True, delete rows in the target
            date window before inserting. Defaults to True.
        start_date (Optional[str]): Start of the date window ("YYYY-MM-DD"). If None,
            inferred from the DataFrame.
        end_date (Optional[str]): End of the date window ("YYYY-MM-DD"). If None,
            inferred from the DataFrame.

    Returns:
        int: Number of rows written.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If database operations fail.
        ValueError: If the DataFrame is empty or required columns are missing.
    """
    if dataframe is None or dataframe.empty:
        raise ValueError("No data to load: the provided DataFrame is empty.") # Immediately checks if the DataFrame is empty and raises a ValueError if so

    if delete_range_before_insert and ("close_approach_date" not in dataframe.columns): # Ensures the DataFrame has the required column if pre-delete is requested and raises an ValueError if not
        raise ValueError(
            "DataFrame must contain 'close_approach_date' column to delete date range." 
        )
    
    # Infer date range from DataFrame if not provided
    if delete_range_before_insert and (start_date is None or end_date is None): # If pre-delete is requested but start_date or end_date is not provided, infers them from the DataFrame (for testing the module by itself)
        start_date = str(dataframe["close_approach_date"].min()) # Infers the start date from the minimum close_approach_date in the DataFrame
        end_date = str(dataframe["close_approach_date"].max()) # Infers the end date from the maximum close_approach_date in the DataFrame

    # Use configured DATABASE_URL if none provided
    if database_url is None:
        database_url = DATABASE_URL

    # Ensure database and table exist
    ensure_database_ready(database_url)

    # Optional pre-delete to keep re-runs idempotent
    if delete_range_before_insert and start_date and end_date:
        deleted_rows = delete_date_range(
            start_date=start_date,
            end_date=end_date,
            database_url=database_url,
            table_name=table_name,
        )
        print(f"[load] Pre-delete: removed {deleted_rows} rows in [{start_date} .. {end_date}]")

    # Create DatabaseManager instance and insert data
    db = DatabaseManager(database_url)
    
    # Optimize DataFrame for PostgreSQL if using PostgreSQL backend
    if db.is_postgres:
        print("[load] Optimizing DataFrame for PostgreSQL...")
        dataframe = prepare_dataframe_for_postgres(dataframe)
    
    with db.get_connection() as conn:
        dataframe.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size,
            method=None,
        )
        conn.commit()
    
    return int(len(dataframe)) # Returns the number of rows written to the database (the length of the DataFrame)


# Verifies functionality when running this file directly
if __name__ == "__main__":
    """
    Script entry point for manual testing.

    Reads the default CSV produced by transform.py (CSV_OUTPUT),
    ensures the database exists, deletes the CSV's date window,
    and appends rows to the "neows" table. Prints a confirmation with
    row count and target database. 
    """
    try:
        print(f"[load] Reading CSV from: {CSV_OUTPUT}")
        records_dataframe = read_csv_to_dataframe(CSV_OUTPUT)

        min_date = str(records_dataframe["close_approach_date"].min())
        max_date = str(records_dataframe["close_approach_date"].max())

        print(f"[load] Ensuring database ready.")
        written_rows = load_dataframe_to_database(
            dataframe = records_dataframe,
            database_url = None,
            table_name = "neows",
            if_exists = "append",
            delete_range_before_insert = True,
            start_date = min_date,
            end_date = max_date,
        )

        print(f"[load] Wrote {written_rows} rows to database.")
        print(f"[load] Warehouse directory: {WAREHOUSE_DIR}")

    except Exception as e:
        print(f"[load] [ERROR] {type(e).__name__}: {e}")
        raise