"""
Pipeline orchestrator for the NASA NeoWs Data Pipeline.

This module wires together the ETL stages:
    1) Fetch     (src.fetch.fetch_feed)
    2) Transform (src.transform.transform_to_dataframe + save_dataframe_to_csv)
    3) Load      (src.load.load_dataframe_to_database)

Run as a module:
    python -m src.pipeline --mode feed --start 2025-10-01 --end 2025-10-03 --demo
    python -m src.pipeline --mode feed --start 2025-10-01 --end 2025-10-03 --live
    python -m src.pipeline --mode browse --pages 5 --demo  # (future feature)

CLI options:
    --mode {feed,browse}   Pipeline mode (default: feed) (browse is a future feature) 
    --start YYYY-MM-DD     Start date (inclusive) - required for feed mode
    --end   YYYY-MM-DD     End date (inclusive) - required for feed mode
    --pages N              Number of pages to fetch - for browse mode (future feature)
    --demo                 Force demo mode (use local sample data)
    --live                 Force live mode (use NASA API with DEMO_KEY)
"""

from __future__ import annotations # Allows the pipeline to use newer type hint syntax in older Python versions

import os # Allows the pipeline to interact with os environment variables eg. os.getenv("NASA_API_KEY")
import sys # Allows the pipeline to interact with the Python runtime environment (eg. sys.exit())
import argparse # Allows the pipeline to parse command line arguments (eg. --mode feed --start 2025-10-01 --end 2025-10-03)
from typing import List, Optional # Allows use of List and Optional in type hints

from .config import CSV_OUTPUT, DB_PATH, DATABASE_URL # Imports the CSV output path, database path, and database URL from the config module
from .fetch import fetch_feed # Imports the fetch_feed function from the fetch module
from .transform import transform_to_dataframe, save_dataframe_to_csv # Imports transform_to_dataframe and save_dataframe_to_csv functions from the transform module
from .load import load_dataframe_to_database, DatabaseManager # Imports the load_dataframe_to_database function and DatabaseManager class from the load module
from .migration_manager import MigrationManager # Imports the MigrationManager class for handling database migrations
from .utils.dates import validate_date_range # Imports the validate_date_range function from the utils.dates module
from .utils.mode_toggle import set_demo_mode_for_process, set_live_mode_for_process # Imports functions to set runtime mode for the pipeline (DEMO = Local sample data, LIVE = NASA API)


def build_arg_parser() -> argparse.ArgumentParser: # Function to build and return the CLI argument parser
    """
    Construct the argument parser for the pipeline Command Line Interface (CLI).

    Returns:
        argparse.ArgumentParser: Configured parser with mode-specific options.
    """
    parser = argparse.ArgumentParser( # Creates a new ArgumentParser object 
        description = "Run the NASA NeoWs data pipeline (fetch, transform, load).", # Description shown in the help message
        formatter_class = argparse.RawDescriptionHelpFormatter, # Allows the epilog to be formatted as raw text
        # Epilog for usage examples (printed when --help is used)
        epilog = """
Typical usage examples:
  %(prog)s --mode feed --start 2025-10-01 --end 2025-10-03
  %(prog)s --mode feed --start 2025-10-01 --end 2025-10-03 --demo
  %(prog)s --mode feed --start 2025-10-01 --end 2025-10-03 --live
  %(prog)s --mode browse --pages 5 --demo  # (future feature)
  %(prog)s --mode migrate  # Apply all pending migrations
  %(prog)s --mode migrate --target 001 --dry-run  # Preview migration to version 001
  %(prog)s --mode migrate --rollback --target 001  # Rollback to version 001
        """
    )

    # Mode selection
    parser.add_argument( # Creates a new command line argument --mode
        "--mode",
        choices = ["feed", "browse", "migrate"], # Allows "feed", "browse", or "migrate" as valid options
        default = "feed", # Default mode is "feed" (browse is a future feature)
        help = "Pipeline execution mode (default: feed) (browse is a future feature, migrate runs database migrations)"
    )

    # Feed mode arguments
    parser.add_argument( # Creates a new command line argument --start to specify the start date (inclusive) for feed mode
        "--start",
        help = "Start date (inclusive) in YYYY-MM-DD format - required for feed mode" 
    )
    parser.add_argument( # Creates a new command line argument --end to specify the end date (inclusive) for feed mode
        "--end",
        help = "End date (inclusive) in YYYY-MM-DD format - required for feed mode"
    )

    # Browse mode arguments (future feature)
    parser.add_argument( # Will create a new command line argument --pages to specify number of pages to fetch for browse mode (future feature)
        "--pages",
        type = int,
        default = 1,
        help = "Number of pages to fetch - for browse mode (default: 1) (future feature)"
    )

    # Migration mode arguments
    parser.add_argument( # Creates a new command line argument --target to specify target migration version
        "--target",
        help = "Target migration version (e.g., '002') - for migrate mode (optional, defaults to latest)"
    )
    parser.add_argument( # Creates a new command line argument --dry-run to preview migration changes
        "--dry-run",
        action = "store_true",
        help = "Preview migration changes without executing - for migrate mode"
    )
    parser.add_argument( # Creates a new command line argument --rollback to rollback migrations
        "--rollback",
        action = "store_true",
        help = "Rollback migrations to target version - for migrate mode"
    )

    # Mode override flags (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group() # Creates a mutually exclusive group for the --demo and --live flags (only one can be specified at a time)
    mode_group.add_argument( # Creates a new command line argument --demo to force demo mode (local sample data)
        "--demo",
        action = "store_true", # If --demo is specified, arg.demo will be set to True; otherwise False
        help = "Force demo mode (use local sample data)"
    )
    mode_group.add_argument( # Creates a new command line argument --live to force live mode (NASA API)
        "--live",
        action = "store_true", # If --live is specified, arg.live will be set to True; otherwise False
        help = "Force live mode (use NASA API with DEMO_KEY)"
    )

    return parser # Returns the configured ArgumentParser object with all the defined arguments


def run_feed_mode(start_date: str, end_date: str) -> int: # Function to run the feed mode ETL pipeline (takes validated user-provided start and date strings as parameters)
    """
    Execute the feed mode ETL pipeline.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        
    Returns:
        int: Exit code (0 = success, non-zero = failure).
    """
    print(f"[pipeline] Running feed ETL for [{start_date} to {end_date}] (DEMO_MODE={os.getenv('DEMO_MODE', '0')})") # Prints the start of the feed ETL process with the date range and current mode (DEMO or LIVE) ()

    # 0) Ensure database schema is up-to-date
    try:
        print("[pipeline] Ensuring database schema is up-to-date...")
        db_manager = DatabaseManager(DATABASE_URL)
        migration_manager = MigrationManager(db_manager)
        
        # Check for pending migrations and apply them automatically
        pending = migration_manager.get_pending_migrations()
        if pending:
            print(f"[pipeline] Applying {len(pending)} pending migrations: {pending}")
            migration_manager.migrate_up()
            print("[pipeline] Database schema updated successfully")
        else:
            print("[pipeline] Database schema is up-to-date")
    except Exception as e:
        print(f"[pipeline][ERROR][auto-migrate] {type(e).__name__}: {e}")
        print("[pipeline][WARNING] Continuing without auto-migration - manual migration may be required")

    # 1) Fetch
    try:
        raw_feed_data = fetch_feed(start_date, end_date) # Calls fetch_feed to retrieve the raw JSON data from the NASA NeoWs API or local sample data (based on mode)
        if "near_earth_objects" not in raw_feed_data: # Validates that the expected key is present in the response
            print("[pipeline][ERROR] Missing 'near_earth_objects' in feed response") # Prints an error message if the key is missing
            return 3
    except Exception as e:
        print(f"[pipeline][ERROR][fetch] {type(e).__name__}: {e}") # Catches and prints any exceptions that occur during the fetch stage
        return 3
    
    # 2) Transform + CSV
    try:
        dataframe = transform_to_dataframe(raw_feed_data) # Calls transform_to_dataframe to convert the raw JSON data into a pandas DataFrame
        if dataframe.empty:
            print("[pipeline][WARN] Transform produced an empty dataset.") # If the DataFrame is empty, print a warning
        save_dataframe_to_csv(dataframe, CSV_OUTPUT) # Else, saves the DataFrame to a CSV file at the configured CSV_OUTPUT path
        print(f"[pipeline] CSV output written to: {CSV_OUTPUT}") # Prints the path where the CSV file was saved
    except Exception as e:
        print(f"[pipeline][ERROR][transform] {type(e).__name__}: {e}") # catches and prints any exceptions that occur during the transform stage
        return 4
    
    # 3) Load (idempotent for the selected window)
    try:
        written_rows = load_dataframe_to_database( # Calls load_dataframe_to_database to load the DataFrame into the database
            dataframe = dataframe, # DataFrame to load
            database_url = DATABASE_URL, # Uses configured database URL (SQLite or PostgreSQL)
            table_name = "neows", # Table name to load data into
            if_exists = "append", # If the table exists, append new data to the existing table
            delete_range_before_insert = True, # Delete existing records in the date range before inserting new data (ensures idempotency)
            start_date = start_date, # User-provided start date for the date range
            end_date = end_date, # User-provided end date for the date range
        )
        print(f"[pipeline] Loaded {written_rows} rows into database: {DATABASE_URL}") # Prints the number of rows written to the database and the database URL
    except Exception as e:
        print(f"[pipeline][ERROR][load] {type(e).__name__}: {e}") # Catches and prints any exceptions that occur during the load stage
        return 5
    
    print("[pipeline] Feed ETL completed successfully. Ad Astra!") # Prints a success message at the end of the feed ETL process
    return 0


def run_browse_mode(pages: int) -> int: # Unimplemented function to run the browse mode ETL pipeline (takes number of pages to fetch as a parameter)
    """
    Execute the browse mode ETL pipeline.
    
    Note: This is a placeholder for future implementation.
    
    Args:
        pages (int): Number of pages to fetch.
        
    Returns:
        int: Exit code (0 = success, non-zero = failure).
    """
    print(f"[pipeline] Browse mode is not yet implemented (pages={pages})")
    print("[pipeline] This feature will fetch detailed asteroid information")
    print("[pipeline] from the /neo/rest/v1/neo/browse endpoint")
    
    # TODO: Implement browse mode
    # 1) Fetch from browse endpoint with pagination
    # 2) Transform browse data to appropriate format  
    # 3) Load to database (possibly different table/schema)
    
    return 6  # Not implemented error code


def run_migrate_mode(target_version: Optional[str] = None, dry_run: bool = False, rollback: bool = False) -> int:
    """
    Execute database migrations.

    Args:
        target_version (str): Target migration version (optional)
        dry_run (bool): Preview changes without executing
        rollback (bool): Rollback migrations instead of applying

    Returns:
        int: Process exit code (0 = success, 7 = migration failure)
    """
    try:
        print("[pipeline] Initializing migration system...")
        
        # Create database manager and migration manager
        db_manager = DatabaseManager(DATABASE_URL)
        migration_manager = MigrationManager(db_manager)
        
        # Validate migrations first
        print("[pipeline] Validating migration files...")
        errors = migration_manager.validate_migrations()
        if errors:
            print("[pipeline][ERROR] Migration validation failed:")
            for error in errors:
                print(f"  - {error}")
            return 7
        
        # Show current status
        print("[pipeline] Current migration status:")
        status = migration_manager.get_migration_status()
        current_version = migration_manager.version_manager.get_current_version()
        print(f"  Current version: {current_version or 'None (fresh database)'}")
        
        for version, applied in status.items():
            status_symbol = "✅" if applied else "⏳"
            print(f"  {status_symbol} Migration {version}: {'Applied' if applied else 'Pending'}")
        
        # Execute migrations
        if rollback:
            if not target_version:
                print("[pipeline][ERROR] Target version required for rollback")
                return 7
            print(f"[pipeline] {'[DRY RUN] ' if dry_run else ''}Rolling back to version {target_version}...")
            migration_manager.migrate_down(target_version, dry_run=dry_run)
        else:
            print(f"[pipeline] {'[DRY RUN] ' if dry_run else ''}Applying migrations{' to version ' + target_version if target_version else ''}...")
            migration_manager.migrate_up(target_version, dry_run=dry_run)
        
        # Show final status
        if not dry_run:
            final_version = migration_manager.version_manager.get_current_version()
            print(f"[pipeline] Migration completed successfully. Current version: {final_version}")
        else:
            print("[pipeline] Dry run completed. No changes were made.")
        
        return 0
        
    except Exception as e:
        print(f"[pipeline][ERROR][migrate] {type(e).__name__}: {e}")
        return 7


def main(argv: List[str] | None = None) -> int: # Main entry point for the pipeline CLI (takes optional command line arguments as a parameter)
    """
    Entry point for the pipeline CLI.

    Args:
        argv (List[str] | None): Command-line arguments; defaults to sys.argv[1:].

    Returns:
        int: Process exit code (0 = success, non-zero = failure stage code).
    """
    parser = build_arg_parser() # Calls build_arg_parser to create the argument parser
    args = parser.parse_args(argv) # Parses the command line arguments (or provided argv list)

    # Handle mode overrides (--demo or --live flags)
    if args.demo: # If --demo flag is specified,
        set_demo_mode_for_process(True) # Force demo mode (local sample data)
        print("[pipeline] Forcing demo mode (sample data)")
    elif args.live: # If --live flag is specified,
        set_live_mode_for_process(True) # Force live mode (NASA API)
        print("[pipeline] Forcing live mode (NASA API)")
    # Otherwise, use .env file settings
    
    # Mode-specific validation and execution
    if args.mode == "feed":
        # Validate required arguments for feed mode
        if not args.start or not args.end:
            print("[pipeline][ERROR] Feed mode requires --start and --end dates.")
            return 2
        
        # Validate date range
        try:
            start_date, end_date = validate_date_range(args.start, args.end)
        except ValueError as e:
            print(f"[pipeline][ERROR] Invalid date range: {e}")
            return 2
        
        return run_feed_mode(start_date, end_date)
    
    elif args.mode == "browse":
        # Browse mode (future feature)
        return run_browse_mode(args.pages)
    
    elif args.mode == "migrate":
        # Migration mode
        return run_migrate_mode(
            target_version=args.target,
            dry_run=args.dry_run,
            rollback=args.rollback
        )
    
    else:
        print(f"[pipeline][ERROR] Unknown mode: {args.mode}")
        return 1
    

if __name__ == "__main__": 
    # Allow: python -m src.pipeline --mode feed --start ... --end ... [--demo]
    #    or: python -m src.pipeline --mode browse --pages N [--demo]  (future feature)
    sys.exit(main())