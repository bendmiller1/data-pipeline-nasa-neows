"""
Pipeline orchestrator for the NASA NeoWs Data Pipeline.

This module wires together the ETL stages:
    1) Fetch     (src.fetch.fetch_feed)
    2) Transform (src.transform.transform_to_dataframe + save_dataframe_to_csv)
    3) Load      (src.load.load_dataframe_to_sqlite)

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
from typing import List # Allows use of List in type hints

from .config import CSV_OUTPUT, DB_PATH # Imports the CSV output path and database path from the config module
from .fetch import fetch_feed # Imports the fetch_feed function from the fetch module
from .transform import transform_to_dataframe, save_dataframe_to_csv # Imports transform_to_dataframe and save_dataframe_to_csv functions from the transform module
from .load import load_dataframe_to_sqlite # Imports the load_dataframe_to_sqlite function from the load module
from .utils.dates import validate_date_range # Imports the validate_date_range function from the utils.dates module
from .utils.env import set_demo_mode_for_process, set_live_mode_for_process # Imports functions to set runtime mode for the pipeline (DEMO = Local sample data, LIVE = NASA API)


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
        """
    )

    # Mode selection
    parser.add_argument( # Creates a new command line argument --mode
        "--mode",
        choices = ["feed", "browse"], # Allows only "feed" or "browse" as valid options
        default = "feed", # Default mode is "feed" (browse is a future feature)
        help = "Pipeline execution mode (default: feed) (browse is a future feature)"
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
        written_rows = load_dataframe_to_sqlite( # Calls load_dataframe_to_sqlite to load the DataFrame into the SQLite database
            dataframe = dataframe, # DataFrame to load
            database_path = DB_PATH, # Path to the SQLite database
            table_name = "neows", # Table name to load data into
            if_exists = "append", # If the table exists, append new data to the existing table
            delete_range_before_insert = True, # Delete existing records in the date range before inserting new data (ensures idempotency)
            start_date = start_date, # User-provided start date for the date range
            end_date = end_date, # User-provided end date for the date range
        )
        print(f"[pipeline] Loaded {written_rows} rows into SQLite database at: {DB_PATH}") # Prints the number of rows written to the database and the database path
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
    
    else:
        print(f"[pipeline][ERROR] Unknown mode: {args.mode}")
        return 1
    

if __name__ == "__main__": 
    # Allow: python -m src.pipeline --mode feed --start ... --end ... [--demo]
    #    or: python -m src.pipeline --mode browse --pages N [--demo]  (future feature)
    sys.exit(main())