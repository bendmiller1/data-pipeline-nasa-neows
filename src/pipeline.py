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

from __future__ import annotations

import os
import sys
import argparse
from typing import List

from .config import CSV_OUTPUT, DB_PATH
from .fetch import fetch_feed
from .transform import transform_to_dataframe, save_dataframe_to_csv
from .load import load_dataframe_to_sqlite
from .utils.dates import validate_date_range
from .utils.env import set_demo_mode_for_process, set_live_mode_for_process


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Construct the argument parser for the pipeline CLI.

    Returns:
        argparse.ArgumentParser: Configured parser with mode-specific options.
    """
    parser = argparse.ArgumentParser(
        description = "Run the NASA NeoWs data pipeline (fetch, transform, load).",
        formatter_class = argparse.RawDescriptionHelpFormatter,
        epilog = """
Typical usage examples:
  %(prog)s --mode feed --start 2025-10-01 --end 2025-10-03
  %(prog)s --mode feed --start 2025-10-01 --end 2025-10-03 --demo
  %(prog)s --mode feed --start 2025-10-01 --end 2025-10-03 --live
  %(prog)s --mode browse --pages 5 --demo  # (future feature)
        """
    )

    # Mode selection
    parser.add_argument(
        "--mode",
        choices = ["feed", "browse"],
        default = "feed",
        help = "Pipeline execution mode (default: feed) (browse is a future feature)"
    )

    # Feed mode arguments
    parser.add_argument(
        "--start",
        help = "Start date (inclusive) in YYYY-MM-DD format - required for feed mode"
    )
    parser.add_argument(
        "--end",
        help = "End date (inclusive) in YYYY-MM-DD format - required for feed mode"
    )

    # Browse mode arguments (future feature)
    parser.add_argument(
        "--pages",
        type = int,
        default = 1,
        help = "Number of pages to fetch - for browse mode (default: 1) (future feature)"
    )

    # Mode override flags (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--demo",
        action = "store_true",
        help = "Force demo mode (use local sample data)"
    )
    mode_group.add_argument(
        "--live",
        action = "store_true",
        help = "Force live mode (use NASA API with DEMO_KEY)"
    )

    return parser


def run_feed_mode(start_date: str, end_date: str) -> int:
    """
    Execute the feed mode ETL pipeline.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        
    Returns:
        int: Exit code (0 = success, non-zero = failure).
    """
    print(f"[pipeline] Running feed ETL for [{start_date} to {end_date}] (DEMO_MODE={os.getenv('DEMO_MODE', '0')})")

    # 1) Fetch
    try:
        raw_feed_data = fetch_feed(start_date, end_date)
        if "near_earth_objects" not in raw_feed_data:
            print("[pipeline][ERROR] Missing 'near_earth_objects' in feed response")
            return 3
    except Exception as e:
        print(f"[pipeline][ERROR][fetch] {type(e).__name__}: {e}")
        return 3
    
    # 2) Transform + CSV
    try:
        dataframe = transform_to_dataframe(raw_feed_data)
        if dataframe.empty:
            print("[pipeline][WARN] Transform produced an empty dataset.")
        save_dataframe_to_csv(dataframe, CSV_OUTPUT)
        print(f"[pipeline] CSV output written to: {CSV_OUTPUT}")
    except Exception as e:
        print(f"[pipeline][ERROR][transform] {type(e).__name__}: {e}")
        return 4
    
    # 3) Load (idempotent for the selected window)
    try:
        written_rows = load_dataframe_to_sqlite(
            dataframe = dataframe,
            database_path = DB_PATH,
            table_name = "neows",
            if_exists = "append",
            delete_range_before_insert = True,
            start_date = start_date,
            end_date = end_date,
        )
        print(f"[pipeline] Loaded {written_rows} rows into SQLite database at: {DB_PATH}")
    except Exception as e:
        print(f"[pipeline][ERROR][load] {type(e).__name__}: {e}")
        return 5
    
    print("[pipeline] Feed ETL completed successfully. Ad Astra!")
    return 0


def run_browse_mode(pages: int) -> int:
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



def main(argv: List[str] | None = None) -> int:
    """
    Entry point for the pipeline CLI.

    Args:
        argv (List[str] | None): Command-line arguments; defaults to sys.argv[1:].

    Returns:
        int: Process exit code (0 = success, non-zero = failure stage code).
    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # Handle mode overrides (--demo or --live flags)
    if args.demo:
        set_demo_mode_for_process(True)
        print("[pipeline] Forcing demo mode (sample data)")
    elif args.live:
        set_live_mode_for_process(True)
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