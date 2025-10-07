"""
Configuration settings for the NASA NeoWs Data Pipeline.

This module handles environment variables, file paths, and mode selection (DEMO vs LIVE).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

#------------------------------------------------------------------------------
# Mode Settings
#------------------------------------------------------------------------------

#DEMO_Mode: if "1" or "true", the pipeline will use local sample data only.
DEMO_MODE = os.getenv("DEMO_MODE", "0").lower() in ("1", "true", "yes")

#------------------------------------------------------------------------------
# File Paths
#------------------------------------------------------------------------------

# Project root (this file is in src/, so go up one level)
ROOT_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = ROOT_DIR / "data"
PROCCESSED_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
SAMPLE_DATA_DIR = ROOT_DIR / "sample_data"

#------------------------------------------------------------------------------
# API Configuration
#------------------------------------------------------------------------------

# NASA API key (use DEMO_KEY if none provided)
NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")

# Base URL for the NASA NeoWs API
NASA_API_BASE_URL = "https://api.nasa.gov/neo/rest/v1"

#------------------------------------------------------------------------------
# Output Files
#------------------------------------------------------------------------------

CSV_OUTPUT = PROCCESSED_DIR / "neows_latest.csv"
DB_PATH = WAREHOUSE_DIR / "neows_data.db"

#------------------------------------------------------------------------------
# Print Settings (debug)
#------------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"DEMO_MODE: {DEMO_MODE}")
    print(f"ROOT_DIR: {ROOT_DIR}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"NASA_API_KEY: {NASA_API_KEY}")