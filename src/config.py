"""
Configuration settings for the NASA NeoWs Data Pipeline.

This module handles environment variables, file paths, and mode selection (DEMO vs LIVE).
"""
# This module acts as a centralized configuration hub for the pipeline, managing environment variables, file paths, and mode settings.

import os #Allows the program to interact with os environment variables eg. os.getenv("NASA_API_KEY")
from pathlib import Path # Allows the program to work with file system path objects in a platform-independent way
from dotenv import load_dotenv # Allows the program to load environment variables from a .env file if present

# Load environment variables from a .env file if present
load_dotenv()

#------------------------------------------------------------------------------
# Mode Settings
#------------------------------------------------------------------------------

#DEMO_Mode: if "1" or "true", the pipeline will use local sample data only.
DEMO_MODE = os.getenv("DEMO_MODE", "0").lower() in ("1", "true", "yes") # Looks for the env variable DEMO_MODE (case insensitive); if not found, defaults to "0" (false). Converts to boolean.

#------------------------------------------------------------------------------
# File Paths
#------------------------------------------------------------------------------

# Project root (this file is in src/, so go up one level)
ROOT_DIR = Path(__file__).resolve().parents[1] # Gets the absolute path (resolved from relative path) to the directory two levels up from this file (the project root)

DATA_DIR = ROOT_DIR / "data" # Designates the data directory within the project root
PROCESSED_DIR = DATA_DIR / "processed" # Designates the processed data directory within the data directory
WAREHOUSE_DIR = DATA_DIR / "warehouse" # Designates the warehouse directory within the data directory
SAMPLE_DATA_DIR = ROOT_DIR / "sample_data" # Designates the sample_data directory within the project root

#------------------------------------------------------------------------------
# API Configuration
#------------------------------------------------------------------------------

# NASA API key (use DEMO_KEY if none provided)
NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY") # Looks for the env variable NASA_API_KEY; if not found, defaults to "DEMO_KEY" (NASA's public demo key)

# Base URL for the NASA NeoWs API
NASA_API_BASE_URL = "https://api.nasa.gov/neo/rest/v1" 

#------------------------------------------------------------------------------
# Output Files
#------------------------------------------------------------------------------

CSV_OUTPUT = PROCESSED_DIR / "neows_latest.csv" # Path to the output CSV file (latest processed data)
DB_PATH = WAREHOUSE_DIR / "neows_data.db" # Path to the SQLite database file (data warehouse)

#------------------------------------------------------------------------------
# Print Settings (debug)
#------------------------------------------------------------------------------

# Verifies configuration when running this file directly
if __name__ == "__main__":
    print(f"DEMO_MODE: {DEMO_MODE}") # Prints the current mode (True for demo, False for live)
    print(f"ROOT_DIR: {ROOT_DIR}") # Prints the root directory path
    print(f"DATA_DIR: {DATA_DIR}") # Prints the data directory path
    print(f"NASA_API_KEY: {NASA_API_KEY}") # Prints the NASA API key being used (for debugging; be cautious with sensitive keys)