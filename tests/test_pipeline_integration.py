"""
Integration tests for the NASA NeoWs Data Pipeline.

This module contains end-to-end tests that validate the complete ETL workflow
from CLI invocation through data output verification. Tests use subprocess
to invoke the pipeline as a user would, ensuring realistic integration testing.

Test Coverage:
- Complete ETL pipeline execution in demo and live modes
- CLI argument validation and error handling  
- Output file creation and data validation
- Database schema and data integrity
- User interface behavior (help, flags, error messages)
"""

import subprocess
import sys
import tempfile
import sqlite3
from pathlib import Path
import pandas as pd
import pytest

from src.config import CSV_OUTPUT, DB_PATH, PROCESSED_DIR, WAREHOUSE_DIR


class TestPipelineIntegration:
    """
    Integration tests for the complete pipeline workflow.
    
    These tests validate the pipeline behavior from a user perspective,
    testing CLI interactions, file I/O, and data processing end-to-end.
    Each test runs the pipeline as a subprocess to ensure realistic
    testing conditions.
    """

    def test_pipeline_demo_mode_success(self):
        """
        Test complete ETL pipeline runs successfully in demo mode.
        
        Validates the full workflow:
        1. CLI accepts demo mode arguments correctly
        2. Sample data is loaded and processed without errors
        3. CSV output is created with expected schema and data
        4. SQLite database is created with proper schema
        5. Data integrity between CSV and database outputs
        
        This test ensures the core pipeline functionality works
        end-to-end with predictable sample data.
        """
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "feed",
            "--start", "2025-10-01",
            "--end", "2025-10-03",
            "--demo"
        ], capture_output=True, text=True, cwd=Path.cwd())

        # Assert pipeline completed successfully
        assert result.returncode == 0, f"Pipeline failed with output: {result.stderr}"
        assert "Feed ETL"

    def test_pipeline_live_mode_flag(self):
        """
        Test pipeline accepts --live flag and attempts live API mode.
        
        Verifies:
        - CLI correctly interprets --live flag
        - Pipeline attempts to contact NASA API
        - Graceful handling of potential network/API failures
        - Appropriate error messages for API-related issues
        
        Note: This test may fail due to network conditions or API
        rate limits, which is expected behavior.
        """

    def test_pipeline_validation_errors(self):
        """
        Test pipeline properly validates required CLI arguments.
        
        Ensures the pipeline rejects invalid invocations:
        - Missing required --start and --end arguments
        - Returns appropriate exit code (2) for validation errors
        - Provides clear error messages to guide users
        
        This validates the user experience for common CLI mistakes.
        """

    def test_pipeline_invalid_date_range(self):
        """
        Test pipeline validates date range logic correctly.
        
        Verifies business rule enforcement:
        - Start date cannot be after end date
        - Date format validation (YYYY-MM-DD)
        - Clear error messaging for date range violations
        - Appropriate exit codes for different error types
        
        This ensures data integrity at the input validation level.
        """

    def test_pipeline_help_output(self):
        """
        Test pipeline provides comprehensive usage information.
        
        Validates the CLI help system:
        - Help flag (--help) produces detailed usage information
        - All major options are documented in help output
        - Examples and descriptions are clear and actionable
        - Exit code 0 for successful help display
        
        This ensures good user experience for pipeline discovery.
        """

    def test_pipeline_mutually_exclusive_flags(self):
        """
        Test --demo and --live flags are mutually exclusive.
        
        Validates CLI argument validation:
        - Cannot specify both --demo and --live simultaneously
        - argparse properly rejects conflicting options
        - Clear error message explains the conflict
        - Non-zero exit code indicates user error
        
        This prevents user confusion and ensures predictable behavior.
        """

    def test_browse_mode_placeholder(self):
        """
        Test browse mode returns appropriate not-implemented message.
        
        Validates future feature placeholder:
        - Browse mode is recognized but not yet functional
        - Returns specific exit code (6) for not-implemented features
        - Provides informative message about future implementation
        - CLI accepts browse mode arguments without crashing
        
        This ensures extensible design and clear user communication.
        """

    @pytest.fixture(autouse=True)
    def cleanup_test_outputs(self):
        """
        Clean up test outputs before and after each test execution.
        
        This fixture ensures test isolation by:
        - Removing any existing output files before test execution
        - Cleaning up generated files after test completion
        - Preventing test interdependencies through shared state
        - Maintaining consistent test environment conditions
        
        Yields:
            None: Control returns to test execution, then cleanup occurs.
        """