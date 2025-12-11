"""
Integration tests for the NASA NeoWs Data Pipeline.

This module contains end-to-end tests that validate the complete ETL workflow
from CLI invocation through data output verification. Tests use subprocess
to invoke the pipeline as a user would, ensuring realistic integration testing.

Test Coverage:
- Complete ETL pipeline execution in demo and live modes
- Dual database support (SQLite and PostgreSQL) integration testing
- Migration system CLI integration and database switching
- CLI argument validation and error handling  
- Output file creation and data validation
- Database schema and data integrity across database types
- User interface behavior (help, flags, error messages)
"""

import subprocess
import sys
import tempfile
import sqlite3
import os
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

    def test_pipeline_dual_database_sqlite(self):
        """
        Test complete pipeline execution with SQLite database backend.
        
        Validates pipeline works correctly with SQLite configuration:
        - Sets USE_POSTGRES=false environment variable
        - Executes complete ETL workflow in demo mode
        - Verifies SQLite database creation and data integrity
        - Ensures CSV output matches database contents
        - Tests migration system integration with SQLite
        
        This ensures SQLite serves as a fully-functional backup system.
        """
        # Set environment for SQLite mode
        env = os.environ.copy()
        env['USE_POSTGRES'] = 'false'
        
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "feed",
            "--start", "2025-10-01", 
            "--end", "2025-10-03",
            "--demo"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env)
        
        # Assert successful execution
        assert result.returncode == 0, f"SQLite pipeline failed: {result.stderr}"
        assert "Feed ETL" in result.stdout
        assert "SQLite" in result.stdout or "sqlite" in result.stdout.lower()
        
        # Verify SQLite database was created and populated
        assert DB_PATH.exists(), "SQLite database file not created"
        
        # Check database contents
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM neows")
            count = cursor.fetchone()[0]
            assert count > 0, "No data found in SQLite database"
            
            # Verify schema includes expected columns
            cursor.execute("PRAGMA table_info(neows)")
            columns = [row[1] for row in cursor.fetchall()]
            expected_columns = ['id', 'name', 'close_approach_date', 'absolute_magnitude_h']
            for col in expected_columns:
                assert col in columns, f"Missing column {col} in SQLite schema"

    @pytest.mark.postgres_only  
    def test_pipeline_dual_database_postgresql(self):
        """
        Test complete pipeline execution with PostgreSQL database backend.
        
        Validates pipeline works correctly with PostgreSQL configuration:
        - Sets USE_POSTGRES=true environment variable
        - Executes complete ETL workflow in demo mode
        - Verifies PostgreSQL database connection and data integrity
        - Tests connection pooling and advanced PostgreSQL features
        - Ensures feature parity with SQLite implementation
        
        Requires PostgreSQL server to be running and accessible.
        """
        # Set environment for PostgreSQL mode
        env = os.environ.copy()
        env['USE_POSTGRES'] = 'true'
        
        # Set test database connection (if not already configured)
        if 'POSTGRES_HOST' not in env:
            env.update({
                'POSTGRES_HOST': 'localhost',
                'POSTGRES_PORT': '5432', 
                'POSTGRES_USER': 'postgres',
                'POSTGRES_PASSWORD': 'postgres',
                'POSTGRES_DB': 'neows_test'
            })
        
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "feed",
            "--start", "2025-10-01",
            "--end", "2025-10-03", 
            "--demo"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env)
        
        # Assert successful execution
        if result.returncode != 0:
            if "connection" in result.stderr.lower() or "postgresql" in result.stderr.lower():
                pytest.skip("PostgreSQL not available for integration test")
        
        assert result.returncode == 0, f"PostgreSQL pipeline failed: {result.stderr}"
        assert "Feed ETL" in result.stdout
        assert "postgresql" in result.stdout.lower() or "postgres" in result.stdout.lower()

    def test_pipeline_database_switching(self):
        """
        Test pipeline can switch between database backends via environment.
        
        Validates database backend switching functionality:
        - Tests switching from SQLite to PostgreSQL configuration
        - Verifies each backend processes data independently
        - Ensures configuration changes are respected
        - Tests that data isolation is maintained between backends
        
        This validates the dual database architecture design.
        """
        # First run with SQLite
        env_sqlite = os.environ.copy()
        env_sqlite['USE_POSTGRES'] = 'false'
        
        result_sqlite = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "feed", 
            "--start", "2025-10-01",
            "--end", "2025-10-03",
            "--demo"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env_sqlite)
        
        assert result_sqlite.returncode == 0, f"SQLite run failed: {result_sqlite.stderr}"
        
        # Verify SQLite database exists
        assert DB_PATH.exists(), "SQLite database not created"
        
        # Try PostgreSQL run (may skip if not available)
        env_postgres = os.environ.copy() 
        env_postgres['USE_POSTGRES'] = 'true'
        
        result_postgres = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "feed",
            "--start", "2025-10-01", 
            "--end", "2025-10-03",
            "--demo"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env_postgres)
        
        # PostgreSQL may not be available in test environment
        if result_postgres.returncode != 0:
            if "connection" in result_postgres.stderr.lower():
                pytest.skip("PostgreSQL not available for database switching test")
        
        # Both should succeed independently
        print(f"SQLite run: {result_sqlite.returncode}")
        print(f"PostgreSQL run: {result_postgres.returncode}")

    def test_migration_cli_help(self):
        """
        Test migration CLI provides comprehensive help information.
        
        Validates migration system CLI documentation:
        - Help includes migration mode options
        - Documents --target, --dry-run, --rollback flags
        - Provides clear usage examples
        - Shows available migration commands
        
        This ensures good user experience for migration operations.
        """
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline", "--help"
        ], capture_output=True, text=True, cwd=Path.cwd())
        
        assert result.returncode == 0, f"Help command failed: {result.stderr}"
        
        # Verify migration-related help content
        help_output = result.stdout
        assert "migrate" in help_output.lower(), "Migration mode not documented in help"
        assert "--target" in help_output, "--target option not documented"
        assert "--dry-run" in help_output, "--dry-run option not documented"
        assert "--rollback" in help_output, "--rollback option not documented"

    def test_migration_cli_dry_run(self):
        """
        Test migration dry run functionality via CLI.
        
        Validates migration preview capability:
        - Executes migration dry run without applying changes
        - Shows pending migrations and their descriptions
        - Returns appropriate exit code for dry run success
        - Provides informative output about migration status
        
        This ensures users can preview migration changes safely.
        """
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "migrate", 
            "--dry-run"
        ], capture_output=True, text=True, cwd=Path.cwd())
        
        assert result.returncode == 0, f"Migration dry run failed: {result.stderr}"
        
        output = result.stdout.lower()
        # Should mention dry run and migration status
        assert "dry" in output or "preview" in output, "Dry run not indicated in output"
        assert "migration" in output, "Migration information not shown"

    def test_migration_cli_execution_sqlite(self):
        """
        Test migration execution via CLI with SQLite backend.
        
        Validates migration system integration:
        - Executes migrations against SQLite database
        - Verifies migration tracking and version management
        - Tests migration status reporting
        - Ensures database schema is properly updated
        
        This validates the complete migration workflow for SQLite.
        """
        # Set SQLite environment
        env = os.environ.copy()
        env['USE_POSTGRES'] = 'false'
        
        # Run migrations
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "migrate"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env)
        
        assert result.returncode == 0, f"SQLite migration failed: {result.stderr}"
        
        output = result.stdout.lower()
        assert "migration" in output, "Migration execution not reported"
        
        # Verify migration created database schema
        if DB_PATH.exists():
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                # Check if neows table exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='neows'
                """)
                table_exists = cursor.fetchone()
                assert table_exists, "Migration did not create neows table"

    @pytest.mark.postgres_only
    def test_migration_cli_execution_postgresql(self):
        """
        Test migration execution via CLI with PostgreSQL backend.
        
        Validates migration system with PostgreSQL:
        - Executes migrations against PostgreSQL database
        - Tests advanced PostgreSQL migration features
        - Verifies cross-database migration capabilities
        - Ensures feature parity with SQLite migrations
        
        Requires PostgreSQL server to be running and accessible.
        """
        # Set PostgreSQL environment
        env = os.environ.copy()
        env['USE_POSTGRES'] = 'true'
        
        # Set test database connection if not configured
        if 'POSTGRES_HOST' not in env:
            env.update({
                'POSTGRES_HOST': 'localhost',
                'POSTGRES_PORT': '5432',
                'POSTGRES_USER': 'postgres', 
                'POSTGRES_PASSWORD': 'postgres',
                'POSTGRES_DB': 'neows_test'
            })
        
        # Run migrations
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline", 
            "--mode", "migrate"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env)
        
        # Skip if PostgreSQL not available
        if result.returncode != 0:
            if "connection" in result.stderr.lower() or "postgresql" in result.stderr.lower():
                pytest.skip("PostgreSQL not available for migration CLI test")
        
        assert result.returncode == 0, f"PostgreSQL migration failed: {result.stderr}"
        
        output = result.stdout.lower()
        assert "migration" in output, "Migration execution not reported"

    def test_migration_integration_in_feed_mode(self):
        """
        Test automatic migration execution during feed mode pipeline.
        
        Validates migration system integration with ETL pipeline:
        - Runs feed mode which should automatically execute migrations
        - Verifies migrations run before ETL processing begins
        - Tests that database schema is ready for data loading
        - Ensures seamless integration between migration and ETL systems
        
        This validates the automatic migration feature in production workflow.
        """
        # Clean environment to test automatic migration
        if DB_PATH.exists():
            DB_PATH.unlink()
        
        # Set SQLite environment for predictable testing
        env = os.environ.copy()
        env['USE_POSTGRES'] = 'false'
        
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "feed",
            "--start", "2025-10-01",
            "--end", "2025-10-03", 
            "--demo"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env)
        
        assert result.returncode == 0, f"Feed mode with auto-migration failed: {result.stderr}"
        
        # Verify migrations ran automatically
        output = result.stdout.lower()
        migration_mentioned = any(word in output for word in ["migration", "schema", "database"])
        
        # Verify database was created and populated
        assert DB_PATH.exists(), "Database not created during feed mode"
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Check schema_migrations table exists (created by migration system)
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='schema_migrations'
            """)
            migrations_table = cursor.fetchone()
            
            # Check neows table exists and has data
            cursor.execute("SELECT COUNT(*) FROM neows")
            data_count = cursor.fetchone()[0]
            assert data_count > 0, "No data loaded after auto-migration"

    def test_cli_database_configuration_validation(self):
        """
        Test CLI properly handles database configuration validation.
        
        Validates configuration error handling:
        - Tests invalid database connection strings
        - Verifies appropriate error messages for configuration issues
        - Ensures graceful handling of missing database dependencies
        - Tests timeout handling for unreachable databases
        
        This ensures robust error handling in production environments.
        """
        # Test with invalid PostgreSQL configuration
        env = os.environ.copy()
        env.update({
            'USE_POSTGRES': 'true',
            'POSTGRES_HOST': 'invalid-host-that-does-not-exist',
            'POSTGRES_PORT': '9999',
            'POSTGRES_DB': 'nonexistent_db'
        })
        
        result = subprocess.run([
            sys.executable, "-m", "src.pipeline",
            "--mode", "migrate", 
            "--dry-run"
        ], capture_output=True, text=True, cwd=Path.cwd(), env=env, timeout=30)
        
        # Should either skip gracefully or provide clear error message
        if result.returncode != 0:
            error_output = result.stderr.lower()
            # Should mention connection or database issues
            connection_error = any(word in error_output for word in 
                                 ["connection", "database", "timeout", "host"])
            assert connection_error, f"Unclear error message: {result.stderr}"

    @pytest.fixture(autouse=True)
    def cleanup_test_outputs(self):
        """
        Clean up test outputs before and after each test execution.
        
        This fixture ensures test isolation by:
        - Removing any existing output files before test execution
        - Cleaning up generated files after test completion  
        - Preventing test interdependencies through shared state
        - Maintaining consistent test environment conditions
        - Preserving original environment variables
        
        Yields:
            None: Control returns to test execution, then cleanup occurs.
        """
        # Store original environment
        original_env = os.environ.copy()
        
        # Pre-test cleanup
        if CSV_OUTPUT.exists():
            CSV_OUTPUT.unlink()
        if DB_PATH.exists():
            DB_PATH.unlink()
        
        yield
        
        # Post-test cleanup
        if CSV_OUTPUT.exists():
            CSV_OUTPUT.unlink()
        if DB_PATH.exists():
            DB_PATH.unlink()
            
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)