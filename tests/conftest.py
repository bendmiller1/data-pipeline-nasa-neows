"""
pytest configuration for dual database testing.

This module provides test fixtures and configuration for testing both SQLite 
and PostgreSQL database backends. It includes parameterized fixtures that 
enable running the same tests against both database types to ensure feature 
parity and compatibility.

Fixtures:
    sqlite_test_db: Temporary SQLite database for isolated testing
    postgres_test_db: PostgreSQL test database (requires running PostgreSQL)
    dual_database: Parameterized fixture for both database types
    sample_neo_data: Sample NEO data for testing database operations
"""

import pytest
import tempfile
import os
from pathlib import Path
from typing import Dict, List, Any
from src.load import DatabaseManager


@pytest.fixture(scope="session")
def sqlite_test_db():
    """
    Create a temporary SQLite database for testing.
    
    Creates an isolated SQLite database file that is automatically cleaned up
    after the test session. Each test session gets a fresh database to ensure
    test isolation.
    
    Yields:
        DatabaseManager: Configured SQLite database manager instance
        
    Note:
        The database file is automatically deleted after the test session ends.
    """
    # Create temporary file for SQLite database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = Path(f.name)
    
    # Create database manager with SQLite URL
    db_url = f"sqlite:///{db_path}"
    db_manager = DatabaseManager(db_url)
    
    yield db_manager
    
    # Cleanup: Remove temporary database file
    try:
        if db_path.exists():
            db_path.unlink()
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture(scope="session")
def postgres_test_db():
    """
    Create PostgreSQL test database connection.
    
    Connects to a PostgreSQL test database. Requires PostgreSQL server to be
    running and accessible. Uses environment variables for configuration or
    defaults to standard test database settings.
    
    Returns:
        DatabaseManager: Configured PostgreSQL database manager instance
        
    Environment Variables:
        POSTGRES_TEST_HOST: PostgreSQL host (default: localhost)
        POSTGRES_TEST_PORT: PostgreSQL port (default: 5432)
        POSTGRES_TEST_USER: PostgreSQL user (default: postgres)
        POSTGRES_TEST_PASSWORD: PostgreSQL password (default: postgres)
        POSTGRES_TEST_DB: PostgreSQL database name (default: neows_test)
        
    Note:
        This fixture assumes PostgreSQL is available. Tests using this fixture
        will be skipped if PostgreSQL connection fails.
    """
    # Get PostgreSQL test configuration from environment
    host = os.getenv("POSTGRES_TEST_HOST", "localhost")
    port = os.getenv("POSTGRES_TEST_PORT", "5432")
    user = os.getenv("POSTGRES_TEST_USER", "postgres")
    password = os.getenv("POSTGRES_TEST_PASSWORD", "postgres")
    database = os.getenv("POSTGRES_TEST_DB", "neows_test")
    
    # Construct PostgreSQL connection URL
    if password:
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        db_url = f"postgresql://{user}@{host}:{port}/{database}"
    
    try:
        db_manager = DatabaseManager(db_url)
        
        # Test connection to ensure PostgreSQL is available
        health = db_manager.test_connection_health(timeout_seconds=2.0)
        if not health.get('healthy', False):
            pytest.skip("PostgreSQL test database not available")
            
        return db_manager
        
    except Exception as e:
        pytest.skip(f"PostgreSQL test database not available: {e}")


@pytest.fixture(params=["sqlite", "postgres"])
def dual_database(request):
    """
    Parameterized fixture providing both SQLite and PostgreSQL databases.
    
    This fixture enables running the same test against both database types
    to ensure feature parity and compatibility. Tests using this fixture
    will run twice: once with SQLite and once with PostgreSQL.
    
    Args:
        request: pytest request object containing parameter information
        
    Returns:
        DatabaseManager: Database manager for the current test parameter
        
    Usage:
        def test_database_feature(dual_database):
            # This test runs twice: once with SQLite, once with PostgreSQL
            result = dual_database.execute_sql("SELECT 1")
            assert result.fetchone()[0] == 1
    """
    if request.param == "sqlite":
        # Create SQLite database inline
        import tempfile
        from pathlib import Path
        from src.load import DatabaseManager
        
        # Create temporary file for SQLite database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = Path(f.name)
        
        # Create database manager with SQLite URL
        db_url = f"sqlite:///{db_path}"
        db_manager = DatabaseManager(db_url)
        
        # Store cleanup info for later
        request.addfinalizer(lambda: db_path.unlink(missing_ok=True))
        
        return db_manager
        
    elif request.param == "postgres":
        # Create PostgreSQL database inline with skip logic
        import os
        from src.load import DatabaseManager
        
        # Get PostgreSQL test configuration from environment
        host = os.getenv("POSTGRES_TEST_HOST", "localhost")
        port = os.getenv("POSTGRES_TEST_PORT", "5432")
        user = os.getenv("POSTGRES_TEST_USER", "postgres")
        password = os.getenv("POSTGRES_TEST_PASSWORD", "postgres")
        database = os.getenv("POSTGRES_TEST_DB", "neows_test")
        
        # Construct PostgreSQL connection URL
        if password:
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        else:
            db_url = f"postgresql://{user}@{host}:{port}/{database}"
        
        try:
            db_manager = DatabaseManager(db_url)
            
            # Test connection to ensure PostgreSQL is available
            health = db_manager.test_connection_health(timeout_seconds=2.0)
            if not health.get('healthy', False):
                pytest.skip("PostgreSQL test database not available")
                
            return db_manager
            
        except Exception as e:
            pytest.skip(f"PostgreSQL test database not available: {e}")
    else:
        pytest.fail(f"Unknown database parameter: {request.param}")


@pytest.fixture
def sample_neo_data():
    """
    Provide sample NEO data for testing database operations.
    
    Returns a list of sample Near Earth Object records that can be used
    for testing database insertion, querying, and validation operations.
    Data includes various data types and edge cases to test database
    compatibility.
    
    Returns:
        List[Dict[str, Any]]: Sample NEO records with complete field set
        
    Usage:
        def test_data_insertion(dual_database, sample_neo_data):
            for record in sample_neo_data:
                # Insert record into database
                pass
    """
    return [
        {
            'id': '2022001',
            'name': '(2022 AA1) Test Asteroid Alpha',
            'close_approach_date': '2025-01-15',
            'absolute_magnitude_h': 18.2,
            'diameter_min_km': 0.045,
            'diameter_max_km': 0.101,
            'is_potentially_hazardous': False,
            'relative_velocity_kps': 12.34,
            'miss_distance_km': 1234567.89,
            'orbiting_body': 'Earth'
        },
        {
            'id': '2022002',
            'name': '(2022 BB2) Test Asteroid Beta',
            'close_approach_date': '2025-02-20',
            'absolute_magnitude_h': 22.8,
            'diameter_min_km': 0.008,
            'diameter_max_km': 0.018,
            'is_potentially_hazardous': True,
            'relative_velocity_kps': 8.76,
            'miss_distance_km': 987654.32,
            'orbiting_body': 'Earth'
        },
        {
            'id': '2022003',
            'name': '(2022 CC3) Test Asteroid Gamma',
            'close_approach_date': '2025-03-10',
            'absolute_magnitude_h': 16.5,
            'diameter_min_km': 0.125,
            'diameter_max_km': 0.279,
            'is_potentially_hazardous': False,
            'relative_velocity_kps': 15.67,
            'miss_distance_km': 2468135.79,
            'orbiting_body': 'Earth'
        },
        {
            'id': '2022004',
            'name': '(2022 DD4) Test Asteroid Delta',
            'close_approach_date': '2025-04-05',
            'absolute_magnitude_h': 20.1,
            'diameter_min_km': 0.025,
            'diameter_max_km': 0.056,
            'is_potentially_hazardous': True,
            'relative_velocity_kps': 9.88,
            'miss_distance_km': 1357924.68,
            'orbiting_body': 'Earth'
        },
        {
            'id': '2022005',
            'name': '(2022 EE5) Test Asteroid Epsilon',
            'close_approach_date': '2025-05-12',
            'absolute_magnitude_h': 19.3,
            'diameter_min_km': 0.035,
            'diameter_max_km': 0.078,
            'is_potentially_hazardous': False,
            'relative_velocity_kps': 11.22,
            'miss_distance_km': 3692581.47,
            'orbiting_body': 'Earth'
        }
    ]


@pytest.fixture
def clean_database(dual_database):
    """
    Provide a clean database with schema ready for testing.
    
    Sets up the database schema and ensures a clean state before each test.
    Automatically cleans up after the test completes.
    
    Args:
        dual_database: Database manager fixture (SQLite or PostgreSQL)
        
    Yields:
        DatabaseManager: Database manager with clean schema
        
    Usage:
        def test_with_clean_db(clean_database):
            # Database has schema but no data
            result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
            assert result.fetchone()[0] == 0
    """
    # Setup: Create schema
    schema_sql = dual_database.get_schema_sql()
    dual_database.execute_sql(schema_sql)
    
    yield dual_database
    
    # Teardown: Clean up data (but keep schema for other tests)
    try:
        dual_database.execute_sql("DELETE FROM neows")
        
        # Also clean up schema_migrations table if it exists
        if dual_database.is_postgres:
            dual_database.execute_sql(
                "DELETE FROM schema_migrations WHERE version LIKE 'test_%'"
            )
        else:
            dual_database.execute_sql(
                "DELETE FROM schema_migrations WHERE version LIKE 'test_%'"
            )
    except Exception:
        # Ignore cleanup errors (table might not exist)
        pass


def pytest_configure(config):
    """
    Configure pytest for dual database testing.
    
    Adds custom markers for database-specific tests and configures
    test collection options.
    
    Args:
        config: pytest configuration object
    """
    config.addinivalue_line(
        "markers", "sqlite_only: mark test to run only with SQLite"
    )
    config.addinivalue_line(
        "markers", "postgres_only: mark test to run only with PostgreSQL"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance benchmark"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )


def pytest_runtest_setup(item):
    """
    Setup hook for individual test items.
    
    Handles test skipping based on database availability and markers.
    
    Args:
        item: pytest test item
    """
    # Skip PostgreSQL-only tests if PostgreSQL is not available
    postgres_only = item.get_closest_marker("postgres_only")
    if postgres_only:
        try:
            # Try to create PostgreSQL connection
            host = os.getenv("POSTGRES_TEST_HOST", "localhost")
            port = os.getenv("POSTGRES_TEST_PORT", "5432")
            user = os.getenv("POSTGRES_TEST_USER", "postgres")
            password = os.getenv("POSTGRES_TEST_PASSWORD", "postgres")
            database = os.getenv("POSTGRES_TEST_DB", "neows_test")
            
            if password:
                db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            else:
                db_url = f"postgresql://{user}@{host}:{port}/{database}"
                
            db_manager = DatabaseManager(db_url)
            health = db_manager.test_connection_health(timeout_seconds=2.0)
            if not health.get('healthy', False):
                pytest.skip("PostgreSQL not available for postgres_only test")
                
        except Exception:
            pytest.skip("PostgreSQL not available for postgres_only test")