"""
Core dual database testing suite for SQLite and PostgreSQL parity validation.

This module contains comprehensive tests that validate identical functionality 
between SQLite and PostgreSQL database backends. Tests use parameterized fixtures
to run the same test logic against both database types, ensuring feature parity
and compatibility across the data pipeline system.

Test Classes:
    TestDatabaseParity: Core functionality validation across database types
    TestSchemaConsistency: Schema creation and structure validation
    TestDataOperations: Data insertion, retrieval, and manipulation testing
    TestConnectionManagement: Connection handling and health testing
    TestIndexPerformance: Index creation and performance validation
"""

import pytest
import time
from typing import Dict, List, Any, Optional
from sqlalchemy import text
from src.load import DatabaseManager


class TestDatabaseParity:
    """
    Test identical functionality across SQLite and PostgreSQL databases.
    
    This test class ensures that core database operations behave identically
    regardless of the underlying database backend. All tests use the 
    dual_database fixture to run against both SQLite and PostgreSQL.
    """

    def test_database_type_detection(self, dual_database):
        """
        Verify database type is correctly detected from connection URL.
        
        Tests that DatabaseManager correctly identifies whether it's connected
        to SQLite or PostgreSQL and sets internal flags appropriately.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test database type detection
        if "sqlite" in dual_database.database_url:
            assert not dual_database.is_postgres
            assert "sqlite" in dual_database.database_url.lower()
        else:
            assert dual_database.is_postgres
            assert "postgresql" in dual_database.database_url.lower()

    def test_connection_establishment(self, dual_database):
        """
        Test basic connection establishment for both database types.
        
        Verifies that database connections can be established and basic
        queries can be executed successfully.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test connection acquisition
        with dual_database.get_connection() as conn:
            assert conn is not None
            
            # Execute simple query appropriate for database type
            if dual_database.is_postgres:
                result = conn.execute(text("SELECT 1 as test_value, version()"))
            else:
                result = conn.execute(text("SELECT 1 as test_value, sqlite_version()"))
            
            row = result.fetchone()
            assert row is not None
            assert row[0] == 1

    def test_connection_health_testing(self, dual_database):
        """
        Verify connection health testing works identically across databases.
        
        Tests the connection health check functionality to ensure it provides
        consistent results and timing information for both database types.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test connection health check
        health_result = dual_database.test_connection_health(
            max_retries=2, 
            timeout_seconds=5.0
        )
        
        # Verify health check structure
        assert isinstance(health_result, dict)
        assert 'healthy' in health_result
        assert 'connection_time' in health_result
        assert 'query_time' in health_result
        
        # Connection should be healthy
        assert health_result['healthy'] is True
        
        # Times should be reasonable
        assert health_result['connection_time'] >= 0
        assert health_result['query_time'] >= 0
        assert health_result['connection_time'] < 2.0  # Should connect quickly
        assert health_result['query_time'] < 1.0      # Simple query should be fast

    def test_pool_status_reporting(self, dual_database):
        """
        Test pool status reporting across different database types.
        
        Verifies that pool status information is provided consistently,
        accounting for differences between PostgreSQL pooling and SQLite
        direct connections.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Get pool status information
        pool_status = dual_database.get_pool_status()
        
        # Verify basic structure
        assert isinstance(pool_status, dict)
        assert 'pool_type' in pool_status
        assert 'engine_url' in pool_status
        assert 'connection_stats' in pool_status
        
        # Verify database-specific details
        if dual_database.is_postgres:
            assert pool_status['pool_type'] == 'postgresql'
            assert 'pool_config' in pool_status
            assert isinstance(pool_status['pool_config'], dict)
        else:
            assert pool_status['pool_type'] == 'sqlite'
            assert 'database_file' in pool_status
            assert pool_status['status'] == "SQLite uses direct connections (no pooling required)"

    def test_warm_up_functionality(self, dual_database):
        """
        Test connection warm-up functionality for both database types.
        
        Verifies that connection warm-up works appropriately for PostgreSQL
        pooling and validates gracefully for SQLite direct connections.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test warm-up with small number of connections
        try:
            dual_database.warm_up_pool(num_connections=2)
            
            # Warm-up should complete without errors
            # For PostgreSQL: connections should be in pool
            # For SQLite: should validate connection and complete
            
            # Verify connection still works after warm-up
            health = dual_database.test_connection_health()
            assert health['healthy'] is True
            
        except Exception as e:
            pytest.fail(f"Connection warm-up failed: {e}")


class TestSchemaConsistency:
    """
    Test schema creation and structure consistency across database types.
    
    Validates that database schemas are created identically and function
    the same way regardless of the underlying database backend.
    """

    def test_schema_sql_generation(self, dual_database):
        """
        Test that appropriate schema SQL is generated for each database type.
        
        Verifies that get_schema_sql() returns database-appropriate DDL
        statements with correct data types and syntax.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Get schema SQL for database type
        schema_sql = dual_database.get_schema_sql()
        
        # Verify schema SQL is not empty
        assert schema_sql is not None
        assert len(schema_sql.strip()) > 0
        
        # Verify contains CREATE TABLE statement
        assert "CREATE TABLE" in schema_sql.upper()
        assert "neows" in schema_sql
        
        # Verify database-specific data types
        if dual_database.is_postgres:
            # PostgreSQL should use specific types
            assert "DATE" in schema_sql.upper()
            assert "BOOLEAN" in schema_sql.upper()
            assert "VARCHAR" in schema_sql.upper()
        else:
            # SQLite should use flexible types
            assert "TEXT" in schema_sql.upper() or "VARCHAR" in schema_sql.upper()
            # SQLite may use INTEGER for boolean representation

    def test_schema_creation_execution(self, dual_database):
        """
        Test actual schema creation in both database types.
        
        Executes the schema creation SQL and verifies that tables and
        indexes are created successfully.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Get and execute schema SQL
        schema_sql = dual_database.get_schema_sql()
        dual_database.execute_sql(schema_sql)
        
        # Verify table was created by attempting to query it
        try:
            result = dual_database.execute_sql("SELECT COUNT(*) FROM neows")
            count = result.fetchone()[0]
            assert count == 0  # Table should be empty but exist
            
        except Exception as e:
            pytest.fail(f"Schema creation failed - table not accessible: {e}")

    def test_table_structure_validation(self, dual_database):
        """
        Validate that created tables have consistent structure.
        
        Checks that the neows table has the expected columns and data types
        appropriate for the database backend.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Create schema first
        schema_sql = dual_database.get_schema_sql()
        dual_database.execute_sql(schema_sql)
        
        # Get table structure information
        if dual_database.is_postgres:
            # PostgreSQL: Query information_schema
            result = dual_database.execute_sql("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'neows'
                ORDER BY ordinal_position
            """)
        else:
            # SQLite: Use PRAGMA table_info
            result = dual_database.execute_sql("PRAGMA table_info(neows)")
        
        columns = result.fetchall()
        assert len(columns) >= 10, f"Expected at least 10 columns, got {len(columns)}"
        
        # Verify key columns exist (column names should be consistent)
        column_names = [col[0] if dual_database.is_postgres else col[1] for col in columns]
        expected_columns = [
            'id', 'name', 'close_approach_date', 'absolute_magnitude_h',
            'diameter_min_km', 'diameter_max_km', 'is_potentially_hazardous',
            'relative_velocity_kps', 'miss_distance_km', 'orbiting_body'
        ]
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Missing column: {expected_col}"

    def test_index_creation_consistency(self, dual_database):
        """
        Test that performance indexes are created consistently.
        
        Verifies that database indexes are created and function properly
        for both SQLite and PostgreSQL backends.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Create schema with indexes
        schema_sql = dual_database.get_schema_sql()
        dual_database.execute_sql(schema_sql)
        
        # Verify indexes exist by checking database metadata
        if dual_database.is_postgres:
            # PostgreSQL: Query pg_indexes
            result = dual_database.execute_sql("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'neows'
                ORDER BY indexname
            """)
        else:
            # SQLite: Query sqlite_master for indexes
            result = dual_database.execute_sql("""
                SELECT name FROM sqlite_master
                WHERE type = 'index' AND tbl_name = 'neows'
                ORDER BY name
            """)
        
        indexes = [row[0] for row in result.fetchall()]
        
        # Should have several performance indexes
        assert len(indexes) >= 4, f"Expected at least 4 indexes, got {len(indexes)}"
        
        # Check for expected index patterns
        index_patterns = ['id', 'date', 'hazardous', 'size']
        for pattern in index_patterns:
            pattern_found = any(pattern in idx.lower() for idx in indexes)
            assert pattern_found, f"No index found for pattern: {pattern}"


class TestDataOperations:
    """
    Test data insertion, retrieval, and manipulation across database types.
    
    Validates that data operations produce identical results regardless
    of the database backend being used.
    """

    def test_single_record_insertion(self, clean_database, sample_neo_data):
        """
        Test insertion of individual records across database types.
        
        Verifies that single record insertion works identically and produces
        the same results for both SQLite and PostgreSQL.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Use first sample record
        record = sample_neo_data[0]
        
        # Insert record using parameterized query
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                diameter_min_km, diameter_max_km, is_potentially_hazardous,
                relative_velocity_kps, miss_distance_km, orbiting_body
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :diameter_min_km, :diameter_max_km, :is_potentially_hazardous,
                :relative_velocity_kps, :miss_distance_km, :orbiting_body
            )
        """
        
        # Execute insertion
        clean_database.execute_sql(insert_sql, record)
        
        # Verify record was inserted
        result = clean_database.execute_sql(
            "SELECT COUNT(*) FROM neows WHERE id = :id", 
            {'id': record['id']}
        )
        count = result.fetchone()[0]
        assert count == 1, f"Record not inserted correctly, count: {count}"
        
        # Verify record data integrity
        select_sql = """
            SELECT id, name, is_potentially_hazardous, absolute_magnitude_h
            FROM neows WHERE id = :id
        """
        result = clean_database.execute_sql(select_sql, {'id': record['id']})
        row = result.fetchone()
        
        assert row is not None
        assert row[0] == record['id']
        assert row[1] == record['name']
        assert bool(row[2]) == record['is_potentially_hazardous']
        assert abs(float(row[3]) - record['absolute_magnitude_h']) < 0.01

    def test_batch_record_insertion(self, clean_database, sample_neo_data):
        """
        Test batch insertion of multiple records.
        
        Verifies that multiple records can be inserted and retrieved
        consistently across database types.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Insert all sample records
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                diameter_min_km, diameter_max_km, is_potentially_hazardous,
                relative_velocity_kps, miss_distance_km, orbiting_body
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :diameter_min_km, :diameter_max_km, :is_potentially_hazardous,
                :relative_velocity_kps, :miss_distance_km, :orbiting_body
            )
        """
        
        # Insert each record
        for record in sample_neo_data:
            clean_database.execute_sql(insert_sql, record)
        
        # Verify total count
        result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
        total_count = result.fetchone()[0]
        assert total_count == len(sample_neo_data)
        
        # Verify data integrity for all records
        result = clean_database.execute_sql("""
            SELECT id, is_potentially_hazardous 
            FROM neows 
            ORDER BY id
        """)
        
        rows = result.fetchall()
        assert len(rows) == len(sample_neo_data)
        
        # Check that hazardous flag is preserved correctly
        for i, row in enumerate(rows):
            original_record = next(r for r in sample_neo_data if r['id'] == row[0])
            assert bool(row[1]) == original_record['is_potentially_hazardous']

    def test_data_type_handling(self, clean_database):
        """
        Test handling of different data types across databases.
        
        Verifies that various data types (strings, numbers, booleans, dates)
        are handled consistently between SQLite and PostgreSQL.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        # Test record with various data type edge cases
        test_record = {
            'id': 'TYPE_TEST_001',
            'name': 'Data Type Test Asteroid (Special chars: éñ中文)',
            'close_approach_date': '2025-12-31',
            'absolute_magnitude_h': 25.789,  # High precision float
            'diameter_min_km': 0.001,        # Very small float
            'diameter_max_km': 999.999,      # Large float
            'is_potentially_hazardous': True,
            'relative_velocity_kps': 0.0,    # Zero value
            'miss_distance_km': 1234567890.123,  # Very large number
            'orbiting_body': 'Earth'
        }
        
        # Insert test record
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                diameter_min_km, diameter_max_km, is_potentially_hazardous,
                relative_velocity_kps, miss_distance_km, orbiting_body
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :diameter_min_km, :diameter_max_km, :is_potentially_hazardous,
                :relative_velocity_kps, :miss_distance_km, :orbiting_body
            )
        """
        
        clean_database.execute_sql(insert_sql, test_record)
        
        # Retrieve and verify data types
        select_sql = """
            SELECT * FROM neows WHERE id = :id
        """
        result = clean_database.execute_sql(select_sql, {'id': test_record['id']})
        row = result.fetchone()
        
        assert row is not None
        
        # Validate specific data type preservation
        # Note: SQLAlchemy Row objects can be accessed by column name directly
        assert row.name == test_record['name']  # Unicode handling
        assert bool(row.is_potentially_hazardous) == test_record['is_potentially_hazardous']
        assert float(row.relative_velocity_kps) == test_record['relative_velocity_kps']  # Zero handling
        assert abs(float(row.absolute_magnitude_h) - test_record['absolute_magnitude_h']) < 0.001

    def test_date_handling_consistency(self, clean_database):
        """
        Test date handling across different database types.
        
        Verifies that date storage and retrieval works consistently,
        accounting for differences in date type handling between databases.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        # Test various date formats
        date_test_cases = [
            ('DATE_TEST_001', '2025-01-01'),
            ('DATE_TEST_002', '2025-12-31'),
            ('DATE_TEST_003', '2030-06-15'),
        ]
        
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                is_potentially_hazardous
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :is_potentially_hazardous
            )
        """
        
        # Insert date test records
        for test_id, test_date in date_test_cases:
            record = {
                'id': test_id,
                'name': f'Date Test {test_id}',
                'close_approach_date': test_date,
                'absolute_magnitude_h': 20.0,
                'is_potentially_hazardous': False
            }
            clean_database.execute_sql(insert_sql, record)
        
        # Retrieve and validate dates
        result = clean_database.execute_sql("""
            SELECT id, close_approach_date 
            FROM neows 
            WHERE id LIKE 'DATE_TEST_%'
            ORDER BY id
        """)
        
        rows = result.fetchall()
        assert len(rows) == len(date_test_cases)
        
        for i, (test_id, expected_date) in enumerate(date_test_cases):
            row_id, row_date = rows[i]
            assert row_id == test_id
            
            # Convert date to string for comparison (handles different return types)
            if hasattr(row_date, 'strftime'):
                date_str = row_date.strftime('%Y-%m-%d')
            else:
                date_str = str(row_date)
            
            assert date_str == expected_date


class TestErrorHandling:
    """
    Test error handling consistency across database types.
    
    Validates that error conditions are handled similarly and produce
    consistent behavior regardless of the database backend.
    """

    def test_duplicate_key_handling(self, clean_database, sample_neo_data):
        """
        Test duplicate primary key constraint handling.
        
        Verifies that duplicate key violations are handled consistently
        across database types.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        record = sample_neo_data[0].copy()
        
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                is_potentially_hazardous
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :is_potentially_hazardous
            )
        """
        
        # Insert record first time - should succeed
        clean_database.execute_sql(insert_sql, record)
        
        # Attempt to insert same primary key (id + close_approach_date) - should fail
        with pytest.raises(Exception):  # Both databases should raise some form of constraint violation
            clean_database.execute_sql(insert_sql, record)

    def test_invalid_sql_handling(self, dual_database):
        """
        Test handling of invalid SQL statements.
        
        Verifies that invalid SQL produces consistent error behavior
        across database types.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test invalid SQL syntax
        with pytest.raises(Exception):
            dual_database.execute_sql("SELECT * FROM nonexistent_table_xyz")
        
        # Test malformed SQL
        with pytest.raises(Exception):
            dual_database.execute_sql("INVALID SQL STATEMENT HERE")

    def test_connection_timeout_handling(self, dual_database):
        """
        Test connection timeout and recovery behavior.
        
        Verifies that connection timeouts are handled gracefully and
        connections can be re-established.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test connection health with very short timeout
        health_result = dual_database.test_connection_health(
            max_retries=1,
            timeout_seconds=0.001  # Very short timeout
        )
        
        # Should either succeed quickly or handle timeout gracefully
        assert isinstance(health_result, dict)
        assert 'healthy' in health_result
        
        # Follow up with normal health check to verify recovery
        normal_health = dual_database.test_connection_health()
        assert normal_health['healthy'] is True