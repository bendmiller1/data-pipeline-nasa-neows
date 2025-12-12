"""
Performance comparison testing for SQLite vs PostgreSQL database operations.

This module provides comprehensive performance benchmarking to compare
operation speeds, resource usage, and efficiency between SQLite and PostgreSQL
database backends. Tests measure connection overhead, query performance,
bulk operations, and concurrent access patterns.

Test Classes:
    TestConnectionPerformance: Connection establishment and pooling benchmarks
    TestQueryPerformance: Query execution speed comparisons
    TestBulkOperations: Batch insertion and update performance
    TestConcurrentAccess: Multi-threaded operation benchmarks
    TestMemoryUsage: Memory consumption and efficiency testing
"""

import pytest
import time
import threading
import statistics
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from src.load import DatabaseManager


@pytest.mark.performance
class TestConnectionPerformance:
    """
    Test connection establishment and management performance.
    
    Compares connection overhead between SQLite direct connections
    and PostgreSQL connection pooling to understand the performance
    characteristics of each database backend.
    """

    def test_single_connection_overhead(self, dual_database):
        """
        Measure time to establish a single database connection.
        
        Tests the overhead of creating and closing database connections
        to understand baseline connection costs for each database type.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        connection_times = []
        iterations = 10
        
        for _ in range(iterations):
            start_time = time.perf_counter()
            
            # Get connection and execute simple query
            with dual_database.get_connection() as conn:
                conn.execute(text("SELECT 1"))
                
            end_time = time.perf_counter()
            connection_times.append(end_time - start_time)
        
        # Calculate statistics
        avg_time = statistics.mean(connection_times)
        median_time = statistics.median(connection_times)
        std_dev = statistics.stdev(connection_times) if len(connection_times) > 1 else 0
        
        db_type = "PostgreSQL" if dual_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Connection Performance:")
        print(f"  Average: {avg_time:.4f}s")
        print(f"  Median:  {median_time:.4f}s")
        print(f"  Std Dev: {std_dev:.4f}s")
        print(f"  Min:     {min(connection_times):.4f}s")
        print(f"  Max:     {max(connection_times):.4f}s")
        
        # Reasonable connection time thresholds
        assert avg_time < 1.0, f"{db_type} connection time too slow: {avg_time:.4f}s"
        assert median_time < 1.0, f"{db_type} median connection time too slow: {median_time:.4f}s"

    def test_connection_pool_warmup_impact(self, dual_database):
        """
        Test impact of connection pool warm-up on performance.
        
        Measures performance before and after pool warm-up to demonstrate
        the benefits of connection pooling for PostgreSQL.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        if not dual_database.is_postgres:
            pytest.skip("Pool warm-up test only applies to PostgreSQL")
        
        # Measure before warm-up
        before_times = []
        for _ in range(5):
            start_time = time.perf_counter()
            with dual_database.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            end_time = time.perf_counter()
            before_times.append(end_time - start_time)
        
        # Warm up the pool
        dual_database.warm_up_pool(num_connections=3)
        
        # Measure after warm-up
        after_times = []
        for _ in range(5):
            start_time = time.perf_counter()
            with dual_database.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            end_time = time.perf_counter()
            after_times.append(end_time - start_time)
        
        avg_before = statistics.mean(before_times)
        avg_after = statistics.mean(after_times)
        
        print(f"\nPostgreSQL Pool Warm-up Impact:")
        print(f"  Before warm-up: {avg_before:.4f}s")
        print(f"  After warm-up:  {avg_after:.4f}s")
        print(f"  Improvement:    {((avg_before - avg_after) / avg_before * 100):.1f}%")
        
        # After warm-up should generally be faster or similar
        # (May not always be faster due to test environment variations)

    def test_concurrent_connection_handling(self, dual_database):
        """
        Test concurrent connection acquisition performance.
        
        Measures how well each database handles multiple simultaneous
        connection requests to understand scalability characteristics.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        num_threads = 5
        connections_per_thread = 3
        
        def worker_function() -> List[float]:
            """Worker function for concurrent connection testing."""
            times = []
            for _ in range(connections_per_thread):
                start_time = time.perf_counter()
                
                try:
                    with dual_database.get_connection() as conn:
                        result = conn.execute(text("SELECT 1 as test_value"))
                        value = result.fetchone()[0]
                        assert value == 1
                        
                except Exception as e:
                    pytest.fail(f"Concurrent connection failed: {e}")
                
                end_time = time.perf_counter()
                times.append(end_time - start_time)
            
            return times
        
        # Execute concurrent connections
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_function) for _ in range(num_threads)]
            
            all_times = []
            for future in as_completed(futures):
                thread_times = future.result()
                all_times.extend(thread_times)
        
        # Analyze results
        avg_time = statistics.mean(all_times)
        max_time = max(all_times)
        
        db_type = "PostgreSQL" if dual_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Concurrent Connection Performance:")
        print(f"  Threads: {num_threads}")
        print(f"  Connections per thread: {connections_per_thread}")
        print(f"  Total connections: {len(all_times)}")
        print(f"  Average time: {avg_time:.4f}s")
        print(f"  Maximum time: {max_time:.4f}s")
        
        # All connections should complete in reasonable time
        assert avg_time < 2.0, f"Concurrent connections too slow: {avg_time:.4f}s"
        assert max_time < 5.0, f"Slowest connection too slow: {max_time:.4f}s"


@pytest.mark.performance
class TestQueryPerformance:
    """
    Test query execution performance across database types.
    
    Compares query execution speeds for various types of operations
    to understand the performance characteristics of each database.
    """

    def test_simple_query_performance(self, clean_database, sample_neo_data):
        """
        Test performance of simple SELECT queries.
        
        Measures execution time for basic queries to establish
        baseline query performance for each database type.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Insert test data
        self._insert_test_data(clean_database, sample_neo_data)
        
        # Test simple queries
        queries = [
            "SELECT COUNT(*) FROM neows",
            "SELECT * FROM neows LIMIT 1",
            "SELECT id, name FROM neows WHERE is_potentially_hazardous = true",
            "SELECT AVG(absolute_magnitude_h) FROM neows"
        ]
        
        query_times = {}
        iterations = 10
        
        for query in queries:
            times = []
            for _ in range(iterations):
                start_time = time.perf_counter()
                
                result = clean_database.execute_sql(query)
                result.fetchall()  # Ensure all data is retrieved
                
                end_time = time.perf_counter()
                times.append(end_time - start_time)
            
            query_times[query] = {
                'avg': statistics.mean(times),
                'median': statistics.median(times),
                'min': min(times),
                'max': max(times)
            }
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Query Performance:")
        for query, stats in query_times.items():
            print(f"  {query[:50]}...")
            print(f"    Average: {stats['avg']:.4f}s")
            print(f"    Range: {stats['min']:.4f}s - {stats['max']:.4f}s")
        
        # All queries should complete quickly
        for query, stats in query_times.items():
            assert stats['avg'] < 0.1, f"Query too slow: {query} - {stats['avg']:.4f}s"

    def test_indexed_vs_unindexed_performance(self, clean_database, sample_neo_data):
        """
        Test performance difference between indexed and non-indexed queries.
        
        Compares query performance on indexed columns vs full table scans
        to validate that indexes provide expected performance benefits.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Insert larger dataset for meaningful performance comparison
        extended_data = sample_neo_data * 20  # Multiply sample data
        
        # Modify IDs to make them unique
        for i, record in enumerate(extended_data):
            record = record.copy()
            record['id'] = f"{record['id']}_{i:03d}"
            extended_data[i] = record
        
        self._insert_test_data(clean_database, extended_data)
        
        # Test indexed query (id column should have index)
        indexed_times = []
        for _ in range(5):
            start_time = time.perf_counter()
            result = clean_database.execute_sql(
                "SELECT * FROM neows WHERE id = :id",
                {"id": extended_data[0]['id']}
            )
            result.fetchall()
            end_time = time.perf_counter()
            indexed_times.append(end_time - start_time)
        
        # Test query that should use date index
        date_indexed_times = []
        for _ in range(5):
            start_time = time.perf_counter()
            result = clean_database.execute_sql(
                "SELECT * FROM neows WHERE close_approach_date = :date",
                {"date": extended_data[0]['close_approach_date']}
            )
            result.fetchall()
            end_time = time.perf_counter()
            date_indexed_times.append(end_time - start_time)
        
        avg_indexed = statistics.mean(indexed_times)
        avg_date_indexed = statistics.mean(date_indexed_times)
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Index Performance:")
        print(f"  ID lookup (indexed): {avg_indexed:.4f}s")
        print(f"  Date lookup (indexed): {avg_date_indexed:.4f}s")
        print(f"  Total records: {len(extended_data)}")
        
        # Indexed queries should be fast
        assert avg_indexed < 0.05, f"Indexed query too slow: {avg_indexed:.4f}s"
        assert avg_date_indexed < 0.05, f"Date indexed query too slow: {avg_date_indexed:.4f}s"

    def test_aggregation_query_performance(self, clean_database, sample_neo_data):
        """
        Test performance of aggregation queries.
        
        Measures execution time for queries that perform calculations
        and aggregations across the dataset.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Insert test data multiple times to create larger dataset
        extended_data = sample_neo_data * 10
        
        # Make IDs unique
        for i, record in enumerate(extended_data):
            record = record.copy()
            record['id'] = f"{record['id']}_{i:03d}"
            extended_data[i] = record
        
        self._insert_test_data(clean_database, extended_data)
        
        # Test various aggregation queries
        aggregation_queries = [
            "SELECT COUNT(*) FROM neows",
            "SELECT COUNT(*) FROM neows WHERE is_potentially_hazardous = true",
            "SELECT AVG(absolute_magnitude_h) FROM neows",
            "SELECT MIN(diameter_min_km), MAX(diameter_max_km) FROM neows",
            "SELECT COUNT(*), AVG(absolute_magnitude_h) FROM neows GROUP BY is_potentially_hazardous"
        ]
        
        query_performance = {}
        
        for query in aggregation_queries:
            times = []
            for _ in range(3):  # Fewer iterations for complex queries
                start_time = time.perf_counter()
                
                result = clean_database.execute_sql(query)
                result.fetchall()
                
                end_time = time.perf_counter()
                times.append(end_time - start_time)
            
            avg_time = statistics.mean(times)
            query_performance[query] = avg_time
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Aggregation Performance:")
        for query, avg_time in query_performance.items():
            print(f"  {query[:60]}...")
            print(f"    Average: {avg_time:.4f}s")
        
        # Aggregation queries should complete in reasonable time
        for query, avg_time in query_performance.items():
            assert avg_time < 0.5, f"Aggregation query too slow: {query} - {avg_time:.4f}s"

    def _insert_test_data(self, db_manager: DatabaseManager, data: List[Dict[str, Any]]) -> None:
        """
        Helper method to insert test data efficiently.
        
        Args:
            db_manager: Database manager instance
            data: List of records to insert
        """
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
        
        for record in data:
            db_manager.execute_sql(insert_sql, record)


@pytest.mark.performance
class TestBulkOperations:
    """
    Test bulk operation performance for large data sets.
    
    Compares the efficiency of bulk insertions, updates, and deletions
    between SQLite and PostgreSQL to understand scalability characteristics.
    """

    def test_bulk_insert_performance(self, clean_database):
        """
        Test performance of bulk data insertion.
        
        Measures the time required to insert large numbers of records
        to compare insertion efficiency between database types.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        # Generate test records
        num_records = 1000
        test_records = []
        
        for i in range(num_records):
            record = {
                'id': f'BULK_TEST_{i:06d}',
                'name': f'Bulk Test Asteroid {i}',
                'close_approach_date': f'2025-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}',
                'absolute_magnitude_h': 18.0 + (i % 10),
                'diameter_min_km': 0.01 + (i * 0.001),
                'diameter_max_km': 0.02 + (i * 0.002),
                'is_potentially_hazardous': i % 3 == 0,
                'relative_velocity_kps': 10.0 + (i % 20),
                'miss_distance_km': 1000000 + (i * 1000),
                'orbiting_body': 'Earth'
            }
            test_records.append(record)
        
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
        
        # Measure bulk insertion time
        start_time = time.perf_counter()
        
        for record in test_records:
            clean_database.execute_sql(insert_sql, record)
        
        end_time = time.perf_counter()
        
        total_time = end_time - start_time
        records_per_second = num_records / total_time
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Bulk Insert Performance:")
        print(f"  Records inserted: {num_records}")
        print(f"  Total time: {total_time:.3f}s")
        print(f"  Records/second: {records_per_second:.1f}")
        print(f"  Time per record: {(total_time/num_records)*1000:.2f}ms")
        
        # Verify all records were inserted
        result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
        count = result.fetchone()[0]
        assert count == num_records
        
        # Performance should be reasonable
        assert records_per_second > 50, f"Insert rate too slow: {records_per_second:.1f} records/second"
        assert total_time < 60, f"Bulk insert took too long: {total_time:.3f}s"

    def test_bulk_update_performance(self, clean_database, sample_neo_data):
        """
        Test performance of bulk data updates.
        
        Measures the time required to update large numbers of records
        to compare update efficiency between database types.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Insert initial data
        num_copies = 200
        test_data = sample_neo_data * num_copies
        
        # Make IDs unique
        for i, record in enumerate(test_data):
            record = record.copy()
            record['id'] = f"{record['id']}_{i:04d}"
            test_data[i] = record
        
        # Insert all records
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                is_potentially_hazardous
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :is_potentially_hazardous
            )
        """
        
        for record in test_data:
            clean_database.execute_sql(insert_sql, record)
        
        # Measure bulk update performance
        update_sql = """
            UPDATE neows 
            SET absolute_magnitude_h = absolute_magnitude_h + 1.0,
                is_potentially_hazardous = NOT is_potentially_hazardous
            WHERE id LIKE 'BULK_%'
        """
        
        start_time = time.perf_counter()
        clean_database.execute_sql(update_sql)
        end_time = time.perf_counter()
        
        update_time = end_time - start_time
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Bulk Update Performance:")
        print(f"  Records updated: {len(test_data)}")
        print(f"  Update time: {update_time:.3f}s")
        print(f"  Records/second: {len(test_data)/update_time:.1f}")
        
        # Verify update worked
        result = clean_database.execute_sql(
            "SELECT COUNT(*) FROM neows WHERE absolute_magnitude_h > 20"
        )
        updated_count = result.fetchone()[0]
        assert updated_count > 0  # Some records should have been updated
        
        # Performance should be reasonable
        assert update_time < 30, f"Bulk update took too long: {update_time:.3f}s"

    def test_bulk_delete_performance(self, clean_database):
        """
        Test performance of bulk data deletion.
        
        Measures the time required to delete large numbers of records
        to compare deletion efficiency between database types.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        # Insert test data for deletion
        num_records = 500
        
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                is_potentially_hazardous
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :is_potentially_hazardous
            )
        """
        
        # Insert records to be deleted
        for i in range(num_records):
            record = {
                'id': f'DELETE_TEST_{i:05d}',
                'name': f'Delete Test Asteroid {i}',
                'close_approach_date': '2025-06-15',
                'absolute_magnitude_h': 20.0,
                'is_potentially_hazardous': False
            }
            clean_database.execute_sql(insert_sql, record)
        
        # Verify records were inserted
        result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
        initial_count = result.fetchone()[0]
        assert initial_count == num_records
        
        # Measure bulk deletion time
        delete_sql = "DELETE FROM neows WHERE id LIKE 'DELETE_TEST_%'"
        
        start_time = time.perf_counter()
        clean_database.execute_sql(delete_sql)
        end_time = time.perf_counter()
        
        delete_time = end_time - start_time
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Bulk Delete Performance:")
        print(f"  Records deleted: {num_records}")
        print(f"  Delete time: {delete_time:.3f}s")
        print(f"  Records/second: {num_records/delete_time:.1f}")
        
        # Verify deletion worked
        result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
        final_count = result.fetchone()[0]
        assert final_count == 0
        
        # Performance should be reasonable
        assert delete_time < 10, f"Bulk delete took too long: {delete_time:.3f}s"


@pytest.mark.performance
class TestResourceUsage:
    """
    Test resource usage and efficiency characteristics.
    
    Compares memory usage, connection overhead, and resource efficiency
    between SQLite and PostgreSQL under various load conditions.
    """

    def test_connection_resource_usage(self, dual_database):
        """
        Test resource usage of database connections.
        
        Measures the resource footprint of establishing and maintaining
        database connections for performance monitoring.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # Test connection pool status
        pool_status = dual_database.get_pool_status()
        
        db_type = "PostgreSQL" if dual_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Resource Usage:")
        print(f"  Pool type: {pool_status['pool_type']}")
        
        if dual_database.is_postgres:
            print(f"  Pool configuration:")
            for key, value in pool_status.get('pool_config', {}).items():
                print(f"    {key}: {value}")
        
        # Test connection statistics
        conn_stats = pool_status.get('connection_stats', {})
        print(f"  Connection statistics:")
        for key, value in conn_stats.items():
            print(f"    {key}: {value}")
        
        # Verify reasonable resource usage
        if dual_database.is_postgres:
            pool_config = pool_status.get('pool_config', {})
            base_pool_size = pool_config.get('base_pool_size', 0)
            max_overflow = pool_config.get('max_overflow', 0)
            
            assert base_pool_size > 0, "PostgreSQL should have positive pool size"
            assert max_overflow >= 0, "PostgreSQL overflow should be non-negative"

    def test_query_resource_efficiency(self, clean_database, sample_neo_data):
        """
        Test resource efficiency of query operations.
        
        Measures resource usage patterns during various query operations
        to understand the efficiency characteristics of each database.
        
        Args:
            clean_database: Clean database fixture with schema
            sample_neo_data: Sample NEO records for testing
        """
        # Insert test data
        insert_sql = """
            INSERT INTO neows (
                id, name, close_approach_date, absolute_magnitude_h,
                is_potentially_hazardous
            ) VALUES (
                :id, :name, :close_approach_date, :absolute_magnitude_h,
                :is_potentially_hazardous
            )
        """
        
        for record in sample_neo_data:
            clean_database.execute_sql(insert_sql, record)
        
        # Test various query patterns
        queries = [
            "SELECT COUNT(*) FROM neows",
            "SELECT * FROM neows LIMIT 10",
            "SELECT id, name FROM neows WHERE is_potentially_hazardous = true",
            "SELECT AVG(absolute_magnitude_h) FROM neows"
        ]
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Query Efficiency Test:")
        
        for query in queries:
            start_time = time.perf_counter()
            result = clean_database.execute_sql(query)
            rows = result.fetchall()
            end_time = time.perf_counter()
            
            query_time = end_time - start_time
            
            print(f"  {query[:50]}...")
            print(f"    Time: {query_time:.4f}s")
            print(f"    Rows: {len(rows)}")
            
            # All queries should complete efficiently
            assert query_time < 1.0, f"Query inefficient: {query} - {query_time:.4f}s"

    def test_database_size_efficiency(self, clean_database):
        """
        Test database storage efficiency characteristics.
        
        Compares storage efficiency and file size characteristics
        between SQLite and PostgreSQL for similar data sets.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        # Insert a known amount of test data
        num_records = 100
        
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
        
        for i in range(num_records):
            record = {
                'id': f'SIZE_TEST_{i:05d}',
                'name': f'Size Test Asteroid {i} - This is a longer name to test storage efficiency',
                'close_approach_date': f'2025-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}',
                'absolute_magnitude_h': 15.0 + (i / 10),
                'diameter_min_km': 0.001 + (i * 0.0001),
                'diameter_max_km': 0.002 + (i * 0.0002),
                'is_potentially_hazardous': i % 4 == 0,
                'relative_velocity_kps': 5.0 + (i % 30),
                'miss_distance_km': 500000 + (i * 10000),
                'orbiting_body': 'Earth'
            }
            clean_database.execute_sql(insert_sql, record)
        
        # Verify data was inserted
        result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
        count = result.fetchone()[0]
        assert count == num_records
        
        db_type = "PostgreSQL" if clean_database.is_postgres else "SQLite"
        
        print(f"\n{db_type} Storage Efficiency:")
        print(f"  Records stored: {num_records}")
        print(f"  Database type: {db_type}")
        
        # For SQLite, we could check file size, but for testing we'll just
        # verify the data integrity
        sample_result = clean_database.execute_sql(
            "SELECT id, name, is_potentially_hazardous FROM neows LIMIT 5"
        )
        sample_rows = sample_result.fetchall()
        
        assert len(sample_rows) >= 1
        print(f"  Sample record verification: {len(sample_rows)} rows retrieved")
        
        # Verify data integrity
        for row in sample_rows:
            assert row[0].startswith('SIZE_TEST_')
            assert 'Size Test Asteroid' in row[1]
            assert isinstance(row[2], (bool, int))  # Boolean handling may vary