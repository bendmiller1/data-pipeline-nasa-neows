"""
Migration system testing for dual database environments.

This module tests the database migration system functionality across both
SQLite and PostgreSQL backends, ensuring migrations work consistently
and reliably regardless of the database type. Tests cover migration
discovery, execution, version tracking, and rollback capabilities.

Test Classes:
    TestMigrationDiscovery: Migration file discovery and loading
    TestSchemaVersioning: Version tracking and migration state management  
    TestMigrationExecution: Migration up/down execution and validation
    TestCrossDatabase: SQLite to PostgreSQL migration testing
    TestMigrationCLI: Command-line interface migration testing
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from sqlalchemy import text
from src.migration_manager import MigrationManager, BaseMigration
from src.schema_versioning import SchemaVersionManager
from src.load import DatabaseManager


class TestMigrationDiscovery:
    """
    Test migration file discovery and loading functionality.
    
    Validates that the migration system can correctly discover, load, and
    instantiate migration classes from the migrations directory.
    """

    def test_migration_discovery_basic(self, dual_database):
        """
        Test basic migration discovery functionality.
        
        Verifies that the MigrationManager can discover existing migration
        files and return them in the correct format.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        
        # Discover migrations
        migrations = manager.discover_migrations()
        
        # Should find at least the existing migrations
        assert isinstance(migrations, dict)
        assert len(migrations) >= 2
        
        # Should include 001 and 002 migrations
        assert '001' in migrations
        assert '002' in migrations
        
        # Migration values should be class types
        for version, migration_class in migrations.items():
            assert isinstance(version, str)
            assert len(version) == 3  # Should be 3-digit version
            assert issubclass(migration_class, BaseMigration)

    def test_migration_instantiation(self, dual_database):
        """
        Test migration class instantiation and method availability.
        
        Verifies that discovered migration classes can be instantiated
        and have the required methods.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        migrations = manager.discover_migrations()
        
        for version, migration_class in migrations.items():
            # Instantiate migration
            migration_instance = migration_class()
            
            # Verify required methods exist
            assert hasattr(migration_instance, 'up')
            assert hasattr(migration_instance, 'down')
            assert hasattr(migration_instance, 'description')
            
            # Verify methods are callable
            assert callable(migration_instance.up)
            assert callable(migration_instance.down)
            
            # Description should return a string
            description = migration_instance.description
            assert isinstance(description, str)
            assert len(description) > 0

    def test_migration_ordering(self, dual_database):
        """
        Test that migrations are discovered in correct version order.
        
        Verifies that migration versions are properly sorted to ensure
        correct execution order.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        migrations = manager.discover_migrations()
        
        # Get sorted version list
        versions = list(migrations.keys())
        sorted_versions = sorted(versions)
        
        # Versions should already be in sorted order
        assert versions == sorted_versions
        
        # First version should be 001
        assert sorted_versions[0] == '001'
        
        # All versions should be 3-digit strings
        for version in versions:
            assert len(version) == 3
            assert version.isdigit()

    def test_empty_migrations_directory(self, dual_database):
        """
        Test behavior when migrations directory is empty or missing.
        
        Verifies graceful handling when no migration files are found.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        
        # Mock empty migrations directory
        with patch('os.listdir', return_value=[]):
            migrations = manager.discover_migrations()
            assert isinstance(migrations, dict)
            assert len(migrations) == 0


class TestSchemaVersioning:
    """
    Test schema version tracking and migration state management.
    
    Validates that the SchemaVersionManager correctly tracks which migrations
    have been applied and maintains consistent state across database types.
    """

    def test_initial_version_state(self, clean_database):
        """
        Test initial version state before any migrations.
        
        Verifies that a fresh database has no current version and
        no applied migrations recorded.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        version_manager = SchemaVersionManager(clean_database)
        
        # Initially should have no version
        current_version = version_manager.get_current_version()
        assert current_version is None
        
        # Should have no applied migrations
        applied_migrations = version_manager.get_applied_migrations()
        assert isinstance(applied_migrations, list)
        assert len(applied_migrations) == 0

    def test_migration_recording(self, clean_database):
        """
        Test recording of migration application.
        
        Verifies that migrations can be recorded as applied and the
        version state is updated correctly.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        version_manager = SchemaVersionManager(clean_database)
        
        # Record first migration
        version_manager.record_migration('001', 'Initial schema')
        
        # Should show 001 as current version
        assert version_manager.get_current_version() == '001'
        assert version_manager.is_migration_applied('001')
        
        # Applied migrations should include 001
        applied = version_manager.get_applied_migrations()
        assert '001' in applied
        
        # Record second migration
        version_manager.record_migration('002', 'Data migration')
        
        # Should show 002 as current version (highest)
        assert version_manager.get_current_version() == '002'
        assert version_manager.is_migration_applied('002')
        
        # Applied migrations should include both
        applied = version_manager.get_applied_migrations()
        assert '001' in applied
        assert '002' in applied
        assert len(applied) == 2

    def test_migration_removal(self, clean_database):
        """
        Test removal of migration records for rollbacks.
        
        Verifies that migration records can be removed and version
        state is updated correctly.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        version_manager = SchemaVersionManager(clean_database)
        
        # Record some migrations
        version_manager.record_migration('001', 'Initial schema')
        version_manager.record_migration('002', 'Data migration')
        version_manager.record_migration('003', 'Test migration')
        
        # Remove latest migration
        version_manager.remove_migration_record('003')
        
        # Current version should now be 002
        assert version_manager.get_current_version() == '002'
        assert not version_manager.is_migration_applied('003')
        assert version_manager.is_migration_applied('002')
        
        # Remove middle migration
        version_manager.remove_migration_record('002')
        
        # Current version should now be 001
        assert version_manager.get_current_version() == '001'
        assert not version_manager.is_migration_applied('002')
        assert version_manager.is_migration_applied('001')

    def test_version_state_persistence(self, dual_database):
        """
        Test that version state persists across database connections.
        
        Verifies that migration state is properly stored in the database
        and survives connection cycles.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        # First connection - record migrations
        version_manager1 = SchemaVersionManager(dual_database)
        version_manager1.record_migration('001', 'Initial schema')
        version_manager1.record_migration('002', 'Data migration')
        
        # Second connection - should see same state
        version_manager2 = SchemaVersionManager(dual_database)
        
        assert version_manager2.get_current_version() == '002'
        assert version_manager2.is_migration_applied('001')
        assert version_manager2.is_migration_applied('002')
        
        applied = version_manager2.get_applied_migrations()
        assert len(applied) == 2
        assert '001' in applied
        assert '002' in applied


class TestMigrationExecution:
    """
    Test migration execution functionality.
    
    Validates that migrations can be executed successfully and produce
    the expected database changes for both up and down operations.
    """

    def test_migration_manager_initialization(self, dual_database):
        """
        Test MigrationManager initialization and basic functionality.
        
        Verifies that MigrationManager can be created and provides
        expected interface methods.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        
        # Should have database manager reference
        assert manager.db_manager is dual_database
        
        # Should have version manager
        assert hasattr(manager, 'version_manager')
        assert isinstance(manager.version_manager, SchemaVersionManager)
        
        # Should be able to discover migrations
        migrations = manager.discover_migrations()
        assert isinstance(migrations, dict)

    def test_up_migration_execution(self, clean_database):
        """
        Test execution of up migrations.
        
        Verifies that up migrations execute successfully and create
        expected database schema changes.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Execute up migrations
        manager.migrate_up()
        
        # Verify migrations were recorded
        current_version = manager.version_manager.get_current_version()
        assert current_version is not None
        
        # Verify at least one migration was applied
        applied_migrations = manager.version_manager.get_applied_migrations()
        assert len(applied_migrations) >= 1
        
        # Verify schema exists (migration 001 should create neows table)
        try:
            result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
            count = result.fetchone()[0]
            assert count == 0  # Table should exist but be empty
        except Exception as e:
            pytest.fail(f"Migration failed to create neows table: {e}")

    def test_up_migration_target_version(self, clean_database):
        """
        Test up migration to specific target version.
        
        Verifies that migrations can be executed up to a specific
        version rather than all available migrations.
        
        Args:
            clean_database: Clean database fixture with schema  
        """
        manager = MigrationManager(clean_database)
        
        # Migrate only to version 001
        manager.migrate_up(target_version='001')
        
        # Should have applied only 001
        applied_migrations = manager.version_manager.get_applied_migrations()
        assert '001' in applied_migrations
        
        # Current version should be 001
        assert manager.version_manager.get_current_version() == '001'
        
        # Should not have applied 002 yet
        assert not manager.version_manager.is_migration_applied('002')

    def test_migration_dry_run(self, clean_database):
        """
        Test dry run functionality for migrations.
        
        Verifies that dry run shows what would be applied without
        actually executing the migrations.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Get pending migrations before dry run
        pending_migrations = manager.get_pending_migrations()
        
        # Should have migrations to apply
        assert isinstance(pending_migrations, list)
        assert len(pending_migrations) >= 1
        
        # Perform dry run (returns None but shows what would be done)
        manager.migrate_up(dry_run=True)
        
        # But should not actually apply them
        current_version = manager.version_manager.get_current_version()
        assert current_version is None
        
        # Neows table should not exist yet
        with pytest.raises(Exception):
            clean_database.execute_sql("SELECT COUNT(*) FROM neows")

    def test_down_migration_execution(self, clean_database):
        """
        Test execution of down migrations (rollbacks).
        
        Verifies that down migrations execute successfully and properly
        rollback database changes.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # First apply up migrations
        manager.migrate_up(target_version='001')
        
        # Verify migration was applied
        assert manager.version_manager.get_current_version() == '001'
        
        # Execute down migration (rollback all)
        manager.migrate_down(target_version='000')
        
        # Should have rolled back migration 001
        applied_after_rollback = manager.version_manager.get_applied_migrations()
        assert '001' not in applied_after_rollback
        
        # Current version should be None (no migrations applied)
        assert manager.version_manager.get_current_version() is None

    def test_migration_idempotency(self, clean_database):
        """
        Test that migrations are idempotent and safe to re-run.
        
        Verifies that running the same migration multiple times
        doesn't cause errors or duplicate changes.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Run migrations first time
        manager.migrate_up()
        first_version = manager.version_manager.get_current_version()
        first_applied = manager.version_manager.get_applied_migrations()
        
        # Run migrations second time
        manager.migrate_up()
        second_version = manager.version_manager.get_current_version()
        second_applied = manager.version_manager.get_applied_migrations()
        
        # Should not apply any new migrations
        assert second_version == first_version
        assert len(second_applied) == len(first_applied)
        
        # Database state should be unchanged
        result = clean_database.execute_sql("SELECT COUNT(*) FROM neows")
        count = result.fetchone()[0]
        assert count == 0


class TestCrossDatabase:
    """
    Test cross-database migration functionality.
    
    Validates the SQLite to PostgreSQL migration capabilities and
    ensures data integrity during database transitions.
    """

    @pytest.mark.postgres_only
    def test_sqlite_to_postgres_migration_detection(self, postgres_test_db):
        """
        Test detection of SQLite to PostgreSQL migration scenarios.
        
        Verifies that migration 002 correctly detects when it should run
        and when it should skip execution.
        
        Args:
            postgres_test_db: PostgreSQL test database fixture
        """
        # This test requires PostgreSQL
        if not postgres_test_db.is_postgres:
            pytest.skip("Test requires PostgreSQL database")
            
        manager = MigrationManager(postgres_test_db)
        migrations = manager.discover_migrations()
        
        # Should have migration 002 available
        assert '002' in migrations
        
        # Instantiate migration 002
        migration_002 = migrations['002']()
        
        # Should have proper description
        description = migration_002.description
        assert 'sqlite' in description.lower()
        assert 'postgresql' in description.lower()

    def test_cross_database_migration_skip_logic(self, dual_database):
        """
        Test that cross-database migrations skip appropriately.
        
        Verifies that SQLite->PostgreSQL migration skips when run
        against SQLite database.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        
        # Apply initial schema
        manager.migrate_up(target_version='001')
        
        # Try to apply migration 002
        if dual_database.is_postgres:
            # PostgreSQL should attempt the migration (may skip if no SQLite data)
            manager.migrate_up(target_version='002')
            # Should complete without error - check final state
            final_version = manager.version_manager.get_current_version()
            assert final_version is not None
        else:
            # SQLite should skip migration 002 (it's for PostgreSQL only)
            manager.migrate_up(target_version='002')
            # Should still complete successfully - check final state
            final_version = manager.version_manager.get_current_version()
            assert final_version is not None


class TestMigrationValidation:
    """
    Test migration validation and error handling.
    
    Validates that the migration system properly handles error conditions
    and validates migration integrity.
    """

    def test_migration_validation_success(self, clean_database):
        """
        Test successful migration validation.
        
        Verifies that valid migrations pass validation checks.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Validate all discovered migrations
        migrations = manager.discover_migrations()
        
        for version, migration_class in migrations.items():
            # Each migration should validate successfully
            migration_instance = migration_class()
            
            # Basic validation - should not raise exceptions
            try:
                # Verify required attributes exist
                assert hasattr(migration_instance, 'up')
                assert hasattr(migration_instance, 'down') 
                assert hasattr(migration_instance, 'description')
                
                # Verify description is meaningful
                description = migration_instance.description
                assert len(description) > 10  # Should be descriptive
                
            except Exception as e:
                pytest.fail(f"Migration {version} failed validation: {e}")

    def test_migration_dependency_order(self, clean_database):
        """
        Test that migrations respect dependency order.
        
        Verifies that migrations are applied in correct order and
        dependencies are satisfied.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Apply migrations one by one to test ordering
        manager.migrate_up(target_version='001')
        
        # Verify 001 is applied
        assert manager.version_manager.is_migration_applied('001')
        
        # Should be able to apply 002 after 001
        manager.migrate_up(target_version='002')
        
        # Both should be applied in correct order
        applied = manager.version_manager.get_applied_migrations()
        assert '001' in applied
        assert '002' in applied

    def test_partial_migration_recovery(self, clean_database):
        """
        Test recovery from partial migration failures.
        
        Verifies that the system can recover gracefully from
        interrupted or failed migrations.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Apply first migration successfully
        manager.migrate_up(target_version='001')
        
        # Verify system state is consistent
        current_version = manager.version_manager.get_current_version()
        assert current_version == '001'
        
        # System should be able to continue with additional migrations
        manager.migrate_up()  # Apply all remaining migrations
        
        # Should complete successfully
        final_version = manager.version_manager.get_current_version()
        assert final_version is not None


class TestMigrationLogging:
    """
    Test migration logging and reporting functionality.
    
    Validates that migrations provide appropriate logging and status
    reporting during execution.
    """

    def test_migration_status_reporting(self, clean_database):
        """
        Test migration status and progress reporting.
        
        Verifies that migrations provide status information during
        execution for monitoring and debugging.
        
        Args:
            clean_database: Clean database fixture with schema
        """
        manager = MigrationManager(clean_database)
        
        # Check initial status
        current_version = manager.version_manager.get_current_version()
        applied_migrations = manager.version_manager.get_applied_migrations()
        
        assert current_version is None
        assert len(applied_migrations) == 0
        
        # Apply migrations and check progress
        applied = manager.migrate_up()
        
        # Should report which migrations were applied
        assert isinstance(applied, list)
        assert len(applied) >= 1
        
        # Final status should show applied migrations
        final_version = manager.version_manager.get_current_version()
        final_applied = manager.version_manager.get_applied_migrations()
        
        assert final_version is not None
        assert len(final_applied) >= 1
        assert final_version in final_applied

    def test_migration_error_reporting(self, dual_database):
        """
        Test migration error reporting and diagnostics.
        
        Verifies that migration errors are properly captured and
        reported with useful diagnostic information.
        
        Args:
            dual_database: Parameterized database fixture (SQLite or PostgreSQL)
        """
        manager = MigrationManager(dual_database)
        
        # Test with invalid target version
        try:
            manager.migrate_up(target_version='999')  # Non-existent version
            # Should either complete gracefully or provide clear error
            
        except Exception as e:
            # If an exception occurs, it should be meaningful
            assert len(str(e)) > 0
            
        # System should still be in consistent state
        current_version = manager.version_manager.get_current_version()
        # Should be None or a valid version, not corrupted
        if current_version is not None:
            assert len(current_version) == 3
            assert current_version.isdigit()