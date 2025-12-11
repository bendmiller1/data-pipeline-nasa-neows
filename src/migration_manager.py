"""
Migration management system for database schema changes.

This module provides the main orchestration for discovering, executing, and
tracking database migrations across both SQLite and PostgreSQL databases.

Classes:
    MigrationManager: Core migration orchestration and execution
    BaseMigration: Abstract base class for individual migrations
"""
import os
import re
import logging
import importlib
import inspect
from typing import List, Dict, Optional, Type
from abc import ABC, abstractmethod
from .load import DatabaseManager
from .schema_versioning import SchemaVersionManager

logger = logging.getLogger(__name__)

class BaseMigration(ABC):
    """
    Abstract base class for database migrations.
    
    All migration classes must inherit from this class and implement
    the up() and down() methods for applying and rolling back changes.
    
    Example:
        >>> class Migration001(BaseMigration):
        ...     def up(self, db_manager):
        ...         # Apply changes
        ...         pass
        ...     def down(self, db_manager):
        ...         # Rollback changes
        ...         pass
        ...     @property
        ...     def description(self):
        ...         return "Create initial schema"
    """
    
    @abstractmethod
    def up(self, db_manager: DatabaseManager) -> None:
        """
        Apply the migration changes.
        
        Args:
            db_manager (DatabaseManager): Database connection manager
            
        Raises:
            Exception: If migration fails to apply
        """
        pass
    
    @abstractmethod
    def down(self, db_manager: DatabaseManager) -> None:
        """
        Rollback the migration changes.
        
        Args:
            db_manager (DatabaseManager): Database connection manager
            
        Raises:
            Exception: If migration fails to rollback
        """
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """
        Human-readable description of what this migration does.
        
        Returns:
            str: Description of the migration's purpose
        """
        pass

class MigrationManager:
    """
    Manages discovery, execution, and tracking of database migrations.
    
    This class coordinates between the SchemaVersionManager for tracking
    and the individual migration files for execution. It provides methods
    to discover available migrations, determine what needs to be run, and
    execute migrations safely with rollback capabilities.
    
    Attributes:
        db_manager (DatabaseManager): Database connection manager
        migrations_dir (str): Path to migrations directory
        version_manager (SchemaVersionManager): Migration version tracker
        
    Example:
        >>> db_manager = DatabaseManager('postgresql')
        >>> migration_manager = MigrationManager(db_manager)
        >>> migration_manager.migrate_up()
    """
    
    def __init__(self, db_manager: DatabaseManager, migrations_dir: str = "src/migrations"):
        """
        Initialize the migration manager.
        
        Args:
            db_manager (DatabaseManager): Database connection manager
            migrations_dir (str): Path to migrations directory
        """
        self.db_manager = db_manager
        self.migrations_dir = migrations_dir
        self.version_manager = SchemaVersionManager(db_manager)
    
    def discover_migrations(self) -> Dict[str, Type[BaseMigration]]:
        """
        Discover and load all migration classes from the migrations directory.
        
        Scans the migrations directory for Python files matching the pattern
        XXX_*.py where XXX is a 3-digit version number, imports them, and
        extracts the migration class.
        
        Returns:
            Dict[str, Type[BaseMigration]]: Dictionary mapping version strings
                                          to migration classes
                                          
        Raises:
            ImportError: If a migration file cannot be imported
            ValueError: If a migration file doesn't contain a valid migration class
            
        Example:
            >>> migrations = manager.discover_migrations()
            >>> print(migrations.keys())  # ['001', '002', '003']
        """
        migrations = {}
        
        if not os.path.exists(self.migrations_dir):
            logger.warning(f"Migrations directory {self.migrations_dir} does not exist")
            return migrations
        
        # Pattern to match migration files: 001_description.py, 002_description.py, etc.
        migration_pattern = re.compile(r'^(\d{3})_.*\.py$')
        
        for filename in sorted(os.listdir(self.migrations_dir)):
            match = migration_pattern.match(filename)
            if not match:
                continue
                
            version = match.group(1)
            module_name = filename[:-3]  # Remove .py extension
            
            try:
                # Import the migration module
                module_path = f"{self.migrations_dir.replace('/', '.')}.{module_name}"
                module = importlib.import_module(module_path)
                
                # Find the migration class in the module
                migration_class = None
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (issubclass(obj, BaseMigration) and 
                        obj != BaseMigration and 
                        not inspect.isabstract(obj)):
                        migration_class = obj
                        break
                
                if migration_class is None:
                    logger.error(f"No valid migration class found in {filename}")
                    continue
                    
                migrations[version] = migration_class
                logger.debug(f"Discovered migration {version}: {filename}")
                
            except Exception as e:
                logger.error(f"Failed to load migration {filename}: {e}")
                continue
        
        logger.info(f"Discovered {len(migrations)} migrations")
        return migrations
    
    def get_pending_migrations(self) -> List[str]:
        """
        Get list of migration versions that haven't been applied yet.
        
        Compares discovered migrations against the applied migrations
        tracked in the database to determine which need to be run.
        
        Returns:
            List[str]: Ordered list of pending migration versions
            
        Example:
            >>> pending = manager.get_pending_migrations()
            >>> print(pending)  # ['002', '003']
        """
        discovered = set(self.discover_migrations().keys())
        applied = set(self.version_manager.get_applied_migrations())
        pending = sorted(discovered - applied)
        
        logger.info(f"Found {len(pending)} pending migrations: {pending}")
        return pending
    
    def migrate_up(self, target_version: Optional[str] = None, dry_run: bool = False) -> None:
        """
        Apply pending migrations up to the target version.
        
        Executes migrations in order from the current version up to the
        specified target version (or all pending migrations if no target
        is specified).
        
        Args:
            target_version (Optional[str]): Stop at this version, or None
                                          to apply all pending migrations
            dry_run (bool): If True, show what would be done without
                          actually executing migrations
                          
        Raises:
            ValueError: If target_version is not found or invalid
            Exception: If a migration fails to execute
            
        Example:
            >>> manager.migrate_up()  # Apply all pending
            >>> manager.migrate_up('003')  # Apply up to version 003
            >>> manager.migrate_up(dry_run=True)  # Preview changes
        """
        migrations = self.discover_migrations()
        pending = self.get_pending_migrations()
        
        if target_version and target_version not in migrations:
            raise ValueError(f"Target version {target_version} not found")
        
        # Filter pending migrations up to target version
        to_apply = []
        for version in pending:
            to_apply.append(version)
            if target_version and version == target_version:
                break
        
        if not to_apply:
            logger.info("No migrations to apply")
            return
        
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Applying {len(to_apply)} migrations: {to_apply}")
        
        for version in to_apply:
            migration_class = migrations[version]
            migration = migration_class()
            
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Applying migration {version}: {migration.description}")
            
            if not dry_run:
                try:
                    migration.up(self.db_manager)
                    self.version_manager.record_migration(version, migration.description)
                    logger.info(f"Successfully applied migration {version}")
                except Exception as e:
                    logger.error(f"Failed to apply migration {version}: {e}")
                    raise
    
    def migrate_down(self, target_version: str, dry_run: bool = False) -> None:
        """
        Rollback migrations down to the target version.
        
        Executes migration rollbacks in reverse order from the current
        version down to (but not including) the specified target version.
        
        Args:
            target_version (str): Rollback down to this version
            dry_run (bool): If True, show what would be done without
                          actually executing rollbacks
                          
        Raises:
            ValueError: If target_version is invalid
            Exception: If a migration rollback fails
            
        Example:
            >>> manager.migrate_down('001')  # Rollback to version 001
            >>> manager.migrate_down('001', dry_run=True)  # Preview rollbacks
        """
        migrations = self.discover_migrations()
        applied = self.version_manager.get_applied_migrations()
        
        # Find migrations to rollback (all applied migrations after target)
        to_rollback = []
        for version in reversed(applied):  # Process in reverse order
            if version <= target_version:
                break
            to_rollback.append(version)
        
        if not to_rollback:
            logger.info("No migrations to rollback")
            return
        
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Rolling back {len(to_rollback)} migrations: {to_rollback}")
        
        for version in to_rollback:
            if version not in migrations:
                logger.error(f"Migration {version} not found for rollback")
                continue
                
            migration_class = migrations[version]
            migration = migration_class()
            
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Rolling back migration {version}: {migration.description}")
            
            if not dry_run:
                try:
                    migration.down(self.db_manager)
                    self.version_manager.remove_migration_record(version)
                    logger.info(f"Successfully rolled back migration {version}")
                except Exception as e:
                    logger.error(f"Failed to rollback migration {version}: {e}")
                    raise
    
    def get_migration_status(self) -> Dict[str, bool]:
        """
        Get the current status of all discovered migrations.
        
        Returns a dictionary showing which migrations are applied (True)
        and which are pending (False).
        
        Returns:
            Dict[str, bool]: Dictionary mapping version strings to
                           application status (True = applied, False = pending)
                           
        Example:
            >>> status = manager.get_migration_status()
            >>> print(status)  # {'001': True, '002': True, '003': False}
        """
        migrations = self.discover_migrations()
        applied = set(self.version_manager.get_applied_migrations())
        
        status = {}
        for version in sorted(migrations.keys()):
            status[version] = version in applied
            
        return status
    
    def validate_migrations(self) -> List[str]:
        """
        Validate all discovered migrations for common issues.
        
        Checks for missing files, invalid class definitions, and other
        potential problems with the migration files.
        
        Returns:
            List[str]: List of validation errors found
            
        Example:
            >>> errors = manager.validate_migrations()
            >>> if errors:
            ...     print("Validation errors:", errors)
        """
        errors = []
        
        try:
            migrations = self.discover_migrations()
        except Exception as e:
            errors.append(f"Failed to discover migrations: {e}")
            return errors
        
        # Check for gaps in version numbering
        versions = sorted(migrations.keys())
        expected_versions = [f"{i:03d}" for i in range(1, len(versions) + 1)]
        
        if versions != expected_versions:
            errors.append(f"Gap in migration versions. Expected: {expected_versions}, Found: {versions}")
        
        # Validate each migration class
        for version, migration_class in migrations.items():
            try:
                migration = migration_class()
                if not hasattr(migration, 'up') or not callable(migration.up):
                    errors.append(f"Migration {version} missing or invalid up() method")
                if not hasattr(migration, 'down') or not callable(migration.down):
                    errors.append(f"Migration {version} missing or invalid down() method")
                if not hasattr(migration, 'description') or not migration.description:
                    errors.append(f"Migration {version} missing or empty description")
            except Exception as e:
                errors.append(f"Migration {version} instantiation failed: {e}")
        
        return errors
