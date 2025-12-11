"""
Schema versioning system for tracking database migrations.

This module provides functionality to track which database migrations have been
applied, enabling safe schema evolution and rollback capabilities for both
SQLite and PostgreSQL databases.

Classes:
    SchemaVersionManager: Core class for migration version tracking
"""
import logging
from typing import List, Optional
from datetime import datetime
from sqlalchemy import text
from load import DatabaseManager

logger = logging.getLogger(__name__)

class SchemaVersionManager:
    """
    Manages schema version tracking for database migrations.
    
    This class handles the creation and management of a schema_migrations table
    that tracks which migrations have been applied to the database. It supports
    both SQLite and PostgreSQL through the DatabaseManager abstraction.
    
    Attributes:
        db_manager (DatabaseManager): Database connection manager instance
        
    Example:
        >>> db_manager = DatabaseManager('postgresql')
        >>> version_manager = SchemaVersionManager(db_manager)
        >>> current_version = version_manager.get_current_version()
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize the schema version manager.
        
        Args:
            db_manager (DatabaseManager): Database connection manager instance
        """
        self.db_manager = db_manager
        self._ensure_migrations_table()
    
    def _ensure_migrations_table(self) -> None:
        """
        Create schema_migrations table if it doesn't exist.
        
        Creates the tracking table used to record which migrations have been
        applied to the database. This method is idempotent and safe to call
        multiple times.
        """
        create_sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY,
            version VARCHAR(255) NOT NULL UNIQUE,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
        """
        
        with self.db_manager.get_connection() as conn:
            conn.execute(text(create_sql))
            conn.commit()
            logger.info("Ensured schema_migrations table exists")
    
    def get_applied_migrations(self) -> List[str]:
        """
        Get list of migration versions that have been applied.
        
        Returns:
            List[str]: Ordered list of migration versions that have been applied
                       to the database, sorted by version number.
                       
        Example:
            >>> applied = version_manager.get_applied_migrations()
            >>> print(applied)  # ['001', '002', '003']
        """
        sql = "SELECT version FROM schema_migrations ORDER BY version"
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute(text(sql))
            return [row[0] for row in cursor.fetchall()]
    
    def get_current_version(self) -> Optional[str]:
        """
        Get the highest applied migration version.
        
        Returns:
            Optional[str]: The most recent migration version that has been
                          applied, or None if no migrations have been run.
                          
        Example:
            >>> current = version_manager.get_current_version()
            >>> print(current)  # '003' or None
        """
        sql = "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
        
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute(text(sql))
            result = cursor.fetchone()
            return result[0] if result else None
    
    def record_migration(self, version: str, description: str = "") -> None:
        """
        Record that a migration has been applied.
        
        Args:
            version (str): Migration version identifier (e.g., '001', '002')
            description (str, optional): Human-readable description of the
                                       migration. Defaults to empty string.
                                       
        Raises:
            DatabaseError: If the migration record cannot be inserted
            
        Example:
            >>> version_manager.record_migration('001', 'Initial schema setup')
        """
        sql = """
        INSERT INTO schema_migrations (version, applied_at, description)
        VALUES (:version, :applied_at, :description)
        """
        
        with self.db_manager.get_connection() as conn:
            conn.execute(text(sql), {"version": version, "applied_at": datetime.now(), "description": description})
            conn.commit()
            logger.info(f"Recorded migration {version}: {description}")
    
    def remove_migration_record(self, version: str) -> None:
        """
        Remove a migration record (for rollbacks).
        
        Args:
            version (str): Migration version identifier to remove
            
        Note:
            This only removes the tracking record, it does not undo the
            actual schema changes. The migration's down() method should
            handle reverting the actual database changes.
            
        Example:
            >>> version_manager.remove_migration_record('003')
        """
        sql = "DELETE FROM schema_migrations WHERE version = :version"
        
        with self.db_manager.get_connection() as conn:
            conn.execute(text(sql), {"version": version})
            conn.commit()
            logger.info(f"Removed migration record {version}")
    
    def is_migration_applied(self, version: str) -> bool:
        """
        Check if a specific migration has been applied.
        
        Args:
            version (str): Migration version identifier to check
            
        Returns:
            bool: True if the migration has been applied, False otherwise
            
        Example:
            >>> if version_manager.is_migration_applied('001'):
            ...     print("Migration 001 has been applied")
        """
        return version in self.get_applied_migrations()
