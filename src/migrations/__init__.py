"""
Database migrations package for NASA NeoWs Data Pipeline.

This package contains database migration files that handle schema changes
and data transformations for both SQLite and PostgreSQL databases.

Migration files follow the naming convention: XXX_description.py where XXX
is a 3-digit version number (e.g., 001_initial_schema.py).

Each migration file must contain a class that inherits from BaseMigration
and implements up(), down(), and description methods.

Available migrations:
    001_initial_schema: Creates the basic neows table structure
    002_sqlite_to_postgres: Migrates data from SQLite to PostgreSQL

Usage:
    # Run migrations via CLI
    python -m src.pipeline --mode migrate
    
    # Programmatic usage
    from src.migration_manager import MigrationManager
    from src.load import DatabaseManager
    
    db_manager = DatabaseManager("postgresql://...")
    migration_manager = MigrationManager(db_manager)
    migration_manager.migrate_up()
"""

# Import migration classes for easier discovery
from ..migration_manager import BaseMigration

__all__ = ["BaseMigration"]