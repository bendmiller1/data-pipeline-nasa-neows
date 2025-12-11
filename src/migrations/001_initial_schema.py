"""
Initial schema migration for NASA NeoWs data pipeline.

Creates the basic neows table structure with appropriate data types
for both SQLite and PostgreSQL databases.
"""
import logging
from sqlalchemy import text
from ..migration_manager import BaseMigration
from ..load import DatabaseManager

logger = logging.getLogger(__name__)

class Migration001(BaseMigration):
    """
    Creates the initial neows table with composite primary key and indexes.
    
    This migration establishes the foundational schema for storing NASA
    Near Earth Object data with optimized structure for date-range queries
    and asteroid lookups.
    """
    
    def up(self, db_manager: DatabaseManager) -> None:
        """
        Create the neows table with appropriate schema for the database type.
        
        Uses DatabaseManager.get_schema_sql() to get the correct CREATE TABLE
        statements for either SQLite or PostgreSQL with proper data types.
        
        Args:
            db_manager (DatabaseManager): Database connection manager
            
        Raises:
            Exception: If table creation fails
        """
        logger.info("Creating initial neows table schema")
        
        try:
            # Get database-appropriate schema SQL
            schema_sql = db_manager.get_schema_sql()
            
            # Execute the schema creation
            db_manager.execute_sql(schema_sql)
            
            logger.info("Successfully created neows table with indexes")
            
        except Exception as e:
            logger.error(f"Failed to create initial schema: {e}")
            raise
    
    def down(self, db_manager: DatabaseManager) -> None:
        """
        Drop the neows table and all associated indexes.
        
        This rollback removes all traces of the neows table structure,
        returning the database to its pre-migration state.
        
        Args:
            db_manager (DatabaseManager): Database connection manager
            
        Raises:
            Exception: If table drop fails
        """
        logger.info("Dropping neows table and indexes")
        
        try:
            # Drop indexes first (some databases require this)
            drop_indexes_sql = """
            DROP INDEX IF EXISTS idx_neows_id;
            DROP INDEX IF EXISTS idx_neows_date;
            DROP INDEX IF EXISTS idx_neows_hazardous;
            DROP INDEX IF EXISTS idx_neows_size;
            """
            
            # Drop the main table
            drop_table_sql = "DROP TABLE IF EXISTS neows;"
            
            # Execute drops
            db_manager.execute_sql(drop_indexes_sql)
            db_manager.execute_sql(drop_table_sql)
            
            logger.info("Successfully dropped neows table and indexes")
            
        except Exception as e:
            logger.error(f"Failed to drop schema: {e}")
            raise
    
    @property
    def description(self) -> str:
        """
        Human-readable description of this migration.
        
        Returns:
            str: Description of what this migration accomplishes
        """
        return "Create initial neows table with composite primary key and performance indexes"