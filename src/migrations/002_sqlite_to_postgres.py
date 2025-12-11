"""
SQLite to PostgreSQL data migration.

Transfers existing NEO data from SQLite database to PostgreSQL with
data validation and type conversion handling.
"""
import logging
import pandas as pd
from typing import Optional
from pathlib import Path
from sqlalchemy import text
from ..migration_manager import BaseMigration
from ..load import DatabaseManager
from ..config import DB_PATH

logger = logging.getLogger(__name__)

class Migration002(BaseMigration):
    """
    Migrates existing NEO data from SQLite to PostgreSQL.
    
    This migration handles the transfer of data from an existing SQLite
    database to PostgreSQL, including data type conversions and validation.
    Only runs when the target database is PostgreSQL and source SQLite exists.
    """
    
    def up(self, db_manager: DatabaseManager) -> None:
        """
        Migrate data from SQLite to PostgreSQL.
        
        Steps:
        1. Check if target is PostgreSQL and source SQLite exists
        2. Connect to source SQLite database
        3. Extract data in batches
        4. Transform data types for PostgreSQL compatibility
        5. Load data into PostgreSQL with validation
        
        Args:
            db_manager (DatabaseManager): Target PostgreSQL database manager
            
        Raises:
            Exception: If migration fails or validation errors occur
        """
        # Only run if target is PostgreSQL
        if not db_manager.is_postgres:
            logger.info("Skipping SQLite->PostgreSQL migration: target is not PostgreSQL")
            return
            
        # Check if source SQLite database exists
        sqlite_path = Path(DB_PATH)
        if not sqlite_path.exists():
            logger.info(f"Skipping SQLite->PostgreSQL migration: {DB_PATH} not found")
            return
            
        logger.info(f"Starting SQLite to PostgreSQL data migration from {DB_PATH}")
        
        try:
            # Connect to source SQLite database
            sqlite_url = f"sqlite:///{DB_PATH}"
            sqlite_manager = DatabaseManager(sqlite_url)
            
            # Check if neows table exists in SQLite
            check_table_sql = """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='neows'
            """
            result = sqlite_manager.execute_sql(check_table_sql)
            if not result.fetchone():
                logger.info("No neows table found in SQLite database")
                return
            
            # Get row count for progress tracking
            count_sql = "SELECT COUNT(*) as total FROM neows"
            count_result = sqlite_manager.execute_sql(count_sql).fetchone()
            total_rows = count_result[0] if count_result else 0
            logger.info(f"Found {total_rows} rows to migrate from SQLite")
            
            if total_rows == 0:
                logger.info("No data to migrate from SQLite")
                return
            
            # Migrate data in batches to handle large datasets
            batch_size = 1000
            migrated_rows = 0
            
            for offset in range(0, total_rows, batch_size):
                batch_sql = f"""
                SELECT 
                    id, name, close_approach_date, absolute_magnitude_h,
                    diameter_min_km, diameter_max_km, is_potentially_hazardous,
                    relative_velocity_kps, miss_distance_km, orbiting_body
                FROM neows 
                ORDER BY close_approach_date, id
                LIMIT {batch_size} OFFSET {offset}
                """
                
                # Extract batch from SQLite
                batch_result = sqlite_manager.execute_sql(batch_sql)
                rows = batch_result.fetchall()
                
                if not rows:
                    break
                
                # Transform and validate data for PostgreSQL
                for row in rows:
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
                    ON CONFLICT (close_approach_date, id) DO NOTHING
                    """
                    
                    # Convert SQLite data types for PostgreSQL
                    params = {
                        'id': row[0],
                        'name': row[1],
                        'close_approach_date': row[2],  # Will be converted to DATE by PostgreSQL
                        'absolute_magnitude_h': row[3],
                        'diameter_min_km': row[4],
                        'diameter_max_km': row[5],
                        'is_potentially_hazardous': bool(row[6]) if row[6] is not None else None,
                        'relative_velocity_kps': row[7],
                        'miss_distance_km': row[8],
                        'orbiting_body': row[9]
                    }
                    
                    try:
                        db_manager.execute_sql(insert_sql, params)
                        migrated_rows += 1
                    except Exception as e:
                        logger.warning(f"Failed to insert row {row[0]} from {row[2]}: {e}")
                        continue
                
                logger.info(f"Migrated batch: {migrated_rows}/{total_rows} rows completed")
            
            # Validate migration
            postgres_count_sql = "SELECT COUNT(*) FROM neows"
            postgres_result = db_manager.execute_sql(postgres_count_sql).fetchone()
            postgres_count = postgres_result[0] if postgres_result else 0
            
            logger.info(f"Migration completed: {migrated_rows} rows migrated, {postgres_count} rows in PostgreSQL")
            
            if postgres_count != migrated_rows:
                logger.warning(f"Row count mismatch: attempted {migrated_rows}, actual {postgres_count}")
            
            # Sample validation: check a few random records
            sample_sql = "SELECT id, name, close_approach_date FROM neows ORDER BY RANDOM() LIMIT 3"
            sample_result = db_manager.execute_sql(sample_sql)
            sample_rows = sample_result.fetchall()
            
            logger.info(f"Sample migrated records: {[dict(zip(['id', 'name', 'date'], row)) for row in sample_rows]}")
            
        except Exception as e:
            logger.error(f"SQLite to PostgreSQL migration failed: {e}")
            raise
    
    def down(self, db_manager: DatabaseManager) -> None:
        """
        Rollback the SQLite to PostgreSQL migration.
        
        This removes all data from the PostgreSQL neows table, effectively
        undoing the migration. Use with caution in production.
        
        Args:
            db_manager (DatabaseManager): Target database manager
            
        Raises:
            Exception: If rollback fails
        """
        if not db_manager.is_postgres:
            logger.info("Skipping migration rollback: target is not PostgreSQL")
            return
            
        logger.info("Rolling back SQLite to PostgreSQL migration")
        
        try:
            # Get current row count
            count_sql = "SELECT COUNT(*) FROM neows"
            current_result = db_manager.execute_sql(count_sql).fetchone()
            current_count = current_result[0] if current_result else 0
            
            logger.warning(f"About to delete {current_count} rows from neows table")
            
            # Delete all data (but keep table structure)
            delete_sql = "DELETE FROM neows"
            db_manager.execute_sql(delete_sql)
            
            # Verify deletion
            verify_result = db_manager.execute_sql(count_sql).fetchone()
            verify_count = verify_result[0] if verify_result else 0
            logger.info(f"Rollback completed: {verify_count} rows remaining in neows table")
            
        except Exception as e:
            logger.error(f"Failed to rollback SQLite migration: {e}")
            raise
    
    @property
    def description(self) -> str:
        """
        Human-readable description of this migration.
        
        Returns:
            str: Description of what this migration accomplishes
        """
        return "Migrate existing NEO data from SQLite to PostgreSQL with validation"