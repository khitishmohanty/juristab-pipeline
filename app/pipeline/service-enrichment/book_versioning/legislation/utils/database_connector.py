import os
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine, text, Row
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from typing import Optional, Dict, Any

class DatabaseConnector:
    """Handles all database interactions."""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.engine = self._create_db_engine()
        self.Session = sessionmaker(bind=self.engine)
        
    def _create_db_engine(self):
        """Creates and returns a SQLAlchemy engine for database connections."""
        try:
            connection_url = URL.create(
                drivername=f"{self.db_config['dialect']}+{self.db_config['driver']}",
                username=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['name']
            )
            print(f"Creating database engine for: {self.db_config['name']}")
            return create_engine(connection_url)
        except Exception as e:
            print(f"Error creating database engine: {e}")
            raise
    
    def read_sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (dict, optional): Query parameters for parameterized queries
            
        Returns:
            pd.DataFrame: Query results as DataFrame
        """
        try:
            return pd.read_sql_query(sql=text(query), con=self.engine, params=params)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    def update_book_version(self, table_name: str, source_id: str, version: int, version_col: str):
        """
        Updates the book_version for a specific source_id in the registry table.
        
        Args:
            table_name (str): The name of the table to update (e.g., legislation_registry)
            source_id (str): The source_id to update
            version (int): The version number to assign
            version_col (str): The name of the version column (e.g., book_version)
        """
        session = self.Session()
        
        try:
            stmt = text(f"""
                UPDATE {table_name}
                SET {version_col} = :version
                WHERE source_id = :source_id
            """)
            
            session.execute(stmt, {
                "version": version,
                "source_id": source_id
            })
            
            session.commit()
            
        except Exception as e:
            print(f"Error updating version for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_step_result(self, table_name: str, source_id: str, step: str, status: str, 
                          duration: float, start_time: datetime, end_time: datetime, 
                          step_columns: dict):
        """
        Updates a step's result if the record exists, otherwise inserts a new record.
        This combines insert and update logic into a single "upsert" operation.
        
        This method logs the versioning status to legislation_enrichment_status table.
        
        Args:
            table_name (str): The name of the status table (e.g., legislation_enrichment_status)
            source_id (str): The source_id to update/insert
            step (str): The step name (e.g., 'book_versioning')
            status (str): Status value ('pass' or 'failed')
            duration (float): Duration in seconds
            start_time (datetime): Start time of the process
            end_time (datetime): End time of the process
            step_columns (dict): Column mapping for the step from config
        """
        session = self.Session()
        
        if step not in step_columns:
            raise ValueError(f"Invalid step name provided: {step}")
        
        step_config = step_columns[step]
        status_col = step_config['status']
        duration_col = step_config['duration']
        start_time_col = step_config['start_time']
        end_time_col = step_config['end_time']

        if status not in ['pass', 'failed']:
            raise ValueError("Invalid status value. Must be 'pass' or 'failed'.")

        try:
            # Check if the record exists
            stmt_select = text(f"SELECT id FROM {table_name} WHERE source_id = :source_id")
            result = session.execute(stmt_select, {"source_id": source_id}).fetchone()

            if result:
                # UPDATE existing record
                stmt_update = text(f"""
                    UPDATE {table_name} 
                    SET {status_col} = :status, 
                        {duration_col} = :duration,
                        {start_time_col} = :start_time,
                        {end_time_col} = :end_time
                    WHERE source_id = :source_id
                """)
                session.execute(stmt_update, {
                    "status": status, 
                    "duration": duration, 
                    "start_time": start_time,
                    "end_time": end_time,
                    "source_id": source_id
                })
                print(f"Updated {step} to '{status}' for source_id: {source_id}")
            else:
                # INSERT new record
                new_id = str(uuid.uuid4())
                stmt_insert = text(f"""
                    INSERT INTO {table_name} (
                        id, source_id, 
                        {status_col}, {duration_col}, {start_time_col}, {end_time_col}
                    )
                    VALUES (:id, :source_id, :status, :duration, :start_time, :end_time)
                """)
                session.execute(stmt_insert, {
                    "id": new_id,
                    "source_id": source_id,
                    "status": status,
                    "duration": duration,
                    "start_time": start_time,
                    "end_time": end_time
                })
                print(f"Inserted new status '{status}' for {step} for source_id: {source_id}")

            session.commit()
        except Exception as e:
            print(f"Error during upsert for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()