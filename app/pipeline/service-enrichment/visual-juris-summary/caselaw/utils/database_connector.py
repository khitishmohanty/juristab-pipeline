import os
import pandas as pd
import uuid
from sqlalchemy import create_engine, text, Row
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from typing import Optional, Dict
from datetime import datetime

class DatabaseConnector:
    """Handles all database interactions."""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.engine = self._create_db_engine()
        self.Session = sessionmaker(bind=self.engine)
        
    def _create_db_engine(self):
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
    
    def read_sql(self, query: str) -> pd.DataFrame:
        print(f"Executing query...")
        try:
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    def get_status_by_source_id(self, table_name: str, source_id: str) -> Optional[Row]:
        session = self.Session()
        try:
            stmt = text(f"SELECT * FROM {table_name} WHERE source_id = :source_id LIMIT 1")
            result = session.execute(stmt, {"source_id": source_id}).fetchone()
            return result
        except Exception as e:
            print(f"Error getting status for source_id {source_id}: {e}")
            raise
        finally:
            session.close()

    def get_records_for_ai_processing(self, table_name: str, column_config: Dict[str, str], source_info: dict, processing_year: int, registry_config: dict) -> pd.DataFrame:
        """
        Queries the status table for records that are ready for AI processing,
        filtered by joining with the source and registry tables to filter by year.
        """
        # Source table details
        source_db = source_info['database']
        source_table = source_info['table']
        fully_qualified_source_table = f"`{source_db}`.`{source_table}`"
        
        # Registry table details from config
        registry_db = registry_config['database']
        registry_table = registry_config['table']
        year_column_name = registry_config['column']
        fully_qualified_registry_table = f"`{registry_db}`.`{registry_table}`"


        print(f"Querying for records from '{fully_qualified_source_table}' for year {processing_year} ready for AI processing in table: {table_name}")
        try:
            text_status_col = column_config['text_extract_status']
            json_status_col = column_config['json_valid_status']
            html_status_col = column_config['html_status']
            
            # MODIFIED: This query now joins with the caselaw_registry table to filter by year.
            # It assumes the registry table has a 'source_id' column to join on.
            query = text(f"""
                SELECT s.source_id, s.`{json_status_col}`, s.`{html_status_col}`
                FROM {table_name} s
                JOIN {fully_qualified_source_table} src ON s.source_id = src.id
                JOIN {fully_qualified_registry_table} reg ON s.source_id = reg.source_id
                WHERE 
                    reg.`{year_column_name}` = :processing_year
                    AND s.`{text_status_col}` = 'pass' 
                    AND (s.`{json_status_col}` != 'pass' OR s.`{html_status_col}` != 'pass')
            """)
            return pd.read_sql_query(query, self.engine, params={"processing_year": processing_year})
        except Exception as e:
            print(f"Error querying for AI-ready records: {e}")
            # MODIFIED: Added more specific guidance to the error message.
            print(f"This might be due to a permissions issue, missing columns (e.g., 'id' on source table, 'source_id'/'{year_column_name}' on registry table), or a mismatch in 'source_id' values.")
            raise

    def insert_initial_status(self, table_name: str, source_id: str, column_config: Dict[str, str]):
        session = self.Session()
        try:
            new_id = str(uuid.uuid4())
            cols = column_config
            stmt = text(f"""
                INSERT INTO {table_name} (
                    id, source_id, 
                    `{cols['text_extract_status']}`, `{cols['text_extract_duration']}`,
                    `{cols['json_valid_status']}`, `{cols['json_valid_duration']}`,
                    `{cols['html_status']}`, `{cols['html_duration']}`,
                    `{cols['token_input']}`, `{cols['token_output']}`,
                    `{cols['token_input_price']}`, `{cols['token_output_price']}`,
                    `{cols['start_time']}`, `{cols['end_time']}`
                )
                VALUES (
                    :id, :source_id, 
                    'not started', 0, 'not started', 0, 'not started', 0, 
                    0, 0, 0.0, 0.0, NULL, NULL
                )
            """)
            session.execute(stmt, {"id": new_id, "source_id": source_id})
            session.commit()
            print(f"Inserted initial status for source_id: {source_id}")
        except Exception as e:
            print(f"Error inserting initial status for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def update_step_result(
        self,
        table_name: str,
        source_id: str,
        step: str,
        status: str,
        duration: float,
        column_config: Dict[str, str],
        token_input: Optional[int] = None,
        token_output: Optional[int] = None,
        token_input_price: Optional[float] = None,
        token_output_price: Optional[float] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        session = self.Session()
        
        step_to_config_keys = {
            'text_extract': ('text_extract_status', 'text_extract_duration'),
            'json_valid': ('json_valid_status', 'json_valid_duration'),
            'jurismap_html': ('html_status', 'html_duration')
        }

        if step not in step_to_config_keys:
            raise ValueError(f"Invalid step name provided: {step}")
        
        status_key, duration_key = step_to_config_keys[step]
        
        set_clauses = [
            f"`{column_config[status_key]}` = :status",
            f"`{column_config[duration_key]}` = :duration"
        ]
        params = {"status": status, "duration": duration, "source_id": source_id}

        if token_input is not None:
            set_clauses.append(f"`{column_config['token_input']}` = :token_input")
            params["token_input"] = token_input
        
        if token_output is not None:
            set_clauses.append(f"`{column_config['token_output']}` = :token_output")
            params["token_output"] = token_output

        if token_input_price is not None:
            set_clauses.append(f"`{column_config['token_input_price']}` = :token_input_price")
            params["token_input_price"] = token_input_price

        if token_output_price is not None:
            set_clauses.append(f"`{column_config['token_output_price']}` = :token_output_price")
            params["token_output_price"] = token_output_price

        if start_time is not None:
            set_clauses.append(f"`{column_config['start_time']}` = :start_time")
            params["start_time"] = start_time
        
        if end_time is not None:
            set_clauses.append(f"`{column_config['end_time']}` = :end_time")
            params["end_time"] = end_time

        if status not in ['pass', 'failed']:
            raise ValueError("Invalid status value. Must be 'pass' or 'failed'.")

        try:
            stmt = text(f"""
                UPDATE {table_name} 
                SET {', '.join(set_clauses)}
                WHERE source_id = :source_id
            """)
            session.execute(stmt, params)
            session.commit()
            print(f"Updated {step} to '{status}' with duration {duration:.2f}s for source_id: {source_id}")
        except Exception as e:
            print(f"Error updating step result for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()
            
    # def upsert_metadata(self, metadata_config: Dict[str, str], source_id: str, filter_data: Dict[str, str]):
    #     """
    #     Inserts or updates a record in the caselaw_metadata table.
    #     This performs an "upsert" operation based on the source_id.

    #     Args:
    #         metadata_config (dict): Configuration for the metadata table.
    #         source_id (str): The unique source identifier for the case.
    #         filter_data (dict): A dictionary of the filter tags to be saved.
    #     """
    #     session = self.Session()
    #     table_name = f"`{metadata_config['database']}`.`{metadata_config['table']}`"
        
    #     # Prepare columns and values for the INSERT part
    #     columns = ['source_id'] + list(filter_data.keys())
    #     column_str = ', '.join([f"`{col}`" for col in columns])
    #     placeholders = ', '.join([f":{col}" for col in columns])
        
    #     # Prepare the UPDATE part for the ON DUPLICATE KEY clause
    #     update_clauses = [f"`{key}` = VALUES(`{key}`)" for key in filter_data.keys()]
    #     update_str = ', '.join(update_clauses)
        
    #     # Prepare the parameters dictionary
    #     params = {'source_id': source_id, **filter_data}
        
    #     try:
    #         # Use INSERT ... ON DUPLICATE KEY UPDATE for an "upsert"
    #         stmt = text(f"""
    #             INSERT INTO {table_name} ({column_str})
    #             VALUES ({placeholders})
    #             ON DUPLICATE KEY UPDATE {update_str}
    #         """)
    #         session.execute(stmt, params)
    #         session.commit()
    #         print(f"Successfully upserted metadata for source_id: {source_id}")
    #     except Exception as e:
    #         print(f"Error upserting metadata for source_id {source_id}: {e}")
    #         session.rollback()
    #         raise # Re-raise the exception to be handled by the caller
    #     finally:
    #         session.close()
