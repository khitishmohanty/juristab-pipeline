import os
import re
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine, text, Row
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from typing import Optional, Dict, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Handles all database interactions."""
    
    # Whitelist of valid table name patterns for security
    VALID_TABLE_PATTERN = re.compile(r'^[a-zA-Z0-9_]+$')
    
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
            logger.info(f"Creating database engine for: {self.db_config['name']}")
            return create_engine(
                connection_url,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600    # Recycle connections after 1 hour
            )
        except Exception as e:
            logger.error(f"Error creating database engine: {e}")
            raise
    
    def _validate_table_name(self, table_name: str) -> None:
        """
        Validates table name to prevent SQL injection.
        
        Args:
            table_name (str): The table name to validate
            
        Raises:
            ValueError: If table name contains invalid characters
        """
        if not self.VALID_TABLE_PATTERN.match(table_name):
            raise ValueError(f"Invalid table name: {table_name}. Only alphanumeric and underscore allowed.")
    
    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope around operations.
        
        Usage:
            with connector.session_scope() as session:
                # do work
                pass
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def read_sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a pandas DataFrame.
        Supports parameterized queries for safety.
        """
        logger.debug(f"Executing query with params: {params is not None}")
        try:
            # Use SQLAlchemy's text() construct for safe parameter binding
            return pd.read_sql_query(sql=text(query), con=self.engine, params=params)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def get_status_by_source_id(self, table_name: str, source_id: str) -> Optional[Row]:
        self._validate_table_name(table_name)
        session = self.Session()
        try:
            stmt = text(f"SELECT * FROM {table_name} WHERE source_id = :source_id LIMIT 1")
            result = session.execute(stmt, {"source_id": source_id}).fetchone()
            return result
        except Exception as e:
            logger.error(f"Error getting status for source_id {source_id}: {e}")
            raise
        finally:
            session.close()

    def insert_initial_status(self, table_name: str, source_id: str) -> str:
        self._validate_table_name(table_name)
        session = self.Session()
        try:
            new_id = str(uuid.uuid4())
            stmt = text(f"""
                INSERT INTO {table_name} (
                    id, source_id, 
                    status_text_processor, duration_text_processor
                )
                VALUES (:id, :source_id, 'not started', 0)
            """)
            session.execute(stmt, {"id": new_id, "source_id": source_id})
            session.commit()
            logger.info(f"Inserted initial status for source_id: {source_id}")
            return new_id
        except Exception as e:
            logger.error(f"Error inserting initial status for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def update_step_result(self, table_name: str, source_id: str, step: str, status: str, 
                          duration: float, start_time: datetime, end_time: datetime, 
                          step_columns: dict):
        """
        Updates the status, duration, start time, and end time for a specific processing step.
        """
        self._validate_table_name(table_name)
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
            stmt = text(f"""
                UPDATE {table_name} 
                SET {status_col} = :status, 
                    {duration_col} = :duration,
                    {start_time_col} = :start_time,
                    {end_time_col} = :end_time
                WHERE source_id = :source_id
            """)
            session.execute(stmt, {
                "status": status, 
                "duration": duration, 
                "start_time": start_time,
                "end_time": end_time,
                "source_id": source_id
            })
            session.commit()
            logger.info(f"Updated {step} to '{status}' with duration {duration:.2f}s for source_id: {source_id}")
        except Exception as e:
            logger.error(f"Error updating step result for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_metadata_counts(self, table_name: str, source_id: str, char_count_col: str, 
                              word_count_col: str, char_count: int, word_count: int):
        """
        Updates or inserts character and word counts in the metadata table.
        If a record with the source_id exists, it's updated. Otherwise, a new record is inserted.
        """
        self._validate_table_name(table_name)
        session = self.Session()
        try:
            # Check if the record exists
            stmt_select = text(f"SELECT source_id FROM {table_name} WHERE source_id = :source_id")
            result = session.execute(stmt_select, {"source_id": source_id}).fetchone()

            if result:
                # Update existing record
                stmt_update = text(f"""
                    UPDATE {table_name}
                    SET {char_count_col} = :char_count,
                        {word_count_col} = :word_count
                    WHERE source_id = :source_id
                """)
                session.execute(stmt_update, {
                    "char_count": char_count,
                    "word_count": word_count,
                    "source_id": source_id
                })
                logger.info(f"Updated metadata for source_id: {source_id}")
            else:
                # Insert new record
                new_id = str(uuid.uuid4())
                stmt_insert = text(f"""
                    INSERT INTO {table_name} (id, source_id, {char_count_col}, {word_count_col})
                    VALUES (:id, :source_id, :char_count, :word_count)
                """)
                session.execute(stmt_insert, {
                    "id": new_id,
                    "source_id": source_id,
                    "char_count": char_count,
                    "word_count": word_count
                })
                logger.info(f"Inserted new metadata for source_id: {source_id}")

            session.commit()
        except Exception as e:
            logger.error(f"Error upserting metadata for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def insert_legislation_section(self, source_id: str, section_id: int):
        """
        Inserts a record into the legislation_sections table.
        
        Args:
            source_id (str): The source_id of the legislation
            section_id (int): The section number (1, 2, 3, etc.)
        """
        session = self.Session()
        try:
            new_id = str(uuid.uuid4())
            stmt = text("""
                INSERT INTO legislation_sections (id, source_id, section_id)
                VALUES (:id, :source_id, :section_id)
            """)
            session.execute(stmt, {
                "id": new_id,
                "source_id": source_id,
                "section_id": section_id
            })
            session.commit()
            logger.debug(f"Inserted section {section_id} for source_id: {source_id}")
        except Exception as e:
            logger.error(f"Error inserting section for source_id {source_id}, section_id {section_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def update_section_extract_status(self, table_name: str, source_id: str, status: str):
        """
        Updates the status_juriscontent_section_extract column.
        
        Args:
            table_name (str): The name of the enrichment status table
            source_id (str): The source_id to update
            status (str): One of 'pass', 'failed', 'not started', 'started'
        """
        self._validate_table_name(table_name)
        session = self.Session()
        
        valid_statuses = ['pass', 'failed', 'not started', 'started']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status value. Must be one of {valid_statuses}")
        
        try:
            stmt = text(f"""
                UPDATE {table_name}
                SET status_juriscontent_section_extract = :status
                WHERE source_id = :source_id
            """)
            session.execute(stmt, {
                "status": status,
                "source_id": source_id
            })
            session.commit()
            logger.info(f"Updated section extract status to '{status}' for source_id: {source_id}")
        except Exception as e:
            logger.error(f"Error updating section extract status for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def clear_existing_sections(self, source_id: str):
        """
        Deletes all existing section records for a given source_id.
        Useful for reprocessing documents.
        
        Args:
            source_id (str): The source_id whose sections should be cleared
        """
        session = self.Session()
        try:
            stmt = text("""
                DELETE FROM legislation_sections
                WHERE source_id = :source_id
            """)
            result = session.execute(stmt, {"source_id": source_id})
            session.commit()
            deleted_count = result.rowcount
            logger.info(f"Cleared {deleted_count} existing sections for source_id: {source_id}")
            return deleted_count
        except Exception as e:
            logger.error(f"Error clearing sections for source_id {source_id}: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def get_section_count(self, source_id: str) -> int:
        """
        Gets the count of sections for a given source_id.
        
        Args:
            source_id (str): The source_id to check
            
        Returns:
            int: Number of sections found
        """
        session = self.Session()
        try:
            stmt = text("""
                SELECT COUNT(*) as count
                FROM legislation_sections
                WHERE source_id = :source_id
            """)
            result = session.execute(stmt, {"source_id": source_id}).fetchone()
            return result.count if result else 0
        except Exception as e:
            logger.error(f"Error getting section count for source_id {source_id}: {e}")
            raise
        finally:
            session.close()
            
    def update_heading_detection_metrics(self, table_name: str, source_id: str, 
                                     token_info: dict):
        """
        Update token usage and pricing information for heading detection.
        
        Args:
            table_name: Name of enrichment status table
            source_id: Source ID being processed
            token_info: Dict with token counts and pricing info
        """
        self._validate_table_name(table_name)
        session = self.Session()
        
        try:
            stmt = text(f"""
                UPDATE {table_name}
                SET token_input_juriscontent_html = :input_tokens,
                    token_output_juriscontent_html = :output_tokens,
                    token_input_price_juriscontent_html = :input_price,
                    token_output_price_juriscontent_html = :output_price
                WHERE source_id = :source_id
            """)
            
            session.execute(stmt, {
                "input_tokens": token_info['input_tokens'],
                "output_tokens": token_info['output_tokens'],
                "input_price": token_info['input_price'],
                "output_price": token_info['output_price'],
                "source_id": source_id
            })
            
            session.commit()
            logger.info(f"Updated heading detection metrics for source_id: {source_id}")
            
        except Exception as e:
            logger.error(f"Error updating heading detection metrics: {e}")
            session.rollback()
            raise
        finally:
            session.close()
            
    def update_heading_detection_metadata(self, table_name: str, source_id: str, 
                                         heading_metadata: dict):
        """
        Update heading detection metadata including token usage, pricing, and heading counts.
        
        Args:
            table_name: Name of enrichment status table
            source_id: Source ID being processed
            heading_metadata: Dict with all heading detection information:
                - input_tokens (int)
                - output_tokens (int)
                - input_price (float)
                - output_price (float)
                - before_processing_heading_count (int)
                - after_processing_heading_count (int)
                - genai_path_used (bool)
        """
        self._validate_table_name(table_name)
        session = self.Session()
        
        try:
            stmt = text(f"""
                UPDATE {table_name}
                SET token_input_juriscontent_html = :input_tokens,
                    token_output_juriscontent_html = :output_tokens,
                    token_input_price_juriscontent_html = :input_price,
                    token_output_price_juriscontent_html = :output_price,
                    juriscontent_html_before_processing_heading_count = :before_count,
                    juriscontent_html_after_processing_heading_count = :after_count,
                    juriscontent_html_genai_path = :genai_path
                WHERE source_id = :source_id
            """)
            
            session.execute(stmt, {
                "input_tokens": heading_metadata.get('input_tokens', 0),
                "output_tokens": heading_metadata.get('output_tokens', 0),
                "input_price": heading_metadata.get('input_price', 0.0),
                "output_price": heading_metadata.get('output_price', 0.0),
                "before_count": heading_metadata.get('before_processing_heading_count', 0),
                "after_count": heading_metadata.get('after_processing_heading_count', 0),
                "genai_path": 'true' if heading_metadata.get('genai_path_used', False) else 'false',
                "source_id": source_id
            })
            
            session.commit()
            
            genai_status = "USED" if heading_metadata.get('genai_path_used', False) else "NOT USED"
            logger.info(f"Updated heading metadata for source_id: {source_id}")
            logger.info(f"  - Gemini path: {genai_status}")
            logger.info(f"  - Headings before: {heading_metadata.get('before_processing_heading_count', 0)}")
            logger.info(f"  - Headings after: {heading_metadata.get('after_processing_heading_count', 0)}")
            
        except Exception as e:
            logger.error(f"Error updating heading detection metadata: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def update_content_verification(self, table_name: str, source_id: str, 
                                   similarity_score: float, status: str):
        """
        Update content verification score and status in the database.
        
        Args:
            table_name (str): Name of enrichment status table
            source_id (str): Source ID being processed
            similarity_score (float): Similarity score between 0.0 and 1.0
            status (str): Verification status ('pass', 'failed', or 'not started')
        """
        self._validate_table_name(table_name)
        session = self.Session()
        
        valid_statuses = ['pass', 'failed', 'not started']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status value. Must be one of {valid_statuses}")
        
        try:
            stmt = text(f"""
                UPDATE {table_name}
                SET juriscontent_html_content_verification_score = :score,
                    juriscontent_html_content_verification_status = :status
                WHERE source_id = :source_id
            """)
            
            session.execute(stmt, {
                "score": similarity_score,
                "status": status,
                "source_id": source_id
            })
            
            session.commit()
            
            logger.info(
                f"Updated content verification for source_id: {source_id} | "
                f"Score: {similarity_score:.4f} | Status: {status.upper()}"
            )
            
        except Exception as e:
            logger.error(f"Error updating content verification: {e}")
            session.rollback()
            raise
        finally:
            session.close()