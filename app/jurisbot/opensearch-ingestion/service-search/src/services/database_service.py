from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from utils import get_logger

class DatabaseService:
    """Service for database operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize database service.
        
        Args:
            config: Database configuration
        """
        self.logger = get_logger(__name__)
        self.config = config
        self.source_engine = self._create_engine(config['database']['source'])
        self.dest_engine = self._create_engine(config['database']['destination'])
    
    def _create_engine(self, db_config: Dict[str, Any]):
        """Create SQLAlchemy engine with PyMySQL."""
        connection_string = (
            f"mysql+pymysql://{db_config.get('username')}:{db_config.get('password')}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        )
        
        self.logger.info(f"Connecting to database: {db_config['host']}/{db_config['name']}")
        
        return create_engine(
            connection_string, 
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600
        )
    
    def get_ingestion_status_summary(self, doc_type: str = 'caselaw') -> Dict[str, int]:
        """
        Get summary of ingestion status counts.
        
        Args:
            doc_type: Document type ('caselaw' or 'legislation')
        
        Returns:
            Dictionary with status counts
        """
        try:
            table_config = self.config['tables'][doc_type]
            table_name = table_config['enrichment_status_table']
            status_field = table_config['status_tracking_fields']['status_field']
            
            query = f"""
                SELECT 
                    COALESCE({status_field}, 'not started') as status,
                    COUNT(*) as count
                FROM {table_name}
                WHERE status_text_processor = 'pass'
                GROUP BY COALESCE({status_field}, 'not started')
            """
            
            with self.dest_engine.connect() as conn:
                result = conn.execute(text(query))
                
                # Convert result to dictionary
                summary = {}
                for row in result:
                    # Access by index since rows are tuples
                    # Index 0 is status, Index 1 is count
                    status = row[0]
                    count = row[1]
                    summary[status] = count
                
                # Add zero counts for missing statuses
                for status in ['pass', 'failed', 'started', 'not started']:
                    if status not in summary:
                        summary[status] = 0
                
                return summary
                
        except Exception as e:
            self.logger.error(f"Error getting status summary: {str(e)}")
            return {'pass': 0, 'failed': 0, 'started': 0, 'not started': 0}
    
    def update_ingestion_status(
        self,
        source_id: str,
        status: str,
        start_time: datetime = None,
        end_time: datetime = None,
        duration: float = None,
        doc_type: str = 'caselaw'
    ) -> bool:
        """
        Update or insert ingestion status for a source_id.
        This will UPDATE if the record exists with the exact source_id,
        otherwise it won't do anything (since record should already exist from text processing).
        
        Args:
            source_id: Source identifier
            status: Status value ('pass', 'failed', 'started', 'not started')
            start_time: Start time of ingestion
            end_time: End time of ingestion
            duration: Duration in seconds
            doc_type: Document type ('caselaw' or 'legislation')
        
        Returns:
            True if successful, False otherwise
        """
        try:
            table_config = self.config['tables'][doc_type]
            table_name = table_config['enrichment_status_table']
            fields = table_config['status_tracking_fields']
            
            # Build update query - we know the record exists because we got it from the join query
            update_parts = [f"{fields['status_field']} = :status"]
            params = {'source_id': source_id, 'status': status}
            
            if start_time is not None:
                update_parts.append(f"{fields['start_time_field']} = :start_time")
                params['start_time'] = start_time
            
            if end_time is not None:
                update_parts.append(f"{fields['end_time_field']} = :end_time")
                params['end_time'] = end_time
            
            if duration is not None:
                update_parts.append(f"{fields['duration_field']} = :duration")
                params['duration'] = duration
            
            update_query = text(f"""
                UPDATE {table_name}
                SET {', '.join(update_parts)}
                WHERE {fields['source_id']} = :source_id
            """)
            
            with self.dest_engine.begin() as conn:  # Use begin() for automatic transaction handling
                result = conn.execute(update_query, params)
                
                # Check if any rows were updated
                if result.rowcount > 0:
                    self.logger.debug(f"Updated status for {source_id}: {status}")
                    return True
                else:
                    self.logger.warning(f"No rows updated for {source_id} - record might not exist in {table_name}")
                    return False
                
        except Exception as e:
            self.logger.error(f"Error updating status for {source_id}: {str(e)}")
            self.logger.error(f"Query was: UPDATE {table_name} SET ... WHERE {fields['source_id']} = '{source_id}'")
            return False
    
    def get_caselaw_for_ingestion(
        self, 
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None,
        exclude_pass: bool = True
    ) -> pd.DataFrame:
        """
        Get caselaw records ready for ingestion (excluding already processed).
        
        Args:
            years: Filter by years (optional)
            jurisdiction_codes: Filter by jurisdiction codes (optional)
            exclude_pass: If True, exclude records with status='pass'
        
        Returns:
            DataFrame with caselaw records
        """
        table_config = self.config['tables']['caselaw']
        status_field = table_config['status_tracking_fields']['status_field']
        
        query = f"""
        SELECT 
            cr.{table_config['registry_fields']['source_id']} as source_id,
            cr.{table_config['registry_fields']['book_name']} as book_name,
            cr.{table_config['registry_fields']['neutral_citation']} as neutral_citation,
            cr.{table_config['registry_fields']['file_path']} as file_path,
            COALESCE(ces.{status_field}, 'not started') as current_status
        FROM 
            {table_config['registry_table']} cr
        INNER JOIN 
            {table_config['enrichment_status_table']} ces 
            ON cr.{table_config['registry_fields']['source_id']} = ces.{table_config['enrichment_fields']['source_id']}
        WHERE 
            cr.{table_config['registry_fields']['status_registration']} = 'pass'
            AND ces.{table_config['enrichment_fields']['status_text_processor']} = 'pass'
        """
        
        # Add status filter
        if exclude_pass:
            query += f" AND (ces.{status_field} IS NULL OR ces.{status_field} != 'pass')"
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"cr.{table_config['registry_fields']['year']} IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"cr.{table_config['registry_fields']['jurisdiction_code']} IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing caselaw query with filters: years={years}, jurisdictions={jurisdiction_codes}, exclude_pass={exclude_pass}")
        
        with self.dest_engine.connect() as conn:
            return pd.read_sql(query, conn)
    
    def get_legislation_for_ingestion(
        self,
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None,
        exclude_pass: bool = True
    ) -> pd.DataFrame:
        """
        Get legislation records ready for ingestion (excluding already processed).
        
        Args:
            years: Filter by years (optional)
            jurisdiction_codes: Filter by jurisdiction codes (optional)
            exclude_pass: If True, exclude records with status='pass'
        
        Returns:
            DataFrame with legislation records
        """
        table_config = self.config['tables']['legislation']
        status_field = table_config['status_tracking_fields']['status_field']
        
        query = f"""
        SELECT 
            lr.{table_config['registry_fields']['source_id']} as source_id,
            lr.{table_config['registry_fields']['book_name']} as book_name,
            lr.{table_config['registry_fields']['file_path']} as file_path,
            lc.{table_config['content_fields']['section_id']} as section_id,
            lc.{table_config['content_fields']['section_name']} as section_name,
            lm.{table_config['metadata_fields']['type_of_document']} as type_of_document,
            COALESCE(les.{status_field}, 'not started') as current_status
        FROM 
            {table_config['registry_table']} lr
        INNER JOIN 
            {table_config['enrichment_status_table']} les 
            ON lr.{table_config['registry_fields']['source_id']} = les.{table_config['enrichment_fields']['source_id']}
        INNER JOIN 
            {table_config['content_table']} lc
            ON lr.{table_config['registry_fields']['source_id']} = lc.{table_config['content_fields']['source_id']}
        INNER JOIN 
            {table_config['metadata_table']} lm
            ON lr.{table_config['registry_fields']['source_id']} = lm.{table_config['metadata_fields']['source_id']}
        WHERE 
            lr.{table_config['registry_fields']['status_registration']} = 'pass'
            AND les.{table_config['enrichment_fields']['status_text_processor']} = 'pass'
        """
        
        # Add status filter
        if exclude_pass:
            query += f" AND (les.{status_field} IS NULL OR les.{status_field} != 'pass')"
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"lr.{table_config['registry_fields']['year']} IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"lr.{table_config['registry_fields']['jurisdiction_code']} IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing legislation query with filters: years={years}, jurisdictions={jurisdiction_codes}, exclude_pass={exclude_pass}")
        
        with self.dest_engine.connect() as conn:
            return pd.read_sql(query, conn)