from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
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
        # Using pymysql for pure Python implementation
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
    
    def get_caselaw_for_ingestion(
        self, 
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get caselaw records ready for ingestion.
        
        Args:
            years: Filter by years (optional)
            jurisdiction_codes: Filter by jurisdiction codes (optional)
        
        Returns:
            DataFrame with caselaw records
        """
        table_config = self.config['tables']['caselaw']
        
        query = f"""
        SELECT 
            cr.{table_config['registry_fields']['source_id']} as source_id,
            cr.{table_config['registry_fields']['book_name']} as book_name,
            cr.{table_config['registry_fields']['neutral_citation']} as neutral_citation,
            cr.{table_config['registry_fields']['file_path']} as file_path
        FROM 
            {table_config['registry_table']} cr
        INNER JOIN 
            {table_config['enrichment_status_table']} ces 
            ON cr.{table_config['registry_fields']['source_id']} = ces.{table_config['enrichment_fields']['source_id']}
        WHERE 
            cr.{table_config['registry_fields']['status_registration']} = 'pass'
            AND ces.{table_config['enrichment_fields']['status_text_processor']} = 'pass'
        """
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"cr.{table_config['registry_fields']['year']} IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"cr.{table_config['registry_fields']['jurisdiction_code']} IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing caselaw query with filters: years={years}, jurisdictions={jurisdiction_codes}")
        
        with self.dest_engine.connect() as conn:
            return pd.read_sql(query, conn)
    
    def get_legislation_for_ingestion(
        self,
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get legislation records ready for ingestion.
        
        Args:
            years: Filter by years (optional)
            jurisdiction_codes: Filter by jurisdiction codes (optional)
        
        Returns:
            DataFrame with legislation records
        """
        table_config = self.config['tables']['legislation']
        
        query = f"""
        SELECT 
            lr.{table_config['registry_fields']['source_id']} as source_id,
            lr.{table_config['registry_fields']['book_name']} as book_name,
            lr.{table_config['registry_fields']['file_path']} as file_path,
            lc.{table_config['content_fields']['section_id']} as section_id,
            lc.{table_config['content_fields']['section_name']} as section_name,
            lm.{table_config['metadata_fields']['type_of_document']} as type_of_document
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
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"lr.{table_config['registry_fields']['year']} IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"lr.{table_config['registry_fields']['jurisdiction_code']} IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing legislation query with filters: years={years}, jurisdictions={jurisdiction_codes}")
        
        with self.dest_engine.connect() as conn:
            return pd.read_sql(query, conn)