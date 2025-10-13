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
            
            # Build update query
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
            
            with self.dest_engine.begin() as conn:
                result = conn.execute(update_query, params)
                
                if result.rowcount > 0:
                    self.logger.debug(f"Updated status for {source_id}: {status}")
                    return True
                else:
                    self.logger.warning(f"No rows updated for {source_id} - record might not exist in {table_name}")
                    return False
                
        except Exception as e:
            self.logger.error(f"Error updating status for {source_id}: {str(e)}")
            return False
    
    def get_caselaw_for_ingestion(
        self, 
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None,
        exclude_pass: bool = True
    ) -> pd.DataFrame:
        """
        Get caselaw records with ALL metadata ready for ingestion.
        
        Args:
            years: Filter by years (optional)
            jurisdiction_codes: Filter by jurisdiction codes (optional)
            exclude_pass: If True, exclude records with status='pass'
        
        Returns:
            DataFrame with caselaw records and full metadata
        """
        table_config = self.config['tables']['caselaw']
        status_field = table_config['status_tracking_fields']['status_field']
        
        # Query to get ALL fields from caselaw_metadata table (matching your screenshot)
        query = f"""
        SELECT 
            -- Registry fields (from caselaw_registry)
            cr.source_id,
            cr.book_name,
            cr.neutral_citation as registry_neutral_citation,
            cr.file_path,
            cr.year as registry_year,
            cr.jurisdiction_code as registry_jurisdiction,
            
            -- All metadata fields from caselaw_metadata (as per your screenshot)
            cm.count_char,
            cm.count_word,
            cm.file_no,
            cm.presiding_officer,
            cm.counsel,  -- Note: It's 'counsel' not 'counsels' in the DB
            cm.law_firm_agency,
            cm.court_type,
            cm.hearing_location,
            cm.judgment_date,
            cm.hearing_dates,
            cm.incident_date,
            cm.keywords,
            cm.legislation_cited,
            cm.affected_sectors,
            cm.practice_areas,
            cm.citation,
            cm.key_issues,
            cm.panelist,
            cm.orders,
            cm.decision,
            cm.cases_cited,
            cm.matter_type,
            cm.parties,
            cm.representation,
            cm.category,
            cm.bjs_number,
            cm.tribunal_name,
            cm.panel_or_division_name,
            cm.jurisdiction_code,
            cm.tribunal_code,
            cm.panel_or_division,
            cm.year,
            cm.decision_number,
            cm.decision_date,
            cm.primary_party,
            cm.secondary_party,
            cm.members,
            cm.member_info_json,
            cm.neutral_citation,
            
            -- Status field
            COALESCE(ces.{status_field}, 'not started') as current_status
        FROM 
            {table_config['registry_table']} cr
        INNER JOIN 
            {table_config['enrichment_status_table']} ces 
            ON cr.source_id = ces.source_id
        LEFT JOIN 
            caselaw_metadata cm
            ON cr.source_id = cm.source_id
        WHERE 
            cr.status_registration = 'pass'
            AND ces.status_text_processor = 'pass'
        """
        
        # Add status filter
        if exclude_pass:
            query += f" AND (ces.{status_field} IS NULL OR ces.{status_field} != 'pass')"
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"cr.year IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"cr.jurisdiction_code IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing caselaw query with ALL metadata fields")
        
        with self.dest_engine.connect() as conn:
            df = pd.read_sql(query, conn)
            
            # Use registry values where metadata is missing
            if 'neutral_citation' not in df.columns or df['neutral_citation'].isna().all():
                df['neutral_citation'] = df['registry_neutral_citation']
            if 'year' not in df.columns or df['year'].isna().all():
                df['year'] = df['registry_year']
            if 'jurisdiction_code' not in df.columns or df['jurisdiction_code'].isna().all():
                df['jurisdiction_code'] = df['registry_jurisdiction']
                
            return df
    
    def get_legislation_for_ingestion(
        self,
        years: Optional[List[int]] = None,
        jurisdiction_codes: Optional[List[str]] = None,
        exclude_pass: bool = True
    ) -> pd.DataFrame:
        """
        Get legislation records with ALL metadata ready for ingestion.
        
        Args:
            years: Filter by years (optional)
            jurisdiction_codes: Filter by jurisdiction codes (optional)
            exclude_pass: If True, exclude records with status='pass'
        
        Returns:
            DataFrame with legislation records and full metadata
        """
        table_config = self.config['tables']['legislation']
        status_field = table_config['status_tracking_fields']['status_field']
        
        # Query to get ALL fields from legislation_metadata table (matching your screenshot)
        query = f"""
        SELECT 
            -- Registry fields (from legislation_registry)
            lr.source_id,
            lr.book_name,
            lr.file_path,
            lr.year as registry_year,
            lr.jurisdiction_code as registry_jurisdiction,
            
            -- Content fields (from legislation_content)
            lc.section_id,
            lc.section_name,
            
            -- All metadata fields from legislation_metadata (as per your screenshot)
            lm.id,
            lm.count_char,
            lm.count_word,
            lm.title_of_legislation,
            lm.legislation_number,
            lm.type_of_document,
            lm.date_of_assent_or_making,
            lm.commencement_date,
            lm.gazette_notification_date,
            lm.second_reading_speech_dates,
            lm.enabling_act,
            lm.amended_legislation,
            lm.purpose_of_the_legislation,
            lm.administering_agency,
            lm.affected_sectors,
            lm.practice_areas,
            lm.keywords,
            
            -- Status field
            COALESCE(les.{status_field}, 'not started') as current_status
        FROM 
            {table_config['registry_table']} lr
        INNER JOIN 
            {table_config['enrichment_status_table']} les 
            ON lr.source_id = les.source_id
        INNER JOIN 
            {table_config['content_table']} lc
            ON lr.source_id = lc.source_id
        LEFT JOIN 
            {table_config['metadata_table']} lm
            ON lr.source_id = lm.source_id
        WHERE 
            lr.status_registration = 'pass'
            AND les.status_text_processor = 'pass'
        """
        
        # Add status filter
        if exclude_pass:
            query += f" AND (les.{status_field} IS NULL OR les.{status_field} != 'pass')"
        
        conditions = []
        if years:
            years_str = ','.join(map(str, years))
            conditions.append(f"lr.year IN ({years_str})")
        
        if jurisdiction_codes:
            codes_str = ','.join([f"'{code}'" for code in jurisdiction_codes])
            conditions.append(f"lr.jurisdiction_code IN ({codes_str})")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        self.logger.info(f"Executing legislation query with ALL metadata fields")
        
        with self.dest_engine.connect() as conn:
            df = pd.read_sql(query, conn)
            
            # Use registry values where metadata is missing
            if 'year' not in df.columns or df['year'].isna().all():
                df['year'] = df['registry_year']
            if 'jurisdiction_code' not in df.columns or df['jurisdiction_code'].isna().all():
                df['jurisdiction_code'] = df['registry_jurisdiction']
                
            return df