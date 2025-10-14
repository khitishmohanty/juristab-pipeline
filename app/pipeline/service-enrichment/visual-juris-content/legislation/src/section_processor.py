import os
import time
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from src.section_extractor import SectionExtractor
from utils.s3_manager import S3Manager
import logging

logger = logging.getLogger(__name__)

class SectionProcessor:
    """
    Processes juriscontent.html files to extract sections based on H1 headings
    and stores them as individual text files in S3.
    """
    
    def __init__(self, config: dict):
        """
        Initializes the SectionProcessor.

        Args:
            config (dict): The application configuration dictionary.
        """
        self.config = config
        self.section_extractor = SectionExtractor()
        
        # Initialize the S3 manager
        self.s3_manager = S3Manager(region_name=config['aws']['default_region'])
        
        # Connect to destination database
        self.dest_db = DatabaseConnector(db_config=config['database']['destination'])
    
    def process_sections(self):
        """
        Main method to run the section extraction pipeline.
        Finds cases with completed juriscontent.html and extracts sections.
        """
        # Configuration for tables and S3
        dest_table_info = self.config['tables']['tables_to_write'][0]
        dest_table = dest_table_info['table']
        
        # Check if section extraction columns exist
        step_columns_config = dest_table_info.get('step_columns', {})
        if 'section_extract' not in step_columns_config:
            logger.error("'section_extract' step columns not configured. Please update config.yaml")
            return
        
        registry_config = self.config.get('tables_registry')
        if not registry_config:
            logger.critical("'tables_registry' configuration not found in config.yaml. Aborting.")
            return

        registry_table = registry_config['table']
        registry_year_col = registry_config['year_column']
        processing_years = registry_config.get('processing_years', [])
        jurisdictions_to_process = registry_config.get('jurisdiction_codes', [])

        # If processing_years is not specified, process all years
        years_to_iterate = processing_years if processing_years else [None]

        # If jurisdiction_codes is empty, fetch all available jurisdictions
        if not jurisdictions_to_process:
            logger.info("Config 'jurisdiction_codes' is empty. Fetching all available jurisdictions from the registry.")
            try:
                juris_df = self.dest_db.read_sql(
                    f"SELECT DISTINCT jurisdiction_code FROM {registry_table} WHERE jurisdiction_code IS NOT NULL AND jurisdiction_code != ''"
                )
                jurisdictions_to_process = juris_df['jurisdiction_code'].tolist()
                if not jurisdictions_to_process:
                    logger.warning("No jurisdictions found in the registry. Aborting.")
                    return
            except Exception as e:
                logger.critical(f"Could not fetch jurisdictions from registry. Aborting. Error: {e}")
                return

        s3_bucket = self.config['aws']['s3']['bucket_name']
        filenames = self.config['enrichment_filenames']
        
        tables_to_read_config = self.config['tables']['tables_to_read']
        jurisdiction_lookup = {item['jurisdiction']: item for item in tables_to_read_config}
        
        logger.info(f"Starting section extraction for jurisdictions: {jurisdictions_to_process}")

        for year in years_to_iterate:
            if year:
                logger.info(f"===== Processing Year: {year} =====")
            else:
                logger.info(f"===== Processing for All Years =====")

            for jurisdiction in jurisdictions_to_process:
                jurisdiction_info = jurisdiction_lookup.get(jurisdiction)
                if not jurisdiction_info:
                    logger.warning(f"Configuration for jurisdiction '{jurisdiction}' not found. Skipping.")
                    continue
                    
                s3_base_folder = jurisdiction_info['s3_folder']
                logger.info(f"--- Checking Jurisdiction: {jurisdiction} ---")

                try:
                    # Query for cases that have completed juriscontent generation but not section extraction
                    query_parts = [
                        f"SELECT reg.source_id",
                        f"FROM {registry_table} AS reg",
                        f"INNER JOIN {dest_table} AS dest ON reg.source_id = dest.source_id",
                        f"WHERE reg.jurisdiction_code = :jurisdiction",
                        f"AND dest.status_juriscontent_html = 'pass'"
                    ]
                    params = {"jurisdiction": jurisdiction}

                    if year is not None:
                        query_parts.append(f"AND reg.{registry_year_col} = :year")
                        params["year"] = year
                    
                    # Only process if section extraction hasn't been completed
                    query_parts.append(
                        f"AND (dest.status_juriscontent_section_extract IS NULL "
                        f"OR dest.status_juriscontent_section_extract != 'pass')"
                    )
                    
                    query = "\n".join(query_parts)
                    cases_to_process_df = self.dest_db.read_sql(query, params=params)
                    
                    log_message_year = f"for year {year} " if year else "for all years "
                    logger.info(f"Found {len(cases_to_process_df)} cases {log_message_year}requiring section extraction.")

                except Exception as e:
                    logger.error(f"Could not query for jurisdiction {jurisdiction}. Skipping. Error: {e}")
                    continue

                if cases_to_process_df.empty:
                    continue

                for index, row in cases_to_process_df.iterrows():
                    source_id = str(row['source_id'])
                    logger.info(f"- Extracting sections for case: {source_id}")
                    
                    case_folder = os.path.join(s3_base_folder, source_id)
                    juriscontent_key = os.path.join(case_folder, filenames['extracted_html'])
                    sections_folder = os.path.join(case_folder, 'section-level-content')
                    
                    self._extract_and_save_sections(
                        s3_bucket, juriscontent_key, sections_folder, 
                        dest_table, source_id
                    )
        
        logger.info("--- Section extraction completed for all configured years and jurisdictions. ---")
    
    def _extract_and_save_sections(self, bucket: str, juriscontent_key: str, 
                               sections_folder: str, status_table: str, source_id: str):
        """
        Extracts sections from juriscontent.html and saves them to S3,
        then updates both legislation_sections and enrichment status tables.
        """
        start_time_utc = datetime.now(timezone.utc)
        
        dest_table_info = self.config['tables']['tables_to_write'][0]
        step_columns_config = dest_table_info['step_columns']
        
        # Update status to 'started'
        try:
            self.dest_db.update_section_extract_status(
                status_table, source_id, 'started'
            )
        except Exception as e:
            logger.warning(f"Could not update status to 'started' for {source_id}: {e}")
        
        try:
            # IMPORTANT: Clear existing section-level-content folder if it exists
            logger.info(f"Checking for existing section-level-content folder...")
            self.s3_manager.clear_and_recreate_folder(bucket, sections_folder)
            
            # Also clear existing sections from database
            logger.info(f"Clearing existing sections from database...")
            deleted_count = self.dest_db.clear_existing_sections(source_id)
            
            # Download juriscontent.html from S3
            html_content = self.s3_manager.get_file_content(bucket, juriscontent_key)
            
            # Extract sections
            sections = self.section_extractor.extract_sections(html_content)
            
            if not sections:
                logger.warning(f"No sections extracted for {source_id}")
                raise Exception("No sections found in juriscontent.html")
            
            logger.info(f"Extracted {len(sections)} sections from {source_id}")
            
            # Save each section to S3 and database
            sections_saved = 0
            for section in sections:
                section_id = section['section_id']
                section_content = self.section_extractor.format_section_content(section)
                
                # Create filename: miniviewer_1.txt, miniviewer_2.txt, etc.
                section_filename = f"miniviewer_{section_id}.txt"
                section_key = os.path.join(sections_folder, section_filename)
                
                # Save to S3
                self.s3_manager.save_text_file(
                    bucket, section_key, section_content, content_type='text/plain'
                )
                
                # Save to database (legislation_sections table)
                self.dest_db.insert_legislation_section(
                    source_id=source_id,
                    section_id=section_id
                )
                
                sections_saved += 1
            
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            
            logger.info(f"✅ Section extraction SUCCESS: {sections_saved} sections saved")
            
            # Update status to 'pass'
            self.dest_db.update_step_result(
                status_table, source_id, 'section_extract', 'pass', duration,
                start_time_utc, end_time_utc, step_columns_config
            )
            
        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            logger.error(f"❌ Section extraction FAILED: {type(e).__name__}: {str(e)}")
            
            # Update status to 'failed'
            self.dest_db.update_step_result(
                status_table, source_id, 'section_extract', 'failed', duration,
                start_time_utc, end_time_utc, step_columns_config
            )