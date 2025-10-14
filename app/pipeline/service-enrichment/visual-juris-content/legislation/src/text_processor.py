import os
import time
import yaml
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from src.html_transformer import HtmlTransformer
from src.section_extractor import SectionExtractor
from utils.s3_manager import S3Manager
import logging

logger = logging.getLogger(__name__)

class TextProcessor:
    """
    Handles the juriscontent.html generation part of the pipeline by identifying
    and processing cases that have been successfully downloaded.
    """
    
    # File size limit for processing (50MB)
    MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
    
    def __init__(self, config: dict):
        """
        Initializes the TextProcessor.

        Args:
            config (dict): The application configuration dictionary.
        """
        self.config = config
        
        # Load the headless rules configuration for the transformer
        try:
            with open(config['headless_rules_path'], 'r') as f:
                headless_rules_config = yaml.safe_load(f)
            logger.info("Successfully loaded headless HTML processing rules.")
        except Exception as e:
            logger.critical(f"Could not load headless_rules.yaml from path: {config['headless_rules_path']}. Aborting. Error: {e}")
            raise

        # Get the heading hierarchy rules path
        heading_hierarchy_rules_path = config.get('heading_hierarchy_rules_path', 'config/heading_hierarchy_rules.yaml')

        # Initialize the main transformer with both rule sets
        self.html_transformer = HtmlTransformer(
            headless_rules_config=headless_rules_config,
            heading_hierarchy_rules_path=heading_hierarchy_rules_path
        )
        
        # Initialize section extractor
        self.section_extractor = SectionExtractor()
        
        # Initialize the S3 manager using the region from the config
        self.s3_manager = S3Manager(region_name=config['aws']['default_region'])
        
        # This processor connects to the destination database
        self.dest_db = DatabaseConnector(db_config=config['database']['destination'])
        
        # Print summary of loaded rules for debugging
        logger.info("Heading Hierarchy Rules loaded:")
        logger.info(self.html_transformer.get_hierarchy_rules_summary())

    def process_cases(self, process_sections: bool = True):
        """
        Main method to run the juriscontent.html generation pipeline.
        It iterates through configured years and jurisdictions to find and process cases.
        
        Args:
            process_sections (bool): If True, also extract sections after juriscontent generation
        """
        # Configuration for tables and S3
        dest_table_info = self.config['tables']['tables_to_write'][0]
        dest_table = dest_table_info['table']
        status_column = dest_table_info['step_columns']['text_extract']['status']

        registry_config = self.config.get('tables_registry')
        if not registry_config:
            logger.critical("'tables_registry' configuration not found in config.yaml. Aborting.")
            return

        registry_table = registry_config['table']
        registry_year_col = registry_config['year_column']
        download_status_col = registry_config['download_status_column']
        processing_years = registry_config.get('processing_years', [])
        jurisdictions_to_process = registry_config.get('jurisdiction_codes', [])

        # If processing_years is not specified, process all years.
        years_to_iterate = processing_years if processing_years else [None]

        # If jurisdiction_codes is empty, fetch all available jurisdictions from the registry.
        if not jurisdictions_to_process:
            logger.info("Config 'jurisdiction_codes' is empty. Fetching all available jurisdictions from the registry.")
            try:
                juris_df = self.dest_db.read_sql(f"SELECT DISTINCT jurisdiction_code FROM {registry_table} WHERE jurisdiction_code IS NOT NULL AND jurisdiction_code != ''")
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
        
        logger.info(f"Starting processing for jurisdictions: {jurisdictions_to_process}")
        logger.info(f"Processing mode: Juriscontent + Sections = {process_sections}")

        for year in years_to_iterate:
            if year:
                logger.info(f"===== Processing Year: {year} =====")
            else:
                logger.info(f"===== Processing for All Years =====")

            for jurisdiction in jurisdictions_to_process:
                jurisdiction_info = jurisdiction_lookup.get(jurisdiction)
                if not jurisdiction_info:
                    logger.warning(f"Configuration for jurisdiction '{jurisdiction}' not found in 'tables_to_read'. Skipping.")
                    continue
                    
                s3_base_folder = jurisdiction_info['s3_folder']
                logger.info(f"--- Checking Jurisdiction: {jurisdiction} ---")

                try:
                    # Query for cases that have been downloaded but not yet processed for text extraction
                    query_parts = [
                        f"SELECT reg.source_id",
                        f"FROM {registry_table} AS reg",
                        f"LEFT JOIN {dest_table} AS dest ON reg.source_id = dest.source_id",
                        f"WHERE reg.jurisdiction_code = :jurisdiction",
                        f"AND reg.{download_status_col} = 'pass'"
                    ]
                    params = {"jurisdiction": jurisdiction}

                    if year is not None:
                        query_parts.append(f"AND reg.{registry_year_col} = :year")
                        params["year"] = year
                    
                    query_parts.append(f"AND (dest.source_id IS NULL OR dest.{status_column} IS NULL OR dest.{status_column} != 'pass')")
                    
                    query = "\n".join(query_parts)
                    cases_to_process_df = self.dest_db.read_sql(query, params=params)
                    
                    log_message_year = f"for year {year} " if year else "for all years "
                    logger.info(f"Found {len(cases_to_process_df)} cases {log_message_year}from registry requiring processing.")

                except Exception as e:
                    logger.error(f"Could not query the registry for jurisdiction {jurisdiction}. Skipping. Error: {e}")
                    continue

                if cases_to_process_df.empty:
                    continue

                for index, row in cases_to_process_df.iterrows():
                    source_id = str(row['source_id'])
                    logger.info(f"\n{'='*70}")
                    logger.info(f"Processing case: {source_id} ({index + 1}/{len(cases_to_process_df)})")
                    logger.info(f"{'='*70}")
                    
                    status_row = self.dest_db.get_status_by_source_id(dest_table, source_id)
                    if not status_row:
                        logger.info(f"No status record for {source_id}. Creating new one.")
                        try:
                            self.dest_db.insert_initial_status(table_name=dest_table, source_id=source_id)
                        except Exception as e:
                            logger.error(f"Failed to insert initial status for {source_id}. Skipping. Error: {e}")
                            continue

                    case_folder = os.path.join(s3_base_folder, source_id)
                    source_html_key = os.path.join(case_folder, filenames['source_html'])
                    output_html_key = os.path.join(case_folder, filenames['extracted_html'])
                    
                    # STAGE 1: Generate juriscontent.html
                    logger.info(f"[STAGE 1] Generating juriscontent.html for {source_id}")
                    juriscontent_success = self._generate_and_save_juriscontent(
                        s3_bucket, source_html_key, output_html_key, dest_table, source_id
                    )
                    
                    # STAGE 2: Extract sections (only if Stage 1 succeeded and process_sections is True)
                    if juriscontent_success and process_sections:
                        logger.info(f"[STAGE 2] Extracting sections for {source_id}")
                        sections_folder = os.path.join(case_folder, 'section-level-content')
                        self._extract_and_save_sections(
                            s3_bucket, output_html_key, sections_folder, 
                            dest_table, source_id
                        )
                    elif not juriscontent_success:
                        logger.warning(f"Skipping section extraction for {source_id} - juriscontent generation failed")
            
        logger.info("\n" + "="*70)
        logger.info("All processing complete for all configured years and jurisdictions.")
        logger.info("="*70)

    def _generate_and_save_juriscontent(self, bucket: str, source_html_key: str, 
                                       output_html_key: str, status_table: str, source_id: str) -> bool:
        """
        Handles HTML download, transformation, saving the new HTML to S3,
        verifying file size, and updating the status database.
        
        Returns:
            bool: True if successful, False otherwise
        """
        start_time_utc = datetime.now(timezone.utc)
        
        dest_table_info = self.config['tables']['tables_to_write'][0]
        step_columns_config = dest_table_info['step_columns']
        
        try:
            # Check file size before processing
            source_size = self.s3_manager.get_file_size(bucket, source_html_key)
            if source_size == 0:
                raise Exception(f"Source file is empty or not found: s3://{bucket}/{source_html_key}")
            
            if source_size > self.MAX_FILE_SIZE_BYTES:
                logger.warning(f"Large file detected ({source_size} bytes) for {source_id}. Processing may be slow.")
            
            html_content = self.s3_manager.get_file_content(bucket, source_html_key)
            
            # Use the new transformer which intelligently handles both file types
            juriscontent_html = self.html_transformer.transform(html_content)
            
            self.s3_manager.save_text_file(bucket, output_html_key, juriscontent_html, content_type='text/html')

            # --- VERIFICATION STEP ---
            output_size = self.s3_manager.get_file_size(bucket, output_html_key)

            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()

            if output_size > 0 and source_size > 0:
                # Success case
                logger.info(f"✅ Juriscontent generation SUCCESS: {output_size} bytes")
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'pass', duration, 
                    start_time_utc, end_time_utc, step_columns_config
                )
                return True
            else:
                # Failure case
                logger.error(f"❌ Juriscontent generation FAILED: output size is {output_size} bytes")
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'failed', duration,
                    start_time_utc, end_time_utc, step_columns_config
                )
                return False

        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            logger.error(f"❌ Juriscontent generation FAILED: {type(e).__name__}: {str(e)}")
            self.dest_db.update_step_result(
                status_table, source_id, 'text_extract', 'failed', duration,
                start_time_utc, end_time_utc, step_columns_config
            )
            return False

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