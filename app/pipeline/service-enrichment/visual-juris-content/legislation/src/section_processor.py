import os
import time
import re
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from src.section_extractor import SectionExtractor
from src.content_verifier import ContentVerifier
from src.html_content_extractor import HtmlContentExtractor
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
        
        # Initialize content verifier if enabled
        verification_config = config.get('content_verification', {})
        self.verification_enabled = verification_config.get('enabled', True)
        
        if self.verification_enabled:
            pass_threshold = verification_config.get('pass_threshold', 0.85)
            self.content_verifier = ContentVerifier(pass_threshold=pass_threshold)
            logger.info(f"Content verification ENABLED (threshold={pass_threshold})")
        else:
            self.content_verifier = None
            logger.info("Content verification DISABLED")
    
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
                    
                    sections_saved = self._extract_and_save_sections(
                        s3_bucket, juriscontent_key, sections_folder, 
                        dest_table, source_id
                    )
                    
                    # Verify content if enabled and sections were saved
                    if sections_saved and self.verification_enabled:
                        logger.info(f"- Verifying content for case: {source_id}")
                        source_html_key = os.path.join(case_folder, filenames['source_html'])
                        self._verify_section_content(
                            s3_bucket, source_html_key, sections_folder,
                            dest_table, source_id
                        )
        
        logger.info("--- Section extraction completed for all configured years and jurisdictions. ---")
    
    def _extract_and_save_sections(self, bucket: str, juriscontent_key: str, 
                               sections_folder: str, status_table: str, source_id: str) -> bool:
        """
        Extracts sections from juriscontent.html and saves them to S3,
        then updates both legislation_sections and enrichment status tables.
        
        Returns:
            bool: True if successful, False if failed
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
            
            return True
            
        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            logger.error(f"✗ Section extraction FAILED: {type(e).__name__}: {str(e)}")
            
            # Update status to 'failed'
            self.dest_db.update_step_result(
                status_table, source_id, 'section_extract', 'failed', duration,
                start_time_utc, end_time_utc, step_columns_config
            )
            
            return False
    
    def _verify_section_content(self, bucket: str, source_html_key: str,
                                sections_folder: str, status_table: str, source_id: str):
        """
        Verify that concatenated section content matches the original miniviewer.html content.
        
        NEW BEHAVIOR:
        1. Extract text from miniviewer.html (excluding notes)
        2. Concatenate all section .txt files
        3. Save both as miniviewer_original.txt and miniviewer_constructed.txt
        4. Compare the two for verification
        
        Args:
            bucket (str): S3 bucket name
            source_html_key (str): S3 key for original miniviewer.html
            sections_folder (str): S3 folder containing section files
            status_table (str): Name of status table
            source_id (str): Source ID being processed
        """
        try:
            logger.info(f"Starting content verification for {source_id}")
            
            # Initialize HTML content extractor
            html_extractor = HtmlContentExtractor()
            
            # Step 1: Check if source HTML file exists
            if not self.s3_manager.check_file_exists(bucket, source_html_key):
                logger.warning(f"Source HTML file not found: s3://{bucket}/{source_html_key}")
                logger.warning("Skipping content verification - marking as 'not started'")
                
                self.dest_db.update_content_verification(
                    status_table, source_id, 0.0, 'not started'
                )
                return
            
            # Step 2: Download and extract text from miniviewer.html
            logger.info(f"Downloading source HTML: {source_html_key}")
            html_content = self.s3_manager.get_file_content(bucket, source_html_key)
            
            logger.info("Extracting text content from HTML (excluding notes)...")
            original_text = html_extractor.extract_text_from_html(html_content)
            
            if not original_text.strip():
                logger.warning("Extracted text from HTML is empty")
                self.dest_db.update_content_verification(
                    status_table, source_id, 0.0, 'failed'
                )
                return
            
            logger.info(f"Extracted {len(original_text)} characters from miniviewer.html")
            
            # Step 3: Save original extracted text as miniviewer_original.txt
            original_text_key = os.path.join(sections_folder, 'miniviewer_original.txt')
            logger.info(f"Saving original extracted text to: {original_text_key}")
            self.s3_manager.save_text_file(
                bucket, original_text_key, original_text, content_type='text/plain'
            )
            logger.info("✓ miniviewer_original.txt saved successfully")
            
            # Step 4: Get all section files from S3
            logger.info(f"Retrieving section files from: {sections_folder}")
            section_files = self._list_section_files(bucket, sections_folder)
            
            if not section_files:
                logger.error("No section files found in S3")
                self.dest_db.update_content_verification(
                    status_table, source_id, 0.0, 'failed'
                )
                return
            
            logger.info(f"Found {len(section_files)} section files")
            
            # Step 5: Download and concatenate all section contents
            section_contents = []
            for section_file in sorted(section_files):  # Sort to ensure correct order
                section_key = os.path.join(sections_folder, section_file)
                try:
                    section_content = self.s3_manager.get_file_content(bucket, section_key)
                    section_contents.append(section_content)
                    logger.debug(f"Downloaded section: {section_file} ({len(section_content)} chars)")
                except Exception as e:
                    logger.error(f"Failed to download section {section_file}: {e}")
                    # Continue with other sections
            
            if not section_contents:
                logger.error("Failed to download any section content")
                self.dest_db.update_content_verification(
                    status_table, source_id, 0.0, 'failed'
                )
                return
            
            # Concatenate sections
            concatenated_text = self.content_verifier.concatenate_section_contents(section_contents)
            
            if not concatenated_text.strip():
                logger.error("Concatenated text is empty")
                self.dest_db.update_content_verification(
                    status_table, source_id, 0.0, 'failed'
                )
                return
            
            logger.info(f"Concatenated {len(section_contents)} sections into {len(concatenated_text)} characters")
            
            # Step 6: Save concatenated text as miniviewer_constructed.txt
            constructed_text_key = os.path.join(sections_folder, 'miniviewer_constructed.txt')
            logger.info(f"Saving constructed text to: {constructed_text_key}")
            self.s3_manager.save_text_file(
                bucket, constructed_text_key, concatenated_text, content_type='text/plain'
            )
            logger.info("✓ miniviewer_constructed.txt saved successfully")
            
            # Step 7: Perform verification
            logger.info("Comparing original HTML-extracted text with concatenated sections...")
            similarity_score, status = self.content_verifier.verify_content(
                original_text, concatenated_text
            )
            
            # Step 8: Update database
            self.dest_db.update_content_verification(
                status_table, source_id, similarity_score, status
            )
            
            # Log detailed comparison if verification failed
            if status == 'failed':
                logger.warning("Content verification FAILED - generating detailed comparison")
                try:
                    diff_report = self.content_verifier.get_detailed_comparison(
                        original_text, concatenated_text, context_lines=5
                    )
                    # Log first 2000 characters of diff
                    logger.debug(f"Diff report (first 2000 chars):\n{diff_report[:2000]}")
                except Exception as e:
                    logger.error(f"Failed to generate detailed comparison: {e}")
            
            logger.info(f"✅ Content verification complete for {source_id}")
            logger.info(f"   - Original text: {len(original_text)} chars (from HTML)")
            logger.info(f"   - Constructed text: {len(concatenated_text)} chars (from sections)")
            logger.info(f"   - Files saved: miniviewer_original.txt, miniviewer_constructed.txt")
            
        except Exception as e:
            logger.error(f"Error during content verification for {source_id}: {e}", exc_info=True)
            # Update to failed status on error
            try:
                self.dest_db.update_content_verification(
                    status_table, source_id, 0.0, 'failed'
                )
            except Exception as db_error:
                logger.error(f"Failed to update verification status: {db_error}")
    
    def _list_section_files(self, bucket: str, sections_folder: str) -> list:
        """
        List all section files (miniviewer_*.txt) in the S3 folder.
        
        CRITICAL: Only returns numbered section files (miniviewer_1.txt, miniviewer_2.txt, etc.)
        Excludes miniviewer_original.txt and miniviewer_constructed.txt
        
        Args:
            bucket (str): S3 bucket name
            sections_folder (str): S3 folder path
            
        Returns:
            list: List of section filenames (not full paths)
        """
        try:
            # Ensure folder path ends with '/'
            if not sections_folder.endswith('/'):
                sections_folder += '/'
            
            response = self.s3_manager.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=sections_folder
            )
            
            if 'Contents' not in response:
                return []
            
            # Pattern to match ONLY numbered section files: miniviewer_1.txt, miniviewer_2.txt, etc.
            # This excludes miniviewer_original.txt and miniviewer_constructed.txt
            section_pattern = re.compile(r'^miniviewer_\d+\.txt$')
            
            section_files = []
            for obj in response['Contents']:
                key = obj['Key']
                filename = os.path.basename(key)
                
                # Match only numbered section files
                if section_pattern.match(filename):
                    section_files.append(filename)
                    logger.debug(f"Found section file: {filename}")
                else:
                    logger.debug(f"Skipping non-section file: {filename}")
            
            logger.info(f"Found {len(section_files)} numbered section files (excluding original/constructed)")
            return section_files
            
        except Exception as e:
            logger.error(f"Error listing section files: {e}")
            return []