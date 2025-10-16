import os
import time
import re
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from src.html_transformer import HtmlTransformer
from src.section_extractor import SectionExtractor
from src.content_verifier import ContentVerifier
from src.html_content_extractor import HtmlContentExtractor
from utils.s3_manager import S3Manager
import logging

logger = logging.getLogger(__name__)

class TextProcessor:
    """
    Handles the juriscontent.html generation pipeline.
    
    Pipeline:
    1. miniviewer.html → Gemini → miniviewer_genai.html (HTML with heading tags)
    2. miniviewer_genai.html → JuriscontentGenerator → juriscontent.html (styled)
    3. juriscontent.html → SectionExtractor → section-level-content/*.txt
    4. Verify: Extract text from miniviewer.html and compare with concatenated sections
    """
    
    MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
    
    def __init__(self, config: dict):
        """
        Initializes the TextProcessor.

        Args:
            config (dict): The application configuration dictionary.
        """
        self.config = config
        self.html_transformer = HtmlTransformer(config=config)
        self.section_extractor = SectionExtractor()
        self.s3_manager = S3Manager(region_name=config['aws']['default_region'])
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
        
        logger.info("TextProcessor initialized with Gemini HTML generation pipeline")

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
        step_columns_config = dest_table_info.get('step_columns', {})
        
        if 'text_extract' not in step_columns_config:
            logger.error("'text_extract' step columns not configured. Please update config.yaml")
            return

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
        
        logger.info(f"Starting processing for jurisdictions: {jurisdictions_to_process}")
        logger.info(f"Processing mode: Juriscontent + Sections = {process_sections}")
        logger.info(f"Pipeline: miniviewer.html → Gemini → miniviewer_genai.html → juriscontent.html → sections")

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
                    
                    # Get status column name from config
                    status_column = step_columns_config['text_extract']['status']
                    query_parts.append(
                        f"AND (dest.source_id IS NULL OR dest.{status_column} IS NULL OR dest.{status_column} != 'pass')"
                    )
                    
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
                    
                    # Check if status record exists
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
                    genai_html_key = os.path.join(case_folder, filenames.get('genai_html', 'miniviewer_genai.html'))
                    output_html_key = os.path.join(case_folder, filenames['extracted_html'])
                    
                    # STAGE 1: Generate juriscontent.html (with AI heading detection if needed)
                    logger.info(f"[STAGE 1] Generating juriscontent.html for {source_id}")
                    juriscontent_success = self._generate_and_save_juriscontent(
                        s3_bucket, source_html_key, genai_html_key, output_html_key, 
                        dest_table, source_id
                    )
                    
                    # STAGE 2: Extract sections (only if Stage 1 succeeded and process_sections is True)
                    if juriscontent_success and process_sections:
                        logger.info(f"[STAGE 2] Extracting sections for {source_id}")
                        sections_folder = os.path.join(case_folder, 'section-level-content')
                        sections_saved = self._extract_and_save_sections(
                            s3_bucket, output_html_key, sections_folder, 
                            dest_table, source_id
                        )
                        
                        # STAGE 3: Content verification (only if Stage 2 succeeded and verification is enabled)
                        if sections_saved and self.verification_enabled:
                            logger.info(f"[STAGE 3] Verifying content for {source_id}")
                            self._verify_section_content(
                                s3_bucket, source_html_key, sections_folder,
                                dest_table, source_id
                            )
                        elif not sections_saved:
                            logger.warning(f"Skipping content verification for {source_id} - section extraction failed")
                        elif not self.verification_enabled:
                            logger.info(f"Skipping content verification for {source_id} - verification disabled in config")
                            
                    elif not juriscontent_success:
                        logger.warning(f"Skipping section extraction for {source_id} - juriscontent generation failed")
            
        logger.info("\n" + "="*70)
        logger.info("All processing complete for all configured years and jurisdictions.")
        logger.info("="*70)

    def _generate_and_save_juriscontent(self, bucket: str, source_html_key: str, 
                                       genai_html_key: str, output_html_key: str, 
                                       status_table: str, source_id: str) -> bool:
        """
        Handles HTML download, transformation with multi-tier heading detection, 
        saving to S3, and updating the status database with token metrics.
        
        Pipeline:
        1. Download miniviewer.html
        2. Transform with multi-tier logic:
           - Original headings → Use as-is
           - Gemini (if tokens ≤ limit) → miniviewer_genai.html
           - Rule-based → miniviewer_rulebased.html
           - No rules applied → Fallback styling
        3. Apply juriscontent styling → juriscontent.html
        4. Save intermediate and final files
        5. Update database with structuring path
        
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
            
            # Download HTML content
            html_content = self.s3_manager.get_file_content(bucket, source_html_key)
            
            # Transform HTML with multi-tier heading detection
            logger.info("Phase 1: Transforming HTML with multi-tier heading detection...")
            juriscontent_html, intermediate_html, token_info, response_json = self.html_transformer.transform(html_content)
            
            # Save final juriscontent.html to S3
            logger.info("Saving juriscontent.html...")
            self.s3_manager.save_text_file(
                bucket, output_html_key, juriscontent_html, content_type='text/html'
            )
            
            # Save intermediate file if generated (Gemini or rule-based)
            if intermediate_html is not None and token_info is not None:
                structuring_path = token_info.get('structuring_path', 'not started')
                
                if structuring_path == 'genai':
                    # Save miniviewer_genai.html
                    logger.info("Saving intermediate miniviewer_genai.html...")
                    self.s3_manager.save_text_file(
                        bucket, genai_html_key, intermediate_html, content_type='text/html'
                    )
                    logger.info("✓ Intermediate miniviewer_genai.html saved")
                    
                elif structuring_path == 'rulebased':
                    # Save miniviewer_rulebased.html
                    case_folder = os.path.dirname(output_html_key)
                    rule_based_key = os.path.join(case_folder, 
                                                  self.config['enrichment_filenames'].get('rule_based_html', 
                                                                                         'miniviewer_rulebased.html'))
                    logger.info("Saving intermediate miniviewer_rulebased.html...")
                    self.s3_manager.save_text_file(
                        bucket, rule_based_key, intermediate_html, content_type='text/html'
                    )
                    logger.info("✓ Intermediate miniviewer_rulebased.html saved")
            
            # Save processing response to S3 if available
            if response_json is not None:
                case_folder = os.path.dirname(output_html_key)
                response_key = os.path.join(case_folder, 
                                           self.config['enrichment_filenames'].get('gemini_response', 
                                                                                  'juriscontent_processing_response.json'))
                
                logger.info(f"Saving processing response to S3: {response_key}")
                self.s3_manager.save_text_file(
                    bucket, response_key, response_json, 
                    content_type='application/json'
                )
                logger.info("✓ Processing response saved successfully")
            
            # Log token usage and structuring path
            if token_info is not None:
                structuring_path = token_info.get('structuring_path', 'not started')
                logger.info(f"Structuring path used: {structuring_path.upper()}")
                
                if token_info.get('generation_success', False):
                    if structuring_path == 'genai':
                        total_cost = token_info['input_price'] + token_info['output_price']
                        logger.info(
                            f"Gemini - Tokens: {token_info['input_tokens']} input, "
                            f"{token_info['output_tokens']} output | Cost: ${total_cost:.6f}"
                        )
                    elif structuring_path == 'rulebased':
                        logger.info("Rule-based detection applied (no token cost)")
                    elif structuring_path == 'original':
                        logger.info("Original headings preserved (no token cost)")
                else:
                    logger.warning(f"Heading detection attempted but used fallback path: {structuring_path}")
            
            # Verification step
            output_size = self.s3_manager.get_file_size(bucket, output_html_key)
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()

            if output_size > 0 and source_size > 0:
                # Success case
                logger.info(f"✅ Juriscontent generation SUCCESS: {output_size} bytes")
                
                # Update status in database
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'pass', duration, 
                    start_time_utc, end_time_utc, step_columns_config
                )
                
                # Update heading metadata with structuring path
                if token_info is not None:
                    try:
                        # Prepare heading metadata dict with new structuring_path field
                        heading_metadata = {
                            'input_tokens': token_info.get('input_tokens', 0),
                            'output_tokens': token_info.get('output_tokens', 0),
                            'input_price': token_info.get('input_price', 0.0),
                            'output_price': token_info.get('output_price', 0.0),
                            'before_processing_heading_count': token_info.get('before_processing_heading_count', 0),
                            'after_processing_heading_count': token_info.get('after_processing_heading_count', 0),
                            'structuring_path': token_info.get('structuring_path', 'not started')
                        }
                        
                        self.dest_db.update_heading_detection_metadata(
                            status_table, source_id, heading_metadata
                        )
                        
                        logger.info(f"Heading metadata saved to database")
                        
                    except Exception as e:
                        logger.error(f"Failed to update heading metadata (non-critical): {e}")
                
                return True
            else:
                # Failure case
                logger.error(f"✗ Juriscontent generation FAILED: output size is {output_size} bytes")
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'failed', duration,
                    start_time_utc, end_time_utc, step_columns_config
                )
                return False

        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            logger.error(f"✗ Juriscontent generation FAILED: {type(e).__name__}: {str(e)}")
            logger.error(f"Stack trace:", exc_info=True)
            
            # Update status to failed
            try:
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'failed', duration,
                    start_time_utc, end_time_utc, step_columns_config
                )
            except Exception as db_error:
                logger.error(f"Failed to update database status: {db_error}")
            
            return False

    def _extract_and_save_sections(self, bucket: str, juriscontent_key: str, 
                           sections_folder: str, status_table: str, source_id: str) -> bool:
        """
        Extracts sections from juriscontent.html and saves them to S3.
        
        CRITICAL: This method NEVER fails. It always produces at least one section file.
        - Multiple headings detected → Multiple section files
        - No headings detected → Single section file with all content
        
        Then updates both legislation_sections and enrichment status tables.
        
        Returns:
            bool: True if successful, False if failed
        """
        start_time_utc = datetime.now(timezone.utc)
        
        dest_table_info = self.config['tables']['tables_to_write'][0]
        step_columns_config = dest_table_info['step_columns']
        
        # Check if section_extract step is configured
        if 'section_extract' not in step_columns_config:
            logger.warning("'section_extract' step not configured in config.yaml. Skipping section extraction.")
            return False
        
        # Update status to 'started'
        try:
            self.dest_db.update_section_extract_status(
                status_table, source_id, 'started'
            )
        except Exception as e:
            logger.warning(f"Could not update status to 'started' for {source_id}: {e}")
        
        try:
            # Clear existing section-level-content folder if it exists
            logger.info(f"Preparing section-level-content folder...")
            self.s3_manager.clear_and_recreate_folder(bucket, sections_folder)
            
            # Clear existing sections from database
            deleted_count = self.dest_db.clear_existing_sections(source_id)
            if deleted_count > 0:
                logger.debug(f"Cleared {deleted_count} existing section records")
            
            # Download juriscontent.html from S3
            html_content = self.s3_manager.get_file_content(bucket, juriscontent_key)
            
            # Extract sections - THIS ALWAYS RETURNS AT LEAST ONE SECTION
            logger.info("Extracting sections from juriscontent.html...")
            sections = self.section_extractor.extract_sections(html_content)
            
            # This should never happen, but just in case
            if not sections:
                logger.error("CRITICAL: Section extractor returned empty list! Creating fallback section.")
                sections = [{
                    'section_id': 1,
                    'content': 'Unable to extract content from document.',
                    'heading': None
                }]
            
            logger.info(f"→ Processing {len(sections)} section(s) for {source_id}")
            
            # Save each section to S3 and database
            sections_saved = 0
            for section in sections:
                section_id = section['section_id']
                section_content = self.section_extractor.format_section_content(section)
                
                # Ensure we have some content
                if not section_content.strip():
                    logger.warning(f"Section {section_id} is empty. Using placeholder.")
                    section_content = "No content in this section."
                
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
                
                # Log summary
                summary = self.section_extractor.get_section_summary(section)
                logger.debug(f"  ✓ Saved {summary}")
            
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            
            # Log appropriate success message
            if sections_saved == 1 and sections[0].get('heading') is None:
                logger.info(f"✅ Section extraction SUCCESS: 1 section (no headings detected)")
            else:
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
            logger.error(f"Stack trace:", exc_info=True)
            
            # Even in failure, try to create a minimal section file
            try:
                logger.info("Attempting to create fallback single section...")
                fallback_content = "Section extraction failed. Content could not be processed."
                section_filename = "miniviewer_1.txt"
                section_key = os.path.join(sections_folder, section_filename)
                
                self.s3_manager.save_text_file(
                    bucket, section_key, fallback_content, content_type='text/plain'
                )
                
                self.dest_db.insert_legislation_section(
                    source_id=source_id,
                    section_id=1
                )
                
                logger.info("✓ Created fallback section file")
            except Exception as fallback_error:
                logger.error(f"Could not create fallback section: {fallback_error}")
            
            # Update status to 'failed'
            try:
                self.dest_db.update_step_result(
                    status_table, source_id, 'section_extract', 'failed', duration,
                    start_time_utc, end_time_utc, step_columns_config
                )
            except Exception as db_error:
                logger.error(f"Failed to update database status: {db_error}")
            
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
            for section_file in sorted(section_files):
                section_key = os.path.join(sections_folder, section_file)
                try:
                    section_content = self.s3_manager.get_file_content(bucket, section_key)
                    section_contents.append(section_content)
                    logger.debug(f"Downloaded section: {section_file} ({len(section_content)} chars)")
                except Exception as e:
                    logger.error(f"Failed to download section {section_file}: {e}")
            
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
                    logger.debug(f"Diff report (first 2000 chars):\n{diff_report[:2000]}")
                except Exception as e:
                    logger.error(f"Failed to generate detailed comparison: {e}")
            
            logger.info(f"✅ Content verification complete for {source_id}")
            logger.info(f"   - Original text: {len(original_text)} chars (from HTML)")
            logger.info(f"   - Constructed text: {len(concatenated_text)} chars (from sections)")
            logger.info(f"   - Files saved: miniviewer_original.txt, miniviewer_constructed.txt")
            
        except Exception as e:
            logger.error(f"Error during content verification for {source_id}: {e}", exc_info=True)
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