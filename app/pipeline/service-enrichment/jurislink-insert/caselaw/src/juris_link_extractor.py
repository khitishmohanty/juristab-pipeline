import re
import time
import yaml
import logging
import sys
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import text, bindparam
from sqlalchemy.exc import SQLAlchemyError
from utils.db import get_db_connection, get_table_name, get_column_names
from utils.aws import get_s3_client, get_s3_bucket_name, get_file_from_s3

class JurisLinkExtractor:
    def __init__(self, config_path='config/config.yaml'):
        self.config_path = config_path
        
        # --- IMPROVEMENT: Configure logger directly within the class ---
        self.logger = logging.getLogger(__name__)
        # Prevent adding duplicate handlers if the script is re-run in the same process
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        # -----------------------------------------------------------------

        self.db_session = get_db_connection(config_path)
        self.s3_client = get_s3_client(config_path)
        self.s3_bucket = get_s3_bucket_name(config_path)
        
        # Table and column names from config
        self.caselaw_registry_table = get_table_name(config_path, 'caselaw_registry')
        self.juris_link_table = get_table_name(config_path, 'juris_link')
        self.enrichment_status_table = get_table_name(config_path, 'caselaw_enrichment_status')
        self.enrichment_cols = get_column_names(config_path, 'caselaw_enrichment_status')

    def process_source_ids(self):
        """
        Main processing loop that iterates through source_ids and extracts jurislinks.
        """
        source_ids_to_process = self._get_source_ids_from_registry()
        
        if not source_ids_to_process:
            self.logger.warning("No new source records found matching the criteria in config.yaml. Exiting.")
            return

        self.logger.info(f"Found {len(source_ids_to_process)} source(s) to process.")

        for source_id, file_path in source_ids_to_process:
            self.logger.info(f"Processing source_id: {source_id}")
            start_time = datetime.now()
            self._update_enrichment_status(source_id, 'started', start_time)

            # Set a default status of 'failed' and wrap the core logic in a try/except/finally block.
            # This ensures the status is always updated correctly, even if errors occur.
            status = 'failed' 
            try:
                with open(self.config_path, 'r') as file:
                    config = yaml.safe_load(file)
                source_file_name = config['enrichment_filenames']['source_file']

                # Correctly parse the S3 key from the full file_path URI
                s3_prefix = f"s3://{self.s3_bucket}/"
                if file_path.startswith(s3_prefix):
                    key_path = file_path[len(s3_prefix):]
                else:
                    key_path = file_path

                s3_key = f"{key_path}/{source_file_name}"

                html_content = get_file_from_s3(self.s3_client, self.s3_bucket, s3_key)

                if html_content:
                    links = self._extract_links_from_html(html_content)
                    self.logger.info(f"Successfully fetched HTML. Found {len(links)} links.")
                    self._process_and_store_links(source_id, links)
                    # The status is only set to 'pass' if the entire process completes without raising an exception.
                    status = 'pass'
                else:
                    self.logger.warning(f"Failed to retrieve content for source_id: {source_id}. Check S3 path: s3://{self.s3_bucket}/{s3_key}")
                    # The status will remain 'failed'
            
            except Exception as e:
                # This block will catch any unhandled errors from the processing steps,
                # including database errors that are re-raised from _insert_juris_link.
                self.logger.error(f"An unhandled exception occurred while processing {source_id}: {e}", exc_info=True)
                # The status will remain 'failed'
            
            finally:
                # This block guarantees that the final status is always recorded in the database.
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                self._update_enrichment_status(source_id, status, start_time, end_time, duration)
                self.logger.info(f"Finished processing source_id: {source_id} with status: {status}")


    def _get_source_ids_from_registry(self):
        """
        Fetches source_id and file_path from the caselaw_registry table 
        for records that have not already been successfully processed.
        """
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            registry_config = config['tables_registry']
            
            years = registry_config['processing_years']
            jurisdictions = registry_config['jurisdiction_codes']
            
            # Get the specific status column name from the config
            status_column = self.enrichment_cols['processing_status']

            self.logger.info(f"Querying for records with years: {years} and jurisdictions: {jurisdictions} that have not passed processing.")

            # MODIFIED QUERY:
            # - LEFT JOINs caselaw_enrichment_status to check the processing status.
            # - WHERE clause now also checks if the status is NOT 'pass'.
            #   Records not in caselaw_enrichment_status will have a NULL status and be included.
            query = text(f"""
                SELECT cr.source_id, cr.file_path 
                FROM {self.caselaw_registry_table} cr
                LEFT JOIN {self.enrichment_status_table} ces ON cr.source_id = ces.source_id
                WHERE cr.{registry_config['column']} IN :years 
                AND cr.jurisdiction_code IN :jurisdiction_codes
                AND (ces.{status_column} IS NULL OR ces.{status_column} != 'pass')
            """)

            result = self.db_session.execute(query.bindparams(
                bindparam('years', expanding=True),
                bindparam('jurisdiction_codes', expanding=True)
            ), {
                'years': years,
                'jurisdiction_codes': jurisdictions
            })
            return result.fetchall()
        except SQLAlchemyError as e:
            self.logger.error(f"Database error while fetching source IDs: {e}", exc_info=True)
            return [] # Return empty list on error to prevent crash
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in _get_source_ids_from_registry: {e}", exc_info=True)
            return []


    def _extract_links_from_html(self, html_content):
        """
        Parses HTML and extracts all hyperlinks.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        for a_tag in soup.find_all('a', href=True):
            links.append({'text': a_tag.get_text(strip=True), 'href': a_tag['href']})
        return links

    def _process_and_store_links(self, source_id, links):
        """
        Processes extracted links, finds related_source_id, and stores them.
        """
        for link in links:
            jurislink = link['href']
            
            # Extract the new IDs from the link
            book_parent_id, book_section_id = self._extract_ids_from_jurislink(jurislink)

            # Proceed if we found at least a parent ID
            if book_parent_id:
                related_source_id = self._get_related_source_id(jurislink)
                self._insert_juris_link(
                    source_id, 
                    jurislink, 
                    related_source_id, 
                    book_parent_id, 
                    book_section_id
                )

    def _get_related_source_id(self, jurislink):
        """
        Looks up the related_source_id from the caselaw_registry table based on the URL.
        """
        try:
            query = text(f"SELECT source_id FROM {self.caselaw_registry_table} WHERE source_url = :jurislink")
            result = self.db_session.execute(query, {'jurislink': jurislink}).fetchone()
            return result[0] if result else None
        except SQLAlchemyError as e:
            self.logger.error(f"Database error looking up related_source_id for {jurislink}: {e}")
            return None


    def _insert_juris_link(self, source_id, jurislink, related_source_id, book_parent_id, book_section_id):
        """
        Inserts a new record into the juris_link table, with retries for deadlocks.
        """
        # --- ADDED: Retry logic for handling deadlocks ---
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Check for existing record to prevent duplicates
                check_query = text(f"""
                    SELECT 1 FROM {self.juris_link_table} 
                    WHERE source_id = :source_id AND jurislink = :jurislink
                """)
                exists = self.db_session.execute(check_query, {
                    'source_id': source_id, 
                    'jurislink': jurislink
                }).fetchone()

                if not exists:
                    insert_query = text(f"""
                        INSERT INTO {self.juris_link_table} 
                        (source_id, jurislink, related_source_id, book_parent_id, book_section_id) 
                        VALUES (:source_id, :jurislink, :related_source_id, :book_parent_id, :book_section_id)
                    """)
                    self.db_session.execute(insert_query, {
                        'source_id': source_id, 
                        'jurislink': jurislink, 
                        'related_source_id': related_source_id,
                        'book_parent_id': book_parent_id,
                        'book_section_id': book_section_id
                    })
                    self.db_session.commit()
                
                return # --- ADDED: Exit the loop on success ---

            except SQLAlchemyError as e:
                # --- MODIFIED: Specific handling for deadlock errors ---
                self.db_session.rollback() # Always rollback on error
                # MySQL deadlock error code is 1213. Table changed is 1412.
                if e.orig and e.orig.errno in (1213, 1412):
                    self.logger.warning(f"Deadlock or table change detected on attempt {attempt + 1}/{max_retries}. Retrying...")
                    if attempt < max_retries - 1:
                         time.sleep(1 + attempt) # Wait a bit before retrying (incremental backoff)
                    else:
                        self.logger.error(f"Final attempt failed for jurislink insertion on source_id {source_id}: {e}")
                        raise # --- ADDED: Re-raise the exception after final attempt ---
                else:
                    self.logger.error(f"A non-retriable database error occurred inserting jurislink for source_id {source_id}: {e}")
                    raise # --- ADDED: Re-raise other unexpected SQL errors ---


    def _update_enrichment_status(self, source_id, status, start_time, end_time=None, duration=None):
        """
        Updates the caselaw_enrichment_status table for a given source_id.
        """
        try:
            # Check if a record for the source_id already exists
            check_query = text(f"SELECT 1 FROM {self.enrichment_status_table} WHERE source_id = :source_id")
            exists = self.db_session.execute(check_query, {'source_id': source_id}).fetchone()

            status_col = self.enrichment_cols['processing_status']
            duration_col = self.enrichment_cols['processing_duration']
            start_time_col = self.enrichment_cols['start_time']
            end_time_col = self.enrichment_cols['end_time']

            if exists:
                # Update existing record
                update_query = text(f"""
                    UPDATE {self.enrichment_status_table} 
                    SET {status_col} = :status, {duration_col} = :duration, 
                        {start_time_col} = :start_time, {end_time_col} = :end_time
                    WHERE source_id = :source_id
                """)
                self.db_session.execute(update_query, {
                    'status': status, 'duration': duration, 'start_time': start_time,
                    'end_time': end_time, 'source_id': source_id
                })
            else:
                # Insert new record
                insert_query = text(f"""
                    INSERT INTO {self.enrichment_status_table} (source_id, {status_col}, {duration_col}, {start_time_col}, {end_col}) 
                    VALUES (:source_id, :status, :duration, :start_time, :end_time)
                """)
                self.db_session.execute(insert_query, {
                    'source_id': source_id, 'status': status, 'duration': duration,
                    'start_time': start_time, 'end_time': end_time
                })
            self.db_session.commit()
        except SQLAlchemyError as e:
            self.logger.error(f"Database error updating enrichment status for source_id {source_id}: {e}")
            self.db_session.rollback()
            
            
    def _extract_ids_from_jurislink(self, jurislink):
        """
        Extracts book_parent_id and book_section_id from a jurislink URL.
        """
        # Pattern to find /article/(\d+) and optionally /section/(\d+)
        pattern = r"/article/(\d+)(?:/section/(\d+))?"
        match = re.search(pattern, jurislink)
        
        if not match:
            return None, None
            
        book_parent_id = match.group(1)
        book_section_id = match.group(2) # This will be None if the section part is not present
        
        return book_parent_id, book_section_id