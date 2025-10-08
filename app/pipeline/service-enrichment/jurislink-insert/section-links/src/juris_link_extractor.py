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
        
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.db_session = get_db_connection(config_path)
        self.s3_client = get_s3_client(config_path)
        self.s3_bucket = get_s3_bucket_name(config_path)
        
        # Table and column names from config
        self.caselaw_registry_table = get_table_name(config_path, 'legislation_registry')
        self.juris_link_table = get_table_name(config_path, 'juris_link') 
        self.enrichment_status_table = get_table_name(config_path, 'legislation_enrichment_status')
        self.enrichment_cols = get_column_names(config_path, 'legislation_enrichment_status')

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

            status = 'failed' 
            try:
                with open(self.config_path, 'r') as file:
                    config = yaml.safe_load(file)
                source_file_name = config['enrichment_filenames']['source_file']

                s3_prefix = f"s3://{self.s3_bucket}/"
                if file_path.startswith(s3_prefix):
                    key_path = file_path[len(s3_prefix):]
                else:
                    key_path = file_path

                s3_key = f"{key_path}/{source_file_name}"

                html_content = get_file_from_s3(self.s3_client, self.s3_bucket, s3_key)

                if html_content:
                    links = self._extract_anchor_links_from_html(html_content)
                    self.logger.info(f"Successfully fetched HTML. Found {len(links)} anchor links to process.")
                    self._process_and_store_links(source_id, links)
                    status = 'pass'
                else:
                    self.logger.warning(f"Failed to retrieve content for source_id: {source_id}. Check S3 path: s3://{self.s3_bucket}/{s3_key}")
            
            except Exception as e:
                self.logger.error(f"An unhandled exception occurred while processing {source_id}: {e}", exc_info=True)
            
            finally:
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
            
            status_column = self.enrichment_cols['processing_status']

            # Build the query conditionally based on whether years list is empty
            if years:  # If years list is not empty, include year filter
                self.logger.info(f"Querying for records with years: {years} and jurisdictions: {jurisdictions} that have not passed processing.")
                
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
            else:  # If years list is empty, omit year filter entirely
                self.logger.info(f"Querying for records with ALL years and jurisdictions: {jurisdictions} that have not passed processing.")
                
                query = text(f"""
                    SELECT cr.source_id, cr.file_path 
                    FROM {self.caselaw_registry_table} cr
                    LEFT JOIN {self.enrichment_status_table} ces ON cr.source_id = ces.source_id
                    WHERE cr.jurisdiction_code IN :jurisdiction_codes
                    AND (ces.{status_column} IS NULL OR ces.{status_column} != 'pass')
                """)
                
                result = self.db_session.execute(query.bindparams(
                    bindparam('jurisdiction_codes', expanding=True)
                ), {
                    'jurisdiction_codes': jurisdictions
                })
            
            return result.fetchall()
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error while fetching source IDs: {e}", exc_info=True)
            return []
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in _get_source_ids_from_registry: {e}", exc_info=True)
            return []

    def _extract_anchor_links_from_html(self, html_content):
        """
        Parses HTML and extracts all anchor links that match the required format 
        (e.g., id="bnj_a_..."), regardless of their parent tag.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []
        
        # MODIFICATION: Find all anchor tags with a matching ID anywhere in the document.
        anchors = soup.find_all('a', id=re.compile(r"^bnj_a_\d+_[a-zA-Z]+_\d+"))
        
        for anchor in anchors:
            # Get the text from the anchor's parent element to provide context.
            parent_text = anchor.parent.get_text(strip=True)
            links.append({
                'text': parent_text, 
                'href': anchor.get('id')
            })
        return links

    def _process_and_store_links(self, source_id, links):
        """
        Processes extracted anchor links, extracts IDs, and stores them in the database.
        """
        for link in links:
            section_link = link['href']
            section_text = link['text'] 

            book_parent_id, book_section_id = self._extract_ids_from_anchor(section_link)

            if book_parent_id and book_section_id:
                self._insert_juris_link(
                    source_id, 
                    section_link, 
                    book_parent_id, 
                    book_section_id,
                    section_text 
                )

    def _insert_juris_link(self, source_id, section_link, book_parent_id, book_section_id, section_text):
        """
        Inserts a new record into the juris_link_extract_section_link table.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                check_query = text(f"""
                    SELECT 1 FROM {self.juris_link_table} 
                    WHERE source_id = :source_id AND section_link = :section_link
                """)
                exists = self.db_session.execute(check_query, {
                    'source_id': source_id, 
                    'section_link': section_link
                }).fetchone()

                if not exists:
                    insert_query = text(f"""
                        INSERT INTO {self.juris_link_table} 
                        (source_id, section_link, book_parent_id, book_section_id, section_text) 
                        VALUES (:source_id, :section_link, :book_parent_id, :book_section_id, :section_text)
                    """)
                    self.db_session.execute(insert_query, {
                        'source_id': source_id, 
                        'section_link': section_link, 
                        'book_parent_id': book_parent_id,
                        'book_section_id': book_section_id,
                        'section_text': section_text
                    })
                    self.db_session.commit()
                
                return

            except SQLAlchemyError as e:
                self.db_session.rollback()
                if "Deadlock" in str(e):
                    self.logger.warning(f"Deadlock detected on attempt {attempt + 1}/{max_retries}. Retrying...")
                    if attempt < max_retries - 1:
                        time.sleep(1 + attempt)
                    else:
                        self.logger.error(f"Final attempt failed for section_link insertion on source_id {source_id}: {e}")
                        raise
                else:
                    self.logger.error(f"A non-retriable database error occurred inserting section_link for source_id {source_id}: {e}")
                    raise


    def _update_enrichment_status(self, source_id, status, start_time, end_time=None, duration=None):
        """
        Updates the caselaw_enrichment_status table for a given source_id.
        """
        try:
            check_query = text(f"SELECT 1 FROM {self.enrichment_status_table} WHERE source_id = :source_id")
            exists = self.db_session.execute(check_query, {'source_id': source_id}).fetchone()

            status_col = self.enrichment_cols['processing_status']
            duration_col = self.enrichment_cols['processing_duration']
            start_time_col = self.enrichment_cols['start_time']
            end_time_col = self.enrichment_cols['end_time']

            if exists:
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
                insert_query = text(f"""
                    INSERT INTO {self.enrichment_status_table} (source_id, {status_col}, {duration_col}, {start_time_col}, {end_time_col}) 
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
            
    def _extract_ids_from_anchor(self, anchor_text):
        """
        Extracts book_parent_id and book_section_id from an anchor link string
        using a flexible regex format.
        Example: bnj_a_403369_sr_2168 -> (403369, 2168)
        """
        pattern = re.compile(r"bnj_a_(\d+)_[a-zA-Z]+_(\d+)")
        match = pattern.search(anchor_text)

        if not match:
            self.logger.debug(f"Could not extract IDs from anchor: {anchor_text}")
            return None, None

        book_parent_id = match.group(1)
        book_section_id = match.group(2)
        
        return book_parent_id, book_section_id
