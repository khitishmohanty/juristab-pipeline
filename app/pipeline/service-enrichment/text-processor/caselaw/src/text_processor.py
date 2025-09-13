import os
import time
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from utils.html_parser import HtmlParser
from utils.s3_manager import S3Manager

class TextProcessor:
    """
    Handles the text extraction part of the pipeline by efficiently identifying
    and processing cases that have not yet been successfully completed.
    """
    def __init__(self, config: dict):
        """
        Initializes the TextProcessor.

        Args:
            config (dict): The application configuration dictionary.
        """
        self.config = config
        self.html_parser = HtmlParser()
        
        # Initialize the S3 manager using the region from the config
        self.s3_manager = S3Manager(region_name=config['aws']['default_region'])
        
        # This processor connects to both source and destination databases
        self.source_db = DatabaseConnector(db_config=config['database']['source'])
        self.dest_db = DatabaseConnector(db_config=config['database']['destination'])

    def process_cases(self):
        """
        Main method to run the text extraction pipeline.
        It iterates through configured years and jurisdictions (or all if not specified) 
        to find and process cases.
        """
        # Configuration for tables and S3
        dest_table_info = self.config['tables']['tables_to_write'][0]
        dest_table = dest_table_info['table']
        status_column = dest_table_info['step_columns']['text_extract']['status']

        registry_config = self.config.get('tables_registry')
        if not registry_config:
            print("FATAL: 'tables_registry' configuration not found in config.yaml. Aborting.")
            return

        registry_table = registry_config['table']
        registry_year_col = registry_config['column']
        processing_years = registry_config.get('processing_years', [])
        jurisdictions_to_process = registry_config.get('jurisdiction_codes', [])

        # If processing_years is not specified in the config, we will process all years by not applying a year filter.
        # We use a list with [None] to signify a single run that queries for all years.
        years_to_iterate = processing_years if processing_years else [None]

        # If jurisdiction_codes is empty in config, fetch all available jurisdictions from the registry.
        if not jurisdictions_to_process:
            print("Config 'jurisdiction_codes' is empty. Fetching all available jurisdictions from the registry.")
            try:
                juris_df = self.dest_db.read_sql(f"SELECT DISTINCT jurisdiction_code FROM {registry_table} WHERE jurisdiction_code IS NOT NULL AND jurisdiction_code != ''")
                jurisdictions_to_process = juris_df['jurisdiction_code'].tolist()
                if not jurisdictions_to_process:
                    print("No jurisdictions found in the registry. Aborting.")
                    return
            except Exception as e:
                print(f"FATAL: Could not fetch jurisdictions from registry. Aborting. Error: {e}")
                return

        s3_bucket = self.config['aws']['s3']['bucket_name']
        filenames = self.config['enrichment_filenames']
        
        tables_to_read_config = self.config['tables']['tables_to_read']
        jurisdiction_lookup = {item['jurisdiction']: item for item in tables_to_read_config}
        
        print(f"Starting processing for jurisdictions: {jurisdictions_to_process}")

        # Outer loop for years. If years_to_iterate is [None], this loop runs once without a year filter.
        for year in years_to_iterate:
            if year:
                print(f"\n===== Processing Year: {year} =====")
            else:
                print(f"\n===== Processing for All Years =====")

            # Inner loop for jurisdictions
            for jurisdiction in jurisdictions_to_process:
                jurisdiction_info = jurisdiction_lookup.get(jurisdiction)
                if not jurisdiction_info:
                    print(f"WARNING: Configuration for jurisdiction '{jurisdiction}' not found in 'tables_to_read'. Skipping.")
                    continue
                    
                s3_base_folder = jurisdiction_info['s3_folder']
                print(f"\n--- Checking Jurisdiction: {jurisdiction} ---")

                try:
                    # Base query and parameters
                    query_parts = [
                        f"SELECT reg.source_id",
                        f"FROM {registry_table} AS reg",
                        f"LEFT JOIN {dest_table} AS dest ON reg.source_id = dest.source_id",
                        f"WHERE reg.jurisdiction_code = :jurisdiction"
                    ]
                    params = {"jurisdiction": jurisdiction}

                    # Conditionally add the year filter if a specific year is provided
                    if year is not None:
                        query_parts.append(f"AND reg.{registry_year_col} = :year")
                        params["year"] = year
                    
                    # Add the final status check condition
                    query_parts.append(f"AND (dest.source_id IS NULL OR dest.{status_column} != 'pass')")
                    
                    query = "\n".join(query_parts)
                    
                    cases_to_process_df = self.dest_db.read_sql(query, params=params)
                    
                    log_message_year = f"for year {year} " if year else "for all years "
                    print(f"Found {len(cases_to_process_df)} cases {log_message_year}from registry requiring processing.")

                except Exception as e:
                    print(f"ERROR: Could not query the registry for jurisdiction {jurisdiction}. Skipping. Error: {e}")
                    continue

                if cases_to_process_df.empty:
                    continue

                for index, row in cases_to_process_df.iterrows():
                    source_id = str(row['source_id'])
                    print(f"- Processing case: {source_id}")
                    
                    status_row = self.dest_db.get_status_by_source_id(dest_table, source_id)
                    if not status_row:
                        print(f"No status record found for {source_id}. Creating new one.")
                        try:
                            self.dest_db.insert_initial_status(table_name=dest_table, source_id=source_id)
                        except Exception as e:
                            print(f"Failed to insert initial status for {source_id}. Skipping. Error: {e}")
                            continue

                    case_folder = os.path.join(s3_base_folder, source_id)
                    html_file_key = os.path.join(case_folder, filenames['source_html'])
                    txt_file_key = os.path.join(case_folder, filenames['extracted_text'])
                    
                    self._extract_and_save_text(
                        s3_bucket, html_file_key, txt_file_key, dest_table, source_id, case_folder
                    )
            
        print("\n--- Text extraction check completed for all configured years and jurisdictions. ---")

    def _extract_and_save_text(self, bucket: str, html_key: str, txt_key: str, status_table: str, source_id: str, case_folder: str):
        """
        Handles HTML download, text extraction, saving artifacts to S3,
        and updating the status database.
        """
        start_time_utc = datetime.now(timezone.utc)
        
        dest_table_info = self.config['tables']['tables_to_write'][0]
        step_columns_config = dest_table_info['step_columns']
        
        try:
            html_content = self.s3_manager.get_file_content(bucket, html_key)
            
            text_content = self.html_parser.extract_text(html_content)
            self.s3_manager.save_text_file(bucket, txt_key, text_content)

            # --- ADDED: Calculate counts and update metadata table ---
            char_count = len(text_content)
            word_count = len(text_content.split())
            print(f"Case {source_id}: Character count = {char_count}, Word count = {word_count}")

            metadata_config = self.config.get('tables_metadata')
            if metadata_config:
                self.dest_db.upsert_metadata_counts(
                    table_name=metadata_config['table'],
                    source_id=source_id,
                    char_count_col=metadata_config['column_count_char'],
                    word_count_col=metadata_config['column_word_char'],
                    char_count=char_count,
                    word_count=word_count
                )
            else:
                print("WARNING: 'tables_metadata' configuration not found in config.yaml. Skipping metadata update.")
            # --- END ADDED ---
            
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            
            print(f"Successfully extracted text for {source_id}.")
            self.dest_db.update_step_result(
                status_table, source_id, 'text_extract', 'pass', duration, 
                start_time_utc, end_time_utc, step_columns_config
            )
        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            print(f"Text extraction FAILED for {source_id}. Error: {e}")
            self.dest_db.update_step_result(
                status_table, source_id, 'text_extract', 'failed', duration,
                start_time_utc, end_time_utc, step_columns_config
            )
