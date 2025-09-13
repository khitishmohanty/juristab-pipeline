import os
import time
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from src.juriscontent_generator import JuriscontentGenerator
from utils.s3_manager import S3Manager

class TextProcessor:
    """
    Handles the juriscontent.html generation part of the pipeline by identifying
    and processing cases that have been successfully downloaded.
    """
    def __init__(self, config: dict):
        """
        Initializes the TextProcessor.

        Args:
            config (dict): The application configuration dictionary.
        """
        self.config = config
        self.juriscontent_generator = JuriscontentGenerator()
        
        # Initialize the S3 manager using the region from the config
        self.s3_manager = S3Manager(region_name=config['aws']['default_region'])
        
        # This processor connects to both source and destination databases
        self.source_db = DatabaseConnector(db_config=config['database']['source'])
        self.dest_db = DatabaseConnector(db_config=config['database']['destination'])

    def process_cases(self):
        """
        Main method to run the juriscontent.html generation pipeline.
        It iterates through configured years and jurisdictions to find and process cases.
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
        registry_year_col = registry_config['year_column']
        download_status_col = registry_config['download_status_column']
        processing_years = registry_config.get('processing_years', [])
        jurisdictions_to_process = registry_config.get('jurisdiction_codes', [])

        # If processing_years is not specified, process all years.
        years_to_iterate = processing_years if processing_years else [None]

        # If jurisdiction_codes is empty, fetch all available jurisdictions from the registry.
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

        for year in years_to_iterate:
            if year:
                print(f"\n===== Processing Year: {year} =====")
            else:
                print(f"\n===== Processing for All Years =====")

            for jurisdiction in jurisdictions_to_process:
                jurisdiction_info = jurisdiction_lookup.get(jurisdiction)
                if not jurisdiction_info:
                    print(f"WARNING: Configuration for jurisdiction '{jurisdiction}' not found in 'tables_to_read'. Skipping.")
                    continue
                    
                s3_base_folder = jurisdiction_info['s3_folder']
                print(f"\n--- Checking Jurisdiction: {jurisdiction} ---")

                try:
                    # Query for cases that have been downloaded but not yet processed
                    query_parts = [
                        f"SELECT reg.source_id",
                        f"FROM {registry_table} AS reg",
                        f"LEFT JOIN {dest_table} AS dest ON reg.source_id = dest.source_id",
                        f"WHERE reg.jurisdiction_code = :jurisdiction",
                        f"AND reg.{download_status_col} = 'pass'" # Check for successful download
                    ]
                    params = {"jurisdiction": jurisdiction}

                    if year is not None:
                        query_parts.append(f"AND reg.{registry_year_col} = :year")
                        params["year"] = year
                    
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
                        print(f"No status record for {source_id}. Creating new one.")
                        try:
                            self.dest_db.insert_initial_status(table_name=dest_table, source_id=source_id)
                        except Exception as e:
                            print(f"Failed to insert initial status for {source_id}. Skipping. Error: {e}")
                            continue

                    case_folder = os.path.join(s3_base_folder, source_id)
                    source_html_key = os.path.join(case_folder, filenames['source_html'])
                    output_html_key = os.path.join(case_folder, filenames['extracted_html'])
                    
                    self._generate_and_save_juriscontent(
                        s3_bucket, source_html_key, output_html_key, dest_table, source_id
                    )
            
        print("\n--- HTML generation check completed for all configured years and jurisdictions. ---")

    def _generate_and_save_juriscontent(self, bucket: str, source_html_key: str, output_html_key: str, status_table: str, source_id: str):
        """
        Handles HTML download, transformation, saving the new HTML to S3,
        verifying file size, and updating the status database.
        """
        start_time_utc = datetime.now(timezone.utc)
        
        dest_table_info = self.config['tables']['tables_to_write'][0]
        step_columns_config = dest_table_info['step_columns']
        
        try:
            html_content = self.s3_manager.get_file_content(bucket, source_html_key)
            
            juriscontent_html = self.juriscontent_generator.generate(html_content)
            
            self.s3_manager.save_text_file(bucket, output_html_key, juriscontent_html, content_type='text/html')

            # --- VERIFICATION STEP ---
            source_size = self.s3_manager.get_file_size(bucket, source_html_key)
            output_size = self.s3_manager.get_file_size(bucket, output_html_key)

            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()

            if output_size > source_size:
                # Success case
                print(f"VERIFICATION PASSED for {source_id}: juriscontent.html size ({output_size} bytes) > miniviewer.html size ({source_size} bytes).")
                print(f"Successfully generated juriscontent.html for {source_id}.")
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'pass', duration, 
                    start_time_utc, end_time_utc, step_columns_config
                )
            else:
                # Failure case
                print(f"VERIFICATION FAILED for {source_id}: juriscontent.html size ({output_size} bytes) is not greater than miniviewer.html size ({source_size} bytes).")
                self.dest_db.update_step_result(
                    status_table, source_id, 'text_extract', 'failed', duration,
                    start_time_utc, end_time_utc, step_columns_config
                )

        except Exception as e:
            end_time_utc = datetime.now(timezone.utc)
            duration = (end_time_utc - start_time_utc).total_seconds()
            print(f"Juriscontent generation FAILED for {source_id}. Error: {e}")
            self.dest_db.update_step_result(
                status_table, source_id, 'text_extract', 'failed', duration,
                start_time_utc, end_time_utc, step_columns_config
            )