import os
import time
from datetime import datetime
import logging
import json
from config.config import Config
from src.database import DatabaseManager
from utils.gemini_client import GeminiClient
from utils.file_utils import get_full_s3_key
from utils.s3_client import S3Manager
import mysql.connector

# Configure root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_records_to_process(db_manager, registry_config, jurisdiction_codes, years):
    """
    Retrieves records from the legislation_registry table that need processing.
    Selects records where AI extraction has a status of 'not started' or 'failed'.
    """
    if not db_manager._get_connection():
        return []

    cursor = db_manager.conn.cursor(dictionary=True)
    try:
        # Base query
        query = f"""
            SELECT 
                lr.source_id, 
                lr.file_path, 
                lr.jurisdiction_code, 
                lr.status_content_download,
                COALESCE(les.status_metadataextract_ai, 'not started') AS status_metadataextract_ai
            FROM 
                legislation_registry AS lr
            LEFT JOIN 
                legislation_enrichment_status AS les ON lr.source_id = les.source_id
            WHERE 
                lr.status_content_download = 'pass'
        """
        
        params = []
        
        # Add jurisdiction filter if codes are provided
        if jurisdiction_codes:
            jurisdiction_placeholders = ', '.join(['%s'] * len(jurisdiction_codes))
            query += f" AND lr.jurisdiction_code IN ({jurisdiction_placeholders})"
            params.extend(jurisdiction_codes)
            
        # Conditionally add the year filter only if the years list is not empty
        if years:
            year_placeholders = ', '.join(['%s'] * len(years))
            query += f" AND lr.{registry_config['column']} IN ({year_placeholders})"
            params.extend(years)
            
        # Add the final status filter
        query += """
            AND (
                les.source_id IS NULL 
                OR les.status_metadataextract_ai IN ('not started', 'failed')
            )
        """
        
        cursor.execute(query, params)
        records = cursor.fetchall()
        
        # Construct a log message that adapts to whether years are being filtered
        year_log_message = f"and years {years}" if years else "for all years"
        logging.info(f"Found {len(records)} records to process for jurisdictions {jurisdiction_codes} {year_log_message}.")
        
        return records
    except mysql.connector.Error as err:
        logging.error(f"Failed to query registry table: {err}")
        return []
    finally:
        cursor.close()
        db_manager.close_connection()


def process_record(record, config, db_columns, prompt_content):
    """
    Processes a single legislation record by running AI extraction.
    """
    source_id = record['source_id']
    logging.info(f"Starting processing for source_id: {source_id}")
    
    # Immediately update status to 'started' to lock the record
    db_manager_starter = DatabaseManager(config.get('database'))
    try:
        db_manager_starter.update_enrichment_status(source_id, {"status_metadataextract_ai": "started"})
    finally:
        db_manager_starter.close_connection()

    overall_start_time = datetime.now()

    # Initialize statuses, metrics, and timestamps
    status_updates = {}
    ai_status = 'failed'  # Default to failed
    metadata = {}
    db_ops_successful = False
    
    # Timestamps and Durations
    ai_start_time = datetime.now()
    ai_end_time = None
    ai_duration = 0.0

    # Token metrics
    input_tokens, output_tokens = 0, 0
    input_price, output_price = 0.0, 0.0

    # S3 setup
    aws_config = config.get('aws')
    s3_manager = S3Manager(region_name=aws_config['default_region'])
    s3_file_key = get_full_s3_key(record['source_id'], record['jurisdiction_code'], config)
    if not s3_file_key:
        logging.error(f"Could not construct S3 file key for {source_id}. Skipping.")
        return
    
    bucket_name_config = next((s3_cfg for s3_cfg in config.get('aws', 's3') if s3_cfg['jurisdiction_code'] == record['jurisdiction_code']), None)
    if not bucket_name_config:
        logging.error(f"Could not find S3 bucket configuration for jurisdiction {record['jurisdiction_code']}.")
        return
    bucket_name = bucket_name_config['bucket_name']

    try:
        legislation_text_content = s3_manager.get_file_content(bucket_name, s3_file_key)
    except Exception as e:
        logging.error(f"Failed to download legislation text file for {source_id}: {e}. Cannot proceed.")
        db_manager = DatabaseManager(config.get('database'))
        fail_updates = {
            "status_metadataextract_ai": 'failed', # Use 'failed' status
            "start_time_metadataextract_ai": overall_start_time,
            "end_time_metadataextract_ai": datetime.now()
        }
        db_manager.update_enrichment_status(source_id, fail_updates)
        db_manager.close_connection()
        return

    # --- AI-based extraction ---
    logging.info(f"Running AI extraction for {source_id}")
    try:
        gemini_config = config.get('models', 'gemini')
        gemini_client = GeminiClient(model_name=gemini_config['model'])
        raw_json, input_tokens, output_tokens = gemini_client.generate_json_from_text(prompt_content, legislation_text_content)
        
        pricing = gemini_config.get('pricing', {})
        input_price = (input_tokens / 1_000_000) * pricing.get('input_per_million', 0.0)
        output_price = (output_tokens / 1_000_000) * pricing.get('output_per_million', 0.0)

        if gemini_client.is_valid_json(raw_json):
            metadata = json.loads(raw_json)
            if metadata:
                ai_status = 'pass'
                json_filename = config.get('enrichment_filenames', 'jurismetadata_json')
                json_s3_key = os.path.join(os.path.dirname(s3_file_key), json_filename)
                s3_manager.save_json_file(bucket_name, json_s3_key, raw_json)
                logging.info(f"AI extraction successful. Saved metadata to {json_s3_key}")
            else:
                ai_status = 'failed'
                logging.warning("AI response was valid JSON but empty.")
        else:
            ai_status = 'failed'
            logging.error("AI response was not valid JSON.")
    except Exception as e:
        ai_status = 'failed'
        logging.error(f"AI extraction error: {e}")
    finally:
        ai_end_time = datetime.now()
        ai_duration = (ai_end_time - ai_start_time).total_seconds()

    # --- Database Operations ---
    db_manager = DatabaseManager(config.get('database'))
    try:
        if ai_status == 'pass' and metadata:
            db_ops_successful = db_manager.upsert_legislation_metadata(metadata, source_id, db_columns)
        else:
            db_ops_successful = True
            
        # Populate the final status update dictionary
        status_updates["status_metadataextract_ai"] = 'pass' if ai_status == 'pass' and db_ops_successful else 'failed'
        status_updates["duration_metadataextract_ai"] = ai_duration
        status_updates["start_time_metadataextract_ai"] = ai_start_time
        status_updates["end_time_metadataextract_ai"] = ai_end_time
        status_updates["token_input_metadataextract_ai"] = input_tokens
        status_updates["token_output_metadataextract_ai"] = output_tokens
        status_updates["token_input_price_metadataextract_ai"] = input_price
        status_updates["token_output_price_metadataextract_ai"] = output_price
        
        db_manager.update_enrichment_status(source_id, status_updates)
    
    finally:
        db_manager.close_connection()

    total_duration = (datetime.now() - overall_start_time).total_seconds()
    logging.info(f"Finished processing {source_id}. Final status: {status_updates['status_metadataextract_ai']}. Total duration: {total_duration:.2f}s")


def main():
    """
    Main function to run the legislation metadata extraction process.
    """
    logging.info("Starting legislation metadata extraction service...")
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config", "config.yaml")
        config = Config(config_path=config_path)
        
        ai_on = config.get('extraction_switch', 'AI_extract')
        
        if not ai_on:
            logging.info("AI extraction is turned off in config.yaml. Exiting.")
            return
            
        logging.info("Configuration loaded. AI extraction is enabled.")

    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    prompt_content = ""
    try:
        prompt_path = os.path.join(script_dir, "config", "prompt.txt")
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_content = f.read()
        logging.info("Successfully loaded AI prompt from config folder.")
    except FileNotFoundError:
        logging.error("'prompt.txt' was not found in the 'config' directory. Cannot proceed. Exiting.")
        return

    db_config = config.get('database')
    registry_config = config.get('tables_registry')
    jurisdiction_codes = [s3_config['jurisdiction_code'] for s3_config in config.get('aws', 's3')]
    processing_years = config.get('tables_registry', 'processing_years')

    legislation_metadata_config = None
    for table_config in config.get('tables', 'tables_to_write'):
        if table_config.get('table') == 'legislation_metadata':
            legislation_metadata_config = table_config
            break
    
    if not legislation_metadata_config:
        logging.error("Could not find 'legislation_metadata' configuration in config.yaml.")
        return

    db_columns = list(legislation_metadata_config['columns'].keys())

    db_manager_for_fetch = DatabaseManager(db_config)
    records_to_process = get_records_to_process(db_manager_for_fetch, registry_config, jurisdiction_codes, processing_years)

    if not records_to_process:
        logging.info("No records to process. Exiting.")
        return

    for record in records_to_process:
        process_record(record, config, db_columns, prompt_content)

    logging.info("All records processed.")

if __name__ == "__main__":
    main()