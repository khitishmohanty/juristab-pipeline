import os
import time
from datetime import datetime
import logging
import json
from config.config import Config
from src.extractor import MetadataExtractor
from src.database import DatabaseManager
from utils.gemini_client import GeminiClient
from utils.llama_client import LlamaClient
from utils.file_utils import get_full_s3_key
from utils.s3_client import S3Manager
import mysql.connector

# Configure root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_records_to_process(db_manager, registry_config, jurisdiction_codes, years):
    """
    Retrieves records from the caselaw_registry table that need processing.
    Selects records where either rule-based or AI extraction has not passed.
    """
    if not jurisdiction_codes:
        logging.warning("No jurisdictions provided for processing. Skipping database query.")
        return []

    if not db_manager._get_connection():
        return []

    cursor = db_manager.conn.cursor(dictionary=True)
    try:
        # --- MODIFIED: Dynamically build the WHERE clause ---
        params = jurisdiction_codes[:]
        where_clauses = [
            "cr.status_content_download = 'pass'",
            f"cr.jurisdiction_code IN ({', '.join(['%s'] * len(jurisdiction_codes))})"
        ]

        # Only add the year filter if the 'years' list is not empty
        if years:
            year_placeholders = ', '.join(['%s'] * len(years))
            where_clauses.append(f"cr.{registry_config['column']} IN ({year_placeholders})")
            params.extend(years)
        
        where_statement = " AND ".join(where_clauses)
        # --- END OF MODIFICATION ---
        
        query = f"""
            SELECT 
                cr.source_id, 
                cr.file_path, 
                cr.jurisdiction_code, 
                cr.status_content_download,
                COALESCE(ces.status_metadataextract_rulebased, 'pending') AS status_metadataextract_rulebased,
                COALESCE(ces.status_metadataextract_ai, 'pending') AS status_metadataextract_ai
            FROM 
                caselaw_registry AS cr
            LEFT JOIN 
                caselaw_enrichment_status AS ces ON cr.source_id = ces.source_id
            WHERE 
                {where_statement}
                AND (
                    ces.source_id IS NULL 
                    OR ces.status_metadataextract_rulebased != 'pass' 
                    OR ces.status_metadataextract_ai != 'pass'
                )
        """
        
        cursor.execute(query, params)
        records = cursor.fetchall()
        
        year_log_message = f"and years {years}" if years else "for all years"
        logging.info(f"Found {len(records)} records to process for jurisdictions {jurisdiction_codes} {year_log_message}.")
        return records
    except mysql.connector.Error as err:
        logging.error(f"Failed to query registry table: {err}")
        return []
    finally:
        cursor.close()
        db_manager.close_connection()


def process_record(record, config, field_mapping, db_columns, use_ai_extraction, prompt_content, ai_provider):
    """
    Processes a single case law record, running rule-based and/or AI extraction
    based on the record's current status and logging individual start/end times.
    """
    source_id = record['source_id']
    logging.info(f"Processing record for source_id: {source_id}")
    overall_start_time = datetime.now()

    # Initialize statuses, metrics, and timestamps
    status_updates = {}
    rulebased_status, ai_status = 'skip', 'skip'
    metadata, counsel_firm_mappings = {}, []
    db_ops_successful = False
    
    # Timestamps and Durations
    rulebased_start_time, rulebased_end_time = None, None
    ai_start_time, ai_end_time = None, None
    rulebased_duration, ai_duration = 0.0, 0.0

    # Metrics
    input_tokens, output_tokens = 0, 0
    total_price = 0.0

    needs_rulebased_processing = record.get('status_metadataextract_rulebased') != 'pass'
    needs_ai_processing = record.get('status_metadataextract_ai') != 'pass' and use_ai_extraction

    if not needs_rulebased_processing and not needs_ai_processing:
        logging.info(f"Skipping source_id: {source_id} as both steps have passed.")
        return

    # S3 setup
    aws_config = config.get('aws')
    s3_manager = S3Manager(region_name=aws_config['default_region'])
    bucket_name = next((s3_cfg['bucket_name'] for s3_cfg in config.get('aws', 's3') if s3_cfg['jurisdiction_code'] == record['jurisdiction_code']), None)

    # --- Step 1: Rule-based extraction ---
    if needs_rulebased_processing:
        logging.info(f"Running rule-based extraction for {source_id}")
        rulebased_start_time = datetime.now()
        try:
            # Fetch the HTML file for rule-based extraction
            s3_file_key_rulebased = get_full_s3_key(source_id, record['jurisdiction_code'], config, 'rulebased')
            if not s3_file_key_rulebased:
                 raise FileNotFoundError("Could not construct S3 key for rule-based file.")
            
            html_content = s3_manager.get_file_content(bucket_name, s3_file_key_rulebased)
            
            extractor = MetadataExtractor(field_mapping=field_mapping)
            extracted_meta, extracted_mappings = extractor.extract_from_html(html_content)
            
            if extracted_meta:
                metadata.update(extracted_meta)
                counsel_firm_mappings.extend(extracted_mappings)
                rulebased_status = 'pass'
                logging.info("Rule-based extraction successful.")
            else:
                rulebased_status = 'failed'
                logging.warning("Rule-based extraction yielded no metadata.")
        except Exception as e:
            rulebased_status = 'failed'
            logging.error(f"Rule-based extraction error: {e}")
        finally:
            rulebased_end_time = datetime.now()
            rulebased_duration = (rulebased_end_time - rulebased_start_time).total_seconds()

    # --- Step 2: AI-based extraction ---
    if needs_ai_processing:
        logging.info(f"Running AI extraction for {source_id} using provider: {ai_provider}")
        ai_start_time = datetime.now()
        ai_client = None
        raw_json = None
        is_valid = False

        try:
            # Fetch the text file for AI extraction
            s3_file_key_ai = get_full_s3_key(source_id, record['jurisdiction_code'], config, 'ai')
            if not s3_file_key_ai:
                 raise FileNotFoundError("Could not construct S3 key for AI file.")
            
            text_content = s3_manager.get_file_content(bucket_name, s3_file_key_ai)

            if ai_provider == 'gemini':
                gemini_config = config.get('models', 'gemini')
                ai_client = GeminiClient(model_name=gemini_config['model'])
                raw_json, input_tokens, output_tokens = ai_client.generate_json_from_text(prompt_content, text_content)
                
                pricing = gemini_config.get('pricing', {})
                input_price = (input_tokens / 1_000_000) * pricing.get('input_per_million', 0.0)
                output_price = (output_tokens / 1_000_000) * pricing.get('output_per_million', 0.0)
                total_price = input_price + output_price
                is_valid = ai_client.is_valid_json(raw_json)

            elif ai_provider == 'huggingface':
                hf_config = config.get('models', 'huggingface')
                ai_client = LlamaClient(model_name=hf_config['model'], base_url=hf_config['base_url'])
                raw_json = ai_client.generate_json_from_text(prompt_content, text_content)
                is_valid = ai_client.is_valid_json(raw_json)
            
            if is_valid:
                ai_data = json.loads(raw_json).get("filter_tags", {})
                if ai_data:
                    ai_status = 'pass'
                    json_filename = config.get('enrichment_filenames', 'jurismetadata_json')
                    json_s3_key = os.path.join(os.path.dirname(s3_file_key_ai), json_filename) # Use AI key for path
                    s3_manager.save_json_file(bucket_name, json_s3_key, raw_json)
                    for key, value in ai_data.items():
                        if not metadata.get(key) and value:
                            metadata[key] = value
                else:
                    ai_status = 'failed'
                    logging.warning("AI response missing 'filter_tags'.")
            else:
                ai_status = 'failed'
                logging.error("AI response was not valid JSON.")
        except Exception as e:
            ai_status = 'failed'
            logging.error(f"AI extraction error: {e}")
        finally:
            ai_end_time = datetime.now()
            ai_duration = (ai_end_time - ai_start_time).total_seconds()
            if ai_provider == 'huggingface' and ai_status == 'pass':
                hf_config = config.get('models', 'huggingface')
                hourly_rate = hf_config.get('pricing', {}).get('perhour', 0.0)
                total_price = (ai_duration / 3600) * hourly_rate
    
    # --- Step 3: Database Operations ---
    # This part of the function remains the same as before
    db_manager = DatabaseManager(config.get('database'))
    try:
        if metadata:
            if db_manager.check_and_upsert_caselaw_metadata(metadata, source_id, db_columns):
                # Counsel mapping is tied to the rule-based step
                if needs_rulebased_processing and counsel_firm_mappings:
                    db_ops_successful = db_manager.insert_counsel_firm_mapping(counsel_firm_mappings, source_id)
                else:
                    db_ops_successful = True
        else:
            db_ops_successful = True 

        # Populate the status update dictionary based on which steps were run
        if needs_rulebased_processing:
            status_updates["status_metadataextract_rulebased"] = 'pass' if rulebased_status == 'pass' and db_ops_successful else 'failed'
            status_updates["duration_metadataextract_rulebased"] = rulebased_duration
            status_updates["start_time_metadataextract_rulebased"] = rulebased_start_time
            status_updates["end_time_metadataextract_rulebased"] = rulebased_end_time
        
        if needs_ai_processing:
            status_updates["status_metadataextract_ai"] = 'pass' if ai_status == 'pass' and db_ops_successful else 'failed'
            status_updates["duration_metadataextract_ai"] = ai_duration
            status_updates["start_time_metadataextract_ai"] = ai_start_time
            status_updates["end_time_metadataextract_ai"] = ai_end_time
            status_updates["token_input_metadataextract_ai"] = input_tokens
            status_updates["token_output_metadataextract_ai"] = output_tokens
            status_updates["token_input_price_metadataextract_ai"] = total_price
            status_updates["token_output_price_metadataextract_ai"] = 0.0
        
        if status_updates:
            db_manager.update_enrichment_status(source_id, status_updates)
    
    finally:
        db_manager.close_connection()

    total_duration = (datetime.now() - overall_start_time).total_seconds()
    logging.info(f"Finished processing {source_id}. Total duration: {total_duration:.2f}s")


def main():
    """
    Main function to run the case law metadata extraction process.
    """
    logging.info("Starting case law metadata extraction service...")
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config", "config.yaml")
        config = Config(config_path=config_path)
        
        rulebased_on = config.get('extraction_switch', 'rulebased_extract')
        ai_on = config.get('extraction_switch', 'AI_extract')
        
        if not rulebased_on:
            logging.info("Rule-based extraction is turned off. Exiting.")
            return
            
        use_ai_extraction = rulebased_on and ai_on
        
        # Determine which AI provider to use based on the config
        ai_provider = None
        if use_ai_extraction:
            ai_model_name = config.get('ai_service_provider')
            if config.get('models', 'gemini', 'model') == ai_model_name:
                ai_provider = 'gemini'
                logging.info(f"AI Service Provider configured: Gemini (model: {ai_model_name})")
            elif config.get('models', 'huggingface', 'model') == ai_model_name:
                ai_provider = 'huggingface'
                logging.info(f"AI Service Provider configured: Hugging Face (model: {ai_model_name})")
            else:
                logging.warning(f"AI service provider '{ai_model_name}' not found in model configurations. AI extraction will be skipped.")
                use_ai_extraction = False
        
        if not ai_provider and ai_on:
            use_ai_extraction = False
            logging.warning("AI extraction is on, but no valid provider was configured.")

        if use_ai_extraction:
            logging.info("Rule-based and AI extraction are enabled.")
        else:
            logging.info("Only rule-based extraction is enabled.")

    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Failed to load configuration: {e}")
        return

    prompt_content = ""
    if use_ai_extraction:
        try:
            prompt_path = os.path.join(script_dir, "config", "prompt.txt")
            with open(prompt_path, 'r') as f:
                prompt_content = f.read()
            logging.info("Successfully loaded AI prompt from config folder.")
        except FileNotFoundError:
            logging.error("AI extraction is on, but 'prompt.txt' was not found in the 'config' directory. AI step will be skipped.")
            use_ai_extraction = False

    db_config = config.get('database')
    registry_config = config.get('tables_registry')
    jurisdiction_codes = [s3_config['jurisdiction_code'] for s3_config in config.get('aws', 's3')]
    processing_years = config.get('tables_registry', 'processing_years')

    db_columns = config.get('database', 'caselaw_metadata_columns')
    if not db_columns:
        logging.error("Could not find 'caselaw_metadata_columns' in config.yaml.")
        return

    field_mapping = {
        "Citation": "citation",
        "Key issues": "key_issues",
        "Catchwords": "keywords",
        "Judgment of": "presiding_officer",
        "Judge": "presiding_officer",
        "Panelist": "panelist",
        "Orders": "orders",
        "Decision": "decision",
        "Decision Date": "judgment_date",
        "Cases Cited": "cases_cited",
        "Legislation Cited": "legislation_cited",
        "Filenumber": "file_no",
        "Hearing Dates": "hearing_date",
        "Jurisdiction": "matter_type",
        "Parties": "parties",
        "Category": "category",
        "BJS Number": "bjs_number"
    }

    db_manager_for_fetch = DatabaseManager(db_config)
    records_to_process = get_records_to_process(db_manager_for_fetch, registry_config, jurisdiction_codes, processing_years)

    if not records_to_process:
        logging.info("No records to process. Exiting.")
        return

    for record in records_to_process:
        process_record(record, config, field_mapping, db_columns, use_ai_extraction, prompt_content, ai_provider)

    logging.info("All records processed.")

if __name__ == "__main__":
    main()

