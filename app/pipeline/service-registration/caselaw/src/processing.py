import logging
import os
from datetime import datetime, timezone
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import text
from utils.database import create_db_engine
from utils.parsing import load_config, load_json_config, parse_citation, parse_parties
from utils.audit import write_audit_log
import json

def verify_content_files(s3_path, source_id):
    """
    Verifies the existence and content of required HTML files for a given source_id in S3.

    Args:
        s3_path (str): The S3 path (e.g., 's3://legal-store/case-laws/nt/').
        source_id (str): The unique identifier for the case.

    Returns:
        str: 'pass' if all files exist and are non-empty, 'fail' otherwise.
    """
    if not s3_path.startswith('s3://'):
        logging.error(f"Invalid S3 path provided to verify_content_files: {s3_path}")
        return 'failed'

    s3 = boto3.client('s3')
    files_to_check = ['excerpt.html', 'miniviewer.html', 'summary.html']
    
    parts = s3_path.replace('s3://', '').split('/')
    bucket_name = parts[0]
    base_prefix = '/'.join(parts[1:]) if len(parts) > 1 else ''
    
    if base_prefix and not base_prefix.endswith('/'):
        base_prefix += '/'

    try:
        for file_name in files_to_check:
            s3_key = f"{base_prefix}{source_id}/{file_name}"
            try:
                response = s3.head_object(Bucket=bucket_name, Key=s3_key)
                if response['ContentLength'] == 0:
                    logging.warning(f"S3 file check failed for s3://{bucket_name}/{s3_key}. Object is empty.")
                    return 'failed'
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logging.warning(f"S3 file check failed for s3://{bucket_name}/{s3_key}. Object is missing.")
                    return 'failed'
                else:
                    logging.error(f"An unexpected AWS error occurred checking {s3_key}: {e}")
                    raise
        logging.info(f"All content files verified in S3 for source_id {source_id}.")
        return 'pass'
    except Exception as e:
        logging.error(f"Error during S3 file verification for source_id {source_id}. Error: {e}")
        return 'failed'

def get_name_from_code(code, all_codes, code_type):
    """
    Gets the full name for a given code from the configuration.
    
    Args:
        code (str): The code to look up (e.g., 'NSWSC', 'AP')
        all_codes (DataFrame): The combined configuration DataFrame
        code_type (str): The type of code ('tribunal' or 'panel_or_division')
    
    Returns:
        str: The full name if found, None otherwise
    """
    if not code:
        return None
    
    matching_rows = all_codes[(all_codes['code'] == code) & (all_codes['type'] == code_type)]
    
    if not matching_rows.empty:
        return matching_rows.iloc[0]['name']
    
    return None

def update_metadata_table(engine, metadata_data, metadata_table='caselaw_metadata'):
    """
    Updates or inserts metadata fields into the caselaw_metadata table.
    
    Args:
        engine: The SQLAlchemy engine for the database.
        metadata_data: Dictionary containing all metadata fields.
        metadata_table: The name of the metadata table (default: 'caselaw_metadata').
    """
    try:
        source_id = metadata_data['source_id']
        
        with engine.connect() as conn:
            # Check if record exists
            check_query = text(f"SELECT source_id FROM {metadata_table} WHERE source_id = :source_id")
            result = conn.execute(check_query, {'source_id': source_id})
            exists = result.fetchone() is not None
            
            if exists:
                # Update existing record with metadata fields only
                update_fields = [
                    'neutral_citation', 'tribunal_code', 'tribunal_name', 
                    'panel_or_division', 'panel_or_division_name', 'jurisdiction_code',
                    'year', 'decision_number', 'decision_date', 'primary_party',
                    'secondary_party', 'members', 'member_info_json'
                ]
                
                # Build update query with only non-None values
                update_data = {k: v for k, v in metadata_data.items() if k in update_fields and v is not None}
                update_data['source_id'] = source_id
                
                update_cols = ", ".join([f"{key} = :{key}" for key in update_data if key != 'source_id'])
                update_query = text(f"""
                    UPDATE {metadata_table} 
                    SET {update_cols}
                    WHERE source_id = :source_id
                """)
                conn.execute(update_query, update_data)
                logging.debug(f"Updated metadata for source_id {source_id} in {metadata_table}")
            else:
                # Insert new record with metadata fields only
                # Build insert query dynamically based on available fields
                fields = list(metadata_data.keys())
                fields_str = ", ".join(fields)
                values_str = ", ".join([f":{field}" for field in fields])
                
                insert_query = text(f"""
                    INSERT INTO {metadata_table} 
                    ({fields_str})
                    VALUES ({values_str})
                """)
                conn.execute(insert_query, metadata_data)
                logging.debug(f"Inserted new metadata record for source_id {source_id} in {metadata_table}")
            
            conn.commit()
            return True
            
    except Exception as e:
        logging.error(f"Failed to update metadata table for source_id {metadata_data.get('source_id')}. Error: {e}")
        return False

def process_caselaw_data():
    """
    Main ETL function to extract, transform, and load caselaw data.
    Updates caselaw_registry (without name fields) and caselaw_metadata (with all fields).
    """
    logging.info("Starting caselaw data processing job.")
    program_start_time = datetime.now(timezone.utc)
    job_status = 'success'
    error_message = None
    
    config = load_config('config/config.yaml')
    if not config:
        logging.critical("Could not load config.yaml. Aborting job.")
        return

    job_name_from_config = config.get('job_name', 'caselaw_etl_job')
    job_id_from_config = config.get('job_id', 'unknown')

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    if not db_user or not db_password:
        logging.critical("DB_USER and DB_PASSWORD environment variables must be set. Aborting job.")
        return
    logging.info("Successfully loaded database credentials from environment.")

    dest_engine = create_db_engine(config['database']['destination'], db_user, db_password)
    if not dest_engine:
        logging.critical("Destination database connection failed. Aborting job.")
        return

    audit_log_config = config.get('audit_log_table', [{}])[0]
    audit_table_name = audit_log_config.get('table')

    try:
        aus_codes = load_json_config('config/australia_config.json')
        nz_codes = load_json_config('config/new_zealand_config.json')
        all_codes = pd.concat([aus_codes, nz_codes], ignore_index=True)

        source_engine = create_db_engine(config['database']['source'], db_user, db_password)
        if not source_engine:
            raise ConnectionError("Source database connection failed.")
            
        dest_table = config['tables']['tables_to_write'][0]['table']
        filepath_from_config = config.get('filepath', 's3://legal-store/case-laws/')
        base_s3_path = "/".join(filepath_from_config.split('/')[:-2]) if 's3://' in filepath_from_config else filepath_from_config
        
        logging.info(f"Program run started at: {program_start_time.isoformat()}")

        for source_info in config['tables']['tables_to_read']:
            source_table = source_info['table']
            jurisdiction_code = source_info.get('jurisdiction')
            storage_folder = source_info.get('storage_folder')

            if not storage_folder:
                logging.warning(f"'storage_folder' not configured for table {source_table}. Skipping this table.")
                continue

            logging.info(f"--- Processing source table: {source_table} (Jurisdiction: {jurisdiction_code}, Storage Folder: {storage_folder}) ---")
            
            source_df = pd.read_sql_table(source_table, source_engine)
            logging.info(f"Read {len(source_df)} records from {source_table}.")

            # Get IDs of all records already marked as 'pass' in the destination table
            try:
                with dest_engine.connect() as connection:
                    query = text(f"SELECT source_id, status_registration FROM {dest_table} WHERE jurisdiction_code = :jurisdiction_code")
                    dest_df = pd.read_sql(query, connection, params={'jurisdiction_code': jurisdiction_code})
                
                processed_ids = dest_df[dest_df['status_registration'] == 'pass']['source_id'].tolist()
                existing_records = dest_df.set_index('source_id').to_dict('index')

            except Exception as e:
                logging.error(f"Could not query destination table '{dest_table}' for existing records. Error: {e}")
                processed_ids = []
                existing_records = {}
            
            # Filter the source DataFrame to exclude successfully processed records
            if processed_ids:
                records_to_process_df = source_df[~source_df['id'].isin(processed_ids)].copy()
            else:
                records_to_process_df = source_df.copy()

            total_to_process = len(records_to_process_df)
            if total_to_process == 0:
                logging.info(f"No new or failed records to process for {source_table}.")
                continue
            
            logging.info(f"Found {total_to_process} records to process for {source_table}.")

            for index, row in records_to_process_df.iterrows():
                record_num = index + 1
                source_id = row['id']
                log_prefix = f"Record {record_num}/{total_to_process} (ID: {source_id})"
                
                logging.info(f"{log_prefix}: Starting processing.")
                record_start_time = datetime.now(timezone.utc)
                
                try:
                    # Check if the record already exists
                    record_exists = source_id in existing_records

                    # Check for known bad data patterns
                    if 'null' in str(row['book_context']).lower():
                        logging.warning(f"{log_prefix}: Source data contains 'null' values in citation: {row['book_context']}")
                        failure_reasons = ["Source data error: contains 'null' values in citation"]
                        final_status = 'failed'
                        reason_for_failure = '; '.join(failure_reasons)
                        
                        # Create minimal record for failed entries (for caselaw_registry)
                        registry_data = {
                            'source_id': source_id,
                            'neutral_citation': row['book_context'],
                            'jurisdiction_code': jurisdiction_code,
                            'tribunal_code': None,
                            'panel_or_division': None,
                            'year': None,  # Keep in both tables
                            'decision_date': None,  # Keep in both tables
                            'file_path': f"{base_s3_path}/{storage_folder}/{source_id}",
                            'source_url': row.get('book_url'),
                            'source_table': source_table,  # Add source table tracking
                            'book_name': row['book_name'],
                            'status_content_download': 'failed'
                        }
                        
                        # Metadata data for failed entries
                        metadata_data = {
                            'source_id': source_id,
                            'neutral_citation': row['book_context'],
                            'tribunal_code': None,
                            'tribunal_name': None,
                            'panel_or_division': None,
                            'panel_or_division_name': None,
                            'jurisdiction_code': jurisdiction_code,
                            'year': None,
                            'decision_number': None,
                            'decision_date': None,
                            'primary_party': None,
                            'secondary_party': None,
                            'members': None,
                            'member_info_json': None
                        }
                    else:
                        # Parse citation to get ALL components
                        citation_details = parse_citation(row['book_context'], all_codes, jurisdiction_code)
                        
                        # Parse party names from book_name
                        primary_party, secondary_party = parse_parties(row['book_name'])
                        
                        # Get full names for tribunal and panel/division
                        tribunal_name = get_name_from_code(
                            citation_details['tribunal_code'], 
                            all_codes, 
                            'tribunal'
                        )
                        
                        panel_or_division_name = get_name_from_code(
                            citation_details['panel_or_division'], 
                            all_codes, 
                            'panel_or_division'
                        )
                        
                        file_path = f"{base_s3_path}/{storage_folder}/{source_id}"
                        storage_folder_s3_path = f"{base_s3_path}/{storage_folder}/"
                        content_download_status = verify_content_files(storage_folder_s3_path, source_id)

                        # Build the record for caselaw_registry (keeping only essential fields)
                        registry_data = {
                            'source_id': source_id,
                            'neutral_citation': row['book_context'],
                            'jurisdiction_code': citation_details['jurisdiction_code'] or jurisdiction_code,
                            'tribunal_code': citation_details['tribunal_code'],
                            'panel_or_division': citation_details['panel_or_division'],
                            'year': citation_details['year'],  # Keep in both tables
                            'decision_date': citation_details['decision_date'],  # Keep in both tables
                            'file_path': file_path,
                            'source_url': row.get('book_url'),
                            'source_table': source_table,  # Add source table tracking
                            'book_name': row['book_name'],
                            'status_content_download': content_download_status,
                        }
                        
                        # Build the data for caselaw_metadata (WITH all fields including names)
                        metadata_data = {
                            'source_id': source_id,
                            'neutral_citation': row['book_context'],
                            'tribunal_code': citation_details['tribunal_code'],
                            'tribunal_name': tribunal_name,
                            'panel_or_division': citation_details['panel_or_division'],
                            'panel_or_division_name': panel_or_division_name,
                            'jurisdiction_code': citation_details['jurisdiction_code'] or jurisdiction_code,
                            'year': citation_details['year'],
                            'decision_number': citation_details['decision_number'],
                            'decision_date': citation_details['decision_date'],
                            'primary_party': primary_party,
                            'secondary_party': secondary_party,
                            'members': citation_details['members'],
                            'member_info_json': json.dumps(citation_details['member_info']) if citation_details['member_info'] else None
                        }

                        failure_reasons = []
                        if content_download_status == 'failed':
                            failure_reasons.append("Missing or empty content files")

                        # Check mandatory fields (adjusted list - decision_number is optional)
                        mandatory_fields = ['neutral_citation', 'jurisdiction_code', 'tribunal_code', 'year', 'book_name']
                        missing_fields = [field for field in mandatory_fields if not registry_data.get(field) and registry_data.get(field) != 0]
                        if missing_fields:
                            failure_reasons.append(f"Missing mandatory fields: {', '.join(missing_fields)}")
                        
                        # Additional validation: Check if tribunal code was successfully parsed
                        if not citation_details['tribunal_code']:
                            failure_reasons.append(f"Could not parse tribunal code from: {row['book_context']}")
                        
                        if failure_reasons:
                            final_status = 'failed'
                            reason_for_failure = '; '.join(failure_reasons)
                            logging.warning(f"{log_prefix}: Marking as 'failed'. Reason(s): {reason_for_failure}")
                        else:
                            final_status = 'pass'
                            reason_for_failure = None
                            decision_info = f"Decision #{citation_details['decision_number']}" if citation_details['decision_number'] else "No decision number"
                            logging.info(f"{log_prefix}: All validations passed. {decision_info}")
                    
                    record_end_time = datetime.now(timezone.utc)
                    record_duration = (record_end_time - record_start_time).total_seconds()

                    registry_data.update({
                        'status_registration': final_status,
                        'reason_failed': reason_for_failure,
                        'start_time_registration': program_start_time,
                        'end_time_registration': record_end_time,
                        'duration_registration': record_duration
                    })

                    # Update caselaw_registry table (without name fields)
                    with dest_engine.connect() as conn:
                        if record_exists:
                            # Remove None values for UPDATE to avoid overwriting with NULL
                            update_data = {k: v for k, v in registry_data.items() if v is not None}
                            update_cols = ", ".join([f"{key} = :{key}" for key in update_data])
                            update_query = text(f"UPDATE {dest_table} SET {update_cols} WHERE source_id = :source_id")
                            conn.execute(update_query, update_data)
                        else:
                            insert_df = pd.DataFrame([registry_data])
                            insert_df.to_sql(dest_table, conn, if_exists='append', index=False)
                        conn.commit()
                    
                    # Update caselaw_metadata table with all metadata fields
                    metadata_success = update_metadata_table(
                        dest_engine, 
                        metadata_data,
                        'caselaw_metadata'
                    )
                    
                    if not metadata_success:
                        logging.warning(f"{log_prefix}: Failed to update metadata table, but main record was processed.")
                    
                    logging.info(f"{log_prefix}: Successfully processed in {record_duration:.4f} seconds. Final status: {final_status}")

                except Exception as e:
                    logging.error(f"{log_prefix}: Failed to process. Error: {e}", exc_info=True)

    except Exception as e:
        job_status = 'failed'
        error_message = str(e)
        logging.critical(f"A critical error occurred, terminating job. Error: {error_message}", exc_info=True)
    finally:
        program_end_time = datetime.now(timezone.utc)
        message = f"Job finished with status: {job_status}."
        if error_message:
            message += f" Details: {error_message}"

        if audit_table_name:
            write_audit_log(
                engine=dest_engine,
                table_name=audit_table_name,
                job_name=job_name_from_config,
                job_id=job_id_from_config,
                start_time=program_start_time,
                end_time=program_end_time,
                status=job_status,
                message=message
            )
        else:
            logging.warning("audit_log_table not configured in config.yaml. Skipping audit log.")

        logging.info("Caselaw data processing job finished.")