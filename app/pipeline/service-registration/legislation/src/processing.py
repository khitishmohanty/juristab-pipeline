import logging
import os
from datetime import datetime, timezone
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import text
from utils.database import create_db_engine
from utils.parsing import load_config, parse_legislation_context
from utils.audit import write_audit_log

def verify_content_files(s3_path, source_id):
    """
    Verifies the existence and content of required HTML files for a given source_id in S3.
    This function is generic and can be used for legislation content as well.

    Args:
        s3_path (str): The S3 path (e.g., 's3://legal-store/legislation/nt/').
        source_id (str): The unique identifier for the legislation document.

    Returns:
        str: 'pass' if all files exist and are non-empty, 'fail' otherwise.
    """
    if not s3_path.startswith('s3://'):
        logging.error(f"Invalid S3 path provided to verify_content_files: {s3_path}")
        return 'fail'

    s3 = boto3.client('s3')
    # Assuming the same file structure for legislation content
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
                    return 'fail'
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logging.warning(f"S3 file check failed for s3://{bucket_name}/{s3_key}. Object is missing.")
                    return 'fail'
                else:
                    logging.error(f"An unexpected AWS error occurred checking {s3_key}: {e}")
                    raise
        logging.info(f"All content files verified in S3 for source_id {source_id}.")
        return 'pass'
    except Exception as e:
        logging.error(f"Error during S3 file verification for source_id {source_id}. Error: {e}")
        return 'fail'

def process_legislation_data():
    """
    Main ETL function to extract, transform, and load legislation data.
    """
    logging.info("Starting legislation data processing job.")
    program_start_time = datetime.now(timezone.utc)
    job_status = 'success'
    error_message = None
    
    config = load_config('config/config.yaml')
    if not config:
        logging.critical("Could not load config.yaml. Aborting job.")
        return

    job_name_from_config = config.get('job_name', 'legislation_etl_job')
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
        source_engine = create_db_engine(config['database']['source'], db_user, db_password)
        if not source_engine:
            raise ConnectionError("Source database connection failed.")
            
        dest_table = config['tables']['tables_to_write'][0]['table']
        filepath_from_config = config.get('filepath', 's3://legal-store/legislation/')
        
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

            # Efficiently check for already processed records
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
            
            # Filter out records that have already been successfully processed
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
                    record_exists = source_id in existing_records

                    context_details = parse_legislation_context(row['book_context'])
                    start_date = context_details.get('start_date')
                    book_version = context_details.get('book_version') # Extract book_version
                    
                    storage_folder_s3_path = f"{filepath_from_config}{storage_folder}/"
                    file_path = f"{filepath_from_config}{storage_folder}/{source_id}"
                    content_download_status = verify_content_files(storage_folder_s3_path, source_id)

                    record_data = {
                        'source_id': source_id,
                        'book_name': row['book_name'],
                        'book_version': book_version, # Add book_version to record
                        'jurisdiction_code': jurisdiction_code,
                        'start_date': start_date,
                        'year': start_date.year if start_date else None,
                        'file_path': file_path,
                        'source_url': row.get('book_url'),
                        'status_content_download': content_download_status,
                    }

                    failure_reasons = []
                    if content_download_status == 'fail':
                        failure_reasons.append("Missing or empty content files")

                    mandatory_fields = ['book_name', 'jurisdiction_code', 'start_date', 'year']
                    missing_fields = [field for field in mandatory_fields if not record_data.get(field)]
                    if missing_fields:
                        failure_reasons.append(f"Missing mandatory fields: {', '.join(missing_fields)}")
                    
                    if failure_reasons:
                        final_status = 'fail'
                        reason_for_failure = '; '.join(failure_reasons)
                        logging.warning(f"{log_prefix}: Marking as 'fail'. Reason(s): {reason_for_failure}")
                    else:
                        final_status = 'pass'
                        reason_for_failure = None
                    
                    record_end_time = datetime.now(timezone.utc)
                    record_duration = (record_end_time - record_start_time).total_seconds()

                    record_data.update({
                        'status_registration': final_status,
                        'reason_failed': reason_for_failure,
                        'start_time_registration': program_start_time,
                        'end_time_registration': record_end_time,
                        'duration_registration': record_duration
                    })

                    with dest_engine.connect() as conn:
                        if record_exists:
                            # Prepare for UPDATE
                            update_cols = ", ".join([f"{key} = :{key}" for key in record_data])
                            update_query = text(f"UPDATE {dest_table} SET {update_cols} WHERE source_id = :source_id")
                            conn.execute(update_query, record_data)
                        else:
                            # Prepare for INSERT
                            insert_df = pd.DataFrame([record_data])
                            insert_df.to_sql(dest_table, conn, if_exists='append', index=False)
                        conn.commit()
                    logging.info(f"{log_prefix}: Successfully processed in {record_duration:.4f} seconds. Final status: {final_status}")

                except Exception as e:
                    logging.error(f"{log_prefix}: Failed to process. Error: {e}", exc_info=True)

    except Exception as e:
        job_status = 'fail'
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

        logging.info("Legislation data processing job finished.")