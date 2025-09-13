import logging
import os
from utils.config_loader import load_config
from utils.db_utils import get_urls_to_crawl, update_scan_result
from utils.scraper import scrape_content, upload_to_s3, get_s3_object_size

def process_record(record, s3_config, table_name, subfolder_name):
    """
    Processes a single record sequentially: scrapes, validates, uploads, 
    and updates status with errors and file sizes.
    """
    record_id = record.get('id')
    url = record.get('book_url')
    book_name = record.get('book_name', 'Unknown Title')
    
    baseline_size, scraped_size = None, None

    s3_bucket = s3_config.get('bucket_name')
    output_filename = s3_config.get('output_filename')
    baseline_filename = s3_config.get('baseline_filename')
    should_validate_size = s3_config.get('validate_content_size', False)

    if not all([url, record_id, s3_bucket, output_filename, baseline_filename]):
        error_msg = f"Skipping record due to missing config data: ID={record_id}"
        logging.warning(error_msg)
        update_scan_result(table_name, record_id, 'fail', error_msg, baseline_size, scraped_size)
        return

    try:
        logging.info(f"STARTING scrape for '{book_name}' (ID: {record_id})")
        content = scrape_content(url)

        if not content:
            error_msg = "Scraping returned no content."
            logging.error(f"FAILED to scrape: {error_msg} for '{book_name}' (ID: {record_id})")
            update_scan_result(table_name, record_id, 'fail', error_msg, baseline_size, scraped_size)
            return
        
        # --- ALWAYS CALCULATE FILE SIZES FOR LOGGING ---
        scraped_size = len(content.encode('utf-8'))
        baseline_s3_key = f"{subfolder_name}/{record_id}/{baseline_filename}"
        baseline_size = get_s3_object_size(s3_bucket, baseline_s3_key)
        
        # --- OPTIONALLY VALIDATE CONTENT SIZE ---
        if should_validate_size:
            if baseline_size == -1:
                error_msg = f"Could not retrieve baseline file '{baseline_s3_key}' from S3 for validation."
                logging.error(f"{error_msg} for '{book_name}' (ID: {record_id})")
                update_scan_result(table_name, record_id, 'fail', error_msg, baseline_size, scraped_size)
                return

            logging.info(f"Scraped size: {scraped_size} bytes, Baseline size: {baseline_size} bytes for '{book_name}'")

            if scraped_size <= baseline_size:
                error_msg = f"Scraped content size ({scraped_size} bytes) is not greater than baseline ({baseline_size} bytes)."
                logging.error(f"VALIDATION FAILED for '{book_name}' (ID: {record_id}): {error_msg}")
                update_scan_result(table_name, record_id, 'fail', error_msg, baseline_size, scraped_size)
                return
        else:
            logging.info("Content size validation is disabled. Skipping.")
            
        # --- UPLOAD TO S3 ---
        s3_key = f"{subfolder_name}/{record_id}/{output_filename}"
        success = upload_to_s3(content, s3_bucket, s3_key)
        
        if success:
            update_scan_result(table_name, record_id, 'pass', None, baseline_size, scraped_size)
            logging.info(f"SUCCESS scraping and uploading '{book_name}' (ID: {record_id})")
        else:
            error_msg = "Failed to upload content to S3."
            logging.error(f"UPLOAD FAILED for '{book_name}' (ID: {record_id})")
            update_scan_result(table_name, record_id, 'fail', error_msg, baseline_size, scraped_size)

    except Exception as e:
        error_msg = f"An unexpected error occurred: {str(e)}"
        logging.error(f"CRITICAL FAILURE for '{book_name}' (ID: {record_id}): {error_msg}", exc_info=True)
        update_scan_result(table_name, record_id, 'fail', error_msg, baseline_size, scraped_size)


def run_crawler():
    """Main function to run the web crawler sequentially."""
    config = load_config()

    s3_config = config.get('s3', {})
    if not s3_config.get('bucket_name'):
        logging.error("S3 bucket name not found in configuration. Exiting.")
        return

    logging.info("Running the crawler in sequential mode.")

    for table_info in config.get('tables_to_crawl', []):
        if table_info.get('enabled'):
            table_name = table_info.get('table_name')
            subfolder_name = table_info.get('subfolder_name')
            jurisdiction = table_info.get('jurisdiction', 'Unknown')
            
            if not all([table_name, subfolder_name]):
                logging.warning(f"Skipping incomplete table configuration: {table_info}")
                continue

            logging.info(f"--- Processing Jurisdiction: {jurisdiction} (Table: {table_name}) ---")
            
            records_to_crawl = get_urls_to_crawl(table_name)
            
            if not records_to_crawl:
                logging.info(f"No new records to crawl for {jurisdiction}.")
                continue

            logging.info(f"Found {len(records_to_crawl)} records to process for {jurisdiction}. Starting sequential crawl...")

            # --- PROCESS RECORDS ONE BY ONE ---
            for record in records_to_crawl:
                try:
                    process_record(record, s3_config, table_name, subfolder_name)
                except Exception as exc:
                    logging.error(f'A critical error occurred while processing record {record.get("id")}: {exc}', exc_info=True)

