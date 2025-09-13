import json
import os
import time

# Import the database engine creator from your utils file
from utils.aws_utils import create_db_engine
from core.config_loader import load_config

from core.audit_log import create_audit_log_entry, update_audit_log_entry
from core.database import get_parent_url_details
from core.config_loader import load_config
from core.driver import initialize_driver
from core.navigation import process_step

# --- Configuration ---
# The sitemap file name is now passed dynamically
MAX_RETRIES = 3 # Maximum number of times to retry a failed journey
NAVIGATION_PATH_DEPTH = int(os.getenv("NAVIGATION_PATH_DEPTH", 3))
    
# --- Lambda Handler ---
def lambda_handler(event, context):
    """
    AWS Lambda handler function. Expects 'parent_url_id', 'sitemap_file_name', and 'destination_table'.
    """
    print("Lambda function invoked.")
    parent_url_id = event.get('parent_url_id')
    sitemap_file_name = event.get('sitemap_file_name')
    destination_table = event.get('destination_table')
    if not all([parent_url_id, sitemap_file_name, destination_table]):
        error_msg = 'Error: parent_url_id, sitemap_file_name, and destination_table are required.'
        print(f"FATAL ERROR: {error_msg}")
        return {'statusCode': 400, 'body': json.dumps(error_msg)}

    print(f"Starting FULL crawler run for parent_url_id: {parent_url_id} using sitemap: {sitemap_file_name}")
    run_crawler(parent_url_id, sitemap_file_name, destination_table)
    
    return {'statusCode': 200, 'body': json.dumps(f'Successfully completed crawling for {parent_url_id}')}

def run_crawler(parent_url_id, sitemap_file_name, destination_table):
    """Main function to initialize and run the crawler."""
    config_file_path = os.path.join('config', sitemap_file_name)
    config = load_config(config_file_path)
    if not config: return

    db_engine = create_db_engine()
    if not db_engine: 
        print("Database engine creation failed. Aborting crawler run.")
        return

    base_url = get_parent_url_details(db_engine, parent_url_id)
    if not base_url: return

    job_name = f"crawling-{parent_url_id}-{sitemap_file_name}"
    audit_log_id = create_audit_log_entry(db_engine, job_name)
    if not audit_log_id: return

    job_state = {'records_saved': 0}
    final_status = 'success'
    
    # --- KEY CHANGE: Added journey retry loop ---
    MAX_JOURNEY_RETRIES = 3

    try:
        for i, journey in enumerate(config['crawler_config']['journeys']):
            journey_succeeded = False
            for attempt in range(MAX_JOURNEY_RETRIES):
                driver = None
                try:
                    if attempt > 0:
                        print(f"\n--- RETRYING Journey '{journey['journey_id']}' (Attempt {attempt + 1}/{MAX_JOURNEY_RETRIES}) ---")
                        time.sleep(5) # Wait before retrying

                    driver = initialize_driver()
                    
                    navigation_path_parts = ["Home"]
                    if journey.get('description'):
                        navigation_path_parts.append(journey['description'])

                    print(f"\n=================================================")
                    print(f"Starting Journey: {journey['description']} ({journey['journey_id']})")
                    print(f"=================================================")
                    
                    driver.get(base_url)
                    
                    for step in journey['steps']:
                        if not process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table):
                            # This will raise an exception to be caught below, triggering a retry
                            raise Exception(f"Step failed in Journey '{journey['journey_id']}'")
                    
                    # If all steps complete without error, mark as success and break the retry loop
                    journey_succeeded = True
                    print(f"\n✅ Journey '{journey['journey_id']}' completed successfully.")
                    break

                except Exception as e:
                    print(f"\n!!! An exception occurred during Journey '{journey['journey_id']}' on attempt {attempt + 1}: {e}")
                
                finally:
                    if driver:
                        print(f"Closing WebDriver for attempt {attempt + 1}.")
                        driver.quit()
            
            if not journey_succeeded:
                print(f"\n❌ Journey '{journey['journey_id']}' failed after {MAX_JOURNEY_RETRIES} attempts.")
                final_status = 'failed'

    except Exception as e:
        print(f"  - An uncaught exception terminated the crawler run: {e}")
        final_status = 'failed'
    
    finally:
        message = f"Job finished. Processed {job_state['records_saved']} new records."
        if final_status == 'failed':
            message = f"Job finished with failures. Processed {job_state['records_saved']} new records."
        update_audit_log_entry(db_engine, audit_log_id, final_status, message)
        print("\nAll journeys finished.")


if __name__ == "__main__":
    parent_url_id_for_testing = "493df9a1-e971-451e-8bf0-de5092019ef1" 
    sitemap_for_testing = "sitemap_legislation_gov_au.json"
    destination_table_for_testing = "l1_scan_legislation_gov_au"
    
    print(f"--- Running in local test mode for parent_url_id: {parent_url_id_for_testing} ---")
    
    if "your_federal_legislation_parent_url_id" in parent_url_id_for_testing:
        print("\nWARNING: Please replace 'your_federal_legislation_parent_url_id' in the script with a valid ID from your database for testing.")
    else:
        run_crawler(parent_url_id_for_testing, sitemap_for_testing, destination_table_for_testing)
