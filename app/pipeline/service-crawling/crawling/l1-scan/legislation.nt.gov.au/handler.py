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
    AWS Lambda handler function.
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
    """Main function to initialize and run the crawler with intelligent resume logic."""
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
    final_error_message = ""
    
    for i, journey in enumerate(config['crawler_config']['journeys']):
        retries = 0
        journey_succeeded = False
        # NEW: State object to track progress within this journey
        journey_state = {'last_completed_index': -1}

        while retries <= MAX_RETRIES and not journey_succeeded:
            driver = None
            try:
                if retries > 0:
                    print(f"\n--- Retrying Journey '{journey['description']}' (Attempt {retries + 1}/{MAX_RETRIES + 1}) ---")

                driver = initialize_driver()
                
                print(f"\nNavigating to base URL for journey: {base_url}")
                driver.get(base_url)

                navigation_path_parts = ["Home", journey.get('description', f'Journey-{i+1}')]
                
                print(f"\n=================================================")
                print(f"Starting Journey: {journey['description']} ({journey['journey_id']})")
                print(f"=================================================")
                
                for step in journey['steps']:
                    # Pass the state object down to the processing functions
                    if not process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, journey_state=journey_state):
                        raise Exception(f"Step failed in Journey '{journey['journey_id']}'")
                
                journey_succeeded = True
                print(f"\n✅ Journey '{journey['description']}' completed successfully on attempt {retries + 1}.")

            except Exception as e:
                retries += 1
                print(f"\n!!! An exception occurred during Journey '{journey['description']}': {e}")
                print(f"  - This was attempt {retries}. Retrying if possible.")
                if retries > MAX_RETRIES:
                    print(f"  - Max retries exceeded for this journey. Marking as failed.")
                    final_status = 'failed'
                    final_error_message += f"Journey '{journey['description']}' failed after {MAX_RETRIES} retries. Last error: {e}\n"
                else:
                    time.sleep(5)
            
            finally:
                if driver:
                    print(f"Closing WebDriver for attempt {retries}.")
                    driver.quit()

    message = f"Successfully processed {job_state['records_saved']} new records."
    if final_status == 'failed':
        message = f"Job failed. Processed {job_state['records_saved']} new records. Last errors: {final_error_message}"
    update_audit_log_entry(db_engine, audit_log_id, final_status, message)
    print("\nAll journeys finished.")

if __name__ == "__main__":
    # This block is for local testing. It simulates the Lambda event.
    parent_url_id_for_testing = "dc7dc489-691b-4df3-8988-2700b6374219" # <--- REPLACE with the actual ID from your DB for the NT site
    sitemap_for_testing = "sitemap_legislation_nt_gov_au.json"
    destination_table_for_testing = "l1_scan_legislation_nt_gov_au"
    
    print(f"--- Running in local test mode for parent_url_id: {parent_url_id_for_testing} ---")
    
    if "your_nt_parent_url_id_here" in parent_url_id_for_testing:
        print("\nWARNING: Please replace 'your_nt_parent_url_id_here' in the script with a valid ID for testing.")
    else:
        run_crawler(parent_url_id_for_testing, sitemap_for_testing, destination_table_for_testing)

