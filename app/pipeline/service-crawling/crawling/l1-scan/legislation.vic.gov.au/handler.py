import json
import os
import time

# Import the database engine creator from your utils file
from utils.aws_utils import create_db_engine
from utils.common import get_page_from_url
from core.audit_log import create_audit_log_entry, update_audit_log_entry
from core.database import get_parent_url_details, save_book_links_to_db
from core.config_loader import load_config
from core.driver import initialize_driver
from core.navigation import process_pagination_loop, process_step

# --- Configuration ---
CONFIG_FILE_PATH_lEGISLATION_VIC_GOV_AU = os.path.join('config', 'sitemap_legislation_vic_gov_au.json')
MAX_RETRIES = 3 # Maximum number of times to retry a failed journey
NAVIGATION_PATH_DEPTH = int(os.getenv("NAVIGATION_PATH_DEPTH", 3)) # Duplicate checking
    
# --- Lambda Handler ---
def lambda_handler(event, context):
    """
    AWS Lambda handler function. Expects a 'parent_url_id' key.
    """
    print("Lambda function invoked.")
    parent_url_id = event.get('parent_url_id')
    if not parent_url_id:
        print("FATAL ERROR: 'parent_url_id' not found in the Lambda event.")
        return {'statusCode': 400, 'body': json.dumps('Error: parent_url_id is required.')}

    # These would ideally come from the event or environment variables
    sitemap_filename = os.path.join('config', 'sitemap_legislation_vic_gov_au.json')
    destination_tablename = "book_links"

    print(f"Starting FULL crawler run for parent_url_id: {parent_url_id}")
    run_crawler(parent_url_id, sitemap_filename, destination_tablename)
    
    return {'statusCode': 200, 'body': json.dumps(f'Successfully completed crawling for {parent_url_id}')}

def run_crawler(parent_url_id, sitemap_filename, destination_tablename):
    """Main function to initialize and run the crawler."""
    config = load_config(sitemap_filename)
    if not config: return
    db_engine = create_db_engine()
    if not db_engine: return
    base_url = get_parent_url_details(db_engine, parent_url_id)
    if not base_url: return

    job_name = f"Surface crawling for parent url ID: {parent_url_id}"
    audit_log_id = create_audit_log_entry(db_engine, job_name)
    if not audit_log_id:
        print("Exiting due to failure to create audit log.")
        return

    job_state = {'records_saved': 0}
    final_status = 'success'
    final_error_message = None
    
    try:
        for i, journey in enumerate(config['crawler_config']['journeys']):
            retries = 0
            journey_id = journey['journey_id']
            resume_from_url = None
            
            while retries < MAX_RETRIES:
                driver = None
                try:
                    navigation_path_parts = ["Home"]
                    for step in journey['steps']:
                        if step.get('is_breadcrumb'):
                            navigation_path_parts.append(step.get('description', ''))
                    navigation_path_parts.append(journey_id)

                    driver = initialize_driver()
                    
                    start_url_for_attempt = resume_from_url or base_url
                    is_resuming = bool(resume_from_url)
                    
                    print(f"\n=================================================")
                    print(f"Starting Journey: {journey['description']} ({journey_id})")
                    print(f"Attempt #{retries + 1}. Starting from URL: {start_url_for_attempt}")
                    print(f"=================================================")
                    
                    driver.get(start_url_for_attempt)
                    
                    start_page = get_page_from_url(start_url_for_attempt) if is_resuming else 1
                    
                    journey_succeeded = True
                    
                    if is_resuming:
                        pagination_step = next((s for s in journey['steps'] if s['action'] == 'numeric_pagination_loop'), None)
                        if pagination_step:
                            if not process_pagination_loop(driver, pagination_step, db_engine, parent_url_id, navigation_path_parts, start_page, job_state, destination_tablename):
                                resume_from_url = driver.current_url
                                print(f"  - Recording resume URL for next attempt: {resume_from_url}")
                                journey_succeeded = False
                        else:
                            print("  - ERROR: In resume mode but could not find a pagination loop step in sitemap.")
                            journey_succeeded = False
                    else:
                        for step in journey['steps']:
                            if not process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, False, job_state, destination_tablename, 1):
                                resume_from_url = driver.current_url
                                print(f"  - Recording resume URL for next attempt: {resume_from_url}")
                                journey_succeeded = False
                                break
                    
                    if journey_succeeded:
                        print(f"\n✅ Journey '{journey_id}' completed successfully.")
                        break
                    else:
                        retries += 1
                        print(f"  - Incrementing retry count to {retries} for journey '{journey_id}'.")

                except Exception as e:
                    retries += 1
                    print(f"\n!!! An unexpected exception occurred during Journey '{journey_id}': {e}")
                    final_error_message = str(e)
                    if driver: resume_from_url = driver.current_url
                    print(f"  - Incrementing retry count to {retries}. Resume URL: {resume_from_url} !!!")
                
                finally:
                    if driver:
                        print(f"Closing WebDriver for attempt #{retries} of Journey '{journey_id}'.")
                        driver.quit()
                
                if retries < MAX_RETRIES:
                    print(f"  - Waiting 10 seconds before retrying journey '{journey_id}'...")
                    time.sleep(10)
            
            if retries >= MAX_RETRIES:
                print(f"\n❌ FATAL: Journey '{journey_id}' failed after {MAX_RETRIES} attempts. Setting job status to 'failed'.")
                final_status = 'failed'
                final_error_message = final_error_message or f"Journey '{journey_id}' failed after max retries."

    except Exception as e:
        print(f"  - An uncaught exception terminated the crawler run: {e}")
        final_status = 'failed'
        final_error_message = str(e)
    
    finally:
        message = f"Successfully processed {job_state['records_saved']} new records."
        if final_status == 'failed':
            message = f"Job failed. Processed {job_state['records_saved']} new records. Last error: {final_error_message}"
        update_audit_log_entry(db_engine, audit_log_id, final_status, message)
        print("\nAll journeys finished.")

if __name__ == "__main__":
    # Define inputs for the crawler function
    parent_url_id_for_testing = "d4886db7-3a22-4ec5-9943-c71bebe7878c"
    sitemap_input = os.path.join('config', 'sitemap_legislation_vic_gov_au.json')
    tablename_input = "l1_scan_legislation_vic_gov_au"
    
    print(f"--- Running in FULL test mode for parent_url_id: {parent_url_id_for_testing} ---")
    print(f"--- Using sitemap: '{sitemap_input}' and destination table: '{tablename_input}' ---")
    run_crawler(parent_url_id_for_testing, sitemap_input, tablename_input)
