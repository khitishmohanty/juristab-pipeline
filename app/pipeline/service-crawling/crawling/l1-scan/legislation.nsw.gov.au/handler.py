import json
import os

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
    AWS Lambda handler function. Expects 'parent_url_id' and 'sitemap_file_name'.
    """
    print("Lambda function invoked.")
    parent_url_id = event.get('parent_url_id')
    sitemap_file_name = event.get('sitemap_file_name')
    if not parent_url_id or not sitemap_file_name:
        print("FATAL ERROR: 'parent_url_id' and 'sitemap_file_name' are required.")
        return {'statusCode': 400, 'body': json.dumps('Error: parent_url_id and sitemap_file_name are required.')}

    print(f"Starting FULL crawler run for parent_url_id: {parent_url_id} using sitemap: {sitemap_file_name}")
    run_crawler(parent_url_id, sitemap_file_name)
    
    return {'statusCode': 200, 'body': json.dumps(f'Successfully completed crawling for {parent_url_id}')}



def run_crawler(parent_url_id, sitemap_file_name, destination_table):
    """Main function to initialize and run the crawler."""
    config_file_path = os.path.join('config', sitemap_file_name)
    config = load_config(config_file_path)
    if not config: return

    db_engine = create_db_engine()
    if not db_engine: return

    base_url = get_parent_url_details(db_engine, parent_url_id)
    if not base_url: return

    job_name = f"crawling-{parent_url_id}-{sitemap_file_name}"
    audit_log_id = create_audit_log_entry(db_engine, job_name)
    if not audit_log_id: return

    job_state = {'records_saved': 0}
    final_status = 'success'
    final_error_message = None
    
    try:
        for i, journey in enumerate(config['crawler_config']['journeys']):
            driver = None
            try:
                driver = initialize_driver()
                
                navigation_path_parts = ["Home"]
                if journey.get('description'):
                    navigation_path_parts.append(journey['description'])

                print(f"\n=================================================")
                print(f"Starting Journey: {journey['description']} ({journey['journey_id']})")
                print(f"=================================================")
                
                driver.get(base_url)
                
                journey_succeeded = True
                for step in journey['steps']:
                    if not process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table):
                        print(f"\n!!! Step failed in Journey '{journey['journey_id']}'. Halting this journey. !!!")
                        journey_succeeded = False
                        final_status = 'failed'
                        final_error_message = f"Journey '{journey['journey_id']}' failed."
                        break
                
                if journey_succeeded:
                    print(f"\n✅ Journey '{journey['journey_id']}' completed successfully.")

            except Exception as e:
                print(f"\n!!! An unexpected exception occurred during Journey '{journey['journey_id']}': {e}")
                final_status = 'failed'
                final_error_message = str(e)
            
            finally:
                if driver:
                    print(f"Closing WebDriver for Journey '{journey['journey_id']}'.")
                    driver.quit()

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
    # This block is for local testing. It simulates the Lambda event.
    parent_url_id_for_testing = "df8edd0e-0f69-484a-930b-67dc6072013b"
    sitemap_for_testing = "sitemap_legislation_nsw_gov_au.json"
    destination_table_for_testing = "l1_scan_legislation_nsw_gov_au"
    
    print(f"--- Running in FULL test mode for parent_url_id: {parent_url_id_for_testing} ---")
    run_crawler(parent_url_id_for_testing, sitemap_for_testing, destination_table_for_testing)

