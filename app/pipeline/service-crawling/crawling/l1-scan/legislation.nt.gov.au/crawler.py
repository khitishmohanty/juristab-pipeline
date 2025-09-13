import json
import os
import time
import re
from sqlalchemy import text
from datetime import datetime
import uuid
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from urllib.parse import urljoin, urlparse, parse_qs

# Import the database engine creator from your utils file
# Ensure you have a utils/aws_utils.py file with a create_db_engine function
from utils.aws_utils import create_db_engine


# --- Configuration ---
MAX_RETRIES = 3 # Maximum number of times to retry a failed journey
NAVIGATION_PATH_DEPTH = int(os.getenv("NAVIGATION_PATH_DEPTH", 3)) # Duplicate checking

def create_audit_log_entry(engine, job_name):
    """Creates a new entry in the audit_log table and returns its ID."""
    audit_id = str(uuid.uuid4())
    print(f"\nCreating audit log entry for job: {job_name} (ID: {audit_id})")
    try:
        with engine.connect() as connection:
            with connection.begin():
                query = text("""
                    INSERT INTO audit_log (id, job_name, start_time, job_status)
                    VALUES (:id, :job_name, :start_time, 'running')
                """)
                params = {"id": audit_id, "job_name": job_name, "start_time": datetime.now()}
                connection.execute(query, params)
        return audit_id
    except Exception as e:
        print(f"  - FATAL ERROR: Could not create audit log entry: {e}")
        return None

def update_audit_log_entry(engine, audit_id, final_status, message):
    """Updates the audit_log entry with the final status and duration."""
    if not audit_id:
        print("  - WARNING: No audit_id provided, cannot update audit log.")
        return

    print(f"\nUpdating audit log entry {audit_id} with status: {final_status}")
    try:
        with engine.connect() as connection:
            with connection.begin():
                start_time_query = text("SELECT start_time FROM audit_log WHERE id = :id")
                start_time_result = connection.execute(start_time_query, {"id": audit_id}).fetchone()
                
                end_time = datetime.now()
                duration = (end_time - start_time_result[0]).total_seconds() if start_time_result else -1.0

                query = text("""
                    UPDATE audit_log 
                    SET end_time = :end_time, job_status = :status, job_duration = :duration, message = :message
                    WHERE id = :id
                """)
                params = {"id": audit_id, "end_time": end_time, "status": final_status, "duration": duration, "message": message}
                connection.execute(query, params)
        print("  - Audit log entry updated successfully.")
    except Exception as e:
        print(f"  - FATAL ERROR: Could not update audit log entry {audit_id}: {e}")
        
def load_config(path):
    """Loads the crawler configuration from a JSON file."""
    print(f"Loading configuration from: {path}")
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at '{path}'. Please ensure it exists.")
        return None
    except json.JSONDecodeError:
        print(f"ERROR: The configuration file at '{path}' is not a valid JSON.")
        return None
    
def get_parent_url_details(engine, parent_url_id):
    """Connects to the database and fetches the base_url for the given ID."""
    print(f"\nFetching base_url for parent_url_id: {parent_url_id}...")
    try:
        with engine.connect() as connection:
            query = text("SELECT base_url FROM parent_urls WHERE id = :id")
            result = connection.execute(query, {"id": parent_url_id}).fetchone()
            if result: return result[0]
            print(f"  - FATAL ERROR: No record found for id='{parent_url_id}'")
            return None
    except Exception as e:
        print(f"  - FATAL ERROR: Could not query database: {e}")
        return None

def save_scraped_data_to_db(engine, scraped_data, parent_url_id, navigation_path_parts, page_num, destination_table):
    """Saves a list of scraped book links to the specified destination table."""
    if not scraped_data: return 0
    
    if not re.match(r"^[a-zA-Z0-9_]+$", destination_table):
        print(f"  - FATAL ERROR: Invalid destination_table name: '{destination_table}'. Aborting save.")
        return 0

    human_readable_path = "/".join(navigation_path_parts) + f"/Page/{page_num}"
    try:
        with engine.connect() as connection:
            with connection.begin():
                path_prefix_parts = navigation_path_parts[:NAVIGATION_PATH_DEPTH]
                path_prefix = "/".join(path_prefix_parts) + "%"
                
                existing_urls_query_str = f"SELECT book_url FROM {destination_table} WHERE parent_url_id = :parent_url_id AND navigation_path LIKE :path_prefix"
                existing_urls_query = text(existing_urls_query_str)
                
                existing_urls_result = connection.execute(existing_urls_query, {"parent_url_id": parent_url_id, "path_prefix": path_prefix}).fetchall()
                existing_urls = {row[0] for row in existing_urls_result}
                records_to_insert = [item for item in scraped_data if item.get('link') not in existing_urls]
                
                if not records_to_insert:
                    print(f"  - All {len(scraped_data)} scraped records for this page already exist. Nothing to insert.")
                    return 0
                print(f"  - Found {len(records_to_insert)} new records to insert.")

                insert_query_str = f"""
                    INSERT INTO {destination_table} (id, parent_url_id, book_name, book_url, navigation_path, date_collected, is_active)
                    VALUES (:id, :parent_url_id, :book_name, :book_url, :navigation_path, :date_collected, :is_active)
                """
                query = text(insert_query_str)

                for item in records_to_insert:
                    params = {
                        "id": str(uuid.uuid4()), "parent_url_id": parent_url_id,
                        "book_name": item.get('title'),
                        "book_url": item.get('link'), "navigation_path": human_readable_path,
                        "date_collected": datetime.now(), "is_active": 1,
                    }
                    connection.execute(query, params)
                print(f"  - Successfully saved {len(records_to_insert)} new records to '{destination_table}'.")
                return len(records_to_insert)
    except Exception as e:
        print(f"  - FATAL ERROR: Failed during database save operation: {e}")
        # We re-raise to ensure the journey retry logic catches this
        raise e
    
def initialize_driver():
    """Initializes a more stable, production-ready Selenium WebDriver."""
    print("Initializing Chrome WebDriver with stability options...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument('--ignore-certificate-errors')
    return webdriver.Chrome(options=options)

def process_alphabet_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, journey_state=None):
    """
    Handles alphabet-based navigation, with logic to resume from a specific letter if a crash occurred.
    """
    target_xpath = step.get('target_xpath')
    if not target_xpath:
        print("  - ERROR: 'target_xpath' not defined for alphabet_loop.")
        return False
        
    print(f"  - Waiting for alphabet links to be present ({target_xpath})...")
    try:
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, target_xpath)))
        alphabet_links = driver.find_elements(By.XPATH, target_xpath)
        num_links = len(alphabet_links)
        print(f"  - Found {num_links} alphabet links to process.")
        if num_links == 0:
            print("  - WARNING: No alphabet links found, skipping loop.")
            return True
    except (TimeoutException, NoSuchElementException) as e:
        print(f"  - FATAL ERROR: Could not find initial alphabet links: {e}")
        return False

    # NEW: Logic to resume from where it left off
    start_index = 0
    if journey_state and journey_state.get('last_completed_index', -1) >= 0:
        start_index = journey_state['last_completed_index'] + 1
        print(f"  - Resuming alphabet loop from index {start_index}.")

    for i in range(start_index, num_links):
        try:
            current_alphabet_links = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.XPATH, target_xpath))
            )
            
            if i >= len(current_alphabet_links):
                print(f"  - ERROR: Index {i} out of bounds after re-finding links.")
                break

            link_to_click = current_alphabet_links[i]
            letter_text = link_to_click.text.strip()
            print(f"\n--- Processing alphabet link {i+1}/{num_links} (Letter: '{letter_text}') ---")
            
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_to_click)
            time.sleep(0.5)
            link_to_click.click()
            time.sleep(1)

            letter_path_parts = navigation_path_parts + [f"Letter-{letter_text}"]
            
            for loop_step in step['loop_steps']:
                if not process_step(driver, loop_step, db_engine, parent_url_id, letter_path_parts, job_state, destination_table, journey_state=journey_state):
                    raise Exception(f"Step failed for letter '{letter_text}'")
            
            # NEW: Update state after successfully processing a letter
            if journey_state is not None:
                journey_state['last_completed_index'] = i
                print(f"  - Successfully completed letter index {i}. State updated.")

        except Exception as e:
            # Re-raise the exception to be caught by the journey-level retry loop
            print(f"  - An error occurred processing letter index {i}. Last successful index was {journey_state.get('last_completed_index', -1)}.")
            raise e
            
    return True

def process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, current_page=1, journey_state=None):
    """Main dispatcher function. Passes journey_state down to relevant actions."""
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', 'No description')}")

    if action == 'click':
        perform_click(driver, step.get('target'))
        time.sleep(2) 
        return True
    
    elif action == 'alphabet_loop':
        return process_alphabet_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, journey_state=journey_state)
    
    elif action == 'process_results':
        scraping_config = step.get('scraping_config')
        if not scraping_config:
            print("  - FATAL ERROR: 'process_results' action requires a 'scraping_config' object.")
            return False
        return scrape_configured_data(driver, step.get('target', {}).get('value'), scraping_config, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_table)
    else:
        print(f"  - WARNING: Unknown action type '{action}'. Skipping.")
    return True

def scrape_configured_data(driver, container_xpath, scraping_config, db_engine, parent_url_id, navigation_path_parts, page_num, job_state, destination_table):
    """
    Generic scraping function that waits for data to load and saves it to the database.
    """
    try:
        wait = WebDriverWait(driver, 20)
        
        print(f"  - Waiting for container to be present ({container_xpath})...")
        wait.until(EC.presence_of_element_located((By.XPATH, container_xpath)))

        row_xpath = scraping_config['row_xpath']
        
        wait.until(EC.presence_of_element_located((By.XPATH, row_xpath)))
        
        rows = driver.find_elements(By.XPATH, row_xpath)
        print(f"  - Found {len(rows)} result rows to scrape.")
        if not rows: 
            return True

        scraped_data = []
        for row in rows:
            row_data = {}
            for column_config in scraping_config['columns']:
                col_name, col_xpath, col_type = column_config['name'], column_config['xpath'], column_config.get('type', 'text')
                try:
                    element = row.find_element(By.XPATH, col_xpath) if col_xpath != '.' else row
                    if col_type == 'text':
                        row_data[col_name] = element.text
                    elif col_type == 'href':
                        row_data[col_name] = urljoin(driver.current_url, element.get_attribute('href'))
                except NoSuchElementException:
                    row_data[col_name] = None
            scraped_data.append(row_data)
        
        if scraped_data:
            new_records = save_scraped_data_to_db(db_engine, scraped_data, parent_url_id, navigation_path_parts, page_num, destination_table)
            job_state['records_saved'] += new_records
        return True
    except TimeoutException:
        print(f"  - INFO: No data rows found in container. This is normal for letters with no legislation.")
        return True
    
def perform_click(driver, target):
    """Waits for an element to be clickable and clicks it."""
    wait = WebDriverWait(driver, 20)
    element_locator = (By.XPATH, target['value'])
    element = wait.until(EC.presence_of_element_located(element_locator))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(0.5) 
    element_to_click = wait.until(EC.element_to_be_clickable(element_locator))
    element_text = element_to_click.text.strip()
    print(f"  - Clicking element with XPath: {target['value']} (Text: '{element_text}')")
    element_to_click.click()

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
