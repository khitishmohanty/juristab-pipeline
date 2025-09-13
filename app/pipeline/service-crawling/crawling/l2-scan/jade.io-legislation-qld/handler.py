# ==============================================================================
# SCRIPT SETUP AND ENVIRONMENT VARIABLE LOADING
# This block MUST run first to ensure all other modules get clean variables.
# ==============================================================================
import os
from dotenv import load_dotenv

# 1. Load the .env file to populate the environment for local runs.
load_dotenv()

# 2. Clean all environment variables that the script will use.
def clean_env_var(var_name, default=""):
    var = os.getenv(var_name, default).strip('"')
    os.environ[var_name] = var
    return var

# Crawler-specific variables
parent_url_id = clean_env_var("PARENT_URL_ID")
sitemap_file_name = clean_env_var("SITEMAP_FILE_NAME")

# Database-specific variables
clean_env_var("DB_DIALECT")
clean_env_var("DB_DRIVER")
clean_env_var("DB_USER")
clean_env_var("DB_PASSWORD")
clean_env_var("DB_HOST")
clean_env_var("DB_NAME")
clean_env_var("DB_PORT", "3306")


# ==============================================================================
# REGULAR IMPORTS AND APPLICATION LOGIC
# These are now guaranteed to see the clean environment variables.
# ==============================================================================
import uuid
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException
import json
import time
import re  # <-- MODIFICATION: Imported 're' for regular expressions
from sqlalchemy import text
from datetime import datetime
from urllib.parse import urljoin
import boto3
from utils.aws_utils import create_db_engine

# --- Configuration ---
MAX_RETRIES = 3
S3_BUCKET = "legal-store"

# --- AWS Clients ---
s3_client = boto3.client('s3')

# --- WebDriver Initialization ---
def initialize_driver():
    if os.getenv('RUNNING_IN_DOCKER'):
        print("Initializing Chrome WebDriver for DOCKER environment...")
        
        # --- Paths guaranteed by the final Dockerfile ---
        service = ChromeService(executable_path="/usr/local/bin/chromedriver")
        options = webdriver.ChromeOptions()
        
        # --- FINAL FIX: Use the correct path for the installed Chrome binary ---
        options.binary_location = "/usr/bin/google-chrome-stable"

        # --- All necessary arguments for a stable headless run ---
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        try:
            print("  - Attempting to start WebDriver...")
            driver = webdriver.Chrome(service=service, options=options)
            print("  - WebDriver initialized successfully.")
            return driver
        except Exception as e:
            # This detailed logging is crucial for debugging startup failures.
            print(f"\n--- WebDriver Initialization Failed ---")
            print(f"  - Exception Type: {type(e).__name__}")
            print(f"  - Exception Args: {e.args}")
            print(f"  - Full Exception: {e}")
            print(f"-------------------------------------\n")
            raise

    else:
        # For running on your local machine
        print("Initializing Chrome WebDriver for LOCAL environment...")
        options = webdriver.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        return webdriver.Chrome(options=options)

# --- (Your other functions go here) ---
def create_audit_log_entry(engine, job_name):
    audit_id = str(uuid.uuid4())
    print(f"\nCreating audit log entry for job: {job_name} (ID: {audit_id})")
    try:
        with engine.connect() as connection:
            with connection.begin():
                query = text("INSERT INTO audit_log (id, job_name, start_time, job_status) VALUES (:id, :job_name, :start_time, 'running')")
                params = {"id": audit_id, "job_name": job_name, "start_time": datetime.now()}
                connection.execute(query, params)
        return audit_id
    except Exception as e:
        print(f"  - FATAL ERROR: Could not create audit log entry: {e}")
        return None

def update_audit_log_entry(engine, audit_id, final_status, message):
    if not audit_id: return
    print(f"\nUpdating audit log entry {audit_id} with status: {final_status}")
    try:
        with engine.connect() as connection:
            with connection.begin():
                start_time_query = text("SELECT start_time FROM audit_log WHERE id = :id")
                start_time_result = connection.execute(start_time_query, {"id": audit_id}).fetchone()
                end_time = datetime.now()
                duration = (end_time - start_time_result[0]).total_seconds() if start_time_result else -1.0
                query = text("UPDATE audit_log SET end_time = :end_time, job_status = :status, job_duration = :duration, message = :message WHERE id = :id")
                params = {"id": audit_id, "end_time": end_time, "status": final_status, "duration": duration, "message": message}
                connection.execute(query, params)
    except Exception as e:
        print(f"  - FATAL ERROR: Could not update audit log entry {audit_id}: {e}")

def load_config(path):
    print(f"Loading configuration from: {path}")
    try:
        with open(path, 'r') as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not load or parse config file '{path}': {e}")
        return None

def get_parent_url_details(engine, parent_url_id):
    print(f"\nFetching base_url for parent_url_id: {parent_url_id}...")
    try:
        with engine.connect() as connection:
            query = text("SELECT base_url FROM parent_urls WHERE id = :id")
            result = connection.execute(query, {"id": parent_url_id}).fetchone()
            if result: return result[0]
            print(f"  - FATAL ERROR: No record found for id='{parent_url_id}'")
    except Exception as e:
        print(f"  - FATAL ERROR: Could not query database: {e}")
    return None

def save_content_to_s3(content, bucket, key):
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType='text/html')
        print(f"    - Successfully saved content to S3: s3://{bucket}/{key}")
    except Exception as e:
        print(f"    - ERROR: Failed to save to S3 bucket '{bucket}': {e}")
        raise

def save_record_and_get_id(engine, data, parent_url_id, navigation_path, table):
    book_name_to_check = data.get('book_name')
    book_context_to_check = data.get('book_context')
    if not book_name_to_check:
        print("    - WARNING: Book name is missing, cannot check for duplicates or insert.")
        return None
    try:
        with engine.connect() as connection:
            with connection.begin():
                find_query = text(f"SELECT id FROM {table} WHERE book_name = :book_name AND book_context = :book_context")
                params_find = {"book_name": book_name_to_check, "book_context": book_context_to_check}
                existing_record = connection.execute(find_query, params_find).fetchone()
                if existing_record:
                    print(f"  - Record for '{book_name_to_check}' already exists. Skipping.")
                    return None
                else:
                    record_id = str(uuid.uuid4())
                    print(f"  - Inserting new record for '{book_name_to_check}' into table '{table}'")
                    insert_query = text(f"""
                        INSERT INTO {table} (id, parent_url_id, book_name, book_context, book_url, navigation_path, date_collected, is_active)
                        VALUES (:id, :parent_url_id, :book_name, :book_context, :book_url, :navigation_path, :date_collected, 1)
                    """)
                    params_insert = {
                        "id": record_id, "parent_url_id": parent_url_id, "book_name": book_name_to_check,
                        "book_context": book_context_to_check, "book_url": data.get('book_url'),
                        "navigation_path": navigation_path, "date_collected": datetime.now()
                    }
                    connection.execute(insert_query, params_insert)
                    print(f"    - DB insert successful. New record ID: {record_id}")
                    return record_id
    except Exception as e:
        print(f"    - FATAL ERROR during database check or insert: {e}")
        raise

def scrape_page_details_and_save(driver, config, db_engine, parent_url_id, nav_path_parts, job_state):
    wait = WebDriverWait(driver, 20)
    try:
        rows = wait.until(EC.presence_of_all_elements_located((By.XPATH, config['row_xpath'])))
        print(f"  - Found {len(rows)} result rows to process.")
    except TimeoutException:
        print("  - No result rows found on this page.")
        return True
    records_processed_this_page = 0
    for i, row in enumerate(rows):
        try:
            row_data = {}
            for col in config['columns']:
                try:
                    element = row.find_element(By.XPATH, col['xpath'])
                    row_data[col['name']] = element.get_attribute('href') if col['type'] == 'href' else element.text
                except NoSuchElementException:
                    row_data[col['name']] = None
            human_readable_path = "/".join(nav_path_parts)
            record_id = save_record_and_get_id(db_engine, row_data, parent_url_id, human_readable_path, config['destination_table'])
            if not record_id: continue
            jurisdiction_folder = config.get("jurisdiction_folder_name", "unknown_jurisdiction")
            #jurisdiction_folder = nav_path_parts[2].lower().replace(" ", "_")
            base_s3_path = f"legislation/{jurisdiction_folder}/{record_id}"
            for tab in config['content_tabs']['tabs']:
                try:
                    tab_button = row.find_element(By.XPATH, tab['click_xpath'])
                    driver.execute_script("arguments[0].click();", tab_button)
                    time.sleep(3)
                    content_container = row.find_element(By.XPATH, tab['content_xpath'])
                    content_html = content_container.get_attribute('outerHTML')
                    
                    # <-- START MODIFICATION -->
                    # Check if the current tab is 'Miniviewer' and modify its HTML if so.
                    if tab['name'] == 'Miniviewer':
                        print("    - Modifying Miniviewer HTML to expand height...")
                        # This regex finds the specific style attribute causing the fixed height and overflow, then removes it.
                        pattern = r'style="height:[^;]+;overflow:auto[^"]*"'
                        content_html = re.sub(pattern, '', content_html, count=1)
                    # <-- END MODIFICATION -->

                    s3_key = f"{base_s3_path}/{tab['name'].lower()}.html"
                    save_content_to_s3(content_html, config['s3_bucket'], s3_key)
                except NoSuchElementException:
                    print(f"    - WARNING: Could not find tab button or content for '{tab['name']}' in row {i+1}. Skipping.")
                except Exception as e:
                    print(f"    - ERROR: An unexpected error occurred while processing tab '{tab['name']}': {e}")
            records_processed_this_page += 1
            job_state['records_saved'] += 1
        except StaleElementReferenceException:
            print(f"  - ERROR: Stale element reference on row {i+1}. Re-finding rows and retrying this page.")
            return "retry_page"
        except Exception as e:
            print(f"  - FATAL ERROR processing row {i+1}: {e}")
            raise
    print(f"  - Finished processing {records_processed_this_page} records for this page.")
    return True

def process_and_paginate(driver, step_config, db_engine, parent_url_id, nav_path_parts, job_state):
    scraping_config = step_config['scraping_config']
    page_num = 1
    while True:
        print(f"\n--- Processing Page {page_num} ---")
        page_nav_path = nav_path_parts + [f"Page-{page_num}"]
        
        result = scrape_page_details_and_save(driver, scraping_config, db_engine, parent_url_id, page_nav_path, job_state)
        
        if result == "retry_page":
            time.sleep(2)
            continue

        try:
            last_url = driver.current_url

            next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, step_config['next_page_xpath'])))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", next_button)
            
            time.sleep(2) 

            if driver.current_url == last_url:
                print("  - URL did not change after clicking 'Next'. Reached the end of pagination.")
                break

            page_num += 1

        except (TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException):
            # **FIX**: This now catches the most common click-related errors and ends pagination gracefully.
            print("  - 'Next' button not found or interaction failed. Reached the end of pagination.")
            break
            
    return True

def process_navigation_loop(driver, step, db_engine, parent_url_id, nav_path_parts, job_state, journey_state):
    target_xpath = step.get('target_xpath')
    wait = WebDriverWait(driver, 30)
    nav_links = driver.find_elements(By.XPATH, target_xpath)
    num_links = len(nav_links)
    if num_links == 0:
        return False
    start_index = journey_state.get('last_completed_index', -1) + 1
    for i in range(start_index, num_links):
        try:
            current_nav_links = driver.find_elements(By.XPATH, target_xpath)
            link_to_click = current_nav_links[i]
            link_text = link_to_click.text.strip()
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_to_click)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", link_to_click)
            wait.until(EC.presence_of_element_located((By.XPATH, "//p[@class='breadcrumb' and contains(text(), 'Search results')]")))
            time.sleep(2)
            item_path_parts = nav_path_parts + [link_text]
            for loop_step in step['loop_steps']:
                if not process_step(driver, loop_step, db_engine, parent_url_id, item_path_parts, job_state, journey_state):
                    raise Exception(f"Step failed for navigation item '{link_text}'")
            driver.back()
            wait.until(EC.presence_of_element_located((By.XPATH, "//h3[normalize-space()='Legislation']")))
            time.sleep(1)
            journey_state['last_completed_index'] = i
        except Exception as e:
            raise e
    return True

def process_step(driver, step, db_engine, parent_url_id, nav_path_parts, job_state, journey_state):
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', 'No description')}")
    if action == 'click':
        wait = WebDriverWait(driver, 10)
        try:
            element = wait.until(EC.element_to_be_clickable((By.XPATH, step['target']['value'])))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", element)
            time.sleep(1)
        except TimeoutException:
            print(f"  - INFO: Element for '{step['description']}' not found or not clickable. Might be optional. Continuing.")
        return True
    elif action == 'pause':
        time.sleep(step.get('duration', 1))
        return True
    elif action == 'navigation_loop':
        return process_navigation_loop(driver, step, db_engine, parent_url_id, nav_path_parts, job_state, journey_state)
    elif action == 'process_and_paginate':
        return process_and_paginate(driver, step, db_engine, parent_url_id, nav_path_parts, job_state)
    else:
        return True

def run_crawler(parent_url_id, sitemap_file_name):
    config_file_path = os.path.join('config', sitemap_file_name)
    config = load_config(config_file_path)
    if not config: return
    db_engine = create_db_engine()
    if not db_engine: return
    base_url = get_parent_url_details(db_engine, parent_url_id)
    if not base_url: return
    job_name = f"crawling-jade-{parent_url_id}"
    audit_log_id = create_audit_log_entry(db_engine, job_name)
    if not audit_log_id: return
    job_state = {'records_saved': 0}
    final_status, final_error_message = 'success', ""
    for i, journey in enumerate(config['crawler_config']['journeys']):
        retries = 0
        journey_succeeded = False
        journey_state = {'last_completed_index': -1}
        while retries <= MAX_RETRIES and not journey_succeeded:
            driver = None
            try:
                if retries > 0: print(f"\n--- Retrying Journey '{journey['description']}' (Attempt {retries + 1}) ---")
                driver = initialize_driver()
                driver.get(base_url)
                nav_path_parts = ["Home", journey.get('description', f'Journey-{i+1}')]
                for step in journey['steps']:
                    if not process_step(driver, step, db_engine, parent_url_id, nav_path_parts, job_state, journey_state):
                        raise Exception(f"Step failed in Journey '{journey['journey_id']}'")
                journey_succeeded = True
            except Exception as e:
                retries += 1
                print(f"\n!!! EXCEPTION during Journey '{journey['description']}'")
                print(f"    - Exception Type: {type(e).__name__}")
                print(f"    - Exception Args: {e.args}")
                print(f"    - Full Exception: {e}")
                if retries > MAX_RETRIES:
                    final_status = 'failed'
                    final_error_message += f"Journey '{journey['description']}' failed after {MAX_RETRIES} retries. Last error: {e}\n"
                else:
                    time.sleep(5)
            finally:
                if driver: driver.quit()
    message = f"Successfully processed {job_state['records_saved']} new records."
    if final_status == 'failed':
        message = f"Job failed. Processed {job_state['records_saved']} records. Errors: {final_error_message}"
    update_audit_log_entry(db_engine, audit_log_id, final_status, message)
    print("\nAll journeys finished.")


if __name__ == "__main__":
    parent_url_id = os.getenv("PARENT_URL_ID")
    sitemap_file_name = os.getenv("SITEMAP_FILE_NAME")
    
    required_vars = ["PARENT_URL_ID", "SITEMAP_FILE_NAME", "DB_DIALECT", "DB_DRIVER", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME", "DB_PORT"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"FATAL ERROR: The following required environment variables are missing or empty: {', '.join(missing_vars)}")
        exit(1)

    print(f"--- Running crawler from Docker container ---")
    print(f"--- Target Parent URL ID: {parent_url_id} ---")
    print(f"--- Target Sitemap: {sitemap_file_name} ---")
    
    run_crawler(parent_url_id, sitemap_file_name)