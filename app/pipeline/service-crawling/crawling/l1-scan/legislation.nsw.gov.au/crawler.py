import json
import os
import time
import re
from sqlalchemy import text
from datetime import datetime
import uuid
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from urllib.parse import urljoin

# Import the database engine creator from your utils file
from utils.aws_utils import create_db_engine


# --- Configuration ---
# The sitemap path is now passed into the run_crawler function.
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
                    INSERT INTO {destination_table} (id, parent_url_id, book_name, book_number, book_url, navigation_path, date_collected, is_active, book_effective_date, book_year)
                    VALUES (:id, :parent_url_id, :book_name, :book_number, :book_url, :navigation_path, :date_collected, :is_active, :book_effective_date, :book_year)
                """
                query = text(insert_query_str)

                for item in records_to_insert:
                    book_year_val, book_effective_date_val = None, None
                    try:
                        if item.get('year'): book_year_val = int(item.get('year'))
                    except (ValueError, TypeError): pass
                    try:
                        if item.get('effective_date'): book_effective_date_val = datetime.strptime(item.get('effective_date'), '%d/%m/%Y').date()
                    except (ValueError, TypeError): pass
                    
                    params = {
                        "id": str(uuid.uuid4()), "parent_url_id": parent_url_id,
                        "book_name": item.get('title'), "book_number": item.get('number'),
                        "book_url": item.get('link'), "navigation_path": human_readable_path,
                        "date_collected": datetime.now(), "is_active": 1,
                        "book_effective_date": book_effective_date_val,
                        "book_year": book_year_val
                    }
                    connection.execute(query, params)
                print(f"  - Successfully saved {len(records_to_insert)} new records to '{destination_table}'.")
                return len(records_to_insert)
    except Exception as e:
        print(f"  - FATAL ERROR: Failed during database save operation: {e}")
        return 0
    
def initialize_driver():
    """Initializes a more stable, production-ready Selenium WebDriver."""
    print("Initializing Chrome WebDriver with stability options...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    return webdriver.Chrome(options=options)

def process_next_button_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table):
    """Dedicated function for simple 'Next' button pagination."""
    page_counter = 1
    while True:
        print(f"\n--- Scraping results on page {page_counter} ---")
        for loop_step in step['loop_steps']:
            if not process_step(driver, loop_step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, current_page=page_counter):
                return False
        
        next_button_xpath = step.get('next_button_xpath')
        if not next_button_xpath:
            print("  - ERROR: 'next_button_xpath' not defined for this loop.")
            return False
        
        click_result = perform_click(driver, {'type': 'xpath', 'value': next_button_xpath}, is_pagination=True)
        if click_result == "browser_crash": return False
        if click_result is None:
            print("  - 'Next' button not found or disabled. Pagination complete for this section.")
            break
        
        page_counter += 1
        time.sleep(2)
    return True


def process_alphabet_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table):
    """Dedicated function for alphabet-based navigation."""
    target_xpath = step.get('target_xpath')
    if not target_xpath:
        print("  - ERROR: 'target_xpath' not defined for alphabet_loop.")
        return False
        
    print(f"  - Finding all alphabet links with XPath: {target_xpath}")
    try:
        alphabet_links = driver.find_elements(By.XPATH, target_xpath)
        alphabet_urls = [a.get_attribute('href') for a in alphabet_links]
        print(f"  - Found {len(alphabet_urls)} alphabet links to process.")
    except Exception as e:
        print(f"  - ERROR: Could not find alphabet links: {e}")
        return False

    for i, url in enumerate(alphabet_urls):
        print(f"\n--- Processing alphabet link {i+1}/{len(alphabet_urls)}: {url} ---")
        try:
            driver.get(url)
            letter_text = url.split('=')[-1] if '=' in url else f"Letter-{i+1}"
            letter_path_parts = navigation_path_parts + [letter_text]
            for loop_step in step['loop_steps']:
                if not process_step(driver, loop_step, db_engine, parent_url_id, letter_path_parts, job_state, destination_table):
                    print(f"  - Step failed within alphabet loop for URL {url}. Skipping to next letter.")
                    break 
        except WebDriverException as e:
            print(f"  - BROWSER CRASH during alphabet loop for URL {url}: {e}")
            return False
            
    return True

# --- Corrected process_step function ---
def process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, current_page=1):
    """Main dispatcher function. Processes a single step from the configuration."""
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', action)}")

    if action == 'click':
        clicked_text = perform_click(driver, step.get('target'))
        if clicked_text == "browser_crash": return False
        return True
    
    elif action == 'alphabet_loop':
        return process_alphabet_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table)
    
    elif action == 'next_button_pagination_loop':
        return process_next_button_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table)

    elif action == 'process_results':
        scraping_config = step.get('scraping_config')
        if not scraping_config:
            print("  - FATAL ERROR: 'process_results' action requires a 'scraping_config' object.")
            return False
        # BUG FIX: The function call signature is now correct.
        return scrape_configured_data(driver, step.get('target', {}).get('value'), scraping_config, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_table)
    else:
        print(f"  - WARNING: Unknown action type '{action}'. Skipping.")
    return True

def scrape_configured_data(driver, container_xpath, scraping_config, db_engine, parent_url_id, navigation_path_parts, page_num, job_state, destination_table):
    """Generic scraping function that saves results directly to the database."""
    try:
        wait = WebDriverWait(driver, 30)
        row_xpath = scraping_config['row_xpath']

        # If a container is specified, wait for it. Otherwise, use the whole body.
        if container_xpath:
            print(f"  - Waiting for table container to be present ({container_xpath})...")
            container_element = wait.until(EC.presence_of_element_located((By.XPATH, container_xpath)))
            print("  - Table container found.")
        else:
            # If no container is specified, the row_xpath should be absolute.
            print("  - No container specified, searching for rows in the whole document.")
            container_element = driver.find_element(By.XPATH, "//body") # The context is the whole page
        
        time.sleep(1)
        
        rows = container_element.find_elements(By.XPATH, row_xpath)
        print(f"  - Found {len(rows)} result rows to scrape using XPath: {row_xpath}")
        if not rows: return True

        scraped_data = []
        for row in rows:
            row_data = {}
            for column_config in scraping_config['columns']:
                col_name, col_xpath, col_type = column_config['name'], column_config['xpath'], column_config.get('type', 'text')
                try:
                    element = row.find_element(By.XPATH, col_xpath)
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
        print(f"  - INFO: Timed out waiting for table rows. Assuming page is empty and continuing.")
        return True
    except WebDriverException as e:
        if "invalid session id" in str(e) or "browser has closed" in str(e):
            print(f"  - FATAL BROWSER CRASH during scraping: {e}")
            return False
        raise
    except Exception as e:
        print(f"  - An unexpected error occurred during scraping: {e}")
        return False
    
    
def perform_click(driver, target, is_pagination=False):
    """Waits for an element to be clickable, clicks it, and returns the element's text."""
    try:
        wait = WebDriverWait(driver, 10 if is_pagination else 20)
        element_locator = (By.XPATH, target['value'])
        element = wait.until(EC.presence_of_element_located(element_locator))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.5) 
        element_to_click = wait.until(EC.element_to_be_clickable(element_locator))
        element_text = element_to_click.text.strip()
        print(f"  - Clicking element with XPath: {target['value']} (Text: '{element_text}')")
        element_to_click.click()
        return element_text
    except (TimeoutException, NoSuchElementException):
        if is_pagination: return None
        print(f"  - ERROR: Click target not found or not clickable: {target['value']}")
        return None
    except WebDriverException as e:
        if "invalid session id" in str(e) or "browser has closed" in str(e):
            print(f"  - FATAL BROWSER CRASH during click: {e}")
            return "browser_crash"
        raise
    
def get_page_from_url(url):
    """Parses a URL to extract the 'page' query parameter."""
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        page = query_params.get('page', [1])[0]
        return int(page)
    except (ValueError, IndexError):
        return 1
        
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