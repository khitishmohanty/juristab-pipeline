import json
import os
import time
from sqlalchemy import text
from datetime import datetime
import uuid
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


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
    """Updates the audit_log entry with the final status, duration, and message."""
    if not audit_id:
        print("  - WARNING: No audit_id provided, cannot update audit log.")
        return

    print(f"\nUpdating audit log entry {audit_id} with status: {final_status}")
    try:
        with engine.connect() as connection:
            with connection.begin():
                # First, get the start_time to calculate duration
                start_time_query = text("SELECT start_time FROM audit_log WHERE id = :id")
                start_time_result = connection.execute(start_time_query, {"id": audit_id}).fetchone()
                
                end_time = datetime.now()
                duration = (end_time - start_time_result[0]).total_seconds() if start_time_result else -1.0

                query = text("""
                    UPDATE audit_log 
                    SET end_time = :end_time, job_status = :status, job_duration = :duration, message = :message
                    WHERE id = :id
                """)
                params = {
                    "id": audit_id,
                    "end_time": end_time,
                    "status": final_status,
                    "duration": duration,
                    "message": message
                }
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

def save_book_links_to_db(engine, scraped_data, parent_url_id, navigation_path_parts, page_num, job_state, destination_tablename):
    """
    Saves a list of scraped book links to the database, checking for duplicates.
    Returns the number of new records inserted.
    """
    if not scraped_data:
        print("  - No data to save for this page.")
        return 0

    human_readable_path = "/".join(navigation_path_parts) + f"/Page/{page_num}"
    
    try:
        with engine.connect() as connection:
            with connection.begin():
                path_prefix_parts = navigation_path_parts[:NAVIGATION_PATH_DEPTH]
                path_prefix = "/".join(path_prefix_parts) + "%"
                print(f"  - Checking for existing records in table '{destination_tablename}' with path prefix: '{path_prefix}'")

                existing_urls_query = text(f"SELECT book_url FROM {destination_tablename} WHERE parent_url_id = :parent_url_id AND navigation_path LIKE :path_prefix")
                existing_urls_result = connection.execute(existing_urls_query, {"parent_url_id": parent_url_id, "path_prefix": path_prefix}).fetchall()
                existing_urls = {row[0] for row in existing_urls_result}
                print(f"  - Found {len(existing_urls)} existing records for this path context.")

                records_to_insert = [item for item in scraped_data if item.get('link') not in existing_urls]

                if not records_to_insert:
                    print("  - All scraped records for this page already exist. Nothing to insert.")
                    return 0

                print(f"  - Found {len(records_to_insert)} new records to insert into '{destination_tablename}'.")
                
                for item in records_to_insert:
                    query = text(f"""
                        INSERT INTO {destination_tablename} (id, parent_url_id, book_name, book_number, book_url, navigation_path, date_collected, is_active)
                        VALUES (:id, :parent_url_id, :book_name, :book_number, :book_url, :navigation_path, :date_collected, :is_active)
                    """)
                    params = {
                        "id": str(uuid.uuid4()), "parent_url_id": parent_url_id,
                        "book_name": item.get('title'), "book_number": item.get('number'),
                        "book_url": item.get('link'), "navigation_path": human_readable_path,
                        "date_collected": datetime.now(), "is_active": 1
                    }
                    connection.execute(query, params)
                
                newly_inserted_count = len(records_to_insert)
                job_state['records_saved'] += newly_inserted_count
                print(f"  - Successfully saved {newly_inserted_count} new records with path: {human_readable_path}")
                return newly_inserted_count

    except Exception as e:
        print(f"  - FATAL ERROR: Failed during database save operation: {e}")
        return 0 # Return 0 on error
    
def initialize_driver():
    """Initializes and returns a more stable, production-ready Selenium WebDriver."""
    print("Initializing Chrome WebDriver with stability options...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-application-cache")
    options.add_argument("--disk-cache-size=0")
    options.add_argument("--media-cache-size=0")
    return webdriver.Chrome(options=options)

def process_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, start_page, job_state, destination_tablename):
    """Dedicated function to handle the pagination loop, including fast-forwarding."""
    page_counter = start_page
    
    if page_counter > 1:
        print(f"\n--- Fast-forwarding to resume from page {page_counter} ---")
        for page_num_to_click in range(2, page_counter):
            print(f"  - Clicking to page {page_num_to_click}...")
            page_locator = {"type": "xpath", "value": step['page_number_xpath_template'].format(page_num=page_num_to_click)}
            click_result = perform_click(driver, page_locator, is_pagination=True)
            if click_result == "browser_crash": return False
            if click_result is None:
                fallback_locator = {"type": "xpath", "value": step['next_button_fallback_xpath']}
                if perform_click(driver, fallback_locator, is_pagination=True) == "browser_crash": return False
            time.sleep(1)

    while True:
        print(f"\n--- Scraping results on page {page_counter} ---")
        for loop_step in step['loop_steps']:
            if not scrape_configured_data(driver, loop_step['target']['value'], loop_step['scraping_config'], db_engine, parent_url_id, navigation_path_parts, page_counter, job_state, destination_tablename):
                print(f"--- Stopping pagination loop due to a scraping error on page {page_counter} ---")
                return False

        next_page_to_click = page_counter + 1
        page_locator = {"type": "xpath", "value": step['page_number_xpath_template'].format(page_num=next_page_to_click)}
        clicked_text = perform_click(driver, page_locator, is_pagination=True)
        
        if clicked_text == "browser_crash": return False
        if clicked_text is not None:
            page_counter += 1
            continue
        
        fallback_locator = {"type": "xpath", "value": step['next_button_fallback_xpath']}
        clicked_text = perform_click(driver, fallback_locator, is_pagination=True)
        
        if clicked_text == "browser_crash": return False
        if clicked_text is not None:
            page_counter += 1
            continue
        
        print("  - Could not find next page number or 'Next' button. Pagination complete.")
        break
    print("--- Numeric pagination loop finished ---")
    return True


def process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, is_resuming, job_state, destination_tablename, current_page=1):
    """
    Main dispatcher function. Processes a single step from the configuration.
    """
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', action)}")

    if action == 'click':
        if is_resuming:
            print("  - In resume mode, skipping initial navigation click.")
            return True
        clicked_text = perform_click(driver, step.get('target'))
        if clicked_text == "browser_crash": return False
        # The navigation path is pre-built, so we don't append here
        return True

    elif action == 'numeric_pagination_loop':
        return process_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_tablename)

    elif action == 'process_results':
        scraping_config = step.get('scraping_config')
        if not scraping_config:
            print("  - FATAL ERROR: 'process_results' action requires a 'scraping_config' object.")
            return False
        return scrape_configured_data(driver, step['target']['value'], scraping_config, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_tablename)
    else:
        print(f"  - WARNING: Unknown action type '{action}'. Skipping.")
    return True

def scrape_configured_data(driver, container_xpath, scraping_config, db_engine, parent_url_id, navigation_path_parts, page_num, job_state, destination_tablename):
    """Generic scraping function that saves results directly to the database."""
    try:
        wait = WebDriverWait(driver, 30)
        print(f"  - Waiting for main results container ({container_xpath})...")
        wait.until(EC.presence_of_element_located((By.XPATH, container_xpath)))
        loading_spinner_xpath = f"{container_xpath}//div[contains(@class, 'rpl-search-results__loading')]"
        print(f"  - Waiting for loading spinner to disappear ({loading_spinner_xpath})...")
        wait.until(EC.invisibility_of_element_located((By.XPATH, loading_spinner_xpath)))
        print("  - Loading spinner gone. Content should be loaded.")
        time.sleep(1)

        row_xpath = scraping_config['row_xpath']
        rows = driver.find_elements(By.XPATH, row_xpath)
        print(f"  - Found {len(rows)} result rows to scrape using XPath: {row_xpath}")
        if not rows: return True

        scraped_data = []
        for row in rows:
            row_data = {}
            for column_config in scraping_config['columns']:
                col_name, col_xpath, col_type = column_config['name'], column_config['xpath'], column_config.get('type', 'text')
                try:
                    element = row.find_element(By.XPATH, col_xpath)
                    row_data[col_name] = element.text if col_type == 'text' else element.get_attribute('href')
                except NoSuchElementException:
                    row_data[col_name] = None
            scraped_data.append(row_data)
        
        if scraped_data:
            save_book_links_to_db(db_engine, scraped_data, parent_url_id, navigation_path_parts, page_num, job_state, destination_tablename)
        return True
    except TimeoutException:
        print(f"  - INFO: Loading spinner did not appear or timed out. Assuming no results and continuing.")
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