import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.scraping import perform_click, scrape_configured_data


def process_next_button_pagination_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table):
    """Dedicated function for simple 'Next' button pagination."""
    page_counter = 1
    while True:
        print(f"\n--- Scraping results on page {page_counter} ---")
        
        step_success = process_step(driver, step['loop_steps'][0], db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, current_page=page_counter)
        
        if not step_success:
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
    """
    Handles alphabet-based navigation by first waiting for the alphabet bar to be present,
    then finding and clicking each alphabet link in turn.
    """
    target_xpath = step.get('target_xpath')
    if not target_xpath:
        print("  - ERROR: 'target_xpath' not defined for alphabet_loop.")
        return False
        
    print(f"  - Waiting for alphabet links to be present ({target_xpath})...")
    try:
        # This is the key wait that ensures the page is ready after the initial click.
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, target_xpath))
        )
        alphabet_links = driver.find_elements(By.XPATH, target_xpath)
        num_links = len(alphabet_links)
        print(f"  - Found {num_links} alphabet links to process.")
        if num_links == 0:
            print("  - WARNING: No alphabet links found, skipping loop.")
            return True
    except (TimeoutException, NoSuchElementException) as e:
        print(f"  - FATAL ERROR: Could not find initial alphabet links: {e}")
        return False

    for i in range(num_links):
        print(f"\n--- Processing alphabet link {i+1}/{num_links} ---")
        try:
            # Re-find the elements in each iteration to get a fresh, non-stale list
            current_alphabet_links = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.XPATH, target_xpath))
            )
            
            if i >= len(current_alphabet_links):
                print(f"  - ERROR: Index {i} out of bounds after re-finding links.")
                break

            link_to_click = current_alphabet_links[i]
            letter_text = link_to_click.text.strip()
            print(f"  - Preparing to click letter: '{letter_text}'")
            
            # This click triggers the JavaScript to load the table for that letter
            link_to_click.click()

            letter_path_parts = navigation_path_parts + [f"Letter-{letter_text}"]
            
            # Process the nested steps (pagination and scraping)
            for loop_step in step['loop_steps']:
                if not process_step(driver, loop_step, db_engine, parent_url_id, letter_path_parts, job_state, destination_table):
                    print(f"  - A step failed within the alphabet loop for letter '{letter_text}'. Skipping to the next letter.")
                    break 
        
        except StaleElementReferenceException:
            print(f"  - RECOVERABLE ERROR: StaleElementReferenceException on letter index {i}. Will retry.")
            continue
        except WebDriverException as e:
            if "invalid session id" in str(e) or "browser has closed" in str(e):
                print(f"  - FATAL BROWSER CRASH during alphabet loop for letter index {i}: {e}")
                return False
            raise
            
    return True

def process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, current_page=1):
    """Main dispatcher function. Processes a single step from the configuration."""
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', 'No description')}")

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
        return scrape_configured_data(driver, step.get('target', {}).get('value'), scraping_config, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_table)
    else:
        print(f"  - WARNING: Unknown action type '{action}'. Skipping.")
    return True