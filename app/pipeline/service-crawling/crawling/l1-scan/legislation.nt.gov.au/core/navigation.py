import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.scraping import perform_click, scrape_configured_data


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
            
            # MODIFIED CLICK METHOD: Use JavaScript to prevent interception by other elements
            driver.execute_script("arguments[0].click();", link_to_click)
            time.sleep(1)

            letter_path_parts = navigation_path_parts + [f"Letter-{letter_text}"]
            
            for loop_step in step['loop_steps']:
                if not process_step(driver, loop_step, db_engine, parent_url_id, letter_path_parts, job_state, destination_table, journey_state=journey_state):
                    raise Exception(f"Step failed for letter '{letter_text}'")
            
            if journey_state is not None:
                journey_state['last_completed_index'] = i
                print(f"  - Successfully completed letter index {i}. State updated.")

        except Exception as e:
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