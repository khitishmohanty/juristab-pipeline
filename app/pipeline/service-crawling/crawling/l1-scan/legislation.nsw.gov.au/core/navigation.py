import time
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By

from core.scraping import perform_click, scrape_configured_data


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