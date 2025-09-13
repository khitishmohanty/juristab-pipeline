import time
from urllib.parse import urljoin, urlparse, parse_qs
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.scraping import perform_click, scrape_configured_data


def process_url_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, base_url):
    """
    Finds all links, extracts their URLs, and then visits each URL in a loop to scrape data.
    This is highly robust against dynamic page updates that break element references.
    """
    target_xpath = step.get('target_xpath')
    if not target_xpath:
        print("  - ERROR: 'url_loop' requires a 'target_xpath'.")
        return False

    print(f"  - Locating all target links to extract URLs from ({target_xpath})...")
    try:
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.XPATH, target_xpath)))
        link_elements = driver.find_elements(By.XPATH, target_xpath)

        urls_to_visit = [link.get_attribute('href') for link in link_elements]
        urls_to_visit = [url for url in urls_to_visit if url]
        
        num_urls = len(urls_to_visit)
        print(f"  - Found {num_urls} URLs to process sequentially.")
        if num_urls == 0:
            print("  - WARNING: No URLs found, skipping loop.")
            return True

    except Exception as e:
        print(f"  - FATAL ERROR: Could not find or extract URLs from initial links: {e}")
        return False

    for i, url in enumerate(urls_to_visit):
        print(f"\n--- Processing URL {i+1}/{num_urls} ---")
        try:
            print(f"  - Navigating to: {url}")
            driver.get(url)

            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            letter = query_params.get('key', [f"URL-{i+1}"])[0]

            url_path_parts = navigation_path_parts + [f"Letter-{letter}"]
            for loop_step in step['loop_steps']:
                if not process_step(driver, loop_step, db_engine, parent_url_id, url_path_parts, job_state, destination_table, base_url):
                    print(f"  - A step failed within the URL loop for URL '{url}'. Halting journey.")
                    return False
        
        except Exception as e:
            print(f"  - RECOVERABLE ERROR: An issue occurred during the loop for URL {i}: {e}. Halting journey.")
            return False
            
    return True

def process_step(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, base_url, current_page=1):
    """Main dispatcher function. Processes a single step from the configuration."""
    action = step.get('action')
    print(f"\nProcessing Step: {step.get('description', 'No description')}")

    if action == 'click':
        clicked_text = perform_click(driver, step.get('target'))
        if clicked_text == "browser_crash": return False
        return True
    
    elif action == 'url_loop':
        return process_url_loop(driver, step, db_engine, parent_url_id, navigation_path_parts, job_state, destination_table, base_url)

    elif action == 'process_results':
        scraping_config = step.get('scraping_config')
        if not scraping_config:
            print("  - FATAL ERROR: 'process_results' action requires a 'scraping_config' object.")
            return False
        return scrape_configured_data(driver, step.get('target', {}).get('value'), scraping_config, db_engine, parent_url_id, navigation_path_parts, current_page, job_state, destination_table, base_url)
    else:
        print(f"  - WARNING: Unknown action type '{action}'. Skipping.")
    return True