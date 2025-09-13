from selenium.webdriver.support.ui import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from urllib.parse import urljoin

from core.database import save_scraped_data_to_db


def scrape_configured_data(driver, container_xpath, scraping_config, db_engine, parent_url_id, navigation_path_parts, page_num, job_state, destination_table):
    """
    Generic scraping function that intelligently waits for data to be loaded 
    into the table before saving results directly to the database.
    """
    try:
        wait = WebDriverWait(driver, 20)
        row_xpath = scraping_config['row_xpath']

        data_loaded_xpath = f"{container_xpath}/{row_xpath}"

        print(f"  - Waiting for data to load in container ({data_loaded_xpath})...")
        wait.until(EC.presence_of_element_located((By.XPATH, data_loaded_xpath)))
        print("  - Data has loaded.")
        
        container_element = driver.find_element(By.XPATH, container_xpath)
        rows = container_element.find_elements(By.XPATH, row_xpath)
        
        print(f"  - Found {len(rows)} result rows to scrape.")
        if not rows: 
            return True

        scraped_data = []
        base_url = driver.current_url 
        for row in rows:
            row_data = {}
            for column_config in scraping_config['columns']:
                col_name, col_xpath, col_type = column_config['name'], column_config['xpath'], column_config.get('type', 'text')
                try:
                    element = row.find_element(By.XPATH, col_xpath)
                    if col_type == 'text':
                        row_data[col_name] = element.text
                    elif col_type == 'href':
                        row_data[col_name] = urljoin(base_url, element.get_attribute('href'))
                except NoSuchElementException:
                    row_data[col_name] = None
            scraped_data.append(row_data)
        
        if scraped_data:
            new_records = save_scraped_data_to_db(db_engine, scraped_data, parent_url_id, navigation_path_parts, page_num, destination_table)
            job_state['records_saved'] += new_records
        return True
    except TimeoutException:
        print(f"  - INFO: No data found in table for this page. This might be normal (e.g., end of pagination).")
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
    """
    Waits for an element to be present, then force-clicks it with JavaScript.
    Retries on StaleElementReferenceException.
    """
    retries = 3
    for i in range(retries):
        try:
            wait = WebDriverWait(driver, 10 if is_pagination else 20)
            element_locator = (By.XPATH, target['value'])
            
            # Wait only for the element to be PRESENT, not clickable.
            element = wait.until(EC.presence_of_element_located(element_locator))
            
            # Scroll the element into the middle of the view for good measure.
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.5)
            
            element_text = element.text.strip()
            print(f"  - Force-clicking element with XPath: {target['value']} (Text: '{element_text}') using JavaScript.")
            
            # Use JavaScript to perform the click, which can bypass overlaying elements.
            driver.execute_script("arguments[0].click();", element)
            
            return element_text  # Return on success
            
        except StaleElementReferenceException:
            print(f"  - WARNING: StaleElementReferenceException caught. Retrying ({i + 1}/{retries})...")
            time.sleep(1)  # Wait before next attempt
            
        except (TimeoutException, NoSuchElementException):
            if is_pagination: return None
            print(f"  - ERROR: Click target not found: {target['value']}")
            return None
        except WebDriverException as e:
            if "invalid session id" in str(e) or "browser has closed" in str(e):
                print(f"  - FATAL BROWSER CRASH during click: {e}")
                return "browser_crash"
            raise
            
    print(f"  - FATAL ERROR: Failed to click element after {retries} retries.")
    return None