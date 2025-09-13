from selenium.webdriver.support.ui import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from urllib.parse import urljoin

from core.database import save_scraped_data_to_db


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