from selenium.webdriver.support.ui import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from urllib.parse import urljoin

from core.database import save_scraped_data_to_db


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