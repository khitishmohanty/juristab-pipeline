import os
import logging
import json
import boto3
import time # Import the time module
from botocore.exceptions import NoCredentialsError, ClientError

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# Build an absolute path to the project's root directory.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_webdriver():
    """
    Initializes and returns a Selenium WebDriver.
    - For local runs, it uses webdriver-manager.
    - For Fargate runs (RUN_ENV=FARGATE), it uses the pre-installed Chrome.
    """
    options = webdriver.ChromeOptions()
    
    if os.getenv('RUN_ENV') == 'FARGATE':
        logging.info("Fargate environment detected. Running Chrome in headless mode.")
        
        # Set the path to the Chrome binary installed by apt-get in the Dockerfile
        options.binary_location = "/usr/bin/google-chrome-stable"
        
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280x1696")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--user-data-dir=/tmp/user-data")
        options.add_argument("--remote-debugging-port=9222")
        
        # Chromedriver is on the system's PATH, so we may not need to specify it.
        # But being explicit is more robust.
        service = ChromeService(executable_path="/usr/local/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)
    else:
        logging.info("Local environment detected. Running Chrome with a visible UI.")
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logging.error(f"Failed to start local Chrome driver: {e}")
            return None
            
    return driver

def handle_initial_popups(driver):
    """Handles common initial pop-ups like cookie banners or login prompts."""
    try:
        no_thanks_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'No thanks')]"))
        )
        no_thanks_button.click()
        logging.info("Clicked 'No thanks' on the login/subscription pop-up.")
        time.sleep(1) # Wait 1 second for the DOM to potentially settle after click
    except TimeoutException:
        logging.info("'No thanks' pop-up not found, continuing.")

    try:
        got_it_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Got it')]"))
        )
        got_it_button.click()
        logging.info("Clicked 'Got it' on the cookie banner.")
        time.sleep(1) # Wait 1 second for the DOM to potentially settle after click
    except TimeoutException:
        logging.info("Cookie banner not found, continuing.")


def get_sitemap():
    """Loads the sitemap configuration from the JSON file using an absolute path."""
    sitemap_path = os.path.join(PROJECT_ROOT, 'config', 'sitemap.json')
    try:
        with open(sitemap_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Sitemap file not found at: {sitemap_path}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from sitemap file: {sitemap_path}")
        return None

def scrape_content(url):
    """
    Scrapes multiple content blocks from a URL based on the sitemap
    and combines them into a single HTML string.
    Includes scrolling logic and logic to find ALL matching elements.
    """
    driver = get_webdriver()
    if not driver:
        return None
        
    try:
        logging.info(f"Navigating to URL: {url}")
        driver.get(url)
        handle_initial_popups(driver)

        # --- SCROLL TO BOTTOM TO LOAD ALL DYNAMIC CONTENT ---
        logging.info("Scrolling down the page to load all dynamic content...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        logging.info("Finished scrolling. All content should be loaded.")
        
        sitemap = get_sitemap()
        if not sitemap:
            return None

        combined_html = []
        wait = WebDriverWait(driver, 20)

        # Loop through all configured selectors in the sitemap
        for selector_info in sitemap.get('content_selectors', []):
            selector = selector_info['selector']
            name = selector_info.get('name', selector)
            
            logging.info(f"Waiting for content block '{name}' with selector: '{selector}'")
            try:
                # Find ALL elements matching the selector
                content_elements = wait.until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                logging.info(f"Found {len(content_elements)} element(s) for '{name}'. Extracting HTML.")
                
                # Loop through each found element and add its HTML
                for element in content_elements:
                    # --- THIS IS THE FIX ---
                    # Add a brief pause to allow javascript to finish rendering the content of the element.
                    time.sleep(0.2)
                    combined_html.append(element.get_attribute('outerHTML'))
                
            except TimeoutException:
                logging.warning(f"Content block '{name}' not found with selector '{selector}'. Skipping.")
        
        return "\n".join(combined_html) if combined_html else None
        
    except Exception as e:
        logging.error(f"An error occurred while scraping {url}: {e}")
        return None
    finally:
        if driver:
            logging.info("Closing the browser.")
            driver.quit()

def get_s3_object_size(bucket_name, s3_key):
    """
    Fetches the size of an object in S3 without downloading it.
    Returns the size in bytes, or -1 if the object is not found or an error occurs.
    """
    s3_client = boto3.client('s3')
    try:
        # head_object is efficient as it only fetches metadata (like size)
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        size = response.get('ContentLength', 0)
        logging.info(f"Baseline file s3://{bucket_name}/{s3_key} has size: {size} bytes.")
        return size
    except ClientError as e:
        # Handle the case where the miniviewer.html file doesn't exist
        if e.response['Error']['Code'] == '404':
            logging.error(f"Baseline file not found at s3://{bucket_name}/{s3_key}")
        else:
            logging.error(f"An S3 client error occurred when fetching size for {s3_key}: {e}")
        return -1
    except NoCredentialsError:
        logging.error("Credentials not available for AWS S3.")
        return -1
    
def upload_to_s3(content, bucket_name, s3_key):
    """Uploads content to a specified S3 bucket."""
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=s3_key, 
            Body=content,
            ContentType='text/html'
        )
        logging.info(f"Successfully uploaded content to s3://{bucket_name}/{s3_key}")
        return True
    except NoCredentialsError:
        logging.error("Credentials not available for AWS S3.")
        return False
    except ClientError as e:
        logging.error(f"An S3 client error occurred: {e}")
        return False