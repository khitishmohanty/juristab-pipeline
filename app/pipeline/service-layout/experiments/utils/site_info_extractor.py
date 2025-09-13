import requests
from bs4 import BeautifulSoup
import os
import json
from urllib.parse import urljoin, urlparse
import re
import time

# --- Configuration ---
DEFAULT_OUTPUT_DIR = "site_configs" # Directory to save the generated files
USER_AGENT = "DocuDiveJuristabSiteInfoExtractor/1.0 (+http://your-contact-or-project-url.com)" # IMPORTANT: Change this
REQUEST_DELAY = 1 # Seconds, polite delay between requests, especially if fetching multiple sitemaps

# --- Helper Functions ---

def get_safe_filename_prefix(base_url):
    """Creates a safe filename prefix from a base URL."""
    hostname = urlparse(base_url).hostname
    if not hostname:
        hostname = base_url.split("://")[-1].split("/")[0] # Fallback for unusual URLs
    safe_prefix = re.sub(r'[^a-zA-Z0-9_-]', '_', hostname)
    return safe_prefix.lower()

def make_request(url, is_xml=False):
    """Makes a request with a user-agent and handles basic errors."""
    print(f"Requesting: {url}")
    headers = {"User-Agent": USER_AGENT}
    try:
        time.sleep(REQUEST_DELAY)
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error during request to {url}: {e}")
        return None

# --- Core Functions ---

def fetch_and_save_robots_txt(base_url, output_dir, filename_prefix):
    """Fetches robots.txt and saves it."""
    robots_url = urljoin(base_url, "/robots.txt")
    response = make_request(robots_url)
    if response:
        filepath = os.path.join(output_dir, f"{filename_prefix}_robots.txt")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Successfully saved robots.txt to: {filepath}")
            return True
        except IOError as e:
            print(f"Error saving robots.txt to {filepath}: {e}")
    return False

def fetch_sitemap_urls_recursive(sitemap_url, visited_sitemaps=None):
    """
    Fetches all <loc> URLs from a sitemap.
    Handles sitemap index files by recursively fetching URLs from nested sitemaps.
    """
    if visited_sitemaps is None:
        visited_sitemaps = set()

    if sitemap_url in visited_sitemaps:
        return [] # Avoid infinite loops

    visited_sitemaps.add(sitemap_url)
    all_loc_urls = []
    response = make_request(sitemap_url, is_xml=True)

    if response:
        # Use 'lxml-xml' or 'xml' for XML parsing with BeautifulSoup
        # 'lxml' is generally more robust for XML
        soup = BeautifulSoup(response.content, 'xml')

        # Check if it's a sitemap index file (contains <sitemap> tags)
        sitemap_tags = soup.find_all('sitemap')
        if sitemap_tags:
            print(f"Found sitemap index: {sitemap_url}. Processing sub-sitemaps...")
            for sitemap_tag in sitemap_tags:
                sub_sitemap_loc = sitemap_tag.find('loc')
                if sub_sitemap_loc and sub_sitemap_loc.text:
                    sub_sitemap_url = sub_sitemap_loc.text.strip()
                    all_loc_urls.extend(fetch_sitemap_urls_recursive(sub_sitemap_url, visited_sitemaps))
        else:
            # It's a regular sitemap (contains <url> tags)
            url_tags = soup.find_all('url')
            for url_tag in url_tags:
                loc = url_tag.find('loc')
                if loc and loc.text:
                    all_loc_urls.append(loc.text.strip())
            print(f"Processed sitemap: {sitemap_url}, found {len(url_tags)} <url> tags.")
    else:
        print(f"Failed to fetch or parse sitemap: {sitemap_url}")

    return all_loc_urls

def generate_sitemap_config(base_url, output_dir, filename_prefix):
    """Fetches all URLs from sitemap(s) and saves them to a JSON config file."""
    initial_sitemap_url = urljoin(base_url, "/sitemap.xml")
    print(f"Starting sitemap fetch from: {initial_sitemap_url}")

    sitemap_urls = fetch_sitemap_urls_recursive(initial_sitemap_url)

    if sitemap_urls:
        filepath = os.path.join(output_dir, f"{filename_prefix}_sitemap_urls.json")
        config_data = {
            "source_sitemap": initial_sitemap_url,
            "retrieved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "base_url": base_url,
            "urls_count": len(sitemap_urls),
            "urls": sorted(list(set(sitemap_urls))) # Store unique, sorted URLs
        }
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=4)
            print(f"Successfully saved {len(sitemap_urls)} sitemap URLs to: {filepath}")
            return True
        except IOError as e:
            print(f"Error saving sitemap URLs to {filepath}: {e}")
        except json.JSONDecodeError as e:
            print(f"Error encoding JSON for sitemap URLs: {e}")
    else:
        print(f"No URLs found or error fetching sitemap for {base_url}")
    return False

# --- Main Execution Logic ---

def extract_all_site_info(target_base_url, output_directory=DEFAULT_OUTPUT_DIR):
    """
    Orchestrates fetching robots.txt and sitemap URLs for a given base URL.
    """
    if not target_base_url.startswith(('http://', 'https://')):
        print(f"Error: Base URL '{target_base_url}' must start with http:// or https://")
        return

    print(f"\nProcessing site: {target_base_url}")
    os.makedirs(output_directory, exist_ok=True)
    safe_prefix = get_safe_filename_prefix(target_base_url)

    # 1. Fetch and save robots.txt
    fetch_and_save_robots_txt(target_base_url, output_directory, safe_prefix)

    # 2. Fetch sitemap(s) and save URLs to JSON config
    generate_sitemap_config(target_base_url, output_directory, safe_prefix)

    print(f"Finished processing for {target_base_url}")

if __name__ == "__main__":
    # --- IMPORTANT: Customize USER_AGENT above before running extensively ---
    if USER_AGENT.startswith("DocuDiveJuristabSiteInfoExtractor/1.0 (+http://your-contact-or-project-url.com)"):
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! WARNING: Please update the USER_AGENT in the script with your details. !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # You might want to exit here if USER_AGENT is not set, or add a confirmation.
        # exit()

    # Example usage:
    # You can add more URLs to this list or get them from user input/file
    urls_to_process = [
        "https://www.legislation.vic.gov.au/",
        # "https://www.google.com/", # Example with a different site (be mindful of terms)
        # "https://www.example.com/"
    ]

    output_location = os.path.join(os.getcwd(), DEFAULT_OUTPUT_DIR) # Save in current_dir/site_configs
    # Or if you want it relative to the script's location:
    # script_dir = os.path.dirname(os.path.abspath(__file__))
    # output_location = os.path.join(script_dir, DEFAULT_OUTPUT_DIR)


    print(f"Output will be saved to: {output_location}")

    for site_url in urls_to_process:
        extract_all_site_info(site_url, output_directory=output_location)