import logging
import re
import yaml
import pandas as pd
from datetime import datetime

def load_config(path='config/config.yaml'):
    """
    Loads the main YAML configuration file.
    """
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {path}")
        return None
    except Exception as e:
        logging.error(f"Error loading YAML configuration from {path}: {e}")
        return None

def load_json_config(path):
    """
    Loads a JSON configuration file into a pandas DataFrame.
    """
    try:
        return pd.read_json(path, orient='records')
    except FileNotFoundError:
        logging.error(f"JSON config file not found at {path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error loading JSON config from {path}: {e}")
        return pd.DataFrame()

def parse_legislation_context(context_str):
    """
    Parses the book_context string to extract the start date and book version.
    Handles various date formats and labels.
    Example contexts:
    - "Start date: 08/08/2025"
    - "Currency date: 01 January 2005"
    - "Version 003 - Start date: 23/11/2019"
    - "Date of assent: 23/07/2025"
    - "Date made: 01/01/2021"

    Args:
        context_str (str): The string from the book_context column.

    Returns:
        dict: A dictionary containing 'start_date' and 'book_version'.
    """
    details = {'start_date': None, 'book_version': None}
    if not isinstance(context_str, str):
        logging.warning("Received non-string context. Cannot parse.")
        return details

    # 1. Extract book version if present
    version_match = re.search(r'Version\s*(\d+)', context_str, re.IGNORECASE)
    if version_match:
        details['book_version'] = version_match.group(1)
        logging.info(f"Successfully parsed book version: {details['book_version']}")

    # 2. Extract date using a flexible pattern for various labels and formats
    date_pattern = r'(?:Start date|Currency date|Date published|Date of assent|Date made):\s*(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}\s+\w+\s+\d{4})'
    date_match = re.search(date_pattern, context_str, re.IGNORECASE)
    
    if date_match:
        date_str = date_match.group(1).strip()
        parsed_date = None
        # Attempt to parse multiple known date string formats
        for fmt in ('%d/%m/%Y', '%d %B %Y'):
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                break  # Stop on the first successful parse
            except ValueError:
                continue # If parsing fails, try the next format
        
        if parsed_date:
            details['start_date'] = parsed_date
            logging.info(f"Successfully parsed start date: {details['start_date']}")
        else:
            logging.warning(f"Could not parse date '{date_str}' from context: {context_str}")
    else:
        logging.warning(f"Could not find a valid date pattern in context: {context_str}")

    return details