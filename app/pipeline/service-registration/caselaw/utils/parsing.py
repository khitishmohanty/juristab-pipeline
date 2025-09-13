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

def parse_parties(book_name):
    """
    Extracts primary and secondary parties from the book_name string.
    """
    if not book_name:
        return None, None
    parts = re.split(r'\s+v\s+', book_name, 1, re.IGNORECASE)
    primary_party = parts[0].strip() if parts else None
    secondary_party = parts[1].strip() if len(parts) > 1 else None
    return primary_party, secondary_party

def deconstruct_citation_code(combined_code, all_codes, jurisdiction_hint=None):
    """
    Deconstructs a combined code (e.g., 'NSWCATAP', 'WASAT') into its parts.
    """
    code_details = {
        'jurisdiction_code': None,
        'tribunal_code': None,
        'panel_or_division': None
    }
    remaining_code = combined_code

    # 1. Determine Jurisdiction using the hint if provided
    if jurisdiction_hint:
        code_details['jurisdiction_code'] = jurisdiction_hint
        if remaining_code.startswith(jurisdiction_hint):
            remaining_code = remaining_code[len(jurisdiction_hint):]
    else:
        # Fallback to searching if no hint is provided.
        jurisdictions = all_codes[all_codes['type'] == 'jurisdiction'].copy()
        jurisdictions['code_len'] = jurisdictions['code'].str.len()
        jurisdictions = jurisdictions.sort_values(by='code_len', ascending=False)
        for _, row in jurisdictions.iterrows():
            if remaining_code.startswith(row['code']):
                code_details['jurisdiction_code'] = row['code']
                remaining_code = remaining_code[len(row['code']):]
                break
    
    # 2. Find Tribunal from the remaining part of the code
    if code_details['jurisdiction_code']:
        tribunals = all_codes[all_codes['type'] == 'tribunal'].copy()
        tribunals['code_len'] = tribunals['code'].str.len()
        tribunals = tribunals.sort_values(by='code_len', ascending=False)
        for _, row in tribunals.iterrows():
            if remaining_code.startswith(row['code']):
                code_details['tribunal_code'] = row['code']
                remaining_code = remaining_code[len(row['code']):]
                break
    
    # 3. The rest is the panel/division
    if remaining_code:
        code_details['panel_or_division'] = remaining_code
    
    return code_details

def parse_citation(citation_str, all_codes, jurisdiction_hint=None):
    """
    Parses a legal citation string to extract structured data.
    Now ignores the decision number as it's no longer needed.
    """
    details = {
        'year': None, 'jurisdiction_code': None, 'tribunal_code': None,
        'panel_or_division': None, 'decision_date': None, 'members': None
        # 'decision_number' has been removed.
    }

    if not citation_str:
        return details

    # Updated pattern to match the decision number but not capture it.
    pattern = re.compile(
        r'\[(\d{4})\]\s+'        # Group 1: Year in brackets, e.g., [2025]
        r'([A-Z]+)\s+'           # Group 2: Combined code, e.g., NSWCATAP
        r'\d+\s+'                # Decision number (matched, but not captured)
        r'\((.*?)\)\s*'          # Group 3: Decision date, e.g., (11 July 2025)
        r'(?:\((.*?)\))?$'       # Group 4: Optional members list, e.g., (M Deane...)
    )
    match = pattern.match(citation_str)
    if not match:
        logging.warning(f"Could not parse citation format: {citation_str}")
        return details

    # Unpack groups according to the new pattern
    year_str, combined_code, date_str, members_str = match.groups()

    details['year'] = int(year_str)
    # The line for 'decision_number' is removed.
    details['members'] = members_str.strip() if members_str else None
    try:
        details['decision_date'] = datetime.strptime(date_str.strip(), '%d %B %Y').date()
    except ValueError:
        logging.warning(f"Could not parse date '{date_str}' in citation: {citation_str}")

    # Deconstruct the combined code
    code_details = deconstruct_citation_code(combined_code, all_codes, jurisdiction_hint)
    details.update(code_details)
            
    return details
