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
    Deconstructs a combined code (e.g., 'NSWCATAP', 'WASAT', 'FamCA') into its parts.
    Improved to handle federal courts and standalone tribunal codes.
    """
    code_details = {
        'jurisdiction_code': None,
        'tribunal_code': None,
        'panel_or_division': None
    }
    
    remaining_code = combined_code
    
    # First, check if the entire code is a known tribunal (for federal courts like FamCA, FCA, etc.)
    tribunals = all_codes[all_codes['type'] == 'tribunal']
    if combined_code in tribunals['code'].values:
        # It's a standalone federal tribunal
        code_details['tribunal_code'] = combined_code
        code_details['jurisdiction_code'] = 'FED'  # Default to federal jurisdiction
        return code_details
    
    # Check for special federal court patterns
    federal_patterns = ['Fed', 'Fam', 'FCA', 'FC', 'AAT', 'HCA']
    for pattern in federal_patterns:
        if combined_code.startswith(pattern):
            # Try to match the longest possible tribunal code
            tribunals_sorted = tribunals.copy()
            tribunals_sorted['code_len'] = tribunals_sorted['code'].str.len()
            tribunals_sorted = tribunals_sorted.sort_values(by='code_len', ascending=False)
            
            for _, row in tribunals_sorted.iterrows():
                if combined_code.startswith(row['code']):
                    code_details['tribunal_code'] = row['code']
                    code_details['jurisdiction_code'] = 'FED'
                    remaining_code = combined_code[len(row['code']):]
                    
                    # Check if there's a panel/division code
                    if remaining_code:
                        panels = all_codes[all_codes['type'] == 'panel_or_division']
                        if remaining_code in panels['code'].values:
                            code_details['panel_or_division'] = remaining_code
                        else:
                            # It might be part of the tribunal code
                            code_details['tribunal_code'] = combined_code
                            remaining_code = ''
                    return code_details
    
    # Standard processing for state-based codes
    # 1. Determine Jurisdiction using the hint if provided
    if jurisdiction_hint:
        code_details['jurisdiction_code'] = jurisdiction_hint
        if remaining_code.startswith(jurisdiction_hint):
            remaining_code = remaining_code[len(jurisdiction_hint):]
    else:
        # Look for jurisdiction codes at the start
        jurisdictions = all_codes[all_codes['type'] == 'jurisdiction'].copy()
        jurisdictions['code_len'] = jurisdictions['code'].str.len()
        jurisdictions = jurisdictions.sort_values(by='code_len', ascending=False)
        
        for _, row in jurisdictions.iterrows():
            if remaining_code.startswith(row['code']):
                code_details['jurisdiction_code'] = row['code']
                remaining_code = remaining_code[len(row['code']):]
                break
    
    # 2. Find Tribunal from the remaining part of the code
    if remaining_code:
        tribunals = all_codes[all_codes['type'] == 'tribunal'].copy()
        tribunals['code_len'] = tribunals['code'].str.len()
        tribunals = tribunals.sort_values(by='code_len', ascending=False)
        
        for _, row in tribunals.iterrows():
            if remaining_code.startswith(row['code']):
                code_details['tribunal_code'] = row['code']
                remaining_code = remaining_code[len(row['code']):]
                break
        
        # If no tribunal found but there's still code, it might be a composite tribunal code
        if not code_details['tribunal_code'] and remaining_code:
            # Check if the entire remaining code might be a tribunal
            if remaining_code in tribunals['code'].values:
                code_details['tribunal_code'] = remaining_code
                remaining_code = ''
    
    # 3. The rest is the panel/division
    if remaining_code:
        panels = all_codes[all_codes['type'] == 'panel_or_division']
        if remaining_code in panels['code'].values:
            code_details['panel_or_division'] = remaining_code
        else:
            # Log unrecognized panel/division codes for debugging
            logging.debug(f"Unrecognized panel/division code: {remaining_code} from {combined_code}")
            code_details['panel_or_division'] = remaining_code
    
    return code_details

def parse_citation(citation_str, all_codes, jurisdiction_hint=None):
    """
    Parses a legal citation string to extract structured data.
    Enhanced to handle various citation formats more robustly.
    """
    details = {
        'year': None, 'jurisdiction_code': None, 'tribunal_code': None,
        'panel_or_division': None, 'decision_date': None, 'members': None
    }

    if not citation_str:
        return details

    # Updated pattern to be more flexible with spacing and optional components
    pattern = re.compile(
        r'\[(\d{4})\]\s+'        # Group 1: Year in brackets
        r'([A-Z][A-Za-z0-9]+)\s+'  # Group 2: Combined code (more flexible)
        r'\d+\s*'                # Decision number (not captured)
        r'(?:\((.*?)\))'         # Group 3: Decision date
        r'(?:\s*\((.*?)\))?'     # Group 4: Optional members list
    )
    
    match = pattern.match(citation_str)
    if not match:
        logging.warning(f"Could not parse citation format: {citation_str}")
        return details

    year_str, combined_code, date_str, members_str = match.groups()

    details['year'] = int(year_str)
    details['members'] = members_str.strip() if members_str else None
    
    # Parse the date with multiple possible formats
    if date_str:
        date_formats = [
            '%d %B %Y',      # 31 August 2022
            '%d %b %Y',      # 31 Aug 2022
            '%d/%m/%Y',      # 31/08/2022
            '%Y-%m-%d'       # 2022-08-31
        ]
        
        for fmt in date_formats:
            try:
                details['decision_date'] = datetime.strptime(date_str.strip(), fmt).date()
                break
            except ValueError:
                continue
        
        if not details['decision_date']:
            logging.warning(f"Could not parse date '{date_str}' in citation: {citation_str}")

    # Deconstruct the combined code
    code_details = deconstruct_citation_code(combined_code, all_codes, jurisdiction_hint)
    details.update(code_details)
    
    # Log if we couldn't determine jurisdiction or tribunal
    if not details['jurisdiction_code'] or not details['tribunal_code']:
        logging.info(f"Partial parsing for citation '{citation_str}': "
                    f"jurisdiction={details['jurisdiction_code']}, "
                    f"tribunal={details['tribunal_code']}, "
                    f"combined_code={combined_code}")
    
    return details