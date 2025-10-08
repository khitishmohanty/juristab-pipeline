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

def parse_member_info(members_str):
    """
    Parses the members string to extract member title, name, and role.
    Examples:
    - "Member J Prentice" -> {'title': 'Member', 'name': 'J Prentice', 'full_text': 'Member J Prentice'}
    - "The Honourable Justice Cronin" -> {'title': 'Justice', 'name': 'Cronin', 'honorific': 'The Honourable'}
    - "K Dordevic SM" -> {'name': 'K Dordevic', 'post_nominal': 'SM'}
    """
    if not members_str:
        return None
    
    member_info = {
        'full_text': members_str.strip(),
        'honorific': None,
        'title': None,
        'name': None,
        'post_nominal': None,
        'role': None
    }
    
    # Remove "The Honourable" and store it
    if 'The Honourable' in members_str:
        member_info['honorific'] = 'The Honourable'
        members_str = members_str.replace('The Honourable', '').strip()
    
    # Common titles/roles
    titles = [
        'Chief Justice', 'Justice', 'Judge', 'Member', 'Senior Member', 
        'Deputy President', 'President', 'Commissioner', 'Magistrate',
        'Principal Member', 'General Member', 'Expert Member'
    ]
    
    # Check for titles
    for title in titles:
        if title in members_str:
            member_info['title'] = title
            members_str = members_str.replace(title, '').strip()
            break
    
    # Check for post-nominals (J, JJ, SM, DP, etc.) at the end
    post_nominal_pattern = r'\s+([A-Z]{1,3})$'
    post_nominal_match = re.search(post_nominal_pattern, members_str)
    if post_nominal_match:
        member_info['post_nominal'] = post_nominal_match.group(1)
        members_str = members_str[:post_nominal_match.start()].strip()
    
    # Check for role descriptions (Vice-President, etc.)
    role_pattern = r'(?:Vice-President|Vice President|Deputy President|Acting President)'
    role_match = re.search(role_pattern, members_str, re.IGNORECASE)
    if role_match:
        member_info['role'] = role_match.group(0)
        members_str = members_str.replace(role_match.group(0), '').strip()
    
    # What's left should be the name
    if members_str:
        member_info['name'] = members_str.strip()
    
    return member_info

def deconstruct_citation_code(combined_code, all_codes, jurisdiction_hint=None):
    """
    Deconstructs a combined code (e.g., 'NSWCATAP', 'WASAT', 'FamCA') into its parts.
    Returns jurisdiction code, tribunal code, and any panel/division suffix.
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
    Parses a legal citation string to extract ALL structured data.
    Format: [Year] TRIBUNAL_CODE Decision_Number (Date) (Members)
    
    Example: [2022] AATA 2108 (20 April 2022) (Member J Prentice)
    Returns:
    - year: 2022
    - tribunal_code: AATA
    - decision_number: 2108
    - decision_date: 2022-04-20
    - jurisdiction_code: FED (inferred)
    - member_info: parsed member details
    """
    details = {
        'year': None, 
        'jurisdiction_code': None, 
        'tribunal_code': None,
        'panel_or_division': None, 
        'decision_number': None,  # Now capturing this
        'decision_date': None, 
        'members': None,
        'member_info': None  # Structured member data
    }

    if not citation_str:
        return details

    # Updated pattern to CAPTURE the decision number
    pattern = re.compile(
        r'\[(\d{4})\]\s+'           # Group 1: Year in brackets
        r'([A-Z][A-Za-z0-9]+)\s+'   # Group 2: Tribunal/Court code
        r'(\d+)\s*'                  # Group 3: Decision number (NOW CAPTURED)
        r'(?:\((.*?)\))'             # Group 4: Decision date
        r'(?:\s*\((.*?)\))?'         # Group 5: Optional members/judges
    )
    
    match = pattern.match(citation_str)
    if not match:
        logging.warning(f"Could not parse citation format: {citation_str}")
        return details

    year_str, tribunal_code, decision_num, date_str, members_str = match.groups()

    # Extract all components
    details['year'] = int(year_str)
    details['decision_number'] = int(decision_num)
    details['members'] = members_str.strip() if members_str else None
    
    # Parse member information into structured format
    if members_str:
        details['member_info'] = parse_member_info(members_str)
    
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

    # Deconstruct the tribunal code to get jurisdiction and panel info
    code_details = deconstruct_citation_code(tribunal_code, all_codes, jurisdiction_hint)
    details.update(code_details)
    
    # Log successful parsing with all components
    if details['jurisdiction_code'] and details['tribunal_code']:
        logging.debug(f"Successfully parsed citation '{citation_str}': "
                     f"Year={details['year']}, "
                     f"Tribunal={details['tribunal_code']}, "
                     f"Decision#={details['decision_number']}, "
                     f"Jurisdiction={details['jurisdiction_code']}")
    else:
        logging.info(f"Partial parsing for citation '{citation_str}': "
                    f"jurisdiction={details['jurisdiction_code']}, "
                    f"tribunal={details['tribunal_code']}, "
                    f"decision#={details['decision_number']}")
    
    return details