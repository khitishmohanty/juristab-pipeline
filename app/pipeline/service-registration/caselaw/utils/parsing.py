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

def is_valid_tribunal_code(code):
    """
    Basic validation to check if a string could be a valid tribunal code.
    Filters out obvious bad data.
    """
    if not code or not isinstance(code, str):
        return False
    
    # Reject single letters except specific valid ones
    if len(code) == 1 and code not in ['O']:
        return False
    
    # Reject common words that are clearly not tribunal codes
    invalid_words = [
        'JULY', 'JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 
        'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER',
        'SUPREME', 'COURT', 'TRIBUNAL', 'PARTIESWHERE', 'NCE', 'UMBER', 
        'CASSINI', 'QD'
    ]
    
    # Don't reject jurisdiction codes here - they might be valid in some contexts
    # Let the deconstruct_citation_code function handle them
    
    if code.upper() in invalid_words:
        return False
    
    # Must contain at least 2 characters (except for special cases like 'O')
    if len(code) < 2 and code not in ['O']:
        return False
    
    # Should start with a letter and contain only letters and numbers
    if not code[0].isalpha():
        return False
    
    if not code.replace('_', '').isalnum():
        return False
    
    return True

def deconstruct_citation_code(combined_code, all_codes, jurisdiction_hint=None):
    """
    Deconstructs a combined code (e.g., 'NSWCATAP', 'WASAT', 'FamCA') into its parts.
    Returns jurisdiction code, tribunal code, and any panel/division suffix.
    
    IMPROVED VERSION: Checks for tribunal codes FIRST to avoid mis-parsing.
    """
    code_details = {
        'jurisdiction_code': None,
        'tribunal_code': None,
        'panel_or_division': None
    }
    
    # Get all tribunal codes, sorted by length (longest first)
    tribunals = all_codes[all_codes['type'] == 'tribunal'].copy()
    tribunals['code_len'] = tribunals['code'].str.len()
    tribunals = tribunals.sort_values(by='code_len', ascending=False)
    
    # STRATEGY 1: Check if the entire code is a known tribunal
    if combined_code in tribunals['code'].values:
        code_details['tribunal_code'] = combined_code
        # Try to infer jurisdiction from the tribunal code
        code_details['jurisdiction_code'] = infer_jurisdiction_from_tribunal(combined_code, jurisdiction_hint)
        return code_details
    
    # STRATEGY 2: Check if the code starts with a known tribunal code
    for _, tribunal_row in tribunals.iterrows():
        tribunal_code = tribunal_row['code']
        if combined_code.startswith(tribunal_code):
            code_details['tribunal_code'] = tribunal_code
            remaining_code = combined_code[len(tribunal_code):]
            
            # Try to infer jurisdiction
            code_details['jurisdiction_code'] = infer_jurisdiction_from_tribunal(tribunal_code, jurisdiction_hint)
            
            # Check if remainder is a panel/division
            if remaining_code:
                panels = all_codes[all_codes['type'] == 'panel_or_division']
                if remaining_code in panels['code'].values:
                    code_details['panel_or_division'] = remaining_code
            
            return code_details
    
    # STRATEGY 3: Check if code ends with known tribunal code (after removing jurisdiction prefix)
    jurisdictions = all_codes[all_codes['type'] == 'jurisdiction'].copy()
    jurisdictions['code_len'] = jurisdictions['code'].str.len()
    jurisdictions = jurisdictions.sort_values(by='code_len', ascending=False)
    
    for _, juris_row in jurisdictions.iterrows():
        juris_code = juris_row['code']
        if combined_code.startswith(juris_code):
            code_details['jurisdiction_code'] = juris_code
            remaining_code = combined_code[len(juris_code):]
            
            # Special handling for codes like "NSWWCCPD" or with spaces "NSW WCC PD"
            # First check if the entire remainder is a known tribunal
            if remaining_code in tribunals['code'].values:
                code_details['tribunal_code'] = remaining_code
                return code_details
            
            # Now check if remainder starts with a tribunal (possibly with panel/division)
            for _, tribunal_row in tribunals.iterrows():
                tribunal_code = tribunal_row['code']
                if remaining_code == tribunal_code:
                    code_details['tribunal_code'] = tribunal_code
                    return code_details
                elif remaining_code.startswith(tribunal_code):
                    code_details['tribunal_code'] = tribunal_code
                    panel_part = remaining_code[len(tribunal_code):]
                    
                    # Check if the rest is a panel/division
                    panels = all_codes[all_codes['type'] == 'panel_or_division']
                    if panel_part in panels['code'].values:
                        code_details['panel_or_division'] = panel_part
                    return code_details
    
    # STRATEGY 4: Common patterns for state-based tribunals not in config
    # This handles cases like NSWADT, VCAT, QIRC, VCC, VSCA
    state_tribunal_patterns = {
        'NSW': ['ADT', 'CTT', 'IRComm', 'WCC'],  # NSW specific tribunals
        'VIC': ['CAT', 'CC', 'SCA'],  # Victorian tribunals (VCAT, VCC, VSCA)
        'QLD': ['IRC', 'QCAT', 'QIRC'],  # Queensland tribunals
        'WA': ['SAT', 'IRC'],  # WA tribunals
        'SA': ['SAT', 'ERD'],  # SA tribunals
        'TAS': ['RAT'],  # Tasmania tribunals
        'ACT': ['AAT'],  # ACT tribunals
    }
    
    # Check for state prefixes in known patterns
    for state_code, tribunal_suffixes in state_tribunal_patterns.items():
        if combined_code.startswith(state_code):
            remaining = combined_code[len(state_code):]
            for suffix in tribunal_suffixes:
                if remaining == suffix or remaining.startswith(suffix):
                    code_details['jurisdiction_code'] = state_code
                    code_details['tribunal_code'] = remaining[:len(suffix)]
                    
                    # Check for any panel/division after the tribunal code
                    if len(remaining) > len(suffix):
                        panel_part = remaining[len(suffix):]
                        panels = all_codes[all_codes['type'] == 'panel_or_division']
                        if panel_part in panels['code'].values:
                            code_details['panel_or_division'] = panel_part
                    return code_details
    
    # STRATEGY 5: Check for single letter state abbreviations (V for VIC, Q for QLD)
    single_letter_mapping = {
        'V': 'VIC',
        'Q': 'QLD',
        'N': 'NSW',
        'S': 'SA',
        'W': 'WA',
        'T': 'TAS'
    }
    
    if combined_code[0] in single_letter_mapping:
        potential_jurisdiction = single_letter_mapping[combined_code[0]]
        remaining = combined_code[1:]
        
        # Check if remaining part could be a tribunal
        if remaining:
            # First check if it's a known tribunal
            if remaining in tribunals['code'].values:
                code_details['jurisdiction_code'] = potential_jurisdiction
                code_details['tribunal_code'] = remaining
                return code_details
            
            # Check common patterns
            common_tribunal_codes = ['CAT', 'CC', 'SCA', 'IRC', 'SAT', 'DC', 'SC', 'MC']
            for tribunal in common_tribunal_codes:
                if remaining == tribunal or remaining.startswith(tribunal):
                    code_details['jurisdiction_code'] = potential_jurisdiction
                    code_details['tribunal_code'] = tribunal
                    
                    if len(remaining) > len(tribunal):
                        panel_part = remaining[len(tribunal):]
                        panels = all_codes[all_codes['type'] == 'panel_or_division']
                        if panel_part in panels['code'].values:
                            code_details['panel_or_division'] = panel_part
                    return code_details
    
    # FALLBACK: If nothing worked, use the hint if available
    if jurisdiction_hint:
        code_details['jurisdiction_code'] = jurisdiction_hint
        # Assume the entire code is the tribunal
        code_details['tribunal_code'] = combined_code
    else:
        # Last resort: log warning and return what we can
        logging.warning(f"Could not fully parse citation code: {combined_code}")
        code_details['tribunal_code'] = combined_code  # Assume it's all tribunal code
    
    return code_details

def infer_jurisdiction_from_tribunal(tribunal_code, jurisdiction_hint=None):
    """
    Helper function to infer jurisdiction from tribunal code.
    """
    # Federal/Commonwealth tribunals
    federal_tribunals = ['HCA', 'FCA', 'FCAFC', 'FamCA', 'FamCAFC', 
                         'FedCFamC1F', 'FedCFamC2F', 'FCCA', 'AAT', 'AATA', 'FWC', 'AIRC',
                         'NNTT', 'DFDAT', 'CTA', 'FMCA', 'FMCAfam', 'FedCFamC1A', 'FedCFamC2G',
                         'FedCFamCG2', 'FEDCFAMC1F', 'FamCa']
    if tribunal_code in federal_tribunals:
        return 'FED'
    
    # New Zealand tribunals
    if tribunal_code.startswith('NZ') or tribunal_code in ['SC', 'CA', 'HC', 'DC', 'FC', 'YC', 
                                                            'EC', 'EmpC', 'ERA', 'MLC', 'MaoriLC',
                                                            'MACA', 'MaoriAC', 'CC', 'CSC', 'WT', 
                                                            'HRRT', 'DT', 'TT', 'MVDT', 'LVT', 'IPT',
                                                            'RSAA', 'SSAA', 'SAA', 'TRA', 'LCRO']:
        return 'NZ'
    
    # State-specific patterns in tribunal codes
    if tribunal_code.startswith('NSW') or tribunal_code in ['ADT', 'NSWADT', 'MHRT']:
        return 'NSW'
    elif tribunal_code.startswith('VIC') or tribunal_code in ['VCAT', 'VCC', 'VSCA', 'VSC', 'VMC', 'VMHT', 'VOCAT']:
        return 'VIC'
    elif tribunal_code.startswith('QLD') or tribunal_code in ['QIRC', 'QCAT', 'QSC', 'QCA', 'QDC', 'QLC', 'QMHRT', 'QMC']:
        return 'QLD'
    elif tribunal_code.startswith('WA') or tribunal_code in ['WASAT', 'WASC', 'WADC', 'WAMC', 'WAMHRT']:
        return 'WA'
    elif tribunal_code.startswith('SA') or tribunal_code in ['SASAT', 'SASC', 'SADC', 'SAMC', 'SAEOT', 'ERD', 'SAWC']:
        return 'SA'
    elif tribunal_code.startswith('TAS') or tribunal_code in ['TASCAT', 'TASC', 'TASSC', 'TASMC', 'TASGAB', 'TSGAB']:
        return 'TAS'
    elif tribunal_code.startswith('ACT') or tribunal_code in ['ACAT', 'ACTSC', 'ACTCA', 'ACTMC']:
        return 'ACT'
    elif tribunal_code.startswith('NT') or tribunal_code in ['NTCAT', 'NTSC', 'NTLC', 'NTMC', 'NTCCA']:
        return 'NT'
    
    # Use hint if available
    return jurisdiction_hint

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
        'decision_number': None,
        'decision_date': None, 
        'members': None,
        'member_info': None
    }

    if not citation_str:
        return details
    
    # Quick check for known bad data patterns
    if ' null ' in citation_str.lower():
        logging.warning(f"Citation contains 'null' values, likely bad source data: {citation_str}")
        # Return empty details to mark as failed
        return details

    # Try multiple patterns to handle different citation formats
    patterns = [
        # Standard pattern: [Year] TRIBUNAL NUMBER (Date)
        re.compile(
            r'\[{1,2}(\d{4})\]{1,2}\s+'  # Group 1: Year in single or double brackets
            r'([A-Z][A-Za-z0-9]+)\s+'    # Group 2: Tribunal/Court code
            r'(\d+)\s*'                   # Group 3: Decision number
            r'(?:\((.*?)\))'              # Group 4: Decision date
            r'(?:\s*\((.*?)\))?'          # Group 5: Optional members/judges
        ),
        # Multi-part tribunal code: [Year] JURISDICTION TRIBUNAL DIVISION NUMBER (Date)
        re.compile(
            r'\[{1,2}(\d{4})\]{1,2}\s+'  # Group 1: Year
            r'([A-Z]+)\s+'                # Group 2: Jurisdiction (NSW, VIC, etc.)
            r'([A-Z]+)\s+'                # Group 3: Tribunal code (WCC, CAT, etc.)
            r'([A-Z]+)\s+'                # Group 4: Division/Panel (PD, AP, etc.)
            r'(\d+)\s*'                   # Group 5: Decision number
            r'(?:\((.*?)\))'              # Group 6: Decision date
            r'(?:\s*\((.*?)\))?'          # Group 7: Optional members/judges
        ),
        # Reversed pattern: [Year] NUMBER TRIBUNAL (Date)
        re.compile(
            r'\[{1,2}(\d{4})\]{1,2}\s+'  # Group 1: Year
            r'(\d+)\s+'                   # Group 2: Decision number (comes first)
            r'([A-Z][A-Za-z0-9]+)\s*'    # Group 3: Tribunal/Court code (comes second)
            r'(?:\((.*?)\))'              # Group 4: Decision date
            r'(?:\s*\((.*?)\))?'          # Group 5: Optional members/judges
        ),
        # Pattern without decision number: [Year] TRIBUNAL ... (Date with year)
        re.compile(
            r'\[{1,2}(\d{4})\]{1,2}\s+'  # Group 1: Year
            r'([A-Z][A-Za-z0-9]+)\s+'    # Group 2: Tribunal/Court code
            r'.*?'                        # Match any characters (non-greedy)
            r'\((\d{1,2}\s+\w+\s+\d{4})\)' # Group 3: Date pattern (DD Month YYYY)
            r'(?:\s*\((.*?)\))?'          # Group 4: Optional members/judges
        )
    ]
    
    match = None
    pattern_used = None
    
    # Try each pattern
    for idx, pattern in enumerate(patterns):
        match = pattern.match(citation_str)
        if match:
            pattern_used = idx
            logging.debug(f"Pattern {idx} matched for citation: {citation_str}")
            break
    
    if not match:
        logging.warning(f"Could not parse citation format: {citation_str}")
        # Add more detail about what patterns were tried
        logging.debug(f"Citation that failed all patterns: {citation_str}")
        return details

    # Process based on which pattern matched
    if pattern_used == 0:  # Standard pattern
        year_str, tribunal_code, decision_num, date_str, members_str = match.groups()
        details['year'] = int(year_str)
        details['decision_number'] = int(decision_num) if decision_num else None
        details['members'] = members_str.strip() if members_str else None
        
    elif pattern_used == 1:  # Multi-part pattern: NSW WCC PD format
        year_str, jurisdiction_code, tribunal_code_part, panel_code, decision_num, date_str, members_str = match.groups()
        details['year'] = int(year_str)
        details['decision_number'] = int(decision_num) if decision_num else None
        details['members'] = members_str.strip() if members_str else None
        # For multi-part codes, we need to handle them specially
        # The jurisdiction is explicit, tribunal is the middle part
        tribunal_code = tribunal_code_part  # Just the tribunal part (WCC)
        # Store the jurisdiction and panel separately
        details['jurisdiction_code'] = jurisdiction_code
        details['panel_or_division'] = panel_code
        
    elif pattern_used == 2:  # Reversed pattern (number before tribunal)
        year_str, decision_num, tribunal_code, date_str, members_str = match.groups()
        details['year'] = int(year_str)
        details['decision_number'] = int(decision_num) if decision_num else None
        details['members'] = members_str.strip() if members_str else None
        
    elif pattern_used == 3:  # Pattern without decision number
        groups = match.groups()
        year_str = groups[0]
        tribunal_code = groups[1]
        date_str = groups[2] if len(groups) > 2 else None
        members_str = groups[3] if len(groups) > 3 else None
        details['year'] = int(year_str)
        details['decision_number'] = None  # No decision number in this format
        details['members'] = members_str.strip() if members_str else None
    
    # Parse member information into structured format
    if details['members']:
        details['member_info'] = parse_member_info(details['members'])
    
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
                parsed_date = datetime.strptime(date_str.strip(), fmt).date()
                details['decision_date'] = parsed_date
                logging.debug(f"Successfully parsed date '{date_str}' as {parsed_date}")
                break
            except ValueError as e:
                logging.debug(f"Failed to parse date '{date_str}' with format '{fmt}': {e}")
                continue
        
        if not details['decision_date']:
            logging.warning(f"Could not parse date '{date_str}' in citation: {citation_str}")

    # Deconstruct the tribunal code to get jurisdiction and panel info
    # Skip this if we already have jurisdiction and panel from multi-part pattern
    if pattern_used == 1:  # Multi-part pattern already set these
        # Just need to validate the tribunal code
        if tribunal_code and not is_valid_tribunal_code(tribunal_code):
            logging.warning(f"Invalid tribunal code detected: '{tribunal_code}' in citation: {citation_str}")
            details['tribunal_code'] = None
        else:
            details['tribunal_code'] = tribunal_code
    else:
        # Normal processing for other patterns
        code_details = deconstruct_citation_code(tribunal_code, all_codes, jurisdiction_hint)
        
        # Validate the tribunal code
        if code_details['tribunal_code'] and not is_valid_tribunal_code(code_details['tribunal_code']):
            logging.warning(f"Invalid tribunal code detected: '{code_details['tribunal_code']}' in citation: {citation_str}")
            code_details['tribunal_code'] = None
        
        details.update(code_details)
    
    # Log successful parsing with all components
    if details['jurisdiction_code'] and details['tribunal_code']:
        if details['decision_number']:
            logging.debug(f"Successfully parsed citation '{citation_str}': "
                         f"Year={details['year']}, "
                         f"Tribunal={details['tribunal_code']}, "
                         f"Decision#={details['decision_number']}, "
                         f"Jurisdiction={details['jurisdiction_code']}")
        else:
            logging.debug(f"Parsed citation without decision number '{citation_str}': "
                         f"Year={details['year']}, "
                         f"Tribunal={details['tribunal_code']}, "
                         f"Jurisdiction={details['jurisdiction_code']}")
    else:
        logging.info(f"Partial parsing for citation '{citation_str}': "
                    f"jurisdiction={details['jurisdiction_code']}, "
                    f"tribunal={details['tribunal_code']}, "
                    f"decision#={details['decision_number']}")
    
    return details