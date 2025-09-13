import re
from thefuzz import fuzz # For fuzzy matching
import string

def _normalize_text(text: str) -> str:
    """
    Converts text to lowercase, removes all punctuations (including backslashes), 
    and normalizes whitespace.
    """
    if not isinstance(text, str):
        return ""
    
    text = text.lower()
    
    # Create a translation table that maps each punctuation character to None (to remove it)
    # string.punctuation typically includes: !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
    translator = str.maketrans('', '', string.punctuation)
    text = text.translate(translator)
    
    # Replace multiple whitespace characters (including newlines, tabs) with a single space
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()


def _verify_item_content_in_direct_text(page_data_dict: dict, direct_text: str, page_num: int) -> dict:
    """
    Verifies 'content' of items in page_data_dict against direct_text.
    Updates 'verification-flag' in each item.
    """
    normalized_direct_text = _normalize_text(direct_text)
    items_key = None

    if "page_elements" in page_data_dict and isinstance(page_data_dict["page_elements"], list):
        items_key = "page_elements"
    elif "items" in page_data_dict and isinstance(page_data_dict["items"], list): # From previous Gemini/OpenAI layout outputs
        items_key = "items"
    # Add other potential keys if the structure containing the list of items varies

    if not items_key and isinstance(page_data_dict, list): # If page_data_dict itself is the list of items
        items_to_verify = page_data_dict
    elif items_key:
        items_to_verify = page_data_dict.get(items_key, [])
    else: # Not a list and known keys not found
        print(f"⚠️ Page {page_num}: Could not find a list of items in page_data_dict for content verification. Structure: {list(page_data_dict.keys())}")
        return page_data_dict


    if not normalized_direct_text:
        print(f"ℹ️ Page {page_num}: Direct text is empty. Marking item content verification as 'Skipped (No Direct Text)'.")
        for item in items_to_verify:
            if isinstance(item, dict):
                item["verification-flag"] = "Skipped (No Direct Text)"
        return page_data_dict

    for item_idx, item in enumerate(items_to_verify):
        if isinstance(item, dict) and "content" in item:
            item_content_value = item.get("content")
            original_flag = item.get("verification-flag", "Not Verified") # Preserve original if not changing

            if item_content_value and isinstance(item_content_value, str):
                normalized_item_content = _normalize_text(item_content_value)
                if normalized_item_content and normalized_item_content in normalized_direct_text:
                    item["verification-flag"] = "Verified (Direct Text Match)"
                else:
                    item["verification-flag"] = "Failed (Direct Text Mismatch)"
            else:
                item["verification-flag"] = "Failed (Invalid Or Empty Content Field)"
        # else: item might not be a dict or have 'content', its flag remains as is or default
    
    # If page_data_dict was the list itself, this function modified items in it.
    # If items_key was used, page_data_dict was modified in place.
    return page_data_dict


def _verify_item_content_in_direct_text_fuzzy(
    page_data_dict: dict, 
    direct_text: str, 
    page_num: int, 
    fuzzy_threshold: int = 88, # Default fuzzy matching threshold (e.g., 88%)
    min_content_len_for_fuzzy: int = 4 # Min length of item content to apply fuzzy match robustly
) -> dict:
    """
    Verifies 'content' of items in page_data_dict against direct_text using fuzzy matching.
    Updates 'verification-flag' in each item.
    """
    normalized_direct_text = _normalize_text(direct_text)
    items_key = None

    if "page_elements" in page_data_dict and isinstance(page_data_dict["page_elements"], list):
        items_key = "page_elements"
    elif "items" in page_data_dict and isinstance(page_data_dict["items"], list):
        items_key = "items"
    
    items_to_verify = []
    if items_key:
        items_to_verify = page_data_dict.get(items_key, [])
    elif isinstance(page_data_dict, list): # If page_data_dict itself is the list of items
        items_to_verify = page_data_dict
    else:
        print(f"⚠️ Page {page_num}: Could not find a list of items in page_data_dict for content verification. Structure: {list(page_data_dict.keys())}")
        return page_data_dict

    if not normalized_direct_text:
        print(f"ℹ️ Page {page_num}: Direct text is empty. Marking item content verification as 'Skipped (No Direct Text)'.")
        for item in items_to_verify:
            if isinstance(item, dict):
                item["verification-flag"] = "Skipped (No Direct Text)"
        return page_data_dict

    for item_idx, item in enumerate(items_to_verify):
        if isinstance(item, dict) and "content" in item:
            item_content_value = item.get("content")
            original_flag = item.get("verification-flag", "Not Verified") 

            if item_content_value and isinstance(item_content_value, str):
                normalized_item_content = _normalize_text(item_content_value)
                if normalized_item_content:
                    match_score = 0
                    if len(normalized_item_content) < min_content_len_for_fuzzy:
                        # For very short strings, exact match is more reliable than fuzzy
                        if normalized_item_content in normalized_direct_text:
                            match_score = 100 
                        item["verification-flag"] = f"Verified (Exact Match)" if match_score == 100 else f"Failed (Exact Mismatch - Short)"
                    else:
                        # Use partial_ratio for fuzzy substring matching
                        match_score = fuzz.partial_ratio(normalized_item_content, normalized_direct_text)
                        if match_score >= fuzzy_threshold:
                            item["verification-flag"] = f"Verified (Match {match_score}%)"
                        else:
                            item["verification-flag"] = f"Failed (Match {match_score}%)"
                else:
                    item["verification-flag"] = "Not Verified (Empty Normalized Item Content)"
            else:
                item["verification-flag"] = "Failed (Invalid Or Empty Content Field)"
        # else: item might not be a dict or have 'content', its flag remains as is
    
    return page_data_dict