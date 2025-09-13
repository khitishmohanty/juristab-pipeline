import re
import json

def attach_page_number_tag(consolidated, page_number: int):
    """Adds 'page_number' to each node if it's not already present."""
    if isinstance(consolidated, list):
        for node in consolidated:
            if isinstance(node, dict) and "page_number" not in node:
                node["page_number"] = page_number
    elif isinstance(consolidated, dict):
        if "document_elements" in consolidated and isinstance(consolidated["document_elements"], list):
            for node in consolidated["document_elements"]:
                if isinstance(node, dict) and "page_number" not in node:
                    node["page_number"] = page_number
        elif "page_number" not in consolidated:
            consolidated["page_number"] = page_number
    return consolidated

def extract_json_string(raw_text: str) -> str:
    """Extracts the first valid JSON array or object from a raw string."""
    json_markdown_match = re.search(r"```json\s*([\s\S]*?)\s*```", raw_text, re.DOTALL)
    if json_markdown_match:
        return json_markdown_match.group(1).strip()
    json_pattern = r"(\[.*?\]|\{.*?\})"
    matches = re.findall(json_pattern, raw_text, re.DOTALL)
    if matches:
        for match in matches:
            try:
                json.loads(match)
                return match
            except json.JSONDecodeError:
                continue
    return ""

def _clean_json_string(text: str) -> str | None:
    """
    Placeholder for your existing extract_json_string utility.
    This version first looks for JSON within markdown ```json ... ``` code blocks.
    If not found, it attempts to find the first balanced JSON object '{...}'
    or array '[...]' in the text.

    Args:
        text: The raw string which might contain JSON.

    Returns:
        The extracted JSON string, or None if not found.
    """
    if not text or not isinstance(text, str):
        return None

    # 1. Try to find JSON within markdown-style code blocks (```json ... ```)
    # re.DOTALL allows '.' to match newline characters.
    # [\s\S]*? is a non-greedy match for any character including newlines.
    match_markdown = re.search(r'```json\s*(\{[\s\S]*?\}|\[[\s\S]*?\])\s*```', text, re.DOTALL)
    if match_markdown:
        return match_markdown.group(1).strip()

    # 2. If no markdown, find the first occurrence of '{' or '['
    # and attempt to extract a balanced JSON structure.
    start_char = ''
    end_char = ''
    start_index = -1

    # Find the earliest start of a JSON object or array
    first_brace_index = text.find('{')
    first_bracket_index = text.find('[')

    if first_brace_index != -1 and (first_bracket_index == -1 or first_brace_index < first_bracket_index):
        start_char = '{'
        end_char = '}'
        start_index = first_brace_index
    elif first_bracket_index != -1:
        start_char = '['
        end_char = ']'
        start_index = first_bracket_index
    else:
        return None # No '{' or '[' found

    # Count braces/brackets to find the end of the first valid JSON structure
    balance = 0
    for i in range(start_index, len(text)):
        if text[i] == start_char:
            balance += 1
        elif text[i] == end_char:
            balance -= 1
        
        if balance == 0:
            # Potential JSON string found
            potential_json_segment = text[start_index : i + 1]
            # Validate if this segment is actually valid JSON
            try:
                json.loads(potential_json_segment)
                return potential_json_segment # Return the first valid segment found
            except json.JSONDecodeError:
                # This segment was balanced but not valid JSON.
                # A more complex version might continue searching or try other heuristics.
                # For this placeholder, we stop after the first balanced segment fails parsing.
                return None 
    
    return None # No balanced JSON structure found and parsed successfully