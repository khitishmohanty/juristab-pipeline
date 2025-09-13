import time
import json
import os
import re # <-- Added import for regular expressions

from services.openai_client import call_openai_with_pdf
from utils.prompt_loader import load_text_prompt
from utils.json_utils import _clean_json_string

openai_prompt_text = load_text_prompt("openai_layout_prompt.txt")


def _call_openai_for_layout(temp_pdf_page_path: str, page_num_actual: int, genai_output_dir: str, metrics: dict) -> dict:
    """Calls OpenAI API for layout extraction and updates metrics."""
    global openai_prompt_text
    openai_json_result_for_return = {} # This will be the dictionary returned by the function
    openai_raw_text = ""
    openai_layout_start_time = time.time()
    
    output_file_path = os.path.join(genai_output_dir, f"page_{page_num_actual}_openai.json") # More accurate name

    try:
        openai_api_call_response = call_openai_with_pdf(
            pdf_path=temp_pdf_page_path,
            prompt=openai_prompt_text
        )
        metrics["time_sec_openai_layout"] = time.time() - openai_layout_start_time
        openai_raw_text = openai_api_call_response.get("text", "") 
        
        # Optional: save the truly raw text from OpenAI if needed for deepest debugging
        # with open(os.path.join(genai_output_dir, f"page_{page_num_actual}_openai_VERY_raw.txt"), "w", encoding="utf-8") as f:
        #    f.write(openai_raw_text)

        metrics.update({
            "openai_api_status": 200,
            "openai_input_tokens": openai_api_call_response.get("input_tokens", 0),
            "openai_output_tokens": openai_api_call_response.get("output_tokens", 0),
            "openai_cost_usd": openai_api_call_response.get("cost", 0.0)
        })

        cleaned_openai_json_str = _clean_json_string(openai_raw_text)
        metrics["openai_response_length"] = len(cleaned_openai_json_str or "")

        parsed_data_for_file = None # To store data before serializing to file

        if cleaned_openai_json_str:
            try:
                parsed_data = json.loads(cleaned_openai_json_str) # This can be a list or dict
                parsed_data_for_file = parsed_data # Will be used for saving to file

                if isinstance(parsed_data, list):
                    # Valid case: OpenAI returned a list of items.
                    # Wrap it in a dict as per function's return type hint `-> dict`.
                    openai_json_result_for_return = {
                        "items": parsed_data, # You can choose a more descriptive key like "page_elements"
                        "page_number": page_num_actual,
                        "_root_type": "list" # Optional: flag indicating the original root type
                    }
                    print(f"✅ Received and parsed OpenAI response for page {page_num_actual}")
                elif isinstance(parsed_data, dict):
                    # Valid case: OpenAI returned a dictionary.
                    openai_json_result_for_return = parsed_data
                    if "page_number" not in openai_json_result_for_return:
                        openai_json_result_for_return["page_number"] = page_num_actual
                    
                    if "error" not in openai_json_result_for_return: # Check if the dict itself isn't an error message
                        print(f"✅ Received and parsed OpenAI response (dictionary) for page {page_num_actual}")
                    else:
                        # The parsed dict already contains an error key (e.g. from _clean_json_string if it returns error dicts)
                        print(f"⚠️ Parsed OpenAI response for page {page_num_actual} is a dictionary containing an error: {openai_json_result_for_return.get('error')}")
                else:
                    # Parsed data is neither list nor dict (e.g., a string, number, boolean from JSON)
                    # This is likely an unexpected format for layout data.
                    error_msg = "OpenAI response parsed to an unexpected data type (not list or dict)"
                    print(f"⚠️ {error_msg} for page {page_num_actual}. Type: {type(parsed_data).__name__}")
                    openai_json_result_for_return = {
                        "error": error_msg,
                        "parsed_data_type": type(parsed_data).__name__,
                        "page_number": page_num_actual,
                        "raw_output_preview": openai_raw_text[:200]
                    }
                    parsed_data_for_file = openai_json_result_for_return # Save error to file

            except json.JSONDecodeError as je:
                print(f"⚠️ JSON DECODING ERROR on cleaned string (OpenAI Call, Page {page_num_actual}): {je}")
                error_msg = f"JSONDecodeError on cleaned string: {je}"
                openai_json_result_for_return = {
                    "error": error_msg, 
                    "cleaned_string_preview": cleaned_openai_json_str[:200] if cleaned_openai_json_str else "None",
                    "raw_output_preview": openai_raw_text[:200], 
                    "page_number": page_num_actual
                }
                # Save the problematic cleaned string or an error if it's None
                parsed_data_for_file = cleaned_openai_json_str if cleaned_openai_json_str else openai_json_result_for_return
        else:
            # _clean_json_string returned None or empty string
            error_msg = "Could not extract/clean valid JSON string from OpenAI response."
            if not openai_raw_text:
                error_msg = "Empty raw response from OpenAI."
            
            print(f"⚠️ {error_msg} for page {page_num_actual}.")
            openai_json_result_for_return = {
                "error": error_msg, 
                "raw_output_preview": openai_raw_text[:200], 
                "page_number": page_num_actual
            }
            parsed_data_for_file = openai_json_result_for_return # Save error to file

        # Save the processed data (parsed JSON object/list or error string/dict) to the JSON file
        try:
            with open(output_file_path, "w", encoding="utf-8") as f:
                if isinstance(parsed_data_for_file, (dict, list)):
                    json.dump(parsed_data_for_file, f, indent=2, ensure_ascii=False)
                elif isinstance(parsed_data_for_file, str): # e.g. problematic cleaned string
                    f.write(parsed_data_for_file)
                else: # Fallback if parsed_data_for_file is None or other unexpected type
                    json.dump({"error": "No valid data to save after cleaning/parsing attempts.", 
                            "raw_output_preview": openai_raw_text[:200]}, f, indent=2, ensure_ascii=False)
        except Exception as e_save:
            print(f"⚠️ Error saving processed OpenAI data to file {output_file_path}: {e_save}")
            # openai_json_result_for_return might already be an error dict, or update it
            if "error" not in openai_json_result_for_return:
                openai_json_result_for_return["error"] = openai_json_result_for_return.get("error","") + f"; File save error: {e_save}"


    except json.JSONDecodeError as je: # Should be less common here if parsing is handled above
        if 'openai_layout_start_time' in locals() and metrics.get("time_sec_openai_layout", 0) == 0:
            metrics["time_sec_openai_layout"] = time.time() - openai_layout_start_time
        print(f"⚠️ Outer JSON DECODING ERROR (OpenAI Call, Page {page_num_actual}): {je}")
        metrics.update({"openai_api_status": 500, "openai_error_message": f"JSONDecodeError: {je}"})
        openai_json_result_for_return = {"error": f"Outer JSONDecodeError from OpenAI: {je}", "raw_output": openai_raw_text, "page_number": page_num_actual}
    except Exception as e:
        if 'openai_layout_start_time' in locals() and metrics.get("time_sec_openai_layout", 0) == 0:
            metrics["time_sec_openai_layout"] = time.time() - openai_layout_start_time
        print(f"⚠️ OpenAI API error (Page {page_num_actual}): {e.__class__.__name__} - {e}")
        metrics.update({"openai_api_status": 500, "openai_error_message": f"{e.__class__.__name__}: {e}"})
        openai_json_result_for_return = {"error": f"OpenAI API call failed: {e}", "page_number": page_num_actual}
    
    # Ensure the returned object has a page number if it's a successful dict not containing an error
    if isinstance(openai_json_result_for_return, dict) and "page_number" not in openai_json_result_for_return:
        openai_json_result_for_return["page_number"] = page_num_actual

    return openai_json_result_for_return