import time
import json
import os
import re

from services.gemini_client import call_gemini_api
from utils.prompt_loader import load_text_prompt
from utils.json_utils import _clean_json_string

gemini_layout_prompt_text = load_text_prompt("gemini_layout_prompt.txt")

def _call_gemini_for_layout(
    pdf_chunk_base64: str,
    start_page_actual: int,
    num_pages_in_chunk: int,
    # genai_output_dir: str, # No longer needed for saving here
    metrics: dict
) -> dict: # Return type is the parsed JSON structure or an error dict
    global gemini_layout_prompt_text
    
    gemini_result_for_return = {}
    gemini_raw_text = ""
    gemini_layout_start_time = time.time()
    
    # File saving logic removed from here
    # if num_pages_in_chunk == 1:
    #     output_filename = f"page_{start_page_actual}_genai.json"
    # else:
    #     end_page_actual = start_page_actual + num_pages_in_chunk - 1
    #     output_filename = f"page_{start_page_actual}_{end_page_actual}_genai.json"
    # output_file_path = os.path.join(genai_output_dir, output_filename)

    parsed_data_to_return = None # This will be what the function returns

    try:
        if not gemini_layout_prompt_text or not gemini_layout_prompt_text.strip():
            raise ValueError("Gemini layout prompt text from gemini_layout_prompt.txt is empty or invalid.")
        
        gemini_api_parts = [{"text": gemini_layout_prompt_text}]
        gemini_api_call_response = call_gemini_api(
            image_base64=pdf_chunk_base64,
            prompt_parts=gemini_api_parts,
            mime_type="application/pdf"
        )
        # Ensure time_sec_gemini_layout is recorded even if subsequent parsing fails
        metrics["time_sec_gemini_layout"] = time.time() - gemini_layout_start_time 
        gemini_raw_text = gemini_api_call_response.get("text", "")
        
        metrics.update({
            "gemini_api_status": 200, # Assuming success if no exception from call_gemini_api
            "gemini_input_tokens": gemini_api_call_response.get("input_tokens", 0),
            "gemini_output_tokens": gemini_api_call_response.get("output_tokens", 0),
            "gemini_cost_usd": gemini_api_call_response.get("cost", 0.0)
        })

        cleaned_gemini_json_str = _clean_json_string(gemini_raw_text)
        metrics["gemini_response_length"] = len(cleaned_gemini_json_str or "")
        
        processed_items_list = [] # For aggregating items if the response is a list or needs restructuring

        if cleaned_gemini_json_str:
            try:
                parsed_data = json.loads(cleaned_gemini_json_str)

                if isinstance(parsed_data, list):
                    for node in parsed_data:
                        if isinstance(node, dict):
                            page_offset = node.get("page_index_in_chunk")
                            if page_offset is not None and isinstance(page_offset, int) and 0 <= page_offset < num_pages_in_chunk:
                                node["page_number"] = start_page_actual + page_offset
                            elif num_pages_in_chunk == 1: # If single page chunk, assign to start_page_actual
                                node["page_number"] = start_page_actual
                            else: # Fallback if page_index_in_chunk is missing/invalid in multi-page
                                node["page_number"] = start_page_actual 
                                node["page_number_assignment_warning"] = f"Node lacks valid 'page_index_in_chunk'. Defaulted page_number to {start_page_actual} (first page of chunk)."
                        processed_items_list.append(node)
                    
                    # Structure for return: a dictionary containing the list of items
                    parsed_data_to_return = {
                        "items": processed_items_list,
                        "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1],
                        "_source_data_type": "list"
                    }
                    print(f"✅ Parsed Gemini list response for chunk pages {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}.")

                elif isinstance(parsed_data, dict):
                    
                    def get_page_offset_from_parsed_key(key_str: str, max_offset_exclusive: int) -> int | None:
                        key_lower = key_str.lower()
                        match_text_num = re.fullmatch(r"(?:page|pg)?[_ ]*(\d+)", key_lower)
                        if match_text_num:
                            num_in_key = int(match_text_num.group(1))
                            offset = -1
                            if key_str.isdigit(): 
                                offset = num_in_key
                            elif "page" in key_lower or "pg" in key_lower : 
                                offset = num_in_key - 1 
                            if 0 <= offset < max_offset_exclusive:
                                return offset
                        return None

                    page_structured_items_for_return = {} 
                    unassigned_dict_keys_values = {} 
                    found_structured_page_data_in_dict = False

                    for key, value_list in parsed_data.items():
                        page_offset = get_page_offset_from_parsed_key(key, num_pages_in_chunk)
                        if page_offset is not None and isinstance(value_list, list):
                            found_structured_page_data_in_dict = True
                            actual_page_num_for_nodes = start_page_actual + page_offset
                            
                            for node in value_list: # Assign page_number to each node
                                if isinstance(node, dict):
                                    node["page_number"] = actual_page_num_for_nodes
                            # For the return structure, ensure consistent PageX keys
                            page_structured_items_for_return[f"Page{actual_page_num_for_nodes}"] = value_list
                        else:
                            unassigned_dict_keys_values[key] = value_list
                    
                    if found_structured_page_data_in_dict:
                        # The main structure is the dict with PageX keys
                        parsed_data_to_return = {**page_structured_items_for_return, **unassigned_dict_keys_values}
                        # Add metadata for clarity if needed by consuming function
                        if "_metadata_from_gemini_parser" not in parsed_data_to_return: # Avoid overwriting
                           parsed_data_to_return["_metadata_from_gemini_parser"] = {
                                "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1],
                                "_source_data_type": "page_structured_dict_recognized"
                           }
                        print(f"✅ Parsed recognized page-structured dict from Gemini for chunk {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}.")

                    else: # Not a recognized page-structured dict; it's a single dictionary.
                        if "page_number" not in parsed_data: # Assign page number if not present
                            parsed_data["page_number"] = start_page_actual
                        
                        items_in_dict = parsed_data.get("items")
                        if isinstance(items_in_dict, list): # If it's a dict containing an 'items' list
                            for node in items_in_dict:
                                if isinstance(node, dict) and "page_number" not in node:
                                    node["page_number"] = start_page_actual
                            # Return structure mirrors the "list" case but indicates it came from a dict
                            parsed_data_to_return = {
                                "items": items_in_dict, 
                                "page_number": start_page_actual, # page_number of the dict itself
                                "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1], # chunk context
                                "_source_data_type": "single_dict_with_items"
                            }
                        else: # No 'items' list, the dict itself is the content
                            parsed_data_to_return = parsed_data # This is the dict itself
                            if "_metadata_from_gemini_parser" not in parsed_data_to_return:
                                parsed_data_to_return["_metadata_from_gemini_parser"] = {
                                    "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1],
                                    "_source_data_type": "single_node_dict" if num_pages_in_chunk == 1 else "ambiguous_single_dict_multi_page"
                                }


                        if num_pages_in_chunk == 1:
                            print(f"✅ Parsed Gemini dict response for single page {start_page_actual}.")
                        else: # Multi-page chunk but received a single, non-page-structured dict
                            error_msg = "Gemini returned a single dictionary for a multi-page chunk without recognized page structure."
                            print(f"⚠️ {error_msg} Chunk: {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}.")
                            if isinstance(parsed_data_to_return, dict): # Add error to the dict
                                parsed_data_to_return["error_ambiguous_multi_page_dict"] = error_msg
                                # Ensure items within get page number, default to first page of chunk.
                                page_items = parsed_data_to_return.get("items") if isinstance(parsed_data_to_return.get("items"),list) else ([parsed_data_to_return] if not parsed_data_to_return.get("items") else [])
                                for item_node in page_items:
                                    if isinstance(item_node, dict):
                                        item_node["page_number"] = start_page_actual
                                        item_node["page_number_assignment_warning"] = "Part of ambiguous multi-page dict. Defaulted to first page of chunk."

                else: 
                    error_msg = f"Gemini response parsed to an unexpected data type: {type(parsed_data).__name__}"
                    print(f"⚠️ {error_msg} for chunk pages {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}.")
                    parsed_data_to_return = { # Error structure
                        "error": error_msg,
                        "parsed_data_type": type(parsed_data).__name__,
                        "raw_output_preview": gemini_raw_text[:200],
                        "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]
                    }
            
            except json.JSONDecodeError as je:
                print(f"⚠️ JSON DECODING ERROR on cleaned string (Chunk: {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}): {je}")
                error_msg = f"JSONDecodeError on cleaned string: {je}"
                parsed_data_to_return = { # Error structure
                    "error": error_msg,
                    "cleaned_string_preview": cleaned_gemini_json_str[:200] if cleaned_gemini_json_str else "None",
                    "raw_output_preview": gemini_raw_text[:200],
                    "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]
                }
        else: 
            error_msg = "Could not extract/clean valid JSON string from Gemini response."
            if not gemini_raw_text: error_msg = "Empty raw response from Gemini."
            print(f"⚠️ {error_msg} for chunk pages {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}.")
            parsed_data_to_return = { # Error structure
                "error": error_msg,
                "raw_output_preview": gemini_raw_text[:200],
                "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]
            }

        # File saving block removed
        # try:
        #     with open(output_file_path, "w", encoding="utf-8") as f:
        #         # ... (saving logic) ...
        # except Exception as e_save:
        #     # ... (error handling for saving) ...

    except json.JSONDecodeError as je: 
        if metrics.get("time_sec_gemini_layout", 0) == 0: metrics["time_sec_gemini_layout"] = time.time() - gemini_layout_start_time
        print(f"⚠️ Outer JSON DECODING ERROR (Chunk: {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}): {je}")
        metrics.update({"gemini_api_status": 500, "gemini_error_message": f"Outer JSONDecodeError: {je}"}) # Assuming 500 for server-side like error
        parsed_data_to_return = {"error": f"Outer JSONDecodeError: {je}", "raw_output": gemini_raw_text, 
                                    "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]}
    except ValueError as ve: 
        if metrics.get("time_sec_gemini_layout", 0) == 0: metrics["time_sec_gemini_layout"] = time.time() - gemini_layout_start_time
        print(f"⚠️ Value Error (Gemini Call construction, Chunk: {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}): {ve}")
        metrics.update({"gemini_api_status": 400, "gemini_error_message": str(ve)}) # Assuming 400 for client-side error
        parsed_data_to_return = {"error": f"Gemini prompt/construction error: {ve}", 
                                    "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]}
    except Exception as e: 
        if metrics.get("time_sec_gemini_layout", 0) == 0: metrics["time_sec_gemini_layout"] = time.time() - gemini_layout_start_time
        print(f"⚠️ Gemini API error (Chunk: {start_page_actual}-{start_page_actual + num_pages_in_chunk - 1}): {e.__class__.__name__} - {e}")
        metrics.update({"gemini_api_status": 500, "gemini_error_message": f"{e.__class__.__name__}: {e}"}) # Assuming 500 for general API errors
        parsed_data_to_return = {"error": f"Gemini API call failed: {e}", 
                                    "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]}

    # Ensure a dictionary is always returned, even if it's just an error dict.
    if parsed_data_to_return is None: # Should not happen if logic above is complete
        parsed_data_to_return = {"error": "Gemini processing failed to produce a returnable structure.",
                                 "processed_chunk_page_range": [start_page_actual, start_page_actual + num_pages_in_chunk - 1]}
    
    # Add processed_chunk_page_range if not already part of the main dict (e.g. if it was just a list)
    if isinstance(parsed_data_to_return, dict) and "processed_chunk_page_range" not in parsed_data_to_return:
         # This might occur if parsed_data_to_return became a simple dict like a single node
         # For safety, ensure it's there. The primary structures (list-wrapper, PageN dicts, error dicts) should have it.
        metadata = parsed_data_to_return.get("_metadata_from_gemini_parser", {})
        if "processed_chunk_page_range" not in metadata:
            parsed_data_to_return["processed_chunk_page_range"] = [start_page_actual, start_page_actual + num_pages_in_chunk - 1]

    return parsed_data_to_return