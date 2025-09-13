import time
import json
import os

from services.gemini_client import call_gemini_api
from utils.json_utils import _clean_json_string, attach_page_number_tag
from services.openai_client import call_openai_with_json, call_openai_with_pdf
from utils.prompt_loader import load_text_prompt


# Load prompt templates
consolidation_prompt_text = load_text_prompt("consolidation_prompt.txt")
sanitize_prompt_text = load_text_prompt("sanitize_prompt.txt")
output_verification_prompt_text = load_text_prompt("output_verification_prompt.txt")

def consolidate_responses(pdf_page_base64: str, gemini_json_input: dict, openai_json_input: dict, prompt_text: str) -> dict:
    """
    Consolidates responses from Gemini and OpenAI using Gemini API.

    Args:
        pdf_page_base64: Base64 string of the PDF page (single page only).
        gemini_json_input: JSON response from Gemini layout model.
        openai_json_input: JSON response from OpenAI layout model.
        prompt_text: Custom prompt text guiding how to consolidate both JSONs.

    Returns:
        dict: Consolidated JSON or fallback error response.
    """

    try:
        # Build the complete prompt for Gemini API
        consolidation_api_prompt_parts = [
            {"text": prompt_text},
            {"text": "You are given two JSON outputs from two different layout extraction models (Gemini and OpenAI)."},
            {"text": "Your job is to consolidate these two JSONs into a single, accurate representation of the page layout and content."},
            {"text": "Strictly follow a valid JSON structure and ensure no content is duplicated or lost."},
            {"text": f"Gemini JSON Input:\n{json.dumps(gemini_json_input, indent=2)}"},
            {"text": f"OpenAI JSON Input:\n{json.dumps(openai_json_input, indent=2)}"}
        ]

        # Call Gemini API with PDF base64 and constructed prompt
        gemini_response = call_gemini_api(
            image_base64=pdf_page_base64,
            prompt_parts=consolidation_api_prompt_parts,
            mime_type="application/pdf"
        )

        result_text = gemini_response.get("text", "")
        
        # MODIFIED LINE: Call placeholder_extract_json_string
        cleaned_json_str = _clean_json_string(result_text)

        if cleaned_json_str:
            try:
                parsed_result = json.loads(cleaned_json_str)
                # Ensure parsed_result is a dictionary before adding keys,
                # though json.loads on a valid JSON object/array string should produce dict/list.
                if not isinstance(parsed_result, dict):
                    # If the top-level JSON is an array or other type, wrap it or handle as error
                    # For now, assuming the expected consolidated output is a dictionary.
                    # If it can be other types, this logic might need adjustment.
                    return {
                        "error": "Consolidated JSON is not a dictionary as expected.",
                        "parsed_content_type": type(parsed_result).__name__,
                        "raw_extracted_json": cleaned_json_str,
                        "gemini_original": gemini_json_input,
                        "openai_original": openai_json_input,
                        "verification_status_internal": "Fallback_ConsolidatedJsonNotDict",
                        "raw_consolidation_output": result_text
                    }

                parsed_result["_consolidation_input_tokens"] = gemini_response.get("input_tokens", 0)
                parsed_result["_consolidation_output_tokens"] = gemini_response.get("output_tokens", 0)
                parsed_result["_consolidation_cost_usd"] = gemini_response.get("cost", 0.0)
                return parsed_result
            except json.JSONDecodeError as je: # Catch error if cleaned_json_str is not valid JSON
                return {
                    "error": "Failed to parse extracted JSON string during consolidation",
                    "error_details": str(je),
                    "extracted_string_preview": cleaned_json_str[:200] if cleaned_json_str else "None",
                    "gemini_original": gemini_json_input,
                    "openai_original": openai_json_input,
                    "verification_status_internal": "Fallback_JsonDecodeErrorOnExtractedString",
                    "raw_consolidation_output": result_text
                }
        else:
            # This block executes if placeholder_extract_json_string returns None (no JSON found)
            return {
                "error": "Consolidation failed to produce a valid JSON string (extraction failed).",
                "gemini_original": gemini_json_input,
                "openai_original": openai_json_input,
                "verification_status_internal": "Fallback_NoValidJsonStringFromConsolidation",
                "raw_consolidation_output": result_text
            }

    except json.JSONDecodeError as je: # This would typically be for issues with json.dumps earlier
        return {
            "gemini_original": gemini_json_input,
            "openai_original": openai_json_input,
            "verification_status_internal": "Fallback_JsonErrorInPromptConstruction", # More specific
            "error_details": str(je)
        }

    except Exception as e:
        return {
            "gemini_original": gemini_json_input,
            "openai_original": openai_json_input,
            "verification_status_internal": "Fallback_ExceptionInConsolidation",
            "error_details": str(e)
        }
        
def _sanitize_response(
    consolidated_output_path: str,
    sanitize_prompt_text_val: str, # Renamed to avoid conflict with global
    page_num_actual: int,
    genai_output_dir: str,
    metrics: dict
) -> tuple[dict | list, str]: # Returns (processed_data, sanitized_output_file_path)
    """
    Performs the sanitization step on the consolidated JSON.
    Returns the processed sanitized data (list or dict, or error dict) and its file path.
    """
    start_time_sanitize = time.time()
    sanitize_api_response = {}
    processed_data = {} # Default to an error dict if things go wrong early
    sanitized_output_path = os.path.join(genai_output_dir, f"page_{page_num_actual}_sanitized.json")
    raw_sanitized_text_output = ""

    try:
        sanitize_api_response = call_openai_with_json(consolidated_output_path, sanitize_prompt_text_val)
        metrics["time_sec_sanitize"] = time.time() - start_time_sanitize
        raw_sanitized_text_output = sanitize_api_response.get("text", "")
        
        extracted_sanitized_json_string = _clean_json_string(raw_sanitized_text_output)

        if extracted_sanitized_json_string:
            try:
                parsed_data = json.loads(extracted_sanitized_json_string)

                if isinstance(parsed_data, (list, dict)):
                    processed_data = parsed_data
                    metrics["sanitize_status"] = "success"
                    print(f"✅ Sanitized content parsed for page {page_num_actual}")
                    #print(f"✅ Sanitized content ({type(parsed_data).__name__}) parsed for page {page_num_actual}.")
                else:
                    error_msg = "Sanitized content parsed to an unexpected basic type (not list or dict)"
                    metrics["sanitize_status"] = f"fail - {error_msg}"
                    processed_data = {
                        "error": error_msg, "parsed_type": type(parsed_data).__name__,
                        "extracted_string_preview": extracted_sanitized_json_string[:200]
                    }
                    print(f"⚠️ {error_msg} for page {page_num_actual}.")
            except json.JSONDecodeError as je:
                metrics["sanitize_status"] = f"fail - JSONDecodeError: {je}"
                processed_data = {
                    "error": "Failed to parse extracted sanitized JSON string", "exception": str(je),
                    "extracted_string_preview": extracted_sanitized_json_string[:200],
                    "raw_output_preview": raw_sanitized_text_output[:200]
                }
                print(f"⚠️ Failed to parse extracted sanitized JSON for page {page_num_actual}: {je}")
        else:
            metrics["sanitize_status"] = "fail - no JSON content found by _clean_json_string"
            processed_data = {
                "error": "No JSON content found in sanitized response by _clean_json_string",
                "raw_output_preview": raw_sanitized_text_output[:200]
            }
            print(f"⚠️ No JSON content found in sanitized response for page {page_num_actual}.")

        metrics.update({
            "sanitize_input_tokens": sanitize_api_response.get("input_tokens", 0),
            "sanitize_output_tokens": sanitize_api_response.get("output_tokens", 0),
            "sanitize_cost_usd": sanitize_api_response.get("cost", 0.0),
        })

    except Exception as e_sanitize_api:
        if 'start_time_sanitize' in locals() and metrics.get("time_sec_sanitize", 0.0) == 0.0:
            metrics["time_sec_sanitize"] = time.time() - start_time_sanitize
        metrics["sanitize_status"] = f"fail - API call error: {str(e_sanitize_api)}"
        processed_data = {"error": f"Sanitization API call failed: {e_sanitize_api}"}
        print(f"⚠️ Sanitization API call failed for page {page_num_actual}: {e_sanitize_api}")
    
    # Save the processed_data (which is the parsed list/dict or an error dict)
    try:
        with open(sanitized_output_path, "w", encoding="utf-8") as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
        print(f"✅ Processed sanitized data for page {page_num_actual}")
        #print(f"✅ Processed sanitized data for page {page_num_actual} saved to {sanitized_output_path}.")
    except Exception as e_save:
        print(f"⚠️ Error saving sanitized data to file {sanitized_output_path}: {e_save}")
        if isinstance(processed_data, dict) and "error" not in processed_data: # Avoid overwriting primary error
            processed_data["error_saving_file"] = str(e_save)


    return processed_data, sanitized_output_path


def _verify_response(
    data_to_verify: dict | list, # This is the processed_sanitized_data
    sanitize_status: str,
    temp_pdf_page_path: str,
    verification_prompt_text_val: str, # Renamed to avoid conflict
    page_num_actual: int,
    metrics: dict
) -> str: # Returns verification_status
    """Performs the verification step on the sanitized data."""
    current_verification_status = "verification_not_run"
    start_time_verification = time.time() # Initialize in case of early exit

    # Determine if we can proceed with verification
    can_verify = False
    if sanitize_status == "success":
        if isinstance(data_to_verify, list): # A list from sanitization is considered verifiable
            can_verify = True
        elif isinstance(data_to_verify, dict) and not data_to_verify.get("error"): # A dict without an error key is verifiable
            can_verify = True

    if can_verify:
        try:
            # Ensure data_to_verify (if dict) has page_number for the prompt,
            # or rely on page_num_actual if it's a list.
            # The prompt construction itself will handle dict or list appropriately with json.dumps.
            prompt_content_for_verification = data_to_verify
            if isinstance(prompt_content_for_verification, dict):
                prompt_content_for_verification.setdefault("page_number", page_num_actual)

            verification_prompt = (
                f"{verification_prompt_text_val}\n\n"
                f"Sanitized JSON to verify for page {page_num_actual}:\n"
                f"{json.dumps(prompt_content_for_verification, indent=2)}"
            )
            verification_api_response = call_openai_with_pdf(temp_pdf_page_path, verification_prompt)
            metrics["time_sec_verification"] = time.time() - start_time_verification
            
            raw_verification_text_output = verification_api_response.get("text", "")
            # Using _clean_json_string to attempt to get JSON from verification output
            extracted_verification_json_string = _clean_json_string(raw_verification_text_output)
            parsed_verification_output = None
            
            if extracted_verification_json_string:
                try:
                    parsed_verification_output = json.loads(extracted_verification_json_string)
                except json.JSONDecodeError:
                    print(f"ℹ️ Verification response for page {page_num_actual} had JSON-like string that failed to parse: {extracted_verification_json_string[:100]}...")
            
            status_found_in_json = False
            if isinstance(parsed_verification_output, dict) and "status" in parsed_verification_output:
                status_text = str(parsed_verification_output.get("status", "")).lower()
                if "pass" in status_text: current_verification_status = "pass"; status_found_in_json = True
                elif "fail" in status_text: current_verification_status = "fail"; status_found_in_json = True
            
            if not status_found_in_json: # Fallback to checking raw text
                raw_verification_text_lower = raw_verification_text_output.lower()
                if "pass" in raw_verification_text_lower: current_verification_status = "pass"
                elif "fail" in raw_verification_text_lower: current_verification_status = "fail"
                else: current_verification_status = f"fail - unclear: {raw_verification_text_output[:100].strip()}"
            
            metrics.update({
                "verification_input_tokens": verification_api_response.get("input_tokens", 0),
                "verification_output_tokens": verification_api_response.get("output_tokens", 0),
                "verification_cost_usd": verification_api_response.get("cost", 0.0)
            })
            print(f"✅ Verification status for page {page_num_actual}: {current_verification_status}")
        except Exception as e_verify_api:
            if metrics.get("time_sec_verification", 0.0) == 0.0 : metrics["time_sec_verification"] = time.time() - start_time_verification
            current_verification_status = f"fail - verification API error: {str(e_verify_api)}"
            print(f"⚠️ Verification API error for page {page_num_actual}: {e_verify_api}")
    else:
        error_reason = "Unknown sanitization issue"
        if isinstance(data_to_verify, dict) and data_to_verify.get("error"):
            error_reason = data_to_verify.get("error")
        elif sanitize_status != "success":
            error_reason = sanitize_status
        
        current_verification_status = f"skipped - sanitize outcome: {error_reason}"
        metrics["time_sec_verification"] = 0.0 # No time spent if skipped
        print(f"⚠️ Verification skipped for page {page_num_actual} due to sanitization issues: {error_reason}")

    metrics["verification_status"] = current_verification_status
    return current_verification_status

def _orchestrate_page_processing( # Renamed from _consolidate_sanitize_verify
    pdf_page_base64: str,
    gemini_json: dict,
    openai_json: dict,
    page_num_actual: int,
    genai_output_dir: str,
    temp_pdf_page_path: str,
    metrics: dict
) -> dict:
    """Orchestrates consolidation, sanitization, and verification for a page."""

    final_page_output = {} # This will be the dictionary returned

    try:
        # --- 1. Consolidation Step ---
        start_time_consolidation = time.time()
        consolidated_data = consolidate_responses( # Returns a dict
            pdf_page_base64, gemini_json, openai_json, consolidation_prompt_text
        )
        metrics["time_sec_consolidation"] = time.time() - start_time_consolidation

        if not isinstance(consolidated_data, dict): # Ensure it's a dict
            consolidated_data = {"error": "Consolidation did not return a dictionary", 
                                "raw_output": str(consolidated_data)}
        consolidated_data = attach_page_number_tag(consolidated_data, page_num_actual)

        metrics.update({
            "genai_response_consolidation_status": 200 if not consolidated_data.get("error") else 500,
            "genai_response_consolidation_response_length": len(json.dumps(consolidated_data)),
            "json_consolidation_error_message": consolidated_data.get("error_details", consolidated_data.get("error", "")),
            "consolidation_input_tokens": consolidated_data.pop("_consolidation_input_tokens", 0),
            "consolidation_output_tokens": consolidated_data.pop("_consolidation_output_tokens", 0),
            "consolidation_cost_usd": consolidated_data.pop("_consolidation_cost_usd", 0.0)
        })
        consolidated_output_path = os.path.join(genai_output_dir, f"page_{page_num_actual}_consolidated.json")
        with open(consolidated_output_path, "w", encoding="utf-8") as f:
            json.dump(consolidated_data, f, indent=2, ensure_ascii=False)
        print(f"✅ Consolidated JSON saved for page {page_num_actual}")

        if consolidated_data.get("error"):
            print(f"⚠️ Consolidation failed for page {page_num_actual}. Aborting further processing for this page.")
            metrics["sanitize_status"] = "skipped_due_to_consolidation_error"
            metrics["verification_status"] = "skipped_due_to_consolidation_error"
            final_page_output = consolidated_data # Return the consolidation error
            final_page_output["page_verification_status"] = metrics["verification_status"]
            return final_page_output

        # --- 2. Sanitization Step ---
        # _sanitize_response returns (processed_data_dict_or_list, saved_file_path)
        # metrics are updated internally by _sanitize_response
        sanitized_data, _ = _sanitize_response(
            consolidated_output_path, sanitize_prompt_text, page_num_actual, genai_output_dir, metrics
        )

        # --- 3. Verification Step ---
        # metrics are updated internally by _verify_response
        verification_status = _verify_response(
            sanitized_data, metrics.get("sanitize_status"), temp_pdf_page_path, 
            output_verification_prompt_text, page_num_actual, metrics
        )
        
        # Prepare the final return object based on sanitized_data, adding verification status
        if isinstance(sanitized_data, list):
            final_page_output = {
                "page_elements": sanitized_data, # Or your preferred key for list data
                "page_number": page_num_actual,
                "page_verification_status": verification_status
            }
        elif isinstance(sanitized_data, dict):
            final_page_output = sanitized_data
            final_page_output.setdefault("page_number", page_num_actual)
            final_page_output["page_verification_status"] = verification_status
        else: # Should be an error dict if not list/dict from _sanitize_response
            final_page_output = {
                "error": "Sanitized data was of unexpected type after processing",
                "data_type": type(sanitized_data).__name__,
                "page_number": page_num_actual,
                "page_verification_status": verification_status
            }
        
        return final_page_output

    except Exception as e_block:
        print(f"⚠️ Critical Error in page processing orchestration for page {page_num_actual}: {e_block}")
        metrics.update({ # Ensure all statuses reflect the block error if not already set
            "genai_response_consolidation_status": metrics.get("genai_response_consolidation_status", 500),
            "sanitize_status": metrics.get("sanitize_status", "fail - orchestration_block_error"),
            "verification_status": "fail - orchestration_block_error"
        })
        error_payload = {
            "error": "Critical failure in page processing orchestration",
            "details": str(e_block), "page_number": page_num_actual,
            "page_verification_status": metrics["verification_status"],
            "gemini_original_available": bool(gemini_json),
            "openai_original_available": bool(openai_json),
            "consolidation_attempt_data": consolidated_data if 'consolidated_data' in locals() else {"error": "Consolidation not reached or failed early"}
        }
        return error_payload
