import os
import sys
import time
from pathlib import Path
import fitz  # PyMuPDF
import json
import copy # For deep copying

# Add project root to PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
# Adjust this path if handler1.py is not two levels deep from project root
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Local imports
try:
    from utils.file_utils import encode_pdf_to_base64
    from utils.pdf_utils import _create_temp_chunk_pdf
    from utils.metrics_utils import _initialize_page_metrics #
    from utils.pdf_text_extractor import (
        extract_text_from_pdf_chunk_pypdf2,
        extract_text_from_chunk_ocr,
        extract_text_and_links_from_chunk_fitz
    )
    from utils.file_converters import convert_book_json_to_html
    from utils import text_utils
    from core.layout_gemini import _call_gemini_for_layout #
    from core.finalizer import _save_results # This will be the modified finalizer
except ImportError as e:
    print(f"Error importing utility modules: {e}. Ensure PYTHONPATH is set correctly or modules are in the right location.")
    sys.exit(1)


def _extract_page_data_from_gemini_chunk_output(gemini_chunk_output: dict, target_actual_page_num: int) -> list:
    """
    Extracts the list of content items for a specific page from the 
    potentially complex structure returned by _call_gemini_for_layout.
    """
    if not isinstance(gemini_chunk_output, dict):
        return [{"error": f"Gemini output for chunk was not a dict for page {target_actual_page_num}"}]

    # Case 1: Output is a dictionary with "PageX" keys
    page_key = f"Page{target_actual_page_num}"
    if page_key in gemini_chunk_output and isinstance(gemini_chunk_output[page_key], list):
        return copy.deepcopy(gemini_chunk_output[page_key])
    
    # Case 2: Output is a dictionary with an "items" list (Gemini parsed a list response)
    # and items have "page_number"
    if "items" in gemini_chunk_output and isinstance(gemini_chunk_output["items"], list):
        page_items = []
        for item in gemini_chunk_output["items"]:
            if isinstance(item, dict) and item.get("page_number") == target_actual_page_num:
                page_items.append(copy.deepcopy(item))
        if page_items: # Found items specifically for this page
            return page_items
        # If items exist but none match target_actual_page_num, this might indicate an issue or
        # that the page wasn't specifically detailed in a list output.
        # However, _call_gemini_for_layout should assign page_number.

    # Case 3: Output is a single dictionary representing a single page's content (for single-page chunks mainly)
    # or an ambiguous dict that _call_gemini_for_layout might have tagged.
    if gemini_chunk_output.get("page_number") == target_actual_page_num and "items" not in gemini_chunk_output and not any(k.startswith("Page") for k in gemini_chunk_output.keys()):
         # This implies the entire dict might be for this page, or it's a single item.
         # We expect a list of items for a page.
        return [copy.deepcopy(gemini_chunk_output)]

    # Case 4: If it's an error structure from Gemini call
    if "error" in gemini_chunk_output:
        return [copy.deepcopy(gemini_chunk_output)] # Propagate the error structure

    # Fallback or if page not found in a structured way (should be rare if _call_gemini_for_layout works as expected)
    # This might mean the chunk didn't yield specific data for this page number,
    # or the structure is unexpected.
    print(f"⚠️ Could not directly extract page {target_actual_page_num} data from Gemini chunk output structure. Output keys: {list(gemini_chunk_output.keys())}")
    return [{"error": f"Could not isolate page {target_actual_page_num} data from Gemini chunk"}]


# New function to handle processing for a single page after Gemini data is available
def _finalize_single_page_processing(
    actual_page_num: int,
    genai_content_for_this_page: list, # This is the selected Gemini output for THIS page
    pypdf2_text_for_page: str,
    ocr_text_for_page: str,
    fitz_page_text_content: str,
    hyperlinks_from_fitz_content: list,
    page_metrics: dict, # The pre-initialized metrics object for this page
    genai_output_dir_path: str,
    fuzzy_match_thresh: int,
    min_direct_text_len: int, # For fallback logic (PyPDF2/OCR)
    min_fitz_text_len: int,   # For selecting verification text
    min_content_len_for_fuzzy: int
):
    """
    Processes a single page's data: fallback text, verification, hyperlink matching, saving outputs.
    This largely contains the per-page logic from the original _process_page_chunk's inner loop.
    """
    page_processing_start_time_specific = time.time() # For this specific finalization

    original_genai_filename = f"page_{actual_page_num}_genai.json"
    final_content_filename = f"page_{actual_page_num}_final_content.json"

    # Update metrics with extraction status (assuming these are passed in or set before calling)
    # page_metrics["pypdf2_status"], page_metrics["pypdf2_char_count"] = ... (set these based on prior extraction)
    # page_metrics["ocr_status"], page_metrics["ocr_char_count"] = ...
    # page_metrics["fitz_extraction_status"], page_metrics["fitz_text_char_count"], page_metrics["fitz_link_count"] = ...

    page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"], page_metrics["fallback_text_char_count"] = "none", "not_attempted", 0 #
    page_metrics["verification_text_source"], page_metrics["content_verification_status"] = "none", "not_attempted" #
    
    chosen_fallback_text_for_page = ""

    # --- Save Fitz crawl data (already extracted before this function) ---
    page_crawl_json_filename = f"page_{actual_page_num}_crawl.json"
    page_crawl_json_filepath = os.path.join(genai_output_dir_path, page_crawl_json_filename)
    page_fitz_content_to_save = {"fitz_extracted_text": fitz_page_text_content, "extracted_hyperlinks": hyperlinks_from_fitz_content}
    try:
        with open(page_crawl_json_filepath, "w", encoding="utf-8") as crawl_file: json.dump(page_fitz_content_to_save, crawl_file, indent=2, ensure_ascii=False)
    except Exception as e_crawl_save: print(f"⚠️ Error writing Fitz crawl data to {page_crawl_json_filepath}: {e_crawl_save}"); page_metrics["error_saving_crawl_json"] = str(e_crawl_save)

    # --- Fallback Text Logic (using pre-extracted PyPDF2 and OCR text) ---
    pypdf2_sufficient_for_fallback = False
    # Assuming pypdf2_text_for_page is not None and its status is known
    if pypdf2_text_for_page and len(pypdf2_text_for_page) > min_direct_text_len:
        chosen_fallback_text_for_page = pypdf2_text_for_page
        page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"], pypdf2_sufficient_for_fallback = "direct_pypdf2", "success_sufficient", True
    elif pypdf2_text_for_page:
        chosen_fallback_text_for_page = pypdf2_text_for_page
        page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"] = "direct_pypdf2", "success_insufficient_length"
    else: # PyPDF2 empty or failed
        page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"] = "direct_pypdf2", page_metrics.get("pypdf2_status", "unknown_extraction_issue")


    if not pypdf2_sufficient_for_fallback:
        # Assuming ocr_text_for_page is not None and its status is known
        if ocr_text_for_page and len(ocr_text_for_page) > min_direct_text_len:
            chosen_fallback_text_for_page = ocr_text_for_page # OCR takes precedence if PyPDF2 insufficient
            page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"] = "ocr_fallback", "success_sufficient"
        elif ocr_text_for_page:
            if not chosen_fallback_text_for_page: chosen_fallback_text_for_page = ocr_text_for_page
            page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"] = "ocr_fallback", "success_insufficient_length"
        elif not chosen_fallback_text_for_page :
             page_metrics["fallback_text_method_used"], page_metrics["fallback_text_status"] = "ocr_fallback", "both_pypdf2_and_ocr_empty" # or map based on actual statuses
        # else: (ocr failed) - status should reflect this if ocr_status was 'extraction_failed'
    page_metrics["fallback_text_char_count"] = len(chosen_fallback_text_for_page) #

    page_crawl_fallback_filename = f"page_{actual_page_num}_crawl_fallback.txt"; page_crawl_fallback_filepath = os.path.join(genai_output_dir_path, page_crawl_fallback_filename)
    try:
        with open(page_crawl_fallback_filepath, "w", encoding="utf-8") as fb_file: fb_file.write(f"--- Page {actual_page_num} ---\n"); fb_file.write(chosen_fallback_text_for_page + "\n\n")
    except Exception as e_save_fallback: print(f"⚠️ Error saving fallback text to {page_crawl_fallback_filepath}: {e_save_fallback}"); page_metrics["error_saving_fallback_txt"] = str(e_save_fallback)

    # --- Text for Content Verification ---
    text_for_content_verification = ""
    if page_metrics.get("fitz_extraction_status") == "success" and fitz_page_text_content and len(fitz_page_text_content) >= min_fitz_text_len:
        text_for_content_verification = fitz_page_text_content
        page_metrics["verification_text_source"] = "fitz" #
    elif chosen_fallback_text_for_page:
        text_for_content_verification = chosen_fallback_text_for_page
        page_metrics["verification_text_source"] = page_metrics["fallback_text_method_used"] #
    else:
        page_metrics["verification_text_source"] = "none_available" #

    # --- Save Original GenAI output for this page ---
    # The genai_content_for_this_page is already a deep copy.
    # Ensure it's a list as expected by downstream consumers of this file.
    final_genai_content_for_this_page_file = genai_content_for_this_page if isinstance(genai_content_for_this_page, list) else [genai_content_for_this_page]
    
    original_genai_filepath = os.path.join(genai_output_dir_path, original_genai_filename)
    try:
        with open(original_genai_filepath, "w", encoding="utf-8") as f_orig_genai: json.dump(final_genai_content_for_this_page_file, f_orig_genai, indent=2, ensure_ascii=False)
    except Exception as e_save_orig_genai: print(f"⚠️ Error saving original Gemini data to {original_genai_filepath}: {e_save_orig_genai}"); page_metrics["error_saving_original_genai_json"] = str(e_save_orig_genai)

    # --- Content Verification (on a deep copy of the GenAI content) ---
    # Make another deepcopy for modifications during verification and hyperlink matching
    content_to_verify_and_finalize = copy.deepcopy(final_genai_content_for_this_page_file)

    if isinstance(content_to_verify_and_finalize, list) and content_to_verify_and_finalize:
        # Skip verification if the content is just an error placeholder
        if not (len(content_to_verify_and_finalize) == 1 and isinstance(content_to_verify_and_finalize[0], dict) and "error" in content_to_verify_and_finalize[0]):
            text_utils._verify_item_content_in_direct_text_fuzzy(
                page_data_dict=content_to_verify_and_finalize, direct_text=text_for_content_verification,
                page_num=actual_page_num, fuzzy_threshold=fuzzy_match_thresh, min_content_len_for_fuzzy=min_content_len_for_fuzzy)
        page_metrics["content_verification_status"] = "fuzzy_attempted" # Or more granular based on outcome #
    elif isinstance(content_to_verify_and_finalize, list) and not content_to_verify_and_finalize:
        page_metrics["content_verification_status"] = "skipped_empty_genai_content" #
    else:
        page_metrics["content_verification_status"] = "skipped_genai_content_not_a_list_or_unexpected_type" #


    # --- Hyperlink Matching ---
    if isinstance(content_to_verify_and_finalize, list) and content_to_verify_and_finalize and hyperlinks_from_fitz_content:
        # Skip if the content is just an error placeholder
        if not (len(content_to_verify_and_finalize) == 1 and isinstance(content_to_verify_and_finalize[0], dict) and "error" in content_to_verify_and_finalize[0]):
            for genai_item in content_to_verify_and_finalize:
                if isinstance(genai_item, dict) and "content" in genai_item:
                    item_content_value = genai_item.get("content")
                    if item_content_value and isinstance(item_content_value, str):
                        normalized_item_content = text_utils._normalize_text(item_content_value)
                        item_matched_hyperlinks = []
                        for hyperlink in hyperlinks_from_fitz_content: # These are for the current page
                            hyperlink_text = hyperlink.get("text")
                            if hyperlink_text and isinstance(hyperlink_text, str):
                                normalized_hyperlink_text = text_utils._normalize_text(hyperlink_text)
                                if normalized_hyperlink_text and normalized_hyperlink_text in normalized_item_content:
                                    matched_link_info = {k: v for k, v in hyperlink.items() if k != 'rect'}
                                    item_matched_hyperlinks.append(matched_link_info)
                        if item_matched_hyperlinks:
                            genai_item["hyperlinks"] = item_matched_hyperlinks
    
    # Post-process: Remove page_number from individual items (should be done in _call_gemini_for_layout or _extract_page...)
    # but double check here. Also ensure correct hyperlink key name.
    if isinstance(content_to_verify_and_finalize, list):
        for genai_item in content_to_verify_and_finalize:
            if isinstance(genai_item, dict):
                if "matched_hyperlinks" in genai_item and "hyperlinks" not in genai_item :
                    genai_item["hyperlinks"] = genai_item.pop("matched_hyperlinks")
                elif "matched_hyperlinks" in genai_item and "hyperlinks" in genai_item and genai_item["hyperlinks"] != genai_item["matched_hyperlinks"]:
                     del genai_item["matched_hyperlinks"]
                if "page_number" in genai_item: # This should ideally not be here if _extract_page... gives clean page items
                    del genai_item["page_number"]


    # --- Save Final Content ---
    final_content_filepath = os.path.join(genai_output_dir_path, final_content_filename)
    try:
        with open(final_content_filepath, "w", encoding="utf-8") as f_final_genai: json.dump(content_to_verify_and_finalize, f_final_genai, indent=2, ensure_ascii=False)
    except Exception as e_save_final_genai: print(f"⚠️ Error saving final Gemini data to {final_content_filepath}: {e_save_final_genai}"); page_metrics["error_saving_final_genai_json"] = str(e_save_final_genai)

    page_data_for_results_json = {
        "page_number": actual_page_num,
        "gemini_original_output_file": original_genai_filename,
        "gemini_final_content_file": final_content_filename,
        "pypdf2_extracted_text": pypdf2_text_for_page, # Pass the already extracted text
        "ocr_extracted_text": ocr_text_for_page,       # Pass the already extracted text
        "chosen_fallback_text": chosen_fallback_text_for_page,
        "fitz_text_extracted": fitz_page_text_content, # Pass the already extracted text
        "fitz_hyperlinks": hyperlinks_from_fitz_content, # Pass the already extracted links
        "text_used_for_verification_source": page_metrics["verification_text_source"],
        "text_used_for_verification_char_count": len(text_for_content_verification),
    }
    # Add error to page_data if one occurred during this finalization
    if "error" in content_to_verify_and_finalize[0] if isinstance(content_to_verify_and_finalize, list) and content_to_verify_and_finalize and isinstance(content_to_verify_and_finalize[0], dict) else False:
        page_data_for_results_json["error"] = content_to_verify_and_finalize[0]["error"]
        # page_metrics["error"] is already a key in _initialize_page_metrics, but it's for general page errors.
        # Let's use "gemini_error_message" for specific Gemini errors if available, or add a new key if this "error" is different.
        # For now, let's assume the main page_metrics["error"] can hold this.
        page_metrics["error"] = content_to_verify_and_finalize[0]["error"]


    page_metrics["time_sec_total_page_finalization"] = time.time() - page_processing_start_time_specific #
    return page_data_for_results_json, page_metrics


def process_pdf(pdf_path: str, output_dir: str, temp_page_dir: str) -> list:
    os.makedirs(temp_page_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    genai_output_dir = os.path.join(output_dir, "genai_outputs")
    os.makedirs(genai_output_dir, exist_ok=True)

    # CHUNK_SIZE here means the window size for Gemini (e.g., 3 pages)
    CHUNK_SIZE_STR = os.environ.get('PDF_CHUNK_SIZE', '3') # Default to 3 for sliding window
    try:
        GEMINI_WINDOW_SIZE = int(CHUNK_SIZE_STR)
        if GEMINI_WINDOW_SIZE <= 0: GEMINI_WINDOW_SIZE = 3; print(f"⚠️ Invalid PDF_CHUNK_SIZE '{CHUNK_SIZE_STR}'. Defaulting to 3.")
    except ValueError: GEMINI_WINDOW_SIZE = 3; print(f"⚠️ PDF_CHUNK_SIZE '{CHUNK_SIZE_STR}' not valid int. Defaulting to 3.")
    print(f"📄 Processing PDF: {pdf_path}. Gemini window size: {GEMINI_WINDOW_SIZE} pages.")

    all_page_responses_for_results_json = []
    all_page_metrics_final_list = [] 
    poppler_bin_path = os.environ.get("POPPLER_PATH")

    MIN_DIRECT_PYPDF2_TEXT_LENGTH_THRESHOLD = int(os.environ.get("MIN_PYPDF2_LEN", "20"))
    MIN_FITZ_TEXT_LENGTH_THRESHOLD = int(os.environ.get("MIN_FITZ_LEN", "20"))
    FUZZY_MATCH_THRESHOLD = int(os.environ.get("FUZZY_THRESHOLD", "88"))
    MIN_CONTENT_LEN_FOR_FUZZY = int(os.environ.get("MIN_CONTENT_FUZZY_LEN", "4"))

    pdf_document_original = None
    try:
        pdf_document_original = fitz.open(pdf_path)
    except Exception as e:
        print(f"❌ Failed to open PDF {pdf_path}: {e}"); return []
    num_pages_total_original = len(pdf_document_original)
    print(f"Total pages in PDF: {num_pages_total_original}")

    # --- Stage 1: Gemini Processing with Sliding Window ---
    gemini_data_per_page = {}  # Stores final selected Gemini output for each actual page number
    
    # NEW: Dictionary to store chunk-level Gemini metrics, mapped to actual page numbers
    chunk_gemini_metrics_map = {}
    
    processed_gemini_pages_set = set() # Tracks actual page numbers whose Gemini data has been selected and stored

    print(f"\n🤖 Starting Gemini Processing with sliding window (size {GEMINI_WINDOW_SIZE})...")
    for i in range(num_pages_total_original): # i is the 0-based index of the *start* of the window
        
        window_page_indices = list(range(i, min(i + GEMINI_WINDOW_SIZE, num_pages_total_original)))
        
        if not window_page_indices:
            continue

        pages_to_store_from_this_window_actual_nums = []
        if i == 0: 
            pages_to_store_from_this_window_actual_nums = [p_idx + 1 for p_idx in window_page_indices]
        else: 
            newest_page_idx_in_window = window_page_indices[-1]
            pages_to_store_from_this_window_actual_nums = [newest_page_idx_in_window + 1]
        
        start_actual_page_num_in_window = window_page_indices[0] + 1
        num_pages_in_window = len(window_page_indices)
        
        print(f"⚙️  Gemini Window: Original Page(s) {[p_idx + 1 for p_idx in window_page_indices]} (Actual: {start_actual_page_num_in_window} to {window_page_indices[-1] + 1})")
        
        temp_gemini_window_pdf_path = os.path.join(temp_page_dir, f"temp_gemini_window_p{start_actual_page_num_in_window}_to_p{window_page_indices[-1]+1}.pdf")
        
        gemini_call_specific_metrics = {} # Initialize for each chunk call
        
        try:
            _create_temp_chunk_pdf(pdf_document_original, window_page_indices[0], num_pages_in_window, temp_gemini_window_pdf_path)
            
            pdf_chunk_base64 = None
            if os.path.exists(temp_gemini_window_pdf_path):
                 pdf_chunk_base64 = encode_pdf_to_base64(temp_gemini_window_pdf_path)
            
            chunk_gemini_json_output = {}

            if pdf_chunk_base64:
                chunk_gemini_json_output = _call_gemini_for_layout(
                    pdf_chunk_base64,
                    start_actual_page_num_in_window,
                    num_pages_in_window,
                    gemini_call_specific_metrics # This dict will be updated by the function
                )
            else:
                print(f"⚠️ Temp Gemini window PDF not found or failed to encode: {temp_gemini_window_pdf_path}")
                # gemini_call_specific_metrics will be empty or only partially filled by _call_gemini_for_layout if error occurs early
                gemini_call_specific_metrics.setdefault("gemini_api_status", "ErrorBeforeAPICall")
                gemini_call_specific_metrics.setdefault("gemini_error_message", "Temp Gemini window PDF missing or failed to encode.")
                gemini_call_specific_metrics.setdefault("time_sec_gemini_layout", 0.0) # Ensure time is set

                for page_idx_in_error_window in window_page_indices:
                    actual_page_num_err = page_idx_in_error_window + 1
                    if actual_page_num_err in pages_to_store_from_this_window_actual_nums and actual_page_num_err not in processed_gemini_pages_set:
                        gemini_data_per_page[actual_page_num_err] = [{"error": "temp_gemini_window_pdf_missing"}]
                        processed_gemini_pages_set.add(actual_page_num_err)
                # Continue to next iteration of Gemini window handled by outer loop structure

            # NEW: Store Gemini call metrics for pages whose data is selected from this chunk
            for page_num_to_get_metrics in pages_to_store_from_this_window_actual_nums:
                if page_num_to_get_metrics not in chunk_gemini_metrics_map: # Store only the first time
                    if gemini_call_specific_metrics: # Check if it's not empty
                        chunk_gemini_metrics_map[page_num_to_get_metrics] = copy.deepcopy(gemini_call_specific_metrics)
                    else: # Fallback if somehow empty after the call
                        chunk_gemini_metrics_map[page_num_to_get_metrics] = {
                            "gemini_api_status": "MetricsObjectEmpty",
                            "gemini_error_message": "gemini_call_specific_metrics was empty after _call_gemini_for_layout.",
                            "time_sec_gemini_layout": 0.0
                        }
            
            # Extract and store the relevant page(s) data from the Gemini output
            for actual_page_to_store in pages_to_store_from_this_window_actual_nums:
                if actual_page_to_store not in processed_gemini_pages_set: # Check if not already processed
                    page_specific_gemini_data = _extract_page_data_from_gemini_chunk_output(
                        chunk_gemini_json_output, actual_page_to_store
                    )
                    gemini_data_per_page[actual_page_to_store] = page_specific_gemini_data
                    processed_gemini_pages_set.add(actual_page_to_store)
                    print(f"    ✅ Stored Gemini data for Page {actual_page_to_store}")


        except Exception as e_gemini_window:
            print(f"❌ Error during Gemini processing for window starting at original page {i+1}: {e_gemini_window}")
            # Populate gemini_call_specific_metrics with error info if not already done by _call_gemini_for_layout
            gemini_call_specific_metrics.setdefault("gemini_api_status", "ExceptionInWindowProcessing")
            gemini_call_specific_metrics.setdefault("gemini_error_message", str(e_gemini_window))
            gemini_call_specific_metrics.setdefault("time_sec_gemini_layout", gemini_call_specific_metrics.get("time_sec_gemini_layout", 0.0)) # Preserve if set

            for page_idx_in_failed_window in window_page_indices:
                actual_page_num_fail = page_idx_in_failed_window + 1
                # Store error for all pages that were meant to be captured by this failed window's Gemini call
                if actual_page_num_fail in pages_to_store_from_this_window_actual_nums:
                    if actual_page_num_fail not in processed_gemini_pages_set:
                         gemini_data_per_page[actual_page_num_fail] = [{"error": f"Gemini window processing failed: {e_gemini_window}"}]
                         processed_gemini_pages_set.add(actual_page_num_fail)
                    # Also store the error metrics for this page in chunk_gemini_metrics_map
                    if actual_page_num_fail not in chunk_gemini_metrics_map:
                         chunk_gemini_metrics_map[actual_page_num_fail] = copy.deepcopy(gemini_call_specific_metrics)

        finally:
            if os.path.exists(temp_gemini_window_pdf_path):
                try: os.remove(temp_gemini_window_pdf_path)
                except Exception as e_delete: print(f"⚠️ Failed to delete temp Gemini window PDF {temp_gemini_window_pdf_path}: {e_delete}")
        
        if len(processed_gemini_pages_set) == num_pages_total_original:
            print("   All pages have had their Gemini data selected.")
            break
    
    print("🤖 Gemini processing phase complete.")

    # --- Stage 2: Per-Page Fitz, OCR, Verification, and Finalization ---
    print(f"\n⚙️  Starting per-page Fitz, OCR, and finalization for {num_pages_total_original} pages...")
    page_processing_overall_start_time = time.time() # For timing the entire Stage 2

    for page_idx in range(num_pages_total_original): # 0-based index
        actual_page_num = page_idx + 1
        page_individual_start_time = time.time() # For this specific page's processing in Stage 2

        page_metrics = _initialize_page_metrics(actual_page_num) #
        
        # NEW: Retrieve and merge Gemini call metrics from Stage 1
        retrieved_chunk_gemini_metrics = chunk_gemini_metrics_map.get(actual_page_num)
        if retrieved_chunk_gemini_metrics:
            page_metrics["gemini_api_status"] = retrieved_chunk_gemini_metrics.get("gemini_api_status") #
            page_metrics["gemini_response_length"] = retrieved_chunk_gemini_metrics.get("gemini_response_length", 0) #
            page_metrics["gemini_error_message"] = retrieved_chunk_gemini_metrics.get("gemini_error_message", "") #
            page_metrics["gemini_input_tokens"] = retrieved_chunk_gemini_metrics.get("gemini_input_tokens", 0) #
            page_metrics["gemini_output_tokens"] = retrieved_chunk_gemini_metrics.get("gemini_output_tokens", 0) #
            page_metrics["gemini_cost_usd"] = retrieved_chunk_gemini_metrics.get("gemini_cost_usd", 0.0) #
            page_metrics["time_sec_gemini_layout"] = retrieved_chunk_gemini_metrics.get("time_sec_gemini_layout", 0.0) #
        else:
            print(f"⚠️ Warning: No chunk-level Gemini metrics found for page {actual_page_num}. Using defaults.")
            # Ensure gemini_error_message reflects this if it's not already set by _initialize_page_metrics
            current_err_msg = page_metrics.get("gemini_error_message", "")
            separator = " | " if current_err_msg else ""
            page_metrics["gemini_error_message"] = f"{current_err_msg}{separator}No chunk-level Gemini API metrics found for this page."


        print(f"   Processing Page {actual_page_num}...")

        temp_single_page_pdf_path = os.path.join(temp_page_dir, f"temp_single_p{actual_page_num}.pdf")
        temp_pdf_creation_start_time = time.time() # For creating the single page PDF
        
        pypdf2_text_for_page, ocr_text_for_page = "", ""
        fitz_page_text_content, hyperlinks_from_fitz = "", []
        
        try:
            _create_temp_chunk_pdf(pdf_document_original, page_idx, 1, temp_single_page_pdf_path)
            page_metrics["time_sec_temp_pdf_creation"] = time.time() - temp_pdf_creation_start_time #

            # Fitz Extraction (on the single page)
            fitz_extraction_start_time = time.time()
            try:
                fitz_data_list = extract_text_and_links_from_chunk_fitz(temp_single_page_pdf_path) #
                if fitz_data_list: 
                    raw_fitz_text, raw_fitz_links = fitz_data_list[0]
                    fitz_page_text_content = raw_fitz_text.strip() if raw_fitz_text else ""
                    hyperlinks_from_fitz = raw_fitz_links if raw_fitz_links else []
                    page_metrics["hyperlink_extraction_status"] = "success_fitz" #
                    page_metrics["hyperlinks_found_count"] = len(hyperlinks_from_fitz) #
                    # The following keys are not in _initialize_page_metrics but were in original handler1.py
                    # Consider adding them to _initialize_page_metrics if they are essential for the final report
                    page_metrics["fitz_extraction_status"] = "success" 
                    page_metrics["fitz_text_char_count"] = len(fitz_page_text_content)
                    page_metrics["fitz_link_count"] = len(hyperlinks_from_fitz) # Redundant with hyperlinks_found_count
                else:
                    page_metrics["hyperlink_extraction_status"] = "empty_result_fitz" #
                    page_metrics["fitz_extraction_status"] = "empty_result_from_fitz_extraction"
            except Exception as e_fitz:
                print(f"   ⚠️ Fitz extraction error for page {actual_page_num}: {e_fitz}")
                page_metrics["hyperlink_extraction_status"] = f"failed_fitz: {e_fitz}" #
                page_metrics["fitz_extraction_status"] = f"fitz_extraction_failed: {e_fitz}"
            page_metrics["time_sec_hyperlink_extraction"] = time.time() - fitz_extraction_start_time #


            # PyPDF2 Extraction (on the single page)
            pypdf2_extraction_start_time = time.time()
            try:
                pypdf2_texts_list = extract_text_from_pdf_chunk_pypdf2(temp_single_page_pdf_path) #
                if pypdf2_texts_list:
                    pypdf2_text_for_page = pypdf2_texts_list[0].strip() if pypdf2_texts_list[0] else ""
                page_metrics["direct_text_char_count"] = len(pypdf2_text_for_page) #
                page_metrics["direct_text_extraction_status"] = "success_pypdf2" if pypdf2_text_for_page else "empty_pypdf2" #
                # Keys from original handler1.py, not in _initialize_page_metrics
                page_metrics["pypdf2_char_count"] = len(pypdf2_text_for_page) # Redundant with direct_text_char_count
                page_metrics["pypdf2_status"] = "success" if pypdf2_text_for_page else "empty_result"
            except Exception as e_pypdf2:
                print(f"   ⚠️ PyPDF2 extraction error for page {actual_page_num}: {e_pypdf2}")
                page_metrics["direct_text_extraction_status"] = f"failed_pypdf2: {e_pypdf2}" #
                page_metrics["pypdf2_status"] = f"pypdf2_extraction_failed: {e_pypdf2}"
            # Add time for pypdf2 if needed: page_metrics["time_sec_pypdf2_extraction"] = time.time() - pypdf2_extraction_start_time


            # OCR Extraction (on the single page)
            ocr_extraction_start_time = time.time()
            try:
                ocr_texts_list = extract_text_from_chunk_ocr(temp_single_page_pdf_path, poppler_path=poppler_bin_path) #
                if ocr_texts_list:
                    ocr_text_for_page = ocr_texts_list[0].strip() if ocr_texts_list[0] else ""
                page_metrics["ocr_text_char_count"] = len(ocr_text_for_page) #
                page_metrics["ocr_text_extraction_status"] = "success_ocr" if ocr_text_for_page else "empty_ocr" #
                # Keys from original handler1.py, not in _initialize_page_metrics
                page_metrics["ocr_char_count"] = len(ocr_text_for_page) # Redundant
                page_metrics["ocr_status"] = "success" if ocr_text_for_page else "empty_result"
            except Exception as e_ocr:
                print(f"   ⚠️ OCR extraction error for page {actual_page_num}: {e_ocr}")
                page_metrics["ocr_text_extraction_status"] = f"failed_ocr: {e_ocr}" #
                page_metrics["ocr_status"] = f"ocr_extraction_failed: {e_ocr}"
            # Add time for ocr if needed: page_metrics["time_sec_ocr_extraction"] = time.time() - ocr_extraction_start_time


        except Exception as e_single_page_prep:
            print(f"   ❌ Error preparing or extracting text for page {actual_page_num}: {e_single_page_prep}")
            page_metrics["error"] = f"single_page_text_extraction_failed: {e_single_page_prep}" # This "error" key is not in _initialize_page_metrics
            final_content_filename = f"page_{actual_page_num}_final_content.json"
            original_genai_filename = f"page_{actual_page_num}_genai.json"
            error_content_for_file = [{"error": page_metrics["error"]}] # Using the ad-hoc "error" key
            try:
                with open(os.path.join(genai_output_dir, final_content_filename), "w", encoding="utf-8") as f_err: json.dump(error_content_for_file, f_err, indent=2)
                with open(os.path.join(genai_output_dir, original_genai_filename), "w", encoding="utf-8") as f_err: json.dump(error_content_for_file, f_err, indent=2)
            except Exception as e_save_err: print(f"   ⚠️ Also failed to save error file for page {actual_page_num}: {e_save_err}")
            
            all_page_responses_for_results_json.append({
                "page_number": actual_page_num, "error": page_metrics["error"],
                "gemini_original_output_file": original_genai_filename,
                "gemini_final_content_file": final_content_filename
            })
            page_metrics["time_sec_total_page_processing"] = time.time() - page_individual_start_time #
            all_page_metrics_final_list.append(page_metrics)
            if os.path.exists(temp_single_page_pdf_path): os.remove(temp_single_page_pdf_path)
            continue 

        finally:
            if os.path.exists(temp_single_page_pdf_path):
                try: os.remove(temp_single_page_pdf_path)
                except Exception as e_del_single: print(f"   ⚠️ Failed to delete temp single page PDF {temp_single_page_pdf_path}: {e_del_single}")
        
        retrieved_genai_content = copy.deepcopy(gemini_data_per_page.get(actual_page_num, [{"error": f"No Gemini content was selected/available for page {actual_page_num}"}]))
        
        # This status indicates if parsed GenAI data was retrieved for the page from Stage 1's output.
        # It's different from page_metrics["gemini_api_status"] which is about the API call itself.
        page_metrics["gemini_content_retrieval_status"] = "success" if not ("error" in retrieved_genai_content[0] if retrieved_genai_content and isinstance(retrieved_genai_content[0],dict) else False) else "error_or_missing" #
        if "error" in retrieved_genai_content[0] if retrieved_genai_content and isinstance(retrieved_genai_content[0],dict) else False:
            # If gemini_error_message is already set (e.g. from chunk metrics), append this. Otherwise, set it.
            current_err_msg = page_metrics.get("gemini_error_message", "")
            retrieval_err_msg = retrieved_genai_content[0]["error"]
            separator = " | " if current_err_msg and retrieval_err_msg else ""
            page_metrics["gemini_error_message"] = f"{current_err_msg}{separator}{retrieval_err_msg}"


        try:
            page_response_data, updated_page_metrics = _finalize_single_page_processing(
                actual_page_num,
                retrieved_genai_content,
                pypdf2_text_for_page,
                ocr_text_for_page,
                fitz_page_text_content,
                hyperlinks_from_fitz,
                page_metrics, 
                genai_output_dir,
                FUZZY_MATCH_THRESHOLD,
                MIN_DIRECT_PYPDF2_TEXT_LENGTH_THRESHOLD,
                MIN_FITZ_TEXT_LENGTH_THRESHOLD,
                MIN_CONTENT_LEN_FOR_FUZZY
            )
            all_page_responses_for_results_json.append(page_response_data)
            # page_metrics (now updated_page_metrics) already contains timings from _finalize_single_page_processing
            updated_page_metrics["time_sec_total_page_processing"] = time.time() - page_individual_start_time #
            all_page_metrics_final_list.append(updated_page_metrics)
        except Exception as e_finalize:
            print(f"   ❌ Error finalizing page {actual_page_num}: {e_finalize}")
            # Ensure the 'error' key exists in page_metrics or add it.
            # _initialize_page_metrics does not have a generic 'error' key, but 'gemini_error_message'.
            # Let's add this to 'gemini_error_message' or a general error field if defined in _initialize_page_metrics.
            current_err_msg = page_metrics.get("gemini_error_message", "")
            finalize_err_msg = f"page_finalization_failed: {e_finalize}"
            separator = " | " if current_err_msg and finalize_err_msg else ""
            page_metrics["gemini_error_message"] = f"{current_err_msg}{separator}{finalize_err_msg}"
            
            # page_metrics["error"] = f"page_finalization_failed: {e_finalize}" # If using an ad-hoc error key

            final_content_filename_err = f"page_{actual_page_num}_final_content.json"
            original_genai_filename_err = f"page_{actual_page_num}_genai.json"
            # Use the error from page_metrics for consistency if it's structured there, e.g. page_metrics["gemini_error_message"]
            error_content_for_file_fin = [{"error": page_metrics.get("gemini_error_message",finalize_err_msg)}]
            try:
                with open(os.path.join(genai_output_dir, final_content_filename_err), "w", encoding="utf-8") as f_err: json.dump(error_content_for_file_fin, f_err, indent=2)
                if not os.path.exists(os.path.join(genai_output_dir, original_genai_filename_err)): 
                    with open(os.path.join(genai_output_dir, original_genai_filename_err), "w", encoding="utf-8") as f_err_o: json.dump(retrieved_genai_content if retrieved_genai_content else error_content_for_file_fin, f_err_o, indent=2)
            except Exception as e_save_err_fin: print(f"   ⚠️ Also failed to save error file during finalization for page {actual_page_num}: {e_save_err_fin}")

            all_page_responses_for_results_json.append({
                "page_number": actual_page_num, "error": page_metrics.get("gemini_error_message", finalize_err_msg),
                "gemini_original_output_file": original_genai_filename_err,
                "gemini_final_content_file": final_content_filename_err
            })
            page_metrics["time_sec_total_page_processing"] = time.time() - page_individual_start_time #
            all_page_metrics_final_list.append(page_metrics)


    if pdf_document_original: pdf_document_original.close()

    print(f"\n📊 Total time for Stage 2 (Per-Page Fitz, OCR, Finalization): {time.time() - page_processing_overall_start_time:.2f} seconds.")

    _save_results(all_page_responses_for_results_json, all_page_metrics_final_list, output_dir) #
    _create_book_output(all_page_responses_for_results_json, genai_output_dir, output_dir)

    book_output_json_path = os.path.join(output_dir, "book_output.json")
    if os.path.exists(book_output_json_path):
        try:
            with open(book_output_json_path, "r", encoding="utf-8") as f_book_json:
                book_data_for_html = json.load(f_book_json)
            convert_book_json_to_html(book_data_for_html, output_dir, "book_output.html")
        except Exception as e_html_conversion:
            print(f"❌ Error converting book_output.json to HTML: {e_html_conversion}")
    else:
        print(f"⚠️ Could not find book_output.json at {book_output_json_path} for HTML conversion.")

    return all_page_metrics_final_list


def _create_book_output(all_page_data_for_results: list, genai_output_dir: str, main_output_dir: str):
    """
    Creates book_output.json by aggregating and transforming page_{N}_final_content.json files.
    The structure will be a dictionary with page numbers as keys.
    The 'rect' node will be removed from 'hyperlinks' (formerly 'matched_hyperlinks').
    The 'page_number' node will be removed from individual items.
    """
    book_data = {}
    print(f"\n📚 Creating consolidated book_output.json in {main_output_dir}")

    sorted_page_data = sorted(all_page_data_for_results, key=lambda x: x.get("page_number", float('inf')))

    for page_data in sorted_page_data:
        page_number = page_data.get("page_number")
        final_content_filename = page_data.get("gemini_final_content_file")

        if page_number is None or not final_content_filename:
            print(f"⚠️ Skipping page for book_output.json due to missing page_number or final_content_filename: {page_data}")
            continue

        final_content_filepath = os.path.join(genai_output_dir, final_content_filename)
        
        page_content_list_for_book = []
        try:
            if os.path.exists(final_content_filepath):
                with open(final_content_filepath, "r", encoding="utf-8") as f_final:
                    page_items_from_file = json.load(f_final)
                
                processed_page_items = copy.deepcopy(page_items_from_file)

                if isinstance(processed_page_items, list):
                    for item in processed_page_items:
                        if isinstance(item, dict):
                            if "hyperlinks" in item and isinstance(item["hyperlinks"], list):
                                transformed_hyperlinks = []
                                for hyperlink_details in item["hyperlinks"]:
                                    if isinstance(hyperlink_details, dict):
                                        link_without_rect = {k: v for k, v in hyperlink_details.items() if k != 'rect'}
                                        transformed_hyperlinks.append(link_without_rect)
                                item["hyperlinks"] = transformed_hyperlinks
                            if "page_number" in item:
                                del item["page_number"]
                    page_content_list_for_book = processed_page_items
                else:
                    print(f"⚠️ Content of {final_content_filepath} for page {page_number} is not a list. Storing as is.")
                    page_content_list_for_book = processed_page_items 
            else:
                print(f"⚠️ File not found: {final_content_filepath} for page {page_number}. Storing error placeholder.")
                page_content_list_for_book = [{"error": f"File {final_content_filename} not found"}]
        
        except json.JSONDecodeError as je:
            print(f"⚠️ Error decoding JSON from {final_content_filepath} for page {page_number}: {je}. Storing error placeholder.")
            page_content_list_for_book = [{"error": f"JSONDecodeError in {final_content_filename}: {str(je)}"}]
        except Exception as e:
            print(f"⚠️ Unexpected error processing {final_content_filepath} for page {page_number}: {e}. Storing error placeholder.")
            page_content_list_for_book = [{"error": f"Unexpected error processing {final_content_filename}: {str(e)}"}]
            
        book_data[str(page_number)] = page_content_list_for_book

    book_output_filepath = os.path.join(main_output_dir, "book_output.json")
    try:
        with open(book_output_filepath, "w", encoding="utf-8") as f_book:
            json.dump(book_data, f_book, indent=2, ensure_ascii=False)
        print(f"✅ Successfully created {book_output_filepath}")
    except Exception as e_save_book:
        print(f"❌ Error saving consolidated book_output.json: {e_save_book}")


if __name__ == "__main__":
    try: import fitz
    except ImportError: print("❌ PyMuPDF (fitz) is not installed. Please install it using: pip install PyMuPDF"); sys.exit(1)
    try: import pytesseract # type: ignore
    except ImportError: print("⚠️ pytesseract library not found. OCR extraction will fail if Tesseract engine is not found.")
    try: from PyPDF2 import PdfReader # type: ignore
    except ImportError: print("⚠️ PyPDF2 library not found. Direct PyPDF2 text extraction will fail.")
    try: from pdf2image import convert_from_path # type: ignore
    except ImportError: print("⚠️ pdf2image library not found. OCR extraction will fail.")
    try: from thefuzz import fuzz # type: ignore
    except ImportError: print("⚠️ thefuzz library not found. Fuzzy verification will fail. pip install thefuzz python-Levenshtein")
 
    base_dir = Path(__file__).resolve().parent
    project_root_path = base_dir.parents[3]
  
    print(f"Project root for __main__ determined as: {project_root_path}")

    pdf_path = project_root_path / "tests" / "assets" / "inputs" / "sample.pdf" 
    output_dir_path = project_root_path / "tests" / "assets" / "outputs" / "functions" / "output_doc_layout"
    temp_pdf_page_dir_path = output_dir_path / "temp_pdf_chunks" 
    
    os.makedirs(output_dir_path, exist_ok=True)
    os.makedirs(temp_pdf_page_dir_path, exist_ok=True)

    print(f"Input PDF path: {pdf_path}")
    print(f"Output directory: {output_dir_path}")
    print(f"Temporary PDF chunk directory: {temp_pdf_page_dir_path}")

    if not pdf_path.exists():
        print(f"❌ ERROR: PDF file not found at {pdf_path}")
        sys.exit(1)

    page_summary_metrics = process_pdf(
        pdf_path=str(pdf_path),
        output_dir=str(output_dir_path),
        temp_page_dir=str(temp_pdf_page_dir_path)
    )

    if page_summary_metrics:
        print(f"✅ PDF processing complete. Metrics for {len(page_summary_metrics)} pages generated and saved via finalizer.py.")
    else:
        print("⚠️ PDF processing completed, but no page summary metrics were returned (check for errors).")