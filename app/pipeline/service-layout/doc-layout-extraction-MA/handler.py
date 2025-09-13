import os
import sys
import time
from pathlib import Path
import fitz
import json


# Add project root to PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.insert(0, project_root)

# Local imports
from utils.file_utils import encode_pdf_to_base64
from utils.pdf_utils import _create_temp_page_pdf
from utils.metrics_utils import _initialize_page_metrics
from utils.pdf_text_extractor import extract_text_from_pdf_page, extract_text_from_ocr, extract_text_and_links_with_fitz
from utils.text_utils import _verify_item_content_in_direct_text_fuzzy

from core.layout_gemini import _call_gemini_for_layout
from core.layout_openai import _call_openai_for_layout
from core.page_processor import _orchestrate_page_processing
from core.finalizer import _save_results



def process_pdf(pdf_path: str, output_dir: str, temp_page_dir: str) -> list:
    os.makedirs(temp_page_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    genai_output_dir = os.path.join(output_dir, "genai_outputs")
    os.makedirs(genai_output_dir, exist_ok=True)

    print(f"📄 Processing PDF: {pdf_path} page by page.")

    all_responses = []
    page_metrics_list = []
    
    poppler_bin_path = None
    MIN_DIRECT_PYPDF2_TEXT_LENGTH_THRESHOLD = 20  # Min characters for direct text to be considered "sufficient"
    MIN_FITZ_TEXT_LENGTH_THRESHOLD = 20
    FUZZY_MATCH_THRESHOLD = 88 # Configurable threshold for fuzzy matching
    
    try:
        pdf_document = fitz.open(pdf_path)
    except Exception as e:
        print(f"❌ Failed to open PDF {pdf_path}: {e}")
        return []

    for page_index in range(len(pdf_document)):
        page_num_actual = page_index + 1
        print("---------------------")
        print(f"📤 Processing page: {page_num_actual}")

        metrics = _initialize_page_metrics(page_num_actual)
        # Metrics for the "previous fallback mechanism" (Direct PyPDF2 -> OCR)
        metrics["fallback_text_method_used"] = "none"
        metrics["fallback_text_status"] = "not_attempted"
        metrics["fallback_text_char_count"] = 0
        # Metrics for Fitz extraction
        metrics["fitz_extraction_status"] = "not_attempted"
        metrics["fitz_text_char_count"] = 0 # Text from Fitz
        metrics["fitz_link_count"] = 0
        # Final chosen text for verification
        metrics["verification_text_source"] = "none"
        
        page_processing_start_time = time.time()

        temp_pdf_page_path = os.path.join(temp_page_dir, f"temp_page_{page_num_actual}.pdf")
        pdf_page_base64 = None
        chosen_text_for_page = ""
        hyperlinks_from_fitz = []
        actual_direct_text_for_page = "" # Store direct text separately for verification

        # Initialize hyperlink data structure for the current page
        # This will now be returned by the subfunction.
        # extracted_hyperlinks_data = {"hyperlinks": [], "status": "not_attempted", "error_message": ""}

        try:
            temp_pdf_creation_start_time = time.time()
            _create_temp_page_pdf(pdf_document, page_index, temp_pdf_page_path)
            metrics["time_sec_temp_pdf_creation"] = time.time() - temp_pdf_creation_start_time
            pdf_page_base64 = encode_pdf_to_base64(temp_pdf_page_path)

            # --- Block 1: "Previous Fallback Mechanism" (Direct PyPDF2 -> OCR) ---
            direct_pypdf2_sufficient = False
            try:
                print(f"ℹ️ Attempting PyPDF2 direct text extraction for page {page_num_actual}")
                direct_pypdf2_text = extract_text_from_pdf_page(temp_pdf_page_path, 0)
                if direct_pypdf2_text and len(direct_pypdf2_text.strip()) > MIN_DIRECT_PYPDF2_TEXT_LENGTH_THRESHOLD:
                    chosen_text_from_fallback = direct_pypdf2_text
                    metrics["fallback_text_method_used"] = "direct_pypdf2"
                    metrics["fallback_text_status"] = "success"
                    direct_pypdf2_sufficient = True
                    print(f"✅ PyPDF2 direct text extracted for fallback mechanism page {page_num_actual}.")
                else:
                    log_msg = "no/empty text" if not direct_pypdf2_text or len(direct_pypdf2_text.strip()) == 0 else "insufficient text"
                    print(f"ℹ️ PyPDF2 direct text extraction yielded {log_msg}. Will attempt OCR for fallback.")
            except Exception as e_direct:
                print(f"⚠️ PyPDF2 direct text extraction failed for page {page_num_actual}: {e_direct}")

            if not direct_pypdf2_sufficient:
                print(f"ℹ️ Attempting OCR for page {page_num_actual} (Fallback Mekanisme 2)...")
                try:
                    ocr_text = extract_text_from_ocr(temp_pdf_page_path, 0, poppler_path=poppler_bin_path)
                    if ocr_text and len(ocr_text.strip()) > 0:
                        chosen_text_from_fallback = ocr_text
                        metrics["fallback_text_method_used"] = "ocr_fallback"
                        metrics["fallback_text_status"] = "success"
                        print(f"✅ OCR text extracted for fallback mechanism page {page_num_actual}.")
                    else:
                        metrics["fallback_text_method_used"] = "ocr_fallback"
                        metrics["fallback_text_status"] = "ocr_empty_result"
                except Exception as e_ocr:
                    print(f"⚠️ OCR extraction failed for page {page_num_actual}: {e_ocr}")
                    metrics["fallback_text_method_used"] = "ocr_fallback"
                    metrics["fallback_text_status"] = f"ocr_fail: {str(e_ocr)}"
            
            metrics["fallback_text_char_count"] = len(chosen_text_from_fallback.strip())
            if chosen_text_from_fallback.strip():
                fb_text_path = os.path.join(genai_output_dir, f"page_{page_num_actual}_fallback_text.txt")
                with open(fb_text_path, "w", encoding="utf-8") as f: f.write(chosen_text_from_fallback)
                #print(f"✅ Fallback text saved to {fb_text_path}")
                print(f"✅ Fallback text saved")


            # --- Block 2: Fitz Text and Link Extraction ---
            print(f"ℹ️ Attempting text and link extraction with Fitz for page {page_num_actual}")
            fitz_output_filename = f"page_{page_num_actual}_fitz_data.json"
            fitz_output_path = os.path.join(genai_output_dir, fitz_output_filename)
            try:
                fitz_page_text, hyperlinks_from_fitz = extract_text_and_links_with_fitz(temp_pdf_page_path, 0)
                fitz_data_to_save = {
                    "page_number": page_num_actual,
                    "fitz_extracted_text": fitz_page_text,
                    "extracted_hyperlinks": hyperlinks_from_fitz
                }
                with open(fitz_output_path, "w", encoding="utf-8") as f:
                    json.dump(fitz_data_to_save, f, indent=2, ensure_ascii=False)
                print(f"✅ Fitz data (text & {len(hyperlinks_from_fitz)} links) for page {page_num_actual} saved.")
                metrics["fitz_extraction_status"] = "success"
                metrics["fitz_text_char_count"] = len(fitz_page_text.strip())
                metrics["fitz_link_count"] = len(hyperlinks_from_fitz)
            except Exception as e_fitz:
                print(f"⚠️ Fitz extraction failed for page {page_num_actual}: {e_fitz}")
                metrics["fitz_extraction_status"] = f"fail: {str(e_fitz)}"
                fitz_page_text = "" # Ensure empty on failure for decision making
                hyperlinks_from_fitz = []
                with open(fitz_output_path, "w", encoding="utf-8") as f: # Save error info
                    json.dump({"error": f"Fitz extraction failed: {str(e_fitz)}", "page_number": page_num_actual}, f, indent=2)

            # --- Determine Text for Content Verification ---
            if metrics["fitz_extraction_status"] == "success" and fitz_page_text.strip():
                text_for_content_verification = fitz_page_text
                metrics["verification_text_source"] = "fitz"
                print(f"ℹ️ Using Fitz-extracted text for content verification on page {page_num_actual}.")
            elif chosen_text_from_fallback.strip():
                text_for_content_verification = chosen_text_from_fallback
                metrics["verification_text_source"] = metrics["fallback_text_method_used"]
                print(f"ℹ️ Using Fallback text for content verification on page {page_num_actual} (Fitz text unavailable/empty).")
            else:
                text_for_content_verification = "" # No usable text from either method
                metrics["verification_text_source"] = "none_available"
                print(f"ℹ️ No text available from Fitz or Fallback for content verification on page {page_num_actual}.")


            # --- GenAI Layout Calls ---
            gemini_json = _call_gemini_for_layout(pdf_page_base64, page_num_actual, genai_output_dir, metrics)
            openai_json = _call_openai_for_layout(temp_pdf_page_path, page_num_actual, genai_output_dir, metrics)

            # --- Orchestration Call ---
            final_page_response = _orchestrate_page_processing(
                pdf_page_base64, gemini_json, openai_json,
                page_num_actual, genai_output_dir, temp_pdf_page_path, metrics
            )

            # --- Content Verification Step ---
            # This modifies final_page_response in place by updating "verification-flag"
            if isinstance(final_page_response, dict):
                final_page_response = _verify_item_content_in_direct_text_fuzzy( # Ensure this function is correctly imported/defined
                    final_page_response, 
                    text_for_content_verification, 
                    page_num_actual,
                    fuzzy_threshold=FUZZY_MATCH_THRESHOLD 
                )
                print(f"✅ Content verification against chosen extracted text completed for page {page_num_actual}.")
            else:
                print(f"⚠️ Skipping content verification for page {page_num_actual} as final_page_response is not a dictionary.")

            all_responses.append(final_page_response)
            print(f"✅ Page {page_num_actual} processed and response appended.")

        except Exception as e_outer_page_processing:
            print(f"❌ Outer error processing page {page_num_actual} (temp PDF: {temp_pdf_page_path}): {e_outer_page_processing}")
            page_error_info = {
                "error": f"General error processing page {page_num_actual}", "details": str(e_outer_page_processing),
                "page_number": page_num_actual, "page_verification_status": "fail - page processing error"
            }
            all_responses.append(page_error_info)
            metrics["verification_status"] = metrics.get("verification_status", "fail - page processing error")
            metrics["text_extraction_status"] = metrics.get("text_extraction_status", "fail_due_to_outer_error")

        finally:
            metrics["time_sec_total_page_processing"] = time.time() - page_processing_start_time
            if os.path.exists(temp_pdf_page_path):
                try:
                    os.remove(temp_pdf_page_path)
                except Exception as e_delete:
                    print(f"⚠️ Failed to delete temporary PDF {temp_pdf_page_path}: {e_delete}")

        page_metrics_list.append(metrics)

    if pdf_document:
        pdf_document.close()

    _save_results(all_responses, page_metrics_list, output_dir)

    return page_metrics_list


if __name__ == "__main__":
    try: import fitz
    except ImportError: print("❌ PyMuPDF (fitz) is not installed. Please install it using: pip install PyMuPDF"); sys.exit(1)
    try: import pytesseract
    except ImportError: print("⚠️ pytesseract library not found. OCR extraction will fail if Tesseract engine is not found.")
    try: from PyPDF2 import PdfReader
    except ImportError: print("⚠️ PyPDF2 library not found. Direct PyPDF2 text extraction will fail.")
    try: from pdf2image import convert_from_path
    except ImportError: print("⚠️ pdf2image library not found. OCR extraction will fail.")
    try: from thefuzz import fuzz # For _verify_item_content_in_direct_text_fuzzy
    except ImportError: print("⚠️ thefuzz library not found. Fuzzy verification will fail. pip install thefuzz python-Levenshtein")

 
    base_dir = Path(__file__).resolve().parent
    project_root_path = base_dir.parents[2]
  

    print(f"Project root determined as: {project_root_path}")

    # Define input PDF path
    pdf_path = project_root_path / "tests" / "assets" / "inputs" / "sample.pdf"
    # Define output directories
    output_dir_path = project_root_path / "tests" / "assets" / "outputs" / "functions" / "output_doc_layout"
    #image_dir_path = output_dir_path / "page_images"
    genai_output_dir = output_dir_path / "genai_outputs"

    # Define temp_pdf_page_dir_path correctly
    temp_pdf_page_dir_path = output_dir_path / "temp_pdf_pages" 
    
    # Ensure output folders exist
    os.makedirs(output_dir_path, exist_ok=True)
    os.makedirs(genai_output_dir, exist_ok=True)
    os.makedirs(temp_pdf_page_dir_path, exist_ok=True)

    print(f"Input PDF path: {pdf_path}")
    print(f"Output directory: {output_dir_path}")
    print(f"Temporary PDF page directory: {temp_pdf_page_dir_path}")

    # Ensure PDF exists
    if not pdf_path.exists():
        print(f"❌ ERROR: PDF file not found at {pdf_path}")
        sys.exit(1)

    # Process PDF for layout extraction
    page_summary_data = process_pdf(
        pdf_path=str(pdf_path),
        output_dir=str(output_dir_path),
        temp_page_dir=str(temp_pdf_page_dir_path) # Pass the new temp_page_dir argument
    )

    # Final summary
    if page_summary_data:
        print("✅ PDF processing complete. Page summary with verification generated.")
    else:
        print("⚠️ PDF processing completed, but no page summary data was returned.")


