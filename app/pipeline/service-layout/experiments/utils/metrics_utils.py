def _initialize_page_metrics(page_num_actual: int) -> dict:
    """Initializes a dictionary to store metrics for a single page."""
    return {
        "page": page_num_actual,
        "time_sec_total_page_processing": 0.0,
        "time_sec_temp_pdf_creation": 0.0,
        "gemini_api_status": None, "gemini_response_length": 0, "gemini_error_message": "",
        "gemini_input_tokens": 0, "gemini_output_tokens": 0, "gemini_cost_usd": 0.0,
        "time_sec_gemini_layout": 0.0,
        "time_sec_hyperlink_extraction": 0.0,
        "hyperlink_extraction_status": "not attempted",
        "hyperlinks_found_count": 0,
        "direct_text_extraction_status" : "not_attempted", 
        "direct_text_char_count" : 0, 
        "ocr_text_extraction_status" : "not_attempted", 
        "ocr_text_char_count" : 0
    }