import os
import argparse
import sys
import json # Added for JSON operations
from google.cloud import documentai_v1 as documentai # For type hinting
from google.protobuf.json_format import MessageToJson # Added to convert Document object to JSON
from pathlib import Path

# Add project root to PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.insert(0, project_root)

from core.layout_documentai import process_pdf, save_document_as_json
from services.documentai_client import process_document_sample

# Configuration from environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")

def main():
    """
    Main function to parse arguments, trigger PDF processing, and save the output.
    """
    # Define base paths using pathlib as per user's structure
    # handler.py is in 'docudive-juristab/app/functions/doc-layout-extraction-DAI/'
    base_dir = Path(__file__).resolve().parent 
    # project_root_path for assets is 'app/' (parents[2] from .../doc-layout-extraction-DAI/)
    assets_project_root = base_dir.parents[2] 
    
    default_pdf_path = assets_project_root / "tests" / "assets" / "inputs" / "sample.pdf"
    
    # Define output directory structure
    output_dir_base = assets_project_root / "tests" / "assets" / "outputs" / "functions" / "output_doc_layout"
    genai_output_dir_abs_path = output_dir_base / "genai_outputs"

    parser = argparse.ArgumentParser(description="Process a PDF document using Google Document AI and save output as JSON.")
    parser.add_argument(
        "pdf_file_path", 
        type=str, 
        nargs='?',
        default=str(default_pdf_path), # Use the defined default path
        help=f"The path to the PDF file to process. Defaults to '{default_pdf_path}'"
    )
    args = parser.parse_args()

    if hasattr(process_document_sample, '__name__') and 'CRITICAL DUMMY' in process_document_sample.__doc__:
         print("ERROR: Exiting due to critical import errors for Document AI client functions.")
         return

    # Resolve the input PDF path
    # If user provides an absolute path, Path(args.pdf_file_path) will handle it.
    # If user provides a relative path, it's resolved relative to the current working directory by Path.
    # To make it relative to script dir if not absolute (current behavior was os.path.join(script_dir, path)):
    pdf_input_path_obj = Path(args.pdf_file_path)
    if not pdf_input_path_obj.is_absolute():
        pdf_full_path = (base_dir / pdf_input_path_obj).resolve()
    else:
        pdf_full_path = pdf_input_path_obj.resolve()
    
    print(f"INFO: Attempting to process PDF from resolved path: {pdf_full_path}")
    document_result = process_pdf(str(pdf_full_path)) # process_pdf expects string path

    if document_result:
        print("\nINFO: Document processing successful.")
        input_pdf_filename = pdf_full_path.name # Get filename from Path object
        
        save_document_as_json(document_result, genai_output_dir_abs_path, input_pdf_filename)
    else:
        print("\nINFO: Document processing failed or was aborted.")

if __name__ == "__main__":
    main()
