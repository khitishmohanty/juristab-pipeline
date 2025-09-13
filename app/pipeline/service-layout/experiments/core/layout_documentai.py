import os
import argparse
import json # Added for JSON operations
from google.cloud import documentai_v1 as documentai # For type hinting
from google.protobuf.json_format import MessageToJson # Added to convert Document object to JSON
from pathlib import Path

from services.documentai_client import process_document_sample

# Configuration from environment variables
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")

def process_pdf(file_path: str) -> documentai.Document | None:
    """
    Processes a given PDF file using Document AI.
    Args:
        file_path: The absolute local path to the PDF document file.
    Returns:
        A documentai.Document object containing the processed information,
        or None if an error occurs or configuration is missing.
    """
    if not all([PROJECT_ID, LOCATION, PROCESSOR_ID]):
        print("ERROR: Missing one or more environment variables: PROJECT_ID, LOCATION, PROCESSOR_ID")
        print(f"Current values: PROJECT_ID='{PROJECT_ID}', LOCATION='{LOCATION}', PROCESSOR_ID='{PROCESSOR_ID}'")
        return None

    if not os.path.exists(file_path):
        print(f"ERROR: The file '{file_path}' does not exist.")
        return None

    print(f"INFO: Processing PDF: {file_path}...")
    print(f"INFO: Using Project ID: {PROJECT_ID}, Location: {LOCATION}, Processor ID: {PROCESSOR_ID}")

    mime_type = "application/pdf"
    document_object = process_document_sample(
        project_id=PROJECT_ID,
        location=LOCATION,
        processor_id=PROCESSOR_ID,
        file_path=file_path,
        mime_type=mime_type
    )
    return document_object

def save_document_as_json(
    document_object: documentai.Document, 
    output_directory_path: str, 
    input_pdf_filename: str
):
    """
    Saves the processed Document AI object as a JSON file.
    Args:
        document_object: The documentai.Document object to save.
        output_directory_path: The absolute path to the directory where the JSON file will be saved.
        input_pdf_filename: The base name of the input PDF file (e.g., "sample.pdf").
    """
    try:
        # Ensure the output directory exists
        os.makedirs(output_directory_path, exist_ok=True)

        # Construct the output filename
        base_name, _ = os.path.splitext(input_pdf_filename)
        output_filename = f"{base_name}_doc_ai_output.json"
        output_filepath = os.path.join(output_directory_path, output_filename)

        # Convert Document object to JSON string
        json_string = MessageToJson(document_object._pb) # Access the underlying protobuf message

        # Write JSON string to file
        with open(output_filepath, "w", encoding="utf-8") as f:
            json.dump(json.loads(json_string), f, indent=4) # Pretty print JSON

        print(f"INFO: Successfully saved Document AI output to: {output_filepath}")

    except Exception as e:
        print(f"ERROR: Failed to save document output as JSON. Error: {e}")