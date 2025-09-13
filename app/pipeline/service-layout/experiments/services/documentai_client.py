import os
import sys

from google.cloud import documentai_v1 as documentai # For type hinting
#from services.document_ai_client import process_document_sample, print_document_output, get_text_from_layout
from dotenv import load_dotenv
from pathlib import Path

from google.cloud import documentai_v1 as documentai
import os

def process_document_sample(
    project_id: str,
    location: str,
    processor_id: str,
    file_path: str,
    mime_type: str = "application/pdf"
) -> documentai.Document | None:
    """
    Processes a document using Google Document AI.

    Args:
        project_id: Your Google Cloud project ID.
        location: The Cloud location of the processor (e.g., 'us', 'eu').
        processor_id: The ID of the Document AI processor.
        file_path: The local path to the document file.
        mime_type: The MIME type of the document (e.g., 'application/pdf', 'image/jpeg').
                   Defaults to 'application/pdf'.

    Returns:
        A documentai.Document object containing the processed information,
        or None if an error occurs.
    """
    try:
        # You must set the GOOGLE_APPLICATION_CREDENTIALS environment variable
        # to the path of your service account key file for authentication.
        # Example:
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/service-account-key.json"
        #
        # Alternatively, if running on Google Cloud services like Cloud Functions,
        # GCE, etc., authentication is often handled automatically.

        # Instantiates a client
        opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
        client = documentai.DocumentProcessorServiceClient(client_options=opts)

        # The full resource name of the processor, e.g.:
        # projects/{project_id}/locations/{location}/processors/{processor_id}
        name = client.processor_path(project_id, location, processor_id)

        # Read the file into memory
        with open(file_path, "rb") as image:
            image_content = image.read()

        # Load Binary Data into Document AI RawDocument Object
        raw_document = documentai.RawDocument(
            content=image_content, mime_type=mime_type
        )

        # Configure the process request
        request = documentai.ProcessRequest(name=name, raw_document=raw_document)

        # Use the Document AI client to process the sample document
        result = client.process_document(request=request)

        # The result.document object contains the processed information.
        return result.document

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def print_document_output(doc_object: documentai.Document):
    """
    Prints some basic information from the processed Document AI object.

    Args:
        doc_object: The documentai.Document object returned by process_document_sample.
    """
    if not doc_object:
        print("No document object to process.")
        return

    print("Document AI Processed Output:\n")

    # Full text of the document
    print(f"Full Text:\n{doc_object.text}\n")
    print("-" * 30)

    # Information about pages and paragraphs
    print(f"Number of pages: {len(doc_object.pages)}\n")
    for i, page in enumerate(doc_object.pages):
        print(f"--- Page {i + 1} ---")
        print(f"  Dimensions: {page.dimension.width}x{page.dimension.height} {page.dimension.unit}")
        print(f"  Detected {len(page.paragraphs)} paragraphs.")
        for j, paragraph in enumerate(page.paragraphs):
            paragraph_text = get_text_from_layout(paragraph.layout, doc_object.text)
            print(f"    Paragraph {j + 1}: {paragraph_text.strip()}")
        
        # You can similarly access page.lines, page.tokens, page.tables, page.form_fields etc.
        # For example, to print form fields (if your processor extracts them):
        if page.form_fields:
            print(f"\n  Form Fields on Page {i + 1}:")
            for field in page.form_fields:
                field_name = get_text_from_layout(field.field_name.layout, doc_object.text).strip()
                field_value = get_text_from_layout(field.field_value.layout, doc_object.text).strip()
                print(f"    Field Name: '{field_name}', Value: '{field_value}'")
                if field.field_name.confidence:
                    print(f"      Name Confidence: {field.field_name.confidence:.2f}")
                if field.field_value.confidence:
                     print(f"      Value Confidence: {field.field_value.confidence:.2f}")
        print("\n")
    print("-" * 30)

    # Information about entities (if your processor extracts them)
    if doc_object.entities:
        print(f"Detected {len(doc_object.entities)} entities.\n")
        for entity in doc_object.entities:
            entity_type = entity.type_
            entity_text = entity.mention_text or get_text_from_layout(entity.text_anchor.text_segments[0] if entity.text_anchor and entity.text_anchor.text_segments else None, doc_object.text) # Fallback for older processors
            print(f"  Entity Type: {entity_type}, Text: '{entity_text.strip()}', Confidence: {entity.confidence:.2f}")
            # Normalized value (if available, e.g., for dates, money)
            if entity.normalized_value:
                print(f"    Normalized Value: {entity.normalized_value.text} ({entity.normalized_value.money_value or entity.normalized_value.date_value or entity.normalized_value.datetime_value})")

    else:
        print("No entities detected by this processor, or entities are not applicable.\n")


def get_text_from_layout(layout: documentai.Document.Page.Layout, text: str) -> str:
    """
    Helper function to extract text segments from the document text based on layout information.
    """
    response = ""
    if layout and layout.text_anchor and layout.text_anchor.text_segments:
        for segment in layout.text_anchor.text_segments:
            start_index = int(segment.start_index)
            end_index = int(segment.end_index)
            response += text[start_index:end_index]
    return response


    
try:
    # Get the directory of the current script (handler.py)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Get the parent directory 'app/functions/'
    functions_dir = os.path.dirname(current_script_dir)
    
    if functions_dir not in sys.path:
        sys.path.insert(0, functions_dir) # Prepend to sys.path to prioritize this path

    #from services.document_ai_client import process_document_sample, print_document_output, get_text_from_layout
    print(f"INFO: Successfully imported Document AI client from: {os.path.join(functions_dir, 'services')}")

except ImportError as e_adjusted:
    print(f"ERROR: Could not import from 'services.document_ai_client' using adjusted sys.path ({functions_dir}). Error: {e_adjusted}")
    print("INFO: Attempting fallback import methods (e.g., relative or direct)...")
    try:
        #from services.document_ai_client import process_document_sample, print_document_output, get_text_from_layout
        print("INFO: Successfully imported using 'from services.document_ai_client' (fallback).")
    except ImportError as e_fallback_services:
        print(f"ERROR: Could not import 'from services.document_ai_client' (fallback). Error: {e_fallback_services}")
        try:
            from app.services.documentai_client import process_document_sample, print_document_output, get_text_from_layout
            print("INFO: Successfully imported directly (document_ai_client.py in same directory as handler.py).")
        except ImportError as e_fallback_direct:
            print(f"ERROR: Failed to import 'document_ai_client' via all attempted methods. Error: {e_fallback_direct}")
            print("CRITICAL: Document AI client functions could not be loaded. The script will likely fail.")
            def process_document_sample(*args, **kwargs):
                print("CRITICAL DUMMY: process_document_sample not imported.")
                return None
            def print_document_output(*args, **kwargs):
                print("CRITICAL DUMMY: print_document_output not imported.")
            def get_text_from_layout(*args, **kwargs):
                print("CRITICAL DUMMY: get_text_from_layout not imported.")
                return ""
# --- End of Import Logic ---
# --- Load Environment Variables from .env file ---
# Assuming your .env file is in the project root directory (e.g., 'docudive-juristab')
# handler.py is in 'docudive-juristab/app/functions/doc-layout-extraction-DAI/'
# We need to go up three levels from current_script_dir to reach the project root.
try:
    if 'current_script_dir' not in globals(): # Define if not defined in import block (e.g. due to import error)
        current_script_dir = os.path.dirname(os.path.abspath(__file__))

    project_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_script_dir)))
    dotenv_path = os.path.join(project_root_dir, '.env')

    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
        print(f"INFO: Loaded environment variables from: {dotenv_path}")
    else:
        print(f"WARNING: .env file not found at {dotenv_path}. Relying on shell environment variables.")
        # Fallback to default load_dotenv() which looks in CWD or goes up.
        # This might find it if .env is placed in 'doc-layout-extraction-DAI' or its parents.
        if not load_dotenv():
             print(f"WARNING: load_dotenv() could not find a .env file automatically.")
except Exception as e_dotenv:
    print(f"WARNING: Error loading .env file: {e_dotenv}. Relying on shell environment variables.")
# --- End of Load Environment Variables ---

