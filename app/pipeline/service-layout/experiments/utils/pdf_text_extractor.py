import os
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
from difflib import SequenceMatcher
import fitz # PyMuPDF 

def extract_text_from_pdf_page(pdf_path: str, page_number: int) -> str:
    """Attempt to extract text from a given PDF page (machine-readable)."""
    # page_number here is 0-indexed for PdfReader
    reader = PdfReader(pdf_path)
    if 0 <= page_number < len(reader.pages):
        text = reader.pages[page_number].extract_text()
        return text or ""
    return ""

def extract_text_from_ocr(pdf_path: str, page_number: int, poppler_path=None) -> str:
    """Render a single page as image and perform OCR to extract text."""
    # page_number is 0-indexed input. convert_from_path expects 1-indexed pages.
    # If pdf_path is a single-page PDF, page_number should be 0, so first/last_page = 1.
    images = convert_from_path(
        pdf_path,
        dpi=300,
        first_page=page_number + 1,
        last_page=page_number + 1,
        poppler_path=poppler_path,
        thread_count=1 # Can help with some intermittent issues on Windows
    )
    return pytesseract.image_to_string(images[0]) if images else ""

def is_fidelity_preserved(text1: str, text2: str, threshold: float = 0.9) -> bool:
    """Check if text2 is similar enough to text1 using SequenceMatcher."""
    return SequenceMatcher(None, text1.strip(), text2.strip()).ratio() >= threshold

def extract_text_and_links_with_fitz(pdf_path: str, page_number: int) -> tuple[str, list[dict]]:
    """
    Extracts full page text and a list of hyperlinks (URL, anchor text, and rectangle)
    from a given PDF page using PyMuPDF (fitz).
    page_number is 0-indexed.
    """
    doc = None
    page_text_content = ""
    hyperlinks_data = []
    try:
        doc = fitz.open(pdf_path)
        if 0 <= page_number < doc.page_count:
            page = doc.load_page(page_number)
            page_text_content = page.get_text("text") or ""
            
            links = page.get_links() # Returns a list of link dicts from fitz
            for link_dict in links:
                if link_dict.get('kind') == fitz.LINK_URI: # Check if it's a URI link
                    uri = link_dict.get('uri')
                    rect = link_dict.get('from_rect') # The fitz.Rect object of the link
                    
                    # Attempt to extract text only from the link's rectangle
                    link_anchor_text = page.get_text("text", clip=rect).strip() if rect else "N/A"
                    
                    if uri:
                        hyperlinks_data.append({
                            "text": link_anchor_text,
                            "url": uri,
                            "rect": [rect.x0, rect.y0, rect.x1, rect.y1] if rect else None
                        })
    except Exception as e:
        print(f"Error processing PDF page {page_number} with fitz in {pdf_path}: {e}")
        # page_text_content and hyperlinks_data will retain their default empty/initial values
    finally:
        if doc:
            doc.close()
            
    return page_text_content, hyperlinks_data

def extract_text_from_pdf_chunk_pypdf2(chunk_pdf_path: str) -> list[str]:
    """Extracts machine-readable text from all pages in a given PDF chunk using PyPDF2."""
    texts_for_pages = []
    try:
        with open(chunk_pdf_path, "rb") as f:
            reader = PdfReader(f)
            for page_num in range(len(reader.pages)):
                page_text = reader.pages[page_num].extract_text()
                texts_for_pages.append(page_text or "")
    except Exception as e:
        print(f"Error reading PDF chunk {chunk_pdf_path} with PyPDF2: {e}")
        # If error, texts_for_pages might be empty or partially filled.
        # Consider how to handle this based on expected number of pages, or let caller check.
    return texts_for_pages

def extract_text_from_chunk_ocr(chunk_pdf_path: str, poppler_path=None) -> list[str]:
    """Renders all pages in a PDF chunk as images and performs OCR on each."""
    texts_for_pages = []
    try:
        images = convert_from_path(
            chunk_pdf_path,
            dpi=300,
            poppler_path=poppler_path,
            thread_count=1
        )
        for i, image in enumerate(images):
            try:
                texts_for_pages.append(pytesseract.image_to_string(image) or "")
            except Exception as ocr_e:
                print(f"Error during OCR for page {i} in chunk {chunk_pdf_path}: {ocr_e}")
                texts_for_pages.append("") # Add empty string for failed OCR page
    except Exception as e:
        print(f"Error converting PDF chunk {chunk_pdf_path} to images for OCR: {e}")
        # If conversion fails, texts_for_pages will be empty.
    return texts_for_pages

# is_fidelity_preserved function remains the same as it's a general utility

def extract_text_and_links_from_chunk_fitz(chunk_pdf_path: str) -> list[tuple[str, list[dict]]]:
    """
    Extracts full page text and hyperlinks from all pages in a PDF chunk using PyMuPDF.
    Returns a list of tuples, one for each page: (page_text, list_of_hyperlinks_on_page).
    """
    doc = None
    all_pages_data = [] # List of (page_text, links_for_page)
    try:
        doc = fitz.open(chunk_pdf_path)
        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            page_text_content = page.get_text("text") or ""
            
            hyperlinks_data_for_page = []
            links = page.get_links() 
            for link_dict in links:
                if link_dict.get('kind') == fitz.LINK_URI:
                    uri = link_dict.get('uri')
                    # CRITICAL: Use 'from' for the rectangle, not 'from_rect' for fitz link dict
                    rect = link_dict.get('from') 
                    
                    link_anchor_text = "N/A"
                    if rect: # Ensure rect is not None before using it
                        try:
                            link_anchor_text = page.get_text("text", clip=rect).strip()
                        except Exception as clip_e:
                             print(f"Warning: could not extract text for link clip on page {page_num} of {chunk_pdf_path}: {clip_e}")
                    
                    if uri:
                        hyperlinks_data_for_page.append({
                            "text": link_anchor_text,
                            "url": uri,
                            "rect": [rect.x0, rect.y0, rect.x1, rect.y1] if rect else None
                        })
            all_pages_data.append((page_text_content, hyperlinks_data_for_page))
    except Exception as e:
        print(f"Error processing PDF chunk {chunk_pdf_path} with fitz: {e}")
        # all_pages_data might be empty or partially filled.
    finally:
        if doc:
            doc.close()
    return all_pages_data