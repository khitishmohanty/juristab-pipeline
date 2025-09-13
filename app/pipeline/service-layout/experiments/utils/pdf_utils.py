import fitz

def _create_temp_page_pdf(pdf_document: fitz.Document, page_index: int, temp_pdf_page_path: str) -> None:
    """Creates a temporary single-page PDF."""
    single_page_doc = fitz.open()
    single_page_doc.insert_pdf(pdf_document, from_page=page_index, to_page=page_index)
    single_page_doc.save(temp_pdf_page_path)
    single_page_doc.close()
    
    
def _create_temp_chunk_pdf(original_pdf_doc: fitz.Document, 
                           start_page_index: int, 
                           num_pages_in_chunk: int, 
                           output_chunk_pdf_path: str):
    """
    Creates a new PDF document containing a chunk of pages from the original document.
    """
    new_pdf_doc = fitz.open()  # Create a new empty PDF
    # Determine the actual end page index in the original document
    # to avoid going out of bounds.
    # PyMuPDF uses 0-based indexing for pages.
    end_page_index_in_original = min(start_page_index + num_pages_in_chunk, len(original_pdf_doc))
    
    if start_page_index < end_page_index_in_original: # Ensure there are pages to copy
        new_pdf_doc.insert_pdf(original_pdf_doc, 
                               from_page=start_page_index, 
                               to_page=end_page_index_in_original - 1) # to_page is inclusive
    
    new_pdf_doc.save(output_chunk_pdf_path)
    new_pdf_doc.close()
    # print(f"Temporary chunk PDF created: {output_chunk_pdf_path} with pages from original index {start_page_index} to {end_page_index_in_original -1}")
