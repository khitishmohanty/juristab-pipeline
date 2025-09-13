from bs4 import BeautifulSoup
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MetadataExtractor:
    """
    Extracts structured metadata from a plain text or HTML case law summary file.
    """
    def __init__(self, field_mapping):
        # Mapping from HTML table label to database column name
        self.field_mapping = field_mapping

    def _clean_text(self, text):
        """
        Cleans up extracted text by stripping whitespace, replacing newlines,
        and returning None for common placeholder values.
        """
        if text:
            text = text.strip()
            text = re.sub(r'\s+', ' ', text)
            # If text is a common placeholder, return None so it will be ignored
            if text.lower() in ['n/a', '[not available]', '[n/a]', 'not applicable']:
                return None
        return text

    def _extract_counsel_firm_mapping(self, representation_text):
        """
        Parses the 'Representation' field to create a list of counsel and firm mappings.
        
        Args:
            representation_text (str): The text from the 'Representation' field.
            
        Returns:
            list: A list of dictionaries, where each dict contains 'counsel' and 'law_firm_agency'.
        """
        if not representation_text:
            return []

        final_mappings = []
        counsel_name = None
        
        # Split by newline for text files
        representation_lines = [self._clean_text(line) for line in representation_text.splitlines() if self._clean_text(line)]

        for line in representation_lines:
            if 'For the ' in line or 'Self-represented' in line:
                if counsel_name:
                    final_mappings.append({'counsel': counsel_name, 'law_firm_agency': None})
                counsel_name = None
                if 'Self-represented' in line:
                    counsel_name = line.replace(' (Self-represented)', '')
                    final_mappings.append({'counsel': counsel_name, 'law_firm_agency': 'Self-represented'})
                    counsel_name = None
            elif 'Mr' in line or 'Ms' in line or 'SC' in line:
                if counsel_name:
                    final_mappings.append({'counsel': counsel_name, 'law_firm_agency': None})
                counsel_name = line
            else:
                if counsel_name:
                    final_mappings.append({'counsel': counsel_name, 'law_firm_agency': line})
                    counsel_name = None
        
        if counsel_name:
            final_mappings.append({'counsel': counsel_name, 'law_firm_agency': None})
            
        return final_mappings


    def extract_from_html(self, file_content):
        """
        MODIFIED: This function now properly extracts data from an HTML table structure.

        Args:
            file_content (str): The raw HTML content of the summary file.

        Returns:
            tuple: A tuple containing a dictionary of metadata and a list of
                   counsel/firm mappings. Returns (None, None) on failure.
        """
        try:
            metadata = {}
            counsel_firm_mappings = []
            soup = BeautifulSoup(file_content, 'html.parser')
            
            # Find the metadata table and iterate through its rows
            table = soup.find('table', class_='metadata')
            if not table:
                logging.warning("Could not find a table with class='metadata' in the HTML content.")
                return None, None
            
            rows = table.find_all('tr')

            for row in rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    # The first cell is the label, the second is the value
                    label = self._clean_text(cells[0].get_text(strip=True))
                    
                    # The value might contain <br> tags, so we join text parts with newlines
                    value = self._clean_text(cells[1].get_text(separator='\n', strip=True))

                    if label and value and label in self.field_mapping:
                        db_column = self.field_mapping[label]
                        metadata[db_column] = value
                        
                        # If we find the "Representation" field, parse it for counsel details
                        if label == "Representation":
                            counsel_firm_mappings = self._extract_counsel_firm_mapping(value)

            logging.info("Successfully extracted metadata from HTML file.")
            return metadata, counsel_firm_mappings

        except Exception as e:
            logging.error(f"An error occurred during HTML extraction: {e}")
            return None, None
