from bs4 import BeautifulSoup, Comment
import logging
import re

logger = logging.getLogger(__name__)

class HtmlContentExtractor:
    """
    Extracts actual content text from HTML, excluding notes, scripts, styles, 
    and other non-content elements.
    """
    
    def __init__(self):
        # Tags to skip entirely
        self.skip_tags = ['script', 'style', 'meta', 'link', 'head', 'nav']
        
        # Class names that typically indicate notes or marginal content
        self.note_classes = [
            'note', 'marginal-note', 'sidenote', 'footnote', 
            'annotation', 'comment', 'aside', 'editorial'
        ]
        
        # ID patterns that typically indicate notes
        self.note_id_patterns = [
            r'note[-_]?\d*',
            r'footnote[-_]?\d*',
            r'marginal[-_]note[-_]?\d*'
        ]
    
    def _is_note_element(self, element) -> bool:
        """
        Check if an element is a note or marginal content.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            bool: True if element is a note, False otherwise
        """
        if not hasattr(element, 'name') or not element.name:
            return False
        
        # Check class attribute
        classes = element.get('class', [])
        if any(note_class in ' '.join(classes).lower() for note_class in self.note_classes):
            return True
        
        # Check id attribute
        element_id = element.get('id', '')
        if element_id:
            if any(re.match(pattern, element_id.lower()) for pattern in self.note_id_patterns):
                return True
        
        # Check if parent has note-related class/id
        if hasattr(element, 'parent') and element.parent:
            parent_classes = element.parent.get('class', [])
            if any(note_class in ' '.join(parent_classes).lower() for note_class in self.note_classes):
                return True
        
        return False
    
    def _should_skip_element(self, element) -> bool:
        """
        Determine if an element should be skipped during text extraction.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            bool: True if element should be skipped
        """
        # Skip comments
        if isinstance(element, Comment):
            return True
        
        # Skip specific tags
        if hasattr(element, 'name') and element.name in self.skip_tags:
            return True
        
        # Skip navigation elements
        if hasattr(element, 'name') and element.name == 'nav':
            return True
        
        # Check for navigator ID (from juriscontent)
        if hasattr(element, 'get') and element.get('id') == 'navigator':
            return True
        
        # Skip note elements
        if self._is_note_element(element):
            logger.debug(f"Skipping note element: {element.name}")
            return True
        
        return False
    
    def strip_barnet_jade_header(self, text: str) -> str:
        """
        Strip the BarNet Jade header from the beginning of the text if present.
        
        Args:
            text (str): Text that may contain the header
            
        Returns:
            str: Text with header removed
        """
        pattern = r'^Content\s+extract\s*-\s*BarNet\s+Jade\s*'
        stripped_text = re.sub(pattern, '', text, count=1, flags=re.IGNORECASE)
        
        if stripped_text != text:
            logger.debug("Stripped 'Content extract - BarNet Jade' header from HTML-extracted text")
        
        return stripped_text
    
    def extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text content from HTML, excluding notes and non-content elements.
        
        Args:
            html_content: The HTML content to extract text from
            
        Returns:
            str: Extracted text content
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove all elements we want to skip
            for tag in self.skip_tags:
                for element in soup.find_all(tag):
                    element.decompose()
            
            # Remove comments
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
            
            # Remove note elements
            for note_class in self.note_classes:
                for element in soup.find_all(class_=re.compile(note_class, re.IGNORECASE)):
                    logger.debug(f"Removing note element with class: {note_class}")
                    element.decompose()
            
            # Extract text from remaining content
            text_parts = []
            
            # Get the main content area if it exists
            main_content = soup.find('main', id='content')
            if not main_content:
                main_content = soup.find('body')
            if not main_content:
                main_content = soup
            
            # Walk through all text nodes
            for element in main_content.descendants:
                # Skip elements that should be excluded
                if self._should_skip_element(element):
                    continue
                
                # Extract text from text nodes
                if isinstance(element, str) and not isinstance(element, Comment):
                    text = element.strip()
                    if text and len(text) > 1:
                        # Check if parent should be skipped
                        if hasattr(element, 'parent') and not self._should_skip_element(element.parent):
                            text_parts.append(text)
            
            # Join all text parts
            extracted_text = '\n'.join(text_parts)
            
            # Clean up excessive whitespace
            extracted_text = re.sub(r'\n{3,}', '\n\n', extracted_text)
            extracted_text = re.sub(r' {2,}', ' ', extracted_text)
            
            logger.info(f"Extracted {len(extracted_text)} characters from HTML")
            logger.debug(f"Extracted {len(text_parts)} text segments")
            
            return extracted_text.strip()
            
        except Exception as e:
            logger.error(f"Error extracting text from HTML: {e}", exc_info=True)
            raise