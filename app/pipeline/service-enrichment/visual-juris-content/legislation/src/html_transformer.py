# src/html_transformer.py

from bs4 import BeautifulSoup
from src.juriscontent_generator import JuriscontentGenerator
from src.headless_html_processor import HeadlessHtmlProcessor
from src.heading_hierarchy_processor import HeadingHierarchyProcessor

class HtmlTransformer:
    """
    Orchestrates the HTML transformation pipeline.
    
    It first applies intelligent heading hierarchy rules, then detects if an HTML 
    document has existing heading tags. If not, it uses the HeadlessHtmlProcessor 
    to inject them before passing the result to the standard JuriscontentGenerator.
    """
    def __init__(self, headless_rules_config: dict, heading_hierarchy_rules_path: str):
        """
        Initialize the transformer with configuration paths.
        
        Args:
            headless_rules_config (dict): Configuration for headless HTML processing
            heading_hierarchy_rules_path (str): Path to heading hierarchy rules config
        """
        self.headless_processor = HeadlessHtmlProcessor(rules_config=headless_rules_config)
        self.hierarchy_processor = HeadingHierarchyProcessor(heading_hierarchy_rules_path)
        self.juriscontent_generator = JuriscontentGenerator()

    def _has_headings(self, soup: BeautifulSoup) -> bool:
        """Check if the document contains any h1-h6 tags."""
        return bool(soup.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']))

    def transform(self, html_content: str) -> str:
        """
        Processes the HTML, applying the appropriate logic.
        
        Processing pipeline:
        1. Apply intelligent heading hierarchy rules (Chapter/Part/Division patterns)
        2. Check if semantic headings exist
        3. If no headings, apply headless processing rules
        4. Apply standard juriscontent generation
        """
        # Step 1: Apply intelligent heading hierarchy rules
        print("INFO: Applying intelligent heading hierarchy rules (Chapter/Part/Division patterns).")
        processed_html = self.hierarchy_processor.process_document(html_content)
        
        # Step 2: Check if we now have semantic headings
        soup = BeautifulSoup(processed_html, 'html.parser')
        
        if not self._has_headings(soup):
            print("INFO: No semantic headings found after hierarchy processing. Applying headless processing rules.")
            processed_html = self.headless_processor.process(processed_html)
        else:
            print("INFO: Semantic headings found after hierarchy processing. Proceeding with juriscontent generation.")
        
        # Step 3: Apply the standard layout and styling
        return self.juriscontent_generator.generate(processed_html)
    
    def get_hierarchy_rules_summary(self) -> str:
        """Get a summary of the loaded heading hierarchy rules."""
        return self.hierarchy_processor.get_rules_summary()