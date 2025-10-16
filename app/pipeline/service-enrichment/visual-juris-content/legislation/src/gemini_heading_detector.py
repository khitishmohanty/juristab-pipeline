import json
import logging
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup, Tag, NavigableString
import re
from utils.gemini_client import GeminiClient

logger = logging.getLogger(__name__)

class GeminiHeadingDetector:
    """
    Uses Gemini AI to detect headings and subheadings in HTML content
    when no semantic heading tags are present.
    
    Uses visual styling cues (bold text, font size, positioning) to identify
    structural elements in Australian legislation documents.
    """
    
    def __init__(self, model_name: str, prompt_path: str):
        """
        Initialize the detector.
        
        Args:
            model_name: Name of Gemini model to use
            prompt_path: Path to the prompt template file
        """
        self.gemini_client = GeminiClient(model_name)
        self.prompt = self._load_prompt(prompt_path)
    
    def _load_prompt(self, prompt_path: str) -> str:
        """Load the heading detection prompt from file."""
        try:
            with open(prompt_path, 'r') as f:
                prompt = f.read()
            logger.info(f"Loaded heading detection prompt from {prompt_path}")
            return prompt
        except Exception as e:
            logger.error(f"Failed to load prompt from {prompt_path}: {e}")
            raise
    
    def detect_headings(self, html_content: str) -> Tuple[Optional[List[Dict]], int, int]:
        """
        Detect headings using Gemini AI based on visual styling cues.
        
        Args:
            html_content: The HTML content to analyze
            
        Returns:
            Tuple of (headings_list, input_tokens, output_tokens)
        """
        try:
            logger.info("Sending HTML to Gemini for heading detection (style-based analysis)")
            
            # Send the full HTML including styling information
            json_response, input_tokens, output_tokens = self.gemini_client.detect_headings(
                self.prompt, html_content
            )
            
            # Validate JSON response
            if not self.gemini_client.is_valid_json(json_response):
                logger.error("Invalid JSON response from Gemini")
                logger.debug(f"Raw response: {json_response[:500]}...")
                return None, input_tokens, output_tokens
            
            headings_data = json.loads(json_response)
            
            # Validate structure
            if not isinstance(headings_data, dict) or 'headings' not in headings_data:
                logger.error("Invalid heading data structure from Gemini")
                logger.debug(f"Response structure: {headings_data}")
                return None, input_tokens, output_tokens
            
            headings_list = headings_data['headings']
            
            if not headings_list:
                logger.warning("Gemini returned empty headings list")
                return None, input_tokens, output_tokens
            
            # Validate and filter headings
            validated_headings = []
            for idx, h in enumerate(headings_list):
                text_len = len(h['text'])
                
                # Reject unreasonably long headings (likely content, not headings)
                if text_len > 500:
                    logger.warning(f"Rejecting overly long heading ({text_len} chars): {h['text'][:100]}...")
                    continue
                
                # Reject empty headings
                if text_len < 2:
                    logger.warning(f"Rejecting empty or too-short heading")
                    continue
                
                validated_headings.append(h)
            
            if len(validated_headings) < len(headings_list):
                logger.info(f"Filtered out {len(headings_list) - len(validated_headings)} invalid headings")
            
            if not validated_headings:
                logger.warning("No valid headings after filtering")
                return None, input_tokens, output_tokens
            
            # Log detected headings with styling context
            logger.info(f"Successfully detected {len(validated_headings)} valid headings")
            logger.info("=" * 80)
            
            # Check heading distribution
            h1_count = sum(1 for h in validated_headings if h['level'] == 'h1')
            h2_count = sum(1 for h in validated_headings if h['level'] == 'h2')
            h3_count = sum(1 for h in validated_headings if h['level'] == 'h3')
            
            logger.info(f"Heading distribution: H1={h1_count}, H2={h2_count}, H3={h3_count}")
            logger.info("-" * 80)
            
            for idx, heading in enumerate(validated_headings[:15]):  # Log first 15
                reasoning = heading.get('reasoning', 'No reasoning provided')
                logger.info(f"  {idx+1}. [{heading['level'].upper()}] pos={heading['position']}")
                logger.info(f"     Text: {heading['text'][:100]}{'...' if len(heading['text']) > 100 else ''}")
                logger.debug(f"     Reasoning: {reasoning}")
            if len(validated_headings) > 15:
                logger.info(f"  ... and {len(validated_headings) - 15} more headings")
            logger.info("=" * 80)
            
            return validated_headings, input_tokens, output_tokens
            
        except Exception as e:
            logger.error(f"Error during heading detection: {e}", exc_info=True)
            return None, 0, 0
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison by removing extra whitespace and special characters."""
        # Replace HTML entities
        text = text.replace('&nbsp;', ' ').replace('\u00a0', ' ')
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        # Strip leading/trailing whitespace
        return text.strip()
    
    def _find_exact_text_element(self, soup: BeautifulSoup, search_text: str) -> Optional[Tag]:
        """
        Find an element containing the exact search text.
        
        For Australian legislation, handles special cases:
        - Multi-line titles spread across multiple <p> tags
        - Section numbers in separate <span> or <b> tags from section text
        - Text split across adjacent elements
        
        Args:
            soup: BeautifulSoup object
            search_text: Text to search for
            
        Returns:
            The element containing the text, or None
        """
        normalized_search = self._normalize_text(search_text)
        logger.debug(f"Searching for: '{normalized_search[:80]}...'")
        
        # Strategy 1: Exact match in a single element
        for tag_name in ['p', 'b', 'strong', 'span', 'div', 'td', 'th', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            elements = soup.find_all(tag_name)
            for element in elements:
                element_text = self._normalize_text(element.get_text())
                
                # Exact match
                if element_text == normalized_search:
                    logger.debug(f"✓ Found exact match in <{tag_name}>")
                    return element
        
        # Strategy 2: Search text starts the element (for section headings)
        for tag_name in ['p', 'div', 'td']:
            elements = soup.find_all(tag_name)
            for element in elements:
                element_text = self._normalize_text(element.get_text())
                
                # Check if search text is at the start
                if element_text.startswith(normalized_search):
                    # Make sure it's not just a partial match of a much longer paragraph
                    # Allow some extra text but not too much (within 50 chars is reasonable for section headers)
                    if len(element_text) - len(normalized_search) < 50:
                        logger.debug(f"✓ Found starting match in <{tag_name}> (within tolerance)")
                        return element
        
        # Strategy 3: Multi-line title (for Act titles spread across multiple <p> tags)
        # Check if search text can be constructed by combining consecutive <p> or <b> tags
        all_p_tags = soup.find_all('p')
        for i, p_tag in enumerate(all_p_tags):
            # Try combining this tag with the next few tags
            combined_text = self._normalize_text(p_tag.get_text())
            
            # Try combining up to 5 subsequent tags
            for j in range(i + 1, min(i + 6, len(all_p_tags))):
                next_tag = all_p_tags[j]
                combined_text += ' ' + self._normalize_text(next_tag.get_text())
                
                if self._normalize_text(combined_text) == normalized_search:
                    logger.debug(f"✓ Found multi-line match across {j-i+1} <p> tags")
                    # Return the first tag - we'll need to handle multi-line in apply_headings
                    return p_tag
        
        # Strategy 4: Fuzzy match for long headings (80% word overlap)
        search_words = set(normalized_search.lower().split())
        if len(search_words) > 4:  # Only for longer text
            for tag_name in ['p', 'div', 'td', 'b', 'strong']:
                elements = soup.find_all(tag_name)
                for element in elements:
                    element_text = self._normalize_text(element.get_text())
                    element_words = set(element_text.lower().split())
                    
                    if search_words and element_words:
                        overlap = len(search_words & element_words) / len(search_words)
                        if overlap >= 0.85:
                            logger.debug(f"✓ Found fuzzy match in <{tag_name}> ({overlap*100:.0f}% overlap)")
                            return element
        
        logger.warning(f"✗ Could not find text in HTML: '{normalized_search[:80]}...'")
        return None
    
    def _create_heading_from_element(self, soup: BeautifulSoup, element: Tag, 
                                     heading_level: str) -> Tag:
        """
        Create a new heading tag and populate it with the element's content.
        
        Args:
            soup: BeautifulSoup object
            element: The source element
            heading_level: The heading level (h1, h2, h3, etc.)
            
        Returns:
            New heading tag
        """
        new_heading = soup.new_tag(heading_level)
        
        # Copy all attributes except class
        for attr, value in element.attrs.items():
            if attr not in ['class', 'style']:
                new_heading[attr] = value
        
        # Move all children from element to heading
        for child in list(element.children):
            new_heading.append(child.extract())
        
        return new_heading
    
    def apply_headings_to_html(self, html_content: str, headings_list: List[Dict]) -> str:
        """
        Apply detected headings to HTML content by wrapping text in heading tags.
        
        This method finds each heading text in the HTML and wraps it with
        the appropriate heading tag (h1, h2, h3, etc.).
        
        Args:
            html_content: Original HTML
            headings_list: List of detected headings from Gemini
            
        Returns:
            Modified HTML with heading tags inserted
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Sort headings by position to process in order
        sorted_headings = sorted(headings_list, key=lambda x: x.get('position', 0))
        
        headings_applied = 0
        headings_failed = 0
        
        for idx, heading_info in enumerate(sorted_headings):
            text = heading_info['text']
            level = heading_info['level']
            
            logger.debug(f"Processing heading {idx+1}/{len(sorted_headings)}: [{level}] {text[:60]}...")
            
            # Find the element containing this text
            found_element = self._find_exact_text_element(soup, text)
            
            if not found_element:
                logger.warning(f"Could not find heading in HTML: {text[:60]}...")
                headings_failed += 1
                continue
            
            # Don't convert if already a heading
            if found_element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                logger.debug(f"Element already is a heading ({found_element.name}), skipping")
                continue
            
            # Create new heading tag
            new_heading = self._create_heading_from_element(soup, found_element, level)
            
            # Replace the found element with the heading
            found_element.replace_with(new_heading)
            headings_applied += 1
            
            logger.debug(f"✓ Applied {level} tag")
        
        logger.info(f"Successfully applied {headings_applied}/{len(sorted_headings)} headings to HTML")
        if headings_failed > 0:
            logger.warning(f"Failed to apply {headings_failed} headings - text not found in HTML")
        
        return str(soup)