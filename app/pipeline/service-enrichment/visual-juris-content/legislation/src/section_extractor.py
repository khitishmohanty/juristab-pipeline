from bs4 import BeautifulSoup, NavigableString, Comment
from typing import List, Dict, Optional
import re
import logging

logger = logging.getLogger(__name__)

class SectionExtractor:
    """
    Extracts sections from juriscontent.html based on heading structure.
    
    CORE LOGIC (OPTION A):
    1. Content BEFORE first H1 heading → Section 1 (extract until first H1 or EOF)
    2. Each H1 + ALL content until next H1 → Separate sections (extract until next H1 or EOF)
    """
    
    def __init__(self):
        pass
    
    def extract_sections(self, html_content: str) -> List[Dict[str, any]]:
        """
        Extract sections from HTML.
        
        Logic:
        1. Find all H1 headings
        2. Extract content BEFORE first H1 (from start until first H1) → Section 1
        3. For each H1: extract H1 text + ALL content until next H1 (or EOF) → Sections 2, 3, etc.
        4. If no H1 headings: return entire document as one section
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        main_content = self._find_main_content(soup)
        
        # Find H1 headings (primary sections for Option A)
        h1_headings = self._find_content_headings(main_content, ['h1'])
        
        if h1_headings:
            logger.info(f"Found {len(h1_headings)} H1 headings")
            return self._extract_sections_from_h1(main_content, h1_headings)
        else:
            # No H1 headings - try H2
            h2_headings = self._find_content_headings(main_content, ['h2'])
            if h2_headings:
                logger.info(f"Found {len(h2_headings)} H2 headings")
                return self._extract_sections_from_h1(main_content, h2_headings)
            else:
                # No headings at all - return entire document
                logger.warning("No H1 or H2 headings found - extracting entire document as one section")
                return self._extract_entire_document(main_content)
    
    def _find_main_content(self, soup: BeautifulSoup):
        """Find the main content area, excluding navigation."""
        main = soup.find('main', id='content')
        if main:
            return main
        
        body = soup.find('body')
        if body:
            return body
        
        return soup
    
    def _find_content_headings(self, main_content, heading_tags: List[str]) -> List:
        """Find headings, excluding those in navigation."""
        all_headings = main_content.find_all(heading_tags)
        
        content_headings = []
        for heading in all_headings:
            # Skip if inside navigation
            is_in_nav = False
            for parent in heading.parents:
                if hasattr(parent, 'name') and parent.name == 'nav':
                    is_in_nav = True
                    break
                if hasattr(parent, 'get') and parent.get('id') == 'navigator':
                    is_in_nav = True
                    break
            
            if not is_in_nav:
                content_headings.append(heading)
        
        return content_headings
    
    def _extract_sections_from_h1(self, main_content, h1_headings: List) -> List[Dict]:
        """
        Extract sections based on H1 headings (Option A).
        
        CRITICAL LOGIC:
        - Section 1: ALL content from start until first H1
        - Section 2: First H1 text + ALL content until second H1 (or EOF)
        - Section 3: Second H1 text + ALL content until third H1 (or EOF)
        - etc.
        """
        sections = []
        section_id = 1
        
        # STEP 1: Extract ALL content BEFORE first H1 heading
        # Extract from start of main_content until first H1
        first_h1 = h1_headings[0]
        content_before = self._extract_text_between_elements(main_content, None, first_h1)
        
        if content_before.strip():
            content_length = len(content_before.strip())
            logger.info(f"Found content before first H1 heading ({content_length} chars)")
            
            sections.append({
                'section_id': section_id,
                'content': content_before,
                'heading': None
            })
            section_id += 1
        else:
            logger.info("No content before first H1 heading")
        
        # STEP 2: For each H1, extract H1 text + ALL content until next H1 (or EOF)
        for i, h1 in enumerate(h1_headings):
            # Get H1 text
            h1_text = self._get_heading_text(h1)
            
            # Determine the next H1 (or None if this is the last one)
            next_h1 = h1_headings[i + 1] if i + 1 < len(h1_headings) else None
            
            # Extract ALL content from after this H1 until next H1 (or EOF)
            content = self._extract_text_between_elements(main_content, h1, next_h1)
            
            # Combine heading + content
            full_content = h1_text + '\n\n' + content if content.strip() else h1_text
            
            sections.append({
                'section_id': section_id,
                'content': full_content,
                'heading': h1_text
            })
            
            content_length = len(content) if content else 0
            logger.debug(f"Section {section_id}: H1 '{h1_text[:40]}...' + {content_length} chars content")
            section_id += 1
        
        if not sections:
            logger.warning("No sections extracted - falling back to entire document")
            return self._extract_entire_document(main_content)
        
        # Verify we have all content
        total_chars = sum(len(s['content']) for s in sections)
        logger.info(f"Extracted {len(sections)} sections with {total_chars} total characters")
        
        return sections
    
    def _extract_text_between_elements(self, container, start_element: Optional, stop_element: Optional) -> str:
        """
        Extract ALL text content between start_element and stop_element.
        
        ENHANCED: Now properly extracts ALL content including:
        - Text in <inline> tags (definition terms, etc.)
        - Text in <block> tags (subclauses, etc.)
        - Text in all paragraph and formatting elements
        - Nested content at any depth
        
        Args:
            container: The container to search in (main content area)
            start_element: Element to start AFTER (None = start from beginning)
            stop_element: Element to STOP AT (None = go to end)
            
        Returns:
            All text content between start and stop
        """
        text_parts = []
        started = (start_element is None)  # If no start element, begin immediately
        
        # Walk through EVERY element in the container in document order
        for element in container.descendants:
            
            # Check if we should start collecting
            if not started:
                if element == start_element:
                    started = True
                    continue  # Skip the start element itself
                else:
                    continue  # Haven't reached start yet
            
            # Check if we should stop collecting
            if stop_element and element == stop_element:
                # We've hit the next H1 - stop here
                break
            
            # CRITICAL: Skip text that's inside the start_element 
            # (to avoid duplicating the H1 heading text)
            if start_element and self._is_inside_element(element, start_element):
                continue
            
            # Skip navigation elements
            if self._should_skip_element(element):
                continue
            
            # Skip HTML comments
            if isinstance(element, Comment):
                continue
            
            # ENHANCED: Collect text from ALL text-containing elements
            if isinstance(element, NavigableString) and not isinstance(element, Comment):
                text = str(element).strip()
                
                # Skip empty text nodes
                if not text or len(text) < 1:
                    continue
                
                parent = element.parent
                if parent and hasattr(parent, 'name'):
                    # Skip navigation and structural elements
                    if parent.name in ['nav', 'script', 'style', 'head']:
                        continue
                    
                    # CRITICAL: Get text from ALL content-bearing elements
                    # This includes inline elements (for definition terms) and block elements
                    if parent.name in [
                        # Paragraph and text elements
                        'p', 'span', 'div', 'td', 'th', 'li', 
                        # Formatting elements
                        'b', 'i', 'strong', 'em', 'a', 'u', 'sub', 'sup',
                        # Heading elements (for nested content)
                        'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                        # CRITICAL: Custom elements used in legislation HTML
                        'inline',  # For definition terms, labels, etc.
                        'block',   # For subclauses, clauses, etc.
                        # Quotes and citations
                        'blockquote', 'q', 'cite'
                    ]:
                        # Clean up the text
                        text = self._clean_text_node(text)
                        if text:
                            text_parts.append(text)
                            logger.debug(f"Extracted from <{parent.name}>: {text[:60]}...")
        
        # Join with newlines to preserve structure
        result = '\n'.join(text_parts)
        
        # Clean up excessive whitespace
        result = re.sub(r'\n{3,}', '\n\n', result)
        result = re.sub(r' {2,}', ' ', result)
        
        # Log extraction stats
        if result:
            logger.debug(f"Extracted {len(text_parts)} text segments, {len(result)} total chars")
        
        return result.strip()
    
    def _clean_text_node(self, text: str) -> str:
        """
        Clean a text node for inclusion in output.
        
        Args:
            text: Raw text from HTML
            
        Returns:
            Cleaned text, or empty string if should be excluded
        """
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        # Skip very short fragments (likely formatting artifacts)
        if len(text) < 2:
            return ''
        
        # Skip common artifacts
        artifacts = ['...', '›', '»', '«', '‹']
        if text in artifacts:
            return ''
        
        return text
    
    def _is_inside_element(self, element, container_element) -> bool:
        """
        Check if element is a child/descendant of container_element.
        This is used to skip text inside the H1 heading itself.
        """
        if element == container_element:
            return True
        
        # Check if any parent is the container_element
        if hasattr(element, 'parents'):
            for parent in element.parents:
                if parent == container_element:
                    return True
        
        return False
    
    def _should_skip_element(self, element) -> bool:
        """Check if element should be skipped during text extraction."""
        # Skip if in navigation
        if hasattr(element, 'name'):
            if element.name in ['nav', 'script', 'style', 'meta', 'link', 'head']:
                return True
        
        # Skip if parent is navigation
        if hasattr(element, 'parents'):
            for parent in element.parents:
                if hasattr(parent, 'name') and parent.name == 'nav':
                    return True
                if hasattr(parent, 'get') and parent.get('id') == 'navigator':
                    return True
        
        return False
    
    def _get_heading_text(self, heading) -> str:
        """Extract clean heading text."""
        if not heading:
            return ""
        
        # Get text content
        text = heading.get_text(strip=True)
        
        # Clean up artifacts
        text = re.sub(r'^\s*§\s*\d+\s*', '', text)
        text = re.sub(r'^[-–—―]\s*', '', text)
        
        return text.strip()
    
    def _extract_entire_document(self, main_content) -> List[Dict]:
        """Extract entire document as single section."""
        content = self._extract_text_between_elements(main_content, None, None)
        
        if not content.strip():
            content = "No content found in document."
        
        logger.info(f"Created single section with {len(content)} characters")
        
        return [{
            'section_id': 1,
            'content': content,
            'heading': None
        }]
    
    def format_section_content(self, section: Dict[str, any]) -> str:
        """Format section content for saving."""
        return section['content']
    
    def get_section_summary(self, section: Dict[str, any]) -> str:
        """Get section summary for logging."""
        section_id = section['section_id']
        heading = section.get('heading')
        content_length = len(section['content'])
        
        if heading:
            return f"Section {section_id}: '{heading[:50]}...' ({content_length} chars)"
        else:
            return f"Section {section_id}: Pre-heading content ({content_length} chars)"