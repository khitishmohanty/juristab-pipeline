from bs4 import BeautifulSoup, NavigableString
from typing import List, Dict
import re

class SectionExtractor:
    """
    Extracts sections from juriscontent.html based on collapsible sections.
    """
    
    def __init__(self):
        pass
    
    def extract_sections(self, html_content: str) -> List[Dict[str, any]]:
        """
        Extracts sections from HTML content.
        
        Logic:
        1. Extract content before first H1 (if exists) → Section 1
        2. For each H1: Extract H1 heading + all its content → Separate sections
        
        Args:
            html_content (str): The juriscontent.html content
            
        Returns:
            List[Dict]: List of sections with their content and metadata
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the main content area
        main_content = soup.find('main', id='content')
        if not main_content:
            body = soup.find('body')
            if body:
                main_content = body
            else:
                main_content = soup
        
        print(f"DEBUG: Main content tag: {main_content.name if hasattr(main_content, 'name') else 'root'}")
        
        # Find all H1 tags in the entire document
        all_h1s = main_content.find_all('h1')
        print(f"DEBUG: Found {len(all_h1s)} H1 tags in main content")
        
        # Print first few H1 texts for debugging
        for idx, h1 in enumerate(all_h1s[:3]):
            h1_text = h1.get_text(strip=True)[:50]
            print(f"DEBUG: H1 #{idx+1}: {h1_text}")
            # Check parent structure
            parent = h1.parent
            print(f"DEBUG: H1 #{idx+1} parent: {parent.name if hasattr(parent, 'name') else 'unknown'}")
            if hasattr(parent, 'get'):
                print(f"DEBUG: H1 #{idx+1} parent id: {parent.get('id')}")
                print(f"DEBUG: H1 #{idx+1} parent class: {parent.get('class')}")
        
        sections = []
        section_id = 1
        
        # Filter out H1s that are in the navigator
        h1_headings = []
        for h1 in all_h1s:
            # Check if this H1 is inside a nav element or has navigator as ancestor
            is_in_nav = False
            for parent in h1.parents:
                if hasattr(parent, 'name'):
                    if parent.name == 'nav':
                        is_in_nav = True
                        break
                    if hasattr(parent, 'get') and parent.get('id') == 'navigator':
                        is_in_nav = True
                        break
            
            if not is_in_nav:
                h1_headings.append(h1)
        
        print(f"DEBUG: After filtering navigator, {len(h1_headings)} H1 tags remain")
        
        if not h1_headings:
            # No H1 found, try H2 as fallback
            print("INFO: No H1 headings found. Falling back to H2 headings.")
            h1_headings = main_content.find_all('h2')
        
        if not h1_headings:
            # No headings found at all, extract entire document as single section
            print("WARNING: No headings found. Extracting entire document as single section.")
            content_text = self._extract_text_content(main_content)
            if content_text.strip():
                sections.append({
                    'section_id': section_id,
                    'content': content_text,
                    'heading': None
                })
            return sections
        
        print(f"INFO: Found {len(h1_headings)} H1 headings for extraction.")
        
        # STEP 1: Extract content BEFORE first H1 heading (if any)
        first_h1 = h1_headings[0]
        first_h1_text = first_h1.get_text(strip=True)
        print(f"DEBUG: First H1 text: {first_h1_text[:100]}")
        
        # Get position of first H1 in the document
        # We need to extract everything that comes before it in document order
        content_before_first_h1 = self._extract_content_before_element(main_content, first_h1)
        
        print(f"DEBUG: Content before first H1 length: {len(content_before_first_h1)}")
        if content_before_first_h1.strip():
            print(f"INFO: Found content before first H1, saving as section {section_id}")
            print(f"DEBUG: Content preview: {content_before_first_h1[:300]}")
            sections.append({
                'section_id': section_id,
                'content': content_before_first_h1,
                'heading': None
            })
            section_id += 1
        else:
            print("INFO: No content found before first H1 heading")
        
        # STEP 2: Extract each H1 heading + its content
        for i, h1_heading in enumerate(h1_headings):
            # Get the heading text
            heading_text = self._extract_heading_text(h1_heading)
            
            # Find next H1 (or None if this is the last one)
            next_h1 = h1_headings[i + 1] if i + 1 < len(h1_headings) else None
            
            # Extract this H1's content (including the heading itself)
            section_content = self._extract_h1_section_content(h1_heading, next_h1)
            
            if section_content.strip():
                print(f"INFO: Extracted section {section_id}: {heading_text[:50]}...")
                sections.append({
                    'section_id': section_id,
                    'content': section_content,
                    'heading': heading_text
                })
                section_id += 1
        
        return sections
    
    def _extract_content_before_element(self, container, target_element) -> str:
        """
        Extract all text content that appears before target_element in document order.
        This uses a simple approach: get all text, then stop when we hit the target.
        """
        content_parts = []
        
        # Walk through all descendants in order
        for element in container.descendants:
            # Stop when we hit the target element
            if element == target_element:
                break
            
            # Skip if we're inside a nav element
            if hasattr(element, 'name') and element.name == 'nav':
                # Skip this entire subtree
                continue
            
            # Check if this element is inside nav
            is_in_nav = False
            if hasattr(element, 'parents'):
                for parent in element.parents:
                    if hasattr(parent, 'name'):
                        if parent.name == 'nav':
                            is_in_nav = True
                            break
                        if hasattr(parent, 'get') and parent.get('id') == 'navigator':
                            is_in_nav = True
                            break
            
            if is_in_nav:
                continue
            
            # Only collect direct text nodes, not from nested elements
            if isinstance(element, NavigableString):
                text = str(element).strip()
                if text and len(text) > 1:  # Ignore single characters
                    # Make sure this text is not inside the nav
                    parent = element.parent
                    if parent and hasattr(parent, 'name'):
                        # Skip if parent is nav or inside nav
                        skip = False
                        if parent.name == 'nav':
                            skip = True
                        if hasattr(parent, 'get') and parent.get('id') == 'navigator':
                            skip = True
                        
                        if not skip:
                            content_parts.append(text)
        
        return '\n'.join(content_parts)
    
    def _extract_h1_section_content(self, h1_heading, next_h1) -> str:
        """
        Extracts content for one H1 section (heading + all content until next H1).
        
        Args:
            h1_heading: The current H1 heading element
            next_h1: The next H1 heading element (or None if last section)
            
        Returns:
            str: The heading text + all section content
        """
        content_parts = []
        
        # 1. Add the H1 heading text itself
        heading_text = self._extract_heading_text(h1_heading)
        if heading_text:
            content_parts.append(heading_text)
        
        # 2. Check if this H1 has a collapsible-content sibling
        collapsible_content = h1_heading.find_next_sibling('div', class_='collapsible-content')
        
        if collapsible_content:
            # Extract from collapsible div
            content = self._extract_text_content(collapsible_content)
            if content:
                content_parts.append(content)
        else:
            # No collapsible div, extract all siblings until next H1
            current = h1_heading.next_sibling
            
            while current:
                # Stop if we reach the next H1
                if current == next_h1:
                    break
                
                # Skip NavigableString whitespace
                if isinstance(current, NavigableString):
                    text = str(current).strip()
                    if text:
                        content_parts.append(text)
                    current = current.next_sibling
                    continue
                
                # Skip if not a tag
                if not hasattr(current, 'name'):
                    current = current.next_sibling
                    continue
                
                # Stop if we hit another H1
                if current.name == 'h1':
                    break
                
                # Extract text from this sibling
                text = self._extract_text_content(current)
                if text:
                    content_parts.append(text)
                
                current = current.next_sibling
        
        return '\n\n'.join(content_parts)
    
    def _extract_heading_text(self, heading_element) -> str:
        """
        Extracts clean heading text, removing section numbers and markup.
        """
        if not heading_element:
            return ""
        
        # Clone to avoid modifying original
        heading_copy = BeautifulSoup(str(heading_element), 'html.parser')
        
        # Remove section number spans
        for span in heading_copy.find_all('span', class_='section-number'):
            span.decompose()
        
        # Remove section separator inlines
        for inline in heading_copy.find_all('inline', class_='section-separator'):
            inline.decompose()
        
        # Get text
        text = heading_copy.get_text(strip=True)
        
        # Clean up common artifacts
        text = re.sub(r'^\s*§\s*\d+\s*', '', text)
        text = re.sub(r'^\s*\d+\s*[-—]\s*', '', text)
        
        return text.strip()
    
    def _extract_text_content(self, element) -> str:
        """
        Extracts clean text from an element, preserving paragraph structure.
        """
        if not element:
            return ""
        
        # Get text with line breaks between elements
        text = element.get_text(separator='\n', strip=True)
        
        # Clean up excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        
        return text.strip()
    
    def format_section_content(self, section: Dict[str, any]) -> str:
        """
        Formats section content for saving.
        """
        return section['content']