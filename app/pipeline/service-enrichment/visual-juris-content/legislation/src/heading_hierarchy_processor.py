import re
import yaml
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup, Tag

class HeadingHierarchyProcessor:
    """
    Processes HTML content to apply intelligent heading hierarchy rules
    based on configurable text patterns.
    """
    
    def __init__(self, rules_config_path: str):
        """
        Initialize with heading hierarchy rules from config file.
        
        Args:
            rules_config_path (str): Path to the heading hierarchy rules YAML file
        """
        self.rules_config = self._load_rules_config(rules_config_path)
        self.hierarchy_rules = self.rules_config.get('heading_hierarchy_rules', {})
        self.enumeration_exclusions = self.rules_config.get('enumeration_exclusions', {})
        self.settings = self.rules_config.get('settings', {})
        
    def _load_rules_config(self, config_path: str) -> dict:
        """Load the heading hierarchy rules from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load heading hierarchy rules from {config_path}: {e}")
            return {}
    
    def _get_text_content(self, element: Tag) -> str:
        """Extract clean text content from an element."""
        return element.get_text(strip=True)
    
    def _matches_pattern(self, text: str, pattern_config: dict) -> bool:
        """Check if text matches a regex pattern."""
        regex = pattern_config.get('regex', '')
        case_insensitive = pattern_config.get('case_insensitive', False)
        
        if not regex:
            return False
            
        flags = re.IGNORECASE if case_insensitive else 0
        try:
            return bool(re.match(regex, text, flags))
        except re.error:
            print(f"Warning: Invalid regex pattern: {regex}")
            return False
    
    def _is_enumeration_pattern(self, text: str) -> bool:
        """Check if text matches enumeration patterns that should be excluded."""
        exclusion_patterns = self.enumeration_exclusions.get('patterns', [])
        
        for pattern_config in exclusion_patterns:
            if self._matches_pattern(text, pattern_config):
                return True
        return False
    
    def _determine_heading_level(self, text: str) -> Optional[str]:
        """
        Determine the appropriate heading level based on text content.
        
        Returns:
            str: Heading level ('h1', 'h2', etc.) or None if not a structural heading
        """
        # Check text length constraints
        min_length = self.settings.get('min_heading_text_length', 2)
        max_length = self.settings.get('max_heading_text_length', 250)
        
        if len(text) < min_length or len(text) > max_length:
            return None
        
        # Check if it's an enumeration pattern first
        if self._is_enumeration_pattern(text):
            return None
        
        # Sort rule categories by priority (highest first)
        rule_categories = []
        for category_name, category_config in self.hierarchy_rules.items():
            priority = category_config.get('priority', 0)
            rule_categories.append((priority, category_name, category_config))
        
        rule_categories.sort(key=lambda x: x[0], reverse=True)
        
        # Check each rule category in priority order
        for priority, category_name, category_config in rule_categories:
            patterns = category_config.get('patterns', [])
            level = category_config.get('level', 'h2')
            
            for pattern_config in patterns:
                if self._matches_pattern(text, pattern_config):
                    return level
        
        return None
    
    def _has_structural_headings(self, soup: BeautifulSoup) -> bool:
        """Check if the document contains any structural headings based on our rules."""
        potential_headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div', 'span'])
        
        for element in potential_headings:
            text = self._get_text_content(element)
            if self._determine_heading_level(text):
                return True
        return False
    
    def _is_likely_heading_element(self, element: Tag) -> bool:
        """
        Determine if an element is likely to be a heading based on its structure.
        This helps identify standalone heading-like elements.
        """
        text = self._get_text_content(element)
        
        # Too short or too long
        if len(text) < 2 or len(text) > 250:
            return False
        
        # Check if element has many child paragraphs (likely a container, not a heading)
        child_paragraphs = element.find_all(['p', 'div'], recursive=False)
        if len(child_paragraphs) > 2:
            return False
        
        # Check if element is inside a paragraph with lots of content
        parent = element.parent
        if parent and parent.name in ['p', 'li', 'td']:
            parent_text = parent.get_text(strip=True)
            # If the element text is much shorter than parent, it might be inline
            if len(parent_text) > len(text) * 3:
                return False
        
        # Check for bold or strong styling
        if element.name in ['strong', 'b']:
            return True
        
        # Check if all text is within a single bold/strong child
        bold_children = element.find_all(['strong', 'b'], recursive=False)
        if len(bold_children) == 1 and len(self._get_text_content(bold_children[0])) > len(text) * 0.8:
            return True
        
        return True
    
    def process_document(self, html_content: str) -> str:
        """
        Process HTML content and apply heading hierarchy rules.
        
        Args:
            html_content (str): Input HTML content
            
        Returns:
            str: Processed HTML with proper heading hierarchy
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all potential heading elements
        potential_headings = []
        
        # Get all existing headings
        existing_headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        potential_headings.extend(existing_headings)
        
        # IMPORTANT: Look for Australian legislation structure
        # These are <block class="section-header"> elements
        section_headers = soup.find_all('block', class_='section-header')
        for block in section_headers:
            potential_headings.append(block)
        
        # Get paragraph-like elements that might be headings
        for element in soup.find_all(['p', 'div', 'span']):
            text = self._get_text_content(element)
            if not text:
                continue
            
            # Skip if already in section-header
            if element.find_parent('block', class_='section-header'):
                continue
            
            # Quick check if text matches any heading pattern
            if self._determine_heading_level(text):
                if self._is_likely_heading_element(element):
                    potential_headings.append(element)
        
        # Process each potential heading
        processed_elements = set()
        
        for element in potential_headings:
            if element in processed_elements:
                continue
            
            # Skip if element was removed from the tree
            if element.parent is None:
                continue
                
            text = self._get_text_content(element)
            
            if not text:
                continue
            
            # Determine if this should be a structural heading
            structural_level = self._determine_heading_level(text)
            
            if structural_level:
                # This matches a structural heading pattern
                self._convert_to_heading(soup, element, structural_level, text)
                processed_elements.add(element)
        
        return str(soup)
    
    def _convert_to_heading(self, soup: BeautifulSoup, element: Tag, level: str, text: str):
        """Convert an element to a proper heading tag."""
        # If it's already a heading tag, just update the level if needed
        if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            if element.name != level:
                element.name = level
            return
        
        # Create new heading element
        new_heading = soup.new_tag(level)
        
        # Preserve any links or formatting within the heading
        if len(list(element.children)) == 1 and isinstance(list(element.children)[0], str):
            # Simple text content
            new_heading.string = text
        else:
            # Complex content - preserve inner HTML
            for child in list(element.children):
                if isinstance(child, str):
                    new_heading.append(child)
                else:
                    new_heading.append(child.extract())
        
        # Replace the original element
        element.replace_with(new_heading)
    
    def get_rules_summary(self) -> str:
        """Return a summary of loaded rules for debugging."""
        summary = ["Heading Hierarchy Rules Summary:"]
        
        for category_name, category_config in self.hierarchy_rules.items():
            level = category_config.get('level', 'unknown')
            priority = category_config.get('priority', 0)
            patterns = category_config.get('patterns', [])
            
            summary.append(f"  {category_name} -> {level} (priority: {priority})")
            for pattern in patterns:
                examples = pattern.get('examples', [])
                summary.append(f"    Pattern: {pattern.get('regex', 'N/A')}")
                if examples:
                    summary.append(f"    Examples: {', '.join(examples[:3])}")
        
        return "\n".join(summary)