# src/headless_html_processor.py

import re
from bs4 import BeautifulSoup, Tag

class HeadlessHtmlProcessor:
    """
    Injects semantic heading tags (h1, h2, etc.) into HTML documents
    that lack them, based on a set of style-based rules.
    """
    def __init__(self, rules_config: dict):
        self.rules = rules_config.get('heading_rules', [])
        self.base_threshold = rules_config.get('base_styles', {}).get('body_font_size_threshold_px', 15)
        self.title_found = False

    def _parse_styles(self, soup: BeautifulSoup) -> dict:
        """
        Parses the <style> tag in the HTML to create a map of
        class names to their CSS properties (font-size, font-weight).
        """
        style_map = {}
        style_tag = soup.find('style')
        if not style_tag:
            return {}

        # Get the style content, handling None case
        style_content = style_tag.string
        if not style_content:
            # Try getting text content if .string is None
            style_content = style_tag.get_text()
        
        # If still no content, return empty dict
        if not style_content:
            return {}

        # Use regex to find class definitions and their properties
        class_definitions = re.findall(r'\.([a-zA-Z0-9_-]+)\s*\{([^}]+)\}', style_content)
        
        for class_name, styles in class_definitions:
            props = {}
            font_size_match = re.search(r'font-size\s*:\s*(\d+)px', styles)
            if font_size_match:
                props['font_size_px'] = int(font_size_match.group(1))

            if 'font-weight:bold' in styles.replace(" ", ""):
                props['font_weight'] = 'bold'
            
            if props:
                style_map[class_name] = props
        
        return style_map

    def _get_element_style(self, element: Tag, style_map: dict) -> dict:
        """Gets the computed style for an element from the style map."""
        element_classes = element.get('class', [])
        for cls in element_classes:
            if cls in style_map:
                return style_map[cls]
        return {}

    def process(self, html_content: str) -> str:
        """
        Applies the heading inference rules to the HTML content.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        style_map = self._parse_styles(soup)
        
        if not style_map:
            return html_content # Cannot process without styles

        potential_headings = soup.find_all(['span', 'div', 'p'])
        
        for element in potential_headings:
            style = self._get_element_style(element, style_map)
            
            if not style or style.get('font_size_px', 0) < self.base_threshold:
                continue

            for rule in self.rules:
                criteria = rule['criteria']
                
                # Check font-weight
                if criteria.get('font_weight') == 'bold' and style.get('font_weight') != 'bold':
                    continue
                
                # Check font-size
                if style.get('font_size_px', 0) < criteria.get('min_font_size_px', self.base_threshold):
                    continue

                # If all criteria match, this is a heading
                text_content = element.get_text(strip=True)
                if not text_content:
                    continue
                
                # Special handling for the title
                if rule.get('is_title', False):
                    if self.title_found:
                        continue # Only one title allowed
                    self.title_found = True
                
                new_heading = soup.new_tag(rule['level'])
                new_heading.string = text_content
                element.replace_with(new_heading)
                
                # Stop checking rules for this element
                break

        self.title_found = False # Reset for next run
        return str(soup)