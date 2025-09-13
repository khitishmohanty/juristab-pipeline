from bs4 import BeautifulSoup, NavigableString, Tag
import re

class HtmlParser:
    """
    Parses HTML content to extract text and structural information.
    """

    def extract_text(self, html_content: str) -> str:
        """
        Extracts plain text from the given HTML content.
        Args:
            html_content (str): The HTML content to parse.
        Returns:
            str: The extracted text, with tags removed.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup.get_text(separator=' ', strip=True)

    def _get_heading_level(self, element: Tag) -> tuple[int, str | None]:
        """
        Determines the heading level of an element using a set of heuristics.
        This is the core logic that identifies structural elements in the source HTML.
        
        Args:
            element (Tag): A BeautifulSoup Tag object.

        Returns:
            A tuple: (level, heading_text) or (0, None) if not a heading.
        """
        # We check the direct text of the element first, but also consider text within a bold tag.
        bold_tag = element.find('b')
        heading_text = bold_tag.get_text(strip=True) if bold_tag else element.get_text(strip=True)
        
        if not heading_text:
            return 0, None

        tag_name = element.name
        style = element.get('style', '')

        # Rule 1: Explicit h1-h6 tags
        if re.match(r'h[1-6]', tag_name):
            return int(tag_name[1]), heading_text
            
        # **NEW RULE**: Specific styles from the source HTML (e.g., `font-size:147%`)
        if tag_name == 'p' and isinstance(style, str):
            if 'font-size:180%' in style:
                return 1, heading_text # Main Title
            if 'font-size:147%' in style and 'PART' in heading_text:
                return 1, heading_text # Part headings
            if 'font-size:127%' in style and re.match(r'Division \d', heading_text):
                return 2, heading_text # Division headings
            if 'font-size:107%' in style and re.match(r'^\d+(\.\d+)*[A-Z]?\s', heading_text):
                level = heading_text.count('.') + 1
                return min(level, 5), heading_text

        # Rule 2: Generic high font size (often a title)
        if isinstance(style, str):
            size_match = re.search(r'font-size\s*:\s*(\d+)%', style)
            if size_match and int(size_match.group(1)) >= 150:
                return 1, heading_text

        # Rule 3: Bolded text that indicates a heading
        if bold_tag:
            if re.match(r'^(PART\s+\d+[A-Z]?|SCHEDULE\s+\d+|ENDNOTES?)\b', heading_text, re.I):
                return 1, heading_text
            # e.g., "1 Name of instrument", "3.2 Allowances"
            if re.match(r'^\d+[A-Z]?(\.\d+[A-Z]?)*\s+[A-Z]', heading_text, re.I):
                level = heading_text.count('.') + 2
                return min(level, 5), heading_text

        return 0, None

    def _build_hierarchy_recursive(self, elements: list) -> list:
        """
        Recursively processes a list of sibling elements to build a hierarchical structure.
        This corrected version properly partitions content between parent and child nodes.
        """
        hierarchy = []
        i = 0
        while i < len(elements):
            element = elements[i]
            level, heading_text = self._get_heading_level(element)

            if level > 0:  # It's a heading.
                current_node = {
                    'level': level,
                    'heading_text': heading_text,
                    'content_tags': [],
                    'children': []
                }

                # Look ahead to find all elements belonging to this heading section.
                # The section ends when we find the next heading of the same or a higher level.
                j = i + 1
                while j < len(elements):
                    next_level, _ = self._get_heading_level(elements[j])
                    if next_level > 0 and next_level <= level:
                        break  # Found a sibling or parent-level heading, so stop.
                    j += 1
                
                children_elements = elements[i + 1:j]

                # Find the index of the first sub-heading within this section.
                first_subheading_index = -1
                for idx, child_el in enumerate(children_elements):
                    child_level, _ = self._get_heading_level(child_el)
                    if child_level > level:
                        first_subheading_index = idx
                        break
                
                if first_subheading_index != -1:
                    # Any content BEFORE the first sub-heading belongs directly to the current node.
                    current_node['content_tags'] = children_elements[:first_subheading_index]
                    # The sub-headings and all their content are passed for recursive processing.
                    sub_elements_for_recursion = children_elements[first_subheading_index:]
                    current_node['children'] = self._build_hierarchy_recursive(sub_elements_for_recursion)
                else:
                    # No sub-headings were found, so all of these elements are direct content.
                    current_node['content_tags'] = children_elements
                    current_node['children'] = []

                hierarchy.append(current_node)
                i = j  # Move the main index to the start of the next sibling section.
            else:
                # This is a non-heading element, so we skip it. It will be picked up
                # as part of the 'children_elements' of a preceding heading.
                i += 1
        
        return hierarchy

    def _build_hierarchy(self, html_content: str) -> tuple[list, str]:
        """Builds a nested list representing the document's structure from the HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the best starting point for content
        content_root = soup.find('doc', class_=re.compile(r'legislative|akbn-root')) or \
                       soup.find('div', class_='article-text') or \
                       soup.body

        if not content_root:
            return [], "Untitled Document"

        title_tag = soup.find('title') or soup.find('shorttitle')
        title = title_tag.get_text(strip=True) if title_tag else "Document"

        # **MODIFIED LOGIC**: Instead of direct children, find all relevant tags (p, blockquote)
        # This is more robust for documents with deeply nested content.
        all_elements = content_root.find_all(['p', 'blockquote'])
        
        hierarchy = self._build_hierarchy_recursive(all_elements)

        # Fallback if the recursive build fails to find any structured headings.
        if not hierarchy and all_elements:
             hierarchy.append({
                'level': 1,
                'heading_text': title,
                'content_tags': all_elements,
                'children': []
            })

        return hierarchy, title
