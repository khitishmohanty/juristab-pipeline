import re
from bs4 import BeautifulSoup, NavigableString

class JuriscontentGenerator:
    """
    A utility class to parse and transform source HTML into a structured,
    collapsible juriscontent.html format. It now assumes that semantic
    heading tags are already present in the source HTML.
    """

    def _format_list_items(self, soup_body):
        """
        Merges list item labels (e.g., '(a)') into the main paragraph
        to ensure they appear on the same line and without extra bullets.
        """
        for li in soup_body.find_all('li'):
            label_tag = li.find('inline', class_='li-label')
            p_tag = li.find('p')

            if label_tag and p_tag:
                label_text = label_tag.get_text(strip=True)
                if label_text and p_tag.find_parent('li') == li:
                    p_tag.insert(0, f"{label_text} ")
                    label_tag.decompose()

    def _format_subclauses(self, soup_body):
        """
        Handles enumerations in 'subclause' blocks, merging the number
        into the following paragraph.
        """
        for block in soup_body.find_all('block', class_='subclause'):
            number_tag = block.find('inline', class_='number')
            p_tag = number_tag.find_next_sibling('p') if number_tag else None

            if number_tag and p_tag and p_tag.find_parent('block') == block:
                number_text = number_tag.get_text(strip=True)
                if number_text:
                    p_tag.insert(0, f"{number_text} ")
                    number_tag.decompose()

    def _standardize_headings(self, soup, soup_body):
        """
        Converts all existing heading tags (h1-h6) into a consistent 
        hierarchy, dynamically promoting headings if higher levels are missing.
        Caps at h5 instead of h3 for more granular structure.
        """
        # Find all heading tags and also block elements that function as headers
        potential_headings = soup_body.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'block'])
        
        # Filter out non-header blocks and empty headings
        valid_headings = []
        for heading in potential_headings:
            # Skip blocks that are not headers
            if heading.name == 'block' and 'section-header' not in heading.get('class', []):
                continue
            
            text_content = heading.get_text(separator=' ', strip=True)
            if not text_content:
                heading.decompose()
                continue
            
            valid_headings.append(heading)
        
        if not valid_headings:
            return
        
        # Determine the minimum heading level present (e.g., if no h1 but h2 exists, min_level = 2)
        heading_levels = []
        for heading in valid_headings:
            if heading.name.startswith('h') and len(heading.name) == 2:
                try:
                    level = int(heading.name[1])
                    heading_levels.append(level)
                except ValueError:
                    pass
            elif heading.name == 'block':
                # Treat header blocks as h2 by default
                heading_levels.append(2)
        
        if not heading_levels:
            return
            
        min_level = min(heading_levels)
        
        # Now convert headings with promotion logic
        for heading in valid_headings:
            text_content = heading.get_text(separator=' ', strip=True)
            
            # Determine original level
            if heading.name.startswith('h') and len(heading.name) == 2:
                try:
                    original_level = int(heading.name[1])
                except ValueError:
                    original_level = 3  # Default fallback
            elif heading.name == 'block':
                original_level = 2  # Treat header blocks as h2
            else:
                original_level = 3  # Default for unknown types
            
            # Calculate promoted level
            # If min_level is 2 (no h1), then h2->h1, h3->h2, h4->h3, etc.
            promoted_level = original_level - (min_level - 1)
            
            # Cap at h5 for lower levels (instead of h3)
            if promoted_level > 5:
                promoted_level = 5
            elif promoted_level < 1:
                promoted_level = 1
                
            new_tag_name = f'h{promoted_level}'
            new_heading = soup.new_tag(new_tag_name)
            
            # Check if heading has complex content (links, etc.) or just text
            if len(list(heading.children)) == 1 and isinstance(list(heading.children)[0], str):
                # Simple text content
                new_heading.string = text_content
            else:
                # Complex content - preserve inner HTML including links
                for child in list(heading.children):
                    if isinstance(child, str):
                        new_heading.append(child)
                    else:
                        new_heading.append(child.extract())
            
            heading.replace_with(new_heading)

    def _get_clean_nav_text(self, heading):
        """
        Extract navigation text from heading, preserving spaces between elements.
        This is specifically for the navigation panel.
        """
        # Use separator=' ' to preserve spaces between different elements
        text = heading.get_text(separator=' ', strip=False)
        # Clean up multiple spaces and strip outer whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def generate(self, html_content: str) -> str:
        """
        Transforms the raw HTML content into the desired juriscontent.html format.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        for tag in soup(["script", "style"]):
            tag.decompose()
        
        head = soup.find('head')
        if not head:
            head = soup.new_tag('head')
            if soup.html:
                soup.html.insert(0, head)
            else:
                soup.insert(0, head)

        body = soup.find('body')
        if not body:
            body_tag = soup.new_tag('body')
            for element in list(soup.children):
                if isinstance(element, NavigableString) and element.strip() == '':
                    continue
                if element.name == 'html':
                     for child in list(element.children):
                         if child.name != 'head':
                            body_tag.append(child.extract())
                elif element.name != 'head':
                    body_tag.append(element.extract())
            
            if soup.html:
                soup.html.append(body_tag)
            else:
                soup.append(body_tag)
            
            body = body_tag

        # --- PRE-PROCESSING STEPS ---
        self._format_list_items(body)
        self._format_subclauses(body)
        self._standardize_headings(soup, body)

        # --- CREATE TWO-PANEL LAYOUT ---
        container = soup.new_tag('div', **{'class': 'container'})
        navigator = soup.new_tag('nav', id='navigator')
        main_content = soup.new_tag('main', id='content')
        
        container.append(navigator)
        container.append(main_content)
        
        # Move all original body content into the <main> panel
        for element in list(body.children):
            main_content.append(element.extract())
        
        body.append(container)

        # --- BUILD NAVIGATOR & ADD IDs TO HEADINGS ---
        nav_ul = soup.new_tag('ul')
        headings_in_main = main_content.find_all(['h1', 'h2', 'h3', 'h4', 'h5'])
        
        # After standardization, H1s should exist (promoted from H2 if needed)
        # So we always show H1 headings in the navigator
        h1_headings = main_content.find_all('h1')
        
        for i, heading in enumerate(h1_headings):
            heading_id = f"section-nav-{i}"
            heading['id'] = heading_id
            
            li = soup.new_tag('li', **{'class': 'nav-title'})
            a = soup.new_tag('a', href=f"#{heading_id}")
            a_strong = soup.new_tag('strong')
            
            # Get the text content for navigation - USE NEW METHOD TO PRESERVE SPACES
            nav_text = self._get_clean_nav_text(heading)
            a_strong.string = nav_text
            
            a.append(a_strong)
            li.append(a)
            nav_ul.append(li)
        
        # If still no headings (rare case), add a placeholder
        if not h1_headings:
            li = soup.new_tag('li', **{'class': 'nav-placeholder'})
            li.string = "No sections available"
            nav_ul.append(li)
        
        # Add IDs to all headings for internal navigation
        for i, heading in enumerate(headings_in_main):
            if not heading.get('id'):  # Don't override ids already set
                heading['id'] = f"section-{i}"
        
        navigator.append(nav_ul)

        # --- HIERARCHY-BUILDING STEP ---
        # All headings H1-H5 are now collapsible
        headings = main_content.find_all(re.compile('^h[1-5]$'))

        # Handle intro content (between title and first collapsible heading)
        if headings:
            first_heading = headings[0]
            # Find all H1s to determine where intro content might be
            h1_tags = main_content.find_all('h1')
            
            # For each H1, check if there's content between it and the next heading
            for h1_tag in h1_tags:
                next_heading = h1_tag.find_next_sibling(re.compile('^h[1-5]$'))
                if next_heading and next_heading in headings:
                    # There's content between this H1 and a collapsible heading
                    intro_wrapper = soup.new_tag('div', **{'class': 'intro-content'})
                    for sibling in list(h1_tag.find_next_siblings()):
                        if sibling == next_heading:
                            break
                        intro_wrapper.append(sibling.extract())
                    if intro_wrapper.contents:
                        next_heading.insert_before(intro_wrapper)

        # Make H1-H5 collapsible
        for heading in reversed(headings):
            heading['class'] = heading.get('class', []) + ['collapsible-heading']
            content_wrapper = soup.new_tag('div', **{'class': 'collapsible-content'})
            current_level = int(heading.name[1])
            
            for sibling in list(heading.find_next_siblings()):
                # Stop at the next heading of same or higher level
                if sibling.name and re.match('^h[1-5]$', sibling.name):
                    sibling_level = int(sibling.name[1])
                    if sibling_level <= current_level:
                        break
                content_wrapper.append(sibling.extract())
            
            heading.insert_after(content_wrapper)
            
        # --- ADD HORIZONTAL LINES AFTER H2 SECTIONS ---
        main_headings = main_content.find_all('h2', recursive=False)
        for heading in main_headings:
            content_wrapper = heading.find_next_sibling('div', class_='collapsible-content')
            if content_wrapper:
                hr_tag = soup.new_tag('hr')
                content_wrapper.insert_after(hr_tag)

        style_tag = soup.new_tag('style')
        style_tag.string = """
            @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&display=swap');
            html { scroll-behavior: smooth; }
            body {
                font-family: 'Poppins', sans-serif;
                font-size: 14px;
                line-height: 1.6;
                margin: 0;
                padding: 1rem 2rem;
            }
            .container { display: flex; gap: 2rem; }
            #navigator {
                width: 250px;
                flex-shrink: 0;
                position: sticky;
                top: 1rem;
                align-self: flex-start;
                height: calc(100vh - 2rem);
                overflow-y: auto;
                padding-right: 1rem;
            }
            #navigator ul {
                list-style-type: none;
                padding: 0;
                margin: 0;
            }
            #navigator .nav-title {
                font-size: 0.95em;
                font-weight: 600;
                margin-bottom: 0.8em;
                padding-bottom: 0.4em;
                border-bottom: 1px solid #e0e0e0;
                word-wrap: break-word;
                line-height: 1.4;
            }
            #navigator .nav-title strong {
                display: block;
                white-space: normal;
            }
            #navigator .nav-placeholder {
                font-style: italic;
                color: #999;
                padding: 0.5em;
            }
            #navigator ul ul { padding-left: 1.5em; }
            #navigator li { margin-bottom: 0.2em; }
            #navigator .nav-collapsible {
                border-bottom: 1px solid #e0e0e0;
                margin-bottom: 0.5em;
                padding-bottom: 0.5em;
            }
            #navigator a {
                text-decoration: none;
                color: #333;
                display: block;
                padding: 0.2em 0.4em;
                border-radius: 4px;
                transition: background-color 0.2s;
                position: relative;
                word-wrap: break-word;
                white-space: normal;
            }
            #navigator a:hover {
                background-color: #f0f0f0;
                color: #000;
            }
            #navigator .nav-collapsible > a {
                font-weight: 500;
                cursor: pointer;
            }
            #navigator .nav-collapsible > a::before {
                content: '+';
                position: absolute;
                left: 0.2em;
                top: 50%;
                transform: translateY(-50%);
                font-weight: bold;
                color: #555;
                width: 1em;
                text-align: center;
            }
            #navigator .nav-collapsible.nav-expanded > a::before {
                content: '−';
            }
            #navigator .nav-collapsible > ul {
                max-height: 0;
                overflow: hidden;
                transition: max-height 0.3s ease-in-out;
            }
            #navigator .nav-collapsible.nav-expanded > ul {
                max-height: 1000px;
                margin-top: 0.25em;
            }
            #content {
                flex-grow: 1;
                min-width: 0;
                border-left: 1px solid #e0e0e0;
                padding-left: 2rem;
                font-size: 14px;
            }
            #content ul {
                list-style-type: none;
                padding-left: 1.5em;
            }
            #content h1 {
                font-size: 14px;
                font-weight: 600;
                margin-bottom: 0.8em;
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 0.4em;
            }
            h1.collapsible-heading { 
                font-size: 14px; 
                font-weight: 600; 
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 0.4em;
            }
            hr {
                border: none;
                border-top: 1px solid #e0e0e0;
                margin: 2em 0;
            }
            .collapsible-heading {
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 0.5em;
                user-select: none;
                margin-top: 1.2em;
                margin-bottom: 0.4em;
            }
            h2.collapsible-heading { font-size: 14px; font-weight: 500; }
            h3.collapsible-heading { font-size: 14px; font-weight: 450; margin-left: 1.5em; }
            h4.collapsible-heading { font-size: 14px; font-weight: 400; margin-left: 3em; }
            h5.collapsible-heading { font-size: 14px; font-weight: 400; margin-left: 4.5em; }
            .collapsible-heading::before {
                content: '−';
                font-weight: bold;
                color: #555;
                width: 1em;
                text-align: center;
                flex-shrink: 0;
            }
            .collapsible-heading.collapsed::before { content: '+'; }
            .intro-content {
                padding-left: 1.5em;
                margin-left: 0.4em;
            }
            .collapsible-content {
                overflow: hidden;
                transition: max-height 0.4s ease-out;
                max-height: 5000px;
                padding-left: 1.5em;
                margin-left: 0.4em;
            }
            h1.collapsible-heading + .collapsible-content { margin-left: 0.4em; }
            h2.collapsible-heading + .collapsible-content { margin-left: 0.4em; }
            h3.collapsible-heading + .collapsible-content { margin-left: 1.9em; }
            h4.collapsible-heading + .collapsible-content { margin-left: 3.4em; }
            h5.collapsible-heading + .collapsible-content { margin-left: 4.9em; }
        """
        head.append(style_tag)
        
        script_tag = soup.new_tag('script')
        script_tag.string = """
            document.addEventListener('DOMContentLoaded', function() {
                // --- Main Content Collapse/Expand Logic ---
                const headings = document.querySelectorAll('.collapsible-heading');
                headings.forEach(heading => {
                    const content = heading.nextElementSibling;
                    if (content && content.classList.contains('collapsible-content')) {
                        heading.classList.add('collapsed');
                        content.style.maxHeight = '0px';
                        content.classList.remove('expanded');
                    }
                });
                
                headings.forEach(heading => {
                    heading.addEventListener('click', (e) => {
                        e.stopPropagation();
                        toggleSection(heading);
                    });
                });
                
                function toggleSection(heading) {
                    heading.classList.toggle('collapsed');
                    const content = heading.nextElementSibling;
                    if (content && content.classList.contains('collapsible-content')) {
                        if (content.style.maxHeight && content.style.maxHeight !== '0px') {
                            content.style.maxHeight = '0px';
                            content.classList.remove('expanded');
                        } else {
                            content.style.maxHeight = content.scrollHeight + 'px';
                            content.classList.add('expanded');
                        }
                        const updateParentHeights = () => {
                            let parent = content.parentElement.closest('.collapsible-content');
                            while (parent) {
                                if (parent.style.maxHeight && parent.style.maxHeight !== '0px') {
                                    parent.style.maxHeight = parent.scrollHeight + 'px';
                                }
                                parent = parent.parentElement.closest('.collapsible-content');
                            }
                        };
                        content.addEventListener('transitionend', updateParentHeights, { once: true });
                    }
                }
                
                // --- Simplified Navigator for H1 only ---
                const navLinks = document.querySelectorAll('#navigator a');
                navLinks.forEach(link => {
                    link.addEventListener('click', function(e) {
                        e.preventDefault();
                        const targetId = this.getAttribute('href');
                        const targetElement = document.querySelector(targetId);
                        if (targetElement) {
                            // First, expand the target if it's collapsed
                            if (targetElement.classList.contains('collapsible-heading') && targetElement.classList.contains('collapsed')) {
                                toggleSection(targetElement);
                            }
                            
                            // Then expand any collapsed parent sections
                            let parent = targetElement.closest('.collapsible-content');
                            while(parent) {
                                const parentHeading = parent.previousElementSibling;
                                if(parentHeading && parentHeading.classList.contains('collapsed')) {
                                    toggleSection(parentHeading);
                                }
                                parent = parent.parentElement.closest('.collapsible-content');
                            }
                            
                            // Finally scroll to the element
                            setTimeout(() => {
                                targetElement.scrollIntoView({
                                    behavior: 'smooth',
                                    block: 'start'
                                });
                            }, 300);
                        }
                    });
                });
            });
        """
        body.append(script_tag)
        
        return str(soup)