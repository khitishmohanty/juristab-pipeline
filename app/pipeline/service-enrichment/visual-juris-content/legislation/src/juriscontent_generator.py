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
            
            # Preserve existing id attribute if present
            if heading.get('id'):
                new_heading['id'] = heading['id']
            
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

        # --- CHECK IF HEADINGS EXIST ---
        h1_headings = main_content.find_all('h1')
        has_headings = len(h1_headings) > 0
        
        # If no headings, hide navigator and adjust layout
        if not has_headings:
            navigator['style'] = 'display: none;'
            main_content['style'] = 'border-left: none;'

        # --- BUILD NAVIGATOR & ADD IDs TO HEADINGS (only if headings exist) ---
        if has_headings:
            nav_ul = soup.new_tag('ul')
            
            for i, h1 in enumerate(h1_headings):
                # Generate section_id for H1 if not already present
                if not h1.get('id'):
                    h1['id'] = f"section_{i + 1}"
                
                heading_id = h1['id']
                
                # Create H1 navigation item
                li = soup.new_tag('li', **{'class': 'nav-title'})
                a = soup.new_tag('a', href=f"#{heading_id}")
                a_strong = soup.new_tag('strong')
                
                nav_text = self._get_clean_nav_text(h1)
                a_strong.string = nav_text
                
                a.append(a_strong)
                li.append(a)
                
                # Find all H2 headings that are children of this H1
                h2_ul = soup.new_tag('ul')
                has_h2_children = False
                
                # Get all H2s between this H1 and the next H1 (or end of document)
                next_h1 = h1_headings[i + 1] if i + 1 < len(h1_headings) else None
                current_element = h1.find_next_sibling()
                
                while current_element:
                    if current_element == next_h1:
                        break
                    
                    # Check if this is an H2 or contains an H2
                    h2_list = []
                    if current_element.name == 'h2':
                        h2_list.append(current_element)
                    else:
                        h2_list = current_element.find_all('h2', recursive=True)
                    
                    for h2 in h2_list:
                        if not h2.get('id'):
                            h2['id'] = f"section-h2-{i}-{len(h2_ul.find_all('li'))}"
                        
                        h2_li = soup.new_tag('li')
                        h2_a = soup.new_tag('a', href=f"#{h2['id']}")
                        h2_a.string = self._get_clean_nav_text(h2)
                        h2_li.append(h2_a)
                        h2_ul.append(h2_li)
                        has_h2_children = True
                    
                    current_element = current_element.find_next_sibling()
                
                # Check if H1 has any actual content (text or child headings)
                h1_content_div = h1.find_next_sibling('div', class_='collapsible-content')
                h1_has_content = False
                
                if h1_content_div:
                    content_text = h1_content_div.get_text(strip=True)
                    if content_text or h1_content_div.find_all():
                        h1_has_content = True
                
                # Add H2 submenu if there are H2 children and mark as collapsible
                if has_h2_children:
                    li['class'] = li.get('class', []) + ['nav-collapsible']
                    li.append(h2_ul)
                
                nav_ul.append(li)
            
            navigator.append(nav_ul)
        else:
            # No headings - add placeholder message
            nav_ul = soup.new_tag('ul')
            li = soup.new_tag('li', **{'class': 'nav-placeholder'})
            li.string = "No sections available"
            nav_ul.append(li)
            navigator.append(nav_ul)
        
        # Add IDs to remaining headings for internal navigation (only if headings exist)
        if has_headings:
            all_headings = main_content.find_all(re.compile(r'^h[1-5]$'))
            for idx, heading in enumerate(all_headings):
                if not heading.get('id'):
                    heading['id'] = f"section-{idx}"

        # --- HIERARCHY-BUILDING STEP (only if headings exist) ---
        if has_headings:
            headings = main_content.find_all(re.compile(r'^h[1-5]$'))

            # Handle intro content (between title and first collapsible heading)
            if headings:
                first_heading = headings[0]
                h1_tags = main_content.find_all('h1')
                
                for h1_tag in h1_tags:
                    next_heading = h1_tag.find_next_sibling(re.compile(r'^h[1-5]$'))
                    if next_heading and next_heading in headings:
                        intro_wrapper = soup.new_tag('div', **{'class': 'intro-content'})
                        for sibling in list(h1_tag.find_next_siblings()):
                            if sibling == next_heading:
                                break
                            intro_wrapper.append(sibling.extract())
                        if intro_wrapper.contents:
                            next_heading.insert_before(intro_wrapper)

            # Make H1-H5 collapsible (but only if they have content)
            for heading in reversed(headings):
                content_wrapper = soup.new_tag('div', **{'class': 'collapsible-content'})
                current_level = int(heading.name[1])
                
                for sibling in list(heading.find_next_siblings()):
                    if sibling.name and re.match(r'^h[1-5]$', sibling.name):
                        sibling_level = int(sibling.name[1])
                        if sibling_level <= current_level:
                            break
                    content_wrapper.append(sibling.extract())
                
                has_content = False
                if content_wrapper.contents:
                    content_text = content_wrapper.get_text(strip=True)
                    if content_text or content_wrapper.find_all():
                        has_content = True
                
                if has_content:
                    heading['class'] = heading.get('class', []) + ['collapsible-heading']
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
            #navigator ul ul { 
                padding-left: 1.5em;
                margin-top: 0.5em;
            }
            #navigator ul ul li {
                font-size: 0.9em;
                font-weight: 400;
                margin-bottom: 0.3em;
                border-bottom: none;
            }
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
                padding-left: 1.2em;
            }
            #navigator .nav-collapsible > a::before {
                content: '+';
                position: absolute;
                left: 0;
                top: 0.2em;
                font-weight: normal;
                font-size: 1.2em;
                color: #666;
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
            #content * {
                text-align: left !important;
                margin-left: 0 !important;
            }
            #content table {
                margin-left: 0 !important;
            }
            #content p {
                margin-left: 0 !important;
                text-indent: 0 !important;
            }
            #content ul {
                list-style-type: none;
                padding-left: 1.5em;
            }
            
            /* CRITICAL: Unified heading sizes - all 14px with weight-based hierarchy */
            #content h1 {
                font-size: 14px;
                font-weight: 600;
                margin-bottom: 0.8em;
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 0.4em;
            }
            #content h2 {
                font-size: 14px;
                font-weight: 500;
                margin-top: 1.2em;
                margin-bottom: 0.4em;
            }
            #content h3 {
                font-size: 14px;
                font-weight: 450;
                margin-top: 1.2em;
                margin-bottom: 0.4em;
            }
            #content h4 {
                font-size: 14px;
                font-weight: 400;
                margin-top: 1em;
                margin-bottom: 0.4em;
            }
            #content h5 {
                font-size: 14px;
                font-weight: 400;
                margin-top: 1em;
                margin-bottom: 0.4em;
            }
            #content h6 {
                font-size: 14px;
                font-weight: 400;
                margin-top: 1em;
                margin-bottom: 0.4em;
            }
            
            /* Collapsible heading styles - also 14px */
            h1.collapsible-heading { 
                font-size: 14px; 
                font-weight: 600; 
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 0.4em;
            }
            h2.collapsible-heading { 
                font-size: 14px; 
                font-weight: 500; 
            }
            h3.collapsible-heading { 
                font-size: 14px; 
                font-weight: 450; 
                margin-left: 1.5em; 
            }
            h4.collapsible-heading { 
                font-size: 14px; 
                font-weight: 400; 
                margin-left: 3em; 
            }
            h5.collapsible-heading { 
                font-size: 14px; 
                font-weight: 400; 
                margin-left: 4.5em; 
            }
            h6.collapsible-heading { 
                font-size: 14px; 
                font-weight: 350; 
                margin-left: 6em; 
            }
            
            hr {
                border: none;
                border-top: 0.5px solid #e0e0e0;
                margin: 2em 0;
                opacity: 0.5;
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
                border-left: 1px solid #d0d0d0;
            }
            .collapsible-content {
                overflow: hidden;
                transition: max-height 0.4s ease-out;
                max-height: 5000px;
                padding-left: 1.5em;
                margin-left: 0.4em;
                border-left: 1px solid #d0d0d0;
            }
            h1.collapsible-heading + .collapsible-content { 
                margin-left: 0.4em;
                border-left: 1px solid #d0d0d0;
            }
            h2.collapsible-heading + .collapsible-content { 
                margin-left: 0.4em;
                border-left: 1px solid #d0d0d0;
            }
            h3.collapsible-heading + .collapsible-content { 
                margin-left: 1.9em;
                border-left: 1px solid #d0d0d0;
            }
            h4.collapsible-heading + .collapsible-content { 
                margin-left: 3.4em;
                border-left: 1px solid #d0d0d0;
            }
            h5.collapsible-heading + .collapsible-content { 
                margin-left: 4.9em;
                border-left: 1px solid #d0d0d0;
            }
            h6.collapsible-heading + .collapsible-content { 
                margin-left: 6.4em;
                border-left: 1px solid #d0d0d0;
            }
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
                
                function getFullHeight(element) {
                    let height = element.scrollHeight;
                    const nestedExpanded = element.querySelectorAll('.collapsible-content.expanded');
                    nestedExpanded.forEach(nested => {
                        height += nested.scrollHeight;
                    });
                    return height;
                }
                
                function updateParentHeights(element) {
                    let parent = element.parentElement.closest('.collapsible-content');
                    while (parent) {
                        if (parent.classList.contains('expanded') || parent.style.maxHeight !== '0px') {
                            parent.style.maxHeight = getFullHeight(parent) + 'px';
                        }
                        parent = parent.parentElement.closest('.collapsible-content');
                    }
                }
                
                function toggleSection(heading) {
                    heading.classList.toggle('collapsed');
                    const content = heading.nextElementSibling;
                    if (content && content.classList.contains('collapsible-content')) {
                        if (content.style.maxHeight && content.style.maxHeight !== '0px') {
                            content.style.maxHeight = '0px';
                            content.classList.remove('expanded');
                        } else {
                            content.classList.add('expanded');
                            const fullHeight = getFullHeight(content);
                            content.style.maxHeight = fullHeight + 'px';
                        }
                        setTimeout(() => {
                            updateParentHeights(content);
                        }, 50);
                    }
                }
                
                // --- Navigator Collapse/Expand for H1 sections ---
                const navCollapsibles = document.querySelectorAll('#navigator .nav-collapsible');
                navCollapsibles.forEach(navItem => {
                    const subMenu = navItem.querySelector('ul');
                    if (subMenu) {
                        const navLink = navItem.querySelector('a');
                        navLink.addEventListener('click', function(e) {
                            if (e.target === navLink || e.target.parentElement === navLink) {
                                e.preventDefault();
                                e.stopPropagation();
                                navItem.classList.toggle('nav-expanded');
                                return false;
                            }
                        });
                    }
                });
                
                // --- Navigator Links Scrolling & Auto-Expand ---
                const navLinks = document.querySelectorAll('#navigator a');
                navLinks.forEach(link => {
                    link.addEventListener('click', function(e) {
                        const targetId = this.getAttribute('href');
                        const targetElement = document.querySelector(targetId);
                        
                        const parentLi = this.parentElement;
                        const hasChildren = parentLi.classList.contains('nav-collapsible');
                        
                        if (targetElement && (!hasChildren || !parentLi.classList.contains('nav-title'))) {
                            e.preventDefault();
                            
                            if (targetElement.classList.contains('collapsible-heading') && targetElement.classList.contains('collapsed')) {
                                toggleSection(targetElement);
                            }
                            
                            let parent = targetElement.closest('.collapsible-content');
                            while(parent) {
                                const parentHeading = parent.previousElementSibling;
                                if(parentHeading && parentHeading.classList.contains('collapsed')) {
                                    toggleSection(parentHeading);
                                }
                                parent = parent.parentElement.closest('.collapsible-content');
                            }
                            
                            setTimeout(() => {
                                targetElement.scrollIntoView({
                                    behavior: 'smooth',
                                    block: 'start'
                                });
                            }, 400);
                        }
                    });
                });
            });
        """
        body.append(script_tag)
        
        return str(soup)