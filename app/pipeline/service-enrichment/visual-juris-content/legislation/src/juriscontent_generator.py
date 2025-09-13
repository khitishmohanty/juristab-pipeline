import re
from bs4 import BeautifulSoup, NavigableString

class JuriscontentGenerator:
    """
    A utility class to parse and transform source HTML into a structured,
    collapsible juriscontent.html format. It handles multiple source HTML structures.
    """

    def _has_negative_indent(self, tag):
        """Checks if a tag has a style attribute with a negative text-indent."""
        if tag.name != 'p':
            return False
        style = tag.get('style', '')
        return bool(re.search(r'text-indent\s*:\s*-', style))

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
        Finds all potential heading elements based on various patterns and converts them
        into a consistent <h2> and <h3> structure.
        """
        def find_potential_headings():
            return soup_body.find_all(['h2', 'h3', 'h4', 'h5', 'h6', 'block'])

        for heading in find_potential_headings():
            if heading.name == 'block' and 'section-header' not in heading.get('class', []):
                continue
            
            anchor = heading.find('a', attrs={'id': True}) or heading.find('a', attrs={'name': True})
            if not anchor:
                continue
            
            text_content = heading.get_text(separator=' ', strip=True)
            
            if not text_content:
                heading.decompose()
                continue

            if heading.name in ['h2', 'h3'] or 'section-level-1' in heading.get('class', []):
                new_heading_tag = 'h2'
            else:
                new_heading_tag = 'h3'
                
            new_heading = soup.new_tag(new_heading_tag)
            new_heading.string = text_content
            
            heading.replace_with(new_heading)

        for p_tag in soup_body.find_all('p'):
            if self._has_negative_indent(p_tag):
                text_content = p_tag.get_text(separator=' ', strip=True)
                
                if len(text_content) > 250 or text_content.endswith('.'):
                    continue

                if p_tag.find('b'):
                    h_tag = soup.new_tag('h2')
                else:
                    if len(re.findall(r'\w', text_content)) < 3:
                        continue
                    h_tag = soup.new_tag('h3')
                
                h_tag.string = text_content
                p_tag.replace_with(h_tag)


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
        headings_in_main = main_content.find_all(['h2', 'h3'])
        current_h2_ul = None
        
        for i, heading in enumerate(headings_in_main):
            heading_id = f"section-{i}"
            heading['id'] = heading_id
            
            li = soup.new_tag('li')
            a = soup.new_tag('a', href=f"#{heading_id}")
            a.string = heading.get_text(strip=True)
            li.append(a)
            
            if heading.name == 'h2':
                li['class'] = 'nav-collapsible'
                nav_ul.append(li)
                current_h2_ul = soup.new_tag('ul')
                li.append(current_h2_ul)
            elif heading.name == 'h3':
                if current_h2_ul is not None:
                    current_h2_ul.append(li)
                else:
                    nav_ul.append(li)
        
        navigator.append(nav_ul)

        # --- HIERARCHY-BUILDING STEP ---
        headings = main_content.find_all(re.compile('^h[2-3]$'))

        if headings:
            first_heading = headings[0]
            intro_wrapper = soup.new_tag('div', **{'class': 'intro-content'})
            for sibling in list(first_heading.find_previous_siblings()):
                intro_wrapper.insert(0, sibling.extract())
            if intro_wrapper.contents:
                first_heading.insert_before(intro_wrapper)

        for heading in reversed(headings):
            heading['class'] = heading.get('class', []) + ['collapsible-heading']
            content_wrapper = soup.new_tag('div', **{'class': 'collapsible-content'})
            current_level = int(heading.name[1])
            for sibling in list(heading.find_next_siblings()):
                if sibling.name and re.match('^h[2-3]$', sibling.name):
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
            @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;700&display=swap');
            html { scroll-behavior: smooth; }
            body {
                font-family: 'Poppins', sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 1rem 2rem;
            }
            .container { display: flex; gap: 2rem; }
            #navigator {
                width: 300px;
                flex-shrink: 0;
                position: sticky;
                top: 1rem;
                align-self: flex-start;
                height: calc(100vh - 2rem);
                overflow-y: auto;
                padding-right: 1.5rem;
            }
            #navigator ul {
                list-style-type: none;
                padding: 0;
                margin: 0;
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
                padding: 0.25em 0.5em;
                padding-left: 1.5em;
                border-radius: 4px;
                transition: background-color 0.2s;
                position: relative;
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
            }
            #content ul {
                list-style-type: none;
                padding-left: 1.5em;
            }
            hr {
                border: none;
                border-top: 1px solid #e0e0e0;
                margin: 2.5em 0;
            }
            .collapsible-heading {
                font-size: inherit;
                font-weight: bold;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 0.5em;
                user-select: none;
                margin-top: 1.5em;
                margin-bottom: 0.5em;
            }
            h3.collapsible-heading { margin-left: 1.5em; }
            .collapsible-heading::before {
                content: '−';
                font-weight: bold;
                color: #555;
                width: 1em;
                text-align: center;
            }
            .collapsible-heading.collapsed::before { content: '+'; }
            .intro-content, .collapsible-content {
                padding-left: 1.5em;
                border-left: 1px solid #e0e0e0;
                margin-left: 0.4em;
            }
            .collapsible-content {
                overflow: hidden;
                transition: max-height 0.4s ease-out;
                max-height: 5000px;
            }
            h3.collapsible-heading + .collapsible-content { margin-left: 1.9em; }
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
                        } else {
                            content.style.maxHeight = content.scrollHeight + 'px';
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
                
                // --- Navigator Logic (Collapsing and Scrolling) ---
                const navLinks = document.querySelectorAll('#navigator a');
                document.querySelectorAll('#navigator .nav-collapsible').forEach(item => {
                    item.classList.remove('nav-expanded');
                });
                navLinks.forEach(link => {
                    link.addEventListener('click', function(e) {
                        e.preventDefault();
                        const parentLi = this.parentElement;
                        if (parentLi && parentLi.classList.contains('nav-collapsible')) {
                            parentLi.classList.toggle('nav-expanded');
                        }
                        const targetId = this.getAttribute('href');
                        const targetElement = document.querySelector(targetId);
                        if (targetElement) {
                            if (targetElement.classList.contains('collapsible-heading') && targetElement.classList.contains('collapsed')) {
                                toggleSection(targetElement);
                            }
                            setTimeout(() => {
                                targetElement.scrollIntoView({
                                    behavior: 'smooth',
                                    block: 'start'
                                });
                            }, 150);
                        }
                    });
                });
            });
        """
        body.append(script_tag)
        
        return str(soup)