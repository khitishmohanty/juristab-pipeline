import os
import json
import pandas as pd
from typing import Union, List, Dict, Any
from pdf2image import convert_from_path
import re # For markdown processing

# --- Helper to load tag mapping ---
def load_tag_mapping():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_path = os.path.join(current_dir, "../config/tag_mapping.json")
        if not os.path.exists(mapping_path):
            project_root_guess = os.path.abspath(os.path.join(current_dir, ".."))
            mapping_path = os.path.join(project_root_guess, "config/tag_mapping.json")
            if not os.path.exists(mapping_path):
                mapping_path = os.path.join(current_dir, "tag_mapping.json")

        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️ Tag mapping file not found at expected path(s). Using default mapping.")
        return {
            "Header": "header", "Title": "h1", "Subtitle": "h2",
            "Heading-h1": "h1", "Heading-h2": "h2", "Heading-h3": "h3",
            "Heading-h4": "h4", "Heading-h5": "h5", "Heading-h6": "h6",
            "Paragraph": "p",
            "List": "ul", "List-l1": "ul", "List-l2": "ul", "List-l3": "ul", "List-l4": "ul",
            "Table": "table", "Figure": "figure",
            "Table of Contents": "nav", "Footer": "footer", "Footnote": "aside",
            "Page number": "span", "Enum": "span", "Endnotes": "section", "Glossary": "section"
        }

# --- Helper to load include config ---
def load_include_config():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "../config/html_include_config.json")
        if not os.path.exists(config_path): # Fallback to current directory
            config_path = os.path.join(current_dir, "html_include_config.json")
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f).get("include_tags", [])
    except Exception as e:
        print(f"⚠️ Failed to load HTML include config: {e}")
        return []


HTML_TAG_MAP = load_tag_mapping()
HTML_INCLUDE_TAGS = load_include_config()

# --- Helper function to escape HTML content ---
def escape_html(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#39;")

# --- Enhanced function to convert markdown and process explicit hyperlinks ---
def process_content_for_html(content_str: str,
                             explicit_hyperlinks: List[Dict[str, str]] = None,
                             is_heading_content: bool = False) -> str:
    if not isinstance(content_str, str):
        content_str = str(content_str)

    processed_content = content_str.strip()

    if is_heading_content:
        # For headings, clean, escape, and ensure single line.
        if processed_content.startswith("**") and processed_content.endswith("**") and len(processed_content) >= 4:
            processed_content = processed_content[2:-2]
        elif processed_content.startswith("*") and processed_content.endswith("*") and len(processed_content) >= 2:
            processed_content = processed_content[1:-1]
        processed_content = escape_html(processed_content.strip())
        # MODIFICATION: Replace newlines in headings with a space for single-line output
        processed_content = processed_content.replace('\n', ' ')
    else:
        # For other content, escape first, then apply markdown, then handle newlines.
        processed_content = escape_html(processed_content)
        processed_content = re.sub(r'\*\*(?=\S)(.*?)(?<=\S)\*\*|__(?=\S)(.*?)(?<=\S)__', r'<strong>\1\2</strong>', processed_content)
        processed_content = re.sub(r'\*(?=\S)(.*?)(?<=\S)\*|_(?=\S)(.*?)(?<=\S)_', r'<em>\1\2</em>', processed_content)
        processed_content = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', processed_content)
        # For non-headings, newlines become <br>
        processed_content = processed_content.replace('\n', '<br>\n')

    if explicit_hyperlinks and isinstance(explicit_hyperlinks, list) and not is_heading_content:
        links_html_parts = ["<div class='explicit-hyperlinks'><strong>Links:</strong><ul>"]
        for link_info in explicit_hyperlinks:
            if isinstance(link_info, dict):
                url = link_info.get("url", "#")
                text = link_info.get("text", url)
                links_html_parts.append(f'<li><a href="{escape_html(url)}">{escape_html(text)}</a></li>')
        links_html_parts.append("</ul></div>")
        processed_content += "\n" + "".join(links_html_parts)

    return processed_content


# --- Function to convert book_output.json content to hierarchical, collapsible HTML ---
def convert_book_json_to_html(
    book_data: Dict[str, List[Dict[str, Any]]],
    output_dir: str,
    output_filename: str = "book_output.html"
) -> None:
    html_body_parts = []
    all_items = []
    # MODIFICATION: Initialize list to collect footnotes
    collected_footnotes_html = []

    sorted_page_numbers = sorted(book_data.keys(), key=lambda p: int(p) if p.isdigit() else float('inf'))
    for page_num_str in sorted_page_numbers:
        page_items = book_data.get(page_num_str, [])
        if isinstance(page_items, list):
            all_items.extend(page_items)
        else:
            all_items.append({"tag": "Error", "content": f"Invalid content structure for page {page_num_str}: {page_items}"})

    open_details_stack = []
    currently_in_list_block = False

    for i, item in enumerate(all_items):
        if not isinstance(item, dict):
            html_body_parts.append(f"<p>Error: Encountered non-dictionary item: {escape_html(str(item))}</p>\n")
            continue

        tag_key = item.get("tag", "Paragraph")
        raw_content = item.get("content", "")
        explicit_hyperlinks = item.get("hyperlinks")

        # MODIFICATION: Handle Footnotes - Collect and skip inline rendering
        if tag_key == "Footnote":
            if currently_in_list_block: # Close list if footnote interrupts it
                html_body_parts.append("</ul>\n")
                currently_in_list_block = False
            
            footnote_html_tag = HTML_TAG_MAP.get("Footnote", "aside")
            # Process footnote content (typically not as a heading)
            processed_footnote_content = process_content_for_html(str(raw_content), explicit_hyperlinks, is_heading_content=False)
            collected_footnotes_html.append(f"<{footnote_html_tag} class='footnote-item'>{processed_footnote_content}</{footnote_html_tag}>\n")
            continue # Skip normal rendering flow for footnotes

        # Rule: Always skip items tagged as "Table of Contents"
        if tag_key == "Table of Contents":
            if currently_in_list_block:
                html_body_parts.append("</ul>\n")
                currently_in_list_block = False
            continue

        # Rule: Filter items based on HTML_INCLUDE_TAGS
        if HTML_INCLUDE_TAGS and (tag_key not in HTML_INCLUDE_TAGS):
            continue

        html_tag_from_map = HTML_TAG_MAP.get(tag_key, "p")
        
        is_title_tag = (tag_key == "Title")
        is_subtitle_tag = (tag_key == "Subtitle")

        if is_title_tag or is_subtitle_tag:
            if currently_in_list_block:
                html_body_parts.append("</ul>\n")
                currently_in_list_block = False
            while open_details_stack:
                open_details_stack.pop()
                html_body_parts.append("</div></details>\n")
            
            processed_content_html = process_content_for_html(str(raw_content), is_heading_content=True) 
            html_body_parts.append(f"<{html_tag_from_map}>{processed_content_html}</{html_tag_from_map}>\n")
            continue

        is_regular_heading = html_tag_from_map.startswith("h") and \
                             len(html_tag_from_map) == 2 and \
                             html_tag_from_map[1].isdigit()
        
        current_heading_level = int(html_tag_from_map[1]) if is_regular_heading else 0
        is_list_item_tag = tag_key.startswith("List")

        if not is_regular_heading and isinstance(raw_content, str):
            temp_check_content = raw_content.strip().lower()
            if re.fullmatch(r'page(\s*no\.?)?(\s*\d+)?\s*', temp_check_content):
                raw_content = ""

        if not is_list_item_tag and currently_in_list_block:
            html_body_parts.append("</ul>\n")
            currently_in_list_block = False
        
        if is_list_item_tag and not currently_in_list_block:
            list_block_tag = HTML_TAG_MAP.get(tag_key, "ul")
            html_body_parts.append(f"<{list_block_tag} class='content-list'>\n")
            currently_in_list_block = True

        if is_regular_heading:
            if currently_in_list_block:
                html_body_parts.append("</ul>\n")
                currently_in_list_block = False
            
            while open_details_stack and open_details_stack[-1] >= current_heading_level:
                open_details_stack.pop()
                html_body_parts.append("</div></details>\n")

            # MODIFICATION: Expanded word list for skipping headings based on content
            content_to_check = str(raw_content).strip().lower()
            skippable_heading_contents = [
                "heading", "headings",
                "content", "contents",
                "table of contents", "tables of contents"
            ]
            if content_to_check in skippable_heading_contents:
                continue

            summary_content_html = process_content_for_html(str(raw_content), is_heading_content=True)
            details_class = f"details-level-{current_heading_level}"
            html_body_parts.append(f"<details open class='{details_class}'>\n  <summary><{html_tag_from_map}>{summary_content_html}</{html_tag_from_map}></summary>\n<div class='collapsible-content-wrapper'>\n")
            open_details_stack.append(current_heading_level)
        
        elif is_list_item_tag:
            list_item_level = 1 
            if "-" in tag_key: 
                try: list_item_level = int(tag_key.split('-l')[-1])
                except ValueError: pass 
            
            li_class = f"li-level-{list_item_level}"
            processed_list_items_html = ""
            actual_list_items_to_render = []
            if isinstance(raw_content, str):
                actual_list_items_to_render = raw_content.split('\n')
            elif isinstance(raw_content, list):
                actual_list_items_to_render = [str(li) for li in raw_content]
            
            for li_content in actual_list_items_to_render:
                li_content_stripped = li_content.strip()
                li_content_stripped = re.sub(r'^[\s]*[\*\-\•\.]\s*', '', li_content_stripped) 
                if li_content_stripped:
                    li_html = process_content_for_html(li_content_stripped, None, is_heading_content=False)
                    processed_list_items_html += f"  <li class='{li_class}'>{li_html}</li>\n" 
            
            if processed_list_items_html:
                 html_body_parts.append(processed_list_items_html)

        else: 
            if isinstance(raw_content, str) and not raw_content.strip() and \
               tag_key != "Table" and not explicit_hyperlinks: # ToC already handled
                continue

            processed_content_html = ""
            if tag_key == "Table":
                processed_content_html += f"<{html_tag_from_map} class='data-table'>\n"
                if isinstance(raw_content, list) and all(isinstance(row, list) for row in raw_content):
                    header_processed_in_list_table = False; tbody_opened = False
                    for i_row, row_data in enumerate(raw_content):
                        is_header_row_struct = (i_row == 0 and isinstance(row_data[0], dict) and row_data[0].get("isHeader"))
                        if is_header_row_struct and not header_processed_in_list_table:
                            processed_content_html += "  <thead>\n"; header_processed_in_list_table = True
                        elif not header_processed_in_list_table and not tbody_opened :
                            processed_content_html += "  <tbody>\n"; tbody_opened = True
                        processed_content_html += "    <tr>\n"
                        default_cell_tag = "th" if is_header_row_struct else "td"
                        for cell_item in row_data:
                            cell_content_str = str(cell_item.get("content", "")) if isinstance(cell_item, dict) else str(cell_item)
                            colspan = cell_item.get("colspan", 1) if isinstance(cell_item, dict) else 1
                            rowspan = cell_item.get("rowspan", 1) if isinstance(cell_item, dict) else 1
                            cell_tag_override = "th" if isinstance(cell_item, dict) and cell_item.get("isHeader") else default_cell_tag
                            attrs = (f' colspan="{colspan}"' if colspan > 1 else "") + (f' rowspan="{rowspan}"' if rowspan > 1 else "")
                            processed_content_html += f"      <{cell_tag_override}{attrs}>{process_content_for_html(cell_content_str, None, False)}</{cell_tag_override}>\n"
                        processed_content_html += "    </tr>\n"
                        if is_header_row_struct and i_row == 0 :
                            processed_content_html += "  </thead>\n"
                            if not tbody_opened and i_row + 1 < len(raw_content):
                                processed_content_html += "  <tbody>\n"; tbody_opened = True
                    if tbody_opened: processed_content_html += "  </tbody>\n"
                elif isinstance(raw_content, str):
                    lines = raw_content.strip().split('\n')
                    header_processed_str_table = False; in_tbody_str_table = False
                    for line_idx, line_content in enumerate(lines):
                        line_content = line_content.strip()
                        if not line_content: continue
                        if '|' in line_content: 
                            cells = [cell.strip() for cell in line_content.split('|') if cell.strip()]
                            if not cells: continue
                            is_md_header_separator = "---" in line_content.replace(" ", "").replace("|","") and \
                                                    all("-" in c or not c for c in cells)
                            if not header_processed_str_table and \
                               (line_idx + 1 < len(lines) and "---" in lines[line_idx+1].replace(" ", "").replace("|","")):
                                if not in_tbody_str_table: processed_content_html += "  <thead>\n"
                                processed_content_html += "    <tr>\n"
                                for cell in cells: processed_content_html += f"      <th>{process_content_for_html(cell, None, False)}</th>\n"
                                processed_content_html += "    </tr>\n"
                                if not in_tbody_str_table: processed_content_html += "  </thead>\n"
                                header_processed_str_table = True
                            elif is_md_header_separator:
                                if not in_tbody_str_table:
                                    processed_content_html += "  <tbody>\n"; in_tbody_str_table = True
                                continue
                            else:
                                if not in_tbody_str_table:
                                     processed_content_html += "  <tbody>\n"; in_tbody_str_table = True
                                processed_content_html += "    <tr>\n"
                                for cell in cells: processed_content_html += f"      <td>{process_content_for_html(cell, None, False)}</td>\n"
                                processed_content_html += "    </tr>\n"
                        else:
                            if not in_tbody_str_table: processed_content_html += "  <tbody>\n"; in_tbody_str_table = True
                            processed_content_html += f"<tr><td colspan='100'>{process_content_for_html(line_content, explicit_hyperlinks if line_idx == 0 else None, False)}</td></tr>\n"
                    if in_tbody_str_table: processed_content_html += "  </tbody>\n"
                else:
                    processed_content_html += f"  <tbody><tr><td>{process_content_for_html(str(raw_content), None, False)}</td></tr></tbody>\n"
                processed_content_html += f"</{html_tag_from_map}>\n"
            else:
                content_to_render = process_content_for_html(str(raw_content), explicit_hyperlinks, is_heading_content=False)
                processed_content_html = f"<{html_tag_from_map}>{content_to_render}</{html_tag_from_map}>\n"
            
            html_body_parts.append(processed_content_html)

    if currently_in_list_block:
        html_body_parts.append("</ul>\n")
    while open_details_stack:
        open_details_stack.pop()
        html_body_parts.append("</div></details>\n")

    # MODIFICATION: Append collected footnotes at the end of the body
    if collected_footnotes_html:
        html_body_parts.append("<hr class='footnotes-separator'>\n") # Optional separator
        html_body_parts.append("<section class='footnotes-section'>\n")
        html_body_parts.append("<h2>Footnotes</h2>\n") # Heading for the footnotes section
        html_body_parts.extend(collected_footnotes_html)
        html_body_parts.append("</section>\n")

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Book Output</title>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            line-height: 1.6; 
            margin: 0 auto; 
            max-width: 900px; 
            padding: 20px; 
            font-size: 16px; 
        }}
        details {{ 
            margin-bottom: 0.25em; 
            border: none; 
            background-color: transparent; 
        }}
        summary {{ 
            padding-top: 0.3em; 
            padding-bottom: 0.3em;
            cursor: pointer; 
            list-style-position: inside;
            font-weight: bold; 
            background-color: transparent; 
            border-bottom: none; 
        }}
        .collapsible-content-wrapper {{ 
            padding-top: 0.5em; 
            padding-bottom: 0.5em;
        }}
        summary > h1, summary > h2, summary > h3, summary > h4, summary > h5, summary > h6 {{ 
            display: inline; 
            margin: 0; 
            font-size: 1em; 
            font-weight: bold;
        }}
        h1, h2 {{ margin-top: 0.8em; margin-bottom: 0.4em; }}

        .details-level-1 {{ padding-left: 0em; }} 
        .details-level-2 {{ padding-left: 1.5em; }}   
        .details-level-3 {{ padding-left: 3em; }} 
        .details-level-4 {{ padding-left: 4.5em; }}
        .details-level-5 {{ padding-left: 6em; }}
        .details-level-6 {{ padding-left: 7.5em; }}

        p {{ margin-top:0; margin-bottom: 0.8em; }}
        
        ul.content-list, ol.content-list {{
            margin-top:0; margin-bottom: 0.8em; 
            padding-left: 0; 
            list-style-type: none;
        }}
        ul.content-list li, ol.content-list li {{
            list-style-type: none; 
            margin-bottom: 0.2em; 
        }}
        li.li-level-1 {{ padding-left: 1em; }} 
        li.li-level-2 {{ padding-left: 2.5em; }} 
        li.li-level-3 {{ padding-left: 4em; }}   
        li.li-level-4 {{ padding-left: 5.5em; }}

        table.data-table {{ width: 100%; border-collapse: collapse; margin-bottom: 1em; }}
        table.data-table th, table.data-table td {{ border: 1px solid #ccc; padding: 8px; text-align: left; }}
        table.data-table th {{ background-color: #f2f2f2; font-weight: bold; }}
        figure {{ margin: 1em 0; text-align: center; }}
        figure img {{ max-width: 100%; height: auto; border: 1px solid #ddd; }}
        figcaption {{ font-style: italic; font-size: 0.9em; color: #555; margin-top: 0.5em; }}
        
        .explicit-hyperlinks {{ font-size: 0.9em; color: #333; background-color: #f0f7fd; border: 1px dashed #add8e6; padding: 8px; margin-top: 8px; border-radius: 3px;}}
        .explicit-hyperlinks ul {{padding-left: 15px; margin-top: 5px;}}
        .explicit-hyperlinks strong {{color: #0056b3;}}

        /* Styles for Footnotes Section */
        .footnotes-separator {{ margin-top: 2em; margin-bottom: 1em; }}
        .footnotes-section {{ margin-top: 1em; padding-top: 1em; border-top: 1px solid #eee; }}
        .footnotes-section h2 {{ font-size: 1.2em; margin-bottom: 0.5em; }}
        .footnote-item {{ 
            font-size: 0.9em; 
            margin-bottom: 0.5em; 
            padding-left: 1em; /* Optional indentation for footnote items */
            background-color: transparent; /* Ensure it doesn't inherit strange backgrounds */
        }}
    </style>
</head>
<body>
    {''.join(html_body_parts)}
</body>
</html>"""

    output_path = os.path.join(output_dir, output_filename)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_html)
        print(f"✅ Hierarchical collapsible HTML file with updated logic created at: {output_path}")
    except Exception as e:
        print(f"❌ Error writing HTML file {output_path}: {e}")


# --- Existing functions (convert_json_to_csv_and_excel, convert_pdf_to_images, convert_json_to_html_simple) ---
# These functions remain unchanged from your provided script.
# For brevity, I am not re-listing them here but they should be part of the final file_converters.py

def convert_json_to_csv_and_excel(
    json_input: Union[str, List[Dict]],
    output_dir: str,
    base_filename: str = "gemini_output"
) -> None:
    if isinstance(json_input, str):
        with open(json_input, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = json_input
    if not data or not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        print(f"ℹ️ No valid list of dictionaries found in json_input for {base_filename}. Skipping CSV/Excel conversion.")
        return
    df = pd.DataFrame(data)
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    excel_path = os.path.join(output_dir, f"{base_filename}.xlsx")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"✅ Saved CSV to: {csv_path}")
    except Exception as e:
        print(f"❌ Error saving CSV {csv_path}: {e}")
    try:
        df.to_excel(excel_path, index=False)
        print(f"✅ Saved Excel to: {excel_path}")
    except Exception as e:
        print(f"❌ Error saving Excel {excel_path}: {e}")

def convert_pdf_to_images(
    pdf_path: str, output_dir: str, image_format: str = "jpeg",
    dpi: int = 200, poppler_path: str = None # type: ignore
) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    images = convert_from_path(pdf_path, dpi=dpi, poppler_path=poppler_path)
    image_paths = []
    for i, img in enumerate(images):
        image_filename = f"page_{i + 1}.{image_format.lower()}"
        image_path = os.path.join(output_dir, image_filename)
        save_format = "JPEG" if image_format.lower() == "jpeg" else image_format.upper()
        img.save(image_path, format=save_format)
        image_paths.append(image_path)
    print(f"✅ Converted {len(image_paths)} pages to images in: {output_dir}")
    return image_paths

# Simple HTML converter (kept for potential other uses, but not for book_output.json)
def convert_json_to_html_simple(
    json_input: Union[str, List[Dict]],
    output_dir: str,
    output_filename: str = "output_simple.html"
) -> None:
    if isinstance(json_input, str):
        with open(json_input, "r", encoding="utf-8") as f: data = json.load(f)
    else: data = json_input
    html_elements = []
    if isinstance(data, list):
        for node in data:
            if isinstance(node, dict):
                tag_key = node.get("tag", "Paragraph"); content = node.get("content", "")
                html_tag = HTML_TAG_MAP.get(tag_key, "p")
                html_elements.append(f"<{html_tag}>{escape_html(content)}</{html_tag}>")
            else: html_elements.append(f"<p>Error: Non-dictionary item found: {escape_html(str(node))}</p>")
    elif isinstance(data, dict):
        html_elements.append(f"<p>Error: Input is a dictionary, simple converter expects a list of items. Try convert_book_json_to_html.</p>")
    else: html_elements.append(f"<p>Error: Invalid JSON input type for simple HTML conversion.</p>")
    full_html = ("<!DOCTYPE html>\n<html lang='en'>\n<head>\n<meta charset=\"UTF-8\">\n" "<title>Generated Simple Document</title>\n</head>\n<body>\n    " + "\n    ".join(html_elements) + "\n</body>\n</html>")
    output_path = os.path.join(output_dir, output_filename);
    with open(output_path, "w", encoding="utf-8") as f: f.write(full_html)
    print(f"✅ Simple HTML file created at: {output_path}")