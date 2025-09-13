import json
import html
import re

class HtmlGenerator:
    """
    Generates an interactive HTML flowchart from a JuriTree JSON object.
    The generated HTML uses TailwindCSS for styling and is fully self-contained.
    """

    def __init__(self):
        """Initializes the generator with a color palette for tags."""
        self.tag_color_palette = [
            '#E96822', '#405FAE', '#96A428', '#CD54E3',
            '#964FE2', '#E7C027', '#40AE5B', '#388EAD'
        ]

    def _get_text_color_for_bg(self, hex_color: str) -> str:
        """Sets the text color for the tags to white."""
        return 'white'

    def _format_tooltip_text(self, text: str) -> str:
        """Finds patterns like 'Reason 1:' and makes them bold."""
        escaped_text = html.escape(text)
        formatted_text = re.sub(r'(Reason\s*\d*:)', r'<strong>\1</strong>', escaped_text, flags=re.IGNORECASE)
        return formatted_text

    def _render_node_html(self, node: dict, is_root: bool = False) -> str:
        """Renders a single flowchart node and its expandable children into an HTML string."""
        if not node:
            return ""

        node_id = html.escape(node.get('id', ''))
        node_type = html.escape(node.get('type', ''))
        raw_title = node.get('title', '')

        tag_html = ""
        display_title = html.escape(raw_title)

        if ':' in raw_title:
            parts = [part.strip() for part in raw_title.split(':', 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                tag_text, title_text = parts
                display_title = html.escape(title_text)

                hash_val = sum(ord(c) for c in tag_text)
                color_index = hash_val % len(self.tag_color_palette)
                tag_bg_color = self.tag_color_palette[color_index]

                tag_text_color = self._get_text_color_for_bg(tag_bg_color)
                tag_html = f'<div class="node-tag" style="background-color: {tag_bg_color}; color: {tag_text_color};">{html.escape(tag_text)}</div>'

        expander_html = ""
        children_html = ""
        if node.get('children'):
            expander_html = f'<div class="node-expander" data-node-id="{node_id}">+</div>'
            children_html = self._render_children_html(node.get('children', []), node.get('id', ''))

        wrapper_id = 'id="root-node-wrapper"' if is_root else ''

        tooltip_data = node.get('tooltip', {})
        tooltip_what = self._format_tooltip_text(tooltip_data.get('what', ''))
        tooltip_who = self._format_tooltip_text(tooltip_data.get('who', ''))
        tooltip_why = self._format_tooltip_text(tooltip_data.get('why', ''))

        reference_data = node.get('reference', {})
        ref_text = html.escape(reference_data.get('refText', ''))
        ref_popup_text = html.escape(reference_data.get('refPopupText', ''))

        return f"""
        <div {wrapper_id} class="flowchart-node-wrapper">
            <div class="flowchart-node {node_type}" data-node-id="{node_id}">
                {tag_html}
                <span>{display_title}</span>
                <div class="tooltip" data-parent-node-id="{node_id}">
                    <div class="popup-close-btn">&times;</div>
                    <div class="tooltip-item"><strong>What:</strong><div class="tooltip-content">{tooltip_what}</div></div>
                    <div class="tooltip-item"><strong>Who:</strong><div class="tooltip-content">{tooltip_who}</div></div>
                    <div class="tooltip-item"><strong>Why:</strong><div class="tooltip-content">{tooltip_why}</div></div>
                    <span class="tooltip-ref">
                        {ref_text}
                        <div class="ref-popup" data-parent-node-id="{node_id}">
                            <div class="popup-close-btn">&times;</div>
                            {ref_popup_text}
                        </div>
                    </span>
                </div>
                {expander_html}
            </div>
        </div>
        {children_html}
        """

    def _render_children_html(self, children: list, parent_id: str) -> str:
        """Renders the children of a node inside a collapsible container."""
        if not children:
            return ""

        # Default connector for a single child is a simple vertical line.
        connector_html = '<div class="w-px h-12 bg-gray-300 mx-auto"></div>'

        # For multiple children, generate an SVG for a T-junction with rounded corners.
        if len(children) > 1:
            num_children = len(children)
            viewbox_width = 1000
            viewbox_height = 50
            h_bar_y = 25
            radius = 10

            x_coords = [((i + 0.5) / num_children) * viewbox_width for i in range(num_children)]
            x_min = min(x_coords)
            x_max = max(x_coords)
            center_x = viewbox_width / 2
            
            path_commands = []
            
            rake_path = f"M {x_min} {viewbox_height} V {h_bar_y + radius} "
            rake_path += f"A {radius} {radius} 0 0 1 {x_min + radius} {h_bar_y} "
            rake_path += f"H {x_max - radius} "
            rake_path += f"A {radius} {radius} 0 0 1 {x_max} {h_bar_y + radius} "
            rake_path += f"V {viewbox_height}"
            path_commands.append(rake_path)
            
            for x in x_coords[1:-1]:
                path_commands.append(f"M {x} {h_bar_y} V {viewbox_height}")

            branch_midpoint_x = (x_min + x_max) / 2
            path_commands.append(f"M {center_x} 0 V {h_bar_y}")
            path_commands.append(f"M {center_x} {h_bar_y} H {branch_midpoint_x}")

            final_path_d = " ".join(path_commands)

            connector_html = f"""
            <div class="connector-svg-container" style="height: {viewbox_height}px;">
                <svg width="100%" height="{viewbox_height}" viewBox="0 0 {viewbox_width} {viewbox_height}" preserveAspectRatio="none">
                    <path d="{final_path_d}" stroke="#d1d5db" stroke-width="1.5" fill="none" />
                </svg>
            </div>
            """

        is_main_branch = any(child.get('type') == 'node-primary-branch' for child in children)

        branch_container_class = "flex flex-row gap-8 w-full flowchart-branch" if is_main_branch else \
                                 ("flex flex-col md:flex-row gap-8 w-full" if len(children) > 1 else "flex flex-col items-center w-full")

        child_wrapper_class = "flex-1 flex flex-col items-center flowchart-column" if len(children) > 1 else "w-full flowchart-column"

        child_branches = [f'<div class="{child_wrapper_class}">{self._render_node_html(child)}</div>' for child in children]

        return f"""
        <div class="node-children-container" id="children-of-{parent_id}">
            <div class="children-content">
                {connector_html}
                <div class="{branch_container_class}">
                    {''.join(child_branches)}
                </div>
            </div>
        </div>
        """

    def generate_html_tree(self, json_data: dict) -> str:
        """Generates the complete HTML string for the interactive flowchart."""
        flowchart_data = json_data.get('flowchart', {})
        title = html.escape(flowchart_data.get('title', 'JuriTree Flowchart'))
        subtitle = html.escape(flowchart_data.get('subtitle', ''))
        root_node = flowchart_data.get('rootNode')
        final_outcome = flowchart_data.get('finalOutcome')

        root_html = self._render_node_html(root_node, is_root=True)
        final_outcome_html = self._render_node_html(final_outcome)

        interstitial_connector = '<div class="w-px h-16 bg-gray-300 mx-auto"></div>' if root_html and final_outcome_html else ''

        javascript_code = """
        document.addEventListener('DOMContentLoaded', () => {
            const viewport = document.getElementById('viewport');
            const zoomContainer = document.getElementById('zoom-container');
            const expandAllToggle = document.getElementById('expand-all-toggle');
            let activeNodePopup = null;
            let activeRefPopup = null;

            const debounce = (func, delay) => {
                let timeoutId;
                return (...args) => {
                    clearTimeout(timeoutId);
                    timeoutId = setTimeout(() => {
                        func.apply(this, args);
                    }, delay);
                };
            };

            // Move all popups to the body to ensure they are in the top-level stacking context
            document.querySelectorAll('.tooltip, .ref-popup').forEach(popup => {
                document.body.appendChild(popup);
            });

            const closeAllPopups = () => {
                if (activeRefPopup) {
                    activeRefPopup.classList.remove('is-visible');
                    activeRefPopup = null;
                }
                if (activeNodePopup) {
                    activeNodePopup.classList.remove('is-visible');
                    activeNodePopup = null;
                }
                document.querySelectorAll('.is-active-node').forEach(n => n.classList.remove('is-active-node'));
            };

            document.querySelectorAll('.flowchart-node').forEach(node => {
                const nodeId = node.dataset.nodeId;
                const tooltip = document.querySelector(`.tooltip[data-parent-node-id="${nodeId}"]`);
                if (!tooltip) return;

                node.addEventListener('click', (event) => {
                    if (event.target.closest('.node-expander, .tooltip-ref, .popup-close-btn')) return;
                    if (tooltip.classList.contains('is-visible')) return;
                    
                    closeAllPopups();
                    activeNodePopup = tooltip;

                    const nodeRect = node.getBoundingClientRect();
                    tooltip.classList.add('is-visible');
                    const tooltipRect = tooltip.getBoundingClientRect();

                    let top = nodeRect.top - tooltipRect.height - 10;
                    if (top < 5) {
                        top = nodeRect.bottom + 10;
                    }
                    let left = nodeRect.left + (nodeRect.width / 2) - (tooltipRect.width / 2);
                    if (left < 5) { left = 5; }
                    if (left + tooltipRect.width > window.innerWidth) { left = window.innerWidth - tooltipRect.width - 5; }
                    
                    tooltip.style.top = `${top}px`;
                    tooltip.style.left = `${left}px`;
                    
                    node.closest('.flowchart-node-wrapper').classList.add('is-active-node');
                });
            });

            document.querySelectorAll('.tooltip-ref').forEach(ref => {
                const parentTooltip = ref.closest('.tooltip');
                if (!parentTooltip) return;

                const nodeId = parentTooltip.dataset.parentNodeId;
                const refPopup = document.querySelector(`.ref-popup[data-parent-node-id="${nodeId}"]`);
                if (!refPopup) return;
                
                ref.addEventListener('click', (event) => {
                    event.stopPropagation();
                    if (refPopup.classList.contains('is-visible')) {
                        refPopup.classList.remove('is-visible');
                        activeRefPopup = null;
                        return;
                    }

                    if(activeRefPopup) activeRefPopup.classList.remove('is-visible');
                    activeRefPopup = refPopup;

                    const refRect = ref.getBoundingClientRect();
                    refPopup.classList.add('is-visible');
                    const popupRect = refPopup.getBoundingClientRect();

                    // Horizontal positioning
                    let left = refRect.right + 10;
                    if (left + popupRect.width > window.innerWidth) {
                        left = refRect.left - popupRect.width - 10;
                    }
                    
                    // Vertical positioning with screen boundary check
                    let top = refRect.top;
                    if (top + popupRect.height > window.innerHeight) {
                        top = window.innerHeight - popupRect.height - 10;
                    }
                     if (top < 0) {
                        top = 5;
                    }
                    
                    refPopup.style.top = `${top}px`;
                    refPopup.style.left = `${left}px`;
                });
            });

            document.querySelectorAll('.popup-close-btn').forEach(btn => {
                btn.addEventListener('click', (event) => {
                    event.stopPropagation();
                    const parentRefPopup = btn.closest('.ref-popup');
                    if (parentRefPopup) {
                        parentRefPopup.classList.remove('is-visible');
                        activeRefPopup = null;
                    } else {
                        closeAllPopups();
                    }
                });
            });
            
            document.addEventListener('mousedown', (event) => {
                if (!event.target.closest('.flowchart-node-wrapper, .tooltip, .ref-popup')) {
                    closeAllPopups();
                }
            });

            document.querySelectorAll('.node-expander').forEach(expander => {
                expander.addEventListener('click', (event) => {
                    event.stopPropagation();
                    closeAllPopups();
                    const nodeId = expander.getAttribute('data-node-id');
                    const childrenContainer = document.getElementById(`children-of-${nodeId}`);
                    if (childrenContainer) {
                        childrenContainer.classList.toggle('is-expanded');
                        expander.textContent = childrenContainer.classList.contains('is-expanded') ? '−' : '+';
                    }
                });
            });
            
            expandAllToggle.addEventListener('change', () => {
                const isExpanded = expandAllToggle.checked;
                document.querySelectorAll('.node-children-container').forEach(c => c.classList.toggle('is-expanded', isExpanded));
                document.querySelectorAll('.node-expander').forEach(e => e.textContent = isExpanded ? '−' : '+');
            });

            const setFlowchartWidth = () => {
                const contentContainer = document.getElementById('flowchart-content');
                const mainBranchContainer = contentContainer.querySelector('.flowchart-branch');
                if(mainBranchContainer) {
                    contentContainer.style.minWidth = '0px';
                    const fullWidth = mainBranchContainer.scrollWidth;
                    contentContainer.style.minWidth = (fullWidth + 50) + 'px';
                }
            };
            const debouncedSetFlowchartWidth = debounce(setFlowchartWidth, 150);
            setFlowchartWidth();
            window.addEventListener('resize', debouncedSetFlowchartWidth);

            let scale = 0.8, panX = 0, panY = 0, isPanning = false, startX = 0, startY = 0;
            const updateTransform = () => {
                zoomContainer.style.transform = `translate(${panX}px, ${panY}px) scale(${scale})`;
            };
            updateTransform();

            viewport.addEventListener('mousedown', (event) => {
                if (event.button !== 0 || event.target.closest('.flowchart-node-wrapper, .tooltip, .ref-popup, #controls-container')) return;
                isPanning = true; viewport.style.cursor = 'grabbing';
                startX = event.clientX - panX; startY = event.clientY - panY;
            });
            window.addEventListener('mouseup', () => { isPanning = false; viewport.style.cursor = 'grab'; });
            viewport.addEventListener('mousemove', (event) => { if (!isPanning) return; panX = event.clientX - startX; panY = event.clientY - startY; updateTransform(); });
            viewport.addEventListener('wheel', (event) => {
                if (event.ctrlKey) {
                    event.preventDefault();
                    closeAllPopups();
                    scale += event.deltaY > 0 ? -0.02 : 0.02;
                    scale = Math.max(0.2, Math.min(2, scale));
                    updateTransform();
                }
            });
            
            const elementsToAnimate = document.querySelectorAll('#flowchart-content > div');
            elementsToAnimate.forEach((el, index) => {
                el.style.animationDelay = `${index * 0.2}s`;
                el.classList.add('animate-in');
            });
        });
        """

        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}: {subtitle}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        html, body {{
            width: 100%; height: 100%; margin: 0; padding: 0;
        }}
        body {{ font-family: 'Poppins', sans-serif; background-color: #FFFFFF; color: black; }}
        .viewport {{ width: 100%; height: 100%; cursor: grab; overflow: auto; }}
        .zoom-container {{ display: inline-block; transform-origin: top left; padding: 2rem; }}
        #flowchart-content {{ display: flex; flex-direction: column; align-items: center; width: 100%; }}
        #flowchart-content > div {{ opacity: 0; }}
        .flowchart-column {{ overflow: visible; }}
        .flowchart-node-wrapper {{ position: relative; margin-top: 30px; width: 100%; display: flex; justify-content: center; padding-bottom: 15px;}}
        .flowchart-node-wrapper.is-active-node {{ z-index: 100; }}
        .flowchart-node {{
            border: 1px solid #E9E5E5; border-radius: 50px; padding: 1rem 1.25rem;
            text-align: center; position: relative; transition: box-shadow 0.3s ease; max-width: 500px; min-width: 160px;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); cursor: pointer;
            font-size: 0.875rem; font-weight: 500; user-select: none;
            color: black; background-color: white; padding-bottom: 1.5rem;
        }}
        .flowchart-node:hover {{ box-shadow: 0 10px 15px -3px rgba(0,0,0,0.07); }}
        .node-tag {{
            position: absolute; top: -20px; left: 50%; transform: translateX(-50%);
            padding: 0.25rem 0.85rem; border-radius: 9999px; font-size: 0.8rem;
            font-weight: 600; z-index: 5; white-space: nowrap; border: 5px solid white;
        }}
        .node-expander {{
            position: absolute; bottom: -12px; left: 50%; transform: translateX(-50%);
            width: 24px; height: 24px; background-color: #718096; color: white;
            border-radius: 50%; border: 2px solid white;
            font-size: 18px; font-weight: 600;
            z-index: 10; cursor: pointer;
            transition: background-color 0.2s;
            display: flex; align-items: center; justify-content: center;
            padding-bottom: 1px;
        }}
        .node-expander:hover {{ background-color: #2d3748; }}
        .node-children-container {{ display: grid; grid-template-rows: 0fr; transition: grid-template-rows 0.5s ease-in-out; overflow: hidden; }}
        .node-children-container.is-expanded {{ grid-template-rows: 1fr; }}
        .children-content {{ min-height: 0; opacity: 0; transition: opacity 0.4s ease-in-out 0.1s; }}
        .node-children-container.is-expanded .children-content {{ opacity: 1; }}
        .tooltip, .ref-popup {{
            visibility: hidden; opacity: 0;
            position: fixed;
            width: 320px;
            background-color: #FFFFFF; color: black;
            text-align: left; padding: 1rem; border-radius: 0.5rem;
            transition: opacity 0.3s;
            box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px -2px rgba(0,0,0,0.1);
            border: 1px solid #e5e7eb; font-size: 0.8rem;
            user-select: text;
        }}
        .tooltip {{ z-index: 2000; }}
        .ref-popup {{ z-index: 2001; width: 350px; color: #4B5563; }}
        .tooltip.is-visible, .ref-popup.is-visible {{
            visibility: visible; opacity: 1; pointer-events: auto;
        }}
        .tooltip-item {{ margin-bottom: 0.75rem; }}
        .tooltip-item:last-of-type {{ margin-bottom: 0; }}
        .tooltip-item > strong {{ display: block; margin-bottom: 0.35rem; font-weight: 600; color: #1f2937; }}
        .tooltip-content {{ color: #4B5563; }}
        .tooltip-ref {{
            display: block; margin-top: 0.75rem; padding-top: 0.75rem;
            border-top: 1px solid #d1d5db; font-style: italic; color: #6B7280;
            position: relative; cursor: help;
        }}
        .popup-close-btn {{
            position: absolute; top: 5px; right: 10px; width: 20px; height: 20px;
            font-size: 1.5rem; line-height: 20px; color: #aaa; text-align: center;
            cursor: pointer; transition: color 0.2s;
        }}
        .popup-close-btn:hover {{ color: #333; }}
        .overflow-visible-temp {{ overflow: visible !important; }}
        #controls-container {{
            position: fixed; top: 20px; left: 20px;
            z-index: 1000; display: flex; align-items: center; gap: 10px;
            background-color: transparent;
            padding: 8px 12px;
        }}
        .toggle-switch-label {{ font-size: 14px; font-weight: 400; color: #808080; }}
        .toggle-switch {{ position: relative; display: inline-block; width: 44px; height: 24px; }}
        .toggle-switch input {{ opacity: 0; width: 0; height: 0; }}
        .slider {{
            position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0;
            background-color: #ccc; transition: .4s; border-radius: 24px;
        }}
        .slider:before {{
            position: absolute; content: ""; height: 18px; width: 18px;
            left: 3px; bottom: 3px; background-color: white;
            transition: .4s; border-radius: 50%;
        }}
        input:checked + .slider {{ background-color: #48BB78; }}
        input:checked + .slider:before {{ transform: translateX(20px); }}
        .animate-in {{ animation: fadeIn 0.8s ease-out forwards, slideUp 0.8s ease-out forwards; }}
        @keyframes fadeIn {{ to {{ opacity: 1; }} }}
        @keyframes slideUp {{ from {{ transform: translateY(20px); }} to {{ transform: translateY(0); }} }}
    </style>
</head>
<body>
    <div id="controls-container">
        <label class="toggle-switch-label">Expand All</label>
        <label class="toggle-switch">
            <input type="checkbox" id="expand-all-toggle">
            <span class="slider"></span>
        </label>
    </div>
    <div id="viewport" class="viewport">
        <div id="zoom-container" class="zoom-container">
            <div id="flowchart-content">
                <div class="w-full">{root_html}</div>
                {interstitial_connector}
                <div class="w-full">{final_outcome_html}</div>
            </div>
        </div>
    </div>
    <script>{javascript_code}</script>
</body>
</html>
        """