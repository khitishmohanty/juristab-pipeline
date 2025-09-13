import json

class HtmlGenerator:
    """
    Generates an interactive HTML page with three switchable visualizations
    from a JurisMap JSON object.
    """

    def generate_html_tree(self, json_data: dict) -> str:
        """
        Takes a dictionary parsed from the JurisMap JSON and returns a complete
        HTML string for an interactive page with three chart types.

        Args:
            json_data (dict): The case data parsed from a JSON file.

        Returns:
            str: A self-contained HTML document as a string.
        """
        json_string_for_html = json.dumps(json_data)

        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JurisMap Visualization</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;700&display=swap" rel="stylesheet" />
    <script src="https://unpkg.com/gridjs/dist/gridjs.umd.js"></script>
    <link href="https://unpkg.com/gridjs/dist/theme/mermaid.min.css" rel="stylesheet" />

    <style>
        body {{
            font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 0; /* Remove body padding */
            background-color: #fff;
        }}
        /* Add padding to the main content container instead */
        .chart-container {{
            display: flex;
            flex-direction: row;
            align-items: flex-start;
            gap: 40px;
            width: 100%;
            max-width: 1800px;
            margin: 0 auto;
            padding: 20px; /* Apply overall page padding here */
            box-sizing: border-box; /* Include padding in element's total width */
        }}
        .view-selector {{
            padding: 15px;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .view-selector h3 {{
            margin-top: 0;
            margin-bottom: 12px;
            font-size: 16px;
            font-weight: 500;
            color: #333;
        }}
        .view-selector-options {{
             display: flex;
             flex-direction: column;
             gap: 10px;
        }}
        .view-selector-options label {{
            cursor: pointer;
            font-size: 13px;
            display: flex;
            align-items: center;
            gap: 8px;
            color: #555;
        }}
        .view-selector-options input[type="radio"] {{
            accent-color: #888;
            width: 16px;
            height: 16px;
        }}
        .tree-container, .chord-container, .table-container {{
            flex-grow: 1;
            position: relative;
            min-height: 600px;
            display: none;
            overflow: hidden; /* Ensure this container itself doesn't cause outside scrolling */
        }}

        /* Table Specific Styles for Alignment and Overflow */
        #table-container {{
            overflow-x: hidden;
            padding-top: 1px;
        }}

        /* Hide the grid.js search input container */
        #table-container .gridjs-head {{
            display: none;
        }}

        #table-container .gridjs-container {{
            margin-top: 0;
            border-radius: 8px;
            box-sizing: border-box;
            width: 100%;
            display: block;
            overflow: hidden; 
        }}

        #table-container .gridjs-wrapper {{
            width: 100%;
            overflow-x: auto;
            box-sizing: border-box;
            border: none;
            border-radius: 0;
        }}

        #table-container .gridjs-table {{
            width: 100%;
            table-layout: auto;
            border-collapse: collapse;
        }}
        #table-container .gridjs-th {{
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            padding: 8px 10px;
        }}
        #table-container .gridjs-td {{
            word-wrap: break-word;
            white-space: normal;
            padding: 8px 10px;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(8px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        .view-fade-in {{
            animation: fadeIn 0.4s ease-out forwards;
        }}
        #tree-svg, #chord-svg {{
            width: 100%;
            height: 100%;
            display: block;
        }}
        .sidebar {{
            width: 300px;
            flex-shrink: 0;
            padding-top: 0;
            margin-top: 0;
        }}
        .details-panel {{
            background-color: white;
            border-radius: 8px;
            border: 1px solid #e9ecef;
            padding: 20px;
            opacity: 1;
            visibility: visible;
            transition: opacity 0.3s ease-in-out, visibility 0.3s ease-in-out;
        }}
        .details-panel.is-hidden {{
            opacity: 0;
            visibility: hidden;
            height: 0;
            padding-top: 0;
            padding-bottom: 0;
            margin-bottom: 0;
            border: none;
            overflow: hidden;
            transition: opacity 0.3s ease-in-out, visibility 0.3s ease-in-out, height 0.3s ease-in-out, padding 0.3s ease-in-out, margin 0.3s ease-in-out, border 0.3s ease-in-out;
        }}
        .details-panel h3 {{
            margin-top: 0; color: black; font-weight: 500;
            border-bottom: 1px solid #dee2e6; padding-bottom: 10px; font-size: 16px;
        }}
        .details-panel p {{
            color: #7A7171; font-size: 12px; line-height: 1.5;
            transition: opacity 0.2s ease-in-out;
        }}
        .gridjs-search-input {{
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #e9ecef;
            border-radius: 6px;
        }}
        .gridjs-thead .gridjs-th {{
            background-color: #f8f9fa;
        }}
        .gridjs-table, .gridjs-td, .gridjs-th {{
            border-color: #e9ecef !important;
        }}
        .gridjs-td {{
            background-color: transparent;
        }}
        .node {{ cursor: pointer; }}
        .node rect {{ stroke: none; transition: transform 0.2s ease-in-out; }}
        .node:hover rect {{ transform: scale(1.05); }}
        .node text {{ font-size: 8px; text-anchor: middle; fill: #333; pointer-events: none; }}
        .node .type-label {{ font-size: 11px; font-weight: 500; fill: white; pointer-events: none; }}
        .link-group .link {{ fill: none; stroke: #ccc; stroke-width: 1.5px; }}
        .link-group:hover .link {{ stroke: #343a40; }}
        .link-group .link-hitbox {{ fill: none; stroke: transparent; stroke-width: 8px; cursor: pointer; }}
        #arrowhead path {{ fill: #ccc; }}
        .link-group:hover #arrowhead path {{ fill: #343a40; }}

        .level-line {{
            stroke: #adb5bd;
            stroke-width: 1px;
            stroke-dasharray: 2,2;
        }}
        .level-label {{
            font-size: 14px;
            font-weight: 500;
            fill: #495057;
        }}

        .chord-group, .chord-path, .chord-label-group {{
            transition: opacity 0.3s ease-in-out, fill-opacity 0.3s ease-in-out;
        }}
        .chord-group.faded, .chord-path.faded, .chord-label-group.faded {{ opacity: 0.1; }}
        .chord-group {{ cursor: pointer; }}
        .chord-group path {{ fill-opacity: 0.8; transition: fill-opacity 0.3s ease-in-out; }}
        .chord-path {{ fill-opacity: 0.65; stroke: #fff; stroke-width: 0.5px; cursor: pointer; }}
        .chord-path.selected {{ fill-opacity: 0.9 !important; }}
        .chord-label-group {{ cursor: pointer; }}
        .chord-label-group .leader-line {{ fill: none; stroke: #ddd; stroke-width: 1px; }}
        .chord-label-group text {{ font-size: 12px; font-weight: 400; fill: #333; stroke: #333; stroke-width: 0; transition: stroke-width 0.3s ease-in-out; }}
        .chord-label-group .party-type {{ fill: #6c757d; stroke: #6c757d; }}
        .chord-label-group .underline {{ stroke-width: 2.5px; }}
        .chord-label-group:hover text, .chord-label-group.selected text {{ stroke-width: 0.4px; }}
        
        .node.faded {{ opacity: 0.2; }}
        .link-group.faded {{ opacity: 0.1; }}
        .link-group.highlighted .link {{ stroke: #343a40; stroke-width: 1.5px; }} 
    </style>
</head>
<body>
    <div class="chart-container">
        <div class="tree-container" id="tree-container">
            <svg id="tree-svg"></svg>
        </div>
        <div class="chord-container" id="chord-container">
            <svg id="chord-svg"></svg>
        </div>
        <div class="table-container" id="table-container"></div>
        <div class="sidebar" id="sidebar">
            <div class="view-selector">
                <h3>View</h3>
                <div class="view-selector-options">
                    <label>
                        <input type="radio" name="view-toggle" value="tree" checked>
                        <span>Party Graph</span>
                    </label>
                    <label>
                        <input type="radio" name="view-toggle" value="chord">
                        <span>Chord Diagram</span>
                    </label>
                    <label>
                        <input type="radio" name="view-toggle" value="table">
                        <span>Table</span>
                    </label>
                </div>
            </div>
            <div class="details-panel" id="details-panel">
                <h3>Details</h3>
                <p id="details-text">Select a person or relationship on the map to see more details here.</p>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener("DOMContentLoaded", function() {{
            const data = {json_string_for_html};
            const defaultDetailsText = "Select a person or relationship on the map to see more details here.";

            const colorMap = {{
                'Judiciary': '#6f42c1', 'Prosecution': '#fd7e14', 'Plaintiff': '#fd7e14',
                'Defendant': '#0d6efd', 'Accused': '#0d6efd', 'Victim': '#20c997',
                'Co-offender': '#dc3545', 'Third Party': '#ffc107', 'Insurer': '#6610f2',
                'Intervener': '#17a2b8', 'Legal Representative': '#0dcaf0', 'Legal Firm': '#0dcaf0',
                'Other parties': '#6c757d'
            }};

            const nodes = [];
            const nodeMap = new Map();
            data.levels.forEach(level => {{
                level.parties.forEach(party => {{
                    const nodeData = {{ id: party.name, ...party, level: level.level_number }};
                    nodes.push(nodeData);
                    nodeMap.set(party.name, nodeData);
                }});
            }});
            const links = data.connections.map(d => ({{ 
                source: nodeMap.get(d.source), 
                target: nodeMap.get(d.target), 
                relationship: d.relationship 
            }})).filter(l => l.source && l.target);


            const treeContainer = document.getElementById('tree-container');
            const chordContainer = document.getElementById('chord-container');
            const tableContainer = document.getElementById('table-container');
            const detailsPanel = document.getElementById('details-panel');
            let isTreeRendered = false;
            let isChordRendered = false;
            let isTableRendered = false;

            const viewToggleRadios = document.querySelectorAll('input[name="view-toggle"]');
            viewToggleRadios.forEach(radio => {{
                radio.addEventListener('change', function() {{
                    const targetView = this.value; 

                    const containers = [treeContainer, chordContainer, tableContainer];
                    let containerToShow;

                    containers.forEach(c => {{
                        c.style.display = 'none';
                        c.classList.remove('view-fade-in');
                    }});
                    
                    if (targetView === 'table') {{
                        detailsPanel.classList.add('is-hidden');
                    }} else {{
                        detailsPanel.classList.remove('is-hidden');
                    }}

                    setTimeout(() => {{
                        if (targetView === 'chord') containerToShow = chordContainer;
                        else if (targetView === 'table') containerToShow = tableContainer;
                        else containerToShow = treeContainer;

                        containerToShow.style.display = 'block';
                        containerToShow.classList.add('view-fade-in');
                        
                        if (targetView === 'chord' && !isChordRendered) {{
                            renderChordChart();
                            isChordRendered = true;
                        }} else if (targetView === 'tree' && !isTreeRendered) {{
                            renderChart();
                            isTreeRendered = true;
                        }} else if (targetView === 'table' && !isTableRendered) {{
                            renderTable();
                            isTableRendered = true;
                        }}
                    }}, 10);
                }});
            }});
            
            treeContainer.style.display = 'block';
            treeContainer.classList.add('view-fade-in');
            renderChart();
            isTreeRendered = true;
            
            function findPathToTop(startNode) {{
                const pathNodes = new Set();
                const pathLinks = new Set();
                const orderedPath = []; 

                if (!startNode || startNode.level === 1) {{
                    if(startNode) pathNodes.add(startNode.id);
                    return {{ pathNodes, pathLinks, orderedPath }};
                }}

                const queue = [startNode];
                const visited = new Set([startNode.id]);
                const predecessors = new Map();
                let topNode = null;

                BFS_LOOP:
                while (queue.length > 0) {{
                    const currentNode = queue.shift();

                    for (const link of links) {{
                        let neighbor = null;
                        if (link.source.id === currentNode.id) {{
                            neighbor = link.target;
                        }} else if (link.target.id === currentNode.id) {{
                            neighbor = link.source;
                        }}

                        if (neighbor && !visited.has(neighbor.id)) {{
                            visited.add(neighbor.id);
                            predecessors.set(neighbor.id, {{ parentNode: currentNode, link: link }});
                            queue.push(neighbor);

                            if (neighbor.level === 1) {{
                                topNode = neighbor;
                                break BFS_LOOP;
                            }}
                        }}
                    }}
                }}

                if (topNode) {{
                    let currentInPath = topNode;
                    while (predecessors.has(currentInPath.id)) {{
                        const pred = predecessors.get(currentInPath.id);
                        if (!pred) break;
                        
                        orderedPath.unshift(pred.link);
                        pathLinks.add(pred.link);
                        pathNodes.add(currentInPath.id);

                        currentInPath = pred.parentNode;
                    }}
                    pathNodes.add(startNode.id);
                }}

                return {{ pathNodes, pathLinks, orderedPath }};
            }}
            
            function renderTable() {{
                const allParties = data.levels.flatMap(l => l.parties);
                
                const tableData = allParties.map(party => [
                    party.name,
                    party.type,
                    party.description
                ]);

                if (tableContainer.grid) {{
                    tableContainer.grid.destroy();
                }}

                tableContainer.grid = new gridjs.Grid({{
                    columns: [
                        {{ 
                            name: 'Party Name',
                            formatter: (cell, row) => {{
                                const partyType = row.cells[1].data;
                                const color = colorMap[partyType] || '#ccc';
                                return gridjs.html(`<div style="display: flex; align-items: center;"><span style="height: 10px; width: 10px; background-color: ${{color}}; border-radius: 50%; margin-right: 8px; flex-shrink: 0;"></span>${{cell}}</div>`);
                            }},
                        }},
                        {{ name: 'Party Type' }},
                        {{ name: 'Description' }}
                    ],
                    data: tableData,
                    search: false, 
                    sort: true,
                    pagination: {{ limit: 15 }},
                    style: {{
                        table: {{ 'font-size': '13px' }},
                        th: {{ 'font-weight': '500' }}
                    }}
                }}).render(tableContainer);
            }}

            function renderChordChart() {{
                const svg = d3.select("#chord-svg");
                svg.selectAll("*").remove();
                const containerWidth = document.querySelector('.chord-container').clientWidth;
                if (containerWidth <= 0) return;
                const containerHeight = document.querySelector('.chord-container').clientHeight;
                const outerRadius = Math.min(containerWidth, containerHeight) * 0.5 - 160;

                if (outerRadius < 20) {{
                    svg.append("text")
                        .attr("x", containerWidth / 2)
                        .attr("y", containerHeight / 2)
                        .attr("text-anchor", "middle")
                        .attr("font-family", "Poppins, sans-serif")
                        .attr("font-size", "14px")
                        .attr("fill", "#888")
                        .text("Screen too small to display Chord Diagram.");
                    return;
                }}
                
                const innerRadius = outerRadius - 20;
                svg.attr("width", containerWidth).attr("height", containerHeight);
                const g = svg.append("g").attr("transform", "translate(" + containerWidth / 2 + "," + containerHeight / 2 + ")");
                
                svg.on("click", () => {{
                    g.selectAll(".selected").classed("selected", false);
                    unhighlightAll();
                    updateDetails(defaultDetailsText);
                }});

                const parties = data.levels.flatMap(l => l.parties);
                const nameToIndex = new Map(parties.map((p, i) => [p.name, i]));
                const indexToParty = new Map(parties.map((p, i) => [i, p]));
                
                const matrix = Array.from({{length: parties.length}}, () => Array(parties.length).fill(0));
                const relationshipMap = new Map();
                
                const connectedPartyNames = new Set();
                data.connections.forEach(conn => {{
                    connectedPartyNames.add(conn.source);
                    connectedPartyNames.add(conn.target);

                    const sourceIndex = nameToIndex.get(conn.source);
                    const targetIndex = nameToIndex.get(conn.target);
                    if (sourceIndex !== undefined && targetIndex !== undefined) {{
                        matrix[sourceIndex][targetIndex] += 1;
                        const key = `${{sourceIndex}}-${{targetIndex}}`;
                        if (!relationshipMap.has(key)) {{
                            relationshipMap.set(key, conn.relationship);
                        }}
                    }}
                }});

                const chord = d3.chordDirected().padAngle(0.05).sortSubgroups(d3.descending).sortChords(d3.descending);
                const chords = chord(matrix);

                function showNodeDetailsInChordView(d) {{
                    const selectedNode = nodeMap.get(d.name);
                    const {{ orderedPath }} = findPathToTop(selectedNode);
                    
                    let detailsHtml = `<p><strong style="color: black;">${{d.name}}</strong></p><p style="margin: 8px 0 0 0;">${{d.description}}</p>`;
                    if (orderedPath && orderedPath.length > 0) {{
                        detailsHtml += `<p style="margin-top: 15px; margin-bottom: 8px; border-top: 1px solid #eee; padding-top: 10px;"><strong style="color: black;">Relationship Path</strong></p>`;
                        detailsHtml += `<ul style="font-size: 12px; padding-left: 0; margin: 0; list-style-type: none;">`;
                        orderedPath.forEach(link => {{
                            detailsHtml += `<li style="margin-bottom: 5px;">${{link.source.name}} <strong style="color: black;">&rarr;</strong> <span style="color: black;">${{link.relationship}}</span> <strong style="color: black;">&rarr;</strong> ${{link.target.name}}</li>`;
                        }});
                        detailsHtml += `</ul>`;
                    }}
                    updateDetails(detailsHtml);
                }}

                function highlightParentPath(d) {{
                    const selectedNode = nodeMap.get(d.name);
                    const {{ pathNodes }} = findPathToTop(selectedNode);
                    pathNodes.add(selectedNode.id);

                    const indicesToHighlight = new Set([...pathNodes].map(name => nameToIndex.get(name)));

                    g.selectAll('.chord-group').classed('faded', gd => !indicesToHighlight.has(gd.index));
                    g.selectAll('.chord-label-group').classed('faded', ld => !indicesToHighlight.has(ld.index));
                    
                    g.selectAll('.chord-path').classed('faded', c =>
                        !(indicesToHighlight.has(c.source.index) && indicesToHighlight.has(c.target.index))
                    );

                    g.selectAll(".chord-path:not(.faded)").style("fill-opacity", 0.9);
                    g.selectAll(".chord-group:not(.faded) path").style("fill-opacity", 1.0);
                }}

                function highlightPathAndParties(element, d) {{
                    g.selectAll(".chord-group, .chord-path, .chord-label-group").classed("faded", true);
                    
                    d3.select(element).classed("faded", false).style("fill-opacity", 0.9);
                    g.selectAll(".chord-group").filter(gd => gd.index === d.source.index || gd.index === d.target.index).classed("faded", false);
                    g.selectAll(".chord-label-group").filter(ld => ld.index === d.source.index || ld.index === d.target.index).classed("faded", false);

                    const sourceParty = indexToParty.get(d.source.index);
                    const targetParty = indexToParty.get(d.target.index);
                    const key = `${{d.source.index}}-${{d.target.index}}`;
                    const relationshipText = relationshipMap.get(key) || 'related to';

                    const detailsHtml = `<span>${{sourceParty.name}}</span> <strong style="color: black;">&rarr;</strong> <span style="color: black;">${{relationshipText}}</span> <strong style="color: black;">&rarr;</strong> <span>${{targetParty.name}}</span>`;
                    updateDetails(detailsHtml);
                }}

                function unhighlightAll() {{
                    g.selectAll(".faded").classed("faded", false);
                    g.selectAll(".chord-path").style("fill-opacity", 0.65);
                    g.selectAll(".chord-group path").style("fill-opacity", 0.8);
                }}
                
                const group = g.append("g").selectAll("g").data(chords.groups).join("g").attr("class", "chord-group")
                    .on("mouseover", (event, d) => {{
                        if (!g.select(".selected").node()) {{
                            const partyData = indexToParty.get(d.index);
                            highlightParentPath(partyData);
                            showNodeDetailsInChordView(partyData);
                        }}
                    }})
                    .on("mouseout", function() {{
                        if (!g.select(".selected").node()) {{
                            unhighlightAll();
                            updateDetails(defaultDetailsText);
                        }}
                    }})
                    .on("click", (event, d) => {{
                        event.stopPropagation();
                        const labelNode = g.selectAll(".chord-label-group").filter(ld => ld.index === d.index).node();
                        if (labelNode) {{
                            d3.select(labelNode).dispatch("click");
                        }}
                    }});

                group.append("path")
                    .attr("fill", d => colorMap[indexToParty.get(d.index).type] || '#ccc')
                    .attr("stroke", d => d3.rgb(colorMap[indexToParty.get(d.index).type] || '#ccc').darker())
                    .attr("d", d3.arc()({{innerRadius: innerRadius, outerRadius: outerRadius}}));
                
                g.append("g").selectAll("path").data(chords).join("path")
                    .attr("class", "chord-path")
                    .attr("d", d3.ribbonArrow().radius(innerRadius - 1))
                    .attr("fill", d => colorMap[indexToParty.get(d.source.index).type] || '#ccc')
                    .attr("stroke", d => d3.rgb(colorMap[indexToParty.get(d.source.index).type] || '#ccc').darker())
                    .on("mouseover", function(event, d) {{
                        if (!g.select(".selected").node()) {{
                            highlightPathAndParties(this, d);
                        }}
                    }})
                    .on("mouseout", function() {{
                        if (!g.select(".selected").node()) {{
                            unhighlightAll();
                            updateDetails(defaultDetailsText);
                        }}
                    }})
                    .on("click", function(event, d) {{
                        event.stopPropagation();
                        const isAlreadySelected = d3.select(this).classed("selected");
                        g.selectAll(".selected").classed("selected", false);
                        if (!isAlreadySelected) {{
                            d3.select(this).classed("selected", true);
                            highlightPathAndParties(this, d);
                        }} else {{
                            unhighlightAll();
                            updateDetails(defaultDetailsText);
                        }}
                    }});

                const labelData = chords.groups.map(d => {{
                    const party = indexToParty.get(d.index);
                    const midAngle = (d.startAngle + d.endAngle) / 2;
                    return {{
                        angle: midAngle,
                        name: party.name,
                        type: party.type,
                        color: colorMap[party.type] || '#ccc',
                        description: party.description,
                        index: d.index
                    }};
                }});

                const labelHeight = 16;
                const rightLabels = labelData.filter(l => l.angle < Math.PI).sort((a,b) => a.angle - b.angle);
                const leftLabels = labelData.filter(l => l.angle >= Math.PI).sort((a,b) => b.angle - a.angle);
                
                let lastYRight = -Infinity;
                rightLabels.forEach(label => {{
                    const angle = label.angle - Math.PI / 2;
                    let y = (outerRadius + 30) * Math.sin(angle);
                    if (y < lastYRight + labelHeight) y = lastYRight + labelHeight;
                    lastYRight = y;
                    label.finalY = y;
                }});
                
                let lastYLeft = -Infinity;
                leftLabels.forEach(label => {{
                    const angle = label.angle - Math.PI / 2;
                    let y = (outerRadius + 30) * Math.sin(angle);
                    if (y < lastYLeft + labelHeight) y = lastYLeft + labelHeight;
                    lastYLeft = y;
                    label.finalY = y;
                }});

                const labelGroup = g.append("g").selectAll("g").data(labelData).join("g")
                    .attr("class", d => "chord-label-group " + (d.angle < Math.PI ? "label-on-right" : "label-on-left"))
                    .on("mouseover", function(event, d) {{
                         if (!g.select(".selected").node()) {{
                            highlightParentPath(d);
                            showNodeDetailsInChordView(d);
                        }}
                    }})
                    .on("mouseout", function() {{
                        if (!g.select(".selected").node()) {{
                            unhighlightAll();
                            updateDetails(defaultDetailsText);
                        }}
                    }})
                    .on("click", function(event, d) {{
                        event.stopPropagation();
                        const group = d3.select(this);
                        const isAlreadySelected = group.classed("selected");
                        
                        g.selectAll(".selected").classed("selected", false);

                        if (!isAlreadySelected) {{
                            group.classed("selected", true);
                            highlightParentPath(d);
                            showNodeDetailsInChordView(d);
                        }} else {{
                            unhighlightAll();
                            updateDetails(defaultDetailsText);
                        }}
                    }});

                labelGroup.each(function(d) {{
                    if (connectedPartyNames.has(d.name)) {{
                        const pathGenerator = (labelData) => {{
                            const angle = labelData.angle - Math.PI / 2;
                            const onRightSide = labelData.angle < Math.PI;
                            const startX = innerRadius * Math.cos(angle);
                            const startY = innerRadius * Math.sin(angle);
                            const elbowX = (outerRadius + 40) * (onRightSide ? 1 : -1);
                            return `M${{startX}},${{startY}}C${{elbowX}},${{startY}} ${{elbowX}},${{labelData.finalY}} ${{elbowX}},${{labelData.finalY}}`;
                        }};

                        d3.select(this).append("path")
                            .attr("class", "leader-line")
                            .attr("d", pathGenerator(d));
                    }}
                }});

                const textLabels = labelGroup.append("text")
                    .attr("transform", d => {{
                        const onRightSide = d.angle < Math.PI;
                        const x = (outerRadius + 45) * (onRightSide ? 1 : -1);
                        return "translate(" + x + "," + d.finalY + ")";
                    }})
                    .attr("dy", "0.35em")
                    .attr("text-anchor", d => d.angle < Math.PI ? "start" : "end");
                
                textLabels.append("tspan").text(d => d.name);
                textLabels.append("tspan").attr("class", "party-type").attr("dx", " 5").text(d => " (" + d.type + ")");
                
                labelGroup.each(function(d) {{
                    const group = d3.select(this);
                    const textNode = group.select("text").node();
                    if (!textNode) return;
                    
                    const bbox = textNode.getBBox();
                    group.append("line")
                        .attr("class", "underline")
                        .attr("stroke", d.color)
                        .attr("transform", group.select("text").attr("transform"))
                        .attr("x1", bbox.x)
                        .attr("x2", bbox.x + bbox.width)
                        .attr("y1", bbox.y + bbox.height + 1)
                        .attr("y2", bbox.y + bbox.height + 1);
                }});
            }}

            function renderChart() {{
                const svg = d3.select("#tree-svg");
                svg.selectAll("*").remove();
                const width = document.querySelector('.tree-container').clientWidth;
                if (width <= 0) {{ setTimeout(renderChart, 100); return; }}
                svg.attr("width", width);
                const defs = svg.append('defs');

                defs.append('marker').attr('id', 'arrowhead').attr('viewBox', '-0 -5 10 10')
                    .attr('refX', 10).attr('refY', 0).attr('orient', 'auto')
                    .attr('markerWidth', 6).attr('markerHeight', 6)
                    .append('svg:path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#ccc');
                defs.append('marker').attr('id', 'arrowhead-hover').attr('viewBox', '-0 -5 10 10')
                    .attr('refX', 10).attr('refY', 0).attr('orient', 'auto')
                    .attr('markerWidth', 6).attr('markerHeight', 6)
                    .append('svg:path').attr('d', 'M0,-5L10,0L0,5').attr('fill', '#343a40');

                /* --- ZOOM & PAN IMPLEMENTATION --- */
                /* Create a group element to contain all chart elements that will be zoomed. */
                const g = svg.append("g");
                /* Define the zoom behavior. The 'on("zoom")' function is called whenever a zoom/pan event occurs. */
                const zoom = d3.zoom().on("zoom", (event) => {{
                    /* 'event.transform' contains the current zoom level and pan offsets. */
                    /* We apply this transformation to our group element 'g'. */
                    g.attr("transform", event.transform);
                }});
                
                /* Apply the zoom behavior to the main SVG. This enables:
                 * - Zooming with the mouse scroll wheel or trackpad pinch gesture.
                 * - Panning by clicking and dragging the chart's background.
                */
                svg.call(zoom);
                /* --- END ZOOM & PAN --- */
                
                svg.on("click", (event) => {{
                    if (event.target === svg.node()) {{
                        unhighlightAllTreeElements();
                        updateDetails(defaultDetailsText);
                    }}
                }});

                const levelInfo = new Map();
                const nodeWidth = 70, nodeHeight = 28;
                const nodesPerRow = Math.floor(width / (nodeWidth + 25));

                const initialVerticalOffset = 30; 
                let yPos = initialVerticalOffset; 

                data.levels.forEach(level => {{
                    const numRows = Math.ceil(level.parties.length / nodesPerRow);
                    const levelHeight = Math.max(120, numRows * (nodeHeight + 50));
                    levelInfo.set(level.level_number, {{ y: yPos, height: levelHeight }});
                    yPos += levelHeight; 
                }});
                
                svg.attr("height", yPos + initialVerticalOffset); 

                nodes.forEach(node => {{
                    const levelData = levelInfo.get(node.level);
                    if (levelData) node.y = levelData.y + levelData.height / 2;
                }});

                const simulation = d3.forceSimulation(nodes)
                    .force("link", d3.forceLink(links).id(d => d.id).distance(100).strength(0.7))
                    .force("charge", d3.forceManyBody().strength(-200))
                    .force("collide", d3.forceCollide().radius(nodeWidth / 2 + 10).iterations(2))
                    .force("x", d3.forceX(width / 2).strength(0.08))
                    .force("y", d3.forceY(d => {{
                        const levelData = levelInfo.get(d.level);
                        return levelData ? levelData.y + levelData.height / 2 : d.y;
                    }}).strength(0.5));
                
                levelInfo.forEach((info, levelNumber) => {{
                    const levelData = data.levels.find(l => l.level_number === levelNumber);
                    g.append("line").attr("x1", 50).attr("x2", width - 50).attr("y1", info.y).attr("y2", info.y).attr("class", "level-line");
                    g.append("text").attr("x", 50).attr("y", info.y + 15).attr("class", "level-label").text(levelData.level_description);
                }});

                function showNodeDetails(d) {{
                    const {{ orderedPath }} = findPathToTop(d);
                    let detailsHtml = `<p><strong style="color: black;">${{d.name}}</strong></p><p style="margin: 8px 0 0 0;">${{d.description}}</p>`;

                    if (orderedPath && orderedPath.length > 0) {{
                        detailsHtml += `<p style="margin-top: 15px; margin-bottom: 8px; border-top: 1px solid #eee; padding-top: 10px;"><strong style="color: black;">Relationship Path</strong></p>`;
                        detailsHtml += `<ul style="font-size: 12px; padding-left: 0; margin: 0; list-style-type: none;">`;
                        orderedPath.forEach(link => {{
                            detailsHtml += `<li style="margin-bottom: 5px;">${{link.source.name}} <strong style="color: black;">&rarr;</strong> <span style="color: black;">${{link.relationship}}</span> <strong style="color: black;">&rarr;</strong> ${{link.target.name}}</li>`;
                        }});
                        detailsHtml += `</ul>`;
                    }}
                    updateDetails(detailsHtml);
                }}

                const linkGroup = g.append("g").selectAll("g").data(links).join("g").attr("class", "link-group")
                    .on("mouseover", function(event, d) {{
                        d3.select(this).raise().select('.link').attr('marker-end', 'url(#arrowhead-hover)');
                        updateDetails('<span>' + d.source.id + '</span> <strong style="color: black;">' + d.relationship + '</strong> <span>' + d.target.id + '</span>');
                    }})
                    .on("mouseout", function() {{
                        if (!d3.select(this).classed("highlighted")) {{
                            d3.select(this).select('.link').attr('marker-end', 'url(#arrowhead)');
                        }}
                        const selectedNode = g.select(".node.highlighted");
                        if (selectedNode.node()) {{
                            showNodeDetails(selectedNode.datum());
                        }} else {{
                            updateDetails(defaultDetailsText);
                        }}
                    }});
                
                const getPath = d => {{
                    if (!d.source || !d.target) return "";
                    const startPoint = getIntersectionPoint(d.source, d.target, nodeWidth, nodeHeight);
                    const endPoint = getIntersectionPoint(d.target, d.source, nodeWidth, nodeHeight);
                    const dx = endPoint.x - startPoint.x;
                    const dy = endPoint.y - startPoint.y;
                    const length = Math.sqrt(dx * dx + dy * dy);
                    if (length < 2) {{
                        return `M${{startPoint.x}},${{startPoint.y}}L${{endPoint.x}},${{endPoint.y}}`;
                    }}
                    const perpDx = -dy / length;
                    const perpDy = dx / length;
                    const bend = length * 0.15;
                    const cp1x = startPoint.x + dx * 0.33 + bend * perpDx;
                    const cp1y = startPoint.y + dy * 0.33 + bend * perpDy;
                    const cp2x = startPoint.x + dx * 0.66 - bend * perpDx;
                    const cp2y = startPoint.y + dy * 0.66 - bend * perpDy;
                    return `M${{startPoint.x}},${{startPoint.y}} C ${{cp1x}},${{cp1y}} ${{cp2x}},${{cp2y}} ${{endPoint.x}},${{endPoint.y}}`;
                }};

                linkGroup.append("path").attr("class", "link-hitbox").attr("d", getPath);
                const visibleLink = linkGroup.append("path").attr("class", "link").attr('marker-end','url(#arrowhead)').attr("d", getPath);
                
                const node = g.append("g").selectAll("g").data(nodes).join("g").attr("class", "node")
                    .call(drag(simulation))
                    .on("click", function(event, d) {{
                        event.stopPropagation();
                        const isAlreadySelected = d3.select(this).classed("highlighted");

                        unhighlightAllTreeElements(); 

                        if (!isAlreadySelected) {{
                            highlightNodeAndConnections(d);
                            showNodeDetails(d);
                        }} else {{
                            updateDetails(defaultDetailsText);
                        }}
                    }});

                node.append("rect").attr("x", -nodeWidth / 2).attr("y", -nodeHeight / 2).attr("width", nodeWidth).attr("height", nodeHeight).attr("rx", 15).attr("ry", 15).attr("fill", d => colorMap[d.type] || colorMap['Other parties']);
                node.append("text").attr("class", "type-label").attr("dy", "0.3em").text(d => d.type.substring(0, 1));
                const nameLabel = node.append("text").attr("y", nodeHeight / 2 + 3).attr("dy", "0.5em").text(d => d.name);

                simulation.on("tick", () => {{
                    nodes.forEach(d => {{
                        const levelData = levelInfo.get(d.level);
                        if (levelData) {{
                            const minX = nodeWidth / 2;
                            const maxX = width - nodeWidth / 2;
                            const minY = levelData.y + nodeHeight / 2 + 10;
                            const maxY = levelData.y + levelData.height - nodeHeight / 2 - 10;

                            d.x = Math.max(minX, Math.min(maxX, d.x));
                            d.y = Math.max(minY, Math.min(maxY, d.y));
                        }}
                    }});
                    visibleLink.attr("d", getPath);
                    linkGroup.selectAll(".link-hitbox").attr("d", getPath);
                    node.attr("transform", d => "translate(" + d.x + "," + d.y + ")");
                    nameLabel.call(wrap, nodeWidth - 5);
                }});

                function highlightNodeAndConnections(selectedNodeData) {{
                    const immediateNodes = new Set([selectedNodeData.id]);
                    const immediateLinks = new Set();
                    links.forEach(link => {{
                        if (link.source.id === selectedNodeData.id || link.target.id === selectedNodeData.id) {{
                            immediateLinks.add(link);
                            immediateNodes.add(link.source.id);
                            immediateNodes.add(link.target.id);
                        }}
                    }});

                    const {{ pathNodes, pathLinks }} = findPathToTop(selectedNodeData);
                    const allNodesToHighlight = new Set([...immediateNodes, ...pathNodes]);
                    const allLinksToHighlight = new Set([...immediateLinks, ...pathLinks]);

                    g.selectAll(".node")
                        .classed("faded", d => !allNodesToHighlight.has(d.id))
                        .classed("highlighted", d => d.id === selectedNodeData.id);

                    g.selectAll(".link-group")
                        .classed("faded", d => !allLinksToHighlight.has(d))
                        .classed("highlighted", d => allLinksToHighlight.has(d));

                    g.selectAll(".link-group.highlighted .link")
                        .attr('marker-end', 'url(#arrowhead-hover)');
                }}

                function unhighlightAllTreeElements() {{
                    g.selectAll(".node").classed("faded", false).classed("highlighted", false);
                    g.selectAll(".link-group").classed("faded", false).classed("highlighted", false);
                    g.selectAll(".link-group .link").attr('marker-end', 'url(#arrowhead)');
                }}
            }}

            function updateDetails(htmlContent) {{
                const detailsPanel = document.getElementById('details-panel');
                const detailsText = document.getElementById('details-text');
                detailsPanel.style.maxHeight = detailsPanel.scrollHeight + 'px';
                detailsText.style.opacity = 0;
                setTimeout(() => {{
                    detailsText.innerHTML = htmlContent;
                    detailsPanel.style.maxHeight = detailsPanel.scrollHeight + 'px';
                    detailsText.style.opacity = 1;
                }}, 200);
            }}

            function debounce(func, wait) {{
                let timeout;
                return function executedFunction(...args) {{
                    const later = () => {{ clearTimeout(timeout); func(...args); }};
                    clearTimeout(timeout);
                    timeout = setTimeout(later, wait);
                }};
            }}
            
            /* --- RESPONSIVENESS IMPLEMENTATION --- */
            /* Listen for the window to be resized. */
            window.addEventListener('resize', debounce(() => {{
                /* Check which visualization is currently active. */
                const isTreeVisible = document.querySelector('input[name="view-toggle"][value="tree"]').checked;
                const isChordVisible = document.querySelector('input[name="view-toggle"][value="chord"]').checked;
                const isTableVisible = document.querySelector('input[name="view-toggle"][value="table"]').checked;

                /* Re-render the active visualization to fit the new screen size.
                 * This makes the chart "autoadjustable". */
                if (isTreeVisible) renderChart();
                else if (isChordVisible) renderChordChart();
                else if (isTableVisible) renderTable();
            }}, 250)); /* Debounce prevents the function from firing too rapidly, improving performance. */
            /* --- END RESPONSIVENESS --- */
            
            function getIntersectionPoint(source, target, nodeWidth, nodeHeight) {{
                const sx = source.x;
                const sy = source.y;
                const tx = target.x;
                const ty = target.y;

                const dx = tx - sx;
                const dy = ty - sy;

                const halfWidth = nodeWidth / 2;
                const halfHeight = nodeHeight / 2;

                let t = Infinity;

                if (dy !== 0) {{
                    const t_y_top = (ty - halfHeight - sy) / dy;
                    if (t_y_top >= 0 && t_y_top <= 1) {{
                        const intersectX = sx + t_y_top * dx;
                        if (intersectX >= tx - halfWidth && intersectX <= tx + halfWidth) {{
                            t = Math.min(t, t_y_top);
                        }}
                    }}
                }}
                if (dy !== 0) {{
                    const t_y_bottom = (ty + halfHeight - sy) / dy;
                    if (t_y_bottom >= 0 && t_y_bottom <= 1) {{
                        const intersectX = sx + t_y_bottom * dx;
                        if (intersectX >= tx - halfWidth && intersectX <= tx + halfWidth) {{
                            t = Math.min(t, t_y_bottom);
                        }}
                    }}
                }}
                if (dx !== 0) {{
                    const t_x_left = (tx - halfWidth - sx) / dx;
                    if (t_x_left >= 0 && t_x_left <= 1) {{
                        const intersectY = sy + t_x_left * dy;
                        if (intersectY >= ty - halfHeight && intersectY <= ty + halfHeight) {{
                            t = Math.min(t, t_x_left);
                        }}
                    }}
                }}
                if (dx !== 0) {{
                    const t_x_right = (tx + halfWidth - sx) / dx;
                    if (t_x_right >= 0 && t_x_right <= 1) {{
                        const intersectY = sy + t_x_right * dy;
                        if (intersectY >= ty - halfHeight && intersectY <= ty + halfHeight) {{
                            t = Math.min(t, t_x_right);
                        }}
                    }}
                }}

                if (t === Infinity) {{
                    const angle = Math.atan2(dy, dx);
                    return {{
                        x: tx - halfWidth * Math.cos(angle) * 1.1, 
                        y: ty - halfHeight * Math.sin(angle) * 1.1
                    }};
                }}

                return {{
                    x: sx + t * dx,
                    y: sy + t * dy
                }};
            }}
            
            function drag(simulation) {{
                function dragstarted(event, d) {{
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    d.fx = d.x;
                    d.fy = d.y;
                }}
                function dragged(event, d) {{
                    d.fx = event.x;
                    d.fy = event.y;
                }}
                function dragended(event, d) {{
                    if (!event.active) simulation.alphaTarget(0);
                    d.fx = null;
                    d.fy = null;
                }}
                return d3.drag()
                    .on("start", dragstarted)
                    .on("drag", dragged)
                    .on("end", dragended);
            }}

            function wrap(text, width) {{
                text.each(function() {{
                    var text = d3.select(this), words = text.text().split(/\\s+/).reverse(), word, line = [],
                        lineNumber = 0, lineHeight = 1.1, y = text.attr("y"), dy = parseFloat(text.attr("dy")),
                        tspan = text.text(null).append("tspan").attr("x", 0).attr("y", y).attr("dy", dy + "em");
                    while (word = words.pop()) {{
                        line.push(word);
                        tspan.text(line.join(" "));
                        if (tspan.node().getComputedTextLength() > width) {{
                            line.pop();
                            tspan.text(line.join(" "));
                            line = [word];
                            tspan = text.append("tspan").attr("x", 0).attr("y", y).attr("dy", ++lineNumber * lineHeight + dy + "em").text(word);
                        }}
                    }}
                }});
            }}
        }});
    </script>
</body>
</html>
        """
        return html_template