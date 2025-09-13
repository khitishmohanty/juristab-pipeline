import json

class HtmlGenerator:
    """
    Generates an interactive HTML visualization from a legal case summary JSON object,
    featuring a standard navigation menu.
    """

    def generate_html_tree(self, json_data: dict) -> str:
        """
        Takes a dictionary parsed from a case summary JSON and returns a complete
        HTML string for the interactive visualization.

        Args:
            json_data (dict): The case data parsed from a JSON file.

        Returns:
            str: A self-contained HTML document as a string.
        """
        # Convert the Python dictionary to a JSON string to be safely embedded in the HTML script tag.
        json_string_for_html = json.dumps(json_data)

        html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Case Law Visualization</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Poppins', sans-serif;
            background-color: #ffffff;
            font-size: 14px;
        }}
        .nav-item {{
            position: relative; /* Required for absolute positioning of child spans */
            cursor: pointer;
            padding: 0.25rem 1rem; /* Reduced vertical padding to bring items closer */
            border-radius: 0.5rem;
            font-size: 14px;
        }}
        /* This span holds the visible text, which fades out */
        .nav-item .text-normal {{
            font-weight: 500;
            color: #374151; /* Gray-700 */
            transition: opacity 0.3s ease-in-out;
        }}
        /* This span holds the bold text, which fades in */
        .nav-item .text-bold {{
            position: absolute;
            top: 0.25rem;  /* Adjusted to match parent's new top padding */
            left: 1rem;   /* Must match parent's left padding */
            font-weight: 600;
            color: #111827; /* Dark Gray-900 */
            opacity: 0; /* Hidden by default */
            transition: opacity 0.3s ease-in-out;
        }}
        /* --- The Animation --- */
        /* Fade out the normal text on hover/active */
        .nav-item:hover .text-normal,
        .nav-item.active .text-normal {{
            opacity: 0;
        }}
        /* Fade in the bold text on hover/active */
        .nav-item:hover .text-bold,
        .nav-item.active .text-bold {{
            opacity: 1;
        }}
        .content-card {{
            display: none;
            background-color: #ffffff;
            border-radius: 0.75rem;
            animation: fadeIn 0.5s ease-out;
            box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1);
        }}
        .content-card.active {{
            display: block;
        }}
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        .card-title {{
            font-family: 'Poppins', sans-serif;
            font-weight: 500;
        }}
    </style>
</head>
<body class="p-4 md:p-8">

    <div class="w-full max-w-5xl mx-auto flex flex-col md:flex-row gap-8">
        <div class="w-full md:w-1/3 lg:w-1/4">
            <nav class="bg-white p-3 rounded-xl">
                <ul id="nav-menu">
                    </ul>
            </nav>
        </div>

        <div id="content-area" class="w-full md:w-2/3 lg:w-3/4">
            </div>
    </div>

    <script>
    const caseData = {json_string_for_html};

    document.addEventListener('DOMContentLoaded', () => {{
        const navMenu = document.getElementById('nav-menu');
        const contentArea = document.getElementById('content-area');

        // Populate navigation and content cards
        caseData.cards.forEach((card, index) => {{
            const navItem = document.createElement('li');
            navItem.className = 'nav-item';
            navItem.dataset.index = index;

            // Create two spans inside the li for the smooth bold transition
            navItem.innerHTML = `
                <span class="text-normal">${{card.menuLabel}}</span>
                <span class="text-bold">${{card.menuLabel}}</span>
            `;
            navMenu.appendChild(navItem);

            // Create Content Card
            const contentCard = document.createElement('div');
            contentCard.className = 'content-card';
            contentCard.dataset.index = index;
            contentCard.innerHTML = generateCardHTML(card);
            contentArea.appendChild(contentCard);
        }});

        const navItems = document.querySelectorAll('.nav-item');
        const contentCards = document.querySelectorAll('.content-card');

        // Function to switch active view
        function switchView(index) {{
            navItems.forEach(item => item.classList.remove('active'));
            contentCards.forEach(card => card.classList.remove('active'));

            if (navItems[index]) {{
                navItems[index].classList.add('active');
            }}
            if (contentCards[index]) {{
                contentCards[index].classList.add('active');
            }}
        }}

        // Add click event listeners to navigation items
        navItems.forEach((item, index) => {{
            item.addEventListener('click', () => switchView(index));
        }});

        // Activate the first item by default
        if (navItems.length > 0) {{
            switchView(0);
        }}
    }});

    // This function generates the inner HTML for each content card on the right
    function generateCardHTML(card) {{
        let contentHTML = '';
        switch (card.type) {{
            case 'overview':
                const listItems = card.content.map(item => `
                    <li class="flex items-start text-sm py-1">
                        <span class="font-semibold w-28 shrink-0">${{item.label}}:</span> 
                        <span class="text-gray-600">${{item.value}}</span>
                    </li>`).join('');
                const highlightColor = card.highlight.status === 'negative' ? 'red' : 'green';
                contentHTML = `
                    <ul class="space-y-2 text-gray-800">${{listItems}}</ul>
                    <div class="bg-${{highlightColor}}-50 p-4 border-t border-${{highlightColor}}-200 mt-4 rounded-b-lg">
                         <p class="text-sm font-semibold text-${{highlightColor}}-800 text-center">${{card.highlight.text}}</p>
                    </div>`;
                break;
            case 'list':
                const listContent = card.content.map(item => {{
                    const itemClass = item.highlight ? 'font-semibold text-gray-800 bg-gray-100 p-3 rounded-md' : '';
                    return `<li class="${{itemClass}}">${{item.text}}</li>`;
                }}).join('');
                contentHTML = `<ul class="space-y-3 text-gray-600 list-disc list-inside text-sm">${{listContent}}</ul>`;
                break;
            case 'timeline':
                 const timelineItems = card.content.map(item => `
                    <div class="relative pl-8 pb-6 last:pb-0">
                        <div class="absolute left-0 top-1.5 h-full w-px bg-gray-200"></div>
                        <div class="absolute left-[-4px] top-1 h-2 w-2 rounded-full ${{item.highlight ? 'bg-blue-600' : 'bg-gray-400'}}"></div>
                        <p class="font-semibold text-sm ${{item.highlight ? 'text-blue-700' : 'text-gray-800'}}">${{item.date}}</p>
                        <p class="text-xs text-gray-500">${{item.event || item.description}}</p>
                    </div>`).join('');
                contentHTML = `<div>${{timelineItems}}</div>`;
                break;
            case 'comparison':
                const sideA = card.content.sideA;
                const sideB = card.content.sideB;
                contentHTML = `
                    <div class="grid md:grid-cols-2 gap-6">
                        <div>
                            <h3 class="font-semibold text-blue-800 text-sm">${{sideA.title}}</h3>
                            <p class="mt-1 text-gray-600 text-xs">${{sideA.text}}</p>
                        </div>
                        <div>
                            <h3 class="font-semibold text-blue-800 text-sm">${{sideB.title}}</h3>
                            <p class="mt-1 text-gray-600 text-xs">${{sideB.text}}</p>
                        </div>
                    </div>`;
                break;
            case 'principle':
                const bodyItems = card.content.body.map(p => `<p class="mt-2 text-gray-600">${{p}}</p>`).join('');
                contentHTML = `
                    <div class="text-sm">
                        <h3 class="font-semibold text-base text-gray-900">${{card.content.title}}</h3>
                        ${{bodyItems}}
                    </div>`;
                break;
            case 'scorecard':
                const scorecardItems = card.content.map(item => `
                    <li class="flex items-start text-gray-700">
                        <div class="w-2 h-2 ${{item.status === 'negative' ? 'bg-red-500' : 'bg-green-500'}} rounded-full mt-1.5 mr-3 flex-shrink-0"></div>
                        <span>${{item.text}}</span>
                    </li>`).join('');
                contentHTML = `<ul class="space-y-3">${{scorecardItems}}</ul>`;
                break;
            case 'text':
                contentHTML = `<div class="text-gray-600 space-y-3 text-sm leading-relaxed">${{card.content}}</div>`;
                break;
        }}

        return `
            <div class="p-6">
                <h2 class="card-title text-xl font-medium text-gray-800">${{card.title}}</h2>
                <div class="mt-4">${{contentHTML}}</div>
            </div>
        `;
    }}
    </script>
</body>
</html>
        """
        return html_template