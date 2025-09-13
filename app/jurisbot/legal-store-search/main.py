import dash
from dash import dcc, html, Input, Output, State, ALL, no_update
import dash_bootstrap_components as dbc
import json
import re

from dotenv import load_dotenv
load_dotenv()

# Assuming 'utils' and 'src' are in the same directory or accessible in the Python path
from utils.auth import get_gcp_token
from src.search_client import perform_search, get_suggestions
from utils.s3_client import load_s3_config, fetch_document_from_s3

# --- CONFIGURATION ---
S3_CONFIG = load_s3_config()
POPPINS_FONT = "https://fonts.googleapis.com/css2?family=Poppins:wght@400;500&display=swap"
FONT_AWESOME = "https://use.fontawesome.com/releases/v5.15.4/css/all.css"

# --- APP INITIALIZATION ---
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.LUX, POPPINS_FONT, FONT_AWESOME],
    suppress_callback_exceptions=True,
    title='JurisTab Legal Store',
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ]
)
server = app.server

# --- APP LAYOUT ---
app.layout = dbc.Container([
    dcc.Store(id='selected-doc-store', data=None),
    dcc.Store(id='search-results-store', data=None),

    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Searching...")),
            dbc.ModalBody(
                html.Div([
                    dbc.Spinner(size="lg", color="primary", spinner_style={"width": "3rem", "height": "3rem"}),
                    html.Div("Processing your query and generating a summary. Please wait.", className="mt-2")
                ], className="text-center")
            ),
        ],
        id="loading-modal", is_open=False, centered=True, backdrop="static", keyboard=False,
    ),

    dbc.Row(
        [
            dbc.Col(
                html.Img(
                    src=app.get_asset_url('logo.png'),
                    height="35px",
                    style={'position': 'relative', 'top': '-2px'}
                ),
                width="auto"
            ),
            dbc.Col(html.Span("|", style={'fontSize': '2rem', 'color': '#dee2e6'}), width="auto", className="px-3"),
            dbc.Col(
                html.H1(
                    "Legal Store",
                    style={'textTransform': 'none', 'margin': '0', 'fontSize': '1.75rem', 'color': '#6c757d'}
                ),
                width="auto"
            ),
        ],
        className="", # <-- CHANGE THIS LINE from "mb-4"
        align="center",
    ),

    dbc.Row([
        dbc.Col(
            html.Div([
                dcc.Input(
                    id='search-input', type='text',
                    placeholder='e.g., cases handled by Mills Oakley',
                    style={'borderRadius': '30px', 'border': '1px solid #ced4da', 'height': '40px'},
                    className='form-control me-2', n_submit=0,
                    autoComplete='off'
                ),
                html.Div(id='suggestions-output', style={'position': 'absolute', 'zIndex': '1050', 'width': '100%'})
            ], style={'position': 'relative'}),
            width=12, lg=8
        ),
        dbc.Col(
            html.Div(
                html.Button('Search', id='search-button', n_clicks=0,
                    style={
                        'backgroundColor': '#003366', 'color': 'white',
                        'borderRadius': '30px', 'border': 'none',
                        'padding': '0 30px', 'height': '40px'
                    })
            ),
            width=12, lg=4, className="mt-2 mt-lg-0"
        )
    ], className="my-5 justify-content-center"), 

    html.Div(
        id='search-content-container',
        style={'display': 'none'},
        children=[
            html.Hr(),
            dbc.Row([
                dbc.Col(
                    dcc.Loading(
                        id="loading-spinner", type="circle", color="primary",
                        children=html.Div(id="results-output", style={'maxHeight': '80vh', 'overflowY': 'auto', 'paddingRight': '15px'})
                    ),
                    md=5
                ),
                dbc.Col(
                    html.Div([
                        dbc.Tabs(
                            id="doc-tabs", active_tab="content-tab",
                            children=[
                                dbc.Tab(label="Content", tab_id="content-tab"),
                                dbc.Tab(label="Juris Map", tab_id="juris-map-tab"),
                                dbc.Tab(label="Juris Tree", tab_id="juris-tree-tab"),
                                dbc.Tab(label="Summary", tab_id="juris-summary-tab"),
                                dbc.Tab(label="Juris Link", tab_id="juris-link-tab"),
                            ],
                        ),
                        dcc.Loading(
                            id="loading-viewer", type="circle", color="primary",
                            children=html.Div(id="tab-content", className="p-2")
                        )
                    ],
                    id="doc-viewer-container",
                    style={"height": "80vh", "border": "1px solid #e0e0e0", "borderRadius": "5px"}),
                    md=7
                )
            ])
        ]
    )
], fluid=True, className="p-3 p-md-5", style={'fontFamily': "'Poppins', sans-serif"})

# --- HELPER FUNCTION ---

# --- MODIFICATION: Reworked to support hover effects and efficient highlighting ---
def format_results(response_json):
    """
    Helper function to format the API response into clickable result cards.
    This version includes the 'result-card' class for hover effects.
    """
    if not response_json or ("results" not in response_json and "summary" not in response_json):
        return dbc.Alert("An error occurred or the search returned no results.", color="danger")

    # --- Summary Generation Logic (Unchanged) ---
    summary_card = []
    if response_json.get("summary", {}).get("summaryText"):
        summary_text = response_json["summary"]["summaryText"]
        references = response_json.get("summary", {}).get("summaryWithMetadata", {}).get("references", [])
        results = response_json.get("results", [])
        doc_id_map = {result['document']['id']: result['document'] for result in results if 'document' in result}

        def create_citation_link(citation_num_str):
            try:
                citation_index = int(citation_num_str) - 1
                if 0 <= citation_index < len(references):
                    doc_resource_name = references[citation_index].get('document', '')
                    doc_id = doc_resource_name.split('/')[-1]
                    if doc_id in doc_id_map:
                        doc_struct_data = doc_id_map[doc_id].get('structData', {})
                        source_id = doc_struct_data.get('source_id')
                        jurisdiction_code = doc_struct_data.get('jurisdiction_code')
                        if source_id and jurisdiction_code:
                            return html.Span(
                                citation_num_str,
                                id={'type': 'view-doc-button', 'index': f'summary-ref-{citation_index}', 'source_id': source_id, 'jurisdiction_code': jurisdiction_code},
                                style={'cursor': 'pointer', 'color': '#003366', 'textDecoration': 'underline', 'fontWeight': '500'},
                                title=f"View source document: {doc_id}"
                            )
            except (ValueError, IndexError) as e:
                print(f"Error creating citation link for '{citation_num_str}': {e}")
            return citation_num_str

        def render_text_with_markdown(text_segment):
            pattern = re.compile(r'(\*.*?\*)')
            parts = pattern.split(text_segment)
            components = []
            for part in parts:
                if not part: continue
                if part.startswith('*') and part.endswith('*'):
                    components.append(html.Strong(part[1:-1]))
                else:
                    components.append(part)
            return components

        summary_body_components = []
        citation_pattern = re.compile(r'(\[[\d,\s]+\])')

        for paragraph in summary_text.strip().split('\n\n'):
            if not paragraph: continue
            paragraph_components = []
            text_parts = citation_pattern.split(paragraph)
            for part in text_parts:
                if not part: continue
                match = citation_pattern.fullmatch(part)
                if match:
                    citation_numbers = match.group(1).strip('[]').split(',')
                    linked_citations = [create_citation_link(num.strip()) for num in citation_numbers if num.strip()]
                    final_citation_block = []
                    for i, link in enumerate(linked_citations):
                        if i > 0: final_citation_block.append(", ")
                        final_citation_block.append(link)
                    paragraph_components.extend(["[", *final_citation_block, "]"])
                else:
                    paragraph_components.extend(render_text_with_markdown(part))
            summary_body_components.append(html.P(paragraph_components, className="card-text"))

        summary_card = [
            html.Div([
                html.H5(
                    ["Summary ", html.I(className="fas fa-chevron-down", id="summary-toggle-icon")],
                    id="summary-collapse-button", className="mb-2",
                    style={'cursor': 'pointer', 'color': '#6c757d', 'textTransform': 'none'}
                ),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(summary_body_components), className="border-0 bg-transparent p-0"),
                    id="summary-collapse", is_open=False,
                )
            ])
        ]

    if not response_json.get("results"):
        if summary_card: return summary_card
        return dbc.Alert("No results found for your query.", color="info")

    # --- Result Card Generation ---
    result_cards = []
    for i, result in enumerate(response_json["results"]):
        doc = result.get('document', {})
        struct_data = doc.get('structData', {})
        source_id = struct_data.get('source_id')
        jurisdiction_code = struct_data.get('jurisdiction_code')
        is_disabled = not (source_id and jurisdiction_code)

        # The entire card body content is now wrapped in a single clickable Div
        card_body_content = html.Div(
            [
                html.Div([
                    html.Span(
                        struct_data.get('neutral_citation'),
                        style={'backgroundColor': '#e9ecef', 'color': 'black', 'padding': '0.2rem 0.4rem', 'borderRadius': '4px', 'marginRight': '8px', 'fontSize': '0.9rem'}
                    ) if struct_data.get('neutral_citation') else None,
                    html.Span(struct_data.get('book_name', 'No Title Available'), style={'color': 'black', 'fontSize': '0.9rem'})
                ], className='mb-2'),
                html.P(
                    (lambda c: " ".join(c.split()[:30]) + "..." if len(c.split()) > 30 else c)(struct_data.get('content', 'No preview available.')),
                    className="card-text", style={'fontSize': '0.9rem'}
                )
            ],
            id={'type': 'view-doc-button', 'index': i, 'source_id': source_id or '', 'jurisdiction_code': jurisdiction_code or ''},
            style={'cursor': 'pointer' if not is_disabled else 'not-allowed', 'opacity': 1 if not is_disabled else 0.6, 'textDecoration': 'none', 'color': 'inherit'}
        )

        # The key change is here: adding className="mb-3 result-card"
        # This allows the CSS in style.css to target the card for the hover effect.
        card = dbc.Card(
            dbc.CardBody(card_body_content),
            id={'type': 'result-card', 'index': i},
            className="mb-3 result-card" # This class is essential for the hover effect
        )
        result_cards.append(card)

    return summary_card + result_cards


# --- CALLBACKS ---

# (This callback remains unchanged)
@app.callback(
    Output("summary-collapse", "is_open"),
    Output("summary-toggle-icon", "className"),
    Input("summary-collapse-button", "n_clicks"),
    State("summary-collapse", "is_open"),
    prevent_initial_call=True,
)
def toggle_summary_collapse(n_clicks, is_open):
    if not n_clicks:
        return no_update, no_update
    new_state = not is_open
    icon_class = "fas fa-chevron-up" if new_state else "fas fa-chevron-down"
    return new_state, icon_class

# (This callback remains unchanged)
@app.callback(
    Output('suggestions-output', 'children'),
    Input('search-input', 'value'),
    prevent_initial_call=True
)
def update_suggestions(query):
    if not query or len(query) < 3:
        return None
    try:
        access_token = get_gcp_token()
        response_json = get_suggestions(query, access_token)
        if 'error' in response_json or not response_json.get('completionResults'):
            return None
        items = [
            dbc.ListGroupItem(s['suggestion'], id={'type': 'suggestion-item', 'suggestion': s['suggestion']}, action=True)
            for s in response_json['completionResults']
        ]
        return dbc.ListGroup(items, style={'boxShadow': '0 4px 8px rgba(0,0,0,0.1)'})
    except Exception as e:
        print(f"Error in suggestions callback: {e}")
        return None

# (This callback remains unchanged)
@app.callback(
    Output('search-input', 'value', allow_duplicate=True),
    Output('suggestions-output', 'children', allow_duplicate=True),
    Input({'type': 'suggestion-item', 'suggestion': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def select_suggestion(n_clicks_list):
    ctx = dash.callback_context
    if not ctx.triggered or not any(n_clicks_list):
        return no_update, no_update
    suggestion_text = ctx.triggered[0]['id']['suggestion']
    return suggestion_text, None

# (This callback remains unchanged)
@app.callback(
    Output('selected-doc-store', 'data'),
    Output('doc-tabs', 'active_tab'),
    Input({'type': 'view-doc-button', 'index': ALL, 'source_id': ALL, 'jurisdiction_code': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def store_selected_document(n_clicks):
    if not any(n_clicks):
        return no_update, no_update
    ctx = dash.callback_context
    button_id = ctx.triggered_id
    doc_data = {
        'source_id': button_id['source_id'],
        'jurisdiction_code': button_id['jurisdiction_code'],
        'index': button_id['index']
    }
    return doc_data, 'content-tab'

# (This callback remains unchanged)
@app.callback(
    Output('tab-content', 'children'),
    Input('doc-tabs', 'active_tab'),
    State('selected-doc-store', 'data')
)
def update_tab_content(active_tab, stored_data):
    if not stored_data or not stored_data.get('source_id'):
        return html.Div("Please select a document from the search results.", className="p-3 text-center")

    source_id = stored_data['source_id']
    jurisdiction_code = stored_data['jurisdiction_code']
    tab_to_file_key = {
        'content-tab': 'source_file',
        'juris-map-tab': 'juris_map',
        'juris-tree-tab': 'juris_tree',
        'juris-summary-tab': 'juris_summary'
    }
    if active_tab == 'juris-link-tab':
        return html.Div("Juris Link content will be available in a future update.", className="p-3 text-center")
    file_key = tab_to_file_key.get(active_tab)
    if not file_key:
        return "Invalid tab selected."
    html_content = fetch_document_from_s3(S3_CONFIG, jurisdiction_code, source_id, file_key)
    return html.Iframe(srcDoc=html_content, style={"width": "100%", "height": "75vh", "border": "none"})

# (This callback remains unchanged)
@app.callback(
    Output('loading-modal', 'is_open'),
    [Input('search-button', 'n_clicks'), Input('search-input', 'n_submit')],
    State('search-input', 'value'),
    prevent_initial_call=True
)
def toggle_loading_modal(n_clicks, n_submit, query):
    if (n_clicks or n_submit) and query:
        return True
    return no_update

# (This callback remains unchanged)
@app.callback(
    Output('search-results-store', 'data'),
    Output('selected-doc-store', 'data', allow_duplicate=True),
    Output('loading-modal', 'is_open', allow_duplicate=True),
    Output('search-content-container', 'style'),
    [Input('search-button', 'n_clicks'), Input('search-input', 'n_submit')],
    State('search-input', 'value'),
    prevent_initial_call=True
)
def perform_new_search(n_clicks, n_submit, query):
    if not (n_clicks or n_submit) or not query:
        return no_update, no_update, no_update, no_update
    try:
        access_token = get_gcp_token()
        response_json = perform_search(query, access_token)
        return response_json, None, False, {'display': 'block'}
    except Exception as e:
        print(f"Error in search callback: {e}")
        error_results = {"error": f"An application error occurred: {e}"}
        return error_results, None, False, {'display': 'block'}

# --- MODIFICATION: This callback now ONLY runs when a new search happens. ---
# It no longer re-renders the list when a selection is made, preventing the flicker.
@app.callback(
    Output('results-output', 'children'),
    Input('search-results-store', 'data')
)
def display_search_results(search_data):
    if not search_data:
        return []
    if 'error' in search_data:
        return dbc.Alert(search_data['error'], color="danger")
    # `format_results` no longer needs the selected index, as another callback handles it.
    return format_results(search_data)

# --- NEW CALLBACK: Handles highlighting efficiently without redrawing the list. ---
# This targets the 'style' of the cards directly, which is very fast.
@app.callback(
    Output({'type': 'result-card', 'index': ALL}, 'style'),
    Input('selected-doc-store', 'data'),
    State({'type': 'result-card', 'index': ALL}, 'id'),
    prevent_initial_call=True
)
def highlight_selected_card(selected_data, card_ids):
    # When a new search is performed, selected_data becomes None.
    # We must reset the style for all cards.
    if not selected_data:
        return [{} for _ in card_ids]

    selected_index_str = str(selected_data.get('index'))
    styles = []
    for card_id in card_ids:
        # Apply highlight style to the selected card
        if str(card_id['index']) == selected_index_str:
            styles.append({'backgroundColor': '#e9ecef'})
        # Apply default style to all other cards
        else:
            styles.append({})
    return styles


# --- RUN THE APP ---
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8081)