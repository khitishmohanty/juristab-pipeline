import dash
from dash import dcc, html, Input, Output, State, no_update, callback
import dash_bootstrap_components as dbc
import base64
import io
import diskcache
from dash import DiskcacheManager

# This new file contains the logic to call the AI model
from src.chatbot_client import get_chatbot_response
from src.file_processors import FileProcessorFactory

# --- BACKGROUND CALLBACK SETUP ---
cache = diskcache.Cache("./cache")
background_callback_manager = DiskcacheManager(cache)

# --- APP INITIALIZATION ---

# Define paths to external CSS for styling
POPPINS_FONT = "https://fonts.googleapis.com/css2?family=Poppins:wght@400;500&display=swap"
FONT_AWESOME = "https://use.fontawesome.com/releases/v5.15.4/css/all.css"

# Initialize the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, POPPINS_FONT, FONT_AWESOME],
    title='JurisBot',
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1.0"}],
    background_callback_manager=background_callback_manager,
)
server = app.server

# --- CHATBOT APP LAYOUT ---
app.layout = dbc.Container([
    # These dcc.Store components hold app state in the user's browser
    dcc.Store(id='chat-history-store', data=[]),
    dcc.Store(id='full-response-store', data=None),
    dcc.Store(id='uploaded-file-content', data=None),
    dcc.Store(id='pending-user-message', data=None),

    # This interval component drives the typing effect (faster speed)
    dcc.Interval(id='typing-interval', interval=20, n_intervals=0, disabled=True),

    # Header section with Logo and Title
    dbc.Row(
        [
            dbc.Col(
                html.Img(src=app.get_asset_url('logo.png'), height="40px"),
                width="auto",
                className="d-flex align-items-center"
            ),
            dbc.Col(
                html.Span("|", style={'fontSize': '2rem', 'color': '#dee2e6', 'lineHeight': '40px'}),
                width="auto",
                className="px-2 d-flex align-items-center"
            ),
            dbc.Col(
                html.H1("JurisBot", style={'margin': '0', 'fontSize': '1.75rem', 'lineHeight': '40px'}),
                width="auto",
                className="d-flex align-items-center"
            ),
        ],
        className="my-4",
        align="center",
        style={'height': '40px'}
    ),

    # This is the main area where chat bubbles will be displayed
    dbc.Row(
        dbc.Col(
            html.Div(id='chat-history', style={
                'height': '65vh',
                'overflowY': 'auto',
                'border': '1px solid #e0e0e0',
                'borderRadius': '15px',
                'padding': '20px',
                'display': 'flex',
                'flexDirection': 'column',
                'backgroundColor': '#ffffff'
            })
        )
    ),

    # User input section with attachment button
    dbc.Row([
        dbc.Col([
            # Container for attached file display with fixed height
            html.Div(
                id='attached-file-display', 
                style={
                    'minHeight': '0px',
                    'maxHeight': '80px',
                    'overflow': 'hidden',
                    'transition': 'all 0.3s ease-in-out',
                    'marginBottom': '10px'
                }
            ),
            
            # Input container with attachment button
            html.Div([
                dcc.Upload(
                    id='upload-file',
                    children=html.Button(
                        "+",
                        id='attachment-button',
                        className='btn btn-link p-0',
                        style={
                            'width': '35px',
                            'height': '35px',
                            'borderRadius': '50%',
                            'backgroundColor': 'transparent',
                            'border': '1px solid #e0e0e0',
                            'color': '#9e9e9e',
                            'fontSize': '20px',
                            'fontWeight': '300',
                            'marginRight': '10px',
                            'cursor': 'pointer',
                            'display': 'inline-flex',
                            'alignItems': 'center',
                            'justifyContent': 'center',
                            'verticalAlign': 'top',
                            'marginTop': '5px',
                            'lineHeight': '1'
                        }
                    ),
                    style={'display': 'inline-block', 'verticalAlign': 'middle'},
                    multiple=False,
                    accept='.txt,.html,.pdf,.docx,.csv,.json'
                ),
                dcc.Textarea(
                    id='chat-input',
                    placeholder='Ask me anything...',
                    className='form-control',
                    style={
                        'borderRadius': '15px',
                        'display': 'inline-block',
                        'width': 'calc(100% - 150px)',
                        'verticalAlign': 'middle',
                        'backgroundColor': '#ffffff',
                        'border': '1px solid #dee2e6',
                        'color': '#495057',
                        'paddingLeft': '20px',
                        'paddingRight': '20px',
                        'paddingTop': '12px',
                        'minHeight': '80px',
                        'height': 'auto',
                        'maxHeight': '200px',
                        'resize': 'none',
                        'overflowY': 'auto',
                        'lineHeight': '1.5',
                        'fontFamily': "'Poppins', sans-serif",
                        'whiteSpace': 'pre-wrap',
                        'wordWrap': 'break-word'
                    },
                    persisted_props=['value'],
                    persistence_type='memory'
                ),
                html.Button('Send', id='send-button', n_clicks=0,
                    style={
                        'backgroundColor': '#003366',
                        'color': 'white',
                        'border': 'none',
                        'borderRadius': '20px',
                        'height': '35px',
                        'width': '75px',
                        'marginLeft': '10px',
                        'cursor': 'pointer',
                        'display': 'inline-block',
                        'verticalAlign': 'top',
                        'marginTop': '0px',
                        'fontSize': '14px'
                    }
                ),
            ], style={'display': 'flex', 'alignItems': 'center'})
        ], width=12)
    ], className="mt-3")

], fluid=True, className="p-3", style={'fontFamily': "'Poppins', sans-serif", 'backgroundColor': '#ffffff', 'minHeight': '100vh'})


# --- CALLBACKS ---

# Callback for file upload handling
@app.callback(
    Output('uploaded-file-content', 'data'),
    Output('attached-file-display', 'children'),
    Input('upload-file', 'contents'),
    State('upload-file', 'filename'),
    prevent_initial_call=True
)
def process_uploaded_file(contents, filename):
    if contents is None:
        return None, ""
    
    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Get file extension
        file_extension = filename.split('.')[-1].lower()
        
        # Process file using the appropriate processor
        processor = FileProcessorFactory.get_processor(file_extension)
        if processor:
            processed_content = processor.process(decoded, filename)
            
            # Create file info to store
            file_data = {
                'filename': filename,
                'content': processed_content,
                'type': file_extension
            }
            
            # Create the attached file display component with animation
            file_display = html.Div([
                dbc.Card(
                    dbc.CardBody([
                        html.Div([
                            html.Div([
                                html.Span(filename, style={'fontSize': '14px', 'color': '#495057'}),
                                html.Br(),
                                html.Span(f"{len(decoded)} bytes", style={'fontSize': '12px', 'color': '#6c757d'}),
                            ], style={'flex': '1'}),
                            html.Button(
                                html.I(className="fas fa-times"),
                                id={'type': 'remove-file-btn', 'index': 0},
                                n_clicks=0,
                                style={
                                    'background': 'transparent',
                                    'border': 'none',
                                    'color': '#6c757d',
                                    'cursor': 'pointer',
                                    'padding': '0',
                                    'fontSize': '14px'
                                }
                            )
                        ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between'}),
                        html.Div(
                            file_extension.upper(),
                            style={
                                'position': 'absolute',
                                'bottom': '5px',
                                'right': '10px',
                                'backgroundColor': '#e9ecef',
                                'color': '#495057',
                                'padding': '2px 8px',
                                'borderRadius': '4px',
                                'fontSize': '10px',
                                'fontWeight': 'bold'
                            }
                        )
                    ], style={'position': 'relative', 'padding': '10px'}),
                    className='attachment-card',
                    style={
                        'backgroundColor': '#f8f9fa',
                        'border': '1px solid #dee2e6',
                        'borderRadius': '8px',
                        'maxWidth': '300px'
                    }
                )
            ])
            
            return file_data, file_display
        else:
            error_display = html.Div([
                html.I(className="fas fa-exclamation-circle text-danger mr-2"),
                f"Unsupported file type: {file_extension}"
            ], style={'color': '#ff6b6b', 'fontSize': '14px'})
            return None, error_display
            
    except Exception as e:
        error_display = html.Div([
            html.I(className="fas fa-exclamation-circle text-danger mr-2"),
            f"Error processing file: {str(e)}"
        ], style={'color': '#ff6b6b', 'fontSize': '14px'})
        return None, error_display

# Callback to immediately show user message and loading indicator
@app.callback(
    Output('chat-history-store', 'data'),
    Output('pending-user-message', 'data'),
    Output('chat-input', 'value'),
    Input('send-button', 'n_clicks'),
    State('chat-input', 'value'),
    State('chat-history-store', 'data'),
    State('uploaded-file-content', 'data'),
    prevent_initial_call=True
)
def add_user_message_and_loading(n_clicks, user_input, chat_history, file_data):
    if not user_input:
        return no_update, no_update, no_update
    
    # Prepare the message with file content if available
    if file_data:
        message_with_file = f"{user_input}\n\n📎 {file_data['filename']}"
        full_context = f"{user_input}\n\n--- File Content ({file_data['filename']}) ---\n{file_data['content']}"
    else:
        message_with_file = user_input
        full_context = user_input
    
    # Add user message and loading indicator
    new_history = chat_history.copy()
    new_history.append({'role': 'user', 'content': message_with_file})
    new_history.append({'role': 'assistant', 'content': 'loading', 'is_loading': True})
    
    # Store the context for the background callback
    pending_message = {
        'full_context': full_context,
        'history_without_loading': chat_history.copy()
    }
    
    return new_history, pending_message, ''

# Background callback to handle API call
@callback(
    Output('full-response-store', 'data'),
    Output('chat-history-store', 'data', allow_duplicate=True),
    Output('uploaded-file-content', 'data', allow_duplicate=True),
    Output('attached-file-display', 'children', allow_duplicate=True),
    Input('pending-user-message', 'data'),
    State('chat-history-store', 'data'),
    background=True,
    prevent_initial_call=True,
    running=[
        (Output('send-button', 'disabled'), True, False),
        (Output('chat-input', 'disabled'), True, False),
    ],
)
def handle_api_call(pending_message, current_history):
    if not pending_message:
        return no_update, no_update, no_update, no_update
    
    # Get the chatbot response
    bot_response = get_chatbot_response(
        pending_message['full_context'], 
        pending_message['history_without_loading']
    )
    
    # Remove loading indicator and prepare updated history
    updated_history = [msg for msg in current_history if not msg.get('is_loading')]
    
    # Clear file data
    return bot_response, updated_history, None, ""

# Add callback to handle file removal
@app.callback(
    Output('uploaded-file-content', 'data', allow_duplicate=True),
    Output('attached-file-display', 'children', allow_duplicate=True),
    Input({'type': 'remove-file-btn', 'index': 0}, 'n_clicks'),
    prevent_initial_call=True
)
def remove_file(n_clicks):
    if n_clicks and n_clicks > 0:
        return None, ""
    return no_update, no_update

# Callback 2: Initiates the typing effect once the full response is available.
@app.callback(
    Output('typing-interval', 'disabled'),
    Output('chat-history-store', 'data', allow_duplicate=True),
    Input('full-response-store', 'data'),
    State('chat-history-store', 'data'),
    prevent_initial_call=True
)
def start_typing_effect(full_response, chat_history):
    if not full_response:
        return True, no_update

    # Add an empty placeholder for the bot's message to start the effect
    chat_history.append({'role': 'assistant', 'content': ''})
    return False, chat_history # False means the interval is now enabled

# Callback 3: The main typing logic, triggered by the interval.
@app.callback(
    Output('chat-history-store', 'data', allow_duplicate=True),
    Output('typing-interval', 'disabled', allow_duplicate=True),
    Input('typing-interval', 'n_intervals'),
    State('full-response-store', 'data'),
    State('chat-history-store', 'data'),
    prevent_initial_call=True
)
def update_typing(n, full_response, chat_history):
    if not chat_history or chat_history[-1]['role'] != 'assistant':
        return no_update, True # Stop if something is wrong

    current_text = chat_history[-1]['content']
    
    # If the displayed text is shorter than the full response, add the next character
    if len(current_text) < len(full_response):
        # Add a blinking cursor effect while typing
        next_char_index = len(current_text)
        chat_history[-1]['content'] = full_response[:next_char_index + 1] + "▌"
        return chat_history, False # Keep the interval running
    else:
        # Typing is done, remove the cursor and disable the interval
        chat_history[-1]['content'] = full_response
        return chat_history, True # True disables the interval

# Callback 5: Renders the chat history from the store to the screen.
@app.callback(
    Output('chat-history', 'children'),
    Input('chat-history-store', 'data')
)
def display_chat_history(chat_history):
    if not chat_history:
        return html.P("No conversation yet. Start by asking a question!", className="text-center text-muted mt-3")

    formatted_messages = []
    for msg in chat_history:
        if msg['role'] == 'user':
            bubble_style = 'd-flex justify-content-end mb-3'
            
            # Create user message with "You" label
            message_content = html.Div([
                html.Div("You", style={
                    'fontSize': '12px',
                    'color': '#6c757d',
                    'marginBottom': '5px',
                    'fontWeight': '500'
                }),
                dbc.Card(
                    dbc.CardBody(
                        dcc.Markdown(msg['content'], link_target="_blank", style={'color': '#495057'})
                    ),
                    style={
                        'maxWidth': '100%', 
                        'borderRadius': '15px', 
                        'backgroundColor': '#ffffff',
                        'border': '1px solid #dee2e6'
                    }
                )
            ], style={'maxWidth': '80%'})
            
            formatted_messages.append(html.Div(message_content, className=bubble_style))
            
        else: # Assistant's turn
            # Check if this is a loading message
            if msg.get('is_loading'):
                bubble_style = 'd-flex justify-content-start mb-3'
                # Create ChatGPT-style loading dots with JurisTab label
                loading_content = html.Div([
                    html.Div("JurisTab", style={
                        'fontSize': '12px',
                        'color': '#6c757d',
                        'marginBottom': '5px',
                        'fontWeight': '500'
                    }),
                    dbc.Card(
                        dbc.CardBody(
                            html.Div([
                                html.Span("●", className='loading-dot', style={'opacity': '0.4'}),
                                html.Span(" ● ", className='loading-dot', style={'opacity': '0.4'}),
                                html.Span("●", className='loading-dot', style={'opacity': '0.4'}),
                            ], style={'fontSize': '20px', 'color': '#6c757d'})
                        ),
                        style={
                            'maxWidth': '100px',
                            'borderRadius': '15px',
                            'backgroundColor': '#f8f9fa',
                            'border': '1px solid #dee2e6'
                        }
                    )
                ], style={'maxWidth': '80%'})
                formatted_messages.append(html.Div(loading_content, className=bubble_style))
                continue
            
            bubble_style = 'd-flex justify-content-start mb-3'
            
            # Create assistant message with "JurisTab" label
            message_content = html.Div([
                html.Div("JurisTab", style={
                    'fontSize': '12px',
                    'color': '#6c757d',
                    'marginBottom': '5px',
                    'fontWeight': '500'
                }),
                dbc.Card(
                    dbc.CardBody(
                        dcc.Markdown(msg['content'], link_target="_blank", style={'color': '#495057'})
                    ),
                    style={
                        'maxWidth': '100%', 
                        'borderRadius': '15px',
                        'backgroundColor': '#ffffff',
                        'border': '1px solid #dee2e6'
                    }
                )
            ], style={'maxWidth': '80%'})
            
            formatted_messages.append(html.Div(message_content, className=bubble_style))

    return formatted_messages

# --- RUN THE APP ---
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)