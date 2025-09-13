import dash
from dash import dcc, html, Input, Output, State, no_update
import dash_bootstrap_components as dbc
import time

# This new file contains the logic to call the AI model
from src.chatbot_client import get_chatbot_response

# --- APP INITIALIZATION ---

# Define paths to external CSS for styling
POPPINS_FONT = "https://fonts.googleapis.com/css2?family=Poppins:wght@400;500&display=swap"
FONT_AWESOME = "https://use.fontawesome.com/releases/v5.15.4/css/all.css"

# Initialize the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.LUX, POPPINS_FONT, FONT_AWESOME],
    title='JurisBot',
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1.0"}],
)
server = app.server

# --- CHATBOT APP LAYOUT ---
app.layout = dbc.Container([
    # These dcc.Store components hold app state in the user's browser
    dcc.Store(id='chat-history-store', data=[]),
    dcc.Store(id='full-response-store', data=None),

    # This interval component drives the typing effect (faster speed)
    dcc.Interval(id='typing-interval', interval=20, n_intervals=0, disabled=True),

    # A modal that appears while the app is waiting for the AI model's response
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Thinking...")),
            dbc.ModalBody(
                html.Div([
                    dbc.Spinner(size="lg", color="primary", spinner_style={"width": "3rem", "height": "3rem"}),
                    html.Div("Please wait while I generate a response.", className="mt-2")
                ], className="text-center")
            ),
        ],
        id="loading-modal", is_open=False, centered=True, backdrop="static", keyboard=False,
    ),

    # Header section with Logo and Title
    dbc.Row(
        [
            dbc.Col(
                html.Img(src=app.get_asset_url('logo.png'), height="40px"),
                width="auto"
            ),
            dbc.Col(
                html.Span("|", style={'fontSize': '2rem', 'color': '#dee2e6'}),
                width="auto",
                className="px-3"
            ),
            dbc.Col(
                html.H1("JurisBot", style={'margin': '0', 'fontSize': '1.75rem'}),
                width="auto"
            ),
        ],
        className="my-4",
        align="center",
    ),

    # This is the main area where chat bubbles will be displayed
    dbc.Row(
        dbc.Col(
            html.Div(id='chat-history', style={
                'height': '70vh',
                'overflowY': 'auto',
                'border': '1px solid #e0e0e0',
                'borderRadius': '15px',
                'padding': '20px',
                'display': 'flex',
                'flexDirection': 'column'
            })
        )
    ),

    # User input section at the bottom of the page
    dbc.Row([
        dbc.Col(
            dcc.Input(
                id='chat-input',
                type='text',
                placeholder='Ask me anything...',
                className='form-control',
                n_submit=0,
                autoComplete='off',
                style={'borderRadius': '30px'}
            ),
            width=10
        ),
        dbc.Col(
            html.Button('Send', id='send-button', n_clicks=0, className='w-100',
                style={
                    'backgroundColor': '#003366', 'color': 'white', 'border': 'none',
                    'borderRadius': '30px', 'height': '100%'
                }
            ),
            width=2
        )
    ], className="mt-3 align-items-center")

], fluid=True, className="p-3", style={'fontFamily': "'Poppins', sans-serif"})


# --- CALLBACKS ---

# Callback 1: Handles user input, calls the AI model, and stores the full response.
# This is a standard (blocking) callback now.
@app.callback(
    Output('full-response-store', 'data'),
    Output('chat-history-store', 'data'),
    Output('loading-modal', 'is_open'),
    Output('chat-input', 'value'),
    Input('send-button', 'n_clicks'),
    Input('chat-input', 'n_submit'),
    State('chat-input', 'value'),
    State('chat-history-store', 'data'),
    prevent_initial_call=True,
)
def handle_user_input(send_clicks, input_submits, user_input, chat_history):
    if not user_input:
        return no_update, no_update, False, no_update

    # Add the user's message to the history
    chat_history.append({'role': 'user', 'content': user_input})

    # Call the chatbot to get the full response (this will block the UI, which is why we show a modal)
    bot_response = get_chatbot_response(user_input, chat_history)

    # When the response is ready, store it, update history, close modal, and clear input
    return bot_response, chat_history, False, ''

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

# Callback 4: Opens the loading modal as soon as the user sends a message.
@app.callback(
    Output('loading-modal', 'is_open', allow_duplicate=True),
    Input('send-button', 'n_clicks'),
    Input('chat-input', 'n_submit'),
    State('chat-input', 'value'),
    prevent_initial_call=True
)
def show_loading_modal(n_clicks, n_submit, value):
    if (n_clicks or n_submit) and value:
        return True
    return no_update

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
            card_style = {'maxWidth': '80%', 'borderRadius': '15px', 'border': '1px solid #007bff'}
            card_body_content = dcc.Markdown(msg['content'], link_target="_blank")
        else: # Assistant's turn
            bubble_style = 'd-flex justify-content-start mb-3'
            card_style = {'maxWidth': '80%', 'borderRadius': '15px'}
            card_body_content = dcc.Markdown(msg['content'], link_target="_blank")

        card = dbc.Card(
            dbc.CardBody(card_body_content),
            style=card_style
        )
        formatted_messages.append(html.Div(card, className=bubble_style))

    return formatted_messages

# --- RUN THE APP ---
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

