# src/chatbot_client.py

import os
from openai import OpenAI

# Load the API key from an environment variable named 'HF_API_KEY'
API_KEY = os.environ.get("HF_API_KEY")

# Initialize the client to connect to the Hugging Face Inference API
client = None
if API_KEY:
    try:
        client = OpenAI(
            base_url="https://router.huggingface.co/v1",
            api_key=API_KEY,
        )
    except Exception as e:
        print(f"Error initializing OpenAI client: {e}")
else:
    # This message will be printed in your server logs if the key isn't set
    print("ERROR: The 'HF_API_KEY' environment variable is not set.")


def get_chatbot_response(question: str, history: list):
    """
    Gets a response from the chatbot model, including the last 2 interactions as context.

    Args:
        question (str): The user's current question.
        history (list): A list of previous chat interactions. 
                        Example: [{'role': 'user', 'content': 'Hi'}, {'role': 'assistant', 'content': 'Hello!'}]

    Returns:
        str: The chatbot's text response.
    """
    if not client:
        return "Sorry, the chatbot service is not configured correctly. The API key may be missing."
        
    # Each interaction consists of a user message and an assistant message.
    # To get the last 2 interactions, we need the last 4 messages.
    context_messages = history[-4:]

    # Add the user's current question to the message list to be sent to the model
    messages_to_send = context_messages + [{"role": "user", "content": question}]

    try:
        # Make the API call to the model
        completion = client.chat.completions.create(
            model="meta-llama/Llama-3.1-70B-Instruct:fireworks-ai",
            messages=messages_to_send,
            stream=False,  # We are not using streaming for simplicity
        )
        response_text = completion.choices[0].message.content
        return response_text
    except Exception as e:
        print(f"An error occurred while calling the API: {e}")
        return "Sorry, I encountered an error while trying to respond. Please check the application logs for more details."