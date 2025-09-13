import pytest
from unittest.mock import patch
from app.services.openai_client import call_openai_api

@pytest.fixture
def sample_image_base64():
    return "base64imagestring"

def test_call_openai_success(sample_image_base64):
    mock_response = type("MockResponse", (), {
        "choices": [
            type("Choice", (), {
                "message": type("Message", (), {"content": "This is a mocked OpenAI response."})()
            })()
        ]
    })()

    with patch("app.services.openai_client.client.chat.completions.create", return_value=mock_response) as mock_create:
        result = call_openai_api(prompt="Describe this image.", image_base64=sample_image_base64)
        assert result == "This is a mocked OpenAI response."
