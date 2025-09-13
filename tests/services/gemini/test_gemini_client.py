import pytest
from unittest.mock import patch
from app.services.gemini_client import call_gemini_api

@pytest.fixture
def sample_image_base64():
    return "base64imagestring"

def test_call_gemini_success(sample_image_base64):
    mock_response = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {"text": "This is a mocked Gemini response."}
                    ]
                }
            }
        ]
    }

    with patch("app.services.gemini_client.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = mock_response

        result = call_gemini_api(sample_image_base64, [{"text": "Describe this image."}])
        assert result == "This is a mocked Gemini response."
