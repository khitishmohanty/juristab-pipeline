import os
import json
import re
import base64
from dotenv import load_dotenv
from app.services.openai_client import call_openai_api

load_dotenv()

def test_integration_with_openai():
    # Get the current directory of this test file
    current_dir = os.path.dirname(__file__)

    # Define paths
    project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
    image_path = os.path.join(project_root, "tests", "assets", "inputs", "sample.jpg")
    output_dir = os.path.join(project_root, "tests", "assets", "outputs", "services")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "openai_output.json")

    # Load and encode the image as base64
    with open(image_path, "rb") as img_file:
        image_base64 = base64.b64encode(img_file.read()).decode("utf-8")

    # Define the prompt
    prompt = """
    This is a document layout detection task. Identify the following items in the page sequentially and give me an output with the text with the following tags. Enum, Figure, Footnote, Header, Heading, List, Paragraph, Table, Table of Contents (ToC), Title, Subtitle, Footer, Page number. Preserve the text styling information(Bold, italic and underline) in the output. Also, identify any act names or citations mentioned, issuance date, compliance date, legislative body, and publication date under a particular tag. if any information is not present, leave that blank. give me the output in json format. in the first column put the numbers by which it can be identified as the correlation between the parent and child items and the associations. Make the node names as correlation-id, tag, content, act-name-citations, issuance-date, compliance-date, legislative-body, publication-date, verification-flag="Not Verified"
    """

    # Call the OpenAI API
    response = call_openai_api(image_base64=image_base64, prompt=prompt)

    # Print raw response
    #print("-------- OpenAI API Response --------")
    #print(response)
    #print("-------- End of Response --------")

    # Clean up markdown-style formatting (if any)
    cleaned_response = response.strip()
    if cleaned_response.startswith("```json") or cleaned_response.startswith("```"):
        cleaned_response = re.sub(r"^```(?:json)?\s*", "", cleaned_response)
        cleaned_response = re.sub(r"\s*```$", "", cleaned_response)

    # Attempt to parse and save the cleaned JSON response
    try:
        parsed_json = json.loads(cleaned_response)
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(parsed_json, json_file, indent=2, ensure_ascii=False)
        print(f"✅ Saved OpenAI output to {output_path}")
    except json.JSONDecodeError:
        print("❌ Response was not valid JSON. File not saved.")

    # Assert the API response is not empty
    assert isinstance(response, str)
    assert len(response) > 0
