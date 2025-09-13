import os
import re
import json
import base64
from app.services.gemini_client import call_gemini_api
from app.utils.prompt_loader import load_json_prompt

def test_integration_with_gemini():
    # Setup paths
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
    image_path = os.path.join(project_root, "tests", "assets", "inputs", "sample.jpg")
    prompt_path = os.path.join(project_root, "tests", "assets", "inputs", "gemini_layout_prompt.json")
    output_dir = os.path.join(project_root, "tests", "assets", "outputs", "services")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "gemini_output.json")

    # Encode the image
    with open(image_path, "rb") as img_file:
        image_base64 = base64.b64encode(img_file.read()).decode("utf-8")

    # Load prompt from JSON
    prompt_data = load_json_prompt("gemini_layout_prompt.json")
    prompt_details = prompt_data.get("prompt_details", {})
    task_description = prompt_details.get("task_description", "")
    output_instructions = prompt_details.get("output_format_instructions", {})
    image_desc = prompt_details.get("input_image_description", "")

    # Construct parts for Gemini API
    prompt_parts = []
    if task_description:
        prompt_parts.append({"text": task_description})
    if output_instructions:
        prompt_parts.append({
            "text": "Please follow the output format and schema strictly:\n" +
                    json.dumps(output_instructions, indent=2)
        })
    if image_desc:
        prompt_parts.append({"text": "Image description:\n" + image_desc})

    # Call Gemini API
    response = call_gemini_api(image_base64, prompt_parts)

    # Clean up markdown block if returned
    cleaned_response = response.strip()
    if cleaned_response.startswith("```json") or cleaned_response.startswith("```"):
        cleaned_response = re.sub(r"^```(?:json)?\s*", "", cleaned_response)
        cleaned_response = re.sub(r"\s*```$", "", cleaned_response)

    # Try parsing and saving JSON
    try:
        parsed_json = json.loads(cleaned_response)
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(parsed_json, json_file, indent=2, ensure_ascii=False)
        print(f"✅ Saved Gemini output to {output_path}")
    except json.JSONDecodeError:
        print("❌ Response was not valid JSON. File not saved.")
        print("🧪 Raw response:\n", cleaned_response)

    # Basic assertions
    assert isinstance(response, str)
    assert len(response.strip()) > 0
