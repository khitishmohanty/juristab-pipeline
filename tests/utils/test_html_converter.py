import os
from app.utils.html_converter import convert_json_to_html

def test_convert_json_to_html_file_creation():
    # Get absolute path to the test directory
    current_dir = os.path.dirname(__file__)
    
    # Define new paths
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))
    input_dir = os.path.join(project_root, "tests", "assets", "inputs")
    output_dir = os.path.join(project_root, "tests", "assets", "outputs", "utils")
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(input_dir, "test_input.json")
    html_path = os.path.join(output_dir, "test_output.html")

    # Ensure the input file exists
    assert os.path.exists(json_path), "❌ test_input.json not found in tests/assets/inputs/"

    # Convert to HTML
    convert_json_to_html(
        json_input=json_path,
        output_dir=output_dir,
        output_filename="test_output.html"
    )

    # Validate output file
    assert os.path.exists(html_path), "❌ HTML output file was not created."

    # Check content in the HTML
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()
        assert "<html>" in html_content
        assert "<body>" in html_content
        assert any(tag in html_content for tag in ["<h1>", "<h2>", "<p>", "<footer>"]), "❌ Expected HTML tags not found"

    # Optional cleanup
    # os.remove(html_path)
