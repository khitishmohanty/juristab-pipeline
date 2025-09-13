import os
import pandas as pd
from app.utils.json_converter import convert_json_to_csv_and_excel

def test_json_to_csv_and_excel_from_file():
    # Get current test directory
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))

    # Define input and output paths
    input_dir = os.path.join(project_root, "tests", "assets", "inputs")
    output_dir = os.path.join(project_root, "tests", "assets", "outputs", "utils")
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(input_dir, "test_input.json")
    csv_path = os.path.join(output_dir, "test_output.csv")
    xlsx_path = os.path.join(output_dir, "test_output.xlsx")

    # Ensure input file exists
    assert os.path.exists(json_path), "❌ test_input.json not found in tests/assets/inputs/"

    # Run conversion
    convert_json_to_csv_and_excel(
        json_input=json_path,
        output_dir=output_dir,
        base_filename="test_output"
    )

    # Validate CSV
    assert os.path.exists(csv_path), "❌ CSV output file not found."
    df_csv = pd.read_csv(csv_path)
    assert df_csv.shape[0] > 0
    assert "correlation-id" in df_csv.columns
    assert "tag" in df_csv.columns

    # Validate Excel
    assert os.path.exists(xlsx_path), "❌ Excel output file not found."
    df_xlsx = pd.read_excel(xlsx_path)
    assert df_xlsx.shape[0] > 0

    # Optional cleanup
    # os.remove(csv_path)
    # os.remove(xlsx_path)
