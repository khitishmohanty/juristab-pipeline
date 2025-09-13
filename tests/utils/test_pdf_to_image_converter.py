import os
import glob
from app.utils.pdf_to_image_converter import convert_pdf_to_images

def test_pdf_to_images_conversion():
    # Define absolute paths
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))
    
    input_dir = os.path.join(project_root, "tests", "assets", "inputs")
    output_dir = os.path.join(project_root, "tests", "assets", "outputs", "utils", "page_images")
    os.makedirs(output_dir, exist_ok=True)

    pdf_path = os.path.join(input_dir, "sample.pdf")

    # Ensure sample PDF exists
    assert os.path.exists(pdf_path), "❌ sample.pdf not found in tests/assets/inputs/"

    # Convert PDF to image files
    image_files = convert_pdf_to_images(pdf_path=pdf_path, output_dir=output_dir)
    # Add `poppler_path=...` if needed on Windows

    # Validate output
    assert len(image_files) > 0, "❌ No image files were generated from PDF."

    for image_file in image_files:
        assert os.path.exists(image_file), f"❌ Image file missing: {image_file}"
        assert image_file.endswith((".jpg", ".jpeg")), f"❌ Invalid file extension: {image_file}"

    # Clean up generated images (optional)
    # for f in glob.glob(os.path.join(output_dir, "*.jpg")):
    #     os.remove(f)
