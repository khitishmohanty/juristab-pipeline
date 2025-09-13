from pathlib import Path
import json

def load_text_prompt(filename: str) -> str:
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent
    prompt_path = project_root / "prompts" / filename

    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


def load_json_prompt(filename: str) -> dict:
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent
    prompt_path = project_root / "prompts" / filename

    with open(prompt_path, "r", encoding="utf-8") as f:
        return json.load(f)
