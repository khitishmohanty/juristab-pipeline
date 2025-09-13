
#import json
import os
import pandas as pd

#from utils.file_converters import convert_json_to_csv_and_excel, convert_json_to_html

def _save_results(all_responses: list, page_metrics: list, output_dir: str) -> None:
    """
    Saves page metrics to Excel/CSV.
    The saving of all_responses to multiple formats (JSON, CSV, Excel, HTML)
    has been commented out as per user request.
    If you need a combined JSON of all responses, handler1.py now creates
    results.json and book_output.json.
    """
    if page_metrics:
        summary_df = pd.DataFrame(page_metrics)
        summary_excel_path = os.path.join(output_dir, "page_summary_with_verification.xlsx")
        try:
            summary_df.to_excel(summary_excel_path, index=False)
            print(f"✅ Page-level summary with verification written to: {summary_excel_path}")
        except Exception as e:
            print(f"❌ Failed to save page summary to Excel: {e}")
            # Fallback to CSV if Excel saving fails
            summary_csv_path = os.path.join(output_dir, "page_summary_with_verification.csv")
            try:
                summary_df.to_csv(summary_csv_path, index=False, encoding='utf-8-sig')
                print(f"✅ Page-level summary written to CSV as fallback: {summary_csv_path}")
            except Exception as e_csv:
                print(f"❌ Failed to save page summary to CSV as fallback: {e_csv}")
    else:
        print("ℹ️ No page metrics to save for page_summary_with_verification.")
