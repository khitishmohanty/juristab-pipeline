import os
import time
import json
from datetime import datetime, timezone
from utils.database_connector import DatabaseConnector
from utils.gemini_client import GeminiClient
from utils.s3_manager import S3Manager
from utils.html_generator import HtmlGenerator

class AiProcessor:
    # __init__ now accepts a 'source_info' dictionary and a 'processing_year'.
    def __init__(self, config: dict, prompt_path: str, source_info: dict, processing_year: int):
        self.config = config
        self.source_info = source_info # Store the specific source config
        self.processing_year = processing_year # Store the processing year
        self.html_generator = HtmlGenerator()
        self.s3_manager = S3Manager(region_name=config['aws']['default_region'])
        self.gemini_client = GeminiClient(model_name=config['models']['gemini']['model'])
        self.dest_db = DatabaseConnector(db_config=config['database']['destination'])
        self.prompt = self._load_prompt(prompt_path)

    def _load_prompt(self, prompt_file: str) -> str:
        try:
            print(f"Loading prompt from: {prompt_file}")
            with open(prompt_file, 'r') as f:
                return f.read()
        except FileNotFoundError:
            print(f"Error: Prompt file not found at '{prompt_file}'")
            raise

    def process_cases(self):
        dest_table_info = self.config['tables']['tables_to_write'][0]
        dest_table = dest_table_info['table']
        column_config = dest_table_info['columns']
        
        # MODIFIED: Pass the entire registry_config dictionary to the database query.
        registry_config = self.config.get('tables_registry', {})
        if not registry_config or 'column' not in registry_config:
            raise ValueError("Configuration error: 'tables_registry' with a 'column' key is not defined in config.yaml")

        cases_df = self.dest_db.get_records_for_ai_processing(
            dest_table, 
            column_config, 
            self.source_info,
            self.processing_year,
            registry_config # Pass the whole dictionary
        )
        
        source_table_name = self.source_info['table'] # Keep this for logging
        print(f"Found {len(cases_df)} cases for year {self.processing_year} from source '{source_table_name}' ready for AI enrichment.")

        for index, row in cases_df.iterrows():
            source_id = str(row['source_id'])
            print(f"\n--- Processing AI/HTML for case: {source_id} ---")
            
            s3_bucket = self.config['aws']['s3']['bucket_name']
            s3_base_folder = self.source_info['s3_dest_folder']
            filenames = self.config['enrichment_filenames']
            
            case_folder = os.path.join(s3_base_folder, source_id)
            txt_file_key = os.path.join(case_folder, filenames['extracted_text'])
            json_file_key = os.path.join(case_folder, filenames['jurismap_json'])
            tree_html_file_key = os.path.join(case_folder, filenames['jurismap_html'])

            json_content = None
            if getattr(row, column_config['json_valid_status']) != 'pass':
                print(f"JSON status is not 'pass'. Running process.")
                text_content = self.s3_manager.get_file_content(s3_bucket, txt_file_key)
                if text_content:
                    json_content = self._generate_and_save_json(text_content, s3_bucket, json_file_key, dest_table, source_id, column_config)
            else:
                print("JSON already generated. Loading from S3.")
                try:
                    json_string = self.s3_manager.get_file_content(s3_bucket, json_file_key)
                    json_content = json.loads(json_string)
                except Exception as e:
                    print(f"Could not load existing JSON for {source_id}. Error: {e}")
                    continue

            if not json_content:
                print(f"Skipping HTML generation for {source_id} due to missing JSON content.")
                continue

            if getattr(row, column_config['html_status']) != 'pass':
                print(f"HTML status is not 'pass'. Running process.")
                self._generate_and_save_html_tree(json_content, s3_bucket, tree_html_file_key, dest_table, source_id, column_config)
            else:
                print("HTML tree already generated. Skipping.")

        print(f"\n--- AI Enrichment check completed for source: {source_table_name} for year {self.processing_year} ---")

    def _generate_and_save_json(self, text_content, bucket, json_key, status_table, source_id, column_config):
        # Record start time as a timezone-aware datetime object for the database
        start_time_dt = datetime.now(timezone.utc)
        end_time_dt = None
        duration = 0

        # Initialize token and price variables
        input_tokens, output_tokens = 0, 0
        input_price, output_price = 0.0, 0.0

        try:
            # Get response and token counts from Gemini
            gemini_response_str, input_tokens, output_tokens = self.gemini_client.generate_json_from_text(self.prompt, text_content)
            
            # Get pricing configuration
            pricing_config = self.config['models']['gemini']['pricing']
            input_price_per_million = pricing_config['input_per_million']
            output_price_per_million = pricing_config['output_per_million']

            # Calculate prices based on token counts
            if input_tokens > 0:
                input_price = (input_price_per_million / 1000000) * input_tokens
            if output_tokens > 0:
                output_price = (output_price_per_million / 1000000) * output_tokens

            # Log the token usage and calculated prices
            print(f"Gemini Token Usage - Input: {input_tokens} (${input_price:.6f}), Output: {output_tokens} (${output_price:.6f})")
            
            # Record end time and calculate duration after the API call
            end_time_dt = datetime.now(timezone.utc)
            duration = (end_time_dt - start_time_dt).total_seconds()

            if self.gemini_client.is_valid_json(gemini_response_str):
                # If JSON is valid, save it and update status to 'pass'
                self.s3_manager.save_json_file(bucket, json_key, gemini_response_str)
                self.dest_db.update_step_result(
                    status_table, source_id, 'json_valid', 'pass', duration, column_config,
                    token_input=input_tokens,
                    token_output=output_tokens,
                    token_input_price=input_price,
                    token_output_price=output_price,
                    start_time=start_time_dt,
                    end_time=end_time_dt
                )
                return json.loads(gemini_response_str)
            else:
                # If JSON is invalid, raise an error to be caught by the except block
                raise ValueError("Gemini response was not valid JSON.")
        except Exception as e:
            # If any exception occurs, record the end time and duration
            if end_time_dt is None:
                end_time_dt = datetime.now(timezone.utc)
            duration = (end_time_dt - start_time_dt).total_seconds()
            
            print(f"JSON generation failed for {source_id}. Error: {e}")
            
            # Update status to 'failed' and log any available data
            self.dest_db.update_step_result(
                status_table, source_id, 'json_valid', 'failed', duration, column_config,
                token_input=input_tokens,
                token_output=output_tokens,
                token_input_price=input_price,
                token_output_price=output_price,
                start_time=start_time_dt,
                end_time=end_time_dt
            )
            return None

    def _generate_and_save_html_tree(self, json_data, bucket, html_key, status_table, source_id, column_config):
        start_time = time.time()
        try:
            html_content = self.html_generator.generate_html_tree(json_data)
            self.s3_manager.save_text_file(bucket, html_key, html_content)
            duration = time.time() - start_time
            self.dest_db.update_step_result(status_table, source_id, 'jurismap_html', 'pass', duration, column_config)
        except Exception as e:
            duration = time.time() - start_time
            print(f"HTML tree generation failed for {source_id}. Error: {e}")
            self.dest_db.update_step_result(status_table, source_id, 'jurismap_html', 'failed', duration, column_config)
