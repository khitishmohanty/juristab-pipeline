import sys
import os
from src.data_transfer import DataTransfer

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Adjust the Python path to include the root directory
# This allows imports like `from src.data_transfer import ...` to work
sys.path.append(script_dir)

if __name__ == "__main__":
    try:
        # Assuming config.yaml is in the 'config' folder relative to the root
        config_file_path = os.path.join(script_dir, 'config', 'config.yaml')
        
        # Create an instance of the DataTransfer class and run the process
        data_transfer_app = DataTransfer(config_file_path)
        data_transfer_app.run()
        
    except ValueError as e:
        print(f"Application failed to start due to a configuration error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
