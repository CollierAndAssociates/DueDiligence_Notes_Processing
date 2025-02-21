import json
import os

def store_output(data, directory):
    """Store output data securely on local drive."""
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        file_path = os.path.join(directory, 'analysis_output.json')
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        
        print(f"Output stored at {file_path}")
    except Exception as e:
        print(f"Error storing output: {e}")