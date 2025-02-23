"""
DD_Output_Storage.py

This module handles the storage of processed analytical data to the local filesystem in JSON format.
It ensures that the output directory exists and securely writes structured data for further processing or review.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - json: Used for serializing data into JSON format.
    - os: Provides file and directory manipulation functionalities.

Project Scope:
    - Creates necessary directories if they do not exist.
    - Writes structured JSON data to a file named 'analysis_output.json'.
    - Implements exception handling to ensure robustness.

Usage Example:
    >>> store_output(data, "./output/")
    Output stored at ./output/analysis_output.json
"""

import json
import os
from typing import Any, Dict

def store_output(data: Dict[str, Any], directory: str) -> None:
    """
    Stores output data securely in JSON format on the local drive.

    Args:
        data (Dict[str, Any]): The structured data to be stored.
        directory (str): The directory path where the file will be saved.

    Returns:
        None

    Raises:
        OSError: If there are issues creating the directory or writing to the file.
        Exception: Catches and logs unexpected errors.

    Example:
        >>> store_output({"summary": "Processed data"}, "./storage/")
        Output stored at ./storage/analysis_output.json
    """
    try:
        # Ensure the target directory exists, create it if necessary
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Define the file path for storing output data
        file_path = os.path.join(directory, 'analysis_output.json')
        
        # Write the JSON data to the file with indentation for readability
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        print(f"Output stored at {file_path}")
    
    except OSError as e:
        print(f"Filesystem error while storing output: {e}")
    except Exception as e:
        print(f"Unexpected error storing output: {e}")

# Suggested Improvements:
# - Implement logging instead of print statements for better debugging and tracking.
# - Allow file name customization instead of using a hardcoded 'analysis_output.json'.
# - Consider adding compression (e.g., gzip) to reduce file size for large datasets.
