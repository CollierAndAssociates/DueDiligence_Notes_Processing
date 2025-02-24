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
import pandas as pd
from typing import Any, Dict, Union

def store_output(data: Union[Dict[str, Any], pd.DataFrame], directory: str) -> None:
    """
    Stores output data securely in JSON (for dictionaries) or CSV (for DataFrames) format.

    Args:
        data (Union[Dict[str, Any], pd.DataFrame]): The structured data to be stored.
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
    if data is None:
        print("❌ Error: Attempted to save output, but data is None!")
        return

    try:
        # Ensure the target directory exists, create it if necessary
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Handling DataFrame vs. Dictionary Storage
        if isinstance(data, pd.DataFrame):
            file_path = os.path.join(directory, 'analysis_output.csv')
            data.to_csv(file_path, index=False)
            print(f"✅ DataFrame output stored at {file_path}")
        elif isinstance(data, dict):
            file_path = os.path.join(directory, 'analysis_output.json')
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"✅ Dictionary output stored at {file_path}")
        else:
            print("❌ Error: Unsupported data format for storage.")

    except OSError as e:
        print(f"Filesystem error while storing output: {e}")
    except Exception as e:
        print(f"Unexpected error storing output: {e}")