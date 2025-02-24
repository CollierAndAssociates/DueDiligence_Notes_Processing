"""
DD_Output_Storage.py

This module handles the storage of processed analytical data to the local filesystem in Excel and JSON format.
It ensures that the output directory exists and securely writes structured data for further processing or review.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - json: Used for serializing data into JSON format.
    - pandas: Used for handling and writing structured data.
    - os: Provides file and directory manipulation functionalities.

Project Scope:
    - Creates necessary directories if they do not exist.
    - Writes structured JSON data to a file named 'analysis_output.json'.
    - Writes structured Excel data to 'analysis_output.xlsx' with multiple sheets.
    - Implements exception handling to ensure robustness.

Usage Example:
    >>> store_output(data, "./output/")
    Output stored at ./output/analysis_output.xlsx
"""

import json
import os
import pandas as pd
from typing import Any, Dict, Union, Optional

def store_output(
    data: Union[Dict[str, Any], pd.DataFrame], 
    directory: str, 
    summary_stats: Optional[pd.DataFrame] = None
) -> None:
    """
    Stores output data securely in Excel (for DataFrames) or JSON (for dictionaries) format.

    Args:
        data (Union[Dict[str, Any], pd.DataFrame]): The structured data to be stored.
        directory (str): The directory path where the file will be saved.
        summary_stats (Optional[pd.DataFrame]): Additional summary statistics to save as a separate sheet in Excel.

    Returns:
        None

    Raises:
        OSError: If there are issues creating the directory or writing to the file.
        Exception: Catches and logs unexpected errors.

    Example:
        >>> store_output(data, "./storage/")
        Output stored at ./storage/analysis_output.xlsx
    """
    if data is None:
        print("❌ Error: Attempted to save output, but data is None!")
        return

    try:
        # Ensure the target directory exists, create it if necessary
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Handling DataFrame Storage (Excel format)
        if isinstance(data, pd.DataFrame):
            file_path = os.path.join(directory, 'analysis_output.xlsx')

            # Writing both main data and summary statistics to separate sheets
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                data.to_excel(writer, sheet_name="Processed Data", index=False)
                
                if summary_stats is not None:
                    summary_stats.to_excel(writer, sheet_name="Summary Statistics", index=True)

            print(f"✅ Excel output stored at {file_path}")

        # Handling Dictionary Storage (JSON format)
        elif isinstance(data, dict):
            file_path = os.path.join(directory, 'analysis_output.json')
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"✅ Dictionary output stored at {file_path}")

        else:
            print("❌ Error: Unsupported data format for storage.")

    except OSError as e:
        print(f"❌ Filesystem error while storing output: {e}")
    except Exception as e:
        print(f"❌ Unexpected error storing output: {e}")
