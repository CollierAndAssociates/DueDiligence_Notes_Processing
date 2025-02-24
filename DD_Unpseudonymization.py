"""
DD_Unpseudonymization.py

This module is responsible for reverting pseudonymized data back to its original form using stored mappings.
It ensures that data previously obfuscated for security purposes can be restored when necessary.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - json: Used for handling structured data formats.
    - sqlite3: Accesses the SQLite database where pseudonymization mappings are stored.
    - hashlib: Used for hashing terms to match pseudonyms.

Usage Example:
    >>> unpseudonymized_data = unpseudonymize(pseudonymized_data)
    Data unpseudonymized successfully.
"""
import pandas as pd
import sqlite3
import os
import json
from typing import Optional

def unpseudonymize(data: pd.DataFrame, mapping_file: str, output_path: str) -> Optional[pd.DataFrame]:
    """
    Replaces pseudonymized terms with original values and saves as an Excel file.

    Args:
        data (pd.DataFrame): The DataFrame with pseudonymized values.
        mapping_file (str): Path to JSON file storing the mapping of pseudonyms to original values.
        output_path (str): Path to save the final Excel file.

    Returns:
        Optional[pd.DataFrame]: The unpseudonymized DataFrame.
    """
    try:
        if not os.path.exists(mapping_file):
            print(f"❌ Mapping file not found: {mapping_file}")
            return None

        # Load the pseudonym mapping
        with open(mapping_file, "r", encoding="utf-8") as f:
            pseudonym_map = json.load(f)

        # Debugging: Print the first few mapping entries
        print("🔍 Loaded pseudonym map:", list(pseudonym_map.items())[:5])

        # Apply mapping to External Entity column
        if "External Entity" in data.columns:
            data["External Entity"] = data["External Entity"].map(pseudonym_map).fillna(data["External Entity"])

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save the unpseudonymized DataFrame to an Excel file
        data.to_excel(output_path, index=False)
        print(f"✅ Unpseudonymized output saved to: {output_path}")

        return data

    except Exception as e:
        print(f"❌ Unexpected error in unpseudonymization: {e}")
        return None