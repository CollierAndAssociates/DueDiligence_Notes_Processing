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
from typing import Optional

def unpseudonymize(data: pd.DataFrame, output_path: str) -> Optional[pd.DataFrame]:
    """
    Replaces pseudonymized terms with original values using SQLite and saves as an Excel file.

    Args:
        data (pd.DataFrame): The DataFrame with pseudonymized values.
        output_path (str): Path to save the final Excel file.

    Returns:
        Optional[pd.DataFrame]: The unpseudonymized DataFrame.
    """
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()

        # Load the pseudonym mappings from SQLite
        c.execute("SELECT original, pseudonym FROM pseudonym_mapping")
        pseudonym_map = {row[1]: row[0] for row in c.fetchall()}  # {hashed_value: original_term}

        conn.close()

        print("🔍 Loaded pseudonym map:", list(pseudonym_map.items())[:5])  # Debugging

        # Apply mapping to External Entity column
        if "External Entity" in data.columns:
            data["External Entity"] = data["External Entity"].replace(pseudonym_map)

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