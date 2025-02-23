"""
DD_Pseudonymization.py

This module handles pseudonymization of sensitive terms and entity names in a dataset.
It replaces specific terms and values in the 'External Entity' column with hashed pseudonyms
while maintaining a mapping for later unpseudonymization.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - pandas: Used for data manipulation and replacement operations.
    - hashlib: Provides hashing capabilities for secure pseudonymization.
    - sqlite3: Used to retrieve stored pseudonymization terms from a local database.

Project Scope:
    - Ensures sensitive information is anonymized before processing.
    - Works as part of a larger workflow for data security and compliance.
    - Supports later unpseudonymization for final reports.

Usage Example:
    >>> df, mapping = pseudonymize(df)
    Pseudonymization complete.

Configuration Steps:
    - Ensure `terms.db` exists and contains terms to be pseudonymized.
    - Run this script as part of the data processing pipeline before analysis.
"""

import pandas as pd
import hashlib
import sqlite3
from typing import Tuple, Dict

def pseudonymize(data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Pseudonymizes sensitive terms and the 'External Entity' column using SHA-256 hashing.

    Args:
        data (pd.DataFrame): The dataset containing sensitive terms and entity names.

    Returns:
        Tuple[pd.DataFrame, Dict[str, str]]: The pseudonymized DataFrame and a mapping for reversal.

    Raises:
        sqlite3.Error: If there is an issue accessing the terms database.
        KeyError: If the 'External Entity' column is missing from the dataset.

    Example:
        >>> df, mapping = pseudonymize(df)
        Pseudonymization complete.
    """
    try:
        # Connect to the SQLite database to retrieve stored terms
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        c.execute("SELECT term FROM terms")
        terms_to_pseudo = [row[0] for row in c.fetchall()]
        conn.close()
        
        mapping: Dict[str, str] = {}

        # Pseudonymize stored terms using SHA-256 hashing
        for term in terms_to_pseudo:
            pseudo = hashlib.sha256(term.strip().encode()).hexdigest()[:10]
            data.replace(term.strip(), pseudo, inplace=True)
            mapping[pseudo] = term.strip()

        # Ensure 'External Entity' column exists before processing
        if 'External Entity' not in data.columns:
            raise KeyError("Missing required column: 'External Entity'")

        # Pseudonymize unique entities in the 'External Entity' column
        for entity in data['External Entity'].unique():
            pseudo = hashlib.sha256(entity.encode()).hexdigest()[:10]
            data['External Entity'].replace(entity, pseudo, inplace=True)
            mapping[pseudo] = entity

        print("Pseudonymization complete.")
        return data, mapping
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None, None
    except KeyError as e:
        print(f"Data error: {e}")
        return None, None
    except Exception as e:
        print(f"Unexpected error in pseudonymization: {e}")
        return None, None

# Suggested Improvements:
# - Allow configuration of hash length for different security needs.
# - Implement a salt-based hashing mechanism to enhance security.
# - Store pseudonymization mappings securely to prevent accidental exposure.