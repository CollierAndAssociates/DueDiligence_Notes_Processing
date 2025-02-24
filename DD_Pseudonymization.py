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
    ✅ Pseudonymization complete.
"""

import pandas as pd
import hashlib
import sqlite3
from typing import Tuple, Dict, Optional


def pseudonymize(data: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, str]]]:
    """
    Pseudonymizes sensitive terms and the 'External Entity' column using SHA-256 hashing.

    Args:
        data (pd.DataFrame): The dataset containing sensitive terms and entity names.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[Dict[str, str]]]: 
            - The pseudonymized DataFrame.
            - A mapping dictionary for later reversal.

    Raises:
        sqlite3.Error: If there is an issue accessing the terms database.
        KeyError: If the 'External Entity' column is missing from the dataset.

    Example:
        >>> df, mapping = pseudonymize(df)
        ✅ Pseudonymization complete.
    """
    try:
        # 🛠️ Print initial dataset sample
        print("\n🔍 Initial Data Sample (Before Pseudonymization):")
        print(data.head())

        # 🔍 Check if DataFrame has missing values BEFORE processing
        print("\n🔍 Null Check BEFORE Pseudonymization:")
        print(data.isnull().sum())

        # Load stored terms from the database
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        c.execute("SELECT term FROM terms")
        terms_to_pseudo = [row[0] for row in c.fetchall()]
        conn.close()

        mapping: Dict[str, str] = {}

        print("\n🔍 Pseudonymizing Terms...")

        # Pseudonymize stored terms using SHA-256 hashin
        term_mapping = {}
        for term in terms_to_pseudo:
            if term and isinstance(term, (str, int, float)):  # Ensure it's a valid type
                term_str = str(term).strip()  # Convert to string and remove whitespace
                pseudo = hashlib.sha256(term_str.encode()).hexdigest()[:10]  # Generate 10-char hash
                
                # 🛠️ Debug: Print each term and its pseudonym
                print(f"🔹 {term_str} -> {pseudo}")

                #data.replace({term_str: pseudo}, inplace=True)  # Apply replacement correctly
                #mapping[pseudo] = term_str  # Store mapping for unpseudonymization

                term_mapping[term_str] = pseudo  # Store mapping for replacement
                
        # Apply pseudonym replacements
        data = data.replace(term_mapping)
        mapping.update(term_mapping)

        # 🛠️ Print dataset sample after term pseudonymization
        print("\n🔍 Data Sample After Term Pseudonymization:")
        print(data.head())

        # Ensure 'External Entity' column exists before processing
        if 'External Entity' not in data.columns:
            print("⚠️ Warning: 'External Entity' column missing. Skipping entity pseudonymization.")
        else:
            print("\n🔍 Pseudonymizing External Entities...")

            # Pseudonymize unique entities in 'External Entity' column
            entity_mapping = {}
            for entity in data['External Entity'].dropna().unique():
                if isinstance(entity, (str, int, float)):  # Ensure it's a valid type
                    entity_str = str(entity).strip()  # Convert to string
                    if entity_str:  # Avoid empty strings
                        pseudo = hashlib.sha256(entity_str.encode()).hexdigest()[:10]

                        # 🛠️ Debug: Print each entity and its pseudonym
                        print(f"🔹 {entity_str} -> {pseudo}")

                        entity_mapping[entity_str] = pseudo  # Store mapping

            # Apply the replacement correctly using `.replace({})`
            data['External Entity'] = data['External Entity'].replace(entity_mapping)
            mapping.update(entity_mapping)  # Add entity mappings to global mapping

        # 🛠️ Print dataset sample after full pseudonymization
        print("\n🔍 Final Data Sample (After Full Pseudonymization):")
        print(data.head())

        print("\n✅ Pseudonymization complete.")
        print("\n🔍 Data After Pseudonymization:")
        print(data.head())
        return data, mapping

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return None, None
    except KeyError as e:
        print(f"❌ Data error: {e}")
        return None, None
    except Exception as e:
        print(f"❌ Unexpected error in pseudonymization: {e}")
        return None, None


# Suggested Improvements:
# - Allow configuration of hash length for different security needs.
# - Implement a salt-based hashing mechanism to enhance security.
# - Store pseudonymization mappings securely to prevent accidental exposure.
