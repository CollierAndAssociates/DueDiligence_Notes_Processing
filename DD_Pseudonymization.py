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
    Pseudonymizes sensitive terms and the 'External Entity' column using SHA-256 hashing and stores the mapping.

    Args:
        data (pd.DataFrame): The dataset containing sensitive terms and entity names.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[Dict[str, str]]]: 
            - The pseudonymized DataFrame.
            - A mapping dictionary for later reversal.

    Raises:
        sqlite3.Error: If there is an issue accessing the terms database.
        KeyError: If the 'External Entity' column is missing from the dataset.
    """
    try:
        print("\n🔍 Initial Data Sample (Before Pseudonymization):")
        print(data.head())

        # Connect to SQLite and ensure mapping table exists
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS pseudonym_mapping (
                original TEXT PRIMARY KEY,
                pseudonym TEXT UNIQUE
            )
        """)
        conn.commit()

        # Load stored terms from the database
        c.execute("SELECT term FROM terms")
        terms_to_pseudo = [row[0] for row in c.fetchall()]

        mapping: Dict[str, str] = {}

        print("\n🔍 Pseudonymizing Terms...")

        # Pseudonymize stored terms using SHA-256 hashing
        term_mapping = {}
        for term in terms_to_pseudo:
            if term and isinstance(term, (str, int, float)):  # Ensure it's a valid type
                term_str = str(term).strip()  # Convert to string and remove whitespace
                pseudo = hashlib.sha256(term_str.encode()).hexdigest()[:10]  # Generate 10-char hash
                
                print(f"🔹 {term_str} -> {pseudo}")  # Debugging

                term_mapping[term_str] = pseudo  # Store mapping
                mapping[pseudo] = term_str  # For reverse lookup

                # Store mapping in the database
                c.execute("INSERT OR IGNORE INTO pseudonym_mapping (original, pseudonym) VALUES (?, ?)", (term_str, pseudo))

        # Apply pseudonym replacements
        data = data.replace(term_mapping)

        # Pseudonymize External Entities
        if 'External Entity' in data.columns:
            print("\n🔍 Pseudonymizing External Entities...")
            entity_mapping = {}
            for entity in data['External Entity'].dropna().unique():
                entity_str = str(entity).strip()
                if entity_str:
                    pseudo = hashlib.sha256(entity_str.encode()).hexdigest()[:10]
                    print(f"🔹 {entity_str} -> {pseudo}")  # Debugging

                    entity_mapping[entity_str] = pseudo
                    mapping[pseudo] = entity_str  # For reverse lookup

                    # Store mapping in the database
                    c.execute("INSERT OR IGNORE INTO pseudonym_mapping (original, pseudonym) VALUES (?, ?)", (entity_str, pseudo))

            data['External Entity'] = data['External Entity'].replace(entity_mapping)

        conn.commit()  # Ensure changes are saved
        conn.close()  # Close connection

        print("\n✅ Pseudonymization complete.")
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
