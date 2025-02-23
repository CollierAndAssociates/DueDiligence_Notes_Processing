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

import json
import sqlite3
import hashlib
from typing import Any, Dict, Optional

def unpseudonymize(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Reverts pseudonymized data back to original terms using stored mappings.

    Args:
        data (Dict[str, Any]): The dictionary containing pseudonymized data.

    Returns:
        Optional[Dict[str, Any]]: The dictionary with unpseudonymized data, or None if an error occurs.

    Raises:
        sqlite3.Error: If there is an issue accessing the database.
        json.JSONDecodeError: If JSON parsing fails.
        Exception: Any unexpected error during processing.
    
    Example:
        >>> unpseudonymized_data = unpseudonymize(pseudonymized_data)
        Data unpseudonymized successfully.
    """
    try:
        # Establish connection to SQLite database
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        
        # Retrieve stored terms for mapping
        c.execute("SELECT term FROM terms")
        terms = [row[0] for row in c.fetchall()]
        conn.close()
        
        # Create a dictionary mapping hashed pseudonyms back to original terms
        mapping = {hashlib.sha256(term.encode()).hexdigest()[:10]: term for term in terms}
        
        # Convert the data dictionary to a JSON string for string-based replacement
        data_str = json.dumps(data)
        
        # Replace pseudonymized values with original terms
        for pseudo, original in mapping.items():
            data_str = data_str.replace(pseudo, original)
        
        # Convert back to dictionary format
        unpseudo_data = json.loads(data_str)
        
        print("Data unpseudonymized successfully.")
        return unpseudo_data
    
    except sqlite3.Error as e:
        print(f"Database error during unpseudonymization: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error during unpseudonymization: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error in unpseudonymization: {e}")
        return None