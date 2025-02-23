"""
DD_Term_Storage.py

This module allows users to input terms and phrases that need to be pseudonymized and securely stores them in an SQLite database.
It ensures that pseudonymization terms are maintained consistently for use across the system.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - sqlite3: Provides an interface to interact with an SQLite database.

Usage Example:
    >>> store_terms()
    Enter terms to pseudonymize separated by commas: John Doe, Acme Corp
    Terms stored successfully.
"""

import sqlite3

def store_terms() -> None:
    """
    Prompt the user to input terms and phrases for pseudonymization and store them securely in an SQLite database.

    This function creates the `terms` table if it does not exist and inserts user-provided terms while ensuring uniqueness.

    Raises:
        sqlite3.Error: If there is an issue with the database connection or query execution.

    Example:
        >>> store_terms()
        Enter terms to pseudonymize separated by commas: Alice, Bob, XYZ Corp
        Terms stored successfully.
    """
    try:
        # Establish connection to SQLite database
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        
        # Create the terms table if it does not exist
        c.execute('''CREATE TABLE IF NOT EXISTS terms (term TEXT UNIQUE)''')
        
        # Prompt user for terms to pseudonymize
        terms = input("Enter terms to pseudonymize separated by commas: ").split(',')
        
        # Insert terms into the database, ensuring uniqueness
        for term in terms:
            c.execute("INSERT OR IGNORE INTO terms (term) VALUES (?)", (term.strip(),))
        
        # Commit changes and close the database connection
        conn.commit()
        conn.close()
        print("Terms stored successfully.")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
