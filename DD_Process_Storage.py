"""
DD_Process_Storage.py

This module allows users to input core business processes and securely store them in an SQLite database.
It ensures consistency in process definitions for classification and analysis in the larger system.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - sqlite3: Provides an interface to interact with an SQLite database.

Project Scope:
    - This script is part of a larger project for due diligence analysis.
    - The stored processes are used for contextual classification and automation in data analysis.

Usage Example:
    >>> store_processes()
    Enter core processes separated by commas: Estimation, Project Management, Procurement
    Processes stored successfully.

Configuration Steps:
    - Ensure `processes.db` exists in the project directory.
    - Run this script to populate core processes before data processing begins.
    - The stored processes will be referenced in the data cleaning phase.
"""

import sqlite3
from typing import List

def store_processes() -> None:
    """
    Prompts the user to input core business processes and stores them securely in an SQLite database.

    This function ensures that the `core_processes` table exists and inserts unique process names provided by the user.
    The stored processes are referenced later for classification and standardization across data analysis.

    Raises:
        sqlite3.Error: If there is an issue with database access.

    Example:
        >>> store_processes()
        Enter core processes separated by commas: Lead Management, Billing, Customer Service
        Processes stored successfully.
    """
    try:
        # Establish connection to SQLite database
        conn = sqlite3.connect('processes.db')
        c = conn.cursor()
        
        # Ensure the core_processes table exists
        c.execute('''CREATE TABLE IF NOT EXISTS core_processes (process TEXT UNIQUE)''')
        
        # Prompt user for process names
        processes: List[str] = input("Enter core processes separated by commas: ").split(',')
        
        # Insert unique process names into the database
        for process in processes:
            cleaned_process = process.strip()
            if cleaned_process:  # Ensure empty strings are not inserted
                c.execute("INSERT OR IGNORE INTO core_processes (process) VALUES (?)", (cleaned_process,))
        
        # Commit changes and close the database connection
        conn.commit()
        conn.close()
        print("Processes stored successfully.")
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Suggested Improvements:
# - Convert this script into a class-based implementation for better modularity.
# - Add command-line arguments to allow batch process insertion without user input.
# - Store database configuration settings in an environment variable or configuration file.