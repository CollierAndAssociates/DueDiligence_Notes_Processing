"""
Data_Ingestion.py

This module is responsible for loading data from an Excel spreadsheet into a Pandas DataFrame.
It ensures efficient data ingestion while handling potential errors gracefully.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - pandas: Used for data manipulation and reading Excel files.

Usage Example:
    >>> df = load_data('data.xlsx')
    >>> print(df.head())
"""

import pandas as pd

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load an Excel file into a Pandas DataFrame.

    Args:
        file_path (str): The full path to the Excel file.

    Returns:
        pd.DataFrame: The loaded data as a Pandas DataFrame.

    Raises:
        FileNotFoundError: If the file path does not exist.
        ValueError: If there is an issue reading the file.

    Example:
        >>> df = load_data("data/interview_notes.xlsx")
        >>> print(df.head())
    """
    try:
        # Attempt to load data from an Excel file using pandas
        data = pd.read_excel(file_path)
        print("Data loaded successfully.")
        return data
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return None
    except ValueError:
        print(f"Error: Unable to read the file - {file_path}")
        return None
    except Exception as e:
        print(f"Unexpected error loading data: {e}")
        return None
