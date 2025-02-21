# Data_Ingestion.py
import pandas as pd

def load_data(file_path):
    """Load Excel data into a pandas DataFrame."""
    try:
        data = pd.read_excel(file_path)
        print("Data loaded successfully.")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None