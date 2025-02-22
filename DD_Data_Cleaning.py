# DD_Data_Cleaning.py
import pandas as pd
import sqlite3
from transformers import pipeline

def clean_and_prepare(data):
    """Clean data and contextually fill missing 'Core System' and 'Core Process' values using LLM."""
    try:
        # Load core processes from database
        conn = sqlite3.connect('processes.db')
        c = conn.cursor()
        c.execute("SELECT process FROM core_processes")
        core_processes = [row[0] for row in c.fetchall()]
        conn.close()

        # Initialize NLP model for classification
        classifier = pipeline('zero-shot-classification', model='facebook/bart-large-mnli')

        # Contextually fill 'Core Process' and 'Core System'
        for idx, row in data.iterrows():
            notes = row['Notes']
            entity = row['Entity']

            # Fill 'Core Process' contextually if missing or unknown
            if pd.isna(row['Core Process']) or row['Core Process'] == 'Unknown':
                classification = classifier(notes, core_processes)
                data.at[idx, 'Core Process'] = classification['labels'][0]

            # Fill 'Core System' contextually if missing
            if pd.isna(row['Core System']):
                related_rows = data[(data['Entity'] == entity) & (data['Core System'].notna())]
                related_core_systems = related_rows[['Notes', 'Core System']].drop_duplicates()
                if not related_core_systems.empty:
                    candidate_systems = related_core_systems['Core System'].tolist()
                    classification = classifier(notes, candidate_systems)
                    data.at[idx, 'Core System'] = classification['labels'][0]

        print("Data cleaned and prepared with contextual LLM filling.")
        return data
    except Exception as e:
        print(f"Error cleaning data: {e}")
        return None