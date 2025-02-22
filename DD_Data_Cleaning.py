# DD_Data_Cleaning.py
import pandas as pd
import sqlite3
import torch
from transformers import pipeline
from joblib import Parallel, delayed

def clean_and_prepare(data):
    """Clean data and contextually fill missing 'Core System' and 'Core Process' values using LLM."""
    try:
        # Load core processes from database
        conn = sqlite3.connect('processes.db')
        c = conn.cursor()
        c.execute("SELECT process FROM core_processes")
        core_processes = [row[0] for row in c.fetchall()]
        conn.close()

        # Check for GPU availability
        device = 0 if torch.cuda.is_available() else -1
        print(f"Device set to use {'GPU' if device == 0 else 'CPU'}")

        # Initialize NLP model for classification
        classifier = pipeline('zero-shot-classification', model='facebook/bart-large-mnli', device=device)

        # Ensure correct dtype for 'Core Process'
        data['Core Process'] = data['Core Process'].astype(str)

        # Collect rows needing classification
        process_indices = []
        process_texts = []

        system_indices = []
        system_texts = []
        system_candidates = []

        for idx, row in data.iterrows():
            notes = row['Notes']
            entity = row['Entity']

            # For Core Process
            if pd.isna(row['Core Process']) or row['Core Process'] == 'Unknown':
                process_indices.append(idx)
                process_texts.append(notes)

            # For Core System
            if pd.isna(row['Core System']):
                related_rows = data[(data['Entity'] == entity) & (data['Core System'].notna())]
                related_core_systems = related_rows[['Notes', 'Core System']].drop_duplicates()
                if not related_core_systems.empty:
                    candidate_systems = related_core_systems['Core System'].tolist()
                    system_indices.append(idx)
                    system_texts.append(notes)
                    system_candidates.append(candidate_systems)

        # Batch classify Core Process
        if process_texts:
            process_results = classifier(process_texts, core_processes, batch_size=8)
            for idx, result in zip(process_indices, process_results):
                data.at[idx, 'Core Process'] = result['labels'][0]

        # Parallel classify Core System
        def classify_system(notes, candidates):
            result = classifier(notes, candidates)
            return result['labels'][0]

        if system_texts:
            system_results = Parallel(n_jobs=4)(delayed(classify_system)(notes, candidates) 
                                                for notes, candidates in zip(system_texts, system_candidates))
            
            for idx, result in zip(system_indices, system_results):
                data.at[idx, 'Core System'] = result

        print("Data cleaned and prepared with contextual LLM filling.")
        return data
    except Exception as e:
        print(f"Error cleaning data: {e}")
        return None
