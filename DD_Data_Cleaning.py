"""
DD_Data_Cleaning.py

This module is responsible for loading data from an Excel spreadsheet into a Pandas DataFrame.
It ensures efficient data ingestion while handling potential errors gracefully.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]
"""
import pandas as pd
import sqlite3
import torch
from transformers import pipeline
from joblib import Parallel, delayed
from datasets import Dataset
from tqdm import tqdm
import time

def clean_and_prepare(data: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and fills missing 'Core System' and 'Core Process' values using NLP-based classification.

    Args:
        data (pd.DataFrame): The dataset containing interview notes and entity information.

    Returns:
        pd.DataFrame: The processed DataFrame with missing values filled.
    """
    try:
        # Load core processes from database
        conn = sqlite3.connect('processes.db')
        c = conn.cursor()
        c.execute("SELECT process FROM core_processes")
        core_processes = [row[0] for row in c.fetchall()]
        conn.close()

        # Check for GPU availability
        device = 0 if torch.cuda.is_available() else -1
        print(f"\n🔍 Device set to use {'GPU' if device == 0 else 'CPU'}")

        # Initialize NLP model for classification with batch support
        classifier = pipeline('zero-shot-classification', model='facebook/bart-large-mnli', device=device)

        print("\n🔍 Data Before Cleaning:")
        print(data.head())

        # Ensure correct dtype for 'Core Process'
        data['Core Process'] = data['Core Process'].astype(str)

        # Debug: Show Core Process counts before NLP classification
        print("\n🔍 Core Process Count BEFORE NLP:")
        print(data['Core Process'].value_counts(dropna=False))

        # Group notes by entity
        entity_groups = data.groupby('Entity')['Notes'].apply(list).to_dict()

        # Prepare dataset for Hugging Face's `datasets.Dataset`
        dataset_dict = {
            "entity": [],
            "notes": [],
        }

        for entity, notes in entity_groups.items():
            dataset_dict["entity"].append(entity)
            dataset_dict["notes"].append(" ".join(notes))  # Merge all notes for an entity

        hf_dataset = Dataset.from_dict(dataset_dict)

        # Debug: Show sample dataset to be processed
        print("\n🔍 Sample Data Sent for NLP Classification:")
        for i in range(min(3, len(hf_dataset))):  # Print first 3 samples
            print(f"Entity: {hf_dataset['entity'][i]} | Notes: {hf_dataset['notes'][i][:200]}...")

        # Process Core Processes in batch
        batch_size = 4
        start_time = time.time()

        results = classifier(hf_dataset['notes'], core_processes, batch_size=batch_size)

        # Debugging classifier output
        print("\n🔍 Sample Classifier Output:")
        for i in range(min(3, len(results))):  # Print first 3 results
            print(f"Entity: {hf_dataset['entity'][i]}")
            print(f"Prediction: {results[i]['labels'][0]} | Scores: {results[i]['scores'][0]:.4f}\n")

        # Assign back the most likely classification
        core_process_mapping = {entity: res['labels'][0] for entity, res in zip(hf_dataset['entity'], results)}

        # Apply classification back to each row
        data['Core Process'] = data['Entity'].map(core_process_mapping)

        # Ensure no missing values in 'Core Process'
        data['Core Process'].fillna('n/a', inplace=True)

        # Debugging output after classification
        print("\n🔍 Core Process Count AFTER NLP:")
        print(data['Core Process'].value_counts(dropna=False))

        # Ensure 'Core Process' isn't entirely NaN after NLP processing
        if data['Core Process'].isnull().sum() == len(data):
            print("❌ Error: Core Process classification failed. No values assigned.")
            return None

        # Estimate processing time
        total_time = time.time() - start_time
        print(f"🔥 Total processing time: {total_time:.2f} sec (~{total_time/len(hf_dataset):.2f} sec per entity)")

        print("✅ Data cleaning complete!")
        
        # Debug: Sample data after processing
        print("\n🔍 Data Sample After Cleaning:")
        print(data.head())
        
        return data
    
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return None
    except KeyError as e:
        print(f"❌ Data error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None