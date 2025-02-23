"""
DD_Data_Cleaning.py

This module processes raw data by filling in missing 'Core System' and 'Core Process' values
using Natural Language Processing (NLP) techniques and contextual analysis.
It integrates GPU acceleration when available for optimized performance.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - pandas: Used for data manipulation.
    - sqlite3: Handles local storage of predefined core processes.
    - torch: Enables GPU acceleration for NLP tasks.
    - transformers: Provides NLP models for classification.
    - joblib: Facilitates parallel processing to speed up classification.

Project Scope:
    - This script is a core component of the due diligence analysis framework.
    - It automates the classification of processes and systems based on interview notes.
    - It improves data standardization by leveraging predefined business process classifications.

Usage Example:
    >>> df = clean_and_prepare(df)
    Device set to use GPU
    Data cleaned and prepared with contextual LLM filling.

Configuration Steps:
    - Ensure `processes.db` contains predefined core processes.
    - Run this script as part of the data processing pipeline.
    - Requires a working GPU setup for optimal NLP model performance.
"""

import pandas as pd
import sqlite3
import torch
from transformers import pipeline
from joblib import Parallel, delayed
from typing import Optional

def clean_and_prepare(data: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Cleans and fills missing 'Core System' and 'Core Process' values using NLP-based classification.

    Args:
        data (pd.DataFrame): The dataset containing interview notes and entity information.

    Returns:
        Optional[pd.DataFrame]: The processed DataFrame with missing values filled, or None on failure.

    Raises:
        sqlite3.Error: If there is an issue accessing the process database.
        KeyError: If expected columns are missing from the dataset.

    Example:
        >>> df = clean_and_prepare(df)
        Device set to use GPU
        Data cleaned and prepared with contextual LLM filling.
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
        print(f"Device set to use {'GPU' if device == 0 else 'CPU'}")

        # Initialize NLP model for classification
        classifier = pipeline('zero-shot-classification', model='facebook/bart-large-mnli', device=device)

        # Ensure correct dtype for 'Core Process'
        data['Core Process'] = data['Core Process'].astype(str)

        # Collect rows needing classification
        process_indices, process_texts = [], []
        system_indices, system_texts, system_candidates = [], [], []

        for idx, row in data.iterrows():
            notes, entity = row['Notes'], row['Entity']

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
        def classify_system(notes: str, candidates: list) -> str:
            """Classifies Core System based on contextual interview notes."""
            result = classifier(notes, candidates)
            return result['labels'][0]

        if system_texts:
            system_results = Parallel(n_jobs=4)(delayed(classify_system)(notes, candidates) 
                                                for notes, candidates in zip(system_texts, system_candidates))
            
            for idx, result in zip(system_indices, system_results):
                data.at[idx, 'Core System'] = result

        print("Data cleaned and prepared with contextual LLM filling.")
        return data
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    except KeyError as e:
        print(f"Data error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

# Suggested Improvements:
# - Implement a caching mechanism to reduce repeated NLP model calls.
# - Optimize batch processing for improved speed in large datasets.
# - Store classification model and database configurations in an external settings file.
