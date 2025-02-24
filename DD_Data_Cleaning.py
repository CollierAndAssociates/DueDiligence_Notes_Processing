"""
DD_Data_Cleaning.py

This module processes raw data by filling in missing 'Core System' and 'Core Process' values
using Natural Language Processing (NLP) techniques and contextual analysis.
It integrates GPU acceleration when available for optimized performance.

Company: C Tech Solutions, LLC (dba Collier & Associates)
Author: Andrew Collier
Date: 2025-02-23

Dependencies:
    - pandas: Used for data manipulation.
    - sqlite3: Handles local storage of predefined core processes.
    - torch: Enables GPU acceleration for NLP tasks.
    - transformers: Provides NLP models for classification.
    - joblib: Facilitates parallel processing to speed up classification.
    - datasets: Optimizes batch processing.

Project Scope:
    - Automates the classification of processes and systems based on interview notes.
    - Ensures data standardization using predefined business process classifications.
    - Leverages GPU-accelerated NLP for fast and accurate classification.

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
from typing import Optional, List, Dict
from datasets import Dataset


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

        # Dictionary to store entity-wide notes
        entity_notes: Dict[str, str] = {}

        # Collect all notes for each entity
        for entity in data['Entity'].unique():
            all_notes = " ".join(data[data['Entity'] == entity]['Notes'].dropna().astype(str))
            entity_notes[entity] = all_notes

        # Process Core Process classification
        process_indices = []
        process_texts = []

        for idx, row in data.iterrows():
            entity = row['Entity']
            row_notes = row['Notes']

            if pd.isna(row['Core Process']) or row['Core Process'] == 'Unknown':
                full_context_notes = f"Context: {entity_notes.get(entity, '')} | Row Notes: {row_notes}"
                process_indices.append(idx)
                process_texts.append(full_context_notes)

        # Batch classify Core Process using entity-wide context
        if process_texts:
            batch_size = 8  # Adjust based on GPU memory
            results = classifier(process_texts, core_processes, batch_size=batch_size)

            for idx, result in zip(process_indices, results):
                data.at[idx, 'Core Process'] = result['labels'][0]

        # Process Core System classification
        system_indices, system_texts, system_candidates = [], [], []

        for idx, row in data.iterrows():
            entity = row['Entity']
            row_notes = row['Notes']

            if pd.isna(row['Core System']):
                related_rows = data[(data['Entity'] == entity) & (data['Core System'].notna())]
                related_core_systems = related_rows[['Notes', 'Core System']].drop_duplicates()

                if not related_core_systems.empty:
                    candidate_systems = related_core_systems['Core System'].tolist()
                    full_context_notes = f"Context: {entity_notes.get(entity, '')} | Row Notes: {row_notes}"
                    system_indices.append(idx)
                    system_texts.append(full_context_notes)
                    system_candidates.append(candidate_systems)

        # Parallel classify Core System using entity-wide context
        def classify_system(notes: str, candidates: List[str]) -> str:
            """Classifies Core System based on contextual interview notes."""
            result = classifier(notes, candidates)
            return result['labels'][0]

        if system_texts:
            system_results = Parallel(n_jobs=2, backend="loky")(
                delayed(classify_system)(notes, candidates)
                for notes, candidates in zip(system_texts, system_candidates)
            )

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
