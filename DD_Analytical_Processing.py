"""
DD_Analytical_Processing.py

This module conducts data analysis, including summarization and sentiment analysis,
on textual data collected from interviews. It uses NLP techniques to derive insights
from qualitative data.

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]

Dependencies:
    - pandas: Used for data manipulation and aggregation.
    - textblob: Provides NLP sentiment analysis capabilities.

Project Scope:
    - Generates a statistical summary of interview data.
    - Conducts sentiment analysis at an overall and categorical level.
    - Outputs structured results that can be further processed for visualization.

Usage Example:
    >>> results = analyze_data(df)
    Analytical processing complete.
"""

import pandas as pd
from textblob import TextBlob
from typing import Dict, Any, Optional

def analyze_data(data: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Conducts data analysis including summarization and sentiment analysis.

    Args:
        data (pd.DataFrame): The dataset containing interview notes and categorical information.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing overall summaries and sentiment analysis results.
        Returns None in case of an error.
    """
    results = {}

    try:
        if data is None or data.empty:
            print("❌ No data available for analysis. Skipping analytical processing.")
            return None

        print("\n✅ Analyzing Data...")
        print("Columns Available:", data.columns.tolist())

        # Ensure required columns exist
        required_columns = ['Notes']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            print(f"❌ Error: Missing columns {missing_columns}. Skipping analysis.")
            return None

        # Generate overall summary statistics
        results['overall_summary'] = data.describe(include='all').to_dict()

        # Sentiment Analysis: Assign a sentiment score to each note entry
        data['Sentiment'] = data['Notes'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)

        # Compute average sentiment across the dataset
        results['sentiment_overall'] = data['Sentiment'].mean()

        # Compute sentiment scores grouped by interview categories (if available)
        if 'Interview Category' in data.columns:
            results['sentiment_by_category'] = data.groupby('Interview Category')['Sentiment'].mean().to_dict()
        else:
            print("⚠️ Warning: 'Interview Category' column missing. Skipping grouped sentiment analysis.")

        print("\n✅ Analytical processing complete.")
        return results

    except Exception as e:
        print(f"❌ Unexpected error in analytical processing: {e}")
        return None