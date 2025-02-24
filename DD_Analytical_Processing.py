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
    - vaderSentiment: Provides improved sentiment analysis over TextBlob.

Project Scope:
    - Generates a statistical summary of interview data.
    - Conducts sentiment analysis at an overall and categorical level.
    - Outputs structured results that can be further processed for visualization.

Usage Example:
    >>> results = analyze_data(df)
    Analytical processing complete.
"""

import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from typing import Optional
import os

def analyze_data(data: pd.DataFrame, output_path: str) -> Optional[pd.DataFrame]:
    """
    Conducts data analysis including summarization and sentiment analysis,
    and saves the results as an Excel file with the original format plus additional metrics.

    Args:
        data (pd.DataFrame): The dataset containing interview notes and categorical information.
        output_path (str): Path to save the final Excel file.

    Returns:
        Optional[pd.DataFrame]: The processed DataFrame with added analysis columns.
    """
    try:
        if data is None or data.empty:
            print("❌ No data available for analysis. Skipping analytical processing.")
            return None

        # Initialize VADER sentiment analyzer
        analyzer = SentimentIntensityAnalyzer()

        # Compute sentiment scores
        data['Sentiment Score'] = data['Notes'].apply(lambda x: analyzer.polarity_scores(str(x))['compound'])

        # Classify sentiment
        def classify_sentiment(score):
            if score > 0.05:
                return "Positive"
            elif score < -0.05:
                return "Negative"
            else:
                return "Neutral"
        
        data['Sentiment Category'] = data['Sentiment Score'].apply(classify_sentiment)

        # Compute overall sentiment score
        sentiment_overall = data['Sentiment Score'].mean()
        
        # Add overall sentiment as a new column for all rows
        data['Sentiment Overall'] = sentiment_overall

        # Generate summary statistics for numerical columns
        summary = data.describe(include='all').transpose()
        summary.reset_index(inplace=True)
        summary.rename(columns={'index': 'Metric'}, inplace=True)

        # Save the final processed DataFrame and summary to an Excel file
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            data.to_excel(writer, sheet_name='Processed Data', index=False)
            summary.to_excel(writer, sheet_name='Summary Statistics', index=False)
        
        print(f"✅ Final output saved to: {output_path}")
        
        return data

    except KeyError as e:
        print(f"❌ Data error: Missing column {e}")
        return None
    except ValueError as e:
        print(f"❌ Value error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error in analytical processing: {e}")
        return None