# Analytical_Processing.py
import pandas as pd
from textblob import TextBlob

def analyze_data(data):
    """Conduct analysis including summaries and sentiment analysis."""
    results = {}
    try:
        # Overall Summary
        results['overall_summary'] = data.describe(include='all').to_dict()

        # Sentiment Analysis
        data['Sentiment'] = data['Notes'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
        results['sentiment_overall'] = data['Sentiment'].mean()
        results['sentiment_by_category'] = data.groupby('Interview Category')['Sentiment'].mean().to_dict()

        print("Analytical processing complete.")
        return results
    except Exception as e:
        print(f"Error in analytical processing: {e}")
        return None