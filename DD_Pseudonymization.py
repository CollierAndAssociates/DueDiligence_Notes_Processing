# Pseudonymization.py
import pandas as pd
import hashlib
import sqlite3

def pseudonymize(data):
    """Pseudonymize terms and 'External Entity' column using stored terms."""
    try:
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        c.execute("SELECT term FROM terms")
        terms_to_pseudo = [row[0] for row in c.fetchall()]
        conn.close()
        
        mapping = {}

        for term in terms_to_pseudo:
            pseudo = hashlib.sha256(term.strip().encode()).hexdigest()[:10]
            data.replace(term.strip(), pseudo, inplace=True)
            mapping[pseudo] = term.strip()

        for entity in data['External Entity'].unique():
            pseudo = hashlib.sha256(entity.encode()).hexdigest()[:10]
            data['External Entity'].replace(entity, pseudo, inplace=True)
            mapping[pseudo] = entity

        print("Pseudonymization complete.")
        return data, mapping
    except Exception as e:
        print(f"Error in pseudonymization: {e}")
        return None, None