# Unpseudonymization.py
import json
import sqlite3

def unpseudonymize(data):
    """Revert pseudonymized data back to original terms using stored terms."""
    try:
        conn = sqlite3.connect('terms.db')
        c = conn.cursor()
        c.execute("SELECT term FROM terms")
        terms = [row[0] for row in c.fetchall()]
        conn.close()
        
        mapping = {hashlib.sha256(term.encode()).hexdigest()[:10]: term for term in terms}

        data_str = json.dumps(data)
        for pseudo, original in mapping.items():
            data_str = data_str.replace(pseudo, original)
        
        unpseudo_data = json.loads(data_str)
        print("Data unpseudonymized successfully.")
        return unpseudo_data
    except Exception as e:
        print(f"Error in unpseudonymization: {e}")
        return None