# Term_Storage.py
import sqlite3

def store_terms():
    """Prompt user to input terms and phrases for pseudonymization and store securely in SQLite."""
    conn = sqlite3.connect('terms.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS terms (term TEXT UNIQUE)''')
    
    terms = input("Enter terms to pseudonymize separated by commas: ").split(',')
    for term in terms:
        c.execute("INSERT OR IGNORE INTO terms (term) VALUES (?)", (term.strip(),))
    
    conn.commit()
    conn.close()
    print("Terms stored successfully.")