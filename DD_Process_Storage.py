import sqlite3

def store_processes():
    """Prompt user to input processes and store securely in SQLite."""
    conn = sqlite3.connect('processes.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS core_processes (process TEXT UNIQUE)''')

    processes = input("Enter core processes separated by commas: ").split(',')
    for process in processes:
        c.execute("INSERT OR IGNORE INTO core_processes (process) VALUES (?)", (process.strip(),))

    conn.commit()
    conn.close()
    print("Processes stored successfully.")