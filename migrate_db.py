
import sqlite3

DB_FILE = "users.db"

def migrate_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute("PRAGMA table_info(gmb_locations)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'source_url' not in columns:
            print("Adding source_url column to gmb_locations...")
            cursor.execute("ALTER TABLE gmb_locations ADD COLUMN source_url TEXT")
            conn.commit()
            print("Migration successful.")
        else:
            print("Column source_url already exists.")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    migrate_db()
