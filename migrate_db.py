import sqlite3
import os
from src.gmb_core.config import config

DB_FILE = config.DATABASE_FILE

def migrate():
    print(f"Migrating database: {DB_FILE}")
    if not os.path.exists(DB_FILE):
        print("Database file not found!")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        # Check columns in gmb_grid_scans
        cursor.execute("PRAGMA table_info(gmb_grid_scans)")
        columns = [row[1] for row in cursor.fetchall()]
        
        print(f"Current columns: {columns}")

        if 'target_place_id' not in columns:
            print("Adding 'target_place_id' column...")
            cursor.execute("ALTER TABLE gmb_grid_scans ADD COLUMN target_place_id TEXT")
            print("Done.")
        else:
            print("'target_place_id' already exists.")

        if 'target_business' not in columns:
            print("Adding 'target_business' column...")
            cursor.execute("ALTER TABLE gmb_grid_scans ADD COLUMN target_business TEXT")
            print("Done.")
        else:
            print("'target_business' already exists.")
            
        conn.commit()
        print("Migration completed successfully.")

    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
