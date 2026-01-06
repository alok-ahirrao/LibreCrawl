import sqlite3
import os

# The actual database file used by the app
db_path = 'users.db'

print(f"Checking database: {db_path}")

if not os.path.exists(db_path):
    print("Database file does not exist!")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# List all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cursor.fetchall()]
print(f"Existing tables: {tables}")

# Check if serp_searches exists
if 'serp_searches' in tables:
    print("serp_searches table exists!")
    cursor.execute("SELECT COUNT(*) FROM serp_searches")
    count = cursor.fetchone()[0]
    print(f"Records in serp_searches: {count}")
else:
    print("serp_searches table does NOT exist. Creating it...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS serp_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            location TEXT,
            lat REAL,
            lng REAL,
            device TEXT DEFAULT 'desktop',
            language TEXT DEFAULT 'en',
            depth INTEGER DEFAULT 10,
            organic_count INTEGER DEFAULT 0,
            local_pack_count INTEGER DEFAULT 0,
            hotel_count INTEGER DEFAULT 0,
            shopping_count INTEGER DEFAULT 0,
            target_rank INTEGER,
            target_url TEXT,
            results_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_serp_searches_keyword 
        ON serp_searches(keyword, created_at DESC)
    ''')
    conn.commit()
    print("serp_searches table created successfully!")

conn.close()
