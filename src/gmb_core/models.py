"""
GMB Core Database Models
Independent SQLite implementation to maintain isolation from CrawlX
"""
import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime
from .config import config

# Database file location
DB_FILE = config.DATABASE_FILE


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def init_gmb_tables():
    """Initialize GMB-specific tables."""
    with get_db() as conn:
        cursor = conn.cursor()

        # 1. GMB Accounts (OAuth Tokens)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                email TEXT NOT NULL,
                access_token TEXT,
                refresh_token TEXT,
                token_expiry TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        ''')
        
        # Index for quick lookup
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_accounts_user 
            ON gmb_accounts(user_id, email)
        ''')

        # 2. GMB Locations (The official business profiles managed)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                google_location_id TEXT NOT NULL,
                google_account_id TEXT,
                location_name TEXT,
                address_lines TEXT, 
                locality TEXT,
                region TEXT,
                postal_code TEXT,
                country TEXT,
                lat REAL,
                lng REAL,
                primary_category TEXT,
                additional_categories TEXT,
                website_url TEXT,
                phone_number TEXT,
                
                -- Cached Stats
                total_reviews INTEGER DEFAULT 0,
                rating REAL DEFAULT 0.0,
                photo_count INTEGER DEFAULT 0,
                
                last_synced_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (account_id) REFERENCES gmb_accounts(id) ON DELETE CASCADE,
                UNIQUE(google_location_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_locations_account 
            ON gmb_locations(account_id)
        ''')

        # 3. GMB Reviews
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id INTEGER,
                google_review_id TEXT NOT NULL,
                reviewer_name TEXT,
                reviewer_photo_url TEXT,
                star_rating INTEGER CHECK (star_rating BETWEEN 1 AND 5),
                comment TEXT,
                review_reply TEXT,
                reply_time TIMESTAMP,
                create_time TIMESTAMP,
                update_time TIMESTAMP,
                
                -- Sentiment Analysis (populated by AI later)
                sentiment_score REAL,
                sentiment_keywords TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (location_id) REFERENCES gmb_locations(id) ON DELETE CASCADE,
                UNIQUE(google_review_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_reviews_location 
            ON gmb_reviews(location_id, create_time DESC)
        ''')

        # 4. Grid Scans (The "God Mode" usage events)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_grid_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id INTEGER,
                keyword TEXT NOT NULL,
                target_business TEXT,
                target_place_id TEXT,
                
                center_lat REAL,
                center_lng REAL,
                radius_meters INTEGER,
                grid_size INTEGER,
                
                status TEXT DEFAULT 'pending',
                total_points INTEGER,
                completed_points INTEGER DEFAULT 0,
                
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                
                FOREIGN KEY (location_id) REFERENCES gmb_locations(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_scans_location 
            ON gmb_grid_scans(location_id, started_at DESC)
        ''')

        # 5. Grid Results (The individual pins)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_grid_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER,
                point_index INTEGER,
                
                lat REAL,
                lng REAL,
                
                target_rank INTEGER,
                target_found BOOLEAN DEFAULT 0,
                
                top_results TEXT,
                
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error TEXT,
                
                FOREIGN KEY (scan_id) REFERENCES gmb_grid_scans(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_results_scan 
            ON gmb_grid_results(scan_id)
        ''')

        # 6. Competitor Profiles
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_competitors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                place_id TEXT UNIQUE,
                name TEXT,
                primary_category TEXT,
                additional_categories TEXT,
                rating REAL,
                review_count INTEGER,
                photo_count INTEGER,
                attributes TEXT,
                
                last_scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_competitors_place 
            ON gmb_competitors(place_id)
        ''')

        # 7. API Response Cache
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_api_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key TEXT UNIQUE NOT NULL,
                response_data TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_cache_key 
            ON gmb_api_cache(cache_key, expires_at)
        ''')

        # 8. SERP Cache (for crawler results)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_serp_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                results_json TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(keyword, lat, lng)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_serp_lookup 
            ON gmb_serp_cache(keyword, lat, lng, expires_at)
        ''')

        # 9. Competitive Analyses
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS competitive_analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_place_id TEXT,
                keyword TEXT,
                competitor_ids TEXT,
                deficits TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        try:
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_competitive_analyses_user 
                ON competitive_analyses(user_place_id)
            ''')
        except Exception as e:
            print(f"Note: Could not create index idx_competitive_analyses_user: {e}")

        # 10. GMB Categories Reference
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id TEXT UNIQUE,
                display_name TEXT,
                parent_category TEXT,
                is_primary_eligible INTEGER DEFAULT 1
            )
        ''')
        
        try:
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_gmb_categories_parent 
                ON gmb_categories(parent_category)
            ''')
        except Exception as e:
            print(f"Note: Could not create index idx_gmb_categories_parent: {e}")

        # 11. SERP Search History
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

        print("GMB Core tables initialized successfully")
        
        # Run migrations for existing databases
        _run_migrations(conn)


def _run_migrations(conn):
    """Add missing columns to existing tables (safe migrations)."""
    cursor = conn.cursor()
    
    # Migration: Add total_points to gmb_grid_scans if missing
    cursor.execute("PRAGMA table_info(gmb_grid_scans)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'total_points' not in columns:
        print("Migration: Adding total_points column to gmb_grid_scans")
        cursor.execute('ALTER TABLE gmb_grid_scans ADD COLUMN total_points INTEGER DEFAULT 25')
    
    if 'completed_points' not in columns:
        print("Migration: Adding completed_points column to gmb_grid_scans")
        cursor.execute('ALTER TABLE gmb_grid_scans ADD COLUMN completed_points INTEGER DEFAULT 0')
    
    if 'radius_meters' not in columns:
        print("Migration: Adding radius_meters column to gmb_grid_scans")
        cursor.execute('ALTER TABLE gmb_grid_scans ADD COLUMN radius_meters INTEGER DEFAULT 3000')
    
    if 'target_business' not in columns:
        print("Migration: Adding target_business column to gmb_grid_scans")
        cursor.execute('ALTER TABLE gmb_grid_scans ADD COLUMN target_business TEXT')
    
    # Migration: Add new columns to gmb_competitors
    cursor.execute("PRAGMA table_info(gmb_competitors)")
    comp_columns = [col[1] for col in cursor.fetchall()]
    
    if 'hours' not in comp_columns:
        print("Migration: Adding hours column to gmb_competitors")
        cursor.execute('ALTER TABLE gmb_competitors ADD COLUMN hours TEXT')
    
    if 'services' not in comp_columns:
        print("Migration: Adding services column to gmb_competitors")
        cursor.execute('ALTER TABLE gmb_competitors ADD COLUMN services TEXT')
    
    if 'post_count' not in comp_columns:
        print("Migration: Adding post_count column to gmb_competitors")
        cursor.execute('ALTER TABLE gmb_competitors ADD COLUMN post_count INTEGER DEFAULT 0')
    
    if 'q_and_a_count' not in comp_columns:
        print("Migration: Adding q_and_a_count column to gmb_competitors")
        cursor.execute('ALTER TABLE gmb_competitors ADD COLUMN q_and_a_count INTEGER DEFAULT 0')
    
    # Migration: Add ai_overview_present to serp_searches
    cursor.execute("PRAGMA table_info(serp_searches)")
    serp_columns = [col[1] for col in cursor.fetchall()]
    
    if 'ai_overview_present' not in serp_columns:
        print("Migration: Adding ai_overview_present column to serp_searches")
        cursor.execute('ALTER TABLE serp_searches ADD COLUMN ai_overview_present BOOLEAN DEFAULT 0')

    # Migration: Fix competitive_analyses table if it exists with wrong schema
    cursor.execute("PRAGMA table_info(competitive_analyses)")
    ca_columns = [col[1] for col in cursor.fetchall()]
    
    if ca_columns and 'user_place_id' not in ca_columns:
        print("Migration: Recreating competitive_analyses table with correct schema")
        cursor.execute('DROP TABLE IF EXISTS competitive_analyses')
        cursor.execute('''
            CREATE TABLE competitive_analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_place_id TEXT,
                keyword TEXT,
                competitor_ids TEXT,
                deficits TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_competitive_analyses_user 
            ON competitive_analyses(user_place_id)
        ''')


# ==================== Helper Functions ====================

def save_location(account_id: int, location_data: dict) -> int:
    """Save or update a location from GBP API response."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        google_location_id = location_data.get('name', '')
        
        # Extract address
        address = location_data.get('storefrontAddress', {})
        latlng = location_data.get('latlng', {})
        
        # Extract categories
        categories = location_data.get('categories', {})
        primary_cat = categories.get('primaryCategory', {}).get('displayName', '')
        additional_cats = json.dumps([
            c.get('displayName', '') 
            for c in categories.get('additionalCategories', [])
        ])
        
        cursor.execute('''
            INSERT INTO gmb_locations (
                account_id, google_location_id, location_name,
                address_lines, locality, region, postal_code, country,
                lat, lng, primary_category, additional_categories,
                website_url, phone_number, last_synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(google_location_id) DO UPDATE SET
                location_name = excluded.location_name,
                address_lines = excluded.address_lines,
                locality = excluded.locality,
                region = excluded.region,
                postal_code = excluded.postal_code,
                lat = excluded.lat,
                lng = excluded.lng,
                primary_category = excluded.primary_category,
                additional_categories = excluded.additional_categories,
                website_url = excluded.website_url,
                phone_number = excluded.phone_number,
                last_synced_at = CURRENT_TIMESTAMP
        ''', (
            account_id,
            google_location_id,
            location_data.get('title', ''),
            '\n'.join(address.get('addressLines', [])),
            address.get('locality', ''),
            address.get('administrativeArea', ''),
            address.get('postalCode', ''),
            address.get('regionCode', ''),
            latlng.get('latitude'),
            latlng.get('longitude'),
            primary_cat,
            additional_cats,
            location_data.get('websiteUri', ''),
            location_data.get('phoneNumbers', {}).get('primaryPhone', '')
        ))
        
        cursor.execute('SELECT id FROM gmb_locations WHERE google_location_id = ?', (google_location_id,))
        return cursor.fetchone()['id']


def save_review(location_id: int, review_data: dict) -> int:
    """Save or update a review from GBP API response."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        google_review_id = review_data.get('name', '').split('/')[-1]
        reviewer = review_data.get('reviewer', {})
        
        cursor.execute('''
            INSERT INTO gmb_reviews (
                location_id, google_review_id, reviewer_name, reviewer_photo_url,
                star_rating, comment, review_reply, reply_time,
                create_time, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(google_review_id) DO UPDATE SET
                star_rating = excluded.star_rating,
                comment = excluded.comment,
                review_reply = excluded.review_reply,
                reply_time = excluded.reply_time,
                update_time = excluded.update_time
        ''', (
            location_id,
            google_review_id,
            reviewer.get('displayName', ''),
            reviewer.get('profilePhotoUrl', ''),
            int(review_data.get('starRating', '0').replace('STAR_RATING_', '').replace('ONE', '1').replace('TWO', '2').replace('THREE', '3').replace('FOUR', '4').replace('FIVE', '5') or 0),
            review_data.get('comment', ''),
            review_data.get('reviewReply', {}).get('comment', ''),
            review_data.get('reviewReply', {}).get('updateTime'),
            review_data.get('createTime'),
            review_data.get('updateTime')
        ))
        
        cursor.execute('SELECT id FROM gmb_reviews WHERE google_review_id = ?', (google_review_id,))
        row = cursor.fetchone()
        return row['id'] if row else 0


def get_cached_serp(keyword: str, lat: float, lng: float) -> dict:
    """Get cached SERP results if not expired."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT results_json FROM gmb_serp_cache 
            WHERE keyword = ? AND lat = ? AND lng = ? AND expires_at > datetime('now')
        ''', (keyword, round(lat, 6), round(lng, 6)))
        
        row = cursor.fetchone()
        if row:
            return json.loads(row['results_json'])
        return None


def save_serp_cache(keyword: str, lat: float, lng: float, results: list, ttl_seconds: int = 3600):
    """Cache SERP results."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gmb_serp_cache (keyword, lat, lng, results_json, expires_at)
            VALUES (?, ?, ?, ?, datetime('now', '+' || ? || ' seconds'))
            ON CONFLICT(keyword, lat, lng) DO UPDATE SET
                results_json = excluded.results_json,
                expires_at = excluded.expires_at,
                created_at = CURRENT_TIMESTAMP
        ''', (keyword, round(lat, 6), round(lng, 6), json.dumps(results), ttl_seconds))


def save_serp_search(search_data: dict) -> int:
    """Save a SERP search to history."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Check if columns exist (for safe migration) - actually done in _run_migrations but good for safety
        
        cursor.execute('''
            INSERT INTO serp_searches (
                keyword, location, lat, lng, device, language, depth,
                organic_count, local_pack_count, hotel_count, shopping_count,
                target_rank, target_url, results_json, ai_overview_present
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            search_data.get('keyword', ''),
            search_data.get('location', ''),
            search_data.get('lat'),
            search_data.get('lng'),
            search_data.get('device', 'desktop'),
            search_data.get('language', 'en'),
            search_data.get('depth', 10),
            search_data.get('organic_count', 0),
            search_data.get('local_pack_count', 0),
            search_data.get('hotel_count', 0),
            search_data.get('shopping_count', 0),
            search_data.get('target_rank'),
            search_data.get('target_url'),
            json.dumps(search_data.get('results', {})),
            1 if search_data.get('results', {}).get('serp_features', {}).get('ai_overview') else 0
        ))
        return cursor.lastrowid


def get_serp_history(limit: int = 50) -> list:
    """Get SERP search history."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, keyword, location, lat, lng, device, language, depth,
                   organic_count, local_pack_count, hotel_count, shopping_count,
                   target_rank, target_url, created_at
            FROM serp_searches
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]


def get_serp_search_by_id(search_id: int) -> dict:
    """Get a specific SERP search with full results."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM serp_searches WHERE id = ?
        ''', (search_id,))
        row = cursor.fetchone()
        if row:
            result = dict(row)
            result['results'] = json.loads(result.get('results_json', '{}'))
            return result
        return None


def delete_serp_search(search_id: int) -> bool:
    """Delete a SERP search from history."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM serp_searches WHERE id = ?', (search_id,))
        return cursor.rowcount > 0
