"""
GMB Core Database Models
Uses centralized PostgreSQL database abstraction layer
"""
from datetime import datetime, timedelta
from src.database import get_db
import json


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
                source_url TEXT,

                description TEXT,
                business_hours TEXT,
                attributes TEXT,
                service_area_type TEXT DEFAULT 'STOREFRONT',
                service_area_places TEXT,

                post_count INTEGER DEFAULT 0,
                last_post_date TIMESTAMP,
                qa_count INTEGER DEFAULT 0,
                
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
                
                average_rank REAL,
                show_to_client BOOLEAN DEFAULT 0,
                client_id TEXT,
                
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

                hours TEXT,
                services TEXT,
                post_count INTEGER DEFAULT 0,
                q_and_a_count INTEGER DEFAULT 0,
                
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

                ai_overview_present BOOLEAN DEFAULT 0,
                show_to_client BOOLEAN DEFAULT 0,
                client_id TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_serp_searches_keyword 
            ON serp_searches(keyword, created_at DESC)
        ''')

        # 12. GMB Health Snapshots - Historical health scoring
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_health_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id INTEGER NOT NULL,
                snapshot_date DATE NOT NULL,
                
                -- Scores (0-100)
                overall_score INTEGER DEFAULT 0,
                profile_score INTEGER DEFAULT 0,
                photos_score INTEGER DEFAULT 0,
                reviews_score INTEGER DEFAULT 0,
                posts_score INTEGER DEFAULT 0,
                qa_score INTEGER DEFAULT 0,
                
                -- Raw Metrics
                metrics_json TEXT,

                show_to_client BOOLEAN DEFAULT 0,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (location_id) REFERENCES gmb_locations(id) ON DELETE CASCADE,
                UNIQUE(location_id, snapshot_date)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_health_location 
            ON gmb_health_snapshots(location_id, snapshot_date DESC)
        ''')

        # 13. GMB Audit Logs - Change history tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                
                action TEXT NOT NULL,
                before_state TEXT,
                after_state TEXT,
                
                actor_type TEXT DEFAULT 'SYSTEM',
                actor_id INTEGER,
                source TEXT DEFAULT 'API',
                source_ip TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_audit_entity 
            ON gmb_audit_logs(entity_type, entity_id, created_at DESC)
        ''')

        # 14. GMB Sync Jobs - Queue-based sync operations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_sync_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT UNIQUE NOT NULL,
                user_id INTEGER,
                account_id INTEGER,
                location_id INTEGER,
                
                job_type TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                priority INTEGER DEFAULT 5,
                
                progress INTEGER DEFAULT 0,
                total_items INTEGER DEFAULT 0,
                
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                error_message TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_sync_status 
            ON gmb_sync_jobs(status, priority DESC, created_at ASC)
        ''')

        # 15. GMB Quota Logs - API quota tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gmb_quota_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,
                method TEXT DEFAULT 'GET',
                
                quota_used INTEGER DEFAULT 1,
                response_code INTEGER,
                response_time_ms INTEGER,
                
                error_message TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_gmb_quota_date 
            ON gmb_quota_logs(created_at DESC)
        ''')

        print("GMB Core tables initialized successfully")



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
            WHERE keyword = ? AND lat = ? AND lng = ? AND expires_at > CURRENT_TIMESTAMP
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
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(keyword, lat, lng) DO UPDATE SET
                results_json = excluded.results_json,
                expires_at = excluded.expires_at,
                created_at = CURRENT_TIMESTAMP
        ''', (keyword, round(lat, 6), round(lng, 6), json.dumps(results), datetime.now() + timedelta(seconds=ttl_seconds)))


def save_serp_search(search_data: dict) -> int:
    """Save a SERP search to history."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Check if columns exist (for safe migration) - actually done in _run_migrations but good for safety
        
        cursor.execute('''
            INSERT INTO serp_searches (
                keyword, location, lat, lng, device, language, depth,
                organic_count, local_pack_count, hotel_count, shopping_count,
                target_rank, target_url, results_json, ai_overview_present, client_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            1 if search_data.get('results', {}).get('serp_features', {}).get('ai_overview') else 0,
            search_data.get('client_id')
        ))
        return cursor.lastrowid


def get_serp_history(limit: int = 50, client_id: str = None) -> list:
    """Get SERP search history."""
    with get_db() as conn:
        cursor = conn.cursor()
        if client_id:
            cursor.execute('''
                SELECT id, keyword, location, lat, lng, device, language, depth,
                       organic_count, local_pack_count, hotel_count, shopping_count,
                       target_rank, target_url, created_at
                FROM serp_searches
                WHERE client_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (client_id, limit))
        else:
            cursor.execute('''
                SELECT id, keyword, location, lat, lng, device, language, depth,
                       organic_count, local_pack_count, hotel_count, shopping_count,
                       target_rank, target_url, created_at
                FROM serp_searches
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]


def get_serp_search_by_id(search_id: int, client_id: str = None) -> dict:
    """Get a specific SERP search with full results."""
    with get_db() as conn:
        cursor = conn.cursor()
        if client_id:
             cursor.execute('SELECT * FROM serp_searches WHERE id = ? AND client_id = ?', (search_id, client_id))
        else:
             cursor.execute('SELECT * FROM serp_searches WHERE id = ?', (search_id,))
        row = cursor.fetchone()
        if row:
            result = dict(row)
            result['results'] = json.loads(result.get('results_json', '{}'))
            return result
        return None


def delete_serp_search(search_id: int, client_id: str = None) -> bool:
    """Delete a SERP search from history."""
    with get_db() as conn:
        cursor = conn.cursor()
        if client_id:
            cursor.execute('DELETE FROM serp_searches WHERE id = ? AND client_id = ?', (search_id, client_id))
        else:
            cursor.execute('DELETE FROM serp_searches WHERE id = ?', (search_id,))
        return cursor.rowcount > 0


# ==================== Health Snapshot Functions ====================

def save_health_snapshot(location_id: int, scores: dict, metrics: dict = None) -> int:
    """Save a health snapshot for a location."""
    with get_db() as conn:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        
        cursor.execute('''
            INSERT INTO gmb_health_snapshots (
                location_id, snapshot_date,
                overall_score, profile_score, photos_score,
                reviews_score, posts_score, qa_score,
                metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(location_id, snapshot_date) DO UPDATE SET
                overall_score = excluded.overall_score,
                profile_score = excluded.profile_score,
                photos_score = excluded.photos_score,
                reviews_score = excluded.reviews_score,
                posts_score = excluded.posts_score,
                qa_score = excluded.qa_score,
                metrics_json = excluded.metrics_json
        ''', (
            location_id,
            today,
            scores.get('overall', 0),
            scores.get('profile', 0),
            scores.get('photos', 0),
            scores.get('reviews', 0),
            scores.get('posts', 0),
            scores.get('qa', 0),
            json.dumps(metrics) if metrics else None
        ))
        return cursor.lastrowid


def get_health_history(location_id: int, days: int = 30) -> list:
    """Get health snapshot history for a location."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM gmb_health_snapshots
            WHERE location_id = ?
            ORDER BY snapshot_date DESC
            LIMIT ?
        ''', (location_id, days))
        results = []
        for row in cursor.fetchall():
            item = dict(row)
            if item.get('metrics_json'):
                item['metrics'] = json.loads(item['metrics_json'])
            results.append(item)
        return results


def get_latest_health_score(location_id: int) -> dict:
    """Get the most recent health snapshot for a location."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM gmb_health_snapshots
            WHERE location_id = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        ''', (location_id,))
        row = cursor.fetchone()
        if row:
            item = dict(row)
            if item.get('metrics_json'):
                item['metrics'] = json.loads(item['metrics_json'])
            return item
        return None


# ==================== Audit Log Functions ====================

def log_audit_event(
    entity_type: str,
    entity_id: int,
    action: str,
    before_state: dict = None,
    after_state: dict = None,
    actor_type: str = 'SYSTEM',
    actor_id: int = None,
    source: str = 'API',
    source_ip: str = None
) -> int:
    """Log an audit event for change history tracking."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gmb_audit_logs (
                entity_type, entity_id, action,
                before_state, after_state,
                actor_type, actor_id, source, source_ip
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entity_type,
            entity_id,
            action,
            json.dumps(before_state) if before_state else None,
            json.dumps(after_state) if after_state else None,
            actor_type,
            actor_id,
            source,
            source_ip
        ))
        return cursor.lastrowid


def get_audit_logs(
    entity_type: str = None,
    entity_id: int = None,
    limit: int = 50
) -> list:
    """Get audit logs, optionally filtered by entity."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        query = 'SELECT * FROM gmb_audit_logs'
        params = []
        conditions = []
        
        if entity_type:
            conditions.append('entity_type = ?')
            params.append(entity_type)
        if entity_id:
            conditions.append('entity_id = ?')
            params.append(entity_id)
        
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        query += ' ORDER BY created_at DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        results = []
        for row in cursor.fetchall():
            item = dict(row)
            if item.get('before_state'):
                item['before_state'] = json.loads(item['before_state'])
            if item.get('after_state'):
                item['after_state'] = json.loads(item['after_state'])
            results.append(item)
        return results


# ==================== Sync Job Functions ====================

def create_sync_job(
    job_type: str,
    user_id: int = None,
    account_id: int = None,
    location_id: int = None,
    priority: int = 5,
    total_items: int = 0
) -> str:
    """Create a new sync job in the queue."""
    import uuid
    job_id = str(uuid.uuid4())
    
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gmb_sync_jobs (
                job_id, user_id, account_id, location_id,
                job_type, priority, total_items
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (job_id, user_id, account_id, location_id, job_type, priority, total_items))
        return job_id


def update_sync_job(
    job_id: str,
    status: str = None,
    progress: int = None,
    error_message: str = None
) -> bool:
    """Update a sync job's status or progress."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        updates = []
        params = []
        
        if status:
            updates.append('status = ?')
            params.append(status)
            if status == 'running':
                updates.append('started_at = CURRENT_TIMESTAMP')
            elif status in ('completed', 'failed'):
                updates.append('completed_at = CURRENT_TIMESTAMP')
        
        if progress is not None:
            updates.append('progress = ?')
            params.append(progress)
        
        if error_message:
            updates.append('error_message = ?')
            params.append(error_message)
            updates.append('retry_count = retry_count + 1')
        
        if not updates:
            return False
        
        params.append(job_id)
        cursor.execute(
            f'UPDATE gmb_sync_jobs SET {", ".join(updates)} WHERE job_id = ?',
            params
        )
        return cursor.rowcount > 0


def get_sync_job(job_id: str) -> dict:
    """Get a sync job by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM gmb_sync_jobs WHERE job_id = ?', (job_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_pending_sync_jobs(limit: int = 10) -> list:
    """Get pending sync jobs ordered by priority."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM gmb_sync_jobs
            WHERE status = 'pending' AND retry_count < max_retries
            ORDER BY priority DESC, created_at ASC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]


def get_sync_job_history(user_id: int = None, limit: int = 20) -> list:
    """Get sync job history, optionally filtered by user."""
    with get_db() as conn:
        cursor = conn.cursor()
        if user_id:
            cursor.execute('''
                SELECT * FROM gmb_sync_jobs
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (user_id, limit))
        else:
            cursor.execute('''
                SELECT * FROM gmb_sync_jobs
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]


# ==================== Quota Logging Functions ====================

def log_quota_usage(
    endpoint: str,
    method: str = 'GET',
    quota_used: int = 1,
    response_code: int = None,
    response_time_ms: int = None,
    error_message: str = None
) -> int:
    """Log an API quota usage event."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gmb_quota_logs (
                endpoint, method, quota_used,
                response_code, response_time_ms, error_message
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (endpoint, method, quota_used, response_code, response_time_ms, error_message))
        return cursor.lastrowid


def get_quota_stats(period: str = 'day') -> dict:
    """Get quota usage statistics for a period (day/week/month)."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        if period == 'day':
            time_filter = "datetime('now', '-1 day')"
        elif period == 'week':
            time_filter = "datetime('now', '-7 days')"
        else:  # month
            time_filter = "datetime('now', '-30 days')"
        
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_requests,
                SUM(quota_used) as total_quota_used,
                SUM(CASE WHEN response_code >= 400 THEN 1 ELSE 0 END) as error_count,
                AVG(response_time_ms) as avg_response_time
            FROM gmb_quota_logs
            WHERE created_at > {time_filter}
        ''')
        row = cursor.fetchone()
        
        # Get breakdown by endpoint
        cursor.execute(f'''
            SELECT endpoint, COUNT(*) as count, SUM(quota_used) as quota
            FROM gmb_quota_logs
            WHERE created_at > {time_filter}
            GROUP BY endpoint
            ORDER BY quota DESC
        ''')
        endpoints = [dict(r) for r in cursor.fetchall()]
        
        return {
            'period': period,
            'total_requests': row['total_requests'] or 0,
            'total_quota_used': row['total_quota_used'] or 0,
            'error_count': row['error_count'] or 0,
            'avg_response_time_ms': round(row['avg_response_time'] or 0, 2),
            'by_endpoint': endpoints
        }


# ==================== Location Helper Functions ====================

def get_all_locations_with_health(account_id: int = None) -> list:
    """Get all locations with their latest health scores."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                l.*,
                h.overall_score,
                h.profile_score,
                h.photos_score,
                h.reviews_score,
                h.posts_score,
                h.qa_score,
                h.snapshot_date as health_date
            FROM gmb_locations l
            LEFT JOIN gmb_health_snapshots h ON l.id = h.location_id
                AND h.snapshot_date = (
                    SELECT MAX(snapshot_date) 
                    FROM gmb_health_snapshots 
                    WHERE location_id = l.id
                )
        '''
        
        if account_id:
            query += ' WHERE l.account_id = ?'
            cursor.execute(query, (account_id,))
        else:
            cursor.execute(query)
        
        return [dict(row) for row in cursor.fetchall()]


# ==================== Crawl-Only Location Functions ====================

def save_crawled_location(business_data: dict, user_id: int = None) -> int:
    """
    Save a location from crawled Google Maps data (no OAuth needed).
    
    Args:
        business_data: Dict with place_id, name, address, lat, lng, hours, attributes, etc.
        user_id: Optional user ID for ownership
        
    Returns:
        Location ID
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        place_id = business_data.get('place_id') or business_data.get('name', '').replace(' ', '_')
        
        # Serialize JSON fields
        hours_json = json.dumps(business_data.get('hours')) if business_data.get('hours') else None
        attributes_json = json.dumps(business_data.get('attributes')) if business_data.get('attributes') else None
        service_area = business_data.get('service_area', {})
        service_area_places_json = json.dumps(service_area.get('areas', [])) if service_area.get('areas') else None
        
        # Check if location already exists
        cursor.execute('SELECT id FROM gmb_locations WHERE google_location_id = ?', (place_id,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing location with all new fields
            cursor.execute('''
                UPDATE gmb_locations SET
                    location_name = ?,
                    address_lines = ?,
                    locality = ?,
                    region = ?,
                    primary_category = ?,
                    lat = ?,
                    lng = ?,
                    website_url = ?,
                    phone_number = ?,
                    source_url = ?,
                    total_reviews = ?,
                    rating = ?,
                    photo_count = ?,
                    business_hours = ?,
                    attributes = ?,
                    description = ?,
                    service_area_type = ?,
                    service_area_places = ?,
                    post_count = ?,
                    last_post_date = ?,
                    qa_count = ?,
                    last_synced_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                business_data.get('name', ''),
                business_data.get('address', ''),
                business_data.get('locality', ''),
                business_data.get('region', ''),
                business_data.get('category', business_data.get('primary_category', '')),
                business_data.get('lat'),
                business_data.get('lng'),
                business_data.get('website'),
                business_data.get('phone'),
                business_data.get('source_url'),
                business_data.get('review_count', 0),
                business_data.get('rating', 0),
                business_data.get('photo_count', 0),
                hours_json,
                attributes_json,
                business_data.get('description'),
                service_area.get('type', 'STOREFRONT'),
                service_area_places_json,
                business_data.get('post_count', 0),
                business_data.get('last_post_date'),
                business_data.get('qa_count', 0),
                existing['id']
            ))
            return existing['id']
        else:
            # Insert new location with all fields
            cursor.execute('''
                INSERT INTO gmb_locations (
                    account_id, google_location_id, location_name,
                    address_lines, locality, region, primary_category,
                    lat, lng, website_url, phone_number, source_url,
                    total_reviews, rating, photo_count,
                    business_hours, attributes, description,
                    service_area_type, service_area_places,
                    post_count, last_post_date, qa_count,
                    last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                None,  # account_id is NULL for crawl-only locations
                place_id,
                business_data.get('name', ''),
                business_data.get('address', ''),
                business_data.get('locality', ''),
                business_data.get('region', ''),
                business_data.get('category', business_data.get('primary_category', '')),
                business_data.get('lat'),
                business_data.get('lng'),
                business_data.get('website'),
                business_data.get('phone'),
                business_data.get('source_url'),
                business_data.get('review_count', 0),
                business_data.get('rating', 0),
                business_data.get('photo_count', 0),
                hours_json,
                attributes_json,
                business_data.get('description'),
                service_area.get('type', 'STOREFRONT'),
                service_area_places_json,
                business_data.get('post_count', 0),
                business_data.get('last_post_date'),
                business_data.get('qa_count', 0)
            ))
            return cursor.lastrowid


def delete_location(location_id: int) -> bool:
    """Delete a location and all related data."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Delete related data first (cascade should handle this, but be explicit)
        cursor.execute('DELETE FROM gmb_health_snapshots WHERE location_id = ?', (location_id,))
        cursor.execute('DELETE FROM gmb_reviews WHERE location_id = ?', (location_id,))
        cursor.execute('DELETE FROM gmb_grid_scans WHERE location_id = ?', (location_id,))
        
        # Delete the location
        cursor.execute('DELETE FROM gmb_locations WHERE id = ?', (location_id,))
        
        return cursor.rowcount > 0


def get_location_by_id(location_id: int) -> dict:
    """Get a single location by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM gmb_locations WHERE id = ?', (location_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


