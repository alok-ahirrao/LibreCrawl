"""
Crawl data persistence module
Handles database operations for storing and retrieving crawl data
Enables crash recovery and historical crawl access
"""
import time
import json
from datetime import datetime, timedelta
from .database import get_db


def init_crawl_tables(enable_migrations=False):
    """Initialize crawl persistence tables"""
    if not enable_migrations:
        return
        
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verify connection health
        try:
            cursor.execute("SELECT 1")
        except Exception as e:
            print(f"Warning: Connection check failed in init_crawl_tables: {e}")
            return # Abort initialization if connection is dead
            
        # Main crawls table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                session_id TEXT NOT NULL,
                base_url TEXT NOT NULL,
                base_domain TEXT,
                status TEXT DEFAULT 'running',

                config_snapshot TEXT,

                urls_discovered INTEGER DEFAULT 0,
                urls_crawled INTEGER DEFAULT 0,
                max_depth_reached INTEGER DEFAULT 0,

                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                last_saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                peak_memory_mb REAL,
                estimated_size_mb REAL,

                can_resume BOOLEAN DEFAULT 1,
                resume_checkpoint TEXT,

                sitemap_urls TEXT,

                resumable BOOLEAN DEFAULT 1,
                
                pagespeed_results TEXT,
                robots_data TEXT,
                llms_data TEXT,

                show_to_client BOOLEAN DEFAULT FALSE,
                client_id TEXT,

                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        ''')

        # Crawled URLs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawled_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_id INTEGER NOT NULL,
                url TEXT NOT NULL,

                status_code INTEGER,
                content_type TEXT,
                size INTEGER,
                is_internal BOOLEAN,
                depth INTEGER,

                title TEXT,
                meta_description TEXT,
                h1 TEXT,
                h2 TEXT,
                h3 TEXT,
                word_count INTEGER,

                canonical_url TEXT,
                lang TEXT,
                charset TEXT,
                viewport TEXT,
                robots TEXT,

                meta_tags TEXT,
                og_tags TEXT,
                twitter_tags TEXT,
                json_ld TEXT,
                analytics TEXT,
                images TEXT,
                hreflang TEXT,
                schema_org TEXT,
                redirects TEXT,
                linked_from TEXT,

                external_links INTEGER,
                internal_links INTEGER,
                


                response_time REAL,
                javascript_rendered BOOLEAN DEFAULT 0,
                
                dom_size INTEGER DEFAULT 0,
                dom_depth INTEGER DEFAULT 0,

                requires_js BOOLEAN DEFAULT 0,
                raw_html_hash TEXT,
                rendered_html_hash TEXT,

                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (crawl_id) REFERENCES crawls(id) ON DELETE CASCADE
            )
        ''')

        # Links table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_id INTEGER,
                source_url TEXT,
                target_url TEXT,
                anchor_text TEXT,
                is_internal BOOLEAN,
                is_nofollow BOOLEAN,
                target_status INTEGER,
                placement TEXT,
                scope TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (crawl_id) REFERENCES crawls (id)
            )
        ''')

        # === Create crawl_issues table ===
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_issues (
                id SERIAL PRIMARY KEY,
                crawl_id INTEGER NOT NULL,
                url TEXT,
                type TEXT,
                category TEXT,
                issue TEXT,
                details TEXT,
                severity TEXT DEFAULT 'info',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (crawl_id) REFERENCES crawls(id) ON DELETE CASCADE
            )
        ''')
        
        # === Create audit_insights table ===
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_insights (
                id SERIAL PRIMARY KEY,
                crawl_id INTEGER NOT NULL UNIQUE,
                insights_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (crawl_id) REFERENCES crawls(id) ON DELETE CASCADE
            )
        ''')
        
        # Create indexes for better performance
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_crawl_issues_crawl_id ON crawl_issues(crawl_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_insights_crawl_id ON audit_insights(crawl_id)')
        except:
            pass  # Indexes may already exist

        print("Crawl persistence tables initialized successfully")

def create_crawl(user_id, session_id, base_url, base_domain, config_snapshot, client_id=None):
    """
    Create a new crawl record with optional client_id for data isolation
    Returns the crawl_id
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO crawls (user_id, session_id, base_url, base_domain, config_snapshot, status, client_id)
                VALUES (?, ?, ?, ?, ?, 'running', ?)
            ''', (user_id, session_id, base_url, base_domain, json.dumps(config_snapshot), client_id))

            crawl_id = cursor.lastrowid
            print(f"Created new crawl record: ID={crawl_id}, URL={base_url}, client_id={client_id}")
            return crawl_id
    except Exception as e:
        print(f"Error creating crawl: {e}")
        return None

def update_crawl_stats(crawl_id, discovered=None, crawled=None, max_depth=None, peak_memory_mb=None, estimated_size_mb=None, pagespeed_results=None, sitemap_urls=None, robots_data=None, llms_data=None):
    """Update crawl statistics"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            updates = []
            params = []

            if discovered is not None:
                updates.append("urls_discovered = ?")
                params.append(discovered)
            if crawled is not None:
                updates.append("urls_crawled = ?")
                params.append(crawled)
            if max_depth is not None:
                updates.append("max_depth_reached = ?")
                params.append(max_depth)
            if peak_memory_mb is not None:
                updates.append("peak_memory_mb = ?")
                params.append(peak_memory_mb)
            if estimated_size_mb is not None:
                updates.append("estimated_size_mb = ?")
                params.append(estimated_size_mb)
            if pagespeed_results is not None:
                updates.append("pagespeed_results = ?")
                params.append(json.dumps(pagespeed_results))
            if sitemap_urls is not None:
                updates.append("sitemap_urls = ?")
                params.append(json.dumps(sitemap_urls))
            if robots_data is not None:
                updates.append("robots_data = ?")
                params.append(json.dumps(robots_data))
            if llms_data is not None:
                updates.append("llms_data = ?")
                params.append(json.dumps(llms_data))

            updates.append("last_saved_at = CURRENT_TIMESTAMP")
            params.append(crawl_id)

            query = f"UPDATE crawls SET {', '.join(updates)} WHERE id = ?"
            cursor.execute(query, params)

            return True
    except Exception as e:
        print(f"Error updating crawl stats: {e}")
        return False

def save_url_batch(crawl_id, urls):
    """
    Batch save crawled URLs
    urls: list of URL result dictionaries from crawler
    """
    if not urls:
        return True

    try:
        with get_db() as conn:
            cursor = conn.cursor()

            # Prepare batch insert
            rows = []
            for url_data in urls:
                row = (
                    crawl_id,
                    url_data.get('url'),
                    url_data.get('status_code'),
                    url_data.get('content_type'),
                    url_data.get('size'),
                    url_data.get('is_internal'),
                    url_data.get('depth'),
                    url_data.get('title'),
                    url_data.get('meta_description'),
                    url_data.get('h1'),
                    json.dumps(url_data.get('h2', [])),
                    json.dumps(url_data.get('h3', [])),
                    url_data.get('word_count'),
                    url_data.get('canonical_url'),
                    url_data.get('lang'),
                    url_data.get('charset'),
                    url_data.get('viewport'),
                    url_data.get('robots'),
                    json.dumps(url_data.get('meta_tags', {})),
                    json.dumps(url_data.get('og_tags', {})),
                    json.dumps(url_data.get('twitter_tags', {})),
                    json.dumps(url_data.get('json_ld', [])),
                    json.dumps(url_data.get('analytics', {})),
                    json.dumps(url_data.get('images', [])),
                    json.dumps(url_data.get('hreflang', [])),
                    json.dumps(url_data.get('schema_org', [])),
                    json.dumps(url_data.get('redirects', [])),
                    json.dumps(url_data.get('linked_from', [])),
                    url_data.get('external_links'),
                    url_data.get('internal_links'),
                    url_data.get('response_time'),
                    url_data.get('javascript_rendered', False),
                    url_data.get('dom_size', 0),
                    url_data.get('dom_depth', 0),
                    url_data.get('requires_js', False),
                    url_data.get('raw_html_hash'),
                    url_data.get('rendered_html_hash')
                )
                rows.append(row)

            cursor.executemany('''
                INSERT INTO crawled_urls (
                    crawl_id, url, status_code, content_type, size, is_internal, depth,
                    title, meta_description, h1, h2, h3, word_count,
                    canonical_url, lang, charset, viewport, robots,
                    meta_tags, og_tags, twitter_tags, json_ld, analytics, images,
                    hreflang, schema_org, redirects, linked_from,
                    external_links, internal_links, response_time, javascript_rendered,
                    dom_size, dom_depth, requires_js, raw_html_hash, rendered_html_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            print(f"Saved {len(urls)} URLs to database for crawl {crawl_id}")
            return True

    except Exception as e:
        print(f"Error saving URL batch: {e}")
        import traceback
        traceback.print_exc()
        return False

def save_links_batch(crawl_id, links):
    """Batch save links"""
    if not links:
        return True

    try:
        with get_db() as conn:
            cursor = conn.cursor()

            rows = []
            for link in links:
                row = (
                    crawl_id,
                    link.get('source_url'),
                    link.get('target_url'),
                    link.get('anchor_text'),
                    link.get('is_internal'),
                    link.get('nofollow', False), # is_nofollow
                    link.get('target_status'),
                    link.get('placement', 'body'),
                    link.get('scope', 'external') # scope
                )
                rows.append(row)

            cursor.executemany('''
                INSERT INTO crawl_links (
                    crawl_id, source_url, target_url, anchor_text,
                    is_internal, is_nofollow, target_status, placement, scope
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            print(f"Saved {len(links)} links to database for crawl {crawl_id}")
            return True

    except Exception as e:
        print(f"Error saving links batch: {e}")
        return False

def save_issues_batch(crawl_id, issues):
    """Batch save SEO issues"""
    if not issues:
        return True

    try:
        with get_db() as conn:
            cursor = conn.cursor()

            rows = []
            for issue in issues:
                row = (
                    crawl_id,
                    issue.get('url'),
                    issue.get('type'),
                    issue.get('category'),
                    issue.get('issue'),
                    issue.get('details')
                )
                rows.append(row)

            cursor.executemany('''
                INSERT INTO crawl_issues (
                    crawl_id, url, type, category, issue, details
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', rows)

            print(f"Saved {len(issues)} issues to database for crawl {crawl_id}")
            return True

    except Exception as e:
        print(f"Error saving issues batch: {e}")
        return False

def save_checkpoint(crawl_id, checkpoint_data):
    """Save queue checkpoint for crash recovery"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE crawls
                SET resume_checkpoint = ?, last_saved_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (json.dumps(checkpoint_data), crawl_id))

            return True
    except Exception as e:
        print(f"Error saving checkpoint: {e}")
        return False

def set_crawl_status(crawl_id, status):
    """
    Update crawl status
    status: 'running', 'paused', 'completed', 'failed', 'stopped', 'archived'
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            if status in ['completed', 'failed', 'stopped']:
                cursor.execute('''
                    UPDATE crawls
                    SET status = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, crawl_id))
            else:
                cursor.execute('''
                    UPDATE crawls
                    SET status = ?
                    WHERE id = ?
                ''', (status, crawl_id))

            print(f"Updated crawl {crawl_id} status to: {status}")
            return True

    except Exception as e:
        print(f"Error setting crawl status: {e}")
        return False

def get_crawl_by_id(crawl_id):
    """Get crawl metadata by ID"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM crawls WHERE id = ?
            ''', (crawl_id,))

            row = cursor.fetchone()
            if row:
                crawl = dict(row)
                # Parse JSON fields
                if crawl.get('config_snapshot'):
                    crawl['config_snapshot'] = json.loads(crawl['config_snapshot'])
                if crawl.get('resume_checkpoint'):
                    crawl['resume_checkpoint'] = json.loads(crawl['resume_checkpoint'])
                if crawl.get('pagespeed_results'):
                    try:
                        crawl['pagespeed_results'] = json.loads(crawl['pagespeed_results'])
                    except:
                        crawl['pagespeed_results'] = []
                if crawl.get('sitemap_urls'):
                    try:
                        crawl['sitemap_urls'] = json.loads(crawl['sitemap_urls'])
                    except:
                        crawl['sitemap_urls'] = []
                if crawl.get('robots_data'):
                    try:
                        crawl['robots_data'] = json.loads(crawl['robots_data'])
                    except:
                        crawl['robots_data'] = {'content': None, 'issues': []}
                
                if crawl.get('llms_data'):
                    try:
                        crawl['llms_data'] = json.loads(crawl['llms_data'])
                    except:
                        crawl['llms_data'] = {'content': None, 'issues': []}
                return crawl
            return None

    except Exception as e:
        print(f"Error fetching crawl: {e}")
        return None

def get_user_crawls(user_id, limit=50, offset=0, status_filter=None, client_id=None):
    """Get all crawls for a user, optionally filtered by client_id for proper isolation."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            # Build query based on filters
            query = 'SELECT * FROM crawls WHERE 1=1'
            params = []
            
            # Filter by user_id if provided
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
            
            # Filter by client_id for client-based isolation
            # Include NULL client_id (legacy crawls) so history isn't lost
            if client_id:
                query += ' AND client_id = ?'
                params.append(client_id)

            if status_filter:
                query += ' AND status = ?'
                params.append(status_filter)

            query += ' ORDER BY started_at DESC LIMIT ? OFFSET ?'
            params.extend([limit, offset])

            cursor.execute(query, params)

            crawls = []
            for row in cursor.fetchall():
                crawl = dict(row)
                # Don't parse full config for list view
                crawl['config_snapshot'] = None  # Save bandwidth
                crawls.append(crawl)

            return crawls

    except Exception as e:
        print(f"Error fetching user crawls: {e}")
        return []

def load_crawled_urls(crawl_id, limit=None, offset=0):
    """Load all crawled URLs for a crawl"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            query = 'SELECT * FROM crawled_urls WHERE crawl_id = ? ORDER BY crawled_at'
            params = [crawl_id]

            if limit:
                query += ' LIMIT ? OFFSET ?'
                params.extend([limit, offset])

            cursor.execute(query, params)

            urls = []
            for row in cursor.fetchall():
                url_data = dict(row)
                # Parse JSON fields
                for field in ['h2', 'h3', 'meta_tags', 'og_tags', 'twitter_tags',
                             'json_ld', 'analytics', 'images', 'hreflang',
                             'schema_org', 'redirects', 'linked_from']:
                    if url_data.get(field):
                        try:
                            url_data[field] = json.loads(url_data[field])
                        except:
                            url_data[field] = []

                urls.append(url_data)

            return urls

    except Exception as e:
        print(f"Error loading crawled URLs: {e}")
        return []

def load_crawl_links(crawl_id, limit=None, offset=0):
    """Load all links for a crawl"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            query = 'SELECT * FROM crawl_links WHERE crawl_id = ?'
            params = [crawl_id]

            if limit:
                query += ' LIMIT ? OFFSET ?'
                params.extend([limit, offset])

            cursor.execute(query, params)

            return [dict(row) for row in cursor.fetchall()]

    except Exception as e:
        print(f"Error loading links: {e}")
        return []

def load_crawl_issues(crawl_id, limit=None, offset=0):
    """Load all issues for a crawl"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            query = 'SELECT * FROM crawl_issues WHERE crawl_id = ?'
            params = [crawl_id]

            if limit:
                query += ' LIMIT ? OFFSET ?'
                params.extend([limit, offset])

            cursor.execute(query, params)

            return [dict(row) for row in cursor.fetchall()]

    except Exception as e:
        print(f"Error loading issues: {e}")
        return []

def get_resume_data(crawl_id):
    """Get all data needed to resume a crawl"""
    crawl = get_crawl_by_id(crawl_id)
    if not crawl:
        return None

    # Only allow resume for paused/failed/running crawls
    if crawl['status'] not in ['paused', 'failed', 'running']:
        return None

    return crawl

def delete_crawl(crawl_id):
    """Delete a crawl and all associated data (CASCADE handles related tables)"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM crawls WHERE id = ?', (crawl_id,))
            print(f"Deleted crawl {crawl_id} and all associated data")
            return True
    except Exception as e:
        print(f"Error deleting crawl: {e}")
        return False

def get_crashed_crawls():
    """Find crawls that were running when server crashed"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM crawls
                WHERE status = 'running'
                ORDER BY started_at DESC
            ''')

            crawls = []
            for row in cursor.fetchall():
                crawl = dict(row)
                crawls.append(crawl)

            return crawls

    except Exception as e:
        print(f"Error finding crashed crawls: {e}")
        return []

def cleanup_old_crawls(days=90):
    """Delete crawls older than specified days (optional maintenance)"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM crawls
                WHERE started_at < %s
                AND status IN ('completed', 'failed', 'stopped')
            ''', ((datetime.now() - timedelta(days=days)),))

            deleted = cursor.rowcount
            print(f"Cleaned up {deleted} old crawls")
            return deleted

    except Exception as e:
        print(f"Error cleaning up old crawls: {e}")
        return 0

def get_crawl_count(user_id, client_id=None):
    """Get total number of crawls for a user, optionally filtered by client_id."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Build query with optional filters
            query = 'SELECT COUNT(*) as count FROM crawls WHERE 1=1'
            params = []
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
            
            if client_id:
                query += ' AND client_id = ?'
                params.append(client_id)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result['count'] if result else 0
    except Exception as e:
        print(f"Error getting crawl count: {e}")
        return 0

def get_database_size_mb():
    """Get total database size in MB (PostgreSQL)"""
    try:
        # For PostgreSQL, query the database size
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pg_database_size(current_database()) as size")
            result = cursor.fetchone()
            if result and result['size']:
                return round(result['size'] / (1024 * 1024), 2)
        return 0
    except Exception as e:
        print(f"Error getting database size: {e}")
        return 0

def save_audit_insights(crawl_id, insights_data):
    """Save AI insights for a crawl"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            # Use PostgreSQL-compatible ON CONFLICT syntax instead of SQLite's INSERT OR REPLACE
            cursor.execute('''
                INSERT INTO audit_insights (crawl_id, insights_json, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(crawl_id) DO UPDATE SET
                    insights_json = excluded.insights_json,
                    updated_at = CURRENT_TIMESTAMP
            ''', (crawl_id, json.dumps(insights_data)))
            print(f"Saved insights for crawl {crawl_id}")
            return True
    except Exception as e:
        print(f"Error saving insights: {e}")
        return False

def get_audit_insights(crawl_id):
    """Get AI insights for a crawl"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT insights_json FROM audit_insights WHERE crawl_id = ?', (crawl_id,))
            row = cursor.fetchone()
            if row and row['insights_json']:
                return json.loads(row['insights_json'])
            return None
    except Exception as e:
        print(f"Error getting insights: {e}")
        return None
