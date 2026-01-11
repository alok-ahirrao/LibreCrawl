
"""
Keyword Research History Database Module
"""
import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime

# Database file location
DB_FILE = 'users.db'

@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_keyword_tables():
    """Initialize keyword history tables"""
    with get_db() as conn:
        cursor = conn.cursor()

        # Keyword History table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS keyword_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL, 
                user_id INTEGER,
                input_params TEXT,
                results TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_keyword_history_type ON keyword_history(type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_keyword_history_user ON keyword_history(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_keyword_history_created ON keyword_history(created_at DESC)')

        # Content Items table - persistent storage for mapped content
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS content_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cluster_topic TEXT NOT NULL,
                primary_keyword TEXT NOT NULL,
                secondary_keywords TEXT,
                content_type TEXT NOT NULL,
                content_type_name TEXT,
                intent TEXT,
                confidence INTEGER DEFAULT 50,
                status TEXT DEFAULT 'draft',
                priority_tier TEXT DEFAULT 'B',
                priority_score INTEGER DEFAULT 0,
                scheduled_date DATE,
                week_number INTEGER,
                notes TEXT,
                brief TEXT,
                user_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for content_items
        # Add columns for multi-tenancy if not exist (Migration)
        try:
            cursor.execute('ALTER TABLE content_items ADD COLUMN client_id TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE content_items ADD COLUMN campaign_title TEXT')
        except:
            pass
        try:
            cursor.execute('ALTER TABLE content_items ADD COLUMN website_url TEXT')
        except:
            pass

        # Create indexes (after ensuring columns exist)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_items_status ON content_items(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_items_scheduled ON content_items(scheduled_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_items_user ON content_items(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_items_client ON content_items(client_id)')

        print("Keyword history tables initialized successfully")

def save_keyword_history(type, input_params, results, user_id=None):
    """
    Save keyword research result to history
    type: 'workflow', 'discovery', 'density', 'competitor', 'cannibalization', 'content_map'
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO keyword_history (type, user_id, input_params, results)
                VALUES (?, ?, ?, ?)
            ''', (type, user_id, json.dumps(input_params), json.dumps(results)))
            
            return cursor.lastrowid
    except Exception as e:
        print(f"Error saving keyword history: {e}")
        return None

def get_keyword_history(user_id=None, type_filter=None, limit=50, offset=0):
    """Get keyword history list"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = 'SELECT id, type, input_params, created_at FROM keyword_history WHERE 1=1'
            params = []
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
            
            if type_filter:
                query += ' AND type = ?'
                params.append(type_filter)
                
            query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            
            history = []
            for row in cursor.fetchall():
                item = dict(row)
                if item.get('input_params'):
                    try:
                        item['input_params'] = json.loads(item['input_params'])
                    except:
                        pass
                history.append(item)
                
            return history
    except Exception as e:
        print(f"Error fetching keyword history: {e}")
        return []

def get_keyword_history_item(history_id, user_id=None):
    """Get full keyword history item details"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = 'SELECT * FROM keyword_history WHERE id = ?'
            params = [history_id]
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
                
            cursor.execute(query, params)
            
            row = cursor.fetchone()
            if row:
                item = dict(row)
                if item.get('input_params'):
                    try:
                        item['input_params'] = json.loads(item['input_params'])
                    except:
                        pass
                if item.get('results'):
                    try:
                        item['results'] = json.loads(item['results'])
                    except:
                        pass
                return item
            
            return None
    except Exception as e:
        print(f"Error fetching keyword history item: {e}")
        return None

def delete_keyword_history(history_id, user_id=None):
    """Delete a history item"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = 'DELETE FROM keyword_history WHERE id = ?'
            params = [history_id]
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
                
            cursor.execute(query, params)
            return True
    except Exception as e:
        print(f"Error deleting keyword history: {e}")
        return False


# =============================================================================
# CONTENT ITEMS CRUD
# =============================================================================

def save_content_item(item_data, user_id=None):
    """
    Save a content item to database
    
    item_data should contain:
    - cluster_topic, primary_keyword, secondary_keywords (list)
    - content_type, content_type_name, intent, confidence
    - status, priority_tier, priority_score
    - scheduled_date (YYYY-MM-DD), week_number, notes, brief (dict)
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Handle JSON fields
            secondary_kw = json.dumps(item_data.get('secondary_keywords', []))
            brief = json.dumps(item_data.get('brief', {})) if item_data.get('brief') else None
            
            cursor.execute('''
                INSERT INTO content_items (
                    cluster_topic, primary_keyword, secondary_keywords,
                    content_type, content_type_name, intent, confidence,
                    status, priority_tier, priority_score,
                    scheduled_date, week_number, notes, brief, user_id,
                    client_id, campaign_title, website_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item_data.get('cluster_topic', ''),
                item_data.get('primary_keyword', ''),
                secondary_kw,
                item_data.get('content_type', 'blog_post'),
                item_data.get('content_type_name', 'Blog Post'),
                item_data.get('intent', 'informational'),
                item_data.get('confidence', 50),
                item_data.get('status', 'draft'),
                item_data.get('priority_tier', 'B'),
                item_data.get('priority_score', 0),
                item_data.get('scheduled_date'),
                item_data.get('week_number'),
                item_data.get('notes'),
                brief,
                user_id,
                item_data.get('client_id'),
                item_data.get('campaign_title'),
                item_data.get('website_url')
            ))
            
            return cursor.lastrowid
    except Exception as e:
        print(f"Error saving content item: {e}")
        return None


def get_content_items(user_id=None, client_id=None, status_filter=None, limit=100, offset=0):
    """Get content items list with optional filtering"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = 'SELECT * FROM content_items WHERE 1=1'
            params = []
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
            
            if client_id is not None:
                query += ' AND client_id = ?'
                params.append(client_id)
            
            if status_filter:
                query += ' AND status = ?'
                params.append(status_filter)
                
            query += ' ORDER BY priority_score DESC, scheduled_date ASC, created_at DESC LIMIT ? OFFSET ?'
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            
            items = []
            for row in cursor.fetchall():
                item = dict(row)
                # Parse JSON fields
                if item.get('secondary_keywords'):
                    try:
                        item['secondary_keywords'] = json.loads(item['secondary_keywords'])
                    except:
                        item['secondary_keywords'] = []
                if item.get('brief'):
                    try:
                        item['brief'] = json.loads(item['brief'])
                    except:
                        item['brief'] = {}
                items.append(item)
                
            return items
    except Exception as e:
        print(f"Error fetching content items: {e}")
        return []


def get_content_item(item_id, user_id=None):
    """Get a single content item by ID"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = 'SELECT * FROM content_items WHERE id = ?'
            params = [item_id]
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
                
            cursor.execute(query, params)
            
            row = cursor.fetchone()
            if row:
                item = dict(row)
                if item.get('secondary_keywords'):
                    try:
                        item['secondary_keywords'] = json.loads(item['secondary_keywords'])
                    except:
                        item['secondary_keywords'] = []
                if item.get('brief'):
                    try:
                        item['brief'] = json.loads(item['brief'])
                    except:
                        item['brief'] = {}
                return item
            
            return None
    except Exception as e:
        print(f"Error fetching content item: {e}")
        return None


def update_content_item(item_id, updates, user_id=None):
    """
    Update a content item
    
    updates can include: status, scheduled_date, week_number, notes, priority_tier
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Build dynamic update query
            set_clauses = ['updated_at = CURRENT_TIMESTAMP']
            params = []
            
            allowed_fields = [
                'status', 'scheduled_date', 'week_number', 'notes', 
                'priority_tier', 'priority_score', 'cluster_topic',
                'primary_keyword', 'content_type', 'intent'
            ]
            
            for field in allowed_fields:
                if field in updates:
                    set_clauses.append(f'{field} = ?')
                    params.append(updates[field])
            
            # Handle JSON fields specially
            if 'secondary_keywords' in updates:
                set_clauses.append('secondary_keywords = ?')
                params.append(json.dumps(updates['secondary_keywords']))
            
            if 'brief' in updates:
                set_clauses.append('brief = ?')
                params.append(json.dumps(updates['brief']))
            
            if len(set_clauses) == 1:  # Only updated_at
                return False
            
            query = f"UPDATE content_items SET {', '.join(set_clauses)} WHERE id = ?"
            params.append(item_id)
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
                
            cursor.execute(query, params)
            return cursor.rowcount > 0
    except Exception as e:
        print(f"Error updating content item: {e}")
        return False


def delete_content_item(item_id, user_id=None):
    """Delete a content item"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            query = 'DELETE FROM content_items WHERE id = ?'
            params = [item_id]
            
            if user_id is not None:
                query += ' AND user_id = ?'
                params.append(user_id)
                
            cursor.execute(query, params)
            return cursor.rowcount > 0
    except Exception as e:
        print(f"Error deleting content item: {e}")
        return False


def bulk_save_content_items(items, user_id=None):
    """Save multiple content items at once"""
    saved_ids = []
    for item in items:
        item_id = save_content_item(item, user_id)
        if item_id:
            saved_ids.append(item_id)
    return saved_ids
