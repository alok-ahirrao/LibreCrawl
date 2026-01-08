
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
