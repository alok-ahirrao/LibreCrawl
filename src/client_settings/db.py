import logging
from src.database import get_db

logger = logging.getLogger(__name__)

def init_client_settings_db():
    """Initialize the client settings table."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS client_module_access (
                    client_id TEXT NOT NULL,
                    module_slug TEXT NOT NULL,
                    is_visible BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (client_id, module_slug)
                )
            ''')
            # Postgres specific: Ensure boolean is handled correctly if we move away from SQLite compatibility layer
            # But DBAdapter handles generic types well.
            logger.info("Client settings DB initialized.")
    except Exception as e:
        logger.error(f"Failed to init client settings DB: {e}")

def get_client_module_access(client_id):
    """
    Get all module access settings for a client.
    Returns: dict { module_slug: is_visible (bool) }
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT module_slug, is_visible 
                FROM client_module_access 
                WHERE client_id = %s
            ''', (client_id,))
            rows = cursor.fetchall()
            
            # Convert 1/0 to True/False explicitly if needed, though DBAdapter/Drivers usually handle it.
            # SQLite returns 1/0 for booleans usually.
            return {row['module_slug']: bool(row['is_visible']) for row in rows}
    except Exception as e:
        logger.error(f"Error fetching module access for client {client_id}: {e}")
        return {}

def toggle_module_access(client_id, module_slug, is_visible):
    """
    Update or insert a module access setting.
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if exists (Postgres has ON CONFLICT, SQLite has INSERT OR REPLACE / ON CONFLICT)
            # Standard SQL UPSERT approach or separated check is safest for cross-db compatibility 
            # if we aren't 100% sure about the DBAdapter's capabilities on specific syntax.
            # However, DBAdapter is used. Let's try standard INSERT w/ ON CONFLICT (Supported by PG and SQLite >= 3.24)
            
            cursor.execute('''
                INSERT INTO client_module_access (client_id, module_slug, is_visible, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(client_id, module_slug) 
                DO UPDATE SET is_visible = excluded.is_visible, updated_at = CURRENT_TIMESTAMP
                RETURNING client_id
            ''', (client_id, module_slug, is_visible))
            
            return True
            
            return True
    except Exception as e:
        logger.error(f"Error toggling module access: {e}")
        return False

def toggle_module_access_bulk(client_id, updates):
    """
    Bulk update module access settings.
    updates: list of dicts { 'module_slug': str, 'is_visible': bool }
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Prepare data for executemany
            params_list = [
                (client_id, update['module_slug'], update['is_visible']) 
                for update in updates
            ]
            
            # Use same UPSERT logic as single toggle
            # Note: We must explicitly return client_id to avoid DBAdapter's 'RETURNING id' injection
            cursor.executemany('''
                INSERT INTO client_module_access (client_id, module_slug, is_visible, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(client_id, module_slug) 
                DO UPDATE SET is_visible = excluded.is_visible, updated_at = CURRENT_TIMESTAMP
                RETURNING client_id
            ''', params_list)
            
            return True
    except Exception as e:
        logger.error(f"Error bulk toggling module access: {e}")
        return False
