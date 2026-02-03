"""
Database Abstraction Layer
Handles connections to either SQLite or PostgreSQL based on configuration.
Using pg8000 as pure-Python Postgres driver with connection pooling.
"""
import os
import sqlite3
import urllib.parse
from contextlib import contextmanager
from datetime import datetime
import threading
import logging
import re
from queue import Queue, Empty
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')  # 'sqlite' or 'postgres'
POSTGRES_URI = os.getenv('POSTGRES_URI') or os.getenv('DATABASE_URL', '')

# Connection Pool Configuration
POOL_SIZE = 5  # Number of connections to maintain in the pool
POOL_TIMEOUT = 30  # Seconds to wait for a connection from the pool

class ConnectionPool:
    """Simple thread-safe connection pool for PostgreSQL"""
    
    def __init__(self, create_connection_func, pool_size=5):
        self._create_connection = create_connection_func
        self._pool = Queue(maxsize=pool_size)
        self._pool_size = pool_size
        self._lock = threading.Lock()
        self._connections_created = 0
        self._initialized = False
        
    def _initialize_pool(self):
        """Create initial pool of connections"""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            logger.info(f"DB Pool: Initializing pool with {self._pool_size} connections")
            for _ in range(self._pool_size):
                try:
                    conn = self._create_connection()
                    self._pool.put_nowait(conn)
                    self._connections_created += 1
                except Exception as e:
                    logger.error(f"DB Pool: Failed to create connection: {e}")
            self._initialized = True
            logger.info(f"DB Pool: Initialized with {self._connections_created} connections")
    
    def get_connection(self, timeout=POOL_TIMEOUT):
        """Get a connection from the pool"""
        self._initialize_pool()
        try:
            conn = self._pool.get(timeout=timeout)
            # Verify connection is still alive
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return conn
            except Exception:
                # Connection is dead, create a new one
                logger.warning("DB Pool: Connection was dead, creating new one")
                try:
                    conn.close()
                except:
                    pass
                return self._create_connection()
        except Empty:
            logger.warning("DB Pool: Pool exhausted, creating additional connection")
            return self._create_connection()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        try:
            # Reset connection state
            conn.rollback()
            self._pool.put_nowait(conn)
        except:
            # Pool is full or connection is bad, just close it
            try:
                conn.close()
            except:
                pass
    
    def close_all(self):
        """Close all connections in the pool"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except:
                pass
        self._initialized = False
        self._connections_created = 0

# Global connection pool (initialized lazily)
_postgres_pool = None
_pool_lock = threading.Lock()


class DBAdapter:
    """Adapts Postgres cursor to behave like SQLite cursor (mostly for query syntax)"""
    def __init__(self, connection, db_type):
        self.conn = connection
        self.db_type = db_type
        self.rowcount = 0
        self.lastrowid = None
        self._cursor_obj = self.conn.cursor()
        self._iter = None

    def execute(self, query, params=None):
        if self.db_type == 'postgres':
            # DDL Transformations
            if 'CREATE TABLE' in query.upper():
                query = re.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', 'SERIAL PRIMARY KEY', query, flags=re.IGNORECASE)
                query = re.sub(r'BOOLEAN\s+DEFAULT\s+1', 'BOOLEAN DEFAULT TRUE', query, flags=re.IGNORECASE)
                query = re.sub(r'BOOLEAN\s+DEFAULT\s+0', 'BOOLEAN DEFAULT FALSE', query, flags=re.IGNORECASE)
            
            # Convert SQLite ? syntax to Postgres %s
            query = query.replace('?', '%s')
        
        # Auto-append RETURNING for INSERTs to support lastrowid
        is_insert = query.strip().upper().startswith('INSERT')
        if self.db_type == 'postgres' and is_insert and 'RETURNING' not in query.upper():
             query = query.rstrip(';') + ' RETURNING id'

        try:
            # Emergency Reconnect Check (since pg8000 seems unstable in threaded waitress)
            # This is a hack because self.conn seems to be closed unexpectedly.
            # But self.conn is passed from get_db wrapper. We cannot reconnect easily unless we have creds.
            # However, we can check if the cursor is dead.
            
            if params:
                self._cursor_obj.execute(query, params)
            else:
                self._cursor_obj.execute(query)
            
            self.rowcount = self._cursor_obj.rowcount
            
            if is_insert and self.db_type == 'postgres':
                try:
                    result = self._cursor_obj.fetchone()
                    if result:
                         self.lastrowid = result[0]
                except Exception:
                     pass
            
        except Exception as e:
            logger.error(f"DBAdapter Error: {e} | Query: {query} | Params: {params} | Connection Open: {self.conn != None}")
            # Retry logic for missing columns (e.g., id)
            if is_insert and 'RETURNING id' in query and ('column "id" does not exist' in str(e) or 'does not exist' in str(e)):
                 query = query.replace(' RETURNING id', '').strip()
                 try:
                    if params:
                        self._cursor_obj.execute(query, params)
                    else:
                        self._cursor_obj.execute(query)
                    self.rowcount = self._cursor_obj.rowcount
                    return 
                 except Exception as retry_e:
                     logger.error(f"DB Error (Retry): {retry_e} | Query: {query}")
                     raise retry_e

            logger.error(f"DB Error: {e} | Query: {query}")
            raise e

    def executemany(self, query, params_list):
        if self.db_type == 'postgres':
             query = query.replace('?', '%s')

        try:
            self._cursor_obj.executemany(query, params_list)
            self.rowcount = self._cursor_obj.rowcount
        except Exception as e:
            logger.error(f"DB Error (executemany): {e} | Query: {query}")
            raise e

    def _make_row(self, row):
        if row is None:
            return None
        if not self._cursor_obj.description:
            return row
        col_names = [d[0] for d in self._cursor_obj.description]
        return DictRow(col_names, row)
            
    def fetchone(self):
        row = self._cursor_obj.fetchone()
        return self._make_row(row)
        
    def fetchall(self):
        rows = self._cursor_obj.fetchall()
        if not self._cursor_obj.description:
            return []
        col_names = [d[0] for d in self._cursor_obj.description]
        return [DictRow(col_names, row) for row in rows]
        
    def fetchmany(self, size=None):
        rows = self._cursor_obj.fetchmany(size)
        if not self._cursor_obj.description:
            return []
        col_names = [d[0] for d in self._cursor_obj.description]
        return [DictRow(col_names, row) for row in rows]
    
    def close(self):
        if hasattr(self, '_cursor_obj'):
            self._cursor_obj.close()

    def __getattr__(self, name):
        if hasattr(self, '_cursor_obj'):
            return getattr(self._cursor_obj, name)
        raise AttributeError(f"'DBAdapter' object has no attribute '{name}'")

class DictRow(dict):
    """Row class that supports both named (dict) and indexed access"""
    def __init__(self, cols, values):
        super().__init__(zip(cols, values))
        self._values = values
    
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)

# Parse URI helper
def parse_postgres_uri(uri):
    # postgresql://user:pass@host:port/dbname
    result = urllib.parse.urlparse(uri)
    username = urllib.parse.unquote(result.username) if result.username else None
    password = urllib.parse.unquote(result.password) if result.password else None
    database = result.path[1:]
    hostname = result.hostname
    port = result.port or 5432
    return username, password, hostname, port, database

def _get_postgres_pool():
    """Get or create the global PostgreSQL connection pool"""
    global _postgres_pool
    
    if _postgres_pool is not None:
        return _postgres_pool
    
    with _pool_lock:
        if _postgres_pool is not None:
            return _postgres_pool
        
        postgres_uri = os.getenv('POSTGRES_URI') or os.getenv('DATABASE_URL', '')
        if not postgres_uri:
            if POSTGRES_URI:
                postgres_uri = POSTGRES_URI
            else:
                raise ValueError("POSTGRES_URI is not set")
        
        user, password, host, port, dbname = parse_postgres_uri(postgres_uri)
        
        def create_connection():
            import pg8000.dbapi
            return pg8000.dbapi.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=dbname
            )
        
        _postgres_pool = ConnectionPool(create_connection, pool_size=POOL_SIZE)
        logger.info("DB: PostgreSQL connection pool created")
        return _postgres_pool

@contextmanager
def get_db(db_name='users.db'):
    """
    Context manager for database connections.
    Uses connection pooling for PostgreSQL to avoid creating new connections each time.
    """
    # Reload config dynamically to catch .env changes loaded by main.py
    current_db_type = os.getenv('DB_TYPE', 'sqlite')

    if current_db_type == 'postgres':
        pool = _get_postgres_pool()
        conn = pool.get_connection()
        
        try:
            # Yield the wrapper for dict-like access
            yield PostgresConnectionWrapper(conn)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            # Return connection to pool instead of closing
            pool.return_connection(conn)
            
    else:
        # SQLite implementation (unchanged)
        db_path = db_name
        if not os.path.exists(db_path) and os.path.exists(os.path.join('..', db_path)):
             db_path = os.path.join('..', db_path)
             
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

class PostgresConnectionWrapper:
    def __init__(self, conn):
        self.conn = conn
        
    def cursor(self):
        return DBAdapter(self.conn, 'postgres')
        
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        self.conn.rollback()
        
    def close(self):
        self.conn.close()
        
    def execute(self, query, params=None):
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor

