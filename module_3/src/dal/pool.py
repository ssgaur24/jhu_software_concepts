# pooled PostgreSQL connection (psycopg v3) with clean shutdown
import atexit
from typing import Optional
from psycopg_pool import ConnectionPool
from src.config import database_url

_POOL: Optional[ConnectionPool] = None  # singleton pool instance

def get_pool() -> ConnectionPool:
    # lazily create the pool once per process
    global _POOL
    if _POOL is None:
        _POOL = ConnectionPool(database_url(), min_size=0, max_size=8)
        atexit.register(close_pool)  # ensure threads stop on interpreter exit
    return _POOL

def get_conn():
    # get a pooled connection; returns to pool on context exit
    return get_pool().connection()

def close_pool() -> None:
    # stop worker threads and free resources
    global _POOL
    if _POOL is not None:
        _POOL.close()
        _POOL = None
