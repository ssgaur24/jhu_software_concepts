"""psycopg3 connection pool with clean shutdown.

Creates one ConnectionPool on first use (lazy singleton) and ensures
worker threads are stopped via atexit or an explicit close_pool() call.
"""

# stdlib + third-party imports
import atexit
from typing import Optional
from psycopg_pool import ConnectionPool

# local imports
from src.config import database_url

# module-level singleton reference to the pool
_POOL: Optional[ConnectionPool] = None  # singleton pool instance


def get_pool() -> ConnectionPool:
    """Return the singleton ConnectionPool, creating it if needed."""
    # lazily create the pool on first access
    global _POOL
    if _POOL is None:
        _POOL = ConnectionPool(database_url(), min_size=0, max_size=8)  # small pool for CLI usage
        atexit.register(close_pool)  # ensure clean shutdown at interpreter exit
    return _POOL


def get_conn():
    """Return a pooled connection; auto-returns to the pool on context exit."""
    # caller uses: with get_conn() as conn: ...
    return get_pool().connection()


def close_pool() -> None:
    """Close the pool and stop worker threads (safe to call multiple times)."""
    # explicit shutdown to avoid "couldn't stop thread ..." messages
    global _POOL
    if _POOL is not None:
        _POOL.close()
        _POOL = None
