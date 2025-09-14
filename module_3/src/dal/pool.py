# pooled PostgreSQL connection (psycopg v3)
from psycopg_pool import ConnectionPool
from src.config import database_url

# create a small pool for fast CLI ops
POOL = ConnectionPool(database_url(), min_size=0, max_size=8)

def get_conn():
    # get a pooled connection; returns to pool on context exit
    return POOL.connection()
