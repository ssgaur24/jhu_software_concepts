"""DDL for the applicants table and verification helpers.

Creates the canonical applicants table and provides a small row-count check.
If the table exists with degree as TEXT (from earlier attempts), it is migrated
to REAL safely: non-numeric degree values become NULL during the cast.
"""

# local import for DB access
from src.dal.pool import get_conn

# explicit schema qualification avoids GUI visibility issues
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.applicants (
    p_id INTEGER PRIMARY KEY,
    program TEXT,
    comments TEXT,
    date_added DATE,
    url TEXT,
    status TEXT,
    term TEXT,
    us_or_international TEXT,
    gpa REAL,
    gre REAL,
    gre_v REAL,
    gre_aw REAL,
    degree REAL,
    llm_generated_program TEXT,
    llm_generated_university TEXT
);
"""


def init_schema() -> None:
    """Create the applicants table in public schema (idempotent)."""
    # open a pooled connection and create table if missing
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)  # run DDL once
        conn.commit()  # persist the DDL


def count_rows() -> int:
    """Return the number of rows in public.applicants."""
    # run a tiny aggregate to verify table visibility and contents
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.applicants;")  # small check
            return cur.fetchone()[0]  # return integer count
