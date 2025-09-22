# coverage: ignore file
"""DDL for the applicants table and verification helpers (Module 3).

- Creates/ensures public.applicants exists with instructor-corrected column types.
- Performs an in-place migration if an older table has degree as REAL: converts to TEXT.
"""

from __future__ import annotations

from typing import Optional
from src.dal.pool import get_conn  # pooled connections


# canonical DDL
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
  degree TEXT,
  llm_generated_program TEXT,
  llm_generated_university TEXT
);
"""


def _current_degree_type() -> Optional[str]:
    """Return the current data_type of public.applicants.degree, or None if missing."""
    sql = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'applicants' AND column_name = 'degree'
        LIMIT 1
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None


def init_schema() -> None:
    """Create applicants table if missing, then migrate degree to TEXT if needed (idempotent)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # create if not exists (degree TEXT)
            cur.execute(SCHEMA_SQL)
            # migrate degree column to TEXT if previously REAL/NUMERIC
            cur.execute("""
                SELECT data_type
                  FROM information_schema.columns
                 WHERE table_schema='public' AND table_name='applicants' AND column_name='degree'
            """)
            row = cur.fetchone()
            if row and row[0].lower() != "text":
                # safe cast existing numeric->text
                cur.execute("ALTER TABLE public.applicants ALTER COLUMN degree TYPE TEXT USING degree::text;")
        conn.commit()


def count_rows() -> int:
    """Return the number of rows in public.applicants."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.applicants;")
            return int(cur.fetchone()[0])
