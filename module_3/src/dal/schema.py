"""DDL for the applicants table and verification helpers.

Creates the canonical applicants table and provides a small row-count check.
If the table exists with degree as TEXT (from earlier attempts), it is migrated
to REAL safely: non-numeric degree values become NULL during the cast.
"""

from src.dal.pool import get_conn

# canonical schema (categorical fields TEXT; numeric scores REAL; one DATE)
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS applicants (
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
    """Create the applicants table and ensure `degree` is REAL (idempotent)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Create table if missing
            cur.execute(SCHEMA_SQL)

            # Minimal migration: if degree is not REAL, coerce to REAL.
            # Non-numeric existing values are turned into NULL safely.
            cur.execute("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = 'applicants' AND column_name = 'degree'
            """)
            row = cur.fetchone()
            if row and row[0].lower() != "real":
                cur.execute("""
                    ALTER TABLE applicants
                    ALTER COLUMN degree TYPE REAL
                    USING NULLIF(regexp_replace(degree::text, '[^0-9\\.-]+', '', 'g'), '')::real
                """)
        conn.commit()

def count_rows() -> int:
    """Return the number of rows in applicants."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            return cur.fetchone()[0]
