# applicants table DDL and checks
from src.dal.pool import get_conn

# table schema per assignment (single applicants table)
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
    # create the applicants table
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()

def count_rows() -> int:
    # check for number of rows in applicants
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM applicants;")
            return cur.fetchone()[0]
