# -*- coding: utf-8 -*-
"""
load_data.py  â€"  Minimal loader for Module 3

Purpose
-------
Read the cleaned JSON produced in Module 2 and load it into a PostgreSQL table
named 'applicants'.

How to run
----------
1) Set your database URL (psycopg3 style), for example:
   Windows (PowerShell):   $env:DATABASE_URL="postgresql://user:pass@localhost:5432/yourdb"
   macOS/Linux (bash):     export DATABASE_URL="postgresql://user:pass@localhost:5432/yourdb"

2) Place your Module 2 cleaned file at one of these paths OR pass it as an arg:
   - module_2/llm_extend_applicant_data.json   (typical repo path), or
   - any path you pass as the first CLI argument.

3) Run:
   python module_3/load_data.py
   or
   python module_3/load_data.py path/to/your_cleaned.json

Notes
-----
- This script has been updated to the simpler implementation.
- Table schema matches the Module 3 assignment columns.
- The JSON often stores numbers inside strings (e.g., "GPA 3.76"). We extract
  the numeric part when possible; otherwise we store NULL.
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Optional
import configparser
import psycopg


def _num(text: Any) -> Optional[float]:
    """
    Return the first number in a string as float, or None.

    Examples:
      "GPA 3.76"  -> 3.76
      "152"       -> 152.0
      "" / None   -> None
    """
    if text is None:
        return None
    s = str(text).strip()
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None

def _read_db_config(path: str = "config.ini") -> dict:
    """
    Read PostgreSQL connection settings from config.ini under [db].
    Returns a dict with keys that psycopg.connect(...) accepts directly.
    """
    cfg = configparser.ConfigParser()
    if not cfg.read(path):
        print(f"ERROR: Could not read '{path}'. Make sure it exists and has a [db] section.")
        sys.exit(1)

    if "db" not in cfg:
        print("ERROR: 'config.ini' is missing the [db] section.")
        sys.exit(1)

    section = cfg["db"]
    # Map INI keys to psycopg.connect keyword args
    return {
        "host": section.get("host", "localhost"),
        "port": int(section.get("port", "5432")),
        "dbname": section.get("database", ""),
        "user": section.get("user", ""),
        "password": section.get("password", ""),
    }

def _extract_row_data(item: dict) -> tuple:
    """Extract and process data from a JSON item for database insertion."""
    # Basic fields
    program = item.get("program", "")
    comments = item.get("comments", "")
    date_added = item.get("date_added", None)
    url = item.get("url", "")
    status = item.get("status", "")
    term = item.get("term", "")
    us_intl = item.get("US/International", "")

    # Numeric fields
    gpa = _num(item.get("GPA"))
    gre = _num(item.get("gre"))
    gre_v = _num(item.get("gre_v"))
    gre_aw = _num(item.get("gre_aw"))

    # Text fields
    degree = item.get("Degree", "")
    llm_prog = item.get("llm-generated-program", "")
    llm_univ = item.get("llm-generated-university", "")

    return (
        program, comments, date_added, url, status, term,
        us_intl, gpa, gre, gre_v, gre_aw, degree,
        llm_prog, llm_univ,
    )

def main() -> None:
    """Load cleaned JSON data into PostgreSQL applicants table."""
    # 1) Read Database Connection parameters
    cfg = _read_db_config("config.ini")

    if not cfg:
        print("ERROR: DATABASE properties are not set.")
        print('Check config.ini.')
        sys.exit(1)

    # 2) Resolve JSON path (default or first CLI arg)
    json_path = sys.argv[1] if len(sys.argv) > 1 else "data/module2llm_extend_applicant_data.json"

    # 3) Load JSON rows from file
    with open(json_path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    # 4) Connect and create table if not exists
    with psycopg.connect(**cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.applicants (
                    p_id SERIAL PRIMARY KEY,
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
                    degree TEXT,  -- corrected: degree is TEXT
                    llm_generated_program TEXT,
                    llm_generated_university TEXT
                );
                """
            )

            # 5) Prepare simple insert statement
            insert_sql = """
                INSERT INTO public.applicants
                (program, comments, date_added, url, status, term,
                 us_or_international, gpa, gre, gre_v, gre_aw, degree,
                 llm_generated_program, llm_generated_university)
                VALUES
                (%s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s,
                 %s, %s);
            """

            # 6) Insert each row
            for item in rows:
                row_data = _extract_row_data(item)
                cur.execute(insert_sql, row_data)

        # 7) Commit once at the end
        conn.commit()

    print(f"Loaded {len(rows)} rows into 'applicants' from: {json_path}")


if __name__ == "__main__":
    main()
