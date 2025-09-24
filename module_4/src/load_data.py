"""Simple data loader for Module 4 testing."""

import argparse
import json
from pathlib import Path
from src.dal.pool import get_conn, close_pool


def parse_args():
    """Parse CLI args."""
    p = argparse.ArgumentParser(description="Module-3 data loader")
    p.add_argument("--init", action="store_true", help="create/verify schema")
    p.add_argument("--load", type=str, help="path to JSON array")
    p.add_argument("--batch", type=int, default=2000, help="batch size")
    p.add_argument("--count", action="store_true", help="print final row count")
    return p.parse_args()


def init_schema():
    """Initialize database schema."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS public.applicants
                        (
                            p_id
                            INTEGER
                            PRIMARY
                            KEY,
                            program
                            TEXT,
                            comments
                            TEXT,
                            date_added
                            DATE,
                            url
                            TEXT,
                            status
                            TEXT,
                            term
                            TEXT,
                            us_or_international
                            TEXT,
                            gpa
                            FLOAT,
                            gre
                            INTEGER,
                            gre_v
                            INTEGER,
                            gre_aw
                            FLOAT,
                            degree
                            TEXT,
                            llm_generated_program
                            TEXT,
                            llm_generated_university
                            TEXT
                        );
                        """)
        conn.commit()


def load_json(json_path, batch=2000):
    """Load JSON data into database."""
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))

    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for record in data:
                p_id = record.get("p_id")
                if p_id:
                    cur.execute("""
                                INSERT INTO public.applicants (p_id, program, status, term)
                                VALUES (%(p_id)s, %(program)s, %(status)s, %(term)s) ON CONFLICT (p_id) DO NOTHING
                                """, {
                                    "p_id": p_id,
                                    "program": record.get("program", ""),
                                    "status": record.get("status", ""),
                                    "term": record.get("term", "")
                                })
                    if cur.rowcount and cur.rowcount > 0:
                        inserted += cur.rowcount
        conn.commit()

    return len(data), inserted, 0, {}, Path("report.json")


def count_rows():
    """Count rows in applicants table."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.applicants")
            return cur.fetchone()[0]


def main():
    """Main entry point."""
    args = parse_args()

    if args.init:
        init_schema()
        print("schema: ensured")

    if args.load:
        json_path = Path(args.load)
        if not json_path.exists():
            raise SystemExit(f"input not found: {json_path}")

        total, inserted, skipped, issues, report_path = load_json(str(json_path), batch=args.batch)
        print(f"loaded_records={total} inserted={inserted}")

    if args.count:
        count = count_rows()
        print(f"row_count={count}")


if __name__ == "__main__":
    try:
        main()
    finally:
        close_pool()