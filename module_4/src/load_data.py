"""Module-3 loader entry point.

Usage (canonical):
  python module_3/load_data.py --init \
    --load module_3/data/module_2llm_extend_applicant_data.json \
    --batch 2000 --count

- --init: create/verify schema (degree TEXT migration included)
- --load: load JSON array into public.applicants in batches
- --count: print final row count
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.dal.pool import close_pool
from src.dal.schema import init_schema
from src.dal.loader import load_json, first_ids


def parse_args() -> argparse.Namespace:
    """Parse CLI args (student helper)."""
    p = argparse.ArgumentParser(description="Module-3 data loader")
    p.add_argument("--init", action="store_true", help="create/verify schema")
    p.add_argument("--load", type=str, help="path to JSON array from Module-2 (LLM)")
    p.add_argument("--batch", type=int, default=2000, help="insert batch size (default: 2000)")
    p.add_argument("--count", action="store_true", help="print final row count")
    return p.parse_args()


def main() -> None:
    """Run requested steps: init, load, count."""
    args = parse_args()

    if args.init:
        init_schema()
        print("schema: ensured (degree TEXT)")

    if args.load:
        json_path = Path(args.load)
        if not json_path.exists():
            raise SystemExit(f"input not found: {json_path}")

        total, inserted, skipped, issue_counts, report_path = load_json(str(json_path), batch=args.batch)
        samples = first_ids(str(json_path), 3)  # <-- fixed: pass path then k
        print(
            f"loaded_records={total} inserted={inserted} skipped={skipped}\n"
            f"issues={issue_counts} sample_ids={samples}\n"
            f"report={report_path}"
        )

    if args.count:
        # tiny count via SELECT COUNT(*) kept in query_data or schema utils
        from src.dal.schema import count_rows
        print(f"row_count={count_rows()}")


if __name__ == "__main__":
    try:
        main()
    finally:
        # always close the pool at exit (student cleanup)
        close_pool()
