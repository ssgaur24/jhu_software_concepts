"""Module-3 entry-point script to initialize schema and load JSON.

Usage:
    python module_3/load_data.py --init \
      --load module_3/data/module_2llm_extend_applicant_data.json \
      --batch 2000 --count
"""

# stdlib imports
import argparse

# local imports
from src.dal.schema import init_schema, count_rows
from src.dal.loader import load_json, first_ids
from src.dal.pool import close_pool


def main() -> None:
    """Parse CLI flags and run init/load/count as requested."""
    # set up CLI flags as per assignment
    p = argparse.ArgumentParser(description="Load Module-2 cleaned applicants into PostgreSQL")
    p.add_argument("--init", action="store_true", help="create applicants table if not exists")
    p.add_argument("--load", metavar="PATH", help="path to cleaned applicants JSON (Module-2)")
    p.add_argument("--batch", type=int, default=2000, help="batch size for inserts (default: 2000)")
    p.add_argument("--count", action="store_true", help="print total rows in applicants")
    args = p.parse_args()

    try:
        # optionally create schema first
        if args.init:
            init_schema()

        # optionally load data in fast batches
        if args.load:
            total, inserted, skipped, issue_counts, report_path = load_json(args.load, batch=args.batch)
            # tiny, fast summary print
            print(
                f"loaded_records={total} inserted={inserted} skipped={skipped} "
                f"issues={issue_counts} sample_ids={first_ids(3)}"
            )
            # path to detailed audit file (kept small and readable)
            print(f"report={report_path}")

        # optionally print a tiny final count
        if args.count:
            print(f"rows={count_rows()}")

    finally:
        # cleanly stop pool threads on exit
        close_pool()


if __name__ == "__main__":
    # standard python entrypoint
    main()
