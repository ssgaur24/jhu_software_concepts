# CLI: init schema, load JSON, and print counts/previews
import argparse
from src.dal.schema import init_schema, count_rows
from src.dal.loader import load_json, first_ids
from src.dal.pool import close_pool  # ensure clean shutdown

def main() -> None:
    # parse flags and run actions
    p = argparse.ArgumentParser(description="Module-3 schema + loader (tiny checks)")
    p.add_argument("--init", action="store_true", help="create applicants table if not exists")
    p.add_argument("--count", action="store_true", help="print total rows in applicants")
    p.add_argument("--load", metavar="PATH", help="path to Module-2 cleaned JSON array")
    args = p.parse_args()

    try:
        if args.init:
            init_schema()

        if args.load:
            total, inserted = load_json(args.load)
            sample = first_ids(3)
            print(f"loaded_records={total} inserted={inserted} sample_ids={sample}")

        if args.count:
            print(f"rows={count_rows()}")

    finally:
        # stop pool worker threads
        close_pool()

if __name__ == "__main__":
    main()
