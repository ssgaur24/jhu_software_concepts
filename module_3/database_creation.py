# CLI: init the schema and print count
import argparse
from src.dal.schema import init_schema, count_rows
from src.dal.pool import close_pool  # ensure clean shutdown

def main() -> None:
    # parse flags and run actions
    p = argparse.ArgumentParser(description="Module-3 schema init and tiny check")
    p.add_argument("--init", action="store_true", help="create applicants table if not exists")
    p.add_argument("--count", action="store_true", help="print total rows in applicants")
    args = p.parse_args()

    try:
        if args.init:
            init_schema()
        if args.count:
            print(f"rows={count_rows()}")
    finally:
        # FIX: stop pool worker threads (fix for 'couldn't stop thread ...' hints)
        close_pool()

if __name__ == "__main__":
    main()
