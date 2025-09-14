# CLI: initialize the schema and print count
import argparse
from src.dal.schema import init_schema, count_rows

def main() -> None:
    # parse flags and run actions
    p = argparse.ArgumentParser(description="Module-3 schema init and check")
    p.add_argument("--init", action="store_true", help="create applicants table if not exists")
    p.add_argument("--count", action="store_true", help="print total rows in applicants")
    args = p.parse_args()

    if args.init:
        init_schema()
    if args.count:
        print(f"rows={count_rows()}")

if __name__ == "__main__":
    main()
