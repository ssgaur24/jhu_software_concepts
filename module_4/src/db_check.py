# coverage: ignore file
"""Tiny DB visibility checker.

Prints:
- config source (env vs ini) and masked URL,
- current_database, current_user, current_schema,
- whether 'public.applicants' exists,
- applicants row count (if exists).
"""

from src.config import database_url_and_source, masked_url
from src.dal.pool import get_conn, close_pool


def main() -> None:
    url, src = database_url_and_source()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user, current_schema()")
                db, user, schema = cur.fetchone()

                cur.execute("SELECT to_regclass('public.applicants') IS NOT NULL")
                exists = bool(cur.fetchone()[0])

                rows = None
                if exists:
                    cur.execute("SELECT COUNT(*) FROM public.applicants")
                    rows = cur.fetchone()[0]

        print(f"config_source={src}")
        print(f"url={masked_url(url)}")
        print(f"db={db} user={user} schema={schema}")
        print(f"applicants_exists={exists}" + (f" rows={rows}" if rows is not None else ""))

    finally:
        close_pool()


if __name__ == "__main__":
    main()
