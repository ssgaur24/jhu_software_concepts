# -*- coding: utf-8 -*-
"""
query_data.py — Single source of truth for Q1–Q12

This module:
1) Reads PostgreSQL settings from config.ini [db].
2) Provides get_rows() for reuse by Flask (returns a list of (question, answer)).
3) Still works as a CLI script: running it prints Q1–Q12 to stdout.

"""

from __future__ import annotations

import configparser
from typing import List, Tuple

import psycopg


def _read_db_config(path: str = "config.ini") -> dict:
    """Read [db] from config.ini and return psycopg.connect kwargs."""
    cfg = configparser.ConfigParser()
    if not cfg.read(path) or "db" not in cfg:
        print("ERROR: config.ini with [db] is required next to this script.")
        raise SystemExit(1)
    db = cfg["db"]
    return {
        "host": db.get("host", "localhost"),
        "port": int(db.get("port", "5432")),
        "dbname": db.get("database", ""),
        "user": db.get("user", ""),
        "password": db.get("password", ""),
    }


def _one_value(cur, sql: str) -> float | int | str | None:
    """Run a query and return the first column of the first row (or None)."""
    cur.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None


def get_rows() -> List[Tuple[str, str]]:
    """
    Compute Q1–Q12 and return a list of (question, answer) pairs.
    This function is used by Flask so the queries live in one place.
    """
    items: List[Tuple[str, str]] = []
    with psycopg.connect(**_read_db_config()) as conn:
        with conn.cursor() as cur:
            # Q1
            q1 = _one_value(cur, """
                SELECT COUNT(*) FROM public.applicants
                WHERE term ILIKE '%fall%' AND term ILIKE '%2025%';
            """)
            items.append(("How many entries applied for Fall 2025?", f"Applicant count: {int(q1 or 0)}"))

            # Q2
            q2 = _one_value(cur, """
                SELECT CASE WHEN COUNT(*)=0 THEN 0
                            ELSE ROUND(
                                (SUM(CASE WHEN LOWER(us_or_international)='international' THEN 1 ELSE 0 END)::numeric*100)
                                / COUNT(*), 2)
                       END
                FROM public.applicants;
            """)
            items.append(("Percent International (to two decimals):", f"{float(q2 or 0):.2f}%"))

            # Q3
            cur.execute("""
                SELECT AVG(gpa), AVG(gre), AVG(gre_v), AVG(gre_aw)
                FROM public.applicants
                WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
            """)
            a = cur.fetchone() or (None, None, None, None)
            def fmt(x): return "NA" if x is None else f"{float(x):.2f}"
            items.append(("Average GPA, GRE, GRE V, GRE AW (where provided):",
                          f"GPA={fmt(a[0])}, GRE={fmt(a[1])}, GRE_V={fmt(a[2])}, GRE_AW={fmt(a[3])}"))

            # Q4
            q4 = _one_value(cur, """
                SELECT AVG(gpa) FROM public.applicants
                WHERE LOWER(us_or_international)='american'
                  AND term ILIKE '%fall%' AND term ILIKE '%2025%'
                  AND gpa IS NOT NULL;
            """)
            items.append(("Avg GPA of American students in Fall 2025:", f"{'NA' if q4 is None else f'{float(q4):.2f}'}"))

            # Q5
            q5 = _one_value(cur, """
                WITH base AS (
                  SELECT status FROM public.applicants
                  WHERE term ILIKE '%fall%' AND term ILIKE '%2025%'
                )
                SELECT CASE WHEN COUNT(*)=0 THEN 0
                            ELSE ROUND(
                              (SUM(CASE WHEN status ILIKE 'accept%%' THEN 1 ELSE 0 END)::numeric * 100) / COUNT(*), 2)
                       END
                FROM base;
            """)
            items.append(("Percent Acceptances in Fall 2025 (to two decimals):", f"{float(q5 or 0):.2f}%"))

            # Q6
            q6 = _one_value(cur, """
                SELECT AVG(gpa) FROM public.applicants
                WHERE term ILIKE '%fall%' AND term ILIKE '%2025%'
                  AND status ILIKE 'accept%%'
                  AND gpa IS NOT NULL;
            """)
            items.append(("Avg GPA of Fall 2025 Acceptances:", f"{'NA' if q6 is None else f'{float(q6):.2f}'}"))

            # Q7
            q7 = _one_value(cur, """
                SELECT COUNT(*) FROM public.applicants
                WHERE (llm_generated_university ILIKE '%hopkins%' OR program ILIKE '%hopkins%')
                  AND (llm_generated_program ILIKE '%computer science%' OR program ILIKE '%computer science%')
                  AND degree ILIKE '%master%';
            """)
            items.append(("JHU Masters in CS — count:", f"{int(q7 or 0)}"))

            # Q8
            q8 = _one_value(cur, """
                SELECT COUNT(*) FROM public.applicants
                WHERE term ILIKE '%2025%'
                  AND status ILIKE 'accept%%'
                  AND (llm_generated_university ILIKE '%georgetown%' OR program ILIKE '%georgetown%')
                  AND (llm_generated_program ILIKE '%computer science%' OR program ILIKE '%computer science%')
                  AND degree ILIKE '%phd%';
            """)
            items.append(("2025 Acceptances — Georgetown PhD CS — count:", f"{int(q8 or 0)}"))

            # Q9 (Top 5 universities)
            cur.execute("""
                SELECT COALESCE(NULLIF(TRIM(llm_generated_university), ''), NULLIF(split_part(program, ' - ', 1), '')) AS uni,
                       COUNT(*) AS c
                FROM public.applicants
                WHERE term ILIKE '%fall%' AND term ILIKE '%2025%'
                GROUP BY uni
                HAVING uni IS NOT NULL
                ORDER BY c DESC, uni ASC
                LIMIT 5;
            """)
            rows = cur.fetchall()
            items.append(("Top 5 Universities (Fall 2025):", ", ".join([f"{u}={int(c)}" for u, c in rows]) or "NA"))

            # Q10 (Status %)
            cur.execute("""
                WITH base AS (
                    SELECT status
                    FROM public.applicants
                    WHERE term ILIKE '%fall%' AND term ILIKE '%2025%' AND status IS NOT NULL
                ), total AS (SELECT COUNT(*)::numeric n FROM base)
                SELECT status,
                       ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM total), 0), 2) AS pct
                FROM base
                GROUP BY status
                ORDER BY pct DESC, status ASC;
            """)
            rows = cur.fetchall()
            items.append(("Status Breakdown % (Fall 2025):", ", ".join([f"{s}={float(p):.2f}%" for s, p in rows]) or "NA"))

            # Q11 (Top 10 universities)
            cur.execute("""
                SELECT COALESCE(NULLIF(TRIM(llm_generated_university), ''), NULLIF(split_part(program, ' - ', 1), '')) AS uni,
                       COUNT(*) AS c
                FROM public.applicants
                WHERE term ILIKE '%fall%' AND term ILIKE '%2025%'
                GROUP BY uni
                HAVING uni IS NOT NULL
                ORDER BY c DESC, uni ASC
                LIMIT 10;
            """)
            rows = cur.fetchall()
            items.append(("Top 10 Universities (Fall 2025):", ", ".join([f"{u}={int(c)}" for u, c in rows]) or "NA"))

            # Q12 (Status % again as a separate item)
            cur.execute("""
                WITH base AS (
                    SELECT status
                    FROM public.applicants
                    WHERE term ILIKE '%fall%' AND term ILIKE '%2025%' AND status IS NOT NULL
                ), total AS (SELECT COUNT(*)::numeric n FROM base)
                SELECT status,
                       ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM total), 0), 2) AS pct
                FROM base
                GROUP BY status
                ORDER BY pct DESC, status ASC;
            """)
            rows = cur.fetchall()
            items.append(("Status Breakdown % (Fall 2025) — Q12:", ", ".join([f"{s}={float(p):.2f}%" for s, p in rows]) or "NA"))

    return items


def main() -> None:
    """CLI entry point: print Q1–Q12."""
    rows = get_rows()
    for q, a in rows:
        print(f"- {q}\n  {a}")


if __name__ == "__main__":
    main()
