# -*- coding: utf-8 -*-
"""
query_data.py — Q&A text for the Analysis page (and CLI)

- Reads DB settings from config.ini [db]
- get_rows(): returns [(question, answer), ...] used by Flask UI
- CLI usage: python module_3/query_data.py  -> prints the same Q&A
"""

from __future__ import annotations

import configparser
from typing import List, Tuple

import psycopg


def _read_db_config(path: str = "config.ini") -> dict:
    """
    Reads database configuration details from config.ini
    """
    cfg = configparser.ConfigParser()
    if not cfg.read(path) or "db" not in cfg:
        print("ERROR: config.ini with [db] is required.")
        raise SystemExit(1)
    s = cfg["db"]
    return {
        "host": s.get("host", "localhost"),
        "port": int(s.get("port", "5432")),
        "dbname": s.get("database", ""),
        "user": s.get("user", ""),
        "password": s.get("password", ""),
    }


def _one_value(cur, sql: str) -> float | int | str | None:
    """
    Execute query given in input and return the result
    """
    cur.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None

def _latest_term_filter(cur) -> tuple[str, str]:
    """
    Returns (label, sql_condition) for the most recent term in the data.
    label -> e.g., "Fall 2025" or "2025" or "the entire dataset"
    sql_condition -> e.g., "term ILIKE '%fall%' AND term ILIKE '%2025%'", or just "TRUE" if no term found
    """
    # 1) Try to pick the latest year from date_added
    cur.execute("""
        SELECT EXTRACT(YEAR FROM MAX(date_added))::int
        FROM public.applicants
        WHERE date_added IS NOT NULL;
    """)
    r = cur.fetchone()
    year = r[0] if r and r[0] is not None else None

    if year is None:
        # 2) Fallback: derive latest year from the term text
        cur.execute("""
            WITH yrs AS (
              SELECT (regexp_matches(term, '(19|20)\\d{2}', 'g'))[1]::int AS y
              FROM public.applicants
              WHERE term IS NOT NULL
            )
            SELECT MAX(y) FROM yrs;
        """)
        r = cur.fetchone()
        year = r[0] if r and r[0] is not None else None

    if year is None:
        return ("the entire dataset", "TRUE")

    # 3) Pick the season within that year with the most rows (fall/spring/summer/winter)
    cur.execute(f"""
        SELECT s, c FROM (
          SELECT 'fall'   AS s, COUNT(*) AS c FROM public.applicants WHERE term ILIKE '%fall%'   AND term ILIKE '%{year}%'
          UNION ALL
          SELECT 'spring' AS s, COUNT(*) AS c FROM public.applicants WHERE term ILIKE '%spring%' AND term ILIKE '%{year}%'
          UNION ALL
          SELECT 'summer' AS s, COUNT(*) AS c FROM public.applicants WHERE term ILIKE '%summer%' AND term ILIKE '%{year}%'
          UNION ALL
          SELECT 'winter' AS s, COUNT(*) AS c FROM public.applicants WHERE term ILIKE '%winter%' AND term ILIKE '%{year}%'
        ) t
        ORDER BY c DESC, s ASC
        LIMIT 1;
    """)
    r = cur.fetchone()
    season = (r[0] if r and r[1] and int(r[1]) > 0 else None)

    if season:
        return (f"{season.title()} {year}", f"term ILIKE '%{season}%' AND term ILIKE '%{year}%'")
    else:
        return (f"{year}", f"term ILIKE '%{year}%'")

_TERM_SEASON = "fall"
_TERM_YEAR = "2024"


def get_rows() -> List[Tuple[str, str]]:
    rows_out: List[Tuple[str, str]] = []
    with psycopg.connect(**_read_db_config()) as conn:
        with conn.cursor() as cur:

            # ---------------- Q1–Q8 (same as before) ----------------
            q_text = "How many entries do you have in your database who have applied for Fall 2024?"
            q1 = _one_value(cur, f"""
                SELECT COUNT(*) FROM public.applicants
                WHERE term ILIKE '%{_TERM_SEASON}%' AND term ILIKE '%{_TERM_YEAR}%';
            """)
            rows_out.append((q_text, f"Answer: Applicant count: {int(q1 or 0)}"))

            q_text = "What percentage of entries are from International students (not American or Other) (to two decimal places)?"
            q2 = _one_value(cur, """
                SELECT CASE WHEN COUNT(*)=0 THEN 0
                            ELSE ROUND(
                              (SUM(CASE WHEN LOWER(us_or_international)='international' THEN 1 ELSE 0 END)::numeric*100)
                              / COUNT(*), 2)
                       END
                FROM public.applicants;
            """)
            rows_out.append((q_text, f"Answer: Percent International: {float(q2 or 0):.2f}"))

            q_text = "What is the average GPA, GRE, GRE V, GRE AW of applicants who provided these metrics?"
            cur.execute("""
                SELECT AVG(gpa), AVG(gre), AVG(gre_v), AVG(gre_aw)
                FROM public.applicants
                WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
            """)
            a = cur.fetchone() or (None, None, None, None)
            def _fmt(x): return "NA" if x is None else f"{float(x):.2f}"
            rows_out.append((q_text,
                             f"Answer: Average GPA: {_fmt(a[0])}, Average GRE: {_fmt(a[1])}, "
                             f"Average GRE V: {_fmt(a[2])}, Average GRE AW: {_fmt(a[3])}"))

            q_text = "What is the average GPA of American students in Fall 2024?"
            q4 = _one_value(cur, f"""
                SELECT AVG(gpa) FROM public.applicants
                WHERE LOWER(us_or_international)='american'
                  AND term ILIKE '%{_TERM_SEASON}%' AND term ILIKE '%{_TERM_YEAR}%'
                  AND gpa IS NOT NULL;
            """)
            rows_out.append((q_text, f"Answer: Average GPA American: {'NA' if q4 is None else f'{float(q4):.2f}'}"))

            q_text = "What percent of entries for Fall 2024 are Acceptances (to two decimal places)?"
            q5 = _one_value(cur, f"""
                WITH base AS (
                    SELECT status FROM public.applicants
                    WHERE term ILIKE '%{_TERM_SEASON}%' AND term ILIKE '%{_TERM_YEAR}%'
                )
                SELECT CASE WHEN COUNT(*)=0 THEN 0
                            ELSE ROUND(
                              (SUM(CASE WHEN status ILIKE 'accept%%' THEN 1 ELSE 0 END)::numeric * 100) / COUNT(*), 2)
                       END
                FROM base;
            """)
            rows_out.append((q_text, f"Answer: Acceptance percent: {float(q5 or 0):.2f}"))

            q_text = "What is the average GPA of applicants who applied for Fall 2024 who applied for Acceptances?"
            q6 = _one_value(cur, f"""
                SELECT AVG(gpa) FROM public.applicants
                WHERE term ILIKE '%{_TERM_SEASON}%' AND term ILIKE '%{_TERM_YEAR}%'
                  AND status ILIKE 'accept%%' AND gpa IS NOT NULL;
            """)
            rows_out.append((q_text, f"Answer: Average GPA Acceptance: {'NA' if q6 is None else f'{float(q6):.2f}'}"))

            q_text = "How many entries applied to Johns Hopkins University for a Masters in Computer Science?"
            q7 = _one_value(cur, """
                SELECT COUNT(*) FROM public.applicants
                WHERE (llm_generated_university ILIKE '%hopkins%' OR program ILIKE '%hopkins%')
                  AND (llm_generated_program ILIKE '%computer science%' OR program ILIKE '%computer science%')
                  AND degree ILIKE '%master%';
            """)
            rows_out.append((q_text, f"Answer: Count: {int(q7 or 0)}"))

            q_text = "How many 2024 entries are Acceptances to Georgetown University for a PhD in Computer Science?"
            q8 = _one_value(cur, f"""
                SELECT COUNT(*) FROM public.applicants
                WHERE term ILIKE '%{_TERM_YEAR}%'
                  AND status ILIKE 'accept%%'
                  AND (llm_generated_university ILIKE '%georgetown%' OR program ILIKE '%georgetown%')
                  AND (llm_generated_program ILIKE '%computer science%' OR program ILIKE '%computer science%')
                  AND degree ILIKE '%phd%';
            """)
            rows_out.append((q_text, f"Answer: Count: {int(q8 or 0)}"))

            # ---------------- custom questions (Q9–Q12) ----------------

            label, term_cond = _latest_term_filter(cur)

            # Q9
            q_text = f"Which five universities have the most entries in the most recent term ({label})?"
            cur.execute(f"""
                            WITH base AS (
                                SELECT COALESCE(
                                         NULLIF(TRIM(llm_generated_university), ''),
                                         NULLIF(split_part(program, ' - ', 1), '')
                                       ) AS uni
                                FROM public.applicants
                                WHERE {term_cond}
                            ),
                            agg AS (
                                SELECT uni, COUNT(*) AS c
                                FROM base
                                WHERE uni IS NOT NULL
                                GROUP BY uni
                            )
                            SELECT uni, c
                            FROM agg
                            ORDER BY c DESC, uni ASC
                            LIMIT 5;
                        """)
            r9 = cur.fetchall()
            if r9:
                top5 = "; ".join([f"{i + 1}) {u} — {int(c)}" for i, (u, c) in enumerate(r9)])
                rows_out.append((q_text, f"Answer: Top 5 by entries — {top5}"))
            else:
                rows_out.append((q_text, "Answer: No entries found for that term (showing none)."))

            # Q10
            q_text = f"What is the outcome mix in the most recent term ({label})?"
            cur.execute(f"""
                            WITH base AS (
                                SELECT split_part(TRIM(status), ' ', 1) AS cat
                                FROM public.applicants
                                WHERE {term_cond} AND status IS NOT NULL
                            ),
                            totals AS (SELECT COUNT(*)::numeric n FROM base)
                            SELECT cat, COUNT(*) AS c,
                                   ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM totals), 0), 2) AS pct
                            FROM base
                            GROUP BY cat
                            ORDER BY c DESC, cat ASC;
                        """)
            r10 = cur.fetchall()
            if r10:
                total = sum(int(c) for _, c, _ in r10)
                mix = "; ".join([f"{cat} — {float(p):.2f}% ({int(c)}/{total})" for cat, c, p in r10])
                rows_out.append((q_text, f"Answer: {mix}"))
            else:
                rows_out.append((q_text, "Answer: No status information recorded for that term."))

            # Q11
            q_text = f"Which degree types are most common in the most recent term ({label})?"
            cur.execute(f"""
                            WITH base AS (
                                SELECT CASE
                                         WHEN degree ILIKE '%phd%'    THEN 'PhD'
                                         WHEN degree ILIKE '%master%' THEN 'Masters'
                                         ELSE COALESCE(NULLIF(TRIM(degree), ''), 'Other')
                                       END AS deg
                                FROM public.applicants
                                WHERE {term_cond}
                            ),
                            totals AS (SELECT COUNT(*)::numeric n FROM base)
                            SELECT deg, COUNT(*) AS c,
                                   ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM totals), 0), 2) AS pct
                            FROM base
                            GROUP BY deg
                            ORDER BY c DESC, deg ASC;
                        """)
            r11 = cur.fetchall()
            if r11:
                total = sum(int(c) for _, c, _ in r11)
                deg_mix = "; ".join([f"{deg} — {float(p):.2f}% ({int(c)}/{total})" for deg, c, p in r11])
                rows_out.append((q_text, f"Answer: {deg_mix}"))
            else:
                rows_out.append((q_text, "Answer: No degree information recorded for that term."))

            # Q12
            q_text = f"For the five universities with the most entries in {label}, what is the average GPA at each?"
            cur.execute(f"""
                            WITH b AS (
                                SELECT
                                    COALESCE(
                                      NULLIF(TRIM(llm_generated_university), ''),
                                      NULLIF(split_part(program, ' - ', 1), '')
                                    ) AS uni,
                                    gpa
                                FROM public.applicants
                                WHERE {term_cond}
                            ),
                            topu AS (
                                SELECT uni, COUNT(*) AS c
                                FROM b
                                WHERE uni IS NOT NULL
                                GROUP BY uni
                                ORDER BY c DESC, uni ASC
                                LIMIT 5
                            )
                            SELECT t.uni,
                                   COUNT(b.gpa) AS n_with_gpa,
                                   ROUND(AVG(b.gpa)::numeric, 2) AS avg_gpa
                            FROM topu t
                            LEFT JOIN b ON b.uni = t.uni AND b.gpa IS NOT NULL
                            GROUP BY t.uni, t.c
                            ORDER BY t.c DESC, t.uni ASC;
                        """)
            r12 = cur.fetchall()
            if r12:
                gpa_text = "; ".join([
                    f"{i + 1}) {uni} — Avg GPA {('NA' if avg is None else f'{float(avg):.2f}')} "
                    f"(n={int(n)})"
                    for i, (uni, n, avg) in enumerate(r12)
                ])
                rows_out.append((q_text, f"Answer: {gpa_text}"))
            else:
                rows_out.append((q_text, "Answer: No GPA data available for that term."))

        return rows_out

def main() -> None:
    for q, a in get_rows():
        print(f"- {q}\n  {a}")

if __name__ == "__main__":
    main()