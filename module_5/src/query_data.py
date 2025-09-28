# -*- coding: utf-8 -*-
"""
query_data.py — Q&A text for the Analysis page (and CLI)

- Reads DB settings from config.ini [db]
- get_rows(): returns [(question, answer), ...] used by Flask UI
- CLI usage: python module_3/query_data.py  -> prints the same Q&A
"""

from __future__ import annotations

import configparser
from typing import List, Tuple, Any

import psycopg
from psycopg import sql


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


def _one_value(cur, stmt: sql.Composed, params: tuple[Any, ...] | dict | None = None) \
        -> float | int | str | None:
    """
    Execute a composed SQL statement that returns a single value (first column of first row).
    """
    cur.execute(stmt, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def _latest_term_filter(cur) -> tuple[str, str]:
    """
    Returns (label, sql_condition) for the most recent term in the data.
    label -> e.g., "Fall 2025" or "2025" or "the entire dataset"
    sql_condition -> safe literal-composed string like "term ILIKE '%fall%'
    AND term ILIKE '%2025%'" or "TRUE"
    """
    tbl = sql.Identifier("public", "applicants")

    # 1) Latest year from date_added
    stmt_year_from_date = sql.SQL("""
        SELECT EXTRACT(YEAR FROM MAX(date_added))::int
        FROM {tbl}
        WHERE date_added IS NOT NULL
        LIMIT 1;
    """).format(tbl=tbl)
    cur.execute(stmt_year_from_date)
    r = cur.fetchone()
    year = r[0] if r and r[0] is not None else None

    if year is None:
        # 2) Fallback: derive latest year from term text
        stmt_year_from_term = sql.SQL("""
            WITH yrs AS (
              SELECT (regexp_matches(term, '(19|20)\\d{2}', 'g'))[1]::int AS y
              FROM {tbl}
              WHERE term IS NOT NULL
            )
            SELECT MAX(y) FROM yrs
            LIMIT 1;
        """).format(tbl=tbl)
        cur.execute(stmt_year_from_term)
        r = cur.fetchone()
        year = r[0] if r and r[0] is not None else None

    if year is None:
        return ("the entire dataset", "TRUE")

    # 3) Pick season within that year having most rows
    year_pat = f"%{year}%"
    stmt_season = sql.SQL("""
        SELECT s, c FROM (
          SELECT 'fall'   AS s, COUNT(*) AS c FROM {tbl} WHERE term ILIKE %(fall)s   AND term ILIKE %(y)s
          UNION ALL
          SELECT 'spring' AS s, COUNT(*) AS c FROM {tbl} WHERE term ILIKE %(spring)s AND term ILIKE %(y)s
          UNION ALL
          SELECT 'summer' AS s, COUNT(*) AS c FROM {tbl} WHERE term ILIKE %(summer)s AND term ILIKE %(y)s
          UNION ALL
          SELECT 'winter' AS s, COUNT(*) AS c FROM {tbl} WHERE term ILIKE %(winter)s AND term ILIKE %(y)s
        ) t
        ORDER BY c DESC, s ASC
        LIMIT 1;
    """).format(tbl=tbl)
    cur.execute(
        stmt_season,
        {
            "fall": "%fall%",
            "spring": "%spring%",
            "summer": "%summer%",
            "winter": "%winter%",
            "y": year_pat,
        },
    )
    r = cur.fetchone()
    season = (r[0] if r and r[1] and int(r[1]) > 0 else None)

    if season:
        return (f"{season.title()} {year}", f"term ILIKE '%{season}%' AND term ILIKE '%{year}%'")

    return (f"{year}", f"term ILIKE '%{year}%'")


_TERM_SEASON = "fall"
_TERM_YEAR = "2024"


def get_rows() -> List[Tuple[str, str]]:
    """
    Fetch result of all the queries
    :return: analysis result set
    """
    rows_out: List[Tuple[str, str]] = []
    with psycopg.connect(**_read_db_config()) as conn:
        with conn.cursor() as cur:
            tbl = sql.Identifier("public", "applicants")

            # ---------------- Q1–Q8 ----------------
            q1_5(cur, rows_out, tbl)

            q6_8(cur, rows_out, tbl)

            # ---------------- custom questions (Q9–Q12) ----------------

            label, term_cond = _latest_term_filter(cur)

            q9(cur, label, rows_out, tbl, term_cond)

            q10(cur, label, rows_out, tbl, term_cond)

            q11(cur, label, rows_out, tbl, term_cond)

            q12(cur, label, rows_out, tbl, term_cond)

        return rows_out


def q1_5(cur, rows_out, tbl):
    """
    Query 1-5 analysis
    :param cur: cursor
    :param rows_out: result
    :param tbl: table
    :return: analysis of the queries
    """
    q_text = "How many entries do you have in your database who have applied for Fall 2024?"
    stmt_q1 = sql.SQL("""
                SELECT COUNT(*) FROM {tbl}
                WHERE term ILIKE %(season)s AND term ILIKE 
                %(year)s
                LIMIT 1;
            """).format(tbl=tbl)
    q1 = _one_value(cur, stmt_q1, {"season": f"%{_TERM_SEASON}%", "year":
        f"%{_TERM_YEAR}%"})
    rows_out.append((q_text, f"Answer: Applicant count: {int(q1 or 0)}"))
    q_text = ("What percentage of entries are from International students "
              "(not American or Other) (to two decimal places)?")
    stmt_q2 = sql.SQL("""
                SELECT CASE WHEN COUNT(*)=0 THEN 0
                            ELSE ROUND(
                              (SUM(CASE WHEN LOWER(us_or_international)='international' THEN 1 ELSE 0 END)::numeric*100)
                              / COUNT(*), 2)
                       END
                FROM {tbl}
                LIMIT 1;
            """).format(tbl=tbl)
    q2 = _one_value(cur, stmt_q2)
    rows_out.append((q_text, f"Answer: Percent International: {float(q2 or 0):.2f}"))
    q_text = "What is the average GPA, GRE, GRE V, GRE AW of applicants who provided these metrics?"
    stmt_q3 = sql.SQL("""
                SELECT AVG(gpa), AVG(gre), AVG(gre_v), AVG(gre_aw)
                FROM {tbl}
                WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL
                LIMIT 1;
            """).format(tbl=tbl)
    cur.execute(stmt_q3)
    a = cur.fetchone() or (None, None, None, None)

    def _fmt(x):
        return "NA" if x is None else \
        f"{float(x):.2f}"

    rows_out.append(
        (
            q_text,
            f"Answer: Average GPA: {_fmt(a[0])}, Average GRE: {_fmt(a[1])}, "
            f"Average GRE V: {_fmt(a[2])}, Average GRE AW: {_fmt(a[3])}",
        )
    )
    q_text = "What is the average GPA of American students in Fall 2024?"
    stmt_q4 = sql.SQL("""
                SELECT AVG(gpa) FROM {tbl}
                WHERE LOWER(us_or_international)='american'
                  AND term ILIKE %(season)s AND term ILIKE %(year)s
                  AND gpa IS NOT NULL
                LIMIT 1;
            """).format(tbl=tbl)
    q4 = _one_value(cur, stmt_q4, {"season": f"%{_TERM_SEASON}%", "year": f"%{_TERM_YEAR}%"})
    rows_out.append((q_text, f"Answer: Average GPA American: "
                             f"{'NA' if q4 is None else f'{float(q4):.2f}'}"))
    q_text = "What percent of entries for Fall 2024 are Acceptances (to two decimal places)?"
    stmt_q5 = sql.SQL("""
                WITH base AS (
                    SELECT status FROM {tbl}
                    WHERE term ILIKE %(season)s AND term ILIKE %(year)s
                )
                SELECT CASE WHEN COUNT(*)=0 THEN 0
                            ELSE ROUND(
                              (SUM(CASE WHEN status ILIKE 'accept%%' THEN 1 ELSE 0 END)::numeric * 100) / COUNT(*), 2)
                       END
                FROM base
                LIMIT 1;
            """).format(tbl=tbl)
    q5 = _one_value(cur, stmt_q5, {"season": f"%{_TERM_SEASON}%", "year": f"%{_TERM_YEAR}%"})
    rows_out.append((q_text, f"Answer: Acceptance percent: {float(q5 or 0):.2f}"))


def q6_8(cur, rows_out, tbl):
    """Queries 6-8 analysis"""
    q_text = ("What is the average GPA of applicants who applied for Fall 2024 "
              "who applied for Acceptances?")
    stmt_q6 = sql.SQL("""
                SELECT AVG(gpa) FROM {tbl}
                WHERE term ILIKE %(season)s AND term ILIKE %(year)s
                  AND status ILIKE 'accept%%' AND gpa IS NOT NULL
                LIMIT 1;
            """).format(tbl=tbl)
    q6 = _one_value(cur, stmt_q6, {"season": f"%{_TERM_SEASON}%", "year": f"%{_TERM_YEAR}%"})
    rows_out.append((q_text, f"Answer: Average GPA Acceptance: "
                             f"{'NA' if q6 is None else f'{float(q6):.2f}'}"))
    q_text = ("How many entries applied to Johns Hopkins University for a Masters "
              "in Computer Science?")
    stmt_q7 = sql.SQL("""
                SELECT COUNT(*) FROM {tbl}
                WHERE (llm_generated_university ILIKE %(hopkins)s OR program ILIKE %(hopkins)s)
                  AND (llm_generated_program ILIKE %(cs)s OR program ILIKE %(cs)s)
                  AND degree ILIKE %(masters)s
                LIMIT 1;
            """).format(tbl=tbl)
    q7 = _one_value(
        cur,
        stmt_q7,
        {"hopkins": "%hopkins%", "cs": "%computer science%", "masters": "%master%"},
    )
    rows_out.append((q_text, f"Answer: Count: {int(q7 or 0)}"))
    q_text = ("How many 2024 entries are Acceptances to Georgetown University for a "
              "PhD in Computer Science?")
    stmt_q8 = sql.SQL("""
                SELECT COUNT(*) FROM {tbl}
                WHERE term ILIKE %(year)s
                  AND status ILIKE 'accept%%'
                  AND (llm_generated_university ILIKE %(gtown)s OR program ILIKE %(gtown)s)
                  AND (llm_generated_program ILIKE %(cs)s OR program ILIKE %(cs)s)
                  AND degree ILIKE %(phd)s
                LIMIT 1;
            """).format(tbl=tbl)
    q8 = _one_value(
        cur,
        stmt_q8,
        {"year": f"%{_TERM_YEAR}%", "gtown": "%georgetown%", "cs":
            "%computer science%", "phd": "%phd%"},
    )
    rows_out.append((q_text, f"Answer: Count: {int(q8 or 0)}"))


def q9(cur, label, rows_out, tbl, term_cond):
    """Query 9 analysis"""
    # Q9
    q_text = f"Which five universities have the most entries in the most recent term ({label})?"
    stmt_q9 = sql.SQL("""
                WITH base AS (
                    SELECT COALESCE(
                             NULLIF(TRIM(llm_generated_university), ''),
                             NULLIF(split_part(program, ' - ', 1), '')
                           ) AS uni
                    FROM {tbl}
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
            """).format(tbl=tbl, term_cond=sql.SQL(term_cond))
    cur.execute(stmt_q9)
    r9 = cur.fetchall()
    if r9:
        top5 = "; ".join([f"{i + 1}) {u} — {int(c)}" for i, (u, c) in enumerate(r9)])
        rows_out.append((q_text, f"Answer: Top 5 by entries — {top5}"))
    else:
        rows_out.append((q_text, "Answer: No entries found for that term (showing none)."))


def q10(cur, label, rows_out, tbl, term_cond):
    """Query 10 analysis"""
    # Q10
    q_text = f"What is the outcome mix in the most recent term ({label})?"
    stmt_q10 = sql.SQL("""
                WITH base AS (
                    SELECT split_part(TRIM(status), ' ', 1) AS cat
                    FROM {tbl}
                    WHERE {term_cond} AND status IS NOT NULL
                ),
                totals AS (SELECT COUNT(*)::numeric n FROM base)
                SELECT cat, COUNT(*) AS c,
                       ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM totals), 0), 2) AS pct
                FROM base
                GROUP BY cat
                ORDER BY c DESC, cat ASC;
            """).format(tbl=tbl, term_cond=sql.SQL(term_cond))
    cur.execute(stmt_q10)
    r10 = cur.fetchall()
    if r10:
        total = sum(int(c) for _, c, _ in r10)
        mix = "; ".join([f"{cat} — {float(p):.2f}% ({int(c)}/{total})" for cat, c, p in r10])
        rows_out.append((q_text, f"Answer: {mix}"))
    else:
        rows_out.append((q_text, "Answer: No status information recorded for that term."))


def q11(cur, label, rows_out, tbl, term_cond):
    """Query 11 analysis"""
    # Q11
    q_text = f"Which degree types are most common in the most recent term ({label})?"
    stmt_q11 = sql.SQL("""
                WITH base AS (
                    SELECT CASE
                             WHEN degree ILIKE '%phd%'    THEN 'PhD'
                             WHEN degree ILIKE '%master%' THEN 'Masters'
                             ELSE COALESCE(NULLIF(TRIM(degree), ''), 'Other')
                           END AS deg
                    FROM {tbl}
                    WHERE {term_cond}
                ),
                totals AS (SELECT COUNT(*)::numeric n FROM base)
                SELECT deg, COUNT(*) AS c,
                       ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM totals), 0), 2) AS pct
                FROM base
                GROUP BY deg
                ORDER BY c DESC, deg ASC;
            """).format(tbl=tbl, term_cond=sql.SQL(term_cond))
    cur.execute(stmt_q11)
    r11 = cur.fetchall()
    if r11:
        total = sum(int(c) for _, c, _ in r11)
        deg_mix = "; ".join([f"{deg} — {float(p):.2f}% ({int(c)}/{total})" for deg, c, p in r11])
        rows_out.append((q_text, f"Answer: {deg_mix}"))
    else:
        rows_out.append((q_text, "Answer: No degree information recorded for "
                                 "that term."))


def q12(cur, label, rows_out, tbl, term_cond):
    """Query 12th result analysis"""
    # Q12
    q_text = (f"For the five universities with the most entries in {label}, "
              f"what is the average GPA at each?")
    stmt_q12 = sql.SQL("""
                WITH b AS (
                    SELECT
                        COALESCE(
                          NULLIF(TRIM(llm_generated_university), ''),
                          NULLIF(split_part(program, ' - ', 1), '')
                        ) AS uni,
                        gpa
                    FROM {tbl}
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
            """).format(tbl=tbl, term_cond=sql.SQL(term_cond))
    cur.execute(stmt_q12)
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


def main() -> None:
    """
    Main invocation entry point.
    :return: prints result of all the analysis
    """
    for q, a in get_rows():
        print(f"- {q}\n  {a}")


if __name__ == "__main__":
    main()
