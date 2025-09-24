"""Run Module-3 SQL queries against public.applicants.

Answers assignment Q1–Q8 and two custom questions.
Implements instructor notes:
- Use llm_generated_university and llm_generated_program for university/program logic.
- Apply reasonability filters: GPA <= 5, GRE <= 400.

Tiny outputs + artifacts text file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import datetime as dt

from src.dal.pool import get_conn, close_pool


# -- helpers ---------------------------------------------------------------

def _fetch_val(sql: str, params: Optional[Iterable[Any]] = None) -> Optional[Any]:
    """Execute a query and return the first column of the first row (or None)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return row[0] if row else None


def _fetch_all(sql: str, params: Optional[Iterable[Any]] = None) -> List[Tuple]:
    """Execute a query and return all rows."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def _write_lines(lines: List[str]) -> Path:
    """Write lines to artifacts/queries_output.txt and return the path (student helper)."""
    base = Path(__file__).resolve().parent
    outdir = base / "artifacts"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "queries_output.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# -- Q1–Q8 (assignment) ----------------------------------------------------

def q1_count_fall_2025() -> int:
    """Count rows with term indicating Fall 2025."""
    sql = """
        SELECT COUNT(*) FROM public.applicants
        WHERE term ILIKE 'fall%%2025%%'
           OR term ILIKE 'fall 2025'
           OR term ILIKE '2025 fall'
           OR term ILIKE '%%2025%%fall%%'
    """
    return int(_fetch_val(sql) or 0)


def q2_pct_international() -> float:
    """Percent of rows with us_or_international = 'international' (case-insensitive)."""
    sql = """
        SELECT CASE WHEN COUNT(*) = 0 THEN 0
            ELSE 100.0 * SUM(CASE WHEN LOWER(us_or_international) = 'international' THEN 1 ELSE 0 END)::float
                 / COUNT(*)
        END
        FROM public.applicants
    """
    return float(_fetch_val(sql) or 0.0)


def q3_avgs() -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Averages of GPA, GRE, GRE_V, GRE_AW with reasonability filters (may return NULLs)."""
    sql = """
        SELECT AVG(CASE WHEN gpa <= 5 THEN gpa END),
               AVG(CASE WHEN gre <= 400 THEN gre END),
               AVG(gre_v),        -- instructor only specified caps for GPA/GRE total
               AVG(gre_aw)
        FROM public.applicants
        WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL
    """
    row = _fetch_all(sql)[0]
    return tuple(row)  # type: ignore[return-value]


def q4_avg_gpa_american_fall2025() -> Optional[float]:
    """Average GPA of American students for Fall 2025 (GPA <= 5 only)."""
    sql = """
        SELECT AVG(CASE WHEN gpa <= 5 THEN gpa END)
        FROM public.applicants
        WHERE gpa IS NOT NULL
          AND LOWER(us_or_international) = 'american'
          AND (term ILIKE 'fall%%2025%%' OR term ILIKE 'fall 2025' OR term ILIKE '2025 fall')
    """
    return _fetch_val(sql)


def q5_pct_accept_fall2025() -> float:
    """Percent accepted among Fall 2025 entries (case-insensitive status prefix 'accept')."""
    sql = """
        SELECT CASE WHEN COUNT(*) = 0 THEN 0
            ELSE 100.0 * SUM(CASE WHEN LOWER(status) LIKE 'accept%%' THEN 1 ELSE 0 END)::float
                 / COUNT(*)
        END
        FROM public.applicants
        WHERE (term ILIKE 'fall%%2025%%' OR term ILIKE 'fall 2025' OR term ILIKE '2025 fall')
    """
    return float(_fetch_val(sql) or 0.0)


def q6_avg_gpa_accept_fall2025() -> Optional[float]:
    """Average GPA (<=5) of accepted entries for Fall 2025."""
    sql = """
        SELECT AVG(CASE WHEN gpa <= 5 THEN gpa END)
        FROM public.applicants
        WHERE gpa IS NOT NULL
          AND LOWER(status) LIKE 'accept%%'
          AND (term ILIKE 'fall%%2025%%' OR term ILIKE 'fall 2025' OR term ILIKE '2025 fall')
    """
    return _fetch_val(sql)


def q7_count_jhu_masters_cs() -> int:
    """Count entries applying to JHU for a master's in Computer Science using LLM fields."""
    sql = """
        SELECT COUNT(*)
        FROM public.applicants
        WHERE
          (llm_generated_university ILIKE '%%johns hopkins%%' OR llm_generated_university ILIKE '%%jhu%%')
          AND (llm_generated_program ILIKE '%%computer%%science%%' OR llm_generated_program ILIKE '%%cs%%')
          AND (llm_generated_program ILIKE '%%master%%' OR llm_generated_program ILIKE '%%ms%%')
    """
    return int(_fetch_val(sql) or 0)


def q8_count_2025_georgetown_phd_cs_accept() -> int:
    """Count 2025 acceptances for Georgetown PhD in CS using LLM fields (date_added year or term contains 2025)."""
    sql = """
        SELECT COUNT(*)
        FROM public.applicants
        WHERE
          ( (date_added IS NOT NULL AND date_part('year', date_added) = 2025)
            OR (term ILIKE '%%2025%%') )
          AND LOWER(status) LIKE 'accept%%'
          AND (llm_generated_university ILIKE '%%georgetown%%')
          AND (llm_generated_program ILIKE '%%computer%%science%%' OR llm_generated_program ILIKE '%%cs%%')
          AND (llm_generated_program ILIKE '%%phd%%' OR llm_generated_program ILIKE '%%doctor%%')
    """
    return int(_fetch_val(sql) or 0)


# -- Q9/Q10 (custom) -------------------------------------------------------

def q9_top5_accept_unis_2025() -> List[Tuple[str, int]]:
    """Top 5 universities by acceptances in 2025 using LLM university (fallback to parsed program)."""
    sql = """
        SELECT
          COALESCE(NULLIF(TRIM(llm_generated_university), ''),
                   CASE WHEN position(' - ' in program) > 0
                        THEN split_part(program, ' - ', 1)
                        ELSE program END) AS university,
          COUNT(*) AS accept_count
        FROM public.applicants
        WHERE ((date_added IS NOT NULL AND date_part('year', date_added) = 2025)
               OR (term ILIKE '%%2025%%'))
          AND LOWER(status) LIKE 'accept%%'
        GROUP BY university
        ORDER BY accept_count DESC, university ASC
        LIMIT 5
    """
    return _fetch_all(sql)


def q10_avg_gre_by_status_year(year: int) -> List[Tuple[str, Optional[float]]]:
    """Average GRE by status for a given calendar year with GRE <= 400 filter."""
    sql = """
        SELECT status,
               AVG(CASE WHEN gre <= 400 THEN gre END) AS avg_gre
        FROM public.applicants
        WHERE date_added IS NOT NULL
          AND date_part('year', date_added) = %s
          AND gre IS NOT NULL
        GROUP BY status
        ORDER BY status
    """
    return _fetch_all(sql, (year,))


def q10_avg_gre_by_status_last_n_years(n_years: int) -> List[Tuple[str, Optional[float]]]:
    """Average GRE by status for the last n calendar years from today (GRE <= 400)."""
    today = dt.date.today()
    start_year = today.year - (n_years - 1)
    sql = """
        SELECT status,
               AVG(CASE WHEN gre <= 400 THEN gre END) AS avg_gre
        FROM public.applicants
        WHERE date_added IS NOT NULL
          AND date_part('year', date_added) BETWEEN %s AND %s
          AND gre IS NOT NULL
        GROUP BY status
        ORDER BY status
    """
    return _fetch_all(sql, (start_year, today.year))


# -- driver ---------------------------------------------------------------

def run_all() -> List[str]:
    """Compute all lines for artifacts output (student tiny run)."""
    lines: List[str] = []

    # Q1–Q8
    lines.append(f"Q1  Fall 2025 entries: {q1_count_fall_2025()}")
    lines.append(f"Q2  % International: {q2_pct_international():.2f}%")

    gpa, gre, gre_v, gre_aw = q3_avgs()

    if all(v is None for v in (gpa, gre, gre_v, gre_aw)):
        lines.append("Q3  Averages (GPA, GRE, GRE_V, GRE_AW): NA")
    else:
        present = []
        if gpa is not None: present.append(f"Average GPA: {gpa:.2f}")
        if gre is not None: present.append(f"Average GRE: {gre:.2f}")
        if gre_v is not None: present.append(f"Average GRE V: {gre_v:.2f}")
        if gre_aw is not None: present.append(f"Average GRE AW: {gre_aw:.2f}")
        lines.append("Q3  " + ", ".join(present))

    q4 = q4_avg_gpa_american_fall2025()
    lines.append(f"Q4  Avg GPA (American, Fall 2025): {q4:.2f}" if q4 is not None else "Q4  Avg GPA (American, Fall 2025): NA")
    lines.append(f"Q5  % Acceptances (Fall 2025): {q5_pct_accept_fall2025():.2f}%")
    q6 = q6_avg_gpa_accept_fall2025()
    lines.append(f"Q6  Avg GPA (Accepted, Fall 2025): {q6:.2f}" if q6 is not None else "Q6  Avg GPA (Accepted, Fall 2025): NA")
    lines.append(f"Q7  JHU Masters CS entries: {q7_count_jhu_masters_cs()}")
    lines.append(f"Q8  2025 Accepted Georgetown PhD CS entries: {q8_count_2025_georgetown_phd_cs_accept()}")

    # Q9/Q10
    top5 = q9_top5_accept_unis_2025()
    lines.append("Q9  Top 5 Accepting Universities in 2025: " +
                 (", ".join([f"{u}={c}" for (u, c) in top5]) if top5 else "NA"))

    def _fmt_status(rows: List[Tuple[str, Optional[float]]]) -> str:
        non_null = [(s, v) for (s, v) in rows if v is not None]
        return ", ".join([f"{s}={v:.2f}" for (s, v) in non_null]) if non_null else "NA"

    q10_2024 = q10_avg_gre_by_status_year(2024)
    lines.append("Q10a Avg GRE by Status (2024): " + _fmt_status(q10_2024))

    q10_last3 = q10_avg_gre_by_status_last_n_years(3)
    lines.append("Q10b Avg GRE by Status (last 3 yrs): " + _fmt_status(q10_last3))

    return lines

# --- New Q11: Top 10 universities by number of entries for Fall 2025 ---

def q11_top_unis_fall_2025(limit: int = 10):
    """Return [(university, count), ...] for Fall 2025 using LLM fields first."""
    sql = """
        SELECT COALESCE(llm_generated_university, NULLIF(split_part(program, ' - ', 1), '')) AS uni,
               COUNT(*) AS c
          FROM public.applicants
         WHERE term ILIKE 'Fall 2025'
         GROUP BY 1
         HAVING COALESCE(llm_generated_university, NULLIF(split_part(program, ' - ', 1), '')) IS NOT NULL
         ORDER BY c DESC, uni ASC
         LIMIT %s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return [(r[0], int(r[1])) for r in cur.fetchall()]

# --- New Q12: Status breakdown (percentages) for Fall 2025  ---

def q12_status_breakdown_fall_2025():
    """Return [(status, pct_float), ...] for Fall 2025 using NUMERIC math (ROUND(...,2) requires NUMERIC in Postgres)."""
    sql = """
        WITH base AS (
            SELECT status
              FROM public.applicants
             WHERE term ILIKE 'Fall 2025' AND status IS NOT NULL
        ),
        totals AS (
            SELECT COUNT(*)::numeric AS n FROM base
        )
        SELECT b.status,
               ROUND((COUNT(*)::numeric * 100) / NULLIF((SELECT n FROM totals), 0), 2) AS pct
          FROM base b
         GROUP BY b.status
         ORDER BY pct DESC, b.status ASC;
    """
    from src.dal.pool import get_conn  # local import to match rest of file style
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [(r[0], float(r[1])) for r in cur.fetchall()]

