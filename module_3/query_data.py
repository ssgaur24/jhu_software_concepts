"""Run Module-3 SQL queries against public.applicants.

Answers assignment Q1–Q8 and two custom questions with tiny console output.
- Outputs are printed and also saved to module_3/artifacts/queries_output.txt
- Uses the connection pool from src.dal.pool
"""

from __future__ import annotations

# stdlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# local
from src.dal.pool import get_conn, close_pool


# -- helpers ---------------------------------------------------------------

def _fetch_val(sql: str, params: Optional[Iterable[Any]] = None) -> Any:
    """Execute a scalar SQL and return the first column of the first row."""
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
    """Write lines to artifacts/queries_output.txt and return the path."""
    base = Path(__file__).resolve().parent
    outdir = base / "artifacts"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "queries_output.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# -- Q1–Q8 (from assignment) ----------------------------------------------
# Q1. How many entries applied for Fall 2025?  (term filter)
# (Assignment list of questions: see PDF.)  # Q refs: Q1–Q8
def q1_count_fall_2025() -> int:
    # count rows where term indicates Fall 2025
    sql = """
        SELECT COUNT(*) FROM public.applicants
        WHERE term ILIKE 'fall%%2025%%'
           OR term ILIKE 'fall 2025'
           OR term ILIKE '2025 fall'
    """
    return int(_fetch_val(sql) or 0)


# Q2. Percentage of entries that are International (not American or Other) — two decimals
def q2_pct_international() -> float:
    # compute share where us_or_international indicates international
    sql = """
        SELECT CASE WHEN COUNT(*) = 0 THEN 0
            ELSE 100.0 * SUM(CASE WHEN LOWER(us_or_international) = 'international' THEN 1 ELSE 0 END)::float
                 / COUNT(*)
        END
        FROM public.applicants
    """
    return float(_fetch_val(sql) or 0.0)


# Q3. Average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics
def q3_avgs() -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    sql = """
        SELECT AVG(gpa), AVG(gre), AVG(gre_v), AVG(gre_aw)
        FROM public.applicants
        WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL
    """
    row = _fetch_all(sql)[0]
    return tuple(row)  # may include None if no data


# Q4. Average GPA of American students in Fall 2025
def q4_avg_gpa_american_fall2025() -> Optional[float]:
    sql = """
        SELECT AVG(gpa)
        FROM public.applicants
        WHERE gpa IS NOT NULL
          AND LOWER(us_or_international) = 'american'
          AND (term ILIKE 'fall%%2025%%' OR term ILIKE 'fall 2025' OR term ILIKE '2025 fall')
    """
    return _fetch_val(sql)


# Q5. Percent of entries for Fall 2025 that are Acceptances (two decimals)
def q5_pct_accept_fall2025() -> float:
    sql = """
        SELECT CASE WHEN COUNT(*) = 0 THEN 0
            ELSE 100.0 * SUM(CASE WHEN LOWER(status) LIKE 'accepted%%' THEN 1 ELSE 0 END)::float
                 / COUNT(*)
        END
        FROM public.applicants
        WHERE term ILIKE 'fall%%2025%%'
           OR term ILIKE 'fall 2025'
           OR term ILIKE '2025 fall'
    """
    return float(_fetch_val(sql) or 0.0)


# Q6. Average GPA of applicants who applied for Fall 2025 who are Acceptances
def q6_avg_gpa_accept_fall2025() -> Optional[float]:
    sql = """
        SELECT AVG(gpa)
        FROM public.applicants
        WHERE gpa IS NOT NULL
          AND LOWER(status) LIKE 'accepted%%'
          AND (term ILIKE 'fall%%2025%%' OR term ILIKE 'fall 2025' OR term ILIKE '2025 fall')
    """
    return _fetch_val(sql)


# Q7. How many entries are from applicants who applied to JHU for a masters degree in CS?
#    Note: DB 'degree' is REAL per assignment; source often contains text (e.g., "PhD") -> NULL in DB.
#    Heuristic: treat "masters" if degree between 0.5 and 1.5 OR program mentions MS/Master.
def q7_count_jhu_masters_cs() -> int:
    sql = """
        SELECT COUNT(*)
        FROM public.applicants
        WHERE
          (
            program ILIKE '%%johns hopkins%%'
            OR program ILIKE '%%hopkins%%'
            OR program ILIKE '%%jhu%%'
          )
          AND (
            program ILIKE '%%computer science%%'
            OR program ILIKE '%%cs%%'
          )
          AND (
            (degree IS NOT NULL AND degree BETWEEN 0.5 AND 1.5)
            OR program ILIKE '%%ms%%'
            OR program ILIKE '%%master%%'
          )
    """
    return int(_fetch_val(sql) or 0)


# Q8. How many entries from 2025 are **acceptances** for Georgetown **PhD** in CS?
#     Interpret "from 2025" as entries with date_added in year 2025.
#     Heuristic for PhD: degree >= 1.5 OR program mentions PhD/Doctor.
def q8_count_2025_georgetown_phd_cs_accept() -> int:
    sql = """
        SELECT COUNT(*)
        FROM public.applicants
        WHERE
          date_part('year', date_added) = 2025
          AND LOWER(status) LIKE 'accepted%%'
          AND (
            program ILIKE '%%georgetown%%'
          )
          AND (
            program ILIKE '%%computer science%%' OR program ILIKE '%%cs%%'
          )
          AND (
            (degree IS NOT NULL AND degree >= 1.5)
            OR program ILIKE '%%phd%%'
            OR program ILIKE '%%doctor%%'
          )
    """
    return int(_fetch_val(sql) or 0)


# -- Two additional custom questions --------------------------------------

# Q9 (custom). Top 5 universities by number of ACCEPTANCES in 2025 (by date_added year)
# Extract "university" as the string before ' - ' in program when present.
def q9_top5_accept_unis_2025() -> List[Tuple[str, int]]:
    sql = """
        SELECT
          CASE WHEN position(' - ' in program) > 0
               THEN split_part(program, ' - ', 1)
               ELSE program
          END AS university,
          COUNT(*) AS accept_count
        FROM public.applicants
        WHERE date_part('year', date_added) = 2025
          AND LOWER(status) LIKE 'accepted%%'
        GROUP BY university
        ORDER BY accept_count DESC, university ASC
        LIMIT 5
    """
    return _fetch_all(sql)


# Q10 (custom). Average GRE Quant for Accepted vs Rejected in 2025 (quick comparison)
def q10_avg_gre_by_status_2025() -> List[Tuple[str, Optional[float]]]:
    sql = """
        SELECT
          CASE
            WHEN LOWER(status) LIKE 'accepted%%' THEN 'Accepted'
            WHEN LOWER(status) LIKE 'rejected%%' THEN 'Rejected'
            ELSE 'Other'
          END AS status_group,
          AVG(gre) AS avg_gre
        FROM public.applicants
        WHERE date_part('year', date_added) = 2025
          AND gre IS NOT NULL
        GROUP BY status_group
        ORDER BY status_group
    """
    return _fetch_all(sql)


# -- main runner -----------------------------------------------------------

def run_all() -> List[str]:
    """Execute all questions and return formatted output lines."""
    lines: List[str] = []
    # Q1–Q8 (assignment)   # refs: assignment Q list
    lines.append(f"Q1  Fall 2025 entries: {q1_count_fall_2025()}")
    lines.append(f"Q2  % International: {q2_pct_international():.2f}%")
    gpa, gre, gre_v, gre_aw = q3_avgs()
    lines.append(
        "Q3  Averages (GPA, GRE, GRE_V, GRE_AW): "
        f"{'%.2f' % gpa if gpa is not None else 'NA'}, "
        f"{'%.2f' % gre if gre is not None else 'NA'}, "
        f"{'%.2f' % gre_v if gre_v is not None else 'NA'}, "
        f"{'%.2f' % gre_aw if gre_aw is not None else 'NA'}"
    )
    lines.append(f"Q4  Avg GPA (American, Fall 2025): {q4_avg_gpa_american_fall2025() if q4_avg_gpa_american_fall2025() is not None else 'NA'}")
    lines.append(f"Q5  % Acceptances (Fall 2025): {q5_pct_accept_fall2025():.2f}%")
    lines.append(f"Q6  Avg GPA (Accepted, Fall 2025): {q6_avg_gpa_accept_fall2025() if q6_avg_gpa_accept_fall2025() is not None else 'NA'}")
    lines.append(f"Q7  JHU Masters CS entries: {q7_count_jhu_masters_cs()}")
    lines.append(f"Q8  2025 Accepted Georgetown PhD CS entries: {q8_count_2025_georgetown_phd_cs_accept()}")

    # custom
    top5 = q9_top5_accept_unis_2025()
    lines.append("Q9  Top 5 Accepting Universities in 2025 (university, count): " +
                 (", ".join([f"{u}={c}" for (u, c) in top5]) if top5 else "NA"))

    gre_by_status = q10_avg_gre_by_status_2025()
    lines.append("Q10 Avg GRE by Status (2025): " +
                 (", ".join([f"{s}={'%.2f' % v if v is not None else 'NA'}" for (s, v) in gre_by_status]) if gre_by_status else "NA"))

    return lines


if __name__ == "__main__":
    try:
        out_lines = run_all()                 # run queries
        for ln in out_lines:                  # tiny console output
            print(ln)
        out_path = _write_lines(out_lines)    # write artifacts/queries_output.txt
        print(f"saved={out_path}")
    finally:
        close_pool()                          # cleanly stop pool threads
