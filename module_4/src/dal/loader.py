# coverage: ignore file
"""Fast JSON loader for Module 3 with assignment-aligned mapping.

- Skips records without a stable id (p_id parsed from entry_url '/result/<id>' or 'p_id').
- Batches inserts in a single transaction (default batch=2000).
- Numeric coercion: gpa/gre/gre_v/gre_aw -> float if possible; else NULL.
- Date parsing: multiple formats; unparsable -> NULL.
- Degree is stored as TEXT (per instructor correction).
- Accepts LLM fields with underscore OR hyphen keys:
    llm_generated_program  | llm-generated-program
    llm_generated_university| llm-generated-university
- Writes a concise audit report to module_3/artifacts/load_report.json.
"""

from __future__ import annotations

import json
import re
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.dal.pool import get_conn  # pooled access


INSERT_SQL = """
INSERT INTO public.applicants (
  p_id, program, comments, date_added, url, status, term, us_or_international,
  gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university
) VALUES (
  %(p_id)s, %(program)s, %(comments)s, %(date_added)s, %(url)s, %(status)s, %(term)s, %(us_or_international)s,
  %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s, %(degree)s, %(llm_generated_program)s, %(llm_generated_university)s
) ON CONFLICT (p_id) DO NOTHING;
"""

_ID_FROM_URL = re.compile(r"/result/(\d+)")
_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%B %d, %Y")


def _to_float(x: Any) -> Optional[float]:
    """Convert numeric-like values to float or None (student helper)."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_date(x: Any) -> Optional[dt.date]:
    """Parse multiple date formats into date or None."""
    if not x:
        return None
    s = str(x).strip()
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # also support abbreviated months like "Mar 01, 2025"
    try:
        return dt.datetime.strptime(s, "%b %d, %Y").date()
    except Exception:
        return None


def _stable_id(rec: Dict[str, Any]) -> Optional[int]:
    """Extract stable numeric id from entry_url or p_id field."""
    if "p_id" in rec and isinstance(rec["p_id"], (int, float)) and int(rec["p_id"]) > 0:
        return int(rec["p_id"])
    url = rec.get("entry_url") or rec.get("url") or ""
    m = _ID_FROM_URL.search(str(url))
    return int(m.group(1)) if m else None


def _compose_program(university: str, program: str) -> str:
    """Compose 'University - Program' when both exist to aid grouping (student helper)."""
    u = (university or "").strip()
    p = (program or "").strip()
    if u and p:
        return f"{u} - {p}"
    return p or u


def _map_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw JSON row into DB insert dict."""
    p_id = _stable_id(rec)
    if p_id is None:
        raise ValueError("missing_stable_id")

    university = rec.get("university", "")
    program_only = rec.get("program", "")

    # Support multiple key spellings
    llm_prog = (
        rec.get("llm_generated_program")
        or rec.get("llm-generated-program")
        or rec.get("standardized_program")
    )
    llm_uni = (
        rec.get("llm_generated_university")
        or rec.get("llm-generated-university")
        or rec.get("standardized_university")
    )

    degree_val = rec.get("degree")
    if degree_val in (None, ""):
        degree_val = rec.get("Degree")  # tolerate title-cased key from some scrapers
        if degree_val == "":
            degree_val = None

    mapped = {
        "p_id": p_id,
        "program": _compose_program(str(university), str(program_only)),
        "comments": (rec.get("comments") or "").strip() or None,
        "date_added": _to_date(rec.get("date_added") or rec.get("acceptance_date") or rec.get("rejection_date")),
        "url": rec.get("entry_url") or rec.get("url") or None,
        "status": rec.get("status") or None,
        "term": rec.get("start_term") or rec.get("term") or None,
        "us_or_international": rec.get("US/International") or rec.get("us_or_international") or None,
        "gpa": _to_float(rec.get("GPA") or rec.get("gpa")),
        "gre": _to_float(rec.get("GRE") or rec.get("gre")),
        "gre_v": _to_float(rec.get("GRE V") or rec.get("gre_v")),
        "gre_aw": _to_float(rec.get("GRE AW") or rec.get("gre_aw")),
        "degree": (str(degree_val).strip() if degree_val not in (None, "") else None),  # TEXT
        "llm_generated_program": (str(llm_prog).strip() if llm_prog else None),
        "llm_generated_university": (str(llm_uni).strip() if llm_uni else None),
    }
    return mapped


def _chunks(lst: List[Dict[str, Any]], n: int):
    """Yield size-n chunks (student helper)."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def first_ids(path: str, k: int = 3) -> List[int]:
    """Return first k stable ids present in the JSON array (student helper)."""
    arr = json.loads(Path(path).read_text(encoding="utf-8"))
    ids: List[int] = []
    for rec in arr:
        try:
            sid = _stable_id(rec)
            if sid is not None:
                ids.append(sid)
                if len(ids) >= k:
                    break
        except Exception:
            continue
    return ids


def load_json(path: str, batch: int = 2000):
    """Load a JSON array file into public.applicants, return summary tuple.

    Returns: (total_records, inserted_rows, skipped_without_id, issue_counts, report_path)
    """
    p = Path(path)
    arr = json.loads(p.read_text(encoding="utf-8"))
    total = len(arr)

    to_insert: List[Dict[str, Any]] = []
    skipped = 0
    issue_counts: Dict[str, int] = {
        "missing_p_id": 0,
        "date_parse_fail": 0,
        "gpa_non_numeric": 0,
        "gre_non_numeric": 0,
        "gre_v_non_numeric": 0,
        "gre_aw_non_numeric": 0,
    }

    # map & validate
    for rec in arr:
        try:
            mapped = _map_record(rec)
            # audit for date parse fail
            if (rec.get("date_added") or rec.get("acceptance_date") or rec.get("rejection_date")) and mapped["date_added"] is None:
                issue_counts["date_parse_fail"] += 1
            # audit numeric parsing
            if rec.get("GPA") not in (None, "") and mapped["gpa"] is None:
                issue_counts["gpa_non_numeric"] += 1
            if rec.get("GRE") not in (None, "") and mapped["gre"] is None:
                issue_counts["gre_non_numeric"] += 1
            if rec.get("GRE V") not in (None, "") and mapped["gre_v"] is None:
                issue_counts["gre_v_non_numeric"] += 1
            if rec.get("GRE AW") not in (None, "") and mapped["gre_aw"] is None:
                issue_counts["gre_aw_non_numeric"] += 1

            to_insert.append(mapped)
        except ValueError:
            issue_counts["missing_p_id"] += 1
            skipped += 1

    # batch insert
    inserted_total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for chunk in _chunks(to_insert, batch):
                cur.executemany(INSERT_SQL, chunk)
                inserted_total += cur.rowcount if cur.rowcount is not None else 0
        conn.commit()

    # write concise report
    art_dir = Path(__file__).resolve().parent.parent / "artifacts"
    art_dir.mkdir(parents=True, exist_ok=True)
    report_path = art_dir / "load_report.json"
    report = {
        "total_records": total,
        "inserted": inserted_total,
        "skipped_without_id": skipped,
        "issue_counts": issue_counts,
        "sample_ids": first_ids(path, k=3),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    return total, inserted_total, skipped, issue_counts, report_path
