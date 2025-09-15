"""Fast JSON loader with assignment-aligned key mapping and robust type handling.

- Batches inserts (default 2000) in a single transaction for speed on ~30k rows.
- Converts numeric fields to float; if conversion fails, stores NULL and records an issue.
- Parses multiple date formats; unparsable dates become NULL and are recorded.
- Records a concise audit report at module_3/artifacts/load_report.json:
  - total_records, inserted_rows, skipped_without_id
  - issue_counts per field (e.g., degree_non_numeric, date_parse_fail)
  - sample p_id lists (first few) for each issue
"""

import json
import re
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

from src.dal.pool import get_conn

INSERT_SQL = """
INSERT INTO applicants (
    p_id, program, comments, date_added, url, status, term, us_or_international,
    gpa, gre, gre_v, gre_aw, degree, llm_generated_program, llm_generated_university
)
VALUES (
    %(p_id)s, %(program)s, %(comments)s, %(date_added)s, %(url)s, %(status)s, %(term)s, %(us_or_international)s,
    %(gpa)s, %(gre)s, %(gre_v)s, %(gre_aw)s, %(degree)s, %(llm_generated_program)s, %(llm_generated_university)s
)
ON CONFLICT (p_id) DO NOTHING;
"""

_ID_FROM_URL = re.compile(r"/result/(\d+)")
_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%B %d, %Y")

def _to_float(x: Any) -> Optional[float]:
    """Convert numeric-like values to float or None."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None

def _parse_date(s: Optional[str]) -> Optional[dt.date]:
    """Parse common date formats; return None on failure."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in _DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def _derive_p_id(entry_url: Optional[str], fallback: Optional[Any]) -> Optional[int]:
    """Prefer extracting numeric id from entry_url; else try an integer fallback."""
    if entry_url:
        m = _ID_FROM_URL.search(entry_url)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
    if fallback is not None:
        try:
            return int(fallback)
        except (TypeError, ValueError):
            return None
    return None

def _map_record(rec: Dict[str, Any], issues: Dict[str, Any], sample_limit: int = 20) -> Optional[Dict[str, Any]]:
    """Map one JSON object to the DB row dict; return None if no stable p_id."""
    # normalize keys (spaces, hyphens, slashes -> underscores; lowercase)
    norm: Dict[str, Any] = {}
    for k, v in rec.items():
        nk = k.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        norm[nk] = v

    entry_url = norm.get("entry_url") or norm.get("url") or ""
    p_id = _derive_p_id(entry_url, norm.get("p_id"))
    if p_id is None:
        issues["missing_p_id"] += 1
        if len(issues["_samples"]["missing_p_id"]) < sample_limit:
            issues["_samples"]["missing_p_id"].append(entry_url or str(norm.get("p_id")))
        return None

    # program text: "University - Program" if both exist
    program_raw = (norm.get("program") or "").strip()
    university_raw = (norm.get("university") or "").strip()
    program = f"{university_raw} - {program_raw}".strip(" -") if university_raw else program_raw

    # date parsing (first non-empty among acceptance/rejection/date_added)
    date_added_raws = [norm.get("acceptance_date"), norm.get("rejection_date"), norm.get("date_added")]
    date_added = None
    for d in date_added_raws:
        date_added = _parse_date(d)
        if date_added:
            break
    if date_added is None and any(d for d in date_added_raws):
        issues["date_parse_fail"] += 1
        if len(issues["_samples"]["date_parse_fail"]) < sample_limit:
            issues["_samples"]["date_parse_fail"].append(p_id)

    # numeric fields
    def _num(field_key_in_norm: str, issue_key: str) -> Optional[float]:
        val = norm.get(field_key_in_norm)
        out = _to_float(val)
        if out is None and (val not in (None, "")):
            issues[issue_key] += 1
            if len(issues["_samples"][issue_key]) < sample_limit:
                issues["_samples"][issue_key].append(p_id)
        return out

    row = {
        "p_id": p_id,
        "program": program,
        "comments": norm.get("comments") or "",
        "date_added": date_added,
        "url": entry_url,
        "status": norm.get("status") or "",
        "term": norm.get("start_term") or norm.get("term") or "",
        "us_or_international": norm.get("us_international") or norm.get("us_or_international") or "",
        "gpa": _num("gpa", "gpa_non_numeric"),
        "gre": _num("gre", "gre_non_numeric"),       # Quant
        "gre_v": _num("gre_v", "gre_v_non_numeric"), # Verbal
        "gre_aw": _num("gre_aw", "gre_aw_non_numeric"),  # AW
        "degree": _num("degree", "degree_non_numeric"),  # per assignment: REAL
        "llm_generated_program": norm.get("llm_generated_program") or "",
        "llm_generated_university": norm.get("llm_generated_university") or "",
    }
    return row

def _init_issue_tracker() -> Dict[str, Any]:
    """Initialize counters and sample holders for load issues."""
    counters = {
        "missing_p_id": 0,
        "date_parse_fail": 0,
        "gpa_non_numeric": 0,
        "gre_non_numeric": 0,
        "gre_v_non_numeric": 0,
        "gre_aw_non_numeric": 0,
        "degree_non_numeric": 0,
        "_samples": {
            "missing_p_id": [],
            "date_parse_fail": [],
            "gpa_non_numeric": [],
            "gre_non_numeric": [],
            "gre_v_non_numeric": [],
            "gre_aw_non_numeric": [],
            "degree_non_numeric": [],
        },
    }
    return counters

def _write_report(total: int, inserted: int, skipped: int, issues: Dict[str, Any], batch: int) -> Path:
    """Write a concise JSON report and return its path."""
    base = Path(__file__).resolve().parents[2]  # .../module_3
    outdir = base / "artifacts"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / "load_report.json"
    report = {
        "summary": {
            "total_records": total,
            "inserted_rows": inserted,
            "skipped_without_id": skipped,
            "batch_size": batch,
        },
        "issue_counts": {k: v for k, v in issues.items() if k != "_samples"},
        "issue_samples": issues["_samples"],  # small p_id lists only
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return path

def load_json(path: str, batch: int = 2000) -> Tuple[int, int, int, Dict[str, int], Path]:
    """Load a JSON array from `path` and insert rows in batches.

    Returns:
        total_records, inserted_rows, skipped_without_id, issue_counts, report_path
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Expected a JSON array of applicant records")

    issues = _init_issue_tracker()
    mapped: List[Dict[str, Any]] = []
    skipped = 0
    for r in data:
        row = _map_record(r or {}, issues)
        if row is None:
            skipped += 1
            continue
        mapped.append(row)

    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(mapped), batch):
                chunk = mapped[i : i + batch]
                cur.executemany(INSERT_SQL, chunk)
                if cur.rowcount and cur.rowcount > 0:
                    inserted += cur.rowcount
        conn.commit()

    report_path = _write_report(len(data), inserted, skipped, issues, batch)
    issue_counts = {k: v for k, v in issues.items() if k != "_samples"}
    return len(data), inserted, skipped, issue_counts, report_path

def first_ids(n: int = 3) -> List[int]:
    """Return the first n p_id values for a tiny preview."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT p_id FROM applicants ORDER BY p_id ASC LIMIT %s;", (n,))
            return [r[0] for r in cur.fetchall()]
