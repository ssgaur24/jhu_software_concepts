# clean.py
# Minimal cleaner for Grad Café scraped data.
# - Pulls accepted/rejected dates from status text when missing.
# - Formats dates to YYYY-MM-DD.
# - Normalizes term codes (e.g., f20 -> Fall 2020).
# - Blanks implausible GRE/GPA values (e.g., GRE > 800, GPA > 10).
# - Keeps unavailable data as "" and does not introduce new keys.

import json
import os
import re
from html import unescape

# ------------------------- tiny utilities ------------------------- #

_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "SEPT": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

def _s(x) -> str:
    """Coerce to trimmed, unescaped string; None -> ""."""
    return unescape((x or "") if isinstance(x, str) else str(x or "")).strip()

def _year(text: str) -> str:
    """Return first 4-digit year or ""."""
    m = re.search(r"(19|20)\d{2}", _s(text))
    return m.group(0) if m else ""

# 3-letter month map keeps it simple
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

def _parse_date_iso(text: str, fallback_year: str = "") -> str:
    """Parse common user date shapes into YYYY-MM-DD; else ''."""
    s = _s(text)
    if not s:
        return ""

    # remove ordinal suffixes like '14th' -> '14' (cheap, optional)
    s = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", s)

    # yyyy-mm-dd
    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # m/d/yy[yy]
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})\s*$", s)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), m.group(3)
        yyyy = int(yy) + 2000 if len(yy) == 2 else int(yy)
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}" if 1 <= mm <= 12 and 1 <= dd <= 31 else ""

    # Mon dd[, yyyy]  e.g., "July 14, 2025" or "Sep 7"
    m = re.match(r"^\s*([A-Za-z]{3,12})\s+(\d{1,2})(?:,\s*(\d{4}))?\s*$", s)
    if m:
        mon_key = (m.group(1)[:3]).upper()
        mon = _MONTHS.get(mon_key, 0)
        dd = int(m.group(2))
        yyyy = m.group(3) or fallback_year
        return f"{int(yyyy):04d}-{mon:02d}-{dd:02d}" if mon and yyyy and 1 <= dd <= 31 else ""

    # dd Mon[ yyyy]  e.g., "14 July 2025" or "7 Sep"
    m = re.match(r"^\s*(\d{1,2})\s+([A-Za-z]{3,12})(?:\s*(\d{4}))?\s*$", s)
    if m:
        dd = int(m.group(1))
        mon_key = (m.group(2)[:3]).upper()
        mon = _MONTHS.get(mon_key, 0)
        yyyy = m.group(3) or fallback_year
        return f"{int(yyyy):04d}-{mon:02d}-{dd:02d}" if mon and yyyy and 1 <= dd <= 31 else ""

    return ""


def _term_norm(term_text: str) -> str:
    """Normalize term like f20/S20/Fall 2020 -> 'Fall 2020'; else original."""
    raw = _s(term_text)
    if not raw:
        return ""
    u = raw.upper().replace(".", "").replace("-", "").replace("_", "").replace(" ", "")

    m = re.match(r"^(F|FA|FALL|S|SP|SPR|SPRING|SU|SUM|SUMMER|W|WIN|WINTER)(\d{2,4})$", u)
    if m:
        code, yy = m.group(1), m.group(2)
    else:
        m = re.match(r"^\s*([A-Za-z]+)\s+(\d{2,4})\s*$", raw, re.I)
        if not m:
            return raw
        code, yy = m.group(1).upper(), m.group(2)

    season = ("Fall" if code.startswith("F")
              else "Spring" if code.startswith("S") and not code.startswith("SU")
              else "Summer" if code.startswith("SU")
              else "Winter" if code.startswith("W")
              else "")
    return f"{season} {int(yy)+2000 if len(yy)==2 else int(yy)}" if season else raw

def _pick_date_from_status(status_text: str, keyword: str, fy: str) -> str:
    """Extract date following '<keyword> on ...' from status, return ISO or ""."""
    m = re.search(rf"(?i){keyword}\s+on\s+([A-Za-z0-9 ,/\-]+)", _s(status_text))
    return _parse_date_iso(m.group(1), fallback_year=fy) if m else ""

def _sanitize_metric(text: str, kind: str) -> str:
    """
    Keep original text if number is plausible; else "".
    - gre: allow 130-170 (subscore), 260-340 (new total), 200-800 (old total).
    - gre_v: 130-170.
    - gre_aw: 0-6.
    - gpa: 0-10.
    """
    s = _s(text)
    if not s:
        return ""
    m = re.search(r"(\d+(?:[.,]\d+)?)", s.replace(",", "."))
    if not m:
        return s
    val = float(m.group(1))
    if kind == "gre":
        return s if (130 <= val <= 170) or (260 <= val <= 340) or (200 <= val <= 800) else ""
    if kind == "gre_v":
        return s if 130 <= val <= 170 else ""
    if kind == "gre_aw":
        return s if 0.0 <= val <= 6.0 else ""
    if kind == "gpa":
        return s if 0.0 <= val <= 10.0 else ""
    return s

# ---------------------------- public API ---------------------------- #

def load_data(path: str = "scraped.json"):
    """Load list from JSON; returns [] if file missing."""
    return json.load(open(path, "r", encoding="utf-8")) if os.path.exists(path) else []

def save_data(data, path: str = "applicant_data.json"):
    """Write JSON (pretty)."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def clean_data(input_path: str = "scraped.json", output_path: str = "applicant_data.json"):
    """
    Minimal cleaning with few branches:
      - dates normalized to YYYY-MM-DD.
      - term normalized (f20/s20/etc.).
      - implausible GRE/GPA blanked.
      - missing stays "".
    """
    rows = load_data(input_path)
    out = []

    for it in rows:
        # Read all fields as strings (no new keys introduced)
        program    = _s(it.get("program_name", "")) + ", "+ _s(it.get("university_name", ""))
        masters_phd     = _s(it.get("masters_phd", ""))
        date_added        = _s(it.get("added_on", ""))
        status          = _s(it.get("status", ""))
        applicant_url   = _s(it.get("applicant_url", ""))
        term            = _s(it.get("term", ""))
        student_type    = _s(it.get("student_type", ""))
        gre             = _s(it.get("gre", ""))
        gre_v           = _s(it.get("gre_v", ""))
        gre_aw          = _s(it.get("gre_aw", ""))
        gpa             = _s(it.get("gpa", ""))
        comments        = _s(it.get("comments", ""))

        # Pull decision dates from status if missing; parse all dates to ISO
        added_on    = _parse_date_iso(date_added) or ""

        # Normalize term and metrics
        term  = _term_norm(term)
        gre   = _sanitize_metric(gre, "gre")
        gre_v = _sanitize_metric(gre_v, "gre_v")
        gre_aw= _sanitize_metric(gre_aw, "gre_aw")
        gpa   = _sanitize_metric(gpa, "gpa")

        out.append({
            "program": program,
            "Degree": masters_phd,
            "date_added": added_on,
            "status": status,
            "url": applicant_url,
            "term": term,
            "US/International": student_type,
            "gre": gre,
            "gre_v": gre_v,
            "gre_aw": gre_aw,
            "GPA": gpa,
            "comments": comments
        })

    save_data(out, output_path)
    return out

if __name__ == "__main__":
    clean_data()
