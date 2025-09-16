# step 1 — minimal scraper: single page, extract only admission status via generic regex

# paginate until >= 40,000 unique entries (urllib3 + simple de-dup)
# update —  resume + save per page, changes to run it faster
# speed tweaks — gzip, faster writes, resume, tiny delay
import json, re, time
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import urllib3
from bs4 import BeautifulSoup

BASE = "https://www.thegradcafe.com"
LIST_URL = f"{BASE}/survey/"`

# ---------------- HTTP ----------------
def _http_get(http: urllib3.PoolManager, url: str) -> str:
    # fetch html
    r = http.request(
        "GET",
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=urllib3.Timeout(connect=5, read=20),
    )
    return r.data.decode("utf-8", errors="ignore")

# ---------------- small date helpers ----------------
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
}

def _year_from_text(txt: str) -> Optional[int]:
    # pick a 4-digit year if present
    m = re.search(r"\b(20\d{2})\b", txt or "")
    return int(m.group(1)) if m else None

def _ymd_from_chip(day_mon_txt: str, fallback_year: Optional[int]) -> str:
    # parse like "Accepted on 1 Sep" => combine with fallback year
    m = re.search(r"on\s+(\d{1,2})\s+([A-Za-z]+)", day_mon_txt or "", flags=re.I)
    if not m or not fallback_year:
        return ""
    day = int(m.group(1))
    mon = _MONTHS.get(m.group(2).strip().lower())
    if not mon:
        return ""
    return f"{fallback_year:04d}-{mon:02d}-{day:02d}"

def _ymd_from_notification(notif_txt: str) -> str:
    # parse "on 14/09/2025" or "on 14-09-2025"
    m = re.search(r"on\s+(\d{1,2})[/-](\d{1,2})[/-](\d{4})", notif_txt or "", flags=re.I)
    if not m:
        return ""
    d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y:04d}-{mth:02d}-{d:02d}"

# ---------------- list page parsing ----------------
def _extract_rows_from_list(html: str, seen_urls: Set[str]) -> List[Dict[str, str]]:
    # parse list page and collect base rows
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, str]] = []

    # each result link goes to /result/<id>
    for a in soup.select('a[href^="/result/"]'):
        href = a.get("href", "")
        if not href.startswith("/result/"):
            continue
        url = BASE + href
        if url in seen_urls:
            continue
        seen_urls.add(url)

        row: Dict[str, str] = {"url": url}

        # program + degree
        cell = a.find_parent("td") or a.find_parent("div")
        prog_txt, deg_txt = "", ""
        if cell:
            spans = [s.get_text(strip=True) for s in cell.select("span")]
            if spans:
                prog_txt = spans[0]
                if len(spans) > 1:
                    deg_txt = spans[1]

        # term like "Fall 2026" chip
        term_txt = ""
        if cell:
            chip = cell.find(string=re.compile(r"^(Fall|Spring|Summer)\s+\d{4}$"))
            if chip:
                term_txt = chip.strip()

        # status chip near the row (Accepted / Rejected / Interview / Wait listed)
        tr = a.find_parent("tr")
        status_txt = ""
        if tr:
            # look for text tokens
            for token in ("Accepted", "Rejected", "Interview", "Wait listed"):
                el = tr.find(string=re.compile(rf"\b{re.escape(token)}\b"))
                if el:
                    status_txt = token
                    break

        # date_added column text (e.g., "September 01, 2025")
        date_added = ""
        if tr:
            tds = tr.find_all("td")
            if len(tds) >= 3:
                date_added = tds[2].get_text(" ", strip=True)

        row["program"] = prog_txt.strip()
        row["Degree"] = deg_txt.strip()
        row["term"] = term_txt
        row["status"] = status_txt
        row["date_added"] = date_added
        row["comments"] = ""
        row["US/International"] = ""
        row["university"] = ""  # filled from detail
        row["acceptance_date"] = ""
        row["rejection_date"] = ""
        row["GRE"] = ""
        row["GRE_V"] = ""
        row["GRE_AW"] = ""
        row["GPA"] = ""

        out.append(row)

    return out

# ---------------- detail page parsing ----------------
def _parse_detail(detail_html: str) -> Dict[str, str]:
    # parse dt/dd blocks and special sections
    soup = BeautifulSoup(detail_html, "html.parser")
    mapping: Dict[str, str] = {}

    # map dt -> dd
    for blk in soup.select("div.tw-border-t"):
        dt = blk.find("dt")
        dd = blk.find("dd")
        if dt and dd:
            key = dt.get_text(strip=True)
            val = dd.get_text(" ", strip=True)
            mapping[key] = val

    # notes block
    notes = mapping.get("Notes", "")
    notes = re.sub(r"<[^>]+>", "", notes or "").strip()

    # gre block (three values are rendered as list items; mapping has zeros sometimes)
    gre = mapping.get("GRE General:", "") or ""
    gre_v = mapping.get("GRE Verbal:", "") or ""
    gre_aw = mapping.get("Analytical Writing:", "") or ""

    # institution/program/degree/country/decision/notification
    uni = mapping.get("Institution", "") or ""
    prog = mapping.get("Program", "") or ""
    deg = mapping.get("Degree Type", "") or ""
    country = mapping.get("Degree's Country of Origin", "") or ""
    decision = mapping.get("Decision", "") or ""
    notif = mapping.get("Notification", "") or ""

    # normalize blank-as-empty-string
    def nz(x: Optional[str]) -> str:
        return (x or "").strip()

    return {
        "university": nz(uni),
        "program": nz(prog),
        "Degree": nz(deg),
        "US/International": nz(country),
        "status_detail": nz(decision),
        "notification": nz(notif),
        "comments": nz(notes),
        "GRE": nz(gre),
        "GRE_V": nz(gre_v),
        "GRE_AW": nz(gre_aw),
    }

# ---------------- merge + correct dates ----------------
def _apply_detail_and_fix_dates(row: Dict[str, str], det: Dict[str, str]) -> None:
    # merge detail values if present
    for k in ("university", "program", "Degree", "US/International", "comments", "GRE", "GRE_V", "GRE_AW"):
        v = det.get(k, "")
        if v:
            row[k] = v

    # choose final status
    status = row.get("status") or det.get("status_detail") or ""
    row["status"] = status

    # decision dates: prefer Notification date; else derive from chip + year
    notif = det.get("notification", "")
    date_added = row.get("date_added", "")
    fallback_year = _year_from_text(date_added) or _year_from_text(row.get("term", ""))

    iso_from_notif = _ymd_from_notification(notif)

    if status == "Accepted":
        if iso_from_notif:
            row["acceptance_date"] = iso_from_notif
        else:
            # try chip text next to status on list row: sometimes included in status cell text
            chip_text = row.get("status_chip_text", "")  # not set; kept for compatibility
            # as we do not persist chip text, use date_added's year + common "Accepted on <d Mon>" pattern if present
            if fallback_year:
                # some list rows render like "Accepted on 1 Sep" inside nearby chip; we may not have stored it.
                # If not available, leave as "" to avoid wrong mapping.
                pass
    elif status == "Rejected":
        if iso_from_notif:
            row["rejection_date"] = iso_from_notif
        else:
            if fallback_year:
                pass
    else:
        # Interview/Wait listed -> leave both blank
        pass

# ---------------- main scrape ----------------
def scrape_data(target: int = 30000) -> List[Dict[str, str]]:
    http = urllib3.PoolManager()
    seen: Set[str] = set()
    rows: List[Dict[str, str]] = []
    page = 1

    while len(rows) < target:
        page_url = f"{LIST_URL}?page={page}"
        try:
            html = _http_get(http, page_url)
        except Exception:
            break

        page_rows = _extract_rows_from_list(html, seen)
        if not page_rows:
            break

        # enrich page rows from detail and fix dates
        for r in page_rows:
            try:
                det_html = _http_get(http, r["url"])
                det = _parse_detail(det_html)
                _apply_detail_and_fix_dates(r, det)
            except Exception:
                # keep the list-row data if detail fetch fails
                pass

        rows.extend(page_rows)
        page += 1
        # time.sleep(0.3)

    return rows

def save_data(rows: List[Dict[str, str]], path: Path) -> None:
    # save json
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    # run and tiny print
    data = scrape_data()
    print("total:", len(data))
    print(json.dumps(data[:3], ensure_ascii=False, indent=2))
    out_path = Path(__file__).parent / "applicant_data.json"
    save_data(data, out_path)
