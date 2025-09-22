# step 1 — minimal scraper: incremental using existing applicant_data.json
# gzip, short timeouts, resume, tiny delay; dedup via entry_ur

#Attempting optimizations:
# Incremental-friendly scraper: extracts a stable record with a correct date_added field.
# Important: the table's displayed date is the "date added to GradCafe". Store it as date_added.
# Do not force it into acceptance_date or rejection_date.
# Incremental-friendly scraper: parses listing pages AND enriches each row by visiting
# the /result/<id> detail page. Correctly captures:
#   - date_added  ............ date the entry was posted on GradCafe (listing col 3)
#   - status ................. from detail "Decision" (fallback: listing heuristic)
#   - acceptance/rejection ... from detail "Notification on <date>" routed by status
#   - US/International ....... from detail "Degree's Country of Origin"
#   - GPA/GRE/GRE V/GRE AW ... from detail <dl> block
#   - program/university/degree... prefer detail labels, fallback to listing
#   - comments ............... prefer detail "Notes", fallback to listing quoted text

import re
import json
import time
import urllib3
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
from bs4 import BeautifulSoup

URL = "https://www.thegradcafe.com/survey/"
TARGET = 30000  # stop when no new rows are found for a page
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip, deflate"}

status_pat = re.compile(r"\b(accept\w*|reject\w*|wait[\s-]*list\w*|interview\w*)\b", re.IGNORECASE)
date_pat = re.compile(
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?)\s+\d{1,2},\s*\d{4}\b"
    r"|\b\d{4}-\d{2}-\d{2}\b"
    r"|\b\d{1,2}/\d{1,2}/\d{4}\b",
    re.IGNORECASE,
)
start_term_pat = re.compile(r"\b(Fall|Spring|Summer|Winter)\s+\d{4}\b", re.IGNORECASE)
university_pat = re.compile(
    r"\b([A-Z][A-Za-z.&'\- ]{2,}(?:University|College|Institute|Polytechnic|Tech|State University))\b"
)
ID_RE = re.compile(r"/result/(\d+)")

# ------------------------- helpers -------------------------

def _http_get(http, url: str) -> str:
    r = http.request("GET", url, headers=HEADERS, timeout=urllib3.Timeout(connect=3.0, read=12.0), preload_content=True)
    return r.data.decode("utf-8", errors="ignore")

def _robots_allowed(http, base_url: str, path: str) -> bool:
    robots_url = urljoin(base_url, "/robots.txt")
    txt = _http_get(http, robots_url)
    disallows, in_star = [], False
    for line in txt.splitlines():
        s = line.strip()
        if not s or s.startswith("#"): continue
        low = s.lower()
        if low.startswith("user-agent:"):
            in_star = (s.split(":",1)[1].strip() == "*"); continue
        if in_star and low.startswith("disallow:"):
            disallows.append(s.split(":",1)[1].strip())
    return not any(rule and path.startswith(rule) for rule in disallows)

def _clean(s: str) -> str:
    return " ".join((s or "").split())

def _to_long_date(token: str) -> str:
    """Return 'Month DD, YYYY' from various tokens like '14/09/2025', '2025-09-14', 'Sep 14, 2025'."""
    if not token: return ""
    t = token.strip()
    fmts = ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]
    # choose dd/mm if first component > 12
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", t):
        a, b, c = t.split("/")
        if int(a) > 12:
            fmts = ["%d/%m/%Y", "%m/%d/%Y"] + fmts
    for f in fmts:
        try:
            d = datetime.strptime(t, f)
            return d.strftime("%B %d, %Y")
        except Exception:
            continue
    return ""

def _norm_status(s: str) -> str:
    s = (s or "").lower()
    if s.startswith("accept"): return "Accepted"
    if s.startswith("reject"): return "Rejected"
    if s.startswith("wait"):   return "Wait listed"
    if s.startswith("interview"): return "Interview"
    return s.title() if s else ""

# --------------------- detail page parsing ---------------------

def _parse_detail_fields(html: str) -> dict:
    """Parse <dl> on /result/<id>."""
    soup = BeautifulSoup(html, "html.parser")
    out = {
        "GPA": "", "GRE": "", "GRE V": "", "GRE AW": "",
        "university": "", "program": "", "degree": "",
        "notes": "", "us_intl": "", "decision": "", "notification_on": ""
    }
    # main <dl> items
    for box in soup.select("dl > div"):
        dt = box.find("dt"); dd = box.find("dd")
        if not dt or not dd: continue
        label = _clean(dt.get_text(" ", strip=True))
        value = _clean(dd.get_text(" ", strip=True))
        if not value: continue
        if label == "Institution":
            out["university"] = value
        elif label == "Program":
            out["program"] = value
        elif label == "Degree Type":
            out["degree"] = value
        elif label == "Undergrad GPA":
            out["GPA"] = value
        elif label == "Degree's Country of Origin":
            out["us_intl"] = value
        elif label == "Decision":
            out["decision"] = value
        elif label == "Notification":
            # e.g., "on 14/09/2025 via Phone"
            m = date_pat.search(value)
            out["notification_on"] = _to_long_date(m.group(0)) if m else ""

    # GRE list
    for li in soup.select("dl li"):
        label_el = li.find("span", class_=lambda c: c and "tw-font-medium" in c)
        if not label_el: continue
        val_els = li.find_all("span")
        if not val_els: continue
        label = _clean(label_el.get_text(" ", strip=True)).rstrip(":")
        value = _clean(val_els[-1].get_text(" ", strip=True))
        if label == "GRE General":
            out["GRE"] = value
        elif label == "GRE Verbal":
            out["GRE V"] = value
        elif label == "Analytical Writing":
            out["GRE AW"] = value

    # Notes
    notes_dt = soup.find("dt", string=re.compile(r"^Notes$", re.I))
    if notes_dt:
        dd = notes_dt.find_next("dd")
        if dd:
            out["notes"] = _clean(dd.get_text(" ", strip=True))

    return out

def _fetch_detail(http, entry_url: str) -> dict:
    try:
        html = _http_get(http, entry_url)
        time.sleep(0.08)  # polite
        return _parse_detail_fields(html)
    except Exception:
        return {}

# --------------------- listing page parsing ---------------------

def _extract_rows_from_html(html: str, page_url: str, seen_urls: set, http) -> list:
    soup = BeautifulSoup(html, "html.parser")
    new_rows = []
    for tr in soup.select("tr"):
        row_text = tr.get_text(" ", strip=True)
        if not status_pat.search(row_text):
            continue

        prev = tr.find_previous("tr")
        prev_text = prev.get_text(" ", strip=True) if prev else ""
        ctx = f"{prev_text} {row_text}".strip()

        # listing heuristics
        m_status = status_pat.search(row_text)
        list_status = _norm_status(m_status.group(1)) if m_status else ""

        tds = tr.find_all("td")
        date_added = ""
        if len(tds) >= 3:
            raw_date = tds[2].get_text(" ", strip=True)
            m_added = date_pat.search(raw_date)
            date_added = m_added.group(0) if m_added else ""

        # start term (best-effort)
        m_term = start_term_pat.search(ctx)
        start_term = m_term.group(0) if m_term else ""

        # entry URL
        a = tr.select_one('a[href^="/result/"]') or (prev.select_one('a[href^="/result/"]') if prev else None)
        entry_url = urljoin(page_url, a["href"]) if a and a.get("href") else ""
        if not entry_url or entry_url in seen_urls:
            continue

        # rough program/degree from listing (may be replaced by detail)
        program = degree = university = ""
        if len(tds) >= 2:
            mid = tds[1]
            spans = mid.select("div span")
            if spans:
                program = _clean(spans[0].get_text(strip=True))
                deg_span = mid.select_one("span.tw-text-gray-500")
                degree = _clean(deg_span.get_text(strip=True)) if deg_span else (_clean(spans[-1].get_text(strip=True)) if len(spans) >= 2 else "")

        comments = ""
        q = re.search(r'"([^"]{3,})"', ctx)
        if q:
            comments = q.group(1)

        # detail enrich
        d = _fetch_detail(http, entry_url)
        status = _norm_status(d.get("decision") or list_status)
        note_date = d.get("notification_on", "")

        # Route notification date into acceptance/rejection when applicable
        acceptance_date = note_date if status == "Accepted" else ""
        rejection_date = note_date if status == "Rejected" else ""

        # Final record
        rec = {
            "status": status,
            "date_added": date_added,              # posted-on date (assignment)
            "acceptance_date": acceptance_date,    # from detail Notification
            "rejection_date": rejection_date,      # from detail Notification
            "start_term": start_term,
            "degree": d.get("degree") or degree,
            "program": d.get("program") or program,
            "university": d.get("university") or university,
            "comments": d.get("notes") or comments,
            "entry_url": entry_url,
            "US/International": d.get("us_intl", ""),
            "GRE": d.get("GRE", ""),
            "GRE V": d.get("GRE V", ""),
            "GPA": d.get("GPA", ""),
            "GRE AW": d.get("GRE AW", ""),
        }
        new_rows.append(rec)
        seen_urls.add(entry_url)
    return new_rows

# --------------------- main ---------------------

def main():
    http = urllib3.PoolManager()
    parsed = urlparse(URL)
    base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or "/"
    if not _robots_allowed(http, base, path):
        print("robots: not allowed for this path — exiting")
        return

    out_path = Path(__file__).parent / "applicant_data.json"
    rows, seen = [], set()
    if out_path.exists():
        try:
            rows = json.loads(out_path.read_text(encoding="utf-8"))
            for r in rows:
                if r.get("entry_url"):
                    seen.add(r["entry_url"])
            print(f"resume: {len(rows)} records")
        except Exception:
            rows, seen = [], set()

    # page 1
    html = _http_get(http, URL)
    page_rows = _extract_rows_from_html(html, URL, seen, http)
    if page_rows:
        rows.extend(page_rows)
        out_path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"page 1 +{len(page_rows)} total {len(rows)}")

    # subsequent pages
    page = 2
    while len(rows) < TARGET:
        page_url = f"{URL}?page={page}"
        html = _http_get(http, page_url)
        before = len(rows)
        page_rows = _extract_rows_from_html(html, page_url, seen, http)
        rows.extend(page_rows)
        added = len(rows) - before
        out_path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"page {page} +{added} total {len(rows)}")
        if added == 0:
            break
        page += 1
        time.sleep(0.05)

    print(f"saved: {out_path} ({len(rows)} records)")

if __name__ == "__main__":
    main()
