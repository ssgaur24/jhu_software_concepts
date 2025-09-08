# step 1 — minimal scraper: single page, extract only admission status via generic regex

# paginate until >= 40,000 unique entries (urllib3 + simple de-dup)
# update —  resume + save per page, changes to run it faster
# speed tweaks — gzip, faster writes, resume, tiny delay

import re
import json
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import urllib3
from bs4 import BeautifulSoup

URL = "https://www.thegradcafe.com/survey/"
TARGET = 30000  # aiming for 30k now to finish

# precompiled patterns
status_pat = re.compile(r"\b(accept\w*|reject\w*|wait[\s-]*list\w*|interview\w*)\b", re.IGNORECASE)
date_pat = re.compile(
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?)\s+\d{1,2},\s*\d{4}\b"
    r"|\b\d{4}-\d{2}-\d{2}\b",
    re.IGNORECASE,
)
start_term_pat = re.compile(r"\b(Fall|Spring|Summer|Winter)\s+\d{4}\b", re.IGNORECASE)
university_pat = re.compile(
    r"\b([A-Z][A-Za-z.&'\- ]{2,}(?:University|College|Institute|Polytechnic|Tech|State University))\b"
)

# request headers + short timeouts (faster)
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Encoding": "gzip, deflate",
}

def _norm_status(s: str) -> str:
    # normalize status token
    s = s.lower()
    if s.startswith("accept"):
        return "Accepted"
    if s.startswith("reject"):
        return "Rejected"
    if s.startswith("wait"):
        return "Wait listed"
    if s.startswith("interview"):
        return "Interview"
    return s.title()

def _http_get(http, url: str) -> str:
    # open page (short timeouts; gzip)
    r = http.request(
        "GET",
        url,
        headers=HEADERS,
        timeout=urllib3.Timeout(connect=3.0, read=10.0),
        preload_content=True,
    )
    return r.data.decode("utf-8", errors="ignore")

def _robots_allowed(http, base_url: str, path: str) -> bool:
    # basic robots.txt check
    robots_url = urljoin(base_url, "/robots.txt")
    txt = _http_get(http, robots_url)
    disallows = []
    in_star = False
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        low = line.lower()
        if low.startswith("user-agent:"):
            agent = line.split(":", 1)[1].strip()
            in_star = (agent == "*")
            continue
        if in_star and low.startswith("disallow:"):
            rule = line.split(":", 1)[1].strip()
            disallows.append(rule)
    for rule in disallows:
        if rule and path.startswith(rule):
            return False
    return True

def _extract_rows_from_html(html: str, page_url: str, seen_urls: set):
    # parse one page and return only new rows
    soup = BeautifulSoup(html, "html.parser")
    new_rows = []

    for tr in soup.select("tr"):
        row_text = tr.get_text(" ", strip=True)
        if not status_pat.search(row_text):
            continue

        # small context: this row + previous row (for term/labels if present)
        prev = tr.find_previous("tr")
        prev_text = prev.get_text(" ", strip=True) if prev else ""
        ctx = f"{prev_text} {row_text}".strip()

        # status
        m_status = status_pat.search(row_text)
        status = _norm_status(m_status.group(1)) if m_status else ""

        # dates
        acceptance_date = ""
        rejection_date = ""
        # try date from this row's cells first (often in the 3rd td)
        tds = tr.find_all("td")
        row_date_text = ""
        if len(tds) >= 3:
            row_date_text = tds[2].get_text(" ", strip=True)
        m_date = date_pat.search(row_date_text) or date_pat.search(ctx)
        if m_date:
            if status == "Accepted":
                acceptance_date = m_date.group(0)
            elif status == "Rejected":
                rejection_date = m_date.group(0)

        # start term
        m_term = start_term_pat.search(ctx)
        start_term = m_term.group(0) if m_term else ""

        # entry url
        entry_url = ""
        a = tr.select_one('a[href^="/result/"]')
        if not a and prev:
            a = prev.select_one('a[href^="/result/"]')
        if a and a.get("href"):
            entry_url = urljoin(page_url, a["href"])
        else:
            entry_url = f"{page_url}#row"

        if entry_url in seen_urls:
            continue

        # program and degree separately (no concatenation)
        program = ""
        degree = ""
        if len(tds) >= 2:
            mid = tds[1]
            spans = mid.select("div span")
            if spans:
                program = spans[0].get_text(strip=True)
                deg_span = mid.select_one("span.tw-text-gray-500")
                if deg_span:
                    degree = deg_span.get_text(strip=True)
                elif len(spans) >= 2:
                    degree = spans[-1].get_text(strip=True)

        # university
        university = ""
        uni_a = tr.find("a", string=university_pat)
        if uni_a:
            university = uni_a.get_text(strip=True)
        if not university:
            m_uni = university_pat.search(ctx)
            university = m_uni.group(1) if m_uni else ""

        # comments (quick heuristic)
        comments = ""
        q = re.search(r'"([^"]{3,})"', ctx)
        if q:
            comments = q.group(1)
        else:
            dash = re.search(r"\s[-–—]\s(.{5,120})$", row_text)
            if dash:
                comments = dash.group(1)

        new_rows.append({
            "status": status,
            "acceptance_date": acceptance_date,
            "rejection_date": rejection_date,
            "start_term": start_term,
            "degree": degree,
            "program": program,
            "university": university,
            "comments": comments,
            "entry_url": entry_url,
        })
        seen_urls.add(entry_url)

    return new_rows

def main():
    http = urllib3.PoolManager()

    # robots check once
    parsed = urlparse(URL)
    base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or "/"
    if not _robots_allowed(http, base, path):
        print("robots: not allowed for this path — exiting")
        return

    out_path = Path(__file__).parent / "applicant_data.json"

    # resume from existing file
    rows = []
    seen = set()
    if out_path.exists():
        try:
            rows = json.loads(out_path.read_text(encoding="utf-8"))
            for r in rows:
                u = r.get("entry_url", "")
                if u:
                    seen.add(u)
            print(f"resume: {len(rows)} records")
        except Exception:
            rows = []
            seen = set()

    # first page
    if len(rows) < TARGET:
        html = _http_get(http, URL)
        page_rows = _extract_rows_from_html(html, URL, seen)
        if page_rows:
            rows.extend(page_rows)
            # fast write (no pretty printing)
            out_path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            print(f"page 1 +{len(page_rows)} total {len(rows)}")

    # paginate; tiny delay; save after each page (compact json for speed)
    page = 2
    while len(rows) < TARGET:
        page_url = f"{URL}?page={page}"
        html = _http_get(http, page_url)
        before = len(rows)
        page_rows = _extract_rows_from_html(html, page_url, seen)
        rows.extend(page_rows)
        added = len(rows) - before

        out_path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"page {page} +{added} total {len(rows)}")

        if added == 0:
            break
        page += 1
        time.sleep(0.05)  # keep a very small pause

    # final line
    print(f"saved: {out_path} ({len(rows)} records)")

if __name__ == "__main__":
    main()
