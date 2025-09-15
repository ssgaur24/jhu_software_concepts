# step 1 — minimal scraper: incremental using existing applicant_data.json
# gzip, short timeouts, resume, tiny delay; dedup via entry_url

import re
import json
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import urllib3
from bs4 import BeautifulSoup

URL = "https://www.thegradcafe.com/survey/"
TARGET = 30000

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

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip, deflate"}

def _norm_status(s: str) -> str:
    s = s.lower()
    if s.startswith("accept"): return "Accepted"
    if s.startswith("reject"): return "Rejected"
    if s.startswith("wait"):   return "Wait listed"
    if s.startswith("interview"): return "Interview"
    return s.title()

def _http_get(http, url: str) -> str:
    r = http.request("GET", url, headers=HEADERS, timeout=urllib3.Timeout(connect=3.0, read=10.0), preload_content=True)
    return r.data.decode("utf-8", errors="ignore")

def _robots_allowed(http, base_url: str, path: str) -> bool:
    robots_url = urljoin(base_url, "/robots.txt")
    txt = _http_get(http, robots_url)
    disallows, in_star = [], False
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"): continue
        low = line.lower()
        if low.startswith("user-agent:"):
            in_star = (line.split(":",1)[1].strip() == "*"); continue
        if in_star and low.startswith("disallow:"):
            disallows.append(line.split(":",1)[1].strip())
    return not any(rule and path.startswith(rule) for rule in disallows)

def _extract_rows_from_html(html: str, page_url: str, seen_urls: set):
    soup = BeautifulSoup(html, "html.parser")
    new_rows = []
    for tr in soup.select("tr"):
        row_text = tr.get_text(" ", strip=True)
        if not status_pat.search(row_text): continue

        prev = tr.find_previous("tr")
        prev_text = prev.get_text(" ", strip=True) if prev else ""
        ctx = f"{prev_text} {row_text}".strip()

        # status
        m_status = status_pat.search(row_text)
        status = _norm_status(m_status.group(1)) if m_status else ""

        # dates
        acceptance_date = rejection_date = ""
        tds = tr.find_all("td")
        row_date_text = tds[2].get_text(" ", strip=True) if len(tds) >= 3 else ""
        m_date = date_pat.search(row_date_text) or date_pat.search(ctx)
        if m_date:
            if status == "Accepted":  acceptance_date = m_date.group(0)
            elif status == "Rejected": rejection_date = m_date.group(0)

        # start term
        m_term = start_term_pat.search(ctx)
        start_term = m_term.group(0) if m_term else ""

        # entry url
        entry_url = ""
        a = tr.select_one('a[href^="/result/"]') or (prev.select_one('a[href^="/result/"]') if prev else None)
        entry_url = urljoin(page_url, a["href"]) if a and a.get("href") else f"{page_url}#row"
        if entry_url in seen_urls: continue

        # program, degree
        program = degree = ""
        if len(tds) >= 2:
            mid = tds[1]
            spans = mid.select("div span")
            if spans:
                program = spans[0].get_text(strip=True)
                deg_span = mid.select_one("span.tw-text-gray-500")
                degree = deg_span.get_text(strip=True) if deg_span else (spans[-1].get_text(strip=True) if len(spans) >= 2 else "")

        # university
        university = ""
        uni_a = tr.find("a", string=university_pat)
        if uni_a: university = uni_a.get_text(strip=True)

        # comments
        comments = ""
        q = re.search(r'"([^"]{3,})"', ctx)
        if q: comments = q.group(1)

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
    parsed = urlparse(URL); base = f"{parsed.scheme}://{parsed.netloc}"; path = parsed.path or "/"
    if not _robots_allowed(http, base, path):
        print("robots: not allowed for this path — exiting"); return

    out_path = Path(__file__).parent / "applicant_data.json"
    rows, seen = [], set()
    if out_path.exists():
        try:
            rows = json.loads(out_path.read_text(encoding="utf-8"))
            for r in rows:
                u = r.get("entry_url","");
                if u: seen.add(u)
            print(f"resume: {len(rows)} records")
        except Exception:
            rows, seen = [], set()

    html = _http_get(http, URL)
    page_rows = _extract_rows_from_html(html, URL, seen)
    if page_rows:
        rows.extend(page_rows)
        out_path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"page 1 +{len(page_rows)} total {len(rows)}")

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
        if added == 0: break
        page += 1; time.sleep(0.05)

    print(f"saved: {out_path} ({len(rows)} records)")

if __name__ == "__main__":
    main()
