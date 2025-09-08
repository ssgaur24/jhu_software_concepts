# step 1 — minimal scraper: single page, extract only admission status via generic regex

from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import json
from pathlib import Path
from urllib.parse import urljoin

URL = "https://www.thegradcafe.com/survey/"

# generic patterns
status_pat = re.compile(r"\b(accept\w*|reject\w*|wait[\s-]*list\w*|interview\w*)\b", re.IGNORECASE)
date_pat = re.compile(
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s*\d{4}\b"
    r"|\b\d{4}-\d{2}-\d{2}\b",
    re.IGNORECASE,
)
start_term_pat = re.compile(r"\b(Fall|Spring|Summer|Winter)\s+\d{4}\b", re.IGNORECASE)
university_pat = re.compile(
    r"\b([A-Z][A-Za-z.&'\- ]{2,}(?:University|College|Institute|Polytechnic|Tech|State University))\b"
)

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

def main():
    # open page
    with urlopen(URL) as resp:
        html = resp.read().decode("utf-8", errors="ignore")

    # parse html
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    # iterate result rows
    for tr in soup.select("tr"):
        # row text
        row_text = tr.get_text(" ", strip=True)
        if not status_pat.search(row_text):
            continue

        # small context window (previous row + this row)
        prev = tr.find_previous("tr")
        prev_text = prev.get_text(" ", strip=True) if prev else ""
        ctx = f"{prev_text} {row_text}".strip()

        # status
        m_status = status_pat.search(row_text)
        status = _norm_status(m_status.group(1)) if m_status else ""

        # dates
        acceptance_date = ""
        rejection_date = ""
        m_date = date_pat.search(ctx)
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
            entry_url = urljoin(URL, a["href"])

        # program + degree from tags in the middle cell
        program_name = ""
        degree = ""
        tds = tr.find_all("td")
        if len(tds) >= 2:
            mid = tds[1]
            # the first span holds program name
            spans = mid.select("div span")
            if spans:
                program_name = spans[0].get_text(strip=True)
                # try dedicated gray span for degree, else last span
                deg_span = mid.select_one("span.tw-text-gray-500")
                if deg_span:
                    degree = deg_span.get_text(strip=True)
                elif len(spans) >= 2:
                    degree = spans[-1].get_text(strip=True)

        # university (try within this row first; else fallback to regex on context)
        university = ""
        uni_a = tr.find("a", string=university_pat)
        if uni_a:
            university = uni_a.get_text(strip=True)
        if not university:
            m_uni = university_pat.search(ctx)
            university = m_uni.group(1) if m_uni else ""

        # comments (simple quote/dash heuristic on context)
        comments = ""
        q = re.search(r'"([^"]{3,})"', ctx)
        if q:
            comments = q.group(1)
        else:
            dash = re.search(r"\s[-–—]\s(.{5,120})$", row_text)
            if dash:
                comments = dash.group(1)

        rows.append({
            "status": status,
            "acceptance_date": acceptance_date,
            "rejection_date": rejection_date,
            "start_term": start_term,
            "degree": degree,
            "program": program,
            "university": university,
            "comments": comments,
            "entry_url": entry_url if entry_url else f"{URL}#row",
        })

    # save json next to this script
    out_path = Path(__file__).parent / "applicant_data.json"
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2))

    # tiny sample
    print(f"saved: {out_path} ({len(rows)} records)")
    for rec in rows[:10]:
        print({k: rec[k] for k in ["status", "program", "degree", "entry_url"]})

if __name__ == "__main__":
    main()