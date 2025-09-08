# step 1 — minimal scraper: single page, extract only admission status via generic regex

from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

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
degree_pat = re.compile(r"\b(PhD|Masters|MBA|MFA|JD|PsyD|EdD|IND|Other)\b", re.IGNORECASE)

def _norm_status(s: str) -> str:
    # normalize status token to a simple label
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

    # get lines to inspect structure
    text = soup.get_text("\n", strip=True)
    lines = [ln for ln in text.split("\n") if ln]

    rows = []
    # find status lines and nearby fields
    for i, ln in enumerate(lines):
        if not status_pat.search(ln):
            continue

        # small context window
        ctx = " ".join(lines[max(0, i - 1): i + 2])

        # status
        m_status = status_pat.search(ln)
        status = _norm_status(m_status.group(1)) if m_status else ""

        # date (assign to acceptance_date if Accepted, else rejection_date if Rejected)
        m_date = date_pat.search(ctx)
        acceptance_date = ""
        rejection_date = ""
        if m_date:
            if status == "Accepted":
                acceptance_date = m_date.group(0)
            elif status == "Rejected":
                rejection_date = m_date.group(0)

        # start term
        m_term = start_term_pat.search(ctx)
        start_term = m_term.group(0) if m_term else ""

        # degree
        m_degree = degree_pat.search(ctx)
        degree = m_degree.group(1) if m_degree else ""

        rows.append({
            "status": status,
            "acceptance_date": acceptance_date,
            "rejection_date": rejection_date,
            "start_term": start_term,
            "degree": degree,
        })

    # show first 3 small dicts
    for rec in rows[:3]:
        print(rec)
    matches = status_pat.findall(text)
    print(f"status_count: {len(matches)}")

if __name__ == "__main__":
    main()