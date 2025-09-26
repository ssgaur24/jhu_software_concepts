# GradCafe Scraper & Cleaner

## Name
Shraddha Shree — JHED: sshree1@jh.edu

## Module Info
Course/Module: Module 2 
Assignment: GradCafe Scraper & Cleaner

## Approach
### Overview
This project scrapes recent admissions entries from TheGradCafe and produces a structured JSON dataset ready for analysis. Unavailable values are preserved as empty strings (""). No new keys are introduced during cleaning.

### Scripts
• scrape.py — HTML fetching and extraction (structure-aware)
• clean.py — minimal normalization/cleaning (date/term/GRE/GPA sanity checks) and JSON output

### Data Model (per row)
Each record is a dict with these keys (all values are strings):
university_name, program_name, masters_phd, added_on, status, applicant_url, term, student_type, gre, gre_v, gre_aw, gpa, comments

## Scraping (scrape.py)
1. Robots gate: check_and_save_robots() downloads and saves robots.txt and checks whether crawling https://www.thegradcafe.com/survey/ is permitted. 
2. Row selection: Only parent <tr> elements without a class attribute are treated as primary rows.
3. Sibling rows: first_tr_sibling_tw() returns the immediate next <tr> only if it has a class matching “tw-border-none” (case-insensitive). This row (if present) contains badges (term, international/american, GRE, GPA). The following immediate “tw-border-none” row (if present) carries comments.
4. Column extraction (parent row):

        td1 div > div → university_name
        td2 div > span:nth-of-type(1) → program_name
        td2 div > span:nth-of-type(2) → masters_ph
        td3 → added_on (raw)
        td4 div → status (raw string; e.g., “Accepted on 30 Nov”)
        td5 → applicant_url (prefers dt:nth-of-type(1) > a:nth-of-type(2), with a simple fallback to any anchor containing “/result/”; query/fragment are stripped)

5. Output: scrape.py writes scraped.json — a list of dicts with the keys above, allowing empty strings where data is missing.

## Cleaning (clean.py)
Cleaning is intentionally minimal and beginner-friendly, with tiny helpers and few branches:

1. Date normalization:
    Dates are parsed to YYYY-MM-DD when possible. Supported inputs include: YYYY-MM-DD, MM/DD/YYYY (or M/D/YY), Mon DD, YYYY, DD Mon YYYY, and month/day without year (falls back to the year found in added_on).
2. 
3. Term normalization: Codes like f20, S20, Fa2020, Fall 2020 are normalized to “Fall 2020”, “Spring 2020”, “Summer 2020”, or “Winter YYYY”.
4. 
5. GRE/GPA sanity checks (keep original text or blank):
   1. gre: allow 130–170 (subscore), 260–340 (new total), 200–800 (old total).
   2. gre_v: 130–170.
   3. gre_aw: 0.0–6.0.
   4. gpa: 0.0–10.0.
   5. Values outside these ranges are blanked ("").
6. Blanks preserved: Any unavailable value remains "".
7. 
8. Output: clean.py writes applicant_data.json with the keys:
   1.     "program": "Social Work, Widener University",
   2.     "Degree": "Masters",
   3.     "date_added": "2025-09-25",
   4.     "status": "Accepted on 23 Sep",
   5.     "url": "https://www.thegradcafe.com/result/986457",
   6.     "term": "Fall 2026",
   7.     "US/International": "American",
   8.     "gre": "",
   9.     "gre_v": "",
   10.     "gre_aw": "",
   11.     "GPA": "GPA 3.76",
   12.     "comments": "".


## Known Bugs / Limitations (and suggested fixes)
4. HTML structure assumptions can break. Selectors assume a consistent table shape; if the site alters markup or injects ads, fields may be missed and become empty.
Fix: add flexible fallbacks (e.g., prefer anchors containing “/result/” within td5) and check for missing nodes.

7. Accepted/Rejected extraction is pattern-based and brittle. Only “Accepted on …” and “Rejected on …” are recognized.
Fix: expand regex to include synonyms (Admit/Denied/Decision Released) and/or map badge labels consistently.

10. GRE ambiguity remains. A bare string like “GRE 168” may be Quant, not total; cleaner only checks numeric plausibility.
Fix: prefer explicit GRE V … / GRE AW … badges; if both GRE and GRE V exist, treat “GRE 13x–17x” as Quant by heuristic.

13. GPA normalization is not implemented. Values like “9/10” or “95%” are not converted to a 4.0 scale.
Fix: parse numerator/denominator or percentage and derive a 4.0-scale value (optionally as a new field if allowed).

16. Status normalization is not enforced. Status is kept as typed (e.g., “Other on 30 Nov”, “Interview on …”).
Fix: map common variants to {accepted, rejected, interview, waitlisted, other}.

19. Student type is not normalized. Variants like “US”, “U.S.”, “Domestic”, “International Student” are kept as-is.
Fix: normalize to {domestic, international} by simple keyword matching.

22. No deduplication. Repeated entries pointing to the same result/ID are not collapsed.
Fix: extract numeric result_id from applicant_url and drop duplicates.

25. Pagination/limits are naive. The scraper loops pages until no parent rows are found or a target length is reached.
Fix: detect last page via pagination controls or server responses.

## How to Run

### Prerequisites:
• Python: 3.10+
• OS: Windows/macOS/Linux
• Dependencies: beautifulsoup4, urllib3 (install via requirements.txt)


### Steps:

Create and activate a virtual environment.

Windows:
```bash
python -m venv .venv
.venv\Scripts\activate
```

macOS/Linux:
```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies:
```bash
pip install -r module_2/requirements.txt
```

Run the scraper (saves scraped.json; respects robots.txt):
```bash
python module_2/scrape.py
```

Run the cleaner (reads scraped.json, writes applicant_data.json):
```bash
python module_2/clean.py
```

(Optional) Verify output count:
```bash
python - << "PY"
import json
with open("module_2/applicant_data.json","r",encoding="utf-8") as f:
data = json.load(f)
print("rows:", len(data))
PY
```

## Robots Compliance
• robots.txt is fetched and saved locally as module_2/robots.txt.
• Include a browser screenshot of robots.txt as module_2/robots_screenshot.jpg.
• README documents the compliance step above.

# LLM cleaning:
```bash
 (cd module_2/llm_hosting && pip install -r requirements.txt)
 python module_2/llm_hosting/app.py --file "module_2/applicant_data.json" > "module_2/llm_extend_applicant_data.json"
```