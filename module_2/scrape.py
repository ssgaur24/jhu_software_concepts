import re
import json
import time

from bs4 import BeautifulSoup, Tag, NavigableString
from urllib.parse import urljoin, urlsplit, urlunsplit, urlparse
import urllib3
http = urllib3.PoolManager(headers={"User-Agent": "Mozilla/5.0"})

url_prefix = "https://www.thegradcafe.com/survey/?page="
target_url = "https://www.thegradcafe.com/survey/"
results = []
target_length = 30000

# ---------- small helpers ----------

def fetch(url: str) -> str:
    r = http.request("GET", url, timeout=urllib3.Timeout(connect=5, read=15))
    return r.data.decode("utf-8", "ignore")

def t(entry: Tag, selector: str) -> str:
    """Return stripped text for the first match of CSS selector under entry, else empty string."""
    node = entry.select_one(selector)
    return node.get_text(strip=True) if node else ""

def first_tr_sibling_tw(tr: Tag) -> Tag | None:
    """
    Return the **immediate next** element-sibling <tr> iff it exists
    and has class 'tw-border-none' (case-insensitive). Otherwise None.
    """
    sib = tr.next_sibling
    # skip whitespace nodes
    while sib and isinstance(sib, NavigableString):
        sib = sib.next_sibling

    # must be a <tr>
    if not (isinstance(sib, Tag) and sib.name == "tr"):
        return None

    # must include class 'tw-border-none' (any casing)
    classes = sib.get("class") or []
    if isinstance(classes, str):
        classes = classes.split()

    return sib if any(c.lower() == "tw-border-none" for c in classes) else None

def has_class(tr: Tag, cls: str) -> bool:
    """True if tr has the given class among its classes."""
    classes = tr.get("class", [])
    return cls in classes if isinstance(classes, list) else classes == cls

# ---------- extractors for each row-kind ----------

def extract_first_dataset(entry: Tag) -> dict:
    """
    Parent row (<tr> with no class).
    Expected columns:
      td1 div>div -> university_name
      td2 div>span(1) -> program_name, span(2) -> masters_phd
      td3 -> added_on (we'll keep full text; can post-process to year)
      td4 div -> parse 'accepted on <date>' / 'rejected on <date>'
      td5 div dt-1 a-2 -> href (applicant URL)
    """
    # td1
    university_name = t(entry, "td:nth-of-type(1) div > div")

    # td2
    program_name = t(entry, "td:nth-of-type(2) div > span:nth-of-type(1)")
    masters_phd  = t(entry, "td:nth-of-type(2) div > span:nth-of-type(2)")

    # td3 (keep raw text; optionally pull year elsewhere)
    added_on = t(entry, "td:nth-of-type(3)")

    # td4: parse accepted/rejected dates
    status_text = t(entry, "td:nth-of-type(4) div")

    # td5: href to applicant
    # inside extract_first_dataset(...)
    origin = f"{urlparse(target_url).scheme}://{urlparse(target_url).netloc}"
    a = entry.select_one("td:nth-of-type(5) div dt:nth-of-type(1) > a:nth-of-type(2)")

    # fallback 1: still inside first <dt>, but if there is only one <a>, use it
    if not a:
        dt1 = entry.select_one("td:nth-of-type(5) dt:nth-of-type(1)")
        if dt1:
            anchors = dt1.select("a")
            if anchors:
                a = anchors[min(1, len(anchors) - 1)]  # pick 2nd if exists, else 1st

    # fallback 2: any anchor in td5 that looks like a result link
    if not a:
        a = entry.select_one("td:nth-of-type(5) a[href*='/result/']")

    if a and a.has_attr("href"):
        href = a["href"]
        abs_url = urljoin(origin, href)  # make absolute
        parts = urlsplit(abs_url)
        url_link = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))  # strip ? and #
    else:
        url_link = ""  # no link found (leave empty string)

    return {
        "university_name": university_name,
        "program_name": program_name,
        "masters_phd": masters_phd,
        "added_on": added_on,
        "status": status_text,
        "applicant_url": url_link,
    }

def extract_second_dataset(entry: Tag | None) -> dict:
    """
    Second row (tr2): must have class 'tw-border-none'.
    Cells:
      td.div.div(2) -> Semester + Year of program start
      td.div.div(3) -> International/American
      td.div.div(4..7) -> any of  'GRE 310', 'GRE V 150', 'GRE AW 3.5', 'GPA 3.62'
    Return empty dict if entry isn’t a matching tr2.
    """
    out = {"term": "", "student_type": "", "gre": "", "gre_v": "", "gre_aw": "", "gpa": ""}
    if not entry or not has_class(entry, "tw-border-none"):
        return out

    # Grab all divs inside td>div>div in order
    divs = entry.select("td > div > div")
    # Defensive indexing per your position notes
    out["term"] = divs[1].get_text(strip=True) if len(divs) > 1 else ""
    out["student_type"] = divs[2].get_text(strip=True) if len(divs) > 2 else ""

    # The remaining slots (4..7) may or may not be present; map by prefix
    for i in range(3, min(len(divs), 7)):
        txt = divs[i].get_text(strip=True)
        if not txt:
            continue
        if re.match(r"^GRE\s+[0-9]", txt, re.I):
            out["gre"] = txt
        elif re.match(r"^GRE\s*V\b", txt, re.I):
            out["gre_v"] = txt
        elif re.match(r"^GRE\s*AW\b", txt, re.I):
            out["gre_aw"] = txt
        elif re.match(r"^GPA\b", txt, re.I):
            out["gpa"] = txt

    return out

def extract_comments(entry: Tag | None) -> str:
    """
    Third row (tr3): only if it ALSO has class 'tw-border-none'; else return blank.
    Content lives under 'td p'.
    """
    if not entry or not has_class(entry, "tw-border-none"):
        return ""
    p = entry.select_one("td p")
    return p.get_text(strip=True) if p else ""

# ---------- main scraper ----------

def scrape_data():
    """
    Iterate pages and build a list of result rows by stitching:
      parent tr (no class) + its next tr (tw-border-none) + optional 3rd tr (tw-border-none for comments).
    Stop when results reach target_length or pages exhaust.
    """
    param_page = 1
    while len(results) <= target_length:
        url = url_prefix + str(param_page)
        html = fetch(url)
        soup = BeautifulSoup(html, "html.parser")

        # Only parent rows: <tr> with no 'class' attribute
        parent_rows = [tr for tr in soup.find_all("tr") if not tr.get("class")]

        if not parent_rows:
            break  # nothing on this page; stop

        for parent in parent_rows:
            row1 = extract_first_dataset(parent)

            # tr2 (details row) : next element <tr>
            tr2 = first_tr_sibling_tw(parent)
            row2 = extract_second_dataset(tr2) if tr2 else {}

            # tr3 (comments row) : next element <tr> after tr2 (may not exist)
            tr3 = first_tr_sibling_tw(tr2) if tr2 else None
            comments = extract_comments(tr3) if tr3 else ""
            if row1["university_name"] != "":
                result_row = {
                    **row1,
                    **row2,
                    "comments": comments,
                }
                results.append(result_row)
        time.sleep(2) #to prevent throttling
        param_page += 1

def create_scraped_json(payload: list[dict], path: str = "scraped.json"):
    """Write the scraped list of dicts to JSON file (UTF-8)."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def check_and_save_robots() -> dict:
    """
    Download robots.txt for the site hosting `target_url`, save it locally,
    and check whether crawling `target_url` is allowed for `user_agent`.

    Returns a small dict: {"robots_url": ..., "saved_to": ..., "allowed": True/False}
    """
    user_agent = "Mozilla/5.0"
    save_path = "robots.txt"
    origin = f"{urlparse(target_url).scheme}://{urlparse(target_url).netloc}"
    robots_url = urljoin(origin, "/robots.txt")

    r = http.request("GET", robots_url, timeout=urllib3.Timeout(connect=5, read=10))
    content = r.data.decode("utf-8", "ignore")

    with open(save_path, "w", encoding="utf-8") as f:
        f.write(content)

    # Use robotparser to evaluate (can_fetch ignores UA wildcards unless matched)
    from urllib import robotparser
    rp = robotparser.RobotFileParser()
    rp.parse(content.splitlines())
    allowed = rp.can_fetch(user_agent, target_url)

    return {"robots_url": robots_url, "saved_to": save_path, "allowed": bool(allowed)}


# ---------- entrypoint ----------

if __name__ == "__main__":
    robot_output = check_and_save_robots()
    if robot_output["allowed"]:
        scrape_data()
        create_scraped_json(results)
    else:
        print(f"Not allowed by robots.txt ({robot_output['robots_url']}).")
        if robot_output.get("error"):
            print("Fetch error:", robot_output["error"])


#a.	Confirm the robot.txt file permits scraping.
#b.	Use urllib3 to request data from grad cafe.
#c.	Use beautifulSoup/regex/string search methods to find admissions data.
"""
table >
tr 1
    td1.div.div value  = university_name

    td2.div.span 1 = program_name
    td2.div.span 2 = Masters_phd

    td3 = extract only year added_on value
    
    td4.div accepted on date rejected on date
    td5 .div.dt1.a2 href value has URL Link to applicant

tr 2  has class tw-border-none and is next tr to the parent tr
    td.div.div1  accepted on date rejected on date  - if tg5 doesnt have data
    td.div.div2 Semester and year of Program start
    td.div.div3 International/American student
    td.div.div 4 5 6 or 7  can have either of these values or none: "GRE 310"   "GRE V 150"  "GRE AW 3.5" "GPA 3.62" , find gre , gre_v, gre_aw, gpa

tr 3  if class = tw-border-none  then 
        tr.td.p value is comments 

else  its next value
    
    
    ·	The data categories pulled SHALL include:
o	Program Name
o	University
o	Comments (if available)
o	Date of Information Added to Grad Café
o	URL link to applicant entry
o	Applicant Status
▪	If Accepted: Acceptance Date
▪	If Rejected: Rejection Date
o	Semester and Year of Program Start (if available)
o	International / American Student (if available)
o	GRE Score (if available)
o	GRE V Score (if available)
o	Masters or PhD (if available)
o	GPA (if available)
o	GRE AW (if available)

"""

