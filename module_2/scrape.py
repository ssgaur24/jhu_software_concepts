# step 1 — minimal scraper: single page, extract only admission status via generic regex

from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

URL = "https://www.thegradcafe.com/survey/"

def main():
    # open page
    with urlopen(URL) as resp:
        html = resp.read().decode("utf-8", errors="ignore")

    # parse html
    soup = BeautifulSoup(html, "html.parser")

    # pull text
    text = soup.get_text(" ", strip=True)

    # find status tokens (generic stems: accept/reject/waitlist/interview)
    status_pat = re.compile(r"\b(?:accept\w*|reject\w*|wait[\s-]*list\w*|interview\w*)\b", re.IGNORECASE)
    matches = status_pat.findall(text)

    # print count
    print(f"status_count: {len(matches)}")

if __name__ == "__main__":
    main()
