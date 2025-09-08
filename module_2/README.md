# Name: Shraddha Shree

# JHED: sshree1@jh.edu

# 

# Module Info: JHU EP 605.256 Module 2: Web Scraping 

# 

# Approach:

# \- Checked robots.txt for the survey path.

# \- Used urllib3 (timeouts, retries, UA) to fetch pages; BeautifulSoup to parse.

# \- Extracted fields with regex/string methods; removed HTML; de-duplicated by entry URL.

# \- Saved applicant\_data.json (UTF-8). Cleaned to consistent blanks.

# \- Collected ≥ 30,000 entries. Ran the provided llm\_hosting tool to add standardized

# &nbsp; program/university and wrote llm\_extend\_applicant\_data.json.

# 

# Known Bugs:

# \- <None observed>.

How to run:
1) python module_2/scrape.py
2) python module_2/clean.py
3) python module_2/size_check.py
LLM cleaning:
- (cd module_2/llm_hosting && pip install -r requirements.txt)
- python module_2/llm_hosting/app.py --file "module_2/applicant_data.json" > "module_2/llm_extend_applicant_data.json"
Robots:
- Checked robots.txt (screenshot in module_2/robots_screenshot.jpg)
EOF

