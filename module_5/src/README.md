# Module 3 — SQL Data Analysis

## Approach
- Create one PostgreSQL table **`applicants`** with the required columns (public schema).
- Use **`load_data.py`** to create the table (if needed) and load the Module-2 extended JSON.
- Use **`app.py`** (Flask) to add two buttons: **Pull Data** (scrape → clean → standardize new rows → load) and **Update Analysis** (refresh results unless a pull is running).
- Centralize the analytics in **`query_data.py`** and reuse them in both CLI and the web page.
- Read database settings from **`config.ini`** under `[db]`.

## Structure
~~~
module_3/
  app.py
  load_data.py
  query_data.py
  config.ini
  requirements.txt
  templates/
    base.html
    index.html
  data/
    module2_llm_extend_applicant_data.json
module_2/
  scrape.py
  clean.py
  llm_hosting/
    app.py
~~~

## Setup (Windows / Python 3.13)
~~~bash
python -m venv .venv
. .venv/Scripts/Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r module_3/requirements.txt
python -m pip install -r module_2/requirements.txt
python -m pip install -r module_2/llm_hosting/requirements.txt
~~~

## Steps to install the LLM module dependencies
~~~bash
# Standard install
python -m pip install -r module_2/llm_hosting/requirements.txt

# Optional (CPU wheel index for llama-cpp on Windows):
py -3.13 -m venv ".venv" && ".venv\Scripts\python" -m pip install -U pip wheel setuptools && ^
".venv\Scripts\python" -m pip install --prefer-binary --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cpu -r "module_2\llm_hosting\requirements.txt"
~~~

## Database config
- Edit **`module_3/config.ini`**:
~~~
[db]
host = localhost
port = 5432
database = gradcafe
user = postgres
password = password
~~~
- Scripts read from `config.ini` directly.

## Load data 
~~~bash
# Use default path expected by load_data.py
python module_3/load_data.py

# Or pass a specific JSON
python module_3/load_data.py module_3/data/module2_llm_extend_applicant_data.json
~~~

## Part B: Run UI
~~~bash
python module_3/app.py
~~~

## Verify database / quick check
~~~bash
# Print Q1–Q12 to console (same queries as UI)
python module_3/query_data.py

# Check row count (example with psql)
# psql -d gradcafe -c "SELECT COUNT(*) FROM public.applicants;"
~~~

## Data mapping
- `status` → status (TEXT)  
- `date_added` → date_added (DATE)  
- `term` → term (TEXT)  
- `degree` → degree (TEXT)  
- `program` → program (TEXT)  
- `comments` → comments (TEXT)  
- `url` → url (TEXT)  
- `US/International` → us_or_international (TEXT)  
- `GPA`, `gre`, `gre_v`, `gre_aw` → gpa / gre / gre_v / gre_aw (REAL; numeric part extracted when present)  
- `llm-generated-program` → llm_generated_program (TEXT)  
- `llm-generated-university` → llm_generated_university (TEXT)

## Example loader output
~~~text
Loaded 1234 rows into 'applicants' from: module_3/data/module2_llm_extend_applicant_data.json
~~~

## Part B — Webpage Buttons (Pull Data & Update Analysis)
- **Pull Data** pipeline:
1) `module_2/scrape.py`
2) `module_2/clean.py`
3) `module_2/llm_hosting/app.py --file module_2/applicant_data.json`  
   - If a previous extended file exists at `module_3/data/module2_llm_extend_applicant_data.json`, the app passes `--only-new --prev <that file>` to standardize only new rows.
4) Save the LLM’s JSON stdout to `module_3/data/module2_llm_extend_applicant_data.json`
5) `module_3/load_data.py module_3/data/module2_llm_extend_applicant_data.json`
- A small lock file **`module_3/pull.lock`** prevents overlapping pulls.
- **Update Analysis**:
- Recomputes and refreshes Q1–Q12 from the database.
- Does nothing while a pull is running; the page shows a notice.

## Notes
- UI is styled with **Bootstrap 5** (CDN) via `templates/base.html`.
- All SQL uses **parameterized** statements.
- SQL filters use simple `ILIKE` patterns for terms, universities, and programs.

## Known issues / bugs
- **Lock file leftovers**: If the server stops while pulling data, `module_3/pull.lock` may remain and block actions; delete the file to clear the state.
- **First-run LLM download**: `llm_hosting/app.py` may download a large model on first run; this can take time and requires network access.
- **CPU performance**: The LLM step can be slow on CPU-only environments; verify that requirements install correctly for your platform.
- **Path expectations**: The app builds paths relative to the repo layout shown above; different layouts may require updating `app.py` path joins.
- **Corrupted previous JSON**: If the previous extended file is malformed, the `--only-new --prev` step will fail; remove or fix the previous JSON and rerun.
- **Date values**: `date_added` must be a valid date string; invalid values will fail insertion.
- **Network/CDN**: Bootstrap assets load from a CDN; a restricted network will render the page unstyled unless you bundle CSS locally.
