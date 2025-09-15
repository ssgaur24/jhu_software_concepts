# Module 3 — SQL Data Analysis

## Approach
- Create one PostgreSQL table **`applicants`** with the required columns.
- Provide **`load_data.py`** to initialize the schema and load the Module-2 cleaned JSON.
- Keep categorical fields as **TEXT**, scores as **REAL**, and the date as **DATE**.
- Print tiny checks only (counts and first IDs). Use batched inserts for speed.

## Structure
~~~
module_3/
  config.ini
  load_data.py
  db_check.py
  requirements.txt
  src/
    config.py
    dal/
      pool.py
      schema.py
      loader.py
  data/
    module_2llm_extend_applicant_data.json
  screenshots/
    <images>
  limitations.pdf
  artifacts/
    load_report.json
~~~

## Setup (Windows / Python 3.13)
~~~bash
python -m venv .venv
. .venv/Scripts/Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r module_3/requirements.txt
~~~

## Database config
- Edit `module_3/config.ini`. If `DATABASE_URL` environment variable is present, it overrides `config.ini`.

## Load data (fast; tiny checks)
~~~bash
python module_3/load_data.py --init \
  --load module_3/data/module_2llm_extend_applicant_data.json \
  --batch 2000 --count
~~~

## Part B: Run UI
~~~bash
python app.py
~~~

## Verify database and table visibility
~~~bash
python module_3/db_check.py
# prints:
# config_source=ini:config.ini   (or env:DATABASE_URL)
# url=postgresql://user:****@localhost:5432/gradcafe
# db=gradcafe user=... schema=public
# applicants_exists=true rows=28596
~~~

## Data mapping
- `status` → status (TEXT)  
- `acceptance_date` / `rejection_date` / `date_added` → date_added (DATE)  
- `start_term` → term (TEXT)  
- `degree` → degree (REAL) — non-numeric becomes **NULL** (audited)  
- `program` + `university` → program (TEXT; `"University - Program"` if both)  
- `comments` → comments (TEXT)  
- `entry_url` → url (TEXT) and `p_id` (digits after `/result/<id>`)  
- `US/International` → us_or_international (TEXT)  
- `GPA`, `GRE`, `GRE V`, `GRE AW` → gpa/gre/gre_v/gre_aw (REAL)  
- `llm-generated-program` → llm_generated_program (TEXT)  
- `llm-generated-university` → llm_generated_university (TEXT)

Records without a stable id are skipped and reported.

## Run results (current dataset)
~~~text
loaded_records=30012 inserted=28596 skipped=1416 issues={'missing_p_id': 1416, 'date_parse_fail': 0, 'gpa_non_numeric': 0, 'gre_non_numeric': 0, 'gre_v_non_numeric': 0, 'gre_aw_non_numeric': 0, 'degree_non_numeric': 28596} sample_ids=[787144, 787145, 787146]
~~~

## Planned follow-ups (concise)
- **Missing `p_id`**: preserve `entry_url` in Module-2; add validation; log skipped rows to CSV.
- **`degree` textual**: keep `degree` as REAL; map text to numeric in Module-2 or use SQL CASE; NULLs are audited.
- **Auditing**: `module_3/artifacts/load_report.json` keeps counts and sample ids.

## Issues encountered (and fixes)
- pip resolver conflicts on Windows / Python 3.13 → pin psycopg 3.2.x and install in `.venv`.
- PyCharm interpreter selection → attach `.venv\Scripts\python.exe` in project and run config.
- Pool worker threads not stopping → explicit `close_pool()` and `atexit`.
- File path mistakes → track `module_3/data/` and use real paths.
- Config precedence → `DATABASE_URL` overrides `config.ini` if present.
- JSON loading speed (≈30k rows) → batched inserts in a single transaction.
- Data formatting quirks → numeric coercion with NULL fallback, robust date parsing, skip rows without id.

## Part B — Webpage Buttons (Pull Data & Update Analysis)

- **Pull Data**: Fetches only *new* entries from GradCafe (incremental), cleans/deduplicates them, runs the instructor’s LLM standardizer to populate `llm-generated-university` / `llm-generated-program`, and **loads the updated JSON into Postgres**. A small lock (`module_3/artifacts/pull.lock`) prevents overlapping pulls.
- **Update Analysis**: Recomputes all on-page answers using the current database. If a pull is running, this button is disabled and does nothing (the page shows a notice).
- Dependencies for the scraper (`urllib3`, `beautifulsoup4`) are installed automatically the first time **Pull Data** runs, based on `module_3/module_2_ref/requirements.txt`.
- **Note:** Numbers (and even which questions have data) may vary by dataset and run; the page reflects the latest rows successfully loaded.

