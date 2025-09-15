# Module 3 — SQL Data Analysis

## Approach 
- Create one PostgreSQL table **`applicants`** with the required columns.
- Provide **`load_data.py`** to initialize the schema and load the **Module-2 cleaned JSON**.
- Keep categorical fields as **TEXT**, scores as **REAL**, and the date as **DATE**.
- Print only tiny checks (counts + first few IDs) and keep runs fast (batched inserts).

## Project structure
~~~
module_3/
  config.ini
  load_data.py                  # REQUIRED entry point
  requirements.txt
  src/
    config.py                   # ENV/INI -> DATABASE_URL resolver
    dal/
      pool.py                   # psycopg3 pool (clean shutdown)
      schema.py                 # DDL + tiny count
      loader.py                 # fast JSON loader (batch inserts)
  data/
    .keep                       # put your JSON here
~~~

## Setup (Windows / Python 3.13)
~~~bash
python -m venv .venv
. .venv/Scripts/Activate.ps1     # Git Bash: source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r module_3/requirements.txt
~~~

`module_3/requirements.txt` pins **psycopg[binary]==3.2.10** and **psycopg_pool 3.2.x** (works on Python 3.13).

## Database config
- Edit `module_3/config.ini` (blank password is OK if local Postgres allows it).
- If set, **`DATABASE_URL`** env var overrides the INI.

## How to run (fast; tiny checks)
~~~bash
# put file here first:
#   module_3/data/module_2llm_extend_applicant_data.json

# initialize schema, load JSON with batches, and print counts
python module_3/load_data.py --init \
  --load module_3/data/module_2llm_extend_applicant_data.json \
  --batch 2000 --count
# example output:
# loaded_records=30000 inserted=30000 skipped=0 sample_ids=[..., ..., ...]
# rows=30000
~~~

## Data mapping (source JSON -> DB)
- `status` → status (TEXT)  
- `acceptance_date` / `rejection_date` / `date_added` → date_added (DATE)  
- `start_term` → term (TEXT)  
- `degree` → degree (TEXT)  
- `program` + `university` → program (TEXT; `"University - Program"` if both)  
- `comments` → comments (TEXT)  
- `entry_url` → url (TEXT) and **p_id** (digits after `/result/<id>`)  
- `US/International` → us_or_international (TEXT)  
- `GPA`, `GRE`, `GRE V`, `GRE AW` → gpa/gre/gre_v/gre_aw (REAL)  
- `llm-generated-program` → llm_generated_program (TEXT)  
- `llm-generated-university` → llm_generated_university (TEXT)

Records without a stable id (no `/result/<id>` and no `p_id`) are **skipped** and reported.

## Issues encountered (and fixes)
- **pip resolver conflicts on Windows / Python 3.13** → Pin to `psycopg[binary]==3.2.10` and `psycopg_pool 3.2.x` in `module_3/requirements.txt`; install inside project `.venv`.
- **PyCharm using the wrong interpreter** → Attach project `.venv\Scripts\python.exe` in *Settings → Project → Python Interpreter* and in the run configuration.
- **Pool worker threads not stopping** → Explicit `close_pool()` at program exit; `atexit` safeguard in the pool module.
- **File path mistakes** → Use a tracked `module_3/data/` folder; run with a real path (no placeholders).
- **Config handling** → `DATABASE_URL` env var (if set) overrides `config.ini`; otherwise `config.ini` is used (password may be blank locally).
- **Python 3.13 compatibility** → Use psycopg 3.2.x (3.1.x binaries aren’t available for 3.13).
- **JSON loading speed (30k rows)** → Batch inserts (default `--batch 2000`) in a single transaction.
- **Data formatting quirks** → Robust date parsing (supports “September 06, 2025”), numeric coercion for GPA/GRE, and key normalization (spaces/hyphens/slashes).

## PEP-8 & docstrings
- Modules and functions include short docstrings (purpose, key args/returns).
- Imports at top, clear names, minimal prints, and fast execution per assignment.
