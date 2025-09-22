API Reference
=============

This section provides API documentation for the key modules in the Grad Cafe Analytics system.

Flask Application (flask_app.py)
---------------------------------

Main Flask application module providing web interface and ETL orchestration.

**Key Functions:**

``create_app(config_overrides=None)``
    Flask application factory function.

    **Parameters:**
    - ``config_overrides`` (dict, optional): Configuration overrides for testing

    **Returns:**
    - ``Flask``: Configured Flask application instance

**Routes:**

- ``GET /`` - Redirect to analysis page
- ``GET /analysis`` - Main analysis dashboard
- ``POST /pull-data`` - Trigger ETL pipeline
- ``POST /update-analysis`` - Refresh analysis calculations
- ``GET /health`` - Health check endpoint

Data Loading (load_data.py)
---------------------------

Command-line interface for database operations and data loading.

**Key Functions:**

``main()``
    Primary entry point for data loading operations.

``parse_args()``
    Parse command-line arguments for the loader.

    **Returns:**
    - ``argparse.Namespace``: Parsed command-line arguments

Query Functions (query_data.py)
-------------------------------

Database query functions for analysis calculations.

**Key Functions:**

``q1_count_fall_2025()``
    Count Fall 2025 applicants.

    **Returns:**
    - ``int``: Number of Fall 2025 entries

``q2_pct_international()``
    Calculate percentage of international students.

    **Returns:**
    - ``float``: Percentage of international students

``q3_avgs()``
    Calculate average GPA, GRE scores.

    **Returns:**
    - ``tuple``: (avg_gpa, avg_gre, avg_gre_v, avg_gre_aw)

``q4_avg_gpa_american_fall2025()``
    Average GPA for American students in Fall 2025.

    **Returns:**
    - ``float`` or ``None``: Average GPA

``q5_pct_accept_fall2025()``
    Acceptance rate for Fall 2025.

    **Returns:**
    - ``float``: Percentage accepted

``q6_avg_gpa_accept_fall2025()``
    Average GPA for accepted Fall 2025 applicants.

    **Returns:**
    - ``float`` or ``None``: Average GPA

``q7_count_jhu_masters_cs()``
    Count JHU Computer Science Masters applications.

    **Returns:**
    - ``int``: Number of matching entries

``q8_count_2025_georgetown_phd_cs_accept()``
    Count Georgetown CS PhD acceptances in 2025.

    **Returns:**
    - ``int``: Number of matching entries

``q9_top5_accept_unis_2025()``
    Top 5 universities by acceptance count in 2025.

    **Returns:**
    - ``list``: List of (university, count) tuples

``q10_avg_gre_by_status_year(year)``
    Average GRE scores by status for a specific year.

    **Parameters:**
    - ``year`` (int): Calendar year to analyze

    **Returns:**
    - ``list``: List of (status, avg_gre) tuples

Configuration Management (config.py)
------------------------------------

Database configuration resolution and URL management.

**Key Functions:**

``database_url()``
    Get the database URL from environment or config files.

    **Returns:**
    - ``str``: PostgreSQL connection URL

``database_url_and_source()``
    Get database URL and its configuration source.

    **Returns:**
    - ``tuple``: (url, source_description)

``masked_url(url)``
    Mask password in URL for safe logging.

    **Parameters:**
    - ``url`` (str): Database URL with potential password

    **Returns:**
    - ``str``: URL with password redacted

Database Check Utilities (db_check.py)
--------------------------------------

**Key Functions:**

``main()``
    Print database connection status and basic statistics.

    Displays:
    - Configuration source and masked URL
    - Current database, user, and schema
    - Whether applicants table exists
    - Row count if table exists

Data Access Layer
-----------------

Connection Pool (dal/pool.py)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Key Functions:**

``get_conn()``
    Get a pooled database connection.

    **Returns:**
    - ``psycopg.Connection``: Database connection (context manager)

``close_pool()``
    Close the connection pool and stop worker threads.

Schema Management (dal/schema.py)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Key Functions:**

``init_schema()``
    Create or verify the database schema.

``count_rows()``
    Count total rows in the applicants table.

    **Returns:**
    - ``int``: Number of rows

Data Loader (dal/loader.py)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Key Functions:**

``load_json(path, batch=2000)``
    Load JSON data into the database.

    **Parameters:**
    - ``path`` (str): Path to JSON file
    - ``batch`` (int): Batch size for inserts

    **Returns:**
    - ``tuple``: (total_records, inserted, skipped, issues, report_path)

``first_ids(path, k=3)``
    Get first k stable IDs from JSON file.

    **Parameters:**
    - ``path`` (str): Path to JSON file
    - ``k`` (int): Number of IDs to return

    **Returns:**
    - ``list``: List of integer IDs

ETL Modules (module_2_ref)
--------------------------

Data Scraping (scrape.py)
~~~~~~~~~~~~~~~~~~~~~~~~~

**Key Functions:**

``main()``
    Primary scraping entry point.

    Features:
    - Incremental scraping with resume capability
    - Robots.txt compliance
    - Polite delays between requests
    - Detail page enrichment

Data Cleaning (clean.py)
~~~~~~~~~~~~~~~~~~~~~~~~

**Key Functions:**

``main()``
    Clean and normalize scraped data.

    Operations:
    - Fill missing fields
    - Strip HTML tags from comments
    - Standardize whitespace

LLM Processing (run.py)
~~~~~~~~~~~~~~~~~~~~~~~

**Key Functions:**

``main()``
    Run LLM standardization on new data only.

    Features:
    - Incremental processing
    - Support for separate Python interpreter
    - University and program name standardization

Flask Routes Reference
----------------------

**GET /**
  Main analysis page redirect.

**GET /analysis**
  Returns the analysis dashboard with current statistics and interactive controls.

  **Response**: HTML page with:
  - Analysis results table
  - "Pull Data" button (``data-testid="pull-data-btn"``)
  - "Update Analysis" button (``data-testid="update-analysis-btn"``)

**POST /pull-data**
  Triggers the complete ETL pipeline: scrape → clean → LLM → load.

  **Returns**:
  - ``200 OK`` with ``{"ok": true, "row_count": N}`` on success
  - ``409 Conflict`` with ``{"busy": true}`` if operation already in progress
  - ``500 Internal Server Error`` with ``{"ok": false, "step": "STEP_NAME"}`` on failure

**POST /update-analysis**
  Refreshes analysis calculations (placeholder for future functionality).

  **Returns**:
  - ``200 OK`` with ``{"ok": true}`` when not busy
  - ``409 Conflict`` with ``{"busy": true}`` if pull operation in progress

**GET /health**
  Health check endpoint.

  **Returns**: ``{"ok": true}``

Database Schema
---------------

The system uses a single PostgreSQL table with the following structure:

.. code-block:: sql

    CREATE TABLE public.applicants (
        p_id INTEGER PRIMARY KEY,
        program TEXT,
        comments TEXT,
        date_added DATE,
        url TEXT,
        status TEXT,
        term TEXT,
        us_or_international TEXT,
        gpa REAL,
        gre REAL,
        gre_v REAL,
        gre_aw REAL,
        degree TEXT,
        llm_generated_program TEXT,
        llm_generated_university TEXT
    );

**Field Descriptions:**

- ``p_id``: Unique identifier extracted from entry URLs
- ``program``: Combined "University - Program" string
- ``comments``: User comments/notes from the application
- ``date_added``: Date the entry was added to GradCafe
- ``url``: Source URL for the entry
- ``status``: Application status (Accepted, Rejected, Wait listed, etc.)
- ``term``: Application term (Fall 2025, Spring 2024, etc.)
- ``us_or_international``: Student origin classification
- ``gpa``: Grade Point Average (filtered ≤ 5.0)
- ``gre``: GRE total score (filtered ≤ 400)
- ``gre_v``: GRE Verbal score
- ``gre_aw``: GRE Analytical Writing score
- ``degree``: Degree type sought
- ``llm_generated_program``: LLM-standardized program name
- ``llm_generated_university``: LLM-standardized university name