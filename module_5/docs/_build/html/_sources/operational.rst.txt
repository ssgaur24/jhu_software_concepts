Operational Notes
=================

This page covers operational aspects of the Grad Cafe Analytics system including busy-state policies, idempotency strategies, and troubleshooting guidance.

Busy-State Policy
-----------------

The system implements busy-state management to prevent concurrent ETL operations:

**Lock File Mechanism**
  - Lock file: ``module_4/artifacts/pull.lock``
  - Created when ``/pull-data`` starts
  - Removed when operation completes (success or failure)
  - Blocks subsequent ``/pull-data`` and ``/update-analysis`` requests

**HTTP Response Codes**
  - ``200 OK``: Operation completed successfully
  - ``409 Conflict``: System busy, operation rejected
  - ``500 Internal Server Error``: Operation failed

**Busy-State Behavior**::

    # When system is idle
    POST /pull-data    → 200 {"ok": true, "row_count": N}
    POST /update-analysis → 200 {"ok": true}

    # When pull operation is running
    POST /pull-data    → 409 {"busy": true}
    POST /update-analysis → 409 {"busy": true}

Idempotency Strategy
--------------------

**Database Level**
  Primary key constraint on ``p_id`` ensures duplicate prevention::

    INSERT INTO public.applicants (...) VALUES (...)
    ON CONFLICT (p_id) DO NOTHING;

**ETL Pipeline Idempotency**
  - Scraper: Resumes from existing ``applicant_data.json``
  - LLM: Only processes new records not in database
  - Loader: Uses ``ON CONFLICT DO NOTHING`` for safe re-runs

**Multiple Pulls**
  Running ``/pull-data`` multiple times with overlapping data maintains consistency:

  1. First pull: Inserts new records
  2. Second pull: Skips existing records (by p_id)
  3. Result: No duplicates, consistent row counts

Uniqueness Keys
---------------

**Primary Identifier: p_id**
  - Extracted from entry URLs: ``/result/12345`` → ``p_id = 12345``
  - Fallback: ``p_id`` field from JSON data
  - Used for idempotency and incremental processing

**URL Pattern Matching**::

    /result/(\d+)  # Regex to extract numeric ID

    Examples:
    https://gradcafe.com/result/12345 → p_id = 12345
    /result/67890                    → p_id = 67890

**Data Deduplication**
  Records without valid ``p_id`` are skipped during loading to maintain data quality.

Troubleshooting
---------------

Common Local Issues
~~~~~~~~~~~~~~~~~~~

**Database Connection Errors**::

    Error: could not connect to server

    Solutions:
    1. Verify PostgreSQL is running
    2. Check DATABASE_URL format
    3. Validate credentials and database exists
    4. Test connection: python src/db_check.py

**Import Errors**::

    ModuleNotFoundError: No module named 'src'

    Solutions:
    1. Run from module_4/ directory
    2. Ensure src/ folder structure is correct
    3. Check Python path in tests

**Test Failures**::

    pytest collection errors

    Solutions:
    1. Verify all tests have pytest markers
    2. Check pytest.ini configuration
    3. Run with -v flag for detailed output

**Coverage Below 100%**::

    FAILED: coverage 95% < 100%

    Solutions:
    1. Run: pytest --cov=src --cov-report=term-missing
    2. Add tests for missing lines
    3. Consider excluding test-only utility functions

Common CI Issues
~~~~~~~~~~~~~~~~

**GitHub Actions Failures**::

    PostgreSQL service not available

    Solutions:
    1. Verify services configuration in workflow
    2. Check PostgreSQL port mapping
    3. Ensure DATABASE_URL matches service config

**Dependency Installation**::

    pip install failed

    Solutions:
    1. Check requirements.txt format
    2. Verify Python version compatibility
    3. Use specific package versions

**Test Timeouts**::

    Tests exceed time limit

    Solutions:
    1. Use mocked external dependencies
    2. Avoid actual web scraping in tests
    3. Implement deterministic test fixtures

Performance Considerations
--------------------------

**ETL Pipeline**
  - Scraper: Implements polite delays (50-80ms between requests)
  - Batch size: 2000 records per database transaction
  - Incremental processing: Only new data processed by LLM

**Database Operations**
  - Connection pooling: min=0, max=8 connections
  - Bulk inserts: Uses executemany() for efficiency
  - Query optimization: Indexed p_id primary key

**Memory Usage**
  - JSON loading: Entire dataset loaded into memory
  - Consider streaming for very large datasets (>100k records)
  - Connection pool cleanup prevents memory leaks

Monitoring and Logging
----------------------

**Application Logs**
  ETL pipeline outputs detailed progress information::

    [PULL DEBUG] step=scrape rc=0
    [PULL DEBUG] step=load rc=0
    [LLM] raw=1500 known_ids=1200 new_rows=300

**Health Check Endpoint**::

    GET /health → {"ok": true}

**Database Status Check**::

    python src/db_check.py

    Output:
    config_source=env:DATABASE_URL
    url=postgresql://user@localhost:5432/db
    db=gradcafe user=myuser schema=public
    applicants_exists=True rows=15234

**Lock File Status**
  Check for stuck operations::

    ls -la module_4/artifacts/pull.lock

    # If exists and old, remove manually:
    rm module_4/artifacts/pull.lock