Architecture
============

System Architecture Overview
-----------------------------

The Grad Cafe Analytics system follows a layered architecture with three primary components:

.. code-block:: text

    ┌─────────────────────────────────────────┐
    │              Web Layer                  │
    │        (Flask Application)              │
    ├─────────────────────────────────────────┤
    │              ETL Layer                  │
    │     (Scrape → Clean → LLM → Load)       │
    ├─────────────────────────────────────────┤
    │            Database Layer               │
    │         (PostgreSQL + DAL)              │
    └─────────────────────────────────────────┘

Web Layer (Flask)
-----------------

**Responsibilities:**

- Serves the analysis page with interactive controls
- Handles user requests for data pulls and analysis updates
- Manages busy-state behavior during long-running operations
- Renders analysis results with proper formatting

**Key Components:**

- ``flask_app.py``: Main Flask application factory
- ``templates/``: HTML templates for web pages
- Route handlers for ``/``, ``/analysis``, ``/pull-data``, ``/update-analysis``

**Design Patterns:**

- Application factory pattern for testability
- Busy-state management with lock files
- JSON API responses for AJAX interactions

ETL Layer (Extract, Transform, Load)
------------------------------------

**Data Pipeline Stages:**

1. **Scrape** (``src/module_2_ref/scrape.py``)
   - Extracts data from The Grad Cafe website
   - Handles incremental updates and resume capability
   - Respects robots.txt and implements polite delays

2. **Clean** (``src/module_2_ref/clean.py``)
   - Normalizes data formats and fills missing fields
   - Strips HTML tags from comments
   - Standardizes field names and data types

3. **LLM Standardization** (``src/module_2_ref/run.py``)
   - Uses local LLM to standardize university and program names
   - Runs incrementally on new data only
   - Supports separate Python interpreter for LLM host

4. **Load** (``src/load_data.py``)
   - Bulk inserts data into PostgreSQL
   - Handles data type conversions and validation
   - Implements idempotency through unique constraints

**Execution Flow:**

The ETL pipeline runs as a coordinated sequence triggered by the ``/pull-data`` endpoint:

.. code-block:: python

    # Pseudo-code flow
    scrape() → clean() → llm_standardize() → load_to_db()

Database Layer
--------------

**Schema Design:**

- Single table ``public.applicants`` with comprehensive fields
- Primary key on ``p_id`` (extracted from entry URLs)
- Support for both raw and LLM-standardized data

**Data Access Layer (DAL):**

- Connection pooling via ``src/dal/pool.py``
- Schema management in ``src/dal/schema.py``
- Data loading utilities in ``src/dal/loader.py``

**Query Layer:**

- Analysis functions in ``src/query_data.py``
- Parameterized queries for security
- Statistical aggregations with proper filtering

**Key Tables:**

applicants
~~~~~~~~~~

- ``p_id`` (INTEGER PRIMARY KEY): Unique identifier
- ``program, university``: Institution and program information
- ``status, term``: Application status and term
- ``gpa, gre, gre_v, gre_aw``: Academic metrics
- ``llm_generated_university, llm_generated_program``: Standardized names
- ``date_added, comments, url``: Metadata and references

Configuration Management
-------------------------

**Configuration Resolution Order:**

1. Environment variable ``DATABASE_URL``
2. ``module_4/config.local.ini`` (local development)
3. ``module_4/config.ini`` (default configuration)

**Security Considerations:**

- No hardcoded credentials
- Environment-based configuration for production
- Connection pooling with automatic cleanup
- Parameterized queries to prevent SQL injection

Testing Architecture
--------------------

**Test Categories:**

- **Web tests**: Flask route and page rendering validation
- **Button tests**: Interactive endpoint behavior and busy-state gating
- **Analysis tests**: Output formatting and percentage precision
- **Database tests**: Schema operations and data integrity
- **Integration tests**: End-to-end workflow validation

**Test Isolation:**

- Fake database connections for unit tests
- Mocked external dependencies (scraping, LLM)
- Test-specific configuration overrides