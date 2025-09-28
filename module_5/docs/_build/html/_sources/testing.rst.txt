Testing Guide
=============

The Grad Cafe Analytics system includes a comprehensive test suite covering all major functionality. This guide explains how to run tests, understand test categories, and work with test fixtures.

Test Categories and Markers
----------------------------

All tests are organized using pytest markers. Each test must be marked with one or more of the following:

**@pytest.mark.web**
  Tests for Flask routes, page rendering, and HTML structure validation.

**@pytest.mark.buttons** 
  Tests for interactive button endpoints and busy-state behavior.

**@pytest.mark.analysis**
  Tests for analysis output formatting, labels, and percentage precision.

**@pytest.mark.db**
  Tests for database schema operations, inserts, and data integrity.

**@pytest.mark.integration**
  End-to-end workflow tests covering the complete data pipeline.

Running Tests
-------------

**Run All Tests**::

    pytest

**Run Specific Test Categories**::

    # Web layer tests only
    pytest -m web
    
    # Button functionality tests
    pytest -m buttons
    
    # Analysis formatting tests  
    pytest -m analysis
    
    # Database tests
    pytest -m db
    
    # Integration tests
    pytest -m integration
    
    # Combined categories
    pytest -m "web or buttons"

**Run with Coverage**::

    # Standard coverage report
    pytest --cov=src --cov-report=term-missing
    
    # HTML coverage report
    pytest --cov=src --cov-report=html
    
    # Fail if coverage below 100%
    pytest --cov=src --cov-fail-under=100

**Run Specific Test Files**::

    pytest tests/test_flask_page.py
    pytest tests/test_buttons.py
    pytest tests/test_analysis_format.py
    pytest tests/test_db_insert.py
    pytest tests/test_integration_end_to_end.py

Test Fixtures and Test Doubles
-------------------------------

**Flask Test Client**

The test suite uses a Flask test client for web testing::

    @pytest.fixture()
    def app():
        return create_app({"TESTING": True})

    @pytest.fixture()
    def client(app):
        return app.test_client()

**Database Test Fixtures**
  
The test suite uses fake database connections to avoid requiring a real PostgreSQL instance during testing::

    class _FakeConn:
        def __init__(self, store):
            self.store = store
        def cursor(self):
            return _FakeCursor(self.store)

**Mocked External Dependencies**

Tests mock external services like scrapers and subprocess calls::

    def fake_run(*args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="row_count=5", stderr="")
    
    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

Expected Selectors
------------------

The HTML templates must include these stable selectors for UI testing:

**Pull Data Button**::

    <button data-testid="pull-data-btn">Pull Data</button>

**Update Analysis Button**::

    <button data-testid="update-analysis-btn">Update Analysis</button>

**Analysis Items**::

    <div class="answer-item">
        Answer: [analysis result]
    </div>

Test Coverage Requirements
--------------------------

The test suite must achieve 100% code coverage across all modules in ``src/``. Key coverage areas:

- **Flask routes**: All endpoints (GET /, GET /analysis, POST /pull-data, POST /update-analysis, GET /health)
- **Database operations**: Schema creation, data insertion, query functions
- **ETL pipeline**: Mocked scraper, cleaner, and loader functions
- **Configuration**: Database URL resolution and connection pooling
- **Error handling**: Failed subprocess calls, database errors, busy-state conflicts

Running Coverage Analysis
-------------------------

Generate detailed coverage reports to identify uncovered code::

    # Terminal report with missing lines
    pytest --cov=src --cov-report=term-missing
    
    # HTML report (opens coverage/index.html)
    pytest --cov=src --cov-report=html
    
    # Enforce 100% coverage (fails if below threshold)
    pytest --cov=src --cov-fail-under=100

**Coverage Output Example**::

    Name                    Stmts   Miss  Cover   Missing
    -----------------------------------------------------
    src/config.py              45      0   100%
    src/db_check.py             25      0   100%
    src/flask_app.py           120      0   100%
    src/load_data.py            35      0   100%
    src/query_data.py           95      0   100%
    src/dal/loader.py           85      0   100%
    src/dal/pool.py             20      0   100%
    src/dal/schema.py           30      0   100%
    -----------------------------------------------------
    TOTAL                      455      0   100%

Troubleshooting Tests
---------------------

**Common Issues:**

1. **Import Errors**: Ensure ``src/`` is in Python path
2. **Database Connection Errors**: Tests should use fake connections
3. **Missing Test Markers**: All tests must be marked with pytest markers
4. **Coverage Below 100%**: Add tests for uncovered code paths
5. **Flaky Tests**: Avoid sleep() calls; use deterministic state checks