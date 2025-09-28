### Shraddha Shree
### JHEID: sshree1@jh.edu

--------------------------------------------------------------------------

# Module 5 - Grad Café Analytics Test Suite and Documentation

A comprehensive test suite and documentation package for the Grad Café analytics application (from Module 3), featuring Flask testing, database integration, linting, security checks, dependency visualization, SQL injection testing, full test coverage, CI/CD automation, and developer documentation via Sphinx.

## Features

- Flask Web Testing: Validates route availability, page rendering, and busy-state behavior for the Analysis page
- Button Behavior Testing: Verifies correct responses for “Pull Data” and “Update Analysis” including error handling and busy gating
- Database Integration Testing: Confirms PostgreSQL inserts, uniqueness, idempotency, and schema compliance
- Analysis Output Testing: Ensures labeled answers and consistent percentage formatting with exactly two decimals
- End-to-End Testing: Simulates the entire workflow (pull → update → render) with mocked scraper/loader for repeatable results
- 100% Test Coverage: Required coverage across all code modules using pytest-cov
- SQL Injection Protection Testing: Verifies query safety and guards against injection attacks
- CI/CD with GitHub Actions: Automated test execution with PostgreSQL service and workflow proof
- Sphinx Documentation: Published docs include overview, setup instructions, architecture notes, API reference, testing guide, and operational notes
- PyLint Linting: Code quality and style validation, with proof in screenshots
- Snyk Testing: Dependency vulnerability scanning for security, with proof in screenshots
- Dependency Graph: pydeps + Graphviz visualization of module dependencies, stored in artifacts and screenshots

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Git
- Graphviz (system dependency required for pydeps)

### Installation

git clone https://github.com/ssgaur24/jhu_software_concepts
cd module_5
pip install -r requirements.txt

### Environment Setup

Create config.ini:
[db]
host = localhost
port = 5432
database = testdb
user = testuser
password = 

### Running Tests

Run the entire marked test suite:
pytest -m "web or buttons or analysis or db or integration"

Run individual test categories:
pytest -m web          # Flask route/page tests
pytest -m buttons      # Button behavior tests
pytest -m analysis     # Output formatting tests
pytest -m db           # Database integration tests
pytest -m integration  # End-to-end workflow tests

### Test Coverage

Generate coverage report:
pytest --cov=src --cov-report=term-missing --cov-fail-under=100

Coverage summary is provided in coverage_summary.txt and screenshot evidence is stored in the screenshots folder.

### Linting

Run pylint for code style validation:
pylint src/

PyLint results and screenshots are placed in the screenshots folder.

### Security Testing

Run Snyk security check:
snyk test

Snyk results and screenshots are placed in the screenshots folder.

### Dependency Graph

Generate dependency graph using pydeps:
pydeps module_5/src/app.py --noshow -T svg -o module_5/depgraph.svg

Graph output and screenshot evidence are stored in the screenshots folder.

## Architecture

### Web Layer (Flask)

- Routes: /, /analysis, /health, /pull-data, /update-analysis
- Templates: Analysis page with stable selectors for testing
- Busy-State: Prevents overlapping pulls and updates

### Data Layer (ETL + Database)

- ETL Pipeline: scrape → clean → LLM → load
- Database: PostgreSQL with Module 3 schema
- Queries: Supports summary analysis and reporting

### Test Layer

- Unit Tests: Routes, formatting, and query functions
- Integration Tests: Pull → Update → Render with mocked pipeline
- Mocking/Fixtures: Fake scraper/loader for deterministic tests

## Test Selectors

The application provides stable test selectors:
- data-testid="pull-data-btn" - Pull Data button
- data-testid="update-analysis-btn" - Update Analysis button
- .answer-item - Analysis answer containers

## CI/CD

GitHub Actions automatically:
1. Starts PostgreSQL service
2. Installs dependencies
3. Runs full test suite
4. Reports coverage results
5. Produces proof artifacts in the screenshots folder

## Screenshots

All assignment requirement evidences are placed in the screenshots folder, including:
- Coverage summary
- PyLint results
- Snyk results
- Dependency graph (pydeps)
- Dependency graph text
- GitHub Actions green run
- Sphinx build proof

## License

This project is part of the Module 5 assignment for academic purposes.
