# Module 4 - Grad Café Analytics Test Suite

A comprehensive test suite for the Grad Café analytics application built in Module 3, featuring Flask testing, database integration, and automated CI/CD.

## Features

- **Flask Web Testing**: Validates page rendering, button functionality, and busy-state behavior
- **Database Integration Testing**: Verifies PostgreSQL writes, idempotency, and schema compliance  
- **Analysis Output Testing**: Ensures proper formatting and two-decimal percentage display
- **End-to-End Testing**: Complete pull → update → render workflows
- **100% Test Coverage**: Comprehensive coverage across all modules
- **GitHub Actions CI**: Automated testing with PostgreSQL service

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Git

### Installation

```bash
git clone https://github.com/ssgaur24/jhu_software_concepts
cd module_4
pip install -r requirements.txt
```

### Environment Setup

Set the database URL:
```bash
export DATABASE_URL="postgresql://user@localhost:5432/dbname"
```

Or create `config.local.ini`:
```ini
[db]
host = localhost
port = 5432
database = testdb
user = testuser
password = 
```

### Running Tests

Run all tests:
```bash
pytest -m "web or buttons or analysis or db or integration"
```

Run specific test categories:
```bash
pytest -m web          # Flask page tests
pytest -m buttons      # Button behavior tests  
pytest -m analysis     # Output formatting tests
pytest -m db           # Database tests
pytest -m integration  # End-to-end tests
```

### Test Coverage

Generate coverage report:
```bash
pytest --cov=src --cov-report=term-missing
```

## Architecture

### Web Layer (Flask)
- **Routes**: `/`, `/analysis`, `/health`, `/pull-data`, `/update-analysis`
- **Templates**: HTML rendering with stable test selectors
- **Busy State**: Lock file and config flag prevent concurrent operations

### Data Layer (ETL + Database)
- **ETL Pipeline**: scrape → clean → LLM → load
- **Database**: PostgreSQL with Module 3 schema
- **Queries**: Analysis functions for statistics and reporting

### Test Layer
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end workflow verification
- **Mocking**: Database and subprocess call isolation
- **Fixtures**: Reusable test data and setup

## Test Selectors

The application provides stable test selectors:
- `data-testid="pull-data-btn"` - Pull Data button
- `data-testid="update-analysis-btn"` - Update Analysis button
- `.answer-item` - Analysis answer containers

## CI/CD

GitHub Actions automatically:
1. Starts PostgreSQL service
2. Installs dependencies
3. Runs full test suite
4. Reports coverage results

## Documentation

Full API documentation available at: [Read the Docs Link]

## License

This project is part of the Module 4 assignment for academic purposes.