# Module 3 — SQL Data Analysis

## Overview
- Load cleaned applicant data (from Module-2) into PostgreSQL using **psycopg v3**.
- Store records in one table: `applicants` with the required columns.
- Provide a small CLI to create the table, load data, and print a tiny verification (row count and first few).

## Environment
- Python 3.10+
- PostgreSQL running locally and reachable via `DATABASE_URL`.

## Setup
1. Create and activate a virtual environment, then install dependencies:
   ```bash
   python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
   python -m pip install -r module_3/requirements.txt
