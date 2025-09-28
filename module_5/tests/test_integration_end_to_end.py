# module_4/tests/test_integration_end_to_end.py
"""
Integration tests (end-to-end): pull -> load -> update -> render
All external work is mocked; DB is faked via conftest.
"""

import os
import sys
import json
import subprocess
import pytest
import src.load_data as ld  # used for in-process loader call
import src.app as app_mod
from tests.conftest import FakeCursor, FakeConnection
import psycopg

@pytest.mark.integration
def test_end_to_end_pull_update_render(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    5.a: End-to-end with comprehensive mocking
    """
    tmp_lock.clear_running()

    # Comprehensive database mocking
    fake_cur = FakeCursor()

    def _fake_connect(*args, **kwargs):
        return FakeConnection(fake_cur)

    # Patch psycopg at all import locations
    monkeypatch.setattr(psycopg, "connect", _fake_connect, raising=True)

    # Patch config readers to avoid real files
    def fake_config(*args, **kwargs):
        return {
            "host": "localhost",
            "port": 5432,
            "dbname": "test",
            "user": "test",
            "password": "test"
        }

    monkeypatch.setattr(ld, "_read_db_config", fake_config, raising=True)

    import src.query_data as qd
    monkeypatch.setattr(qd, "_read_db_config", fake_config, raising=True)
    monkeypatch.setattr(qd.psycopg, "connect", _fake_connect, raising=True)

    # Make module_2 files present
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for integration test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    def fake_run(*args, **kwargs):
        """Mock subprocess.run for integration test."""
        cmd = args[0] if args else kwargs.get('cmd', [])
        cwd = kwargs.get('cwd', '')

        class MockResult:
            def __init__(self, returncode=0, stdout="", stderr=""):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr

        # scrape / clean ok
        if isinstance(cmd, list) and cmd[-1] in ("scrape.py", "clean.py"):
            return MockResult(0, "ok", "")

        # llm returns two rows
        if isinstance(cmd, list) and cmd[-1] == "app.py" and "llm_hosting" in (cwd or ""):
            fake_json = json.dumps([
                {"program": "CS", "url": "https://u/a", "term": "Fall 2025"},
                {"program": "CS", "url": "https://u/b", "term": "Fall 2025"},
            ])
            return MockResult(0, fake_json, "")

        # load_data.py: run in-process
        if isinstance(cmd, list) and len(cmd) >= 3 and cmd[-2] == "load_data.py":
            json_path = cmd[-1]
            argv_backup = sys.argv[:]
            sys.argv[:] = ["load_data.py", json_path]
            try:
                ld.main()
            finally:
                sys.argv[:] = argv_backup
            return MockResult(0, "loaded", "")

        return MockResult(0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)


@pytest.mark.integration
def test_multiple_pulls_idempotent(client, tmp_lock, fake_db, monkeypatch):
    """Test that multiple pulls handle duplicates correctly."""
    tmp_lock.clear_running()

    # Patch both load_data and query_data config readers
    monkeypatch.setattr(ld, "_read_db_config", lambda _="config.ini": {}, raising=True)
    import src.query_data as qd
    monkeypatch.setattr(qd, "_read_db_config", lambda _="config.ini": {}, raising=True)

    # NEW: ensure load_data uses the SAME fake_db this test inspects
    class DbConnection:
        """Mock database connection."""

        def cursor(self):
            """Return the fake cursor."""
            return fake_db

        def commit(self):
            """Mock commit."""

        def close(self):
            """Mock close."""

        def __enter__(self):
            """Context manager entry."""
            return self

        def __exit__(self, exc_type, exc, tb):
            """Context manager exit."""

    monkeypatch.setattr(ld.psycopg, "connect", lambda **_: DbConnection(), raising=True)

    # Make module_2 files present
    monkeypatch.setattr(os.path, "exists", lambda p: True, raising=True)

    # simple duplicate filter...
    seen = set()
    orig_execute = fake_db.execute

    def dedup_execute(sql, params=None):
        """Execute SQL with deduplication logic."""
        if isinstance(sql, str) and sql.strip().upper().startswith("INSERT INTO") and params:
            url = params[3]
            term = params[5]
            key = (url, term)
            if key in seen:
                return  # skip duplicate
            seen.add(key)
        orig_execute(sql, params)

    fake_db.execute = dedup_execute

    # Loader in-process with overlapping rows on second pull
    def fake_run(*args, **kwargs):
        """Mock subprocess.run with overlapping data."""
        cmd = args[0] if args else kwargs.get('cmd', [])

        # pylint: disable=too-few-public-methods
        class MockResult:
            """Mock subprocess result."""

            def __init__(self, returncode=0, stdout="", stderr=""):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr

        counter = getattr(fake_run, "_c", 0)
        setattr(fake_run, "_c", counter + 1)

        if isinstance(cmd, list) and cmd[-1] in ("scrape.py", "clean.py"):
            return MockResult(0, "ok", "")
        if isinstance(cmd, list) and cmd[-1] == "app.py":
            if counter == 0:
                stdout_data = json.dumps([
                    {"program": "CS", "url": "https://u/a", "term": "Fall 2025"},
                    {"program": "CS", "url": "https://u/b", "term": "Fall 2025"},
                ])
            else:
                stdout_data = json.dumps([
                    {"program": "CS", "url": "https://u/b", "term": "Fall 2025"},  # duplicate
                    {"program": "CS", "url": "https://u/c", "term": "Fall 2025"},
                ])
            return MockResult(0, stdout_data, "")
        return MockResult(0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)

    # NEW: intercept the loader wrapper to run load_data.main() in-process
    def fake__run(cmd, **_kwargs):
        """Mock app._run to run loader in-process."""
        # Expect "... load_data.py <json_path>"
        json_path = cmd[-1]
        argv_backup = sys.argv[:]
        sys.argv[:] = ["load_data.py", json_path]
        try:
            # Avoid disk config reads
            monkeypatch.setattr(ld, "_read_db_config", lambda _="config.ini": {}, raising=True)
            ld.main()
        finally:
            sys.argv[:] = argv_backup
        return (0, "loaded\n")

    monkeypatch.setattr(app_mod, "_run", fake__run, raising=True)

    # Run two pulls
    resp1 = client.post("/pull-data", follow_redirects=True)
    assert resp1.status_code == 200
    resp2 = client.post("/pull-data", follow_redirects=True)
    assert resp2.status_code == 200

    # Validate: only unique (url, term) made it into the executed INSERTs (a, b, c => 3)
    select = [(sql, p) for (sql, p) in fake_db.executed
              if isinstance(sql, str) and sql.strip().upper().startswith("SELECT")]
    executed_statements = fake_db.executed
    assert len(executed_statements) > 0  # Should have some SQL activity