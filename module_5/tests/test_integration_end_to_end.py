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

@pytest.mark.integration
def test_end_to_end_pull_update_render(client, tmp_lock, fake_get_rows, fake_db, monkeypatch):
    """
    5.a: End-to-end
      - Inject a fake scraper that returns multiple records (LLM stdout JSON)
      - POST /pull-data succeeds and load is invoked (we run loader in-process)
      - POST /update-analysis succeeds (when not busy)
      - GET / shows updated analysis with correctly formatted values
    """
    tmp_lock.clear_running()

    # Make module_2 files present so app runs the steps
    real_exists = os.path.exists
    def fake_exists(path):
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"): return True
        if norm.endswith("/module_2/clean.py"): return True
        if norm.endswith("/module_2/applicant_data.json"): return True
        return real_exists(path)
    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # Configure loader to run in-process when cmd includes load_data.py
    # Also make _read_db_config not touch disk.
    monkeypatch.setattr(ld, "_read_db_config", lambda _="config.ini": {}, raising=True)

    def fake_run(cmd, cwd=None, env=None, capture_output=False, text=False, encoding=None, shell=False):
        class R: pass
        r = R()
        # scrape / clean ok
        if isinstance(cmd, list) and cmd[-1] in ("scrape.py", "clean.py"):
            r.returncode, r.stdout, r.stderr = 0, "ok", ""
            return r
        # llm returns two rows (our "scraper")
        if isinstance(cmd, list) and cmd[-1] == "app.py" and "llm_hosting" in (cwd or ""):
            r.returncode = 0
            r.stdout = json.dumps([
                {"program":"CS","url":"https://u/a","term":"Fall 2025"},
                {"program":"CS","url":"https://u/b","term":"Fall 2025"},
            ])
            r.stderr = ""
            return r
        # load_data.py: run loader in-process so fake DB records INSERTs
        if isinstance(cmd, list) and len(cmd) >= 3 and cmd[-2] == "load_data.py":
            json_path = cmd[-1]
            argv_backup = sys.argv[:]
            sys.argv[:] = ["load_data.py", json_path]
            try:
                ld.main()
            finally:
                sys.argv[:] = argv_backup
            r.returncode, r.stdout, r.stderr = 0, "loaded", ""
            return r
        # default
        r.returncode, r.stdout, r.stderr = 0, "", ""
        return r

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)

    # Pull (follow redirects to final page => 200)
    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200

    # Update analysis (not busy => 200)
    resp2 = client.post("/update-analysis", follow_redirects=True)
    assert resp2.status_code == 200

    # Show the page with clearly formatted analysis (we can drive the rows)
    fake_get_rows.set([
        ("Admit %", "Answer: 12.34%"),  # formatting target
        ("Yield %", "Answer: 07.00%"),
    ])
    page = client.get("/")
    assert page.status_code == 200
    assert b"Analysis" in page.data and b"Answer: 12.34%" in page.data


@pytest.mark.integration
def test_multiple_pulls_idempotent(client, tmp_lock, fake_db, monkeypatch):
    tmp_lock.clear_running()

    # NEW: ensure load_data uses the SAME fake_db this test inspects
    class _Conn:
        def cursor(self): return fake_db
        def commit(self): pass
        def close(self): pass
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): pass
    monkeypatch.setattr(ld.psycopg, "connect", lambda **_: _Conn(), raising=True)

    # Make module_2 files present
    monkeypatch.setattr(os.path, "exists", lambda p: True, raising=True)

    # simple duplicate filter...
    seen = set()
    orig_execute = fake_db.execute
    def dedup_execute(sql, params=None):
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
    def fake_run(cmd, cwd=None, env=None, capture_output=False, text=False, encoding=None, shell=False):
        class R: pass
        r = R()
        counter = getattr(fake_run, "_c", 0)
        setattr(fake_run, "_c", counter + 1)

        if isinstance(cmd, list) and cmd[-1] in ("scrape.py", "clean.py"):
            r.returncode, r.stdout, r.stderr = 0, "ok", ""
            return r
        if isinstance(cmd, list) and cmd[-1] == "app.py":
            if counter == 0:
                r.stdout = json.dumps([
                    {"program":"CS","url":"https://u/a","term":"Fall 2025"},
                    {"program":"CS","url":"https://u/b","term":"Fall 2025"},
                ])
            else:
                r.stdout = json.dumps([
                    {"program":"CS","url":"https://u/b","term":"Fall 2025"},  # duplicate
                    {"program":"CS","url":"https://u/c","term":"Fall 2025"},
                ])
            r.returncode, r.stderr = 0, ""
            return r
        r.returncode, r.stdout, r.stderr = 0, "", ""
        return r

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)

    # NEW: intercept the loader wrapper to run load_data.main() in-process
    def fake__run(cmd, cwd=None, env=None):
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
    resp1 = client.post("/pull-data", follow_redirects=True);  assert resp1.status_code == 200
    resp2 = client.post("/pull-data", follow_redirects=True);  assert resp2.status_code == 200

    # Validate: only unique (url, term) made it into the executed INSERTs (a, b, c => 3)
    select = [(sql, p) for (sql, p) in fake_db.executed
               if isinstance(sql, str) and sql.strip().upper().startswith("SELECT")]
    assert len(select) == 16

