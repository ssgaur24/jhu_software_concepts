# module_4/tests/test_flask_page.py
"""
Flask Page + Buttons tests for Module 4 (beginner-friendly).
- Verifies the analysis page loads with required UI.
- Verifies button behavior for 'Pull Data' and 'Update Analysis':
  * normal (not running) vs. 'pull in progress' (running) cases
  * failure path shows an error message
All external work is faked by conftest.py (no real DB, no real subprocess).
"""

import os
import runpy
import subprocess
import pytest
from flask import Flask


@pytest.mark.web
def test_app_routes_exist(client, fake_get_rows):
    """
    Ensure the analysis page route is served by the app.
    We assert GET "/" works (status 200) and renders a known Q/A row.
    """
    fake_get_rows.set([("Q1: Applicants?", "Answer: 10")])
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Q1: Applicants?" in resp.data
    assert b"Answer: 10" in resp.data


@pytest.mark.web
def test_get_analysis_page_loads_and_has_buttons(client, fake_get_rows):
    """
    Page must contain:
      - 'Analysis' text (header)
      - 'Pull Data' and 'Update Analysis' button labels
      - at least one 'Answer:' label in the rendered Q/A rows
    """
    fake_get_rows.set([("Q2: Admit %", "Answer: 12.34%")])
    resp = client.get("/")
    body = resp.data
    assert resp.status_code == 200
    assert b"Analysis" in body
    assert b"Pull Data" in body
    assert b"Update Analysis" in body
    assert b"Answer:" in body


@pytest.mark.buttons
def test_pull_data_runs_when_not_running(client, tmp_lock, fake_get_rows):
    """
    When no lock file exists ('pull not running'), POST /pull-data should succeed.
    We expect a redirect back to the page (commonly 302) and an 'info/success'
    status on the subsequent GET.
    """
    # Ensure "not running"
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # Trigger the pipeline (faked in conftest via fake_subprocess)
    post = client.post("/pull-data")
    assert post.status_code in (200, 302)  # many apps redirect after POST

    # After redirect, the page should show a status message and not be "running"
    page = client.get("/")
    body = page.data
    assert b"id='status'" in body
    assert b"id='pull_running'>false" in body


@pytest.mark.buttons
def test_pull_data_ignored_when_running(client, tmp_lock, fake_get_rows):
    """
    When the lock file exists ('pull in progress'), POST /pull-data should NOT run
    and should respond with a redirect or a 'busy' message as implemented.
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])

    post = client.post("/pull-data")
    # Expect a redirect (commonly back to "/") or an HTTP 409, depending on app
    assert post.status_code in (200, 302, 409)

    # The page should reflect "still running"
    page = client.get("/")
    assert b"id='pull_running'>true" in page.data

    # Clean up for isolation
    tmp_lock.clear_running()


@pytest.mark.buttons
def test_update_analysis_runs_when_not_running(client, tmp_lock, fake_get_rows):
    """
    When not running, POST /update-analysis should recompute analysis (faked)
    and return back to the page with a normal status.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    post = client.post("/update-analysis")
    assert post.status_code in (200, 302)

    page = client.get("/")
    body = page.data
    assert b"id='status'" in body
    assert b"id='pull_running'>false" in body


@pytest.mark.buttons
def test_update_analysis_ignored_when_running(client, tmp_lock, fake_get_rows):
    """
    When running, POST /update-analysis should be ignored.
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])

    post = client.post("/update-analysis")
    assert post.status_code in (200, 302, 409)

    page = client.get("/")
    assert b"id='pull_running'>true" in page.data

    tmp_lock.clear_running()


@pytest.mark.buttons
def test_pull_data_failure_reports_error(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Failure path: make the first subprocess call return a non-zero rc so the app
    reports an error. This covers the error branch in app.py.
    """
    # Fail only the first call; delegate all other calls to the real patched fake
    call_count = {"n": 0}

    def failing_run(*args, **kwargs):
        """Mock subprocess run with first call failing."""
        call_count["n"] += 1
        # On first pipeline step, simulate failure
        if call_count["n"] == 1:
            # pylint: disable=too-few-public-methods
            class FailedResult:
                """Mock failed subprocess result."""
                returncode = 1
                stdout = ""
                stderr = "boom"

            return FailedResult()

        # Otherwise, let conftest's fake_subprocess handle
        from subprocess import run as real_run
        return real_run(*args, **kwargs)

    # Apply our failure on top of the existing fake_subprocess
    monkeypatch.setattr(subprocess, "run", failing_run, raising=True)

    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    post = client.post("/pull-data")
    assert post.status_code in (200, 302)

    page = client.get("/")
    body = page.data
    # Expect an error-like status level or message (depends on app.py wording)
    # We at least assert the status container is present.
    assert b"id='status'" in body


# ---------------------------------------------------------------------------
# Additional tests focused on app.py branches (Part 1 of 2)
# ---------------------------------------------------------------------------

@pytest.mark.web
def test_is_pull_running(tmp_lock, client):
    """
    is_pull_running() reflects presence of the lock file:
    - false when lock is absent
    - true when lock is present
    """
    # lock absent -> false
    tmp_lock.clear_running()
    r1 = client.get("/health")
    assert r1.json["pull_running"] is False

    # lock present -> true
    tmp_lock.set_running()
    r2 = client.get("/health")
    assert r2.json["pull_running"] is True

    tmp_lock.clear_running()


@pytest.mark.web
def test_start_pull_lock(app_test_module, client):
    """
    start_pull_lock() creates the lock file so the app considers a pull 'running'.
    We indirectly verify via /health.
    """
    # Call the function directly
    app_test_module.start_pull_lock()
    try:
        r = client.get("/health")
        assert r.json["pull_running"] is True
    finally:
        app_test_module.clear_pull_lock()


@pytest.mark.web
def test_clear_pull_lock_success(app_test_module, client, tmp_lock):
    """
    clear_pull_lock() removes the lock file when it exists.
    After clearing, /health should show pull_running=false.
    """
    tmp_lock.set_running()  # ensure it exists
    app_test_module.clear_pull_lock()
    r = client.get("/health")
    assert r.json["pull_running"] is False


@pytest.mark.web
def test_clear_pull_lock_exception(monkeypatch, app_test_module, client, tmp_lock):
    """
    clear_pull_lock() swallows exceptions (broad try/except in app.py).
    We simulate an OSError from os.remove and verify the app doesn't crash.
    """
    tmp_lock.set_running()

    real_remove = os.remove

    def boom_remove(path):
        """Mock os.remove that raises an exception."""
        raise OSError("nope")

    try:
        monkeypatch.setattr(os, "remove", boom_remove, raising=True)
        # Should not raise; exception is swallowed
        app_test_module.clear_pull_lock()
    finally:
        # Restore and ensure cleanup
        monkeypatch.setattr(os, "remove", real_remove, raising=True)
        app_test_module.clear_pull_lock()

    r = client.get("/health")
    assert r.json["pull_running"] is False


@pytest.mark.buttons
def test__run(monkeypatch, app_test_module):
    """
    _run(cmd, cwd, env) should return (rc, tail_of_stdout_stderr).
    We simulate a child process that prints to stdout and stderr with rc=0.
    """

    # pylint: disable=too-few-public-methods
    class MockResult:
        """Mock subprocess result with output."""
        returncode = 0
        stdout = "hello\n" * 10
        stderr = "warn\n" * 10

    def fake_run(*args, **kwargs):
        """Mock subprocess.run that validates parameters and returns result."""
        # Validate minimal expectations about how _run calls subprocess.run
        assert kwargs.get("capture_output") is True
        assert kwargs.get("text") is True
        assert kwargs.get("shell") is False
        return MockResult()

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)
    rc, tail = app_test_module._run(["python", "-u", "x.py"], cwd=".", env={})
    assert rc == 0
    assert "hello" in tail and "warn" in tail


@pytest.mark.web
def test_index(client, fake_get_rows):
    """
    GET / should render:
      - 'Analysis' header
      - both button labels
      - at least one Q/A pair with 'Answer:' label
    """
    fake_get_rows.set([("Q: Sample?", "Answer: 1.23%")])
    resp = client.get("/")
    b = resp.data
    assert resp.status_code == 200
    assert b"Analysis" in b
    assert b"Pull Data" in b
    assert b"Update Analysis" in b
    assert b"Answer:" in b


@pytest.mark.buttons
def test_pull_data_is_pull_runnung(client, tmp_lock, fake_get_rows):  # keeping your original name
    """
    POST /pull-data should be ignored when a pull is already running.
    The route returns a redirect with a warning message.
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])
    r = client.post("/pull-data")
    assert r.status_code in (200, 302, 409)
    page = client.get("/")
    assert b"pull_running'>true" in page.data
    tmp_lock.clear_running()


@pytest.mark.buttons
def test_pull_data_applicant_data_json_not_found(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    After scrape/clean succeed, if module_2/applicant_data.json is missing,
    app should show 'module_2/applicant_data.json not found'.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    def ok_run(*args, **kwargs):
        """Mock successful subprocess run."""

        # pylint: disable=too-few-public-methods
        class OkResult:
            """Mock successful result."""
            def __init__(self):
                self.returncode = 0
                self.stdout = "ok"
                self.stderr = ""

        return OkResult()

    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists to control which files appear to exist."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return False  # trigger the not-found branch
        return real_exists(path)

    monkeypatch.setattr(subprocess, "run", ok_run, raising=True)
    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"module_2/applicant_data.json not found" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_llm_standardizer_extended_json_exists(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    If the LLM 'extended' JSON already exists, the app constructs the LLM command
    accordingly (uses the extended file). We don't assert internals of the command—
    just verify the flow proceeds without error and renders a status area.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    def ok_run(*args, **kwargs):
        """Mock successful subprocess run with JSON output."""

        # pylint: disable=too-few-public-methods
        class JsonResult:
            """Mock result with JSON output."""
            def __init__(self):
                self.returncode = 0
                self.stdout = "[]"  # valid JSON array (so not the invalid JSON branch)
                self.stderr = ""

        return JsonResult()

    real_exists = os.path.exists

    def fake_exists(path):
        """Mock file existence for extended JSON scenario."""
        # Simulate presence of all required scripts and both applicant JSONs
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return True
        # Extended LLM file exists -> app should prefer/handle it
        if "extend" in norm or "llm_extend_applicant_data.json" in norm:
            return True
        return real_exists(path)

    monkeypatch.setattr(subprocess, "run", ok_run, raising=True)
    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    # We only assert that the page rendered with a status box; command details are internal.
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_exception(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Any unexpected exception in the pipeline is caught; app shows 'Pull failed:'.
    Here we raise an exception from subprocess.run to hit the except block.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    def boom(*args, **kwargs):
        """Mock subprocess run that raises an exception."""
        raise RuntimeError("unexpected")

    monkeypatch.setattr(subprocess, "run", boom, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Pull failed:" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_update_analysis_is_pull_running(client, tmp_lock, fake_get_rows):
    """
    When a pull is running, POST /update-analysis should be ignored with the
    message 'Update ignored: a data pull is currently running.'
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])

    resp = client.post("/update-analysis", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Update ignored: a data pull is currently running." in resp.data
    assert b"id='status'" in resp.data

    tmp_lock.clear_running()


@pytest.mark.buttons
def test_update_analysis_success(client, tmp_lock, fake_get_rows):
    """
    When not running, POST /update-analysis should succeed with
    'Analysis updated with the latest data.'
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    resp = client.post("/update-analysis", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Analysis updated with the latest data." in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.web
def test_health_ok(client, tmp_lock):
    """
    /health returns JSON with 'ok': True and current 'pull_running' flag.
    """
    tmp_lock.clear_running()
    r1 = client.get("/health")
    assert r1.status_code == 200
    assert r1.json.get("ok") is True
    assert r1.json.get("pull_running") is False

    tmp_lock.set_running()
    r2 = client.get("/health")
    assert r2.status_code == 200
    assert r2.json.get("pull_running") is True

    tmp_lock.clear_running()


@pytest.mark.buttons
def test_pull_data_scrape_fail(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Fail the SCRAPE step (rc!=0). Make module_2/scrape.py "exist" so the route
    actually tries to run it and returns the expected scrape failure message.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # 1) Make only SCRAPE exist
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for scrape failure test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # 2) Fail the first subprocess call
    calls = {"n": 0}

    def fake_run(*args, **kwargs):
        """Mock subprocess run with first call failing."""
        calls["n"] += 1

        # pylint: disable=too-few-public-methods
        class Result:
            """Mock subprocess result."""
            def __init__(self):
                self.returncode = 1 if calls["n"] == 1 else 0
                self.stdout = "" if calls["n"] == 1 else "ok"
                self.stderr = "scrape failed" if calls["n"] == 1 else ""

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)

    # Follow redirects so ?msg=... shows in the final HTML
    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Pull step failed in scrape" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_clean_fail(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Fail the CLEAN step (rc!=0). Make both SCRAPE and CLEAN exist so the route
    runs SCRAPE (ok) then CLEAN (fail), then shows the clean failure message.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # 1) Make SCRAPE and CLEAN exist
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for clean failure test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # 2) SCRAPE ok, CLEAN fail
    calls = {"n": 0}

    def fake_run(*args, **kwargs):
        """Mock subprocess run with clean step failing."""
        calls["n"] += 1

        # pylint: disable=too-few-public-methods
        class Result:
            """Mock subprocess result."""
            def __init__(self):
                self.returncode = 0 if calls["n"] == 1 else 1
                self.stdout = "ok" if calls["n"] == 1 else ""
                self.stderr = "" if calls["n"] == 1 else "clean failed"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Pull step failed in clean" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_llm_step_fail(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    LLM rc!=0. Make SCRAPE and CLEAN exist and succeed, and also pretend
    applicant_data.json exists so the route gets to the LLM step and fails there.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # 1) SCRAPE, CLEAN, applicant_data.json exist
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for LLM failure test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # 2) SCRAPE ok, CLEAN ok, LLM fail
    calls = {"n": 0}

    def run_fail_llm(*args, **kwargs):
        """Mock subprocess run with LLM step failing."""
        calls["n"] += 1

        # pylint: disable=too-few-public-methods
        class Result:
            """Mock subprocess result."""
            def __init__(self):
                if calls["n"] in (1, 2):
                    self.returncode, self.stdout, self.stderr = 0, "ok", ""
                else:
                    self.returncode, self.stdout, self.stderr = 1, "", "llm failed"

        return Result()

    monkeypatch.setattr(subprocess, "run", run_fail_llm, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"LLM step failed (rc=" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_llm_step_invalid_json(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    LLM rc=0 but stdout is not JSON. Make it to the LLM step and return a
    non-JSON body; the route should show the invalid JSON message.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # 1) Make SCRAPE, CLEAN, applicant_data.json exist
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for invalid JSON test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # 2) SCRAPE ok, CLEAN ok, LLM ok rc but bad stdout
    calls = {"n": 0}

    def run_invalid_json(*args, **kwargs):
        """Mock subprocess run with invalid JSON output."""
        calls["n"] += 1

        # pylint: disable=too-few-public-methods
        class Result:
            """Mock subprocess result."""
            def __init__(self):
                if calls["n"] in (1, 2):
                    self.returncode, self.stdout, self.stderr = 0, "ok", ""
                else:
                    self.returncode, self.stdout, self.stderr = 0, "NOT_JSON", ""

        return Result()

    monkeypatch.setattr(subprocess, "run", run_invalid_json, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"LLM step output is not valid JSON" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_load_db_fail(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Load step rc!=0. Make it through SCRAPE/CLEAN (ok) and LLM (valid JSON),
    then fail the LOAD step; the route should show the load failure message.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # 1) Make SCRAPE, CLEAN, applicant_data.json exist
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for load failure test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # 2) SCRAPE ok, CLEAN ok, LLM ok (valid '[]'), LOAD fails
    calls = {"n": 0}

    def run_load_fail(*args, **kwargs):
        """Mock subprocess run with load step failing."""
        calls["n"] += 1

        # pylint: disable=too-few-public-methods
        class Result:
            """Mock subprocess result."""
            def __init__(self):
                if calls["n"] in (1, 2):  # scrape/clean
                    self.returncode, self.stdout, self.stderr = 0, "ok", ""
                elif calls["n"] == 3:  # llm
                    self.returncode, self.stdout, self.stderr = 0, "[]", ""
                else:  # load
                    self.returncode, self.stdout, self.stderr = 1, "", "load failed"

        return Result()

    monkeypatch.setattr(subprocess, "run", run_load_fail, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Load step failed (rc=" in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_success(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Full happy path. Make SCRAPE/CLEAN exist and succeed, applicant_data.json exist,
    LLM returns valid JSON, and LOAD succeeds; should show the success message.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # 1) Needed files exist
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for success test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        if norm.endswith("/module_2/clean.py"):
            return True
        if norm.endswith("/module_2/applicant_data.json"):
            return True
        # also allow writing extended_json; directory creation uses os.makedirs
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # 2) All steps ok
    calls = {"n": 0}

    def run_all_ok(*args, **kwargs):
        """Mock subprocess run with all steps succeeding."""
        calls["n"] += 1

        # pylint: disable=too-few-public-methods
        class Result:
            """Mock subprocess result."""
            def __init__(self):
                if calls["n"] in (1, 2):
                    self.returncode, self.stdout, self.stderr = 0, "ok", ""
                elif calls["n"] == 3:  # LLM
                    self.returncode, self.stdout, self.stderr = 0, "[]", ""
                else:  # LOAD
                    self.returncode, self.stdout, self.stderr = 0, "loaded", ""

        return Result()

    monkeypatch.setattr(subprocess, "run", run_all_ok, raising=True)

    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Pull complete. New data (if any) added." in resp.data
    assert b"id='status'" in resp.data


@pytest.mark.buttons
def test_pull_data_exception_covers_clear_lock_and_message(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Covers the OSError except in pull_data.
    We force an OSError on the first pipeline call, after making SCRAPE
    appear to exist so the code enters the try-block. We then verify:
      - the error message "Pull failed: ..." appears
      - the lock is cleared (pull_running=false)
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    # Ensure the code tries to run SCRAPE (enter try-block)
    real_exists = os.path.exists

    def fake_exists(path):
        """Mock os.path.exists for exception test."""
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"):
            return True
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # First subprocess call raises OSError -> triggers except path
    def boom_run(*args, **kwargs):
        """Mock subprocess run that raises an OSError."""
        raise OSError("kaboom")  # Changed from RuntimeError to OSError

    monkeypatch.setattr(subprocess, "run", boom_run, raising=True)

    # The app should catch the OSError and redirect with error message
    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Pull failed:" in resp.data  # message set by the except block
    # Lock must be cleared in the except block
    health = client.get("/health")
    assert health.json["pull_running"] is False



@pytest.mark.web
def test_main_guard_runs_without_starting_server(monkeypatch):
    """
    Covers: if __name__ == "__main__": app.run(...)
    We monkeypatch Flask.run to a no-op so executing the module as __main__
    reaches the line without opening a port.
    """
    monkeypatch.setattr(Flask, "run", lambda self, **kw: None, raising=True)
    # Execute src/app.py as __main__ (hits the main guard)
    runpy.run_module("app", run_name="__main__")