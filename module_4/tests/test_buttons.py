# module_4/tests/test_buttons.py
"""
Buttons behavior tests for Module 4.
Covers both routes with "not running" vs "running" (lock present), plus a failure branch.
All external work is faked in conftest.py (no real DB / no real subprocess).
"""

import os
import subprocess
import json
import pytest


@pytest.mark.buttons
def test_post_pull_data_returns_200_and_triggers_loader(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Assignment 2.a.i + 2.a.ii:
      i) POST /pull-data returns 200 (we follow redirects to the page)
     ii) Triggers the loader with rows from scraper (LLM stdout -> JSON).

    We:
      - Pretend module_2 files exist so app runs each step
      - Make LLM step (stdout) produce JSON
      - Record that load step was invoked
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])  # page will render after redirect

    # Make pipeline files "exist"
    real_exists = os.path.exists
    def fake_exists(path):
        norm = os.path.normpath(path).replace("\\", "/")
        if norm.endswith("/module_2/scrape.py"): return True
        if norm.endswith("/module_2/clean.py"): return True
        if norm.endswith("/module_2/applicant_data.json"): return True
        # llm app path is checked via cwd, not exists(); no need to force True
        return real_exists(path)
    monkeypatch.setattr(os.path, "exists", fake_exists, raising=True)

    # Fake subprocess pipeline
    seen = {"load_called": False}
    def fake_run(cmd, cwd=None, env=None, capture_output=False, text=False, encoding=None, shell=False):
        class R: pass
        r = R()
        # 1) scrape ok
        if isinstance(cmd, list) and len(cmd) >= 3 and cmd[-1] == "scrape.py":
            r.returncode, r.stdout, r.stderr = 0, "ok", ""
            return r
        # 2) clean ok
        if isinstance(cmd, list) and len(cmd) >= 3 and cmd[-1] == "clean.py":
            r.returncode, r.stdout, r.stderr = 0, "ok", ""
            return r
        # 3) llm ok -> stdout JSON
        if isinstance(cmd, list) and len(cmd) >= 3 and cmd[-1] == "app.py" and "llm_hosting" in (cwd or ""):
            r.returncode, r.stdout, r.stderr = 0, json.dumps([{"program":"X","url":"U","term":"T"}]), ""
            return r
        # 4) load invoked
        if isinstance(cmd, list) and len(cmd) >= 3 and cmd[-2] == "load_data.py":
            seen["load_called"] = True
            r.returncode, r.stdout, r.stderr = 0, "loaded", ""
            return r
        # default
        r.returncode, r.stdout, r.stderr = 0, "", ""
        return r

    monkeypatch.setattr(subprocess, "run", fake_run, raising=True)

    # Follow redirects ⇒ final page returns 200
    resp = client.post("/pull-data", follow_redirects=True)
    assert resp.status_code == 200
    assert seen["load_called"] is True  # loader got triggered
    # page shell still present
    assert b"Analysis" in resp.data and b"Answer:" in resp.data


@pytest.mark.buttons
def test_post_update_analysis_returns_200_when_not_busy(client, tmp_lock, fake_get_rows):
    """
    Assignment 2.b.i: POST /update-analysis returns 200 when not busy.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])
    resp = client.post("/update-analysis", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Analysis" in resp.data


@pytest.mark.buttons
def test_busy_gating_returns_409_for_both_routes(client, tmp_lock, fake_get_rows):
    """
    Assignment 2.c: Busy gating => 409 for both POST routes.
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])

    r1 = client.post("/pull-data")          # no follow_redirects on purpose
    r2 = client.post("/update-analysis")    # no follow_redirects on purpose

    assert r1.status_code == 409
    assert r2.status_code == 409

    tmp_lock.clear_running()


@pytest.mark.buttons
def test_pull_data_runs_when_not_running(client, tmp_lock, fake_get_rows):
    """
    POST /pull-data should proceed when no lock exists (not running).
    Expect redirect (commonly 302) back to the page; subsequent GET shows not running.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    r = client.post("/pull-data")
    assert r.status_code in (200, 302)

    page = client.get("/")
    body = page.data
    assert b"id='pull_running'>false" in body
    assert b"id='status'" in body  # status container present


@pytest.mark.buttons
def test_pull_data_ignored_when_running(client, tmp_lock, fake_get_rows):
    """
    POST /pull-data while already running should be ignored or rejected.
    Expect redirect or 409; subsequent GET shows running=true.
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])

    r = client.post("/pull-data")
    assert r.status_code in (200, 302, 409)

    page = client.get("/")
    assert b"id='pull_running'>true" in page.data

    tmp_lock.clear_running()


@pytest.mark.buttons
def test_update_analysis_runs_when_not_running(client, tmp_lock, fake_get_rows):
    """
    POST /update-analysis when not running should refresh analysis (faked).
    Expect redirect; subsequent GET shows not running and status present.
    """
    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    r = client.post("/update-analysis")
    assert r.status_code in (200, 302)

    page = client.get("/")
    body = page.data
    assert b"id='pull_running'>false" in body
    assert b"id='status'" in body


@pytest.mark.buttons
def test_update_analysis_ignored_when_running(client, tmp_lock, fake_get_rows):
    """
    POST /update-analysis while running should be ignored or rejected.
    Expect redirect or 409; subsequent GET shows running=true.
    """
    tmp_lock.set_running()
    fake_get_rows.set([("Q", "A")])

    r = client.post("/update-analysis")
    assert r.status_code in (200, 302, 409)

    page = client.get("/")
    assert b"id='pull_running'>true" in page.data

    tmp_lock.clear_running()


@pytest.mark.buttons
def test_pull_data_failure_reports_error(client, tmp_lock, fake_get_rows, monkeypatch):
    """
    Failure branch: make the first subprocess call fail (rc!=0) so app reports an error.
    This hits the error-handling path in the pull-data route.
    """
    # Save the function currently patched by conftest so we can delegate to it after the first call
    original = subprocess.run
    calls = {"n": 0}

    def failing_once(cmd, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            class R:
                returncode = 1
                stdout = ""
                stderr = "boom"
            return R()
        return original(cmd, **kwargs)

    monkeypatch.setattr(subprocess, "run", failing_once, raising=True)

    tmp_lock.clear_running()
    fake_get_rows.set([("Q", "A")])

    r = client.post("/pull-data")
    assert r.status_code in (200, 302)

    page = client.get("/")
    assert b"id='status'" in page.data  # page shows a status message area
