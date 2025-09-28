# conftest.py
# -----------------------------------------------------------------------------
# Purpose:
#   - Make imports from module_4/src work inside tests
#   - Replace real DB with a tiny fake (no network, no real tables)
#   - Replace subprocess.run so scrape/clean/LLM/load never actually run
#   - Replace render_template so tests don't need real HTML templates
#   - Provide a Flask test client ready to use in tests
#   - Provide simple helpers to simulate "pull is running" via a lock file
# -----------------------------------------------------------------------------

import sys
import os
import json
from pathlib import Path
import types
import pytest


# =============================================================================
# 1) Make "from app import app" etc. import from module_4/src
# =============================================================================
ROOT = Path(__file__).resolve().parents[1]   # -> module_4/
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


# =============================================================================
# 2) Tiny Fake DB so tests never touch a real PostgreSQL server
#    - Push the next fetch result(s) in your test.
#    - Code under test calls psycopg.connect() -> we give it a FakeConnection.
# =============================================================================
class FakeCursor:
    """
    Minimal cursor:
      - records SQL you "execute"
      - returns values you preloaded into queues for fetchone()/fetchall()
    """
    def __init__(self):
        self.executed = []          # list of (sql, params) the code ran
        self._one_queue = []        # values to return for fetchone()
        self._all_queue = []        # lists of rows to return for fetchall()

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    # ---- test helpers (you call these in your tests) ----
    def push_one(self, value):
        """Next call to fetchone() will return this value."""
        self._one_queue.append(value)

    def push_all(self, rows):
        """Next call to fetchall() will return this list of rows."""
        self._all_queue.append(rows)

    # ---- what the app code will call ----
    def fetchone(self):
        return self._one_queue.pop(0) if self._one_queue else None

    def fetchall(self):
        return self._all_queue.pop(0) if self._all_queue else []

    # context manager support: "with conn.cursor() as cur:"
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FakeConnection:
    """Very small connection that always returns the same FakeCursor."""
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


@pytest.fixture
def fake_db(monkeypatch):
    """
    Replaces psycopg.connect with a function that returns our FakeConnection.
    Usage in tests:
        cur = fake_db
        cur.push_one(123)        # next fetchone() => 123
        cur.push_all([("a",)])   # next fetchall() => [("a",)]
    """
    cur = FakeCursor()

    def _fake_connect(*_, **__):
        return FakeConnection(cur)

    # Import psycopg and patch its connect function
    import importlib
    psycopg = importlib.import_module("psycopg")
    monkeypatch.setattr(psycopg, "connect", _fake_connect, raising=True)

    return cur  # return the cursor so tests can preload results


# =============================================================================
# 3) Fake subprocess.run so scrape/clean/LLM/load are "pretend runs"
#    - We look at the command string and return a simple "success" object.
#    - For the LLM step we return a small JSON list on stdout.
# =============================================================================
class SimpleCompleted:
    """Tiny stand-in for subprocess.CompletedProcess (only fields we use)."""
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def fake_subprocess(monkeypatch):
    """
    Patches subprocess.run:
      - scrape.py        -> rc=0, "scrape ok"
      - clean.py         -> rc=0, "clean ok"
      - llm_hosting/...  -> rc=0, stdout is JSON array (what app expects)
      - load_data.py     -> rc=0, "loaded ok"
    """
    def _fake_run(cmd, cwd=None, env=None, capture_output=False, text=False, encoding=None):
        cmd_str = " ".join(map(str, cmd))
        if "scrape.py" in cmd_str:
            return SimpleCompleted(0, "scrape ok", "")
        if "clean.py" in cmd_str:
            return SimpleCompleted(0, "clean ok", "")
        if "llm_hosting" in cmd_str or "llm_hosting/app.py" in cmd_str:
            fake_rows = [
                {
                    "program": "X",
                    "Degree": "Masters",
                    "llm-generated-program": "Computer Science",
                    "llm-generated-university": "Example University",
                    "url": "http://example",
                    "date_added": "2025-01-01",
                }
            ]
            return SimpleCompleted(0, json.dumps(fake_rows), "")
        if "load_data.py" in cmd_str:
            return SimpleCompleted(0, "loaded ok", "")
        return SimpleCompleted(0, "", "")

    import subprocess
    monkeypatch.setattr(subprocess, "run", _fake_run, raising=True)


# =============================================================================
# 4) Load app.py and make it template-free + lock-file to tmp
#    - We replace render_template with a tiny HTML builder
#    - We redirect LOCK_PATH to a temp file so tests can simulate "pull running"
# =============================================================================
@pytest.fixture
def app_mod(monkeypatch, tmp_path):
    """
    Imports src/app.py and patches:
      - render_template(...) with a tiny HTML string (easy assertions)
      - LOCK_PATH -> tmp file (safe place for "pull running" flag)
    Returns the imported module so tests can access app_mod.app (Flask app).
    """
    import importlib
    app_mod = importlib.import_module("app")

    # Build very small HTML for predictable tests
    # in tests/conftest.py, inside app_mod fixture:
    def _fake_render_template(*_args, **kwargs):
        """
        Minimal HTML so tests can assert page title, buttons, status, and Q/A.

        Accept **kwargs** because app.index() might pass the message as
        'msg' or 'status' or 'status_msg', and the level as 'level' or 'status_level'.
        """
        # Pull values using multiple possible keys (be forgiving)
        rows = kwargs.get("rows", [])
        pull_running = kwargs.get("pull_running", False)
        report_exists = kwargs.get("report_exists", False)

        # Status message and level could be passed with different names
        status_msg = (
                kwargs.get("status_msg")
                or kwargs.get("status")
                or kwargs.get("msg")
                or ""
        )
        status_level = kwargs.get("status_level") or kwargs.get("level") or "info"

        # Build tiny, predictable HTML
        lines = []
        lines.append("<html><body>")
        lines.append("<h1>Analysis</h1>")  # page title
        # Buttons (label-only so tests can assert presence)
        lines.append("<button id='btn-pull'>Pull Data</button>")
        lines.append("<button id='btn-update'>Update Analysis</button>")
        # Status scaffolding + the visible message text
        lines.append(f"<div id='status' data-level='{status_level}'>{status_msg}</div>")
        lines.append(f"<div id='pull_running'>{str(pull_running).lower()}</div>")
        lines.append(f"<div id='report_exists'>{str(report_exists).lower()}</div>")
        # Render Q/A rows
        for i, (q, a) in enumerate(rows, start=1):
            lines.append(
                f"<div class='qa' data-i='{i}'>"
                f"<span class='q'>{q}</span><span class='a'>Answer:{a}</span></div>"
            )
        lines.append("</body></html>")
        return "\n".join(lines)

    # Patch the function the app calls to render HTML
    monkeypatch.setattr(app_mod, "render_template", _fake_render_template, raising=True)

    # Point the lock path into a temporary folder
    lock_path = tmp_path / "pull.lock"
    monkeypatch.setattr(app_mod, "LOCK_PATH", str(lock_path), raising=True)

    return app_mod


# =============================================================================
# 5) Flask test client ready to go (DB + subprocess already faked by fixtures)
# =============================================================================
@pytest.fixture
def client(app_mod, fake_db, fake_subprocess):
    """
    Creates a Flask test client with TESTING mode on.
    Because app_mod/fake_db/fake_subprocess are listed as dependencies here,
    they are applied automatically before you get the client.
    """
    flask_app = app_mod.app
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


# =============================================================================
# 6) Simple helpers to set/clear the "pull is running" lock file
# =============================================================================
@pytest.fixture
def tmp_lock(app_mod):
    """
    Gives you two helper functions:
      tmp_lock.set_running() -> create the lock file
      tmp_lock.clear_running() -> remove the lock file
    Use this to simulate that a Pull is in progress.
    """
    lock_file = Path(app_mod.LOCK_PATH)

    def set_running():
        lock_file.write_text("running", encoding="utf-8")

    def clear_running():
        if lock_file.exists():
            lock_file.unlink()

    return types.SimpleNamespace(set_running=set_running, clear_running=clear_running)


# =============================================================================
# 7) Optional: bypass SQL entirely by faking query_data.get_rows()
#    - Useful for UI checks when you don't care about SQL paths.
# =============================================================================
@pytest.fixture
def fake_get_rows(monkeypatch):
    """
    Overrides both query_data.get_rows AND app.get_rows so the Flask route
    sees our fake rows even though app.py imported the symbol at import time.
    """
    import importlib
    qmod = importlib.import_module("query_data")
    app_mod = importlib.import_module("app")  # app.py did: from query_data import get_rows

    def set_rows(rows):
        monkeypatch.setattr(qmod, "get_rows", lambda: rows, raising=True)
        monkeypatch.setattr(app_mod, "get_rows", lambda: rows, raising=True)

    return types.SimpleNamespace(set=set_rows)

