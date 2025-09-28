# module_4/tests/test_db_insert.py
"""
Tests for load_data.py (DB insert path) — beginner-friendly and minimal.

Covers:
- _num() helper (None, success, and no-match cases)
- _read_db_config() error/success paths
- main(): config-not-found path and happy path (with fake DB)
- __main__ guard invoking main() (without actually loading data twice)

All DB calls use the fake psycopg connection from conftest.py (fake_db).
All filesystem/config access uses tmp paths + monkeypatching where needed.
"""

import os
import sys
import json
import runpy
import configparser
import pytest
from src.load_data import _num
from src.load_data import _read_db_config
import src.load_data as ld


# -----------------------------
# _num helper
# -----------------------------

@pytest.mark.db
def test__num_text_is_none():
    """
    _num(None) -> None (gracefully handles missing numeric text).
    """

    assert _num(None) is None


@pytest.mark.db
def test__num_success():
    """
    _num should parse the first number and return it as float.
    """

    assert _num("GPA 3.76") == 3.76
    assert _num("152") == 152.0
    assert _num(" score: -2.5 ") == -2.5


@pytest.mark.db
def test__num_else_None():
    """
    If the text contains no number, _num returns None.
    """

    assert _num("no numbers here") is None


# -----------------------------
# _read_db_config helper
# -----------------------------

@pytest.mark.db
def test__read_db_config_not_cfg_read(monkeypatch, capsys, tmp_path):
    """
    When configparser.ConfigParser.read(path) returns [] (file not found or unreadable),
    function prints an error and sys.exit(1).
    """

    # Monkeypatch the ConfigParser.read method to always return []
    monkeypatch.setattr(configparser.ConfigParser, "read", lambda self, p: [], raising=True)

    with pytest.raises(SystemExit):
        _read_db_config(str(tmp_path / "missing.ini"))

    out = capsys.readouterr().out
    assert "ERROR: Could not read" in out  # message from load_data.py


@pytest.mark.db
def test__read_db_config_db_not_in_config(monkeypatch, capsys, tmp_path):
    """
    If the file loads but has no [db] section, function prints a message and exits.
    """

    # Fake parser with read()->['config.ini'] but no 'db' section
    class FakeParser(configparser.ConfigParser):
        def read(self, path):
            # Pretend it read successfully
            return [str(path)]
        def __contains__(self, key):
            return False  # no [db]

    monkeypatch.setattr(configparser, "ConfigParser", FakeParser, raising=True)

    with pytest.raises(SystemExit):
        _read_db_config(str(tmp_path / "config.ini"))

    out = capsys.readouterr().out
    assert "ERROR: 'config.ini' is missing the [db] section." in out


@pytest.mark.db
def test__read_db_config_success(tmp_path):
    """
    Happy path: returns a dict with host, port, dbname, user, password.
    """

    ini = tmp_path / "config.ini"
    ini.write_text(
        "[db]\n"
        "host=localhost\n"
        "port=5432\n"
        "database=testdb\n"
        "user=alice\n"
        "password=secret\n",
        encoding="utf-8",
    )

    cfg = _read_db_config(str(ini))
    assert cfg["host"] == "localhost"
    assert cfg["port"] == 5432
    assert cfg["dbname"] == "testdb"
    assert cfg["user"] == "alice"
    assert cfg["password"] == "secret"


# -----------------------------
# main() — config-not-found and success
# -----------------------------

@pytest.mark.db
def test_main_cfg_not_found(monkeypatch, capsys):
    """
    main() prints two error lines and exits when _read_db_config returns {}.
    (We bypass _read_db_config's own exit by monkeypatching it to return {}.)
    """

    monkeypatch.setattr(ld, "_read_db_config", lambda _p="config.ini": {}, raising=True)

    with pytest.raises(SystemExit):
        ld.main()

    out = capsys.readouterr().out
    assert "ERROR: DATABASE properties are not set." in out
    assert "Check config.ini." in out


@pytest.mark.db
def test_main_success(tmp_path, monkeypatch, fake_db, capsys):
    """
    End-to-end success of main():
      - reads config.ini from CWD
      - loads a JSON file with one row
      - creates table + inserts row
      - commits and prints the success summary
    Uses fake DB from conftest.py (no real database).
    """

    # Arrange: working directory contains config.ini and json file
    monkeypatch.chdir(tmp_path)

    (tmp_path / "config.ini").write_text(
        "[db]\n"
        "host=localhost\n"
        "port=5432\n"
        "database=testdb\n"
        "user=alice\n"
        "password=secret\n",
        encoding="utf-8",
    )
    data_file = tmp_path / "rows.json"
    data_file.write_text(json.dumps([
        {
            "program": "CS",
            "comments": "",
            "date_added": "2025-01-01",
            "url": "http://x",
            "status": "Accepted",
            "term": "Fall 2025",
            "US/International": "International",
            "GPA": "3.76",
            "gre": "320",
            "gre_v": "160",
            "gre_aw": "4.5",
            "Degree": "Masters",
            "llm-generated-program": "Computer Science",
            "llm-generated-university": "Some Uni"
        }
    ]), encoding="utf-8")

    # Make main() read our temp data file by patching sys.argv
    argv_backup = sys.argv[:]
    sys.argv[:] = ["load_data.py", str(data_file)]

    try:
        ld.main()
    finally:
        sys.argv[:] = argv_backup  # always restore

    # Assert: success print
    out = capsys.readouterr().out
    assert f"Loaded 1 rows into 'applicants' from: {data_file}" in out

    # Assert: the fake cursor executed CREATE TABLE + INSERT
    cur = fake_db
    # at least 2 execute calls: one CREATE TABLE, one INSERT
    executed_sqls = " ".join(sql for (sql, _params) in cur.executed)
    assert "CREATE TABLE IF NOT EXISTS public.applicants" in executed_sqls
    assert "INSERT INTO public.applicants" in executed_sqls


@pytest.mark.db
def test_main_invoke(monkeypatch):
    """
    Covers the main-guard:
        if __name__ == "__main__": main()

    Strategy:
    - Set sys.argv so pytest flags don't become a JSON path.
    - Make configparser.ConfigParser.read(...) return [] so _read_db_config()
      exits early. That proves the guard actually invoked main(), and we don't
      need any real files or DB.
    - Expect SystemExit from the early exit path.
    """
    # 1) Ensure argv is clean for the module we're about to run
    monkeypatch.setattr(sys, "argv", ["load_data.py"], raising=True)

    # 2) Force _read_db_config() to take the early-exit path
    monkeypatch.setattr(configparser.ConfigParser, "read", lambda self, p: [], raising=True)

    # 3) Execute the module as __main__ so the guard line runs
    with pytest.raises(SystemExit):
        runpy.run_module("load_data", run_name="__main__")

