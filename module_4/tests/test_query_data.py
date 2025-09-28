# module_4/tests/test_query_data.py
"""
Unit tests for query_data helpers (beginner-friendly).
We avoid a real database by passing tiny fake cursor objects directly.
This raises coverage on query_data.py without needing complex SQL setups.
"""

import pytest
from src.query_data import _one_value
from src.query_data import _latest_term_filter

class _FakeCursorOne:
    """
    Minimal cursor whose fetchone() returns a fixed tuple once.
    Used to test _one_value(cur, sql) returning the first column.
    """
    def __init__(self, row):
        self._row = row
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        # Return the provided row exactly once; then None (like no rows)
        result, self._row = self._row, None
        return result


class _FakeCursorLatestYear:
    """
    Minimal cursor for _latest_term_filter tests.

    Behavior:
    - First call (MAX(date_added) query) -> returns the configured 'year_row'
    - Any further calls -> safe defaults (no crash). We don't rely on them here.
    """
    def __init__(self, year_row):
        self._calls = 0
        self._year_row = year_row  # e.g., (2025,) or (None,) or None
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._calls += 1

    def fetchone(self):
        # First fetch corresponds to the MAX(date_added) query
        if self._calls == 1:
            return self._year_row
        # Later fetches return None (not needed for our simple assertions)
        return None

    def fetchall(self):
        # Not used in these focused tests
        return []


@pytest.mark.db
def test_one_value_returns_first_column():
    """
    _one_value should execute the SQL and return the first column
    of the first row, or None if there is no row.
    """


    # Case 1: row exists -> return first column
    cur = _FakeCursorOne((123, "extra"))
    out = _one_value(cur, "SELECT 123, 'x'")
    assert out == 123
    assert "SELECT 123" in cur.executed[0][0]

    # Case 2: no row -> returns None
    cur2 = _FakeCursorOne(None)
    out2 = _one_value(cur2, "SELECT nothing")
    assert out2 is None


@pytest.mark.db
def test_latest_term_filter_uses_date_added_year_if_present():
    """
    _latest_term_filter should prefer the year derived from MAX(date_added)
    when available, and build a term condition including that year.
    We assert that both 'label' and 'sql_condition' include the year text.
    """

    fake_cur = _FakeCursorLatestYear((2025,))
    label, cond = _latest_term_filter(fake_cur)

    # Be flexible about exact wording; assert the important parts:
    assert "2025" in label    # e.g., "Fall 2025" or "2025"
    assert "2025" in cond     # e.g., "term ILIKE '%2025%'"


@pytest.mark.db
def test_latest_term_filter_falls_back_when_no_year():
    """
    When MAX(date_added) is NULL/None, _latest_term_filter falls back.
    We don't assert the exact fallback wording (it can be just 'TRUE'
    or a year parsed from term text) — we only assert it returns a
    (label, condition) pair of strings to keep the app working.
    """

    fake_cur = _FakeCursorLatestYear((None,))
    label, cond = _latest_term_filter(fake_cur)
    assert isinstance(label, str)
    assert isinstance(cond, str)
    assert label != "" and cond != ""

# module_4/tests/test_query_data.py
"""
Unit tests for query_data.py (beginner-friendly, no real DB).
- _read_db_config: error/success paths
- _latest_term_filter: season branch
- get_rows(): Q1–Q12 happy and empty branches
- main() and the __main__ guard printing

All DB calls use the fake psycopg connection from conftest.py (fake_db).
"""

import configparser
import runpy
import pytest
import src.query_data as qd

# ---------------------------------------------------------------------------
# Helpers: preload results for the sequence used by get_rows()
# ---------------------------------------------------------------------------

def _preload_q1_to_q8_and_term(fake_cur, *, year=2025, season=("fall", 5)):
    """
    Push the sequence of fetch results required by get_rows() up to the term:
      Q1..Q8 -> fetchone() calls
      latest-term: year -> fetchone(); season -> fetchone()
    Values are simple but valid so formatting works.
    """
    # Q1..Q8 (fetchone)
    fake_cur.push_one((10,))               # Q1
    fake_cur.push_one((37.12,))            # Q2
    fake_cur.push_one((3.5, 320.0, 160.0, 4.5))  # Q3: AVG tuple
    fake_cur.push_one((3.40,))             # Q4
    fake_cur.push_one((25.55,))            # Q5
    fake_cur.push_one((3.60,))             # Q6
    fake_cur.push_one((2,))                # Q7
    fake_cur.push_one((1,))                # Q8

    # latest-term year, then season-with-count
    fake_cur.push_one((year,))             # year from MAX(date_added)
    fake_cur.push_one(season)              # ('fall', 5)


# ---------------------------------------------------------------------------
# _read_db_config
# ---------------------------------------------------------------------------

@pytest.mark.db
def test__read_db_config_config_or_db_missing(monkeypatch, capsys, tmp_path):
    """
    Negative: either config file not readable or no [db] section.
    Should print: 'ERROR: config.ini with [db] is required.' and raise SystemExit.
    """
    # 1) unreadable: ConfigParser.read returns []
    monkeypatch.setattr(configparser.ConfigParser, "read", lambda self, p: [], raising=True)
    with pytest.raises(SystemExit):
        qd._read_db_config(str(tmp_path / "missing.ini"))
    out = capsys.readouterr().out
    assert "ERROR: config.ini with [db] is required." in out

    # 2) file "read", but [db] missing
    class _FakeParser(configparser.ConfigParser):
        def read(self, path):
            return [str(path)]
        def __contains__(self, key):
            return False  # no [db]
    monkeypatch.setattr(configparser, "ConfigParser", _FakeParser, raising=True)
    with pytest.raises(SystemExit):
        qd._read_db_config(str(tmp_path / "config.ini"))
    out2 = capsys.readouterr().out
    assert "ERROR: config.ini with [db] is required." in out2


@pytest.mark.db
def test__read_db_config_success(tmp_path):
    """
    Success: returns dict with host/port/dbname/user/password.
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
    cfg = qd._read_db_config(str(ini))
    assert cfg["host"] == "localhost"
    assert cfg["port"] == 5432
    assert cfg["dbname"] == "testdb"
    assert cfg["user"] == "alice"
    assert cfg["password"] == "secret"


# ---------------------------------------------------------------------------
# _latest_term_filter
# ---------------------------------------------------------------------------

@pytest.mark.db
def test__latest_term_filter_if_season(fake_db):
    """
    Season branch: when a year exists and a season row with count>0 is returned,
    function should return ('Fall 2025', "term ILIKE '%fall%' AND term ILIKE '%2025%'").
    """
    # year
    fake_db.push_one((2025,))
    # ('fall', count>0)
    fake_db.push_one(("fall", 7))
    label, cond = qd._latest_term_filter(fake_db)
    assert label == "Fall 2025"
    assert "term ILIKE '%fall%'" in cond and "term ILIKE '%2025%'" in cond


# ---------------------------------------------------------------------------
# get_rows: Q1–Q8 (basic) + then each Q9–Q12 branch covered separately
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_get_rows_q1_to_q8_only(fake_db, monkeypatch):
    """
    Basic run through Q1..Q8 with a valid term; for Q9..Q12 we push empty lists
    just to complete the function (their detailed branches tested below).
    """
    # avoid reading real config; FakeConnection ignores values anyway
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db, year=2025, season=("fall", 5))

    # Q9..Q12 all empty
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert len(rows) == 12
    # Spot check that the first group formatted cleanly
    flat_answers = " ".join(a for _, a in rows)
    assert "Applicant count:" in flat_answers
    assert "Percent International:" in flat_answers
    assert "Average GPA:" in flat_answers
    assert "Average GPA American:" in flat_answers
    assert "Acceptance percent:" in flat_answers
    assert "Average GPA Acceptance:" in flat_answers
    assert "Count:" in flat_answers


@pytest.mark.db
def test_get_rows_q9_ans(fake_db, monkeypatch):
    """
    Q9 has rows -> builds 'Top 5 by entries — ...' list.
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    # Q9 non-empty
    fake_db.push_all([("Uni A", 5), ("Uni B", 4), ("Uni C", 3), ("Uni D", 2), ("Uni E", 1)])
    # Q10..Q12 empty to end quickly
    fake_db.push_all([])
    fake_db.push_all([])
    fake_db.push_all([])

    rows = qd.get_rows()
    q9_q, q9_a = rows[8]
    assert "Top 5 by entries" in q9_a
    assert "1) Uni A — 5" in q9_a


@pytest.mark.db
def test_get_rows_q9_no_ans(fake_db, monkeypatch):
    """
    Q9 empty -> 'No entries found for that term'.
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert "No entries found for that term" in rows[8][1]


@pytest.mark.db
def test_get_rows_q10_ans(fake_db, monkeypatch):
    """
    Q10 has rows -> builds 'cat — pct% (c/total)' mix string.
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([("Accept", 7, 70.0), ("Reject", 3, 30.0)])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert "Accept — 70.00% (7/10)" in rows[9][1]


@pytest.mark.db
def test_get_rows_q10_no_ans(fake_db, monkeypatch):
    """
    Q10 empty -> 'No status information recorded for that term.'
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert "No status information recorded for that term." in rows[9][1]


@pytest.mark.db
def test_get_rows_q11_ans(fake_db, monkeypatch):
    """
    Q11 has rows -> builds degree mix string with percentages and totals.
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([("Masters", 6, 60.0), ("PhD", 4, 40.0)])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert "Masters — 60.00% (6/10)" in rows[10][1]
    assert "PhD — 40.00% (4/10)" in rows[10][1]


@pytest.mark.db
def test_get_rows_q11_no_ans(fake_db, monkeypatch):
    """
    Q11 empty -> 'No degree information recorded for that term.'
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert "No degree information recorded for that term." in rows[10][1]


@pytest.mark.db
def test_get_rows_q12_ans(fake_db, monkeypatch):
    """
    Q12 has rows -> builds per-university GPA line with 'Avg GPA' and n=.
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([("Uni A", 3, 3.5), ("Uni B", 2, 3.7)])  # Q12

    rows = qd.get_rows()
    assert "1) Uni A — Avg GPA 3.50 (n=3)" in rows[11][1]
    assert "2) Uni B — Avg GPA 3.70 (n=2)" in rows[11][1]


@pytest.mark.db
def test_get_rows_q12_no_ans(fake_db, monkeypatch):
    """
    Q12 empty -> 'No GPA data available for that term.'
    """
    monkeypatch.setattr(qd, "_read_db_config", lambda path="config.ini": {}, raising=True)

    _preload_q1_to_q8_and_term(fake_db)
    fake_db.push_all([])  # Q9
    fake_db.push_all([])  # Q10
    fake_db.push_all([])  # Q11
    fake_db.push_all([])  # Q12

    rows = qd.get_rows()
    assert "No GPA data available for that term." in rows[11][1]


# ---------------------------------------------------------------------------
# main() printing & __main__ guard
# ---------------------------------------------------------------------------

@pytest.mark.db
def test_main_prints_rows(monkeypatch, capsys):
    """
    query_data.main(): prints '- Q' and 'Answer:' lines for rows returned by get_rows().
    """
    monkeypatch.setattr(qd, "get_rows", lambda: [("Q1", "Answer: A1"), ("Q2", "Answer: A2")], raising=True)
    qd.main()
    out = capsys.readouterr().out
    assert "- Q1" in out and "Answer: A1" in out
    assert "- Q2" in out and "Answer: A2" in out


# REPLACE your failing test___name___main_guard in tests/test_query_data.py with this:

import configparser
import runpy
import sys
import pytest

@pytest.mark.db
def test___name___main_guard(monkeypatch, fake_db):
    """
    Covers the main guard in query_data.py:
        if __name__ == '__main__': main()

    We avoid real config/DB use by:
    - forcing ConfigParser.read(...) -> [] so _read_db_config exits early,
    - cleaning sys.argv so pytest flags don't get treated as args,
    - expecting SystemExit (which proves the guard invoked main()).

    fake_db is included so psycopg.connect is patched globally if reached.
    """
    # Clean argv for the module being executed
    monkeypatch.setattr(sys, "argv", ["query_data.py"], raising=True)

    # Make _read_db_config fail early on the fresh module
    monkeypatch.setattr(configparser.ConfigParser, "read", lambda self, p: [], raising=True)

    # Execute a fresh copy as __main__ so the guard runs; expect early exit
    with pytest.raises(SystemExit):
        runpy.run_module("query_data", run_name="__main__")
