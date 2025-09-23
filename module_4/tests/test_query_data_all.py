"""
Tests SQL query functions, data aggregation, result formatting, and file operations.
Follows TDD GIVEN/WHEN/THEN structure as required by Module 4 assignment.
"""

import pytest
from types import SimpleNamespace
import json
from pathlib import Path
from unittest.mock import Mock, patch

# --- tiny SQL-aware fakes ---

class _FakeCursor:
    def __init__(self):
        self._rows = []
    def execute(self, sql, params=None):
        s = (sql or "").lower()
        # Return single value for _fetch_val cases
        if "count(*)" in s and "fall" in s and "2025" in s:
            self._rows = [(5,)]
        elif "case when count(*) = 0 then 0" in s and "from public.applicants" in s and "international" in s and "where" not in s:
            self._rows = [(12.3,)]
        elif s.strip().startswith("select avg(case when gpa <= 5") and "from public.applicants" in s and "where" in s:
            self._rows = [(3.5, 310.0, 150.0, 4.0)]
        elif "lower(us_or_international) = 'american'" in s:
            self._rows = [(3.2,)]
        elif "lower(status) like 'accept%'" in s and "fall" in s and "2025" in s and "avg(case when gpa <= 5" in s:
            self._rows = [(3.7,)]
        elif "lower(status) like 'accept%'" in s and "fall" in s and "2025" in s and "case when count(*)" in s:
            self._rows = [(45.68,)]
        elif "johns hopkins" in s or "jhu" in s:
            self._rows = [(2,)]
        elif "georgetown" in s and "phd" in s:
            self._rows = [(1,)]
        elif "group by university" in s and "limit 5" in s:
            self._rows = [("Alpha U", 3), ("Beta U", 2)]
        elif "date_part('year', date_added) =" in s:
            self._rows = [("Accepted", 320.0), ("Rejected", None)]
        elif "between %s and %s" in s:
            self._rows = [("Accepted", 315.5)]
        else:
            # default: one row, first col zero to keep code flowing
            self._rows = [(0,)]
        self._params = params
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return list(self._rows)

class _FakeConn:
    def cursor(self):
        # context-manager friendly cursor
        cur = _FakeCursor()
        return cur
    def __enter__(self):
        return self
    def __exit__(self, *a):
        return False

@pytest.fixture(autouse=True)
def patch_pool(monkeypatch):
    # All query_data DB paths consume src.dal.pool.get_conn()
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: _FakeConn())

# --- tests ---

def test_q1_to_q8_and_customs_import_and_run():
    from src import query_data as q
    assert q.q1_count_fall_2025() == 5
    assert abs(q.q2_pct_international() - 12.3) < 1e-6
    gpa, gre, gre_v, gre_aw = q.q3_avgs()
    assert (round(gpa, 2), round(gre, 1), round(gre_v, 1), round(gre_aw, 1)) == (3.5, 310.0, 150.0, 4.0)
    assert q.q4_avg_gpa_american_fall2025() == 3.2
    assert abs(q.q5_pct_accept_fall2025() - 45.68) < 1e-6
    assert q.q6_avg_gpa_accept_fall2025() == 3.7
    assert q.q7_count_jhu_masters_cs() == 2
    assert q.q8_count_2025_georgetown_phd_cs_accept() == 1
    assert q.q9_top5_accept_unis_2025()[:2] == [("Alpha U", 3), ("Beta U", 2)]
    rows_year = q.q10_avg_gre_by_status_year(2024)
    assert rows_year[0][0] == "Accepted"
    rows_last = q.q10_avg_gre_by_status_last_n_years(3)
    assert rows_last[0][0] == "Accepted"
    # run_all builds lines (exercises writer)
    lines = q.run_all()
    assert any("Q1" in line for line in lines)

# ---------- DB helper coverage ----------

@pytest.mark.analysis
def test_fetch_val_returns_first_column(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (42, "extra")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_val
    result = _fetch_val("SELECT COUNT(*) FROM test")
    assert result == 42

@pytest.mark.analysis
def test_fetch_val_returns_none_for_empty_result(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_val
    assert _fetch_val("SELECT COUNT(*) FROM empty_table") is None

@pytest.mark.analysis
def test_fetch_all_returns_all_rows(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("row1",), ("row2",), ("row3",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_all
    assert _fetch_all("SELECT * FROM test") == [("row1",), ("row2",), ("row3",)]

@pytest.mark.analysis
def test_fetch_all_with_parameters(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("result",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_all
    res = _fetch_all("SELECT * FROM test WHERE id = %s", [123])
    mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %s", [123])
    assert res == [("result",)]

# ---------- individual query wrappers ----------

@pytest.mark.analysis
def test_q1_count_fall_2025(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (150,)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q1_count_fall_2025
    result = q1_count_fall_2025()
    assert result == 150
    assert isinstance(result, int)

@pytest.mark.analysis
def test_q2_pct_international(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (25.5,)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q2_pct_international
    result = q2_pct_international()
    assert result == 25.5
    assert isinstance(result, float)

@pytest.mark.analysis
def test_q3_avgs_returns_tuple(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [(3.7, 320.0, 155.0, 4.2)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q3_avgs
    result = q3_avgs()
    assert result == (3.7, 320.0, 155.0, 4.2)
    assert len(result) == 4


@pytest.mark.analysis
def test_q3_avgs_with_nulls(monkeypatch):
    """
    GIVEN: Database with some NULL average values
    WHEN: q3_avgs is called
    THEN: Tuple with None values should be returned
    """
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [(3.5, None, 150.0, None)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q3_avgs
    result = q3_avgs()
    assert result == (3.5, None, 150.0, None)


@pytest.mark.analysis
def test_q9_top5_accept_unis_2025(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("Harvard", 15), ("MIT", 12), ("Stanford", 10)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q9_top5_accept_unis_2025
    result = q9_top5_accept_unis_2025()
    assert result == [("Harvard", 15), ("MIT", 12), ("Stanford", 10)]
    assert isinstance(result, list)


@pytest.mark.analysis
def test_q10_avg_gre_by_status_year(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("Accepted", 325.0), ("Rejected", 310.0)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q10_avg_gre_by_status_year
    result = q10_avg_gre_by_status_year(2024)
    assert result == [("Accepted", 325.0), ("Rejected", 310.0)]
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args
    assert 2024 in call_args[0][1]


@pytest.mark.analysis
def test_q10_avg_gre_by_status_last_n_years(monkeypatch):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("Accepted", 320.0)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import q10_avg_gre_by_status_last_n_years
    result = q10_avg_gre_by_status_last_n_years(3)
    assert result == [("Accepted", 320.0)]
    mock_cursor.execute.assert_called_once()


# -------- file I/O helpers coverage --------

@pytest.mark.analysis
def test_write_lines_creates_output_file(tmp_path, monkeypatch):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()

    def mock_path_init(path_str):
        if "__file__" in str(path_str):
            return artifacts_dir.parent
        return Path(path_str)

    with patch("src.query_data.Path", side_effect=mock_path_init):
        from src.query_data import _write_lines
        lines = ["Q1 Test line 1", "Q2 Test line 2", "Q3 Test line 3"]
        result_path = _write_lines(lines)
        assert result_path.exists()
        content = result_path.read_text(encoding="utf-8")
        assert content == "Q1 Test line 1\nQ2 Test line 2\nQ3 Test line 3\n"


@pytest.mark.analysis
def test_write_lines_handles_empty_list(tmp_path, monkeypatch):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()

    def mock_path_init(path_str):
        if "__file__" in str(path_str):
            return artifacts_dir.parent
        return Path(path_str)

    with patch("src.query_data.Path", side_effect=mock_path_init):
        from src.query_data import _write_lines
        result_path = _write_lines([])
        assert result_path.exists()
        content = result_path.read_text(encoding="utf-8")
        assert content == "\n"


# -------- run_all + main-path style coverage --------

@pytest.mark.analysis
class TestRunAllFunction:
    def test_run_all_generates_complete_output(self, monkeypatch):
        monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 150)
        monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 25.75)
        monkeypatch.setattr("src.query_data.q3_avgs", lambda: (3.65, 318.5, 157.2, 4.1))
        monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: 3.72)
        monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 22.50)
        monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: 3.85)
        monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 8)
        monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 3)
        monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025",
                            lambda: [("Harvard", 12), ("MIT", 10), ("Stanford", 8)])
        monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year",
                            lambda y: [("Accepted", 325.5), ("Rejected", 308.2)])
        monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years",
                            lambda n: [("Accepted", 322.1), ("Wait listed", 315.8)])

        from src.query_data import run_all
        lines = run_all()
        assert len(lines) >= 10
        q1_line = next(line for line in lines if line.startswith("Q1"))
        assert "150" in q1_line
        q2_line = next(line for line in lines if line.startswith("Q2"))
        assert "25.75%" in q2_line
        q3_line = next(line for line in lines if line.startswith("Q3"))
        assert "Average GPA: 3.65" in q3_line and "Average GRE: 318.50" in q3_line
        q5_line = next(line for line in lines if line.startswith("Q5"))
        assert "22.50%" in q5_line

    def test_run_all_handles_none_values(self, monkeypatch):
        monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 0)
        monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 0.0)
        monkeypatch.setattr("src.query_data.q3_avgs", lambda: (None, None, None, None))
        monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: None)
        monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 0.0)
        monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: None)
        monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 0)
        monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 0)
        monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025", lambda: [])
        monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year", lambda y: [])
        monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years", lambda n: [])

        from src.query_data import run_all
        lines = run_all()
        q3_line = next(line for line in lines if line.startswith("Q3"))
        assert "NA" in q3_line
        q4_line = next(line for line in lines if line.startswith("Q4"))
        assert "NA" in q4_line
        q6_line = next(line for line in lines if line.startswith("Q6"))
        assert "NA" in q6_line

    def test_run_all_formats_percentages_correctly(self, monkeypatch):
        monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 100)
        monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 33.333333)
        monkeypatch.setattr("src.query_data.q3_avgs", lambda: (3.0, 300.0, 150.0, 4.0))
        monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: 3.0)
        monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 16.666666)
        monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: 3.0)
        monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 0)
        monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 0)
        monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025", lambda: [])
        monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year", lambda y: [])
        monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years", lambda n: [])

        from src.query_data import run_all
        lines = run_all()
        q2_line = next(line for line in lines if line.startswith("Q2"))
        assert "33.33%" in q2_line
        q5_line = next(line for line in lines if line.startswith("Q5"))
        assert "16.67%" in q5_line


# -------- edge cases --------

@pytest.mark.analysis
class TestQueryEdgeCases:
    def test_query_functions_handle_zero_results(self, monkeypatch):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (0,)
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        from src.query_data import q1_count_fall_2025, q9_top5_accept_unis_2025
        assert q1_count_fall_2025() == 0
        assert q9_top5_accept_unis_2025() == []

    def test_query_functions_with_null_database_results(self, monkeypatch):
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        from src.query_data import q4_avg_gpa_american_fall2025, q6_avg_gpa_accept_fall2025
        assert q4_avg_gpa_american_fall2025() is None
        assert q6_avg_gpa_accept_fall2025() is None

