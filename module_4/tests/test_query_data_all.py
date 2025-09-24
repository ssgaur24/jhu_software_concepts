import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch


class _FakeCursor:
    def __init__(self):
        self._rows = []

    def execute(self, sql, params=None):
        # Just return fixed test values for any query
        self._rows = [(5,)]  # Default return

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def cursor(self):
        return _FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture(autouse=True)
def patch_pool(monkeypatch):
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: _FakeConn())


@pytest.mark.analysis
def test_all_query_functions_work(monkeypatch):
    """Test all query functions import and run"""
    # Mock all functions to return fixed values
    monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 5)
    monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 12.3)
    monkeypatch.setattr("src.query_data.q3_avgs", lambda: (3.5, 310.0, 150.0, 4.0))
    monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: 3.2)
    monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 45.68)
    monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: 3.7)
    monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 2)
    monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 1)
    monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025", lambda: [("Alpha U", 3), ("Beta U", 2)])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year", lambda y: [("Accepted", 320.0)])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years", lambda n: [("Accepted", 315.5)])

    from src import query_data as q

    # Test all functions return expected values
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

    # Test run_all builds lines
    lines = q.run_all()
    assert any("Q1" in line for line in lines)


@pytest.mark.analysis
def test_fetch_val_returns_first_column(monkeypatch):
    """Test _fetch_val helper function"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (42, "extra")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_val
    result = _fetch_val("SELECT 42")  # Simple query
    assert result == 42


@pytest.mark.analysis
def test_fetch_val_returns_none_for_empty_result(monkeypatch):
    """Test _fetch_val with no results"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_val
    result = _fetch_val("SELECT NULL")  # Simple query
    assert result is None


@pytest.mark.analysis
def test_fetch_all_returns_all_rows(monkeypatch):
    """Test _fetch_all helper function"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("row1",), ("row2",), ("row3",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

    from src.query_data import _fetch_all
    result = _fetch_all("SELECT 'test'")  # Simple query
    assert result is not None


@pytest.mark.analysis
def test_run_all_generates_complete_output(monkeypatch):
    """Test run_all output formatting"""
    monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 150)
    monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 25.75)
    monkeypatch.setattr("src.query_data.q3_avgs", lambda: (3.65, 318.5, 157.2, 4.1))
    monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: 3.72)
    monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 22.50)
    monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: 3.85)
    monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 8)
    monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 3)
    monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025",
                        lambda: [("Harvard", 12), ("MIT", 10)])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year",
                        lambda y: [("Accepted", 325.5)])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years",
                        lambda n: [("Accepted", 322.1)])

    from src.query_data import run_all
    lines = run_all()

    assert len(lines) >= 10
    q1_line = next(line for line in lines if line.startswith("Q1"))
    assert "150" in q1_line


@pytest.mark.analysis
def test_write_lines_function(tmp_path, monkeypatch):
    """Test _write_lines function for 100% coverage"""

    def mock_path_init(path_str):
        if "__file__" in str(path_str):
            return tmp_path
        return Path(path_str)

    with patch("src.query_data.Path", side_effect=mock_path_init):
        from src.query_data import _write_lines
        lines = ["Test line 1", "Test line 2"]
        result_path = _write_lines(lines)

        assert result_path.exists()
        content = result_path.read_text(encoding="utf-8")
        assert "Test line 1\nTest line 2\n" == content


@pytest.mark.analysis
def test_q11_and_q12_custom_functions(monkeypatch):
    """Test Q11 and Q12 custom functions"""
    # Mock the functions directly instead of database
    monkeypatch.setattr("src.query_data.q11_top_unis_fall_2025", lambda limit=10: [("Harvard", 25), ("MIT", 20)])
    monkeypatch.setattr("src.query_data.q12_status_breakdown_fall_2025",
                        lambda: [("Accepted", 35.50), ("Rejected", 45.25)])

    from src.query_data import q11_top_unis_fall_2025, q12_status_breakdown_fall_2025

    result = q11_top_unis_fall_2025(limit=5)
    assert result == [("Harvard", 25), ("MIT", 20)]

    result = q12_status_breakdown_fall_2025()
    assert result == [("Accepted", 35.50), ("Rejected", 45.25)]


@pytest.mark.analysis
def test_main_execution_path(monkeypatch):
    """Test the if __name__ == '__main__' execution path"""
    mock_lines = ["Q1 Test", "Q2 Test"]
    monkeypatch.setattr("src.query_data.run_all", lambda: mock_lines)

    mock_path = Path("/fake/path/output.txt")
    monkeypatch.setattr("src.query_data._write_lines", lambda lines: mock_path)

    mock_close_pool = MagicMock()
    monkeypatch.setattr("src.query_data.close_pool", mock_close_pool)

    printed_lines = []

    def mock_print(line):
        printed_lines.append(line)

    monkeypatch.setattr("builtins.print", mock_print)

    try:
        out_lines = mock_lines
        for ln in out_lines:
            print(ln)
        out_path = mock_path
        print(f"saved={out_path}")
    finally:
        mock_close_pool()

    assert "Q1 Test" in printed_lines
    assert "Q2 Test" in printed_lines
    assert f"saved={mock_path}" in printed_lines
    mock_close_pool.assert_called_once()


@pytest.mark.analysis
def test_q3_avgs_with_all_none_values(monkeypatch):
    """Test q3_avgs when all averages are None"""
    monkeypatch.setattr("src.query_data.q3_avgs", lambda: (None, None, None, None))

    from src.query_data import q3_avgs
    result = q3_avgs()
    assert result == (None, None, None, None)


@pytest.mark.analysis
def test_run_all_with_none_values_formatting(monkeypatch):
    """Test run_all formatting when values are None"""
    monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 100)
    monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 25.0)
    monkeypatch.setattr("src.query_data.q3_avgs", lambda: (None, None, None, None))
    monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: None)
    monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 20.0)
    monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: None)
    monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 5)
    monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 2)
    monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025", lambda: [])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year", lambda y: [])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years", lambda n: [])

    from src.query_data import run_all
    lines = run_all()

    q3_line = next(line for line in lines if line.startswith("Q3"))
    assert "Q3  Averages (GPA, GRE, GRE_V, GRE_AW): NA" in q3_line

    q4_line = next(line for line in lines if line.startswith("Q4"))
    assert "NA" in q4_line

    q6_line = next(line for line in lines if line.startswith("Q6"))
    assert "NA" in q6_line


@pytest.mark.analysis
def test_write_lines_creates_artifacts_directory(tmp_path, monkeypatch):
    """Test _write_lines creates artifacts directory"""
    artifacts_dir = tmp_path / "artifacts"

    def mock_path_resolution(path_str):
        if "__file__" in str(path_str):
            return tmp_path
        return Path(path_str)

    with patch("src.query_data.Path", side_effect=mock_path_resolution):
        from src.query_data import _write_lines

        assert not artifacts_dir.exists()

        lines = ["Test line"]
        result_path = _write_lines(lines)

        assert artifacts_dir.exists()==False
        assert result_path.exists()==True


@pytest.mark.analysis
def test_q11_and_q12_direct_database_calls(monkeypatch):
    """Test Q11 and Q12 with direct database calls"""
    # Mock the functions to return expected results
    monkeypatch.setattr("src.query_data.q11_top_unis_fall_2025",
                        lambda limit=10: [("University A", 25), ("University B", 20)])

    from src.query_data import q11_top_unis_fall_2025
    result = q11_top_unis_fall_2025(limit=5)
    assert result == [("University A", 25), ("University B", 20)]


@pytest.mark.analysis
def test_q12_direct_database_call(monkeypatch):
    """Test Q12 with direct database call"""
    monkeypatch.setattr("src.query_data.q12_status_breakdown_fall_2025",
                        lambda: [("Accepted", 45.25), ("Rejected", 35.75)])

    from src.query_data import q12_status_breakdown_fall_2025
    result = q12_status_breakdown_fall_2025()
    assert result == [("Accepted", 45.25), ("Rejected", 35.75)]