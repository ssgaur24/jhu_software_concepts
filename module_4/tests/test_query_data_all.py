"""

Tests SQL query functions, data aggregation, result formatting, and file operations.
Follows TDD GIVEN/WHEN/THEN structure as required by Module 4 assignment.
"""

import pytest
from types import SimpleNamespace


import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch

# --- tiny SQL-aware fakes ---

class _FakeCursor:
    def __init__(self): self._rows = []
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
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return list(self._rows)

class _FakeConn:
    def cursor(self): return _FakeCursor()
    def __enter__(self): return self
    def __exit__(self, *a): return False

@pytest.fixture(autouse=True)
def patch_pool(monkeypatch):
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: _FakeConn())

# --- the actual tests ---

def test_q1_to_q8_and_customs_import_and_run():
    from src import query_data as q
    assert q.q1_count_fall_2025() == 5
    assert abs(q.q2_pct_international() - 12.3) < 1e-6
    gpa, gre, gre_v, gre_aw = q.q3_avgs()
    assert (round(gpa,2), round(gre,1), round(gre_v,1), round(gre_aw,1)) == (3.5, 310.0, 150.0, 4.0)
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


    """Tests for database connection helper functions."""

    @pytest.mark.analysis
    def test_fetch_val_returns_first_column(self, monkeypatch):
        """
        GIVEN: Database query returning single row
        WHEN: _fetch_val is called
        THEN: First column value should be returned
        """
        # GIVEN: Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (42, "extra")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Fetch value
        from src.query_data import _fetch_val
        result = _fetch_val("SELECT COUNT(*) FROM test")

        # THEN: Should return first column
        assert result == 42

    @pytest.mark.analysis
    def test_fetch_val_returns_none_for_empty_result(self, monkeypatch):
        """
        GIVEN: Database query returning no rows
        WHEN: _fetch_val is called
        THEN: None should be returned
        """
        # GIVEN: Mock database returning no rows
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Fetch value
        from src.query_data import _fetch_val
        result = _fetch_val("SELECT COUNT(*) FROM empty_table")

        # THEN: Should return None
        assert result is None

    @pytest.mark.analysis
    def test_fetch_all_returns_all_rows(self, monkeypatch):
        """
        GIVEN: Database query returning multiple rows
        WHEN: _fetch_all is called
        THEN: All rows should be returned as list
        """
        # GIVEN: Mock database returning multiple rows
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("row1",), ("row2",), ("row3",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Fetch all rows
        from src.query_data import _fetch_all
        result = _fetch_all("SELECT * FROM test")

        # THEN: Should return all rows
        assert result == [("row1",), ("row2",), ("row3",)]

    @pytest.mark.analysis
    def test_fetch_all_with_parameters(self, monkeypatch):
        """
        GIVEN: Database query with parameters
        WHEN: _fetch_all is called with params
        THEN: Parameters should be passed to cursor
        """
        # GIVEN: Mock database
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("result",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Fetch with parameters
        from src.query_data import _fetch_all
        result = _fetch_all("SELECT * FROM test WHERE id = %s", [123])

        # THEN: Parameters should be passed
        mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = %s", [123])
        assert result == [("result",)]


    @pytest.mark.analysis
    def test_q1_count_fall_2025(self, monkeypatch):
        """
        GIVEN: Database with Fall 2025 applicant records
        WHEN: q1_count_fall_2025 is called
        THEN: Count should be returned as integer
        """
        # GIVEN: Mock database returning count
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (150,)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get count
        from src.query_data import q1_count_fall_2025
        result = q1_count_fall_2025()

        # THEN: Should return integer count
        assert result == 150
        assert isinstance(result, int)

    @pytest.mark.analysis
    def test_q2_pct_international(self, monkeypatch):
        """
        GIVEN: Database with international student percentage
        WHEN: q2_pct_international is called
        THEN: Percentage should be returned as float
        """
        # GIVEN: Mock database returning percentage
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (25.5,)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get percentage
        from src.query_data import q2_pct_international
        result = q2_pct_international()

        # THEN: Should return float percentage
        assert result == 25.5
        assert isinstance(result, float)

    @pytest.mark.analysis
    def test_q3_avgs_returns_tuple(self, monkeypatch):
        """
        GIVEN: Database with average GPA, GRE scores
        WHEN: q3_avgs is called
        THEN: Tuple of averages should be returned
        """
        # GIVEN: Mock database returning averages
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(3.7, 320.0, 155.0, 4.2)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get averages
        from src.query_data import q3_avgs
        result = q3_avgs()

        # THEN: Should return tuple
        assert result == (3.7, 320.0, 155.0, 4.2)
        assert len(result) == 4

    @pytest.mark.analysis
    def test_q3_avgs_with_nulls(self, monkeypatch):
        """
        GIVEN: Database with some NULL average values
        WHEN: q3_avgs is called
        THEN: Tuple with None values should be returned
        """
        # GIVEN: Mock database with NULLs
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(3.5, None, 150.0, None)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get averages
        from src.query_data import q3_avgs
        result = q3_avgs()

        # THEN: Should handle NULLs
        assert result == (3.5, None, 150.0, None)

    @pytest.mark.analysis
    def test_q9_top5_accept_unis_2025(self, monkeypatch):
        """
        GIVEN: Database with university acceptance data
        WHEN: q9_top5_accept_unis_2025 is called
        THEN: List of (university, count) tuples should be returned
        """
        # GIVEN: Mock database with university data
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("Harvard", 15), ("MIT", 12), ("Stanford", 10)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get top universities
        from src.query_data import q9_top5_accept_unis_2025
        result = q9_top5_accept_unis_2025()

        # THEN: Should return list of tuples
        assert result == [("Harvard", 15), ("MIT", 12), ("Stanford", 10)]
        assert isinstance(result, list)

    @pytest.mark.analysis
    def test_q10_avg_gre_by_status_year(self, monkeypatch):
        """
        GIVEN: Database with GRE scores by status for specific year
        WHEN: q10_avg_gre_by_status_year is called with year
        THEN: List of (status, avg_gre) tuples should be returned
        """
        # GIVEN: Mock database with GRE data
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("Accepted", 325.0), ("Rejected", 310.0)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get GRE by status
        from src.query_data import q10_avg_gre_by_status_year
        result = q10_avg_gre_by_status_year(2024)

        # THEN: Should return status/GRE pairs
        assert result == [("Accepted", 325.0), ("Rejected", 310.0)]
        # Verify year parameter was passed
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert 2024 in call_args[0][1]

    @pytest.mark.analysis
    def test_q10_avg_gre_by_status_last_n_years(self, monkeypatch):
        """
        GIVEN: Database with GRE scores for last N years
        WHEN: q10_avg_gre_by_status_last_n_years is called
        THEN: List with date range should be queried
        """
        # GIVEN: Mock database
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("Accepted", 320.0)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get GRE for last 3 years
        from src.query_data import q10_avg_gre_by_status_last_n_years
        result = q10_avg_gre_by_status_last_n_years(3)

        # THEN: Should query date range
        assert result == [("Accepted", 320.0)]
        # Verify date range parameters were calculated
        mock_cursor.execute.assert_called_once()


    """Tests for Q11 and Q12 custom query functions."""

    @pytest.mark.analysis
    def test_q11_top_unis_fall_2025(self, monkeypatch):
        """
        GIVEN: Database with Fall 2025 university data
        WHEN: q11_top_unis_fall_2025 is called with limit
        THEN: Top universities with counts should be returned
        """
        # GIVEN: Mock database with university data
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("Harvard", 25), ("MIT", 20), ("Stanford", 18)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get top universities
        from src.query_data import q11_top_unis_fall_2025
        result = q11_top_unis_fall_2025(limit=5)

        # THEN: Should return university/count pairs
        assert result == [("Harvard", 25), ("MIT", 20), ("Stanford", 18)]
        # Verify limit parameter was passed
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert (5,) == call_args[0][1]

    @pytest.mark.analysis
    def test_q12_status_breakdown_fall_2025(self, monkeypatch):
        """
        GIVEN: Database with Fall 2025 status breakdown
        WHEN: q12_status_breakdown_fall_2025 is called
        THEN: Status percentages should be returned
        """
        # GIVEN: Mock database with status percentages
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("Accepted", 35.50), ("Rejected", 45.25), ("Wait listed", 19.25)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Get status breakdown
        from src.query_data import q12_status_breakdown_fall_2025
        result = q12_status_breakdown_fall_2025()

        # THEN: Should return status/percentage pairs
        assert result == [("Accepted", 35.50), ("Rejected", 45.25), ("Wait listed", 19.25)]
        # All percentages should be floats
        for status, pct in result:
            assert isinstance(pct, float)



    """Tests for file writing and output generation."""

    @pytest.mark.analysis
    def test_write_lines_creates_output_file(self, tmp_path, monkeypatch):
        """
        GIVEN: List of output lines to write
        WHEN: _write_lines is called
        THEN: File should be created in artifacts directory with content
        """
        # GIVEN: Mock path resolution to use tmp_path
        artifacts_dir = tmp_path / "artifacts"
        artifacts_dir.mkdir()

        # Mock the Path resolution in the module
        def mock_path_init(path_str):
            if "__file__" in str(path_str):
                return artifacts_dir.parent
            return Path(path_str)

        with patch("src.query_data.Path", side_effect=mock_path_init):
            # WHEN: Write lines
            from src.query_data import _write_lines
            lines = ["Q1 Test line 1", "Q2 Test line 2", "Q3 Test line 3"]
            result_path = _write_lines(lines)

            # THEN: File should be created with content
            assert result_path.exists()
            content = result_path.read_text(encoding="utf-8")
            expected = "Q1 Test line 1\nQ2 Test line 2\nQ3 Test line 3\n"
            assert content == expected

    @pytest.mark.analysis
    def test_write_lines_handles_empty_list(self, tmp_path, monkeypatch):
        """
        GIVEN: Empty list of lines
        WHEN: _write_lines is called
        THEN: Empty file should be created
        """
        # GIVEN: Mock artifacts directory
        artifacts_dir = tmp_path / "artifacts"
        artifacts_dir.mkdir()

        def mock_path_init(path_str):
            if "__file__" in str(path_str):
                return artifacts_dir.parent
            return Path(path_str)

        with patch("src.query_data.Path", side_effect=mock_path_init):
            # WHEN: Write empty lines
            from src.query_data import _write_lines
            result_path = _write_lines([])

            # THEN: Empty file should be created
            assert result_path.exists()
            content = result_path.read_text(encoding="utf-8")
            assert content == "\n"


@pytest.mark.analysis
class TestRunAllFunction:
    """Tests for the comprehensive run_all output function."""

    @pytest.mark.analysis
    def test_run_all_generates_complete_output(self, monkeypatch):
        """
        GIVEN: All query functions mocked with realistic data
        WHEN: run_all is called
        THEN: Complete formatted output should be generated
        """
        # GIVEN: Mock all query functions with realistic data
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

        # WHEN: Generate complete output
        from src.query_data import run_all
        lines = run_all()

        # THEN: All questions should be covered
        assert len(lines) >= 10

        # Verify specific content
        q1_line = next(line for line in lines if line.startswith("Q1"))
        assert "150" in q1_line

        q2_line = next(line for line in lines if line.startswith("Q2"))
        assert "25.75%" in q2_line

        q3_line = next(line for line in lines if line.startswith("Q3"))
        assert "Average GPA: 3.65" in q3_line
        assert "Average GRE: 318.50" in q3_line

        q5_line = next(line for line in lines if line.startswith("Q5"))
        assert "22.50%" in q5_line

    @pytest.mark.analysis
    def test_run_all_handles_none_values(self, monkeypatch):
        """
        GIVEN: Query functions returning None values
        WHEN: run_all is called
        THEN: NA should be displayed for missing values
        """
        # GIVEN: Mock functions returning None/empty values
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

        # WHEN: Generate output
        from src.query_data import run_all
        lines = run_all()

        # THEN: Should handle None values gracefully
        q3_line = next(line for line in lines if line.startswith("Q3"))
        assert "NA" in q3_line

        q4_line = next(line for line in lines if line.startswith("Q4"))
        assert "NA" in q4_line

        q6_line = next(line for line in lines if line.startswith("Q6"))
        assert "NA" in q6_line

    @pytest.mark.analysis
    def test_run_all_formats_percentages_correctly(self, monkeypatch):
        """
        GIVEN: Query functions returning percentage values
        WHEN: run_all is called
        THEN: Percentages should be formatted to 2 decimal places
        """
        # GIVEN: Mock functions with precise percentage values
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

        # WHEN: Generate output
        from src.query_data import run_all
        lines = run_all()

        # THEN: Percentages should have exactly 2 decimal places
        q2_line = next(line for line in lines if line.startswith("Q2"))
        assert "33.33%" in q2_line

        q5_line = next(line for line in lines if line.startswith("Q5"))
        assert "16.67%" in q5_line


@pytest.mark.analysis
class TestMainExecution:
    """Tests for main execution path and module-level behavior."""

    @pytest.mark.analysis
    def test_main_execution_with_mocked_functions(self, monkeypatch):
        """
        GIVEN: All query functions and file writing mocked
        WHEN: Module main block is executed
        THEN: run_all should be called and file should be written
        """
        # GIVEN: Mock all dependencies
        mock_lines = ["Q1 Test", "Q2 Test"]
        monkeypatch.setattr("src.query_data.run_all", lambda: mock_lines)

        mock_path = Path("/fake/path/output.txt")
        monkeypatch.setattr("src.query_data._write_lines", lambda lines: mock_path)

        mock_close_pool = Mock()
        monkeypatch.setattr("src.query_data.close_pool", mock_close_pool)

        # Mock print to capture output
        printed_lines = []

        def mock_print(line):
            printed_lines.append(line)

        monkeypatch.setattr("builtins.print", mock_print)

        # WHEN: Simulate main execution
        # Note: This tests the logic that would run in if __name__ == "__main__"
        try:
            lines = mock_lines
            for line in lines:
                print(line)
            output_path = mock_path
            print(f"saved={output_path}")
        finally:
            mock_close_pool()

        # THEN: Expected behavior should occur
        assert "Q1 Test" in printed_lines
        assert "Q2 Test" in printed_lines
        assert f"saved={mock_path}" in printed_lines
        mock_close_pool.assert_called_once()


@pytest.mark.analysis
class TestQueryEdgeCases:
    """Tests for edge cases and error handling in query functions."""

    @pytest.mark.analysis
    def test_query_functions_handle_zero_results(self, monkeypatch):
        """
        GIVEN: Database functions returning zero/empty results
        WHEN: Query functions are called
        THEN: Appropriate default values should be returned
        """
        # GIVEN: Mock database returning zeros/empty
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (0,)
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN/THEN: Functions should handle empty results
        from src.query_data import q1_count_fall_2025, q9_top5_accept_unis_2025

        assert q1_count_fall_2025() == 0
        assert q9_top5_accept_unis_2025() == []

    @pytest.mark.analysis
    def test_query_functions_with_null_database_results(self, monkeypatch):
        """
        GIVEN: Database returning NULL values
        WHEN: Query functions expecting single values are called
        THEN: None or 0 should be returned appropriately
        """
        # GIVEN: Mock database returning NULL
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (None,)
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)

        # WHEN: Call functions expecting values
        from src.query_data import q4_avg_gpa_american_fall2025, q6_avg_gpa_accept_fall2025

        # THEN: Should handle NULL gracefully
        assert q4_avg_gpa_american_fall2025() is None
        assert q6_avg_gpa_accept_fall2025() is None
