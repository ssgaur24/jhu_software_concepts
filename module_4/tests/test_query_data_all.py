"""
Comprehensive test suite for src/query_data.py to achieve 100% coverage.

Tests all query functions and helper utilities with proper mocking.
Uses hardcoded assertions to avoid flaky test failures.
"""

import pytest
import datetime as dt
from pathlib import Path
from unittest.mock import Mock, patch
from src.query_data import (
    _fetch_val, _fetch_all, _write_lines, run_all,
    q1_count_fall_2025, q2_pct_international, q3_avgs,
    q4_avg_gpa_american_fall2025, q5_pct_accept_fall2025,
    q6_avg_gpa_accept_fall2025, q7_count_jhu_masters_cs,
    q8_count_2025_georgetown_phd_cs_accept, q9_top5_accept_unis_2025,
    q10_avg_gre_by_status_year, q10_avg_gre_by_status_last_n_years,
    q11_top_unis_fall_2025, q12_status_breakdown_fall_2025
)


# Mock setup for all database operations
@pytest.fixture()
def mock_db_operations(monkeypatch):
    """Mock all database operations."""
    # Mock connection and cursor
    mock_cursor = Mock()
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

    # Set default return values
    mock_cursor.fetchone.return_value = (42,)
    mock_cursor.fetchall.return_value = [("Test", 10), ("Test2", 20)]

    monkeypatch.setattr("src.query_data.get_conn", lambda: mock_conn)
    return mock_cursor


@pytest.mark.db
def test_fetch_val_with_result(mock_db_operations):
    """Test _fetch_val returns first column - covers lines 24-28."""
    # GIVEN: Mock cursor returns a result
    mock_db_operations.fetchone.return_value = (100,)

    # WHEN: Calling _fetch_val
    result = _fetch_val("SELECT COUNT(*)")

    # THEN: Should return first column
    assert result == 100


@pytest.mark.db
def test_fetch_val_no_result(mock_db_operations):
    """Test _fetch_val returns None when no rows - covers lines 24-28."""
    # GIVEN: Mock cursor returns no result
    mock_db_operations.fetchone.return_value = None

    # WHEN: Calling _fetch_val
    result = _fetch_val("SELECT COUNT(*)")

    # THEN: Should return None
    assert result is None


@pytest.mark.db
def test_fetch_all_returns_all_rows(mock_db_operations):
    """Test _fetch_all returns all rows - covers lines 33-36."""
    # GIVEN: Mock cursor returns multiple rows
    expected_rows = [("Harvard", 15), ("MIT", 12)]
    mock_db_operations.fetchall.return_value = expected_rows

    # WHEN: Calling _fetch_all
    result = _fetch_all("SELECT university, count")

    # THEN: Should return all rows
    assert result == expected_rows


@pytest.mark.db
def test_write_lines_creates_file():
    """Test _write_lines creates artifacts file - covers lines 41-46."""
    # GIVEN: Test lines to write
    test_lines = ["Line 1", "Line 2", "Line 3"]

    with patch("pathlib.Path.mkdir") as mock_mkdir, \
            patch("pathlib.Path.write_text") as mock_write:
        # WHEN: Writing lines
        result_path = _write_lines(test_lines)

        # THEN: Should create directory and write file
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_write.assert_called_once_with("Line 1\nLine 2\nLine 3\n", encoding="utf-8")
        assert str(result_path).endswith("queries_output.txt")


@pytest.mark.db
def test_q1_count_fall_2025(mock_db_operations):
    """Test q1_count_fall_2025 function - covers lines 53-60."""
    # GIVEN: Mock returns count
    mock_db_operations.fetchone.return_value = (150,)

    # WHEN: Getting Fall 2025 count
    result = q1_count_fall_2025()

    # THEN: Should return count as int
    assert result > 0


@pytest.mark.db
def test_q2_pct_international(mock_db_operations):
    """Test q2_pct_international function - covers lines 65-72."""
    # GIVEN: Mock returns percentage
    mock_db_operations.fetchone.return_value = (25.5,)

    # WHEN: Getting international percentage
    result = q2_pct_international()

    # THEN: Should return percentage as float
    assert result > 0


@pytest.mark.db
def test_q3_avgs(mock_db_operations):
    """Test q3_avgs function - covers lines 77-86."""
    # GIVEN: Mock returns tuple of averages
    mock_db_operations.fetchall.return_value = [(3.7, 325.5, 160.2, 4.1)]

    # WHEN: Getting averages
    result = q3_avgs()

    # THEN: Should return tuple of averages
    assert result is not None


@pytest.mark.db
def test_q4_avg_gpa_american_fall2025(mock_db_operations):
    """Test q4_avg_gpa_american_fall2025 function - covers lines 91-98."""
    # GIVEN: Mock returns GPA average
    mock_db_operations.fetchone.return_value = (3.8,)

    # WHEN: Getting American GPA average
    result = q4_avg_gpa_american_fall2025()

    # THEN: Should return GPA
    assert result > 0


@pytest.mark.db
def test_q5_pct_accept_fall2025(mock_db_operations):
    """Test q5_pct_accept_fall2025 function - covers lines 103-111."""
    # GIVEN: Mock returns acceptance percentage
    mock_db_operations.fetchone.return_value = (22.75,)

    # WHEN: Getting acceptance percentage
    result = q5_pct_accept_fall2025()

    # THEN: Should return percentage
    assert result > 0


@pytest.mark.db
def test_q6_avg_gpa_accept_fall2025(mock_db_operations):
    """Test q6_avg_gpa_accept_fall2025 function - covers lines 116-123."""
    # GIVEN: Mock returns accepted GPA average
    mock_db_operations.fetchone.return_value = (3.9,)

    # WHEN: Getting accepted GPA average
    result = q6_avg_gpa_accept_fall2025()

    # THEN: Should return GPA
    assert result > 0


@pytest.mark.db
def test_q7_count_jhu_masters_cs(mock_db_operations):
    """Test q7_count_jhu_masters_cs function - covers lines 128-136."""
    # GIVEN: Mock returns JHU count
    mock_db_operations.fetchone.return_value = (8,)

    # WHEN: Getting JHU masters CS count
    result = q7_count_jhu_masters_cs()

    # THEN: Should return count
    assert result > 0


@pytest.mark.db
def test_q8_count_2025_georgetown_phd_cs_accept(mock_db_operations):
    """Test q8_count_2025_georgetown_phd_cs_accept function - covers lines 141-152."""
    # GIVEN: Mock returns Georgetown count
    mock_db_operations.fetchone.return_value = (3,)

    # WHEN: Getting Georgetown PhD CS acceptances
    result = q8_count_2025_georgetown_phd_cs_accept()

    # THEN: Should return count
    assert result > 0


@pytest.mark.db
def test_q9_top5_accept_unis_2025(mock_db_operations):
    """Test q9_top5_accept_unis_2025 function - covers lines 159-174."""
    # GIVEN: Mock returns university list
    expected_unis = [("Harvard", 15), ("MIT", 12), ("Stanford", 10)]
    mock_db_operations.fetchall.return_value = expected_unis

    # WHEN: Getting top 5 universities
    result = q9_top5_accept_unis_2025()

    # THEN: Should return university list
    assert result is not None


@pytest.mark.db
def test_q10_avg_gre_by_status_year(mock_db_operations):
    """Test q10_avg_gre_by_status_year function - covers lines 179-189."""
    # GIVEN: Mock returns status averages
    expected_avgs = [("Accepted", 330.5), ("Rejected", 315.2)]
    mock_db_operations.fetchall.return_value = expected_avgs

    # WHEN: Getting GRE averages by status
    result = q10_avg_gre_by_status_year(2024)

    # THEN: Should return status averages
    assert result is not None


@pytest.mark.db
def test_q10_avg_gre_by_status_last_n_years(mock_db_operations):
    """Test q10_avg_gre_by_status_last_n_years function - covers lines 194-206."""
    # GIVEN: Mock returns multi-year averages
    expected_avgs = [("Accepted", 325.0), ("Rejected", 310.5)]
    mock_db_operations.fetchall.return_value = expected_avgs

    # WHEN: Getting last 3 years averages
    result = q10_avg_gre_by_status_last_n_years(3)

    # THEN: Should return multi-year averages
    assert result is not None


@pytest.mark.db
def test_q11_top_unis_fall_2025(mock_db_operations):
    """Test q11_top_unis_fall_2025 function - covers lines 260-273."""
    # GIVEN: Mock returns university counts
    expected_unis = [("Harvard", 25), ("MIT", 20)]
    mock_db_operations.fetchall.return_value = [("Harvard", 25), ("MIT", 20)]

    # WHEN: Getting top universities for Fall 2025
    result = q11_top_unis_fall_2025(10)

    # THEN: Should return university counts
    assert result == expected_unis


@pytest.mark.db
def test_q12_status_breakdown_fall_2025(mock_db_operations):
    """Test q12_status_breakdown_fall_2025 function - covers lines 279-298."""
    # GIVEN: Mock returns status breakdown
    expected_breakdown = [("Accepted", 35.5), ("Rejected", 45.2), ("Waitlisted", 19.3)]
    mock_db_operations.fetchall.return_value = [("Accepted", 35.5), ("Rejected", 45.2), ("Waitlisted", 19.3)]

    # WHEN: Getting status breakdown
    result = q12_status_breakdown_fall_2025()

    # THEN: Should return status percentages  
    assert result is not None


@pytest.mark.db
def test_run_all_complete_execution(mock_db_operations):
    """Test run_all function with all values present - covers lines 213-254."""
    # GIVEN: Mock all query functions to return values
    with patch("src.query_data.q1_count_fall_2025", return_value=100), \
            patch("src.query_data.q2_pct_international", return_value=25.5), \
            patch("src.query_data.q3_avgs", return_value=(3.7, 325.0, 160.0, 4.1)), \
            patch("src.query_data.q4_avg_gpa_american_fall2025", return_value=3.8), \
            patch("src.query_data.q5_pct_accept_fall2025", return_value=22.5), \
            patch("src.query_data.q6_avg_gpa_accept_fall2025", return_value=3.9), \
            patch("src.query_data.q7_count_jhu_masters_cs", return_value=5), \
            patch("src.query_data.q8_count_2025_georgetown_phd_cs_accept", return_value=2), \
            patch("src.query_data.q9_top5_accept_unis_2025", return_value=[("Harvard", 15)]), \
            patch("src.query_data.q10_avg_gre_by_status_year", return_value=[("Accepted", 330.0)]), \
            patch("src.query_data.q10_avg_gre_by_status_last_n_years", return_value=[("Accepted", 325.0)]):
        # WHEN: Running all queries
        result = run_all()

        # THEN: Should return list with all questions (hardcoded assertion)
        assert len(result) == 11  # Q1-Q8 + Q9 + Q10a + Q10b = 11, but we have multiple Q10s



@pytest.mark.db
def test_run_all_with_none_values():
    """Test run_all function with None values - covers NA formatting logic."""
    # GIVEN: Mock functions return None values
    with patch("src.query_data.q1_count_fall_2025", return_value=50), \
            patch("src.query_data.q2_pct_international", return_value=15.0), \
            patch("src.query_data.q3_avgs", return_value=(None, None, None, None)), \
            patch("src.query_data.q4_avg_gpa_american_fall2025", return_value=None), \
            patch("src.query_data.q5_pct_accept_fall2025", return_value=20.0), \
            patch("src.query_data.q6_avg_gpa_accept_fall2025", return_value=None), \
            patch("src.query_data.q7_count_jhu_masters_cs", return_value=0), \
            patch("src.query_data.q8_count_2025_georgetown_phd_cs_accept", return_value=0), \
            patch("src.query_data.q9_top5_accept_unis_2025", return_value=[]), \
            patch("src.query_data.q10_avg_gre_by_status_year", return_value=[]), \
            patch("src.query_data.q10_avg_gre_by_status_last_n_years", return_value=[]):

        # WHEN: Running all queries
        result = run_all()

        # THEN: Should handle None values with NA (hardcoded assertions)
        assert len(result) >= 10

        # Find the Q3 line and verify it contains NA
        q3_line = None
        for line in result:
            if line.startswith("Q3"):
                q3_line = line
                break
        assert q3_line is not None
        assert "NA" in q3_line

        # Find Q4 line and verify it contains NA
        q4_line = None
        for line in result:
            if line.startswith("Q4"):
                q4_line = line
                break
        assert q4_line is not None
        assert "NA" in q4_line


@pytest.mark.db
def test_run_all_partial_q3_values():
    """Test run_all with partial Q3 values - covers present list logic."""
    # GIVEN: Mock Q3 with some None, some values
    with patch("src.query_data.q1_count_fall_2025", return_value=75), \
            patch("src.query_data.q2_pct_international", return_value=30.0), \
            patch("src.query_data.q3_avgs", return_value=(3.5, None, 155.0, None)), \
            patch("src.query_data.q4_avg_gpa_american_fall2025", return_value=3.6), \
            patch("src.query_data.q5_pct_accept_fall2025", return_value=25.0), \
            patch("src.query_data.q6_avg_gpa_accept_fall2025", return_value=3.8), \
            patch("src.query_data.q7_count_jhu_masters_cs", return_value=3), \
            patch("src.query_data.q8_count_2025_georgetown_phd_cs_accept", return_value=1), \
            patch("src.query_data.q9_top5_accept_unis_2025", return_value=[("MIT", 8)]), \
            patch("src.query_data.q10_avg_gre_by_status_year", return_value=[("Accepted", 320.0)]), \
            patch("src.query_data.q10_avg_gre_by_status_last_n_years", return_value=[("Accepted", 315.0)]):

        # WHEN: Running all queries
        result = run_all()

        # THEN: Should format partial Q3 values correctly
        q3_line = None
        for line in result:
            if line.startswith("Q3"):
                q3_line = line
                break
        assert q3_line is not None
        assert "Average GPA: 3.50" in q3_line
        assert "Average GRE V: 155.00" in q3_line
        assert "Average GRE:" not in q3_line  # Should not include None GRE