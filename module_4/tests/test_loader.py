"""
Comprehensive test suite for src/dal/loader.py to achieve 100% coverage.

Tests data loading functionality including mapping, validation, batching,
and error handling scenarios without hardcoded passwords.

Follows assignment requirements with proper pytest markers.
"""

import json
import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock as Mock, patch
import pytest
from src.dal.loader import (
    _to_float, _to_date, _stable_id, _compose_program, _map_record,
    _chunks, first_ids, load_json
)


# In-memory fake "DB" behavior with executemany support
class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self.rowcount = 0  # emulate DB API attr when inserts happen

    def execute(self, sql, params=None):
        """Handle single execute calls."""
        if isinstance(params, dict) and "p_id" in params:
            pid = params["p_id"]
            if pid is None:
                return
            # idempotent insert
            if pid not in self.store["ids"]:
                self.store["ids"].add(pid)
                self.store["rows"].append(params)
                self.rowcount += 1

    def executemany(self, sql, param_list):
        """Handle batch execute calls."""
        batch_inserted = 0
        for params in param_list:
            if isinstance(params, dict) and "p_id" in params:
                pid = params["p_id"]
                if pid is None:
                    continue
                # idempotent insert - only insert if not already present
                if pid not in self.store["ids"]:
                    self.store["ids"].add(pid)
                    self.store["rows"].append(params)
                    batch_inserted += 1
        self.rowcount = batch_inserted  # Set rowcount to number actually inserted

    def fetchall(self):
        """Not used by these tests; keep for safety."""
        return [(len(self.store["rows"]),)]


def _conn_ctx(store):
    """MagicMock connection whose .cursor() returns a context-managed cursor."""
    cur = _FakeCursor(store)
    cur_cm = Mock()
    cur_cm.__enter__.return_value = cur
    cur_cm.__exit__.return_value = False

    conn = Mock()
    conn.cursor.return_value = cur_cm
    conn.commit = Mock()  # Add commit method
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    return conn


@pytest.fixture()
def store():
    return {"ids": set(), "rows": []}


@pytest.fixture(autouse=True)
def patch_pool(monkeypatch, store):
    """Every call to get_conn() returns a fresh context-manageable connection."""
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: _conn_ctx(store))


# -----------------------------
# Tests for mapping utilities
# -----------------------------
@pytest.mark.db
def test_to_float_int_and_float_types():
    """Test _to_float with int and float types - covers line 45."""
    # GIVEN: Integer and float values
    # WHEN: Converting to float
    # THEN: Should return float versions
    assert _to_float(42) == 42.0  # This covers line 45 (int case)
    assert _to_float(3.14) == 3.14  # This covers line 45 (float case)


@pytest.mark.db
def test_to_float_various_inputs():
    """Test _to_float with various input types."""
    # Test None
    assert _to_float(None) is None

    # Test string numbers
    assert _to_float("3.5") == 3.5
    assert _to_float(" 3.5 ") == 3.5

    # Test empty/whitespace strings
    assert _to_float("") is None
    assert _to_float("   ") is None

    # Test invalid strings
    assert _to_float("not_a_number") is None


@pytest.mark.db
def test_stable_id_from_p_id_field():
    """Test _stable_id with p_id field - covers line 75."""
    # GIVEN: Record with valid p_id
    rec = {"p_id": 123}

    # WHEN: Extracting stable ID
    result = _stable_id(rec)

    # THEN: Should return the p_id as int (covers line 75)
    assert result == 123


@pytest.mark.db
def test_stable_id_various_scenarios():
    """Test _stable_id with different input scenarios."""
    # Test with float p_id
    assert _stable_id({"p_id": 123.0}) == 123

    # Test with zero p_id (invalid)
    assert _stable_id({"p_id": 0}) is None

    # Test with negative p_id (invalid)
    assert _stable_id({"p_id": -1}) is None

    # Test with string p_id (invalid type)
    assert _stable_id({"p_id": "not_a_number"}) is None

    # Test with entry_url
    assert _stable_id({"entry_url": "/result/456"}) == 456

    # Test with url field
    assert _stable_id({"url": "/result/789"}) == 789

    # Test with invalid URL
    assert _stable_id({"entry_url": "/invalid/path"}) is None

    # Test with no valid fields
    assert _stable_id({}) is None


@pytest.mark.db
def test_to_date_various_formats():
    """Test _to_date with multiple date formats."""
    # Test standard formats
    assert _to_date("2025-03-15") == dt.date(2025, 3, 15)
    assert _to_date("03/15/2025") == dt.date(2025, 3, 15)
    assert _to_date("2025/03/15") == dt.date(2025, 3, 15)
    assert _to_date("March 15, 2025") == dt.date(2025, 3, 15)
    assert _to_date("Mar 15, 2025") == dt.date(2025, 3, 15)

    # Test invalid dates
    assert _to_date(None) is None
    assert _to_date("") is None
    assert _to_date("invalid_date") is None


@pytest.mark.db
def test_compose_program():
    """Test _compose_program function."""
    # Test with both university and program
    assert _compose_program("Harvard", "CS") == "Harvard - CS"

    # Test with empty university
    assert _compose_program("", "CS") == "CS"

    # Test with empty program
    assert _compose_program("Harvard", "") == "Harvard"

    # Test with whitespace
    assert _compose_program("  Harvard  ", "  CS  ") == "Harvard - CS"

    # Test with both empty
    assert _compose_program("", "") == ""


@pytest.mark.db
def test_map_record_complete():
    """Test _map_record with complete record."""
    # GIVEN: Complete record
    rec = {
        "p_id": 123,
        "university": "Harvard",
        "program": "Computer Science",
        "status": "Accepted",
        "GPA": "3.8",
        "GRE": "330",
        "GRE V": "160",
        "GRE AW": "4.5",
        "comments": "Great program",
        "date_added": "2025-01-15",
        "entry_url": "/result/123",
        "start_term": "Fall 2025",
        "US/International": "American",
        "degree": "Bachelor",
        "llm_generated_program": "MS Computer Science",
        "llm_generated_university": "Harvard University"
    }

    # WHEN: Mapping record
    mapped = _map_record(rec)

    # THEN: Should map all fields correctly
    assert mapped["p_id"] == 123
    assert mapped["program"] == "Harvard - Computer Science"
    assert mapped["status"] == "Accepted"
    assert mapped["gpa"] == 3.8
    assert mapped["gre"] == 330.0
    assert mapped["gre_v"] == 160.0
    assert mapped["gre_aw"] == 4.5
    assert mapped["comments"] == "Great program"
    assert mapped["date_added"] == dt.date(2025, 1, 15)
    assert mapped["url"] == "/result/123"
    assert mapped["term"] == "Fall 2025"
    assert mapped["us_or_international"] == "American"
    assert mapped["degree"] == "Bachelor"
    assert mapped["llm_generated_program"] == "MS Computer Science"
    assert mapped["llm_generated_university"] == "Harvard University"


@pytest.mark.db
def test_map_record_alternative_field_names():
    """Test _map_record with alternative field names."""
    # Test with alternative field names
    rec = {
        "p_id": 124,
        "gpa": "3.7",  # lowercase gpa
        "gre": "325",  # lowercase gre
        "gre_v": "155",  # underscore version
        "gre_aw": "4.0",  # underscore version
        "term": "Spring 2025",  # direct term field
        "us_or_international": "International",  # underscore version
        "url": "/result/124",  # url instead of entry_url
        "acceptance_date": "2025-02-01",  # alternative date field
        "rejection_date": "2025-03-01",  # should prefer acceptance_date
        "Degree": "Master",  # title case Degree
        "llm-generated-program": "PhD Physics",  # hyphen version
        "llm-generated-university": "MIT",  # hyphen version
        "standardized_program": "Backup Program"  # should be overridden
    }

    mapped = _map_record(rec)

    assert mapped["gpa"] == 3.7
    assert mapped["gre"] == 325.0
    assert mapped["gre_v"] == 155.0
    assert mapped["gre_aw"] == 4.0
    assert mapped["term"] == "Spring 2025"
    assert mapped["us_or_international"] == "International"
    assert mapped["url"] == "/result/124"
    assert mapped["date_added"] == dt.date(2025, 2, 1)  # Should prefer acceptance_date
    assert mapped["degree"] == "Master"
    assert mapped["llm_generated_program"] == "PhD Physics"
    assert mapped["llm_generated_university"] == "MIT"


@pytest.mark.db
def test_map_record_missing_stable_id():
    """Test _map_record raises error when no stable ID found."""
    # GIVEN: Record without valid p_id or entry_url
    rec = {"program": "CS"}

    # WHEN/THEN: Should raise ValueError
    with pytest.raises(ValueError, match="missing_stable_id"):
        _map_record(rec)


@pytest.mark.db
def test_chunks():
    """Test _chunks utility function."""
    # GIVEN: List to chunk
    lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # WHEN: Chunking with size 3
    chunks = list(_chunks(lst, 3))

    # THEN: Should create correct chunks
    assert len(chunks) == 4
    assert chunks[0] == [1, 2, 3]
    assert chunks[1] == [4, 5, 6]
    assert chunks[2] == [7, 8, 9]
    assert chunks[3] == [10]


@pytest.mark.db
def test_first_ids_with_exception_handling(tmp_path):
    """Test first_ids handles exceptions gracefully - covers line 155."""
    # GIVEN: JSON with problematic records that cause exceptions
    test_data = [
        {"p_id": 1, "program": "CS"},
        {"malformed": "data"},  # This might cause exception in _stable_id
        {"p_id": 2, "program": "Math"},
        None,  # This will definitely cause exception
        {"p_id": 3, "program": "Physics"}
    ]

    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    # WHEN: Getting first IDs with k=5
    ids = first_ids(str(test_file), k=5)

    # THEN: Should continue after exceptions and return valid IDs (covers line 155)
    assert ids == [1, 2, 3]  # Should skip problematic records


@pytest.mark.db
def test_first_ids_normal_case(tmp_path):
    """Test first_ids with normal records."""
    # GIVEN: Normal JSON data
    test_data = [
        {"entry_url": "/result/11"},
        {"entry_url": "/result/12"},
        {"entry_url": "/result/13"},
        {"entry_url": "/result/14"},
        {"entry_url": "/result/15"}
    ]

    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    # WHEN: Getting first 3 IDs
    ids = first_ids(str(test_file), k=3)

    # THEN: Should return first 3 IDs
    assert ids == [11, 12, 13]


@pytest.mark.db
def test_load_json_complete_workflow(store):
    """Test load_json with working mocks."""
    test_data = [
        {"p_id": 21, "program": "CS"},
        {"p_id": 22, "program": "Math"},
        {"program": "Physics"}  # No p_id, will be skipped
    ]

    # Mock cursor that returns rowcount
    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_cursor.executemany = Mock()

    # Mock connection context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
    mock_conn.commit = Mock()

    with patch("src.dal.loader.json.loads", return_value=test_data), \
            patch("src.dal.loader.get_conn", return_value=mock_conn), \
            patch("src.dal.loader.first_ids", return_value=[21, 22]), \
            patch("pathlib.Path.read_text", return_value=json.dumps(test_data)), \
            patch("pathlib.Path.mkdir"), \
            patch("pathlib.Path.write_text") as mock_write, \
            patch("pathlib.Path.resolve") as mock_resolve:
        # Mock the artifacts path resolution
        mock_resolve.return_value.parent.parent.__truediv__.return_value.mkdir = Mock()
        mock_resolve.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = Mock()

        # WHEN
        total, inserted, skipped, issues, report_path = load_json("fake.json", batch=2)

        # THEN
        assert total == 3
        assert inserted == 2  # From mock_cursor.rowcount
        assert skipped == 1

@pytest.mark.db
def test_load_json_issue_counting(store):
    """Test load_json issue counting functionality."""
    test_data = [
        {
            "p_id": 31,
            "GPA": "invalid_gpa",  # Should trigger gpa_non_numeric
            "GRE": "invalid_gre",  # Should trigger gre_non_numeric
            "GRE V": "invalid_v",  # Should trigger gre_v_non_numeric
            "GRE AW": "invalid_aw",  # Should trigger gre_aw_non_numeric
            "date_added": "invalid_date"  # Should trigger date_parse_fail
        },
        {
            "p_id": 32,
            "GPA": "",  # Empty string, should not trigger non_numeric count
            "acceptance_date": "2025-01-15"  # Valid date
        }
    ]

    # Mock cursor that returns rowcount
    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_cursor.executemany = Mock()

    # Mock connection context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
    mock_conn.commit = Mock()

    with patch("src.dal.loader.json.loads", return_value=test_data), \
            patch("src.dal.loader.get_conn", return_value=mock_conn), \
            patch("src.dal.loader.first_ids", return_value=[31, 32]), \
            patch("pathlib.Path.read_text", return_value=json.dumps(test_data)), \
            patch("pathlib.Path.mkdir"), \
            patch("pathlib.Path.write_text"), \
            patch("pathlib.Path.resolve") as mock_resolve:
        # Mock the artifacts path resolution
        mock_resolve.return_value.parent.parent.__truediv__.return_value.mkdir = Mock()
        mock_resolve.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = Mock()

        # WHEN
        total, inserted, skipped, issues, report_path = load_json("fake.json")

        # THEN
        assert total == 2
        assert inserted == 2
        assert skipped == 0

        # Verify issue counting
        assert issues["gpa_non_numeric"] == 1
        assert issues["gre_non_numeric"] == 1
        assert issues["gre_v_non_numeric"] == 1
        assert issues["gre_aw_non_numeric"] == 1
        assert issues["date_parse_fail"] == 1
        assert issues["missing_p_id"] == 0


@pytest.mark.db
def test_load_json_batching_behavior(store):
    """Test load_json batching with different batch sizes."""
    test_data = [{"p_id": i, "program": "CS"} for i in range(51, 56)]

    # Mock cursor that returns rowcount
    mock_cursor = Mock()
    mock_cursor.rowcount = 5
    mock_cursor.executemany = Mock()

    # Mock connection context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
    mock_conn.commit = Mock()

    with patch("src.dal.loader.json.loads", return_value=test_data), \
            patch("src.dal.loader.get_conn", return_value=mock_conn), \
            patch("src.dal.loader.first_ids", return_value=[51, 52, 53]), \
            patch("pathlib.Path.read_text", return_value=json.dumps(test_data)), \
            patch("pathlib.Path.mkdir"), \
            patch("pathlib.Path.write_text"), \
            patch("pathlib.Path.resolve") as mock_resolve:
        # Mock the artifacts path resolution
        mock_resolve.return_value.parent.parent.__truediv__.return_value.mkdir = Mock()
        mock_resolve.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = Mock()

        # WHEN
        total, inserted, skipped, issues, report_path = load_json("fake.json", batch=2)

        # THEN
        assert total == 5
        assert inserted == 15  # From mock_cursor.rowcount
        assert skipped == 0


@pytest.mark.db
def test_load_json_cursor_rowcount_none():
    """Test load_json handles cursor.rowcount being None - covers line 207."""
    # GIVEN: Mock cursor that returns None for rowcount
    class MockCursorWithNoneRowcount:
        def __init__(self):
            self.rowcount = None  # Simulate rowcount being None

        def executemany(self, sql, params):
            pass  # Do nothing, keep rowcount as None

    mock_cursor = MockCursorWithNoneRowcount()
    mock_cursor_cm = Mock()
    mock_cursor_cm.__enter__.return_value = mock_cursor
    mock_cursor_cm.__exit__.return_value = False

    mock_conn = Mock()
    mock_conn.cursor.return_value = mock_cursor_cm
    mock_conn.commit = Mock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = False

    # Create test data
    test_data = [{"p_id": 41, "program": "CS"}]

    # Mock Path operations
    with patch("src.dal.loader.Path") as mock_path_class:
        mock_path = Mock()
        mock_path.read_text.return_value = json.dumps(test_data)
        mock_path_class.return_value = mock_path

        # Mock artifacts directory
        mock_art_dir = Mock()
        mock_report_path = Mock()
        mock_report_path.exists.return_value = True
        mock_art_dir.__truediv__.return_value = mock_report_path

        with patch("src.dal.loader.Path.resolve") as mock_resolve, \
             patch("src.dal.loader.get_conn", return_value=mock_conn):
            mock_parent = Mock()
            mock_parent.__truediv__.return_value = mock_art_dir
            mock_resolve.return_value.parent.parent = mock_parent

            # WHEN: Loading with mocked connection
            total, inserted, skipped, issues, report_path = load_json("fake_path.json")

            # THEN: Should handle None rowcount gracefully (covers line 207)
            assert total == 1
            assert inserted == 0  # Should be 0 when rowcount is None


@pytest.mark.db
def test_map_record_edge_cases():
    """Test _map_record with edge cases and empty values."""
    # GIVEN: Record with various empty/None values
    rec = {
        "p_id": 99,
        "comments": "",  # Empty string should become None
        "degree": "",  # Empty degree should become None
        "llm_generated_program": "",  # Empty should become None
        "llm_generated_university": "",  # Empty should become None
    }

    # WHEN: Mapping record
    mapped = _map_record(rec)

    # THEN: Empty strings should be converted to None
    assert mapped["p_id"] == 99
    assert mapped["comments"] is None
    assert mapped["degree"] is None
    assert mapped["llm_generated_program"] is None
    assert mapped["llm_generated_university"] is None