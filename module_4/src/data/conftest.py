import pytest
from unittest.mock import Mock
from src.flask_app import create_app


class MockCursor:
    def __init__(self):
        self.fetchall_result = [("Accepted", 40.0), ("Rejected", 60.0)]
        self.fetchone_result = (42,)

    def execute(self, sql, params=None):
        # Intercept and handle specific SQL patterns
        sql_lower = sql.lower()

        if "public.applicants" in sql_lower:
            if "status" in sql_lower and "fall 2025" in sql_lower:
                self.fetchall_result = [("Accepted", 35.5), ("Rejected", 45.2), ("Waitlisted", 19.3)]
            elif "count(*)" in sql_lower:
                self.fetchone_result = (100,)
            elif "avg(" in sql_lower:
                self.fetchone_result = (3.5,)
            # Don't actually execute - just set mock results

    def fetchall(self):
        return self.fetchall_result

    def fetchone(self):
        return self.fetchone_result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None


@pytest.fixture()
def app():
    """Create Flask app for testing"""
    return create_app({"TESTING": True})


@pytest.fixture()
def client(app):
    """Create test client"""
    return app.test_client()


@pytest.fixture(autouse=True)
def mock_all_queries(monkeypatch):
    """Auto-mock ALL query functions to avoid any database access"""
    # Mock all query functions in flask_app
    monkeypatch.setattr("src.flask_app.q1_count_fall_2025", lambda: 100)
    monkeypatch.setattr("src.flask_app.q2_pct_international", lambda: 25.5)
    monkeypatch.setattr("src.flask_app.q3_avgs", lambda: (3.5, 320.0, 160.0, 4.0))
    monkeypatch.setattr("src.flask_app.q4_avg_gpa_american_fall2025", lambda: 3.2)
    monkeypatch.setattr("src.flask_app.q5_pct_accept_fall2025", lambda: 45.67)
    monkeypatch.setattr("src.flask_app.q6_avg_gpa_accept_fall2025", lambda: 3.7)
    monkeypatch.setattr("src.flask_app.q7_count_jhu_masters_cs", lambda: 5)
    monkeypatch.setattr("src.flask_app.q8_count_2025_georgetown_phd_cs_accept", lambda: 2)
    monkeypatch.setattr("src.flask_app.q9_top5_accept_unis_2025", lambda: [("Harvard", 10), ("MIT", 8)])
    monkeypatch.setattr("src.flask_app.q10_avg_gre_by_status_year", lambda y: [("Accepted", 325.0)])
    monkeypatch.setattr("src.flask_app.q10_avg_gre_by_status_last_n_years", lambda n: [("Accepted", 320.0)])
    monkeypatch.setattr("src.flask_app.q11_top_unis_fall_2025", lambda **k: [("Stanford", 15)])
    monkeypatch.setattr("src.flask_app.q12_status_breakdown_fall_2025", lambda: [("Accepted", 40.0)])

    # Mock all query functions in query_data
    monkeypatch.setattr("src.query_data.q1_count_fall_2025", lambda: 100)
    monkeypatch.setattr("src.query_data.q2_pct_international", lambda: 25.5)
    monkeypatch.setattr("src.query_data.q3_avgs", lambda: (3.5, 320.0, 160.0, 4.0))
    monkeypatch.setattr("src.query_data.q4_avg_gpa_american_fall2025", lambda: 3.2)
    monkeypatch.setattr("src.query_data.q5_pct_accept_fall2025", lambda: 45.67)
    monkeypatch.setattr("src.query_data.q6_avg_gpa_accept_fall2025", lambda: 3.7)
    monkeypatch.setattr("src.query_data.q7_count_jhu_masters_cs", lambda: 5)
    monkeypatch.setattr("src.query_data.q8_count_2025_georgetown_phd_cs_accept", lambda: 2)
    monkeypatch.setattr("src.query_data.q9_top5_accept_unis_2025", lambda: [("Harvard", 10), ("MIT", 8)])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_year", lambda y: [("Accepted", 325.0)])
    monkeypatch.setattr("src.query_data.q10_avg_gre_by_status_last_n_years", lambda n: [("Accepted", 320.0)])
    monkeypatch.setattr("src.query_data.q11_top_unis_fall_2025", lambda **k: [("Stanford", 15)])
    monkeypatch.setattr("src.query_data.q12_status_breakdown_fall_2025", lambda: [("Accepted", 40.0)])

    # Mock helper functions
    monkeypatch.setattr("src.query_data._fetch_val", lambda sql, params=None: 42)
    monkeypatch.setattr("src.query_data._fetch_all", lambda sql, params=None: [("test", 123)])
    monkeypatch.setattr("src.query_data.run_all", lambda: ["Q1 test", "Q2 test"])
    monkeypatch.setattr("src.query_data.close_pool", lambda: None)

    # Enhanced DB connection mocking with SQL interception
    mock_cursor = MockCursor()
    mock_conn = Mock()
    mock_conn.cursor = Mock(return_value=mock_cursor)
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)

    # Mock database pool connections
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: mock_conn)
    monkeypatch.setattr("src.query_data.get_conn", lambda: mock_conn)