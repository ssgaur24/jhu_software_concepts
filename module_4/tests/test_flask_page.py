import re
import pytest
from src.flask_app import create_app  # your factory

@pytest.fixture
def app():
    # TESTING=True triggers your fast-paths
    return create_app({"TESTING": True, "BUSY": False})

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.mark.web
def test_analysis_page_loads_and_has_requirements(client):
    r = client.get("/analysis")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "Analysis" in body or "Answer:" in body
    # two-decimal percentage appears in demo row
    assert re.search(r"\b\d{1,3}\.\d{2}%\b", body)

@pytest.mark.buttons
def test_pull_data_busy_returns_409():
    app = create_app({"TESTING": True, "BUSY": True})
    c = app.test_client()
    r = c.post("/pull-data")
    assert r.status_code == 409
    assert r.get_json().get("busy") in (True, 1, "true")

@pytest.mark.buttons
def test_pull_data_ok_fast_path(client):
    r = client.post("/pull-data")
    assert r.status_code == 200
    assert r.get_json().get("ok") is True

@pytest.mark.buttons
def test_update_analysis_ok(client):
    r = client.post("/update-analysis")
    assert r.status_code == 200
    assert r.get_json()["ok"] is True

@pytest.mark.web
def test_health_probe(client):
    assert client.get("/health").status_code == 200
