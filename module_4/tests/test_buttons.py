from types import SimpleNamespace

import pytest


@pytest.mark.buttons
def test_pull_data_ok(monkeypatch, client):
    # GIVEN: not busy
    app = client.application
    app.config["BUSY"] = False

    # Fake the inner dependency used by _run(...) inside flask_app:
    # return an object with returncode/stdout/stderr like subprocess.run would.
    def fake_run(*args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="row_count=5", stderr="")

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    # WHEN
    resp = client.post("/pull-data")

    # THEN
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["row_count"] == 5

@pytest.mark.buttons
def test_update_analysis_ok(client):
    # GIVEN: not busy
    client.application.config["BUSY"] = False
    resp = client.post("/update-analysis")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

@pytest.mark.buttons
def test_busy_gating(client):
    # GIVEN: app busy
    client.application.config["BUSY"] = True

    # WHEN pull is attempted
    r1 = client.post("/pull-data")
    # THEN: 409 busy
    assert r1.status_code == 409
    assert r1.get_json()["busy"] is True

    # WHEN update is attempted
    r2 = client.post("/update-analysis")
    assert r2.status_code == 409
    assert r2.get_json()["busy"] is True
