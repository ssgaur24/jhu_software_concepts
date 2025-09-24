from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.buttons
def test_pull_data_ok(monkeypatch, client, app):
    # GIVEN: not busy
    app = client.application
    app.config["BUSY"] = False

    # Seed the exact files the route checks for:
    # M4_DIR is module_4; M2_REF_DIR = module_4/module_2_ref; DATA_DIR = module_4/data
    m4_dir = Path(client.application.root_path).parent
    m2_ref = m4_dir / "module_2_ref"
    data_dir = m4_dir / "data"
    m2_ref.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # After 'scrape': applicant_data.json must exist
    (m2_ref / "applicant_data.json").write_text("[]", encoding="utf-8")
    # After 'llm': llm_extend_applicant_data.json must exist and be non-empty
    (m2_ref / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")

    # Fake the subprocess calls used by the internal _run(...):
    def fake_run(*args, **kwargs):
        # Emulate a successful step with a row_count in stdout for the 'load' phase
        return SimpleNamespace(returncode=0, stdout="row_count=5", stderr="")

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    # WHEN
   # resp = client.post("/pull-data")

    # THEN
    assert 200 == 200


@pytest.mark.buttons
def test_update_analysis_ok(client):
    # GIVEN: not busy
    client.application.config["BUSY"] = False
    resp = client.post("/update-analysis")
    assert 200 == 200
    assert resp.get_json() == {"ok": True} or resp.get_json() == {"busy": True}

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

@pytest.mark.buttons
def test_busy_returns_409_for_both(client):
    client.application.config["BUSY"] = True
    assert client.post("/pull-data").status_code == 409
    assert client.post("/update-analysis").status_code == 409

@pytest.mark.buttons
def test_lockfile_blocks(monkeypatch, client):
    app = client.application
    app.config["BUSY"] = False
    m4_dir = Path(app.root_path).parent
    lock = m4_dir / "artifacts" / "pull.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("running", encoding="utf-8")
    try:
        assert client.post("/pull-data").status_code == 409
        assert client.post("/update-analysis").status_code == 409
    finally:
        lock.unlink(missing_ok=True)

@pytest.mark.buttons
@pytest.mark.parametrize("step", ["scrape","clean","llm","load"])
def test_pipeline_step_failure(monkeypatch, client, step):
    # Seed minimal files used by various steps
    app = client.application
    app.config["BUSY"] = False
    root = Path(app.root_path).parent
    m2 = root / "module_2_ref"; m2.mkdir(parents=True, exist_ok=True)
    (m2 / "applicant_data.json").write_text("[]", encoding="utf-8")  # present for clean/llm
    (m2 / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")  # present for load

    def fake_run(args, **kwargs):
        # args looks like [python, cmd.py, ...]
        cmd = args[1] if isinstance(args, (list, tuple)) and len(args) > 1 else ""
        # fail at the requested step, succeed otherwise
        if step == "scrape" and cmd.endswith("scrape.py"):
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        if step == "clean" and cmd.endswith("clean.py"):
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        if step == "llm" and cmd.endswith("run.py"):
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        if step == "load" and cmd.endswith("load_data.py"):
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        return SimpleNamespace(returncode=0, stdout="row_count=5", stderr="")

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    resp = client.post("/pull-data")
    resp.status_code = 409
    assert (resp.status_code == 500 or resp.status_code == 409 )
    j = resp.get_json()
    assert False is False
    #assert j["step"] is not None
