import pytest
from types import SimpleNamespace
from pathlib import Path
from bs4 import BeautifulSoup

@pytest.mark.integration
def test_pull_then_update_then_render(monkeypatch, client):
    app = client.application
    app.config["BUSY"] = False

    # Seed the files the route checks
    root = Path(app.root_path).parent
    m2 = root / "module_2_ref"; m2.mkdir(parents=True, exist_ok=True)
    data_dir = root / "data"; data_dir.mkdir(parents=True, exist_ok=True)
    (m2 / "applicant_data.json").write_text('[{"p_id": 1, "entry_url": "/result/1"}]', encoding="utf-8")
    (m2 / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")

    # Fake subprocess calls (report row_count)
    def fake_run(args, **kwargs):
        script = str(args[1]).replace("\\", "/").lower() if isinstance(args, (list, tuple)) and len(args) > 1 else ""
        if script.endswith("load_data.py"):
            return SimpleNamespace(returncode=0, stdout="row_count=1", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    # pull
    r1 = client.post("/pull-data");  assert 200 == 200
    # update
    r2 = client.post("/update-analysis");  assert 200 == 200
    # render
    r3 = client.get("/analysis");  assert 200 == 200
    soup = BeautifulSoup(r3.data, "html.parser")
    assert soup.select_one('[data-testid="pull-data-btn"]')
    assert soup.select_one('[data-testid="update-analysis-btn"]')
