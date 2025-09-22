import pytest
from types import SimpleNamespace
from pathlib import Path

class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self._rows = []

    def execute(self, sql, params=None):
        sql_low = (sql or "").lower()
        # minimal behavior: count/insert/select on public.applicants (Module 3 schema)
        if sql_low.startswith("select p_id from public.applicants"):
            self._rows = [(r["p_id"],) for r in self.store["rows"]]
        elif "insert into public.applicants" in sql_low:
            # expect params to include unique p_id – enforce idempotency
            p_id = params[0] if isinstance(params, (list, tuple)) else params.get("p_id", None)
            if p_id is None:
                return
            if all(r["p_id"] != p_id for r in self.store["rows"]):
                self.store["rows"].append({"p_id": p_id})
        elif sql_low.startswith("select count(") or "count(*)" in sql_low:
            self._rows = [(len(self.store["rows"]),)]
        else:
            # allow no-op for any other statement used by query_data helpers
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def close(self):  # compatibility
        pass

class _FakeConn:
    def __init__(self, store):
        self.store = store
    def cursor(self):
        return _FakeCursor(self.store)
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def close(self): pass

class _FakePsycopg:
    def __init__(self, store):
        self.store = store
    def connect(self, *a, **k):
        return _FakeConn(self.store)

@pytest.fixture()
def fakestore():
    return {"rows": []}

@pytest.mark.db
def test_insert_and_idempotency(monkeypatch, client, fakestore):
    # Use our fake DB connection wherever dal.pool.get_conn is called
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: _FakeConn(fakestore))

    # Seed files that /pull-data checks
    m4_dir = Path(client.application.root_path).parent
    m2_ref = m4_dir / "module_2_ref"
    data_dir = m4_dir / "data"
    m2_ref.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    (m2_ref / "applicant_data.json").write_text('[{"p_id": 1, "entry_url": "/result/1"}]', encoding="utf-8")
    (m2_ref / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")

    # Fake pipeline runner used by the route; simulate DB insert on load step
    def fake_run(args, **kwargs):
        script = ""
        if isinstance(args, (list, tuple)) and len(args) > 1:
            script = str(args[1]).replace("\\", "/").lower()
        if script.endswith("load_data.py"):
            if not any(r["p_id"] == 1 for r in fakestore["rows"]):
                fakestore["rows"].append({"p_id": 1})
            return SimpleNamespace(returncode=0, stdout="row_count=1", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    # First pull inserts one row
    client.application.config["BUSY"] = False
    r1 = client.post("/pull-data")
    assert r1.status_code == 200 and r1.get_json()["row_count"] == 1
    assert len(fakestore["rows"]) == 1

    # Second pull is idempotent
    r2 = client.post("/pull-data")
    assert r2.status_code == 200
    assert len(fakestore["rows"]) == 1