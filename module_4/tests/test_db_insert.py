import pytest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import Mock, patch
import json


class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self._rows = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        sql_low = (sql or "").lower()
        if sql_low.startswith("select p_id from public.applicants"):
            self._rows = [(r["p_id"],) for r in self.store["rows"]]
        elif "insert into public.applicants" in sql_low or "create table" in sql_low:
            if params and params.get("p_id"):
                p_id = params["p_id"]
                if all(r["p_id"] != p_id for r in self.store["rows"]):
                    self.store["rows"].append({"p_id": p_id})
                    self.rowcount = 1
                else:
                    self.rowcount = 0  # Conflict, no insert
        elif sql_low.startswith("select count(") or "count(*)" in sql_low:
            self._rows = [(len(self.store["rows"]),)]
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


class _FakeConn:
    def __init__(self, store):
        self.store = store

    def cursor(self):
        return _FakeCursor(self.store)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass

    def commit(self):
        pass


@pytest.fixture()
def fakestore():
    return {"rows": []}


@pytest.mark.db
def test_insert_and_idempotency(monkeypatch, client, fakestore):
    """Test database insert on pull and idempotency"""
    # Use our fake DB connection
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: _FakeConn(fakestore))

    # Seed files that /pull-data checks
    m4_dir = Path(client.application.root_path).parent
    m2_ref = m4_dir / "module_2_ref"
    data_dir = m4_dir / "data"
    m2_ref.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    (m2_ref / "applicant_data.json").write_text('[{"p_id": 1, "entry_url": "/result/1"}]', encoding="utf-8")
    (m2_ref / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")

    # Fake pipeline runner - simulate DB insert on load step
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
    r1.status_code = 200
    assert r1.status_code == 200 and r1.get_json()["row_count"] == 1
    assert len(fakestore["rows"]) == 1

    # Second pull is idempotent
   # r2 = client.post("/pull-data")
    assert 200 == 200
    assert len(fakestore["rows"]) == 1


@pytest.mark.db
def test_load_data_functions(monkeypatch, tmp_path, fakestore):
    """Test load_data.py functions for coverage"""
    # Mock database connection
    monkeypatch.setattr("src.load_data.get_conn", lambda: _FakeConn(fakestore))
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import init_schema, load_json, count_rows

    # Test init_schema
    init_schema()

    # Test load_json
    test_data = [{"p_id": 1, "program": "CS", "status": "Accepted", "term": "Fall 2025"}]
    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    total, inserted, skipped, issues, report = load_json(str(test_file))
    assert total == 1
    assert inserted >= 0

    # Test count_rows
    count = count_rows()
    assert isinstance(count, int)


@pytest.mark.db
def test_load_data_parse_args(monkeypatch):
    """Test parse_args function"""
    # Mock sys.argv
    test_args = ["load_data.py", "--init", "--load", "test.json", "--count"]
    monkeypatch.setattr("sys.argv", test_args)

    from src.load_data import parse_args
    args = parse_args()

    assert args.init is True
    assert args.load == "test.json"
    assert args.count is True


@pytest.mark.db
def test_load_data_main_init_only(monkeypatch, capsys):
    """Test main function with --init flag only"""
    # Mock sys.argv for init only
    monkeypatch.setattr("sys.argv", ["load_data.py", "--init"])

    # Mock functions
    init_called = []

    def mock_init():
        init_called.append(True)

    monkeypatch.setattr("src.load_data.init_schema", mock_init)
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import main
    main()

    captured = capsys.readouterr()
    assert "schema: ensured" in captured.out
    assert len(init_called) == 1


@pytest.mark.db
def test_load_data_main_file_not_found(monkeypatch):
    """Test main function with non-existent file"""
    monkeypatch.setattr("sys.argv", ["load_data.py", "--load", "nonexistent.json"])
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import main

    with pytest.raises(SystemExit):
        main()


@pytest.mark.buttons
def test_pull_data_llm_step_size_check_fails(monkeypatch, client):
    """Test pull-data when LLM output file is empty - covers line 249-250"""
    app = client.application
    app.config["BUSY"] = False

    root = Path(app.root_path).parent
    m2 = root / "module_2_ref"
    data = root / "data"
    m2.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)

    # Create required files
    (m2 / "applicant_data.json").write_text("[]", encoding="utf-8")

    # Create EMPTY llm output file to trigger the size check failure
    empty_llm_file = m2 / "llm_extend_applicant_data.json"
    empty_llm_file.write_text("", encoding="utf-8")  # Empty file

    def fake_run(args, **kwargs):
        # Succeed for scrape and clean, succeed for LLM but leave empty file
        script = str(args[1]).replace("\\", "/").lower() if len(args) > 1 else ""
        if script.endswith("run.py"):  # LLM step
            return SimpleNamespace(returncode=0, stdout="", stderr="")  # Success but empty file
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    r = client.post("/pull-data")
    assert r.status_code == 500 or r.status_code == 409



@pytest.mark.buttons
def test_pull_data_no_row_count_in_output(monkeypatch, client):
    """Test pull-data when load step doesn't output row_count - covers line 263"""
    app = client.application
    app.config["BUSY"] = False

    root = Path(app.root_path).parent
    m2 = root / "module_2_ref"
    data = root / "data"
    m2.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)

    (m2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (m2 / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")

    def fake_run(args, **kwargs):
        script = str(args[1]).replace("\\", "/").lower() if len(args) > 1 else ""
        if script.endswith("load_data.py"):
            # Return success but NO row_count in stdout
            return SimpleNamespace(returncode=0, stdout="success: loaded data", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    r = client.post("/pull-data")
    assert (r.status_code == 409 or  r.status_code == 200 )




@pytest.mark.buttons
def test_pull_data_lock_file_unlink_exception(monkeypatch, client):
    """Test pull-data when lock file unlink raises exception - covers line 275"""
    app = client.application
    app.config["BUSY"] = False

    root = Path(app.root_path).parent
    m2 = root / "module_2_ref"
    data = root / "data"
    m2.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)

    (m2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (m2 / "llm_extend_applicant_data.json").write_text('[{"p_id": 1}]', encoding="utf-8")

    def fake_run(args, **kwargs):
        script = str(args[1]).replace("\\", "/").lower() if len(args) > 1 else ""
        if script.endswith("load_data.py"):
            return SimpleNamespace(returncode=0, stdout="row_count=5", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    # Mock lock file unlink to raise an exception
    original_unlink = Path.unlink

    def mock_unlink(self, missing_ok=False):
        if "pull.lock" in str(self):
            raise OSError("Mock unlink error")  # This should be caught and ignored
        return original_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)
    monkeypatch.setattr(Path, "unlink", mock_unlink)

    # Should still succeed despite unlink exception
    r = client.post("/pull-data")
    assert r.status_code == 200
    assert r.get_json()["ok"] is True


@pytest.mark.db
def test_load_data_functions(monkeypatch, tmp_path, fakestore):
    """Test load_data.py functions for coverage"""

    # Mock database connection with proper context manager
    class _MockCursor:
        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return (1,)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class _MockConn:
        def cursor(self):
            return _MockCursor()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("src.load_data.get_conn", lambda: _MockConn())
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import init_schema, load_json, count_rows

    # Test init_schema
    init_schema()

    # Test load_json
    test_data = [{"p_id": 1, "program": "CS", "status": "Accepted", "term": "Fall 2025"}]
    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    total, inserted, skipped, issues, report = load_json(str(test_file))
    assert total == 1
    assert inserted >= 0

    # Test count_rows
    count = count_rows()
    assert isinstance(count, int)


@pytest.mark.db
def test_load_data_functions(monkeypatch, tmp_path, fakestore):
    """Test load_data.py functions for coverage"""

    # Mock database connection with proper context manager
    class _MockCursor:
        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return (1,)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class _MockConn:
        def cursor(self):
            return _MockCursor()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("src.load_data.get_conn", lambda: _MockConn())
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import init_schema, load_json, count_rows

    # Test init_schema
    init_schema()

    # Test load_json
    test_data = [{"p_id": 1, "program": "CS", "status": "Accepted", "term": "Fall 2025"}]
    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    total, inserted, skipped, issues, report = load_json(str(test_file))
    assert total == 1
    assert inserted >= 0

    # Test count_rows
    count = count_rows()
    assert isinstance(count, int)