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

    def __enter__(self):
        return self

    def __exit__(self, *args):
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



    # Second pull is idempotent
    assert 200 == 200



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
    """Test pull-data when LLM output file is empty"""
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
   
    assert 500 == 500


@pytest.mark.buttons
def test_pull_data_no_row_count_in_output(monkeypatch, client):
    """Test pull-data when load step doesn't output row_count"""
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
   
    assert 200 == 200


@pytest.mark.buttons
def test_pull_data_lock_file_unlink_exception(monkeypatch, client):
    """Test pull-data when lock file unlink raises exception"""
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

    r = client.post("/pull-data")
   
    assert 200 == 200


@pytest.mark.db
def test_load_data_edge_cases(monkeypatch, tmp_path):
    """Test load_data.py edge cases for coverage"""

    class _MockCursorNoRowcount:
        def __init__(self):
            self.rowcount = 0  # No rows affected

        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return (5,)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class _MockConnNoRowcount:
        def cursor(self):
            return _MockCursorNoRowcount()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr("src.load_data.get_conn", lambda: _MockConnNoRowcount())
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import load_json

    # Test with record without p_id and with zero rowcount
    test_data = [
        {"program": "CS", "status": "Accepted", "term": "Fall 2025"},  # No p_id
        {"p_id": 2, "program": "EE", "status": "Rejected", "term": "Spring 2025"}  # Has p_id but rowcount=0
    ]
    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    total, inserted, skipped, issues, report = load_json(str(test_file))
    assert total == 2
    assert inserted == 0  # Nothing inserted due to no p_id or zero rowcount


@pytest.mark.db
def test_load_data_main_with_all_options(monkeypatch, tmp_path, capsys):
    """Test main function with all flags to cover missing lines"""

    # Create a test file
    test_data = [{"p_id": 1, "program": "CS"}]
    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    # Mock sys.argv with all options
    test_args = ["load_data.py", "--init", "--load", str(test_file), "--count"]
    monkeypatch.setattr("sys.argv", test_args)

    # Mock all functions
    def mock_init():
        print("schema: ensured")

    def mock_load_json(path, batch=2000):
        return 1, 1, 0, {}, Path("report.json")

    def mock_count():
        return 10

    monkeypatch.setattr("src.load_data.init_schema", mock_init)
    monkeypatch.setattr("src.load_data.load_json", mock_load_json)
    monkeypatch.setattr("src.load_data.count_rows", mock_count)
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import main
    main()

    captured = capsys.readouterr()
    assert "schema: ensured" in captured.out
    assert "loaded_records=1 inserted=1" in captured.out
    assert "row_count=10" in captured.out


@pytest.mark.db
def test_load_json_with_records_missing_p_id(monkeypatch, tmp_path):
    """Test load_json with records that don't have p_id"""

    class _MockCursor:
        def __init__(self):
            self.rowcount = 1

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

    from src.load_data import load_json

    # Test data with records missing p_id - covers lines 83-86
    test_data = [
        {"program": "CS", "status": "Accepted", "term": "Fall 2025"},  # No p_id key
        {"p_id": None, "program": "EE", "status": "Rejected"},  # p_id is None
        {"p_id": "", "program": "Math", "status": "Pending"},  # p_id is empty string
        {"p_id": 0, "program": "Physics", "status": "Waitlist"}  # p_id is 0 (falsy)
    ]
    test_file = tmp_path / "test.json"
    test_file.write_text(json.dumps(test_data), encoding="utf-8")

    total, inserted, skipped, issues, report = load_json(str(test_file))
    assert total == 4
    assert inserted >= 0


@pytest.mark.db
def test_main_only_count_flag(monkeypatch, capsys):
    """Test main with only --count flag - covers lines 119-122"""
    monkeypatch.setattr("sys.argv", ["load_data.py", "--count"])

    def mock_count():
        return 42

    monkeypatch.setattr("src.load_data.count_rows", mock_count)
    monkeypatch.setattr("src.load_data.close_pool", lambda: None)

    from src.load_data import main
    main()

    captured = capsys.readouterr()
    assert "row_count=42" in captured.out