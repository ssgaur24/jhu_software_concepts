import pytest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup

@pytest.mark.web
def test_analysis_page_loads_and_has_required_elements(client):
    resp = client.get("/analysis")
    assert resp.status_code == 200

    soup = BeautifulSoup(resp.data, "html.parser")
    # Title / header
    assert soup.find("h1") and "Analysis" in soup.find("h1").get_text()
    # Buttons by stable selectors (SHALL)
    assert soup.select_one('[data-testid="pull-data-btn"]') is not None
    assert soup.select_one('[data-testid="update-analysis-btn"]') is not None
    # At least one “Answer:” label on page (SHALL)
    assert any("Answer:" in el.get_text() for el in soup.select(".answer-item"))

@pytest.mark.web
def test_root_aliases_to_analysis(client):
    r = client.get("/")
    assert r.status_code == 200
    assert b"Analysis" in r.data

@pytest.mark.web
def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.get_json()["ok"] is True

@pytest.mark.web
def test_analysis_optional_extras_are_guarded(monkeypatch, client):
    # force q11/q12 to raise to exercise the try/except branches
    monkeypatch.setattr("src.flask_app.q11_top_unis_fall_2025", lambda **k: (_ for _ in ()).throw(Exception("x")))
    monkeypatch.setattr("src.flask_app.q12_status_breakdown_fall_2025", lambda : (_ for _ in ()).throw(Exception("y")))
    r = client.get("/analysis")
    assert r.status_code == 200
    soup = BeautifulSoup(r.data, "html.parser")
    assert soup.find("h1")

@pytest.mark.web
def test_index_sets_report_and_lock_flags(client, monkeypatch, tmp_path):
    app = client.application
    root = Path(app.root_path).parent
    # ensure artifacts dir and report file exist
    art = root / "artifacts"; art.mkdir(parents=True, exist_ok=True)
    (art / "load_report.json").write_text("{}", encoding="utf-8")
    # ensure no lock for this test
    (art / "pull.lock").unlink(missing_ok=True)

    r = client.get("/")
    assert r.status_code == 200
    soup = BeautifulSoup(r.data, "html.parser")
    assert soup.find("h1")  # page renders

@pytest.mark.web
def test_build_rows_formats_na_and_strings(monkeypatch, client):
    # force different shapes to drive _format_q3 and _format_status_avgs code paths
    mp = monkeypatch.setattr
    mp("src.flask_app.q1_count_fall_2025", lambda: 0)
    mp("src.flask_app.q2_pct_international", lambda: 0.0)
    # q3: mix of values/None
    mp("src.flask_app.q3_avgs", lambda: (3.7, None, 150.0, None))
    mp("src.flask_app.q4_avg_gpa_american_fall2025", lambda: None)
    mp("src.flask_app.q5_pct_accept_fall2025", lambda: 12.3456)
    mp("src.flask_app.q6_avg_gpa_accept_fall2025", lambda: 3.91)
    mp("src.flask_app.q7_count_jhu_masters_cs", lambda: 2)
    mp("src.flask_app.q8_count_2025_georgetown_phd_cs_accept", lambda: 1)
    mp("src.flask_app.q9_top5_accept_unis_2025", lambda: [("A", 3), ("B", 2)])
    mp("src.flask_app.q10_avg_gre_by_status_year", lambda y: [("Accepted", 320.0), ("Rejected", None)])
    mp("src.flask_app.q10_avg_gre_by_status_last_n_years", lambda n: [("Accepted", 315.5)])

    # optional extras throw to hit guarded excepts
    mp("src.flask_app.q11_top_unis_fall_2025", lambda **k: (_ for _ in ()).throw(Exception("skip")))
    mp("src.flask_app.q12_status_breakdown_fall_2025", lambda : (_ for _ in ()).throw(Exception("skip")))

    r = client.get("/analysis")
    assert r.status_code == 200
    t = r.get_data(as_text=True)
    assert "Average GPA: 3.70" in t and "Average GRE V: 150.00" in t
    assert "Average GRE" in t  # q3 string assembled
    assert "Percent International: 0.00%" in t
    assert "Acceptance percent: 12.35%" in t

@pytest.mark.buttons
def test_pull_data_happy_path_with_requirements(monkeypatch, client, tmp_path):
    app = client.application
    app.config["BUSY"] = False
    root = Path(app.root_path).parent
    m2 = root / "module_2_ref"; m2.mkdir(parents=True, exist_ok=True)
    data = root / "data"; data.mkdir(parents=True, exist_ok=True)
    (m2 / "applicant_data.json").write_text("[]", encoding="utf-8")
    (m2 / "llm_extend_applicant_data.json").write_text('[{"p_id":1}]', encoding="utf-8")
    # create requirements.txt so deps branch runs
    (m2 / "requirements.txt").write_text("pytest==8.0.0", encoding="utf-8")

    def fake_run(args, **kwargs):
        # emit row_count on the load step, succeed otherwise
        script = str(args[1]).replace("\\","/").lower() if isinstance(args,(list,tuple)) and len(args)>1 else ""
        if script.endswith("load_data.py"):
            return SimpleNamespace(returncode=0, stdout="row_count=7", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr("src.flask_app.subprocess.run", fake_run)

    r = client.post("/pull-data")
    assert r.status_code == 200
    assert r.get_json()["row_count"] == 7  # hit row_count parsing

@pytest.mark.buttons
def test_busy_flag_cleared_even_on_error(monkeypatch, client):
    app = client.application
    app.config["BUSY"] = False
    root = Path(app.root_path).parent
    (root / "module_2_ref").mkdir(parents=True, exist_ok=True)

    # fail early (scrape step returns non-zero); finalizer should clear BUSY + remove lock
    def fake_fail(args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")
    monkeypatch.setattr("src.flask_app.subprocess.run", fake_fail)

    r = client.post("/pull-data")
    assert r.status_code == 500
    # BUSY cleared + no lingering lock
    assert app.config.get("BUSY") is False
    lock = (root / "artifacts" / "pull.lock")
    assert not lock.exists()


@pytest.mark.web
def test_create_app_with_config_overrides(self):
    """
    GIVEN: Custom configuration overrides provided
    WHEN: create_app is called with config_overrides
    THEN: App should be created with custom configuration applied
    """
    from src.flask_app import create_app

    # GIVEN: Configuration overrides
    config_overrides = {
        "TESTING": True,
        "DATABASE_URL": "postgresql://test@localhost/testdb",
        "BUSY": False
    }

    # WHEN: Create app with overrides
    app = create_app(config_overrides)

    # THEN: Configuration should be applied
    assert app.config["TESTING"] is True
    assert app.config["DATABASE_URL"] == "postgresql://test@localhost/testdb"
    assert app.config["BUSY"] is False


@pytest.mark.web
def test_create_app_without_overrides(self):
    """
    GIVEN: No configuration overrides provided
    WHEN: create_app is called with None config
    THEN: App should be created with default configuration
    """
    from src.flask_app import create_app

    # WHEN: Create app without overrides
    app = create_app(None)

    # THEN: Default configuration should be set
    assert "BUSY" in app.config
    assert app.config["BUSY"] is False


@pytest.mark.web
def test_create_app_sets_up_paths(self, monkeypatch):
    """
    GIVEN: Module structure with directories
    WHEN: create_app is called
    THEN: Required directories should be created
    """
    from src.flask_app import create_app

    # Mock Path operations to avoid filesystem changes
    mock_mkdir = Mock()
    with patch.object(Path, 'mkdir', mock_mkdir):
        # WHEN: Create app
        app = create_app({"TESTING": True})

        # THEN: App should be created successfully
        assert app is not None


@pytest.mark.web
class TestRouteHandlers:
    """Tests for Flask route handlers and template rendering."""

    @pytest.mark.web
    def test_index_route_renders_analysis(self, monkeypatch):
        """
        GIVEN: Flask app with mocked query functions
        WHEN: GET / route is accessed
        THEN: Analysis page should render with data
        """
        from src.flask_app import create_app

        # GIVEN: Mock all query functions
        self._mock_query_functions(monkeypatch)

        app = create_app({"TESTING": True})

        # WHEN: Access index route
        with app.test_client() as client:
            response = client.get("/")

        # THEN: Should render successfully
        assert response.status_code == 200
        assert b"Analysis" in response.data

    @pytest.mark.web
    def test_analysis_route_alias(self, monkeypatch):
        """
        GIVEN: Flask app with analysis route
        WHEN: GET /analysis route is accessed
        THEN: Same content as index should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Mock query functions
        self._mock_query_functions(monkeypatch)

        app = create_app({"TESTING": True})

        # WHEN: Access analysis route
        with app.test_client() as client:
            response = client.get("/analysis")

        # THEN: Should render same as index
        assert response.status_code == 200
        assert b"Analysis" in response.data

    @pytest.mark.web
    def test_health_route(self):
        """
        GIVEN: Flask app
        WHEN: GET /health route is accessed
        THEN: Health check JSON should be returned
        """
        from src.flask_app import create_app

        app = create_app({"TESTING": True})

        # WHEN: Access health route
        with app.test_client() as client:
            response = client.get("/health")

        # THEN: Should return health status
        assert response.status_code == 200
        data = response.get_json()
        assert data["ok"] is True

    def _mock_query_functions(self, monkeypatch):
        """Helper to mock all query functions with default values."""
        monkeypatch.setattr("src.flask_app.q1_count_fall_2025", lambda: 100)
        monkeypatch.setattr("src.flask_app.q2_pct_international", lambda: 25.0)
        monkeypatch.setattr("src.flask_app.q3_avgs", lambda: (3.5, 320.0, 155.0, 4.0))
        monkeypatch.setattr("src.flask_app.q4_avg_gpa_american_fall2025", lambda: 3.6)
        monkeypatch.setattr("src.flask_app.q5_pct_accept_fall2025", lambda: 20.0)
        monkeypatch.setattr("src.flask_app.q6_avg_gpa_accept_fall2025", lambda: 3.8)
        monkeypatch.setattr("src.flask_app.q7_count_jhu_masters_cs", lambda: 5)
        monkeypatch.setattr("src.flask_app.q8_count_2025_georgetown_phd_cs_accept", lambda: 2)
        monkeypatch.setattr("src.flask_app.q9_top5_accept_unis_2025", lambda: [("Harvard", 10)])
        monkeypatch.setattr("src.flask_app.q10_avg_gre_by_status_year", lambda y: [("Accepted", 325.0)])
        monkeypatch.setattr("src.flask_app.q10_avg_gre_by_status_last_n_years", lambda n: [("Accepted", 320.0)])
        # Mock optional functions to not throw
        monkeypatch.setattr("src.flask_app.q11_top_unis_fall_2025", lambda **k: [("Test U", 5)])
        monkeypatch.setattr("src.flask_app.q12_status_breakdown_fall_2025", lambda: [("Accepted", 30.0)])


@pytest.mark.buttons
class TestPullDataEndpoint:
    """Tests for pull-data endpoint functionality and edge cases."""

    @pytest.mark.buttons
    def test_pull_data_success_with_row_count(self, monkeypatch):
        """
        GIVEN: App not busy and successful subprocess execution
        WHEN: POST /pull-data is called
        THEN: Success response with row count should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Setup for successful execution
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_success(monkeypatch, app, row_count=42)

        # WHEN: Call pull-data endpoint
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: Should succeed with row count
        assert response.status_code == 200
        data = response.get_json()
        assert data["ok"] is True
        assert data["row_count"] == 42

    @pytest.mark.buttons
    def test_pull_data_busy_via_config_flag(self):
        """
        GIVEN: App with BUSY flag set to True
        WHEN: POST /pull-data is called
        THEN: 409 status with busy=true should be returned
        """
        from src.flask_app import create_app

        # GIVEN: App marked as busy
        app = create_app({"TESTING": True, "BUSY": True})

        # WHEN: Attempt pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: Should return busy status
        assert response.status_code == 409
        data = response.get_json()
        assert data["busy"] is True

    @pytest.mark.buttons
    def test_pull_data_busy_via_lock_file(self, monkeypatch):
        """
        GIVEN: Lock file exists indicating operation in progress
        WHEN: POST /pull-data is called
        THEN: 409 status should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Mock lock file existence
        app = create_app({"TESTING": True, "BUSY": False})

        # Mock the LOCK_FILE.exists() to return True
        with patch.object(Path, 'exists', return_value=True):
            # WHEN: Attempt pull-data
            with app.test_client() as client:
                response = client.post("/pull-data")

        # THEN: Should return busy status
        assert response.status_code == 409
        data = response.get_json()
        assert data["busy"] is True

    @pytest.mark.buttons
    def test_pull_data_scrape_step_failure(self, monkeypatch):
        """
        GIVEN: Scrape step returns non-zero exit code
        WHEN: POST /pull-data is called
        THEN: 500 status with step=scrape should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Setup with scrape failure
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_failure(monkeypatch, app, fail_step="scrape")

        # WHEN: Call pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: Should return error with step info
        assert response.status_code == 500
        data = response.get_json()
        assert data["ok"] is False
        assert data["step"] == "scrape"

    @pytest.mark.buttons
    def test_pull_data_clean_step_failure(self, monkeypatch):
        """
        GIVEN: Clean step returns non-zero exit code
        WHEN: POST /pull-data is called
        THEN: 500 status with step=clean should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Setup with clean failure
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_failure(monkeypatch, app, fail_step="clean")

        # WHEN: Call pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: Should return clean step error
        assert response.status_code == 500
        data = response.get_json()
        assert data["ok"] is False
        assert data["step"] == "clean"

    @pytest.mark.buttons
    def test_pull_data_llm_step_failure(self, monkeypatch):
        """
        GIVEN: LLM step returns non-zero exit code
        WHEN: POST /pull-data is called
        THEN: 500 status with step=llm should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Setup with LLM failure
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_failure(monkeypatch, app, fail_step="llm")

        # WHEN: Call pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: Should return LLM step error
        assert response.status_code == 500
        data = response.get_json()
        assert data["ok"] is False
        assert data["step"] == "llm"

    @pytest.mark.buttons
    def test_pull_data_load_step_failure(self, monkeypatch):
        """
        GIVEN: Load step returns non-zero exit code
        WHEN: POST /pull-data is called
        THEN: 500 status with step=load should be returned
        """
        from src.flask_app import create_app

        # GIVEN: Setup with load failure
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_failure(monkeypatch, app, fail_step="load")

        # WHEN: Call pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: Should return load step error
        assert response.status_code == 500
        data = response.get_json()
        assert data["ok"] is False
        assert data["step"] == "load"

    @pytest.mark.buttons
    def test_pull_data_clears_busy_flag_on_success(self, monkeypatch):
        """
        GIVEN: Successful pull-data execution
        WHEN: POST /pull-data completes
        THEN: BUSY flag should be cleared
        """
        from src.flask_app import create_app

        # GIVEN: Setup for success
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_success(monkeypatch, app)

        # WHEN: Execute pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: BUSY flag should be cleared
        assert response.status_code == 200
        assert app.config["BUSY"] is False

    @pytest.mark.buttons
    def test_pull_data_clears_busy_flag_on_error(self, monkeypatch):
        """
        GIVEN: Failed pull-data execution
        WHEN: POST /pull-data fails
        THEN: BUSY flag should still be cleared
        """
        from src.flask_app import create_app

        # GIVEN: Setup for failure
        app = create_app({"TESTING": True, "BUSY": False})
        self._setup_pull_data_failure(monkeypatch, app, fail_step="scrape")

        # WHEN: Execute pull-data
        with app.test_client() as client:
            response = client.post("/pull-data")

        # THEN: BUSY flag should be cleared even on error
        assert response.status_code == 500
        assert app.config["BUSY"] is False

    def _setup_pull_data_success(self, monkeypatch, app, row_count=5):
        """Helper to setup successful pull-data execution."""
        # Mock required paths and files
        self._mock_paths_and_files(monkeypatch, app)

        # Mock successful subprocess execution
        def mock_run(cmd, **kwargs):
            if any("load_data.py" in str(arg) for arg in cmd):
                return SimpleNamespace(returncode=0, stdout=f"row_count={row_count}", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr("src.flask_app.subprocess.run", mock_run)

    def _setup_pull_data_failure(self, monkeypatch, app, fail_step):
        """Helper to setup failed pull-data execution."""
        # Mock required paths and files
        self._mock_paths_and_files(monkeypatch, app)

        # Mock subprocess failure for specific step
        def mock_run(cmd, **kwargs):
            script_name = ""
            if isinstance(cmd, (list, tuple)) and len(cmd) > 1:
                script_name = str(cmd[1]).lower()

            if fail_step == "scrape" and "scrape.py" in script_name:
                return SimpleNamespace(returncode=1, stdout="", stderr="scrape failed")
            elif fail_step == "clean" and "clean.py" in script_name:
                return SimpleNamespace(returncode=1, stdout="", stderr="clean failed")
            elif fail_step == "llm" and "run.py" in script_name:
                return SimpleNamespace(returncode=1, stdout="", stderr="llm failed")
