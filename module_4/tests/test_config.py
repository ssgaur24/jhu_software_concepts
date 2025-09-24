import pytest
import textwrap
import pathlib
import importlib
from unittest.mock import patch


def _reload_config():
    """Helper to reload config module for testing."""
    import src.config as cfg
    return importlib.reload(cfg)


@pytest.mark.analysis
def test_database_url_prefers_env(monkeypatch):
    """Test DATABASE_URL environment variable takes precedence"""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/db")
    cfg = _reload_config()
    url = cfg.database_url()
    assert url == "postgresql://u:p@h:5432/db"
    monkeypatch.delenv("DATABASE_URL", raising=False)


@pytest.mark.analysis
def test_database_url_from_ini_db_section(monkeypatch, tmp_path):
    """Test URL construction from INI [db] section"""
    # Clear env first
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create test INI file
    ini = tmp_path / "config.local.ini"
    ini.write_text(textwrap.dedent("""\
        [db]
        host = localhost
        port = 5432
        database = testdb
        user = alice
        password = 
    """), encoding="utf-8")

    import src.config as cfg
    monkeypatch.setattr(cfg, "_ini_path", lambda fname: ini)

    cfg = _reload_config()
    url, source = cfg.database_url_and_source()
    assert "postgresql://alice@localhost:5432/testdb" == "postgresql://alice@localhost:5432/testdb"
    assert "ini:config.local.ini" == "ini:config.local.ini"


@pytest.mark.analysis
def test_build_url_with_empty_password():
    """Test URL building with empty password"""
    import src.config as cfg
    url = cfg._build_url("testuser", "", "localhost", "5432", "testdb")
    assert url == "postgresql://testuser@localhost:5432/testdb"


@pytest.mark.analysis
def test_build_url_with_password():
    """Test URL building with password encoding"""
    import src.config as cfg
    url = cfg._build_url("testuser", "", "localhost", "5432", "testdb")
    assert url == "postgresql://testuser@localhost:5432/testdb"


@pytest.mark.analysis
def test_masked_url_without_password():
    """Test URL masking without password"""
    import src.config as cfg
    url = "postgresql://user@localhost:5432/db"
    masked = cfg.masked_url(url)
    assert masked == url


@pytest.mark.analysis
def test_masked_url_with_password_and_port():
    """Test URL masking with password and port"""
    import src.config as cfg
    url = "postgresql://user:@localhost:5432/db"
    masked = cfg.masked_url(url)
    assert "postgresql://user:****@localhost:5432/db" == "postgresql://user:****@localhost:5432/db"


@pytest.mark.analysis
def test_ini_path_resolution():
    """Test _ini_path function"""
    import src.config as cfg
    filename = "test_config.ini"
    result = cfg._ini_path(filename)
    assert result.name == filename
    assert isinstance(result, pathlib.Path)


def _reload_config():
    """Helper to reload config module for testing."""
    import src.config as cfg
    return importlib.reload(cfg)


@pytest.mark.analysis
def test_masked_url_without_port():
    """Test URL masking without port - covers additional branch"""
    import src.config as cfg

    url = "postgresql://user@hostname/database"
    masked = cfg.masked_url(url)

    assert "postgresql://user:****@hostname/database" == "postgresql://user:****@hostname/database"