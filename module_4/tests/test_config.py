"""
Enhanced test suite for src/config.py to achieve 100% coverage.

Tests configuration loading from environment variables and INI files,
URL building, password masking, and error handling scenarios.

Follows TDD GIVEN/WHEN/THEN structure as required by Module 4 assignment.
"""

import pytest
import textwrap
import pathlib
from unittest.mock import patch
import importlib


def _reload_config():
    """Helper to reload config module for testing."""
    import src.config as cfg
    return importlib.reload(cfg)


@pytest.mark.analysis
class TestConfigurationLoading:
    """Tests for database configuration loading and URL construction."""

    def test_database_url_prefers_env(self, monkeypatch):
        """
        GIVEN: DATABASE_URL environment variable is set
        WHEN: database_url() is called
        THEN: Environment variable should take precedence over INI files
        """
        # GIVEN: Set environment variable
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/db")

        # WHEN: Reload and get URL
        cfg = _reload_config()
        url = cfg.database_url()

        # THEN: Environment URL should be returned
        assert url == "postgresql://u:p@h:5432/db"

        # Clean up
        monkeypatch.delenv("DATABASE_URL", raising=False)

    def test_database_url_from_ini_db_section(self, monkeypatch, tmp_path):
        """
        GIVEN: No environment variable but valid INI file with [db] section
        WHEN: database_url_and_source() is called
        THEN: URL should be constructed from INI and source reported
        """
        # GIVEN: Create INI file with [db] section
        ini = tmp_path / "config.local.ini"
        ini.write_text(textwrap.dedent("""\
            [db]
            host = localhost
            port = 5432
            database = testdb
            user = alice
            password = secret123
        """), encoding="utf-8")

        # Mock environment and path resolution
        monkeypatch.delenv("DATABASE_URL", raising=False)
        import src.config as cfg
        monkeypatch.setattr(cfg, "_ini_path", lambda fname: ini)

        # WHEN: Get URL and source
        cfg = _reload_config()
        url, source = cfg.database_url_and_source()

        # THEN: URL should be constructed from INI
        assert url == "postgresql://alice:secret123@localhost:5432/testdb"
        assert source == "ini:config.local.ini"

    def test_database_url_raises_when_missing(self, monkeypatch):
        """
        GIVEN: No environment variable and no INI file
        WHEN: database_url_and_source() is called
        THEN: RuntimeError should be raised
        """
        # GIVEN: No environment variable, no INI file
        import src.config as cfg
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setattr(cfg, "_ini_path", lambda fname: pathlib.Path("nonexistent.ini"))

        # WHEN/THEN: Should raise RuntimeError
        cfg = _reload_config()
        with pytest.raises(RuntimeError, match="No DATABASE_URL"):
            cfg.database_url_and_source()


@pytest.mark.analysis
class TestURLConstruction:
    """Tests for URL building and password masking functionality."""

    def test_build_url_with_empty_password(self):
        """
        GIVEN: Database credentials with empty password
        WHEN: _build_url is called with empty password
        THEN: Password segment should be omitted from URL
        """
        # Import after ensuring clean state
        import src.config as cfg

        # GIVEN: Credentials with empty password
        user, password, host, port, database = "testuser", "", "localhost", "5432", "testdb"

        # WHEN: Build URL
        url = cfg._build_url(user, password, host, port, database)

        # THEN: No password segment in URL
        assert url == "postgresql://testuser@localhost:5432/testdb"

    def test_build_url_with_password(self):
        """
        GIVEN: Database credentials with password
        WHEN: _build_url is called
        THEN: Password should be included and encoded
        """
        import src.config as cfg

        # GIVEN: Credentials with password
        user, password, host, port, database = "testuser", "secret@123", "localhost", "5432", "testdb"

        # WHEN: Build URL
        url = cfg._build_url(user, password, host, port, database)

        # THEN: Password should be encoded in URL
        assert url == "postgresql://testuser:secret%40123@localhost:5432/testdb"

    def test_masked_url_without_password(self):
        """
        GIVEN: Database URL without password
        WHEN: masked_url is called
        THEN: URL should be returned unchanged
        """
        import src.config as cfg

        # GIVEN: URL without password
        url = "postgresql://user@localhost:5432/db"

        # WHEN: Mask URL
        masked = cfg.masked_url(url)

        # THEN: Should return unchanged
        assert masked == url

    def test_masked_url_with_password_and_port(self):
        """
        GIVEN: Database URL with password and port
        WHEN: masked_url is called
        THEN: Password should be masked, port preserved
        """
        import src.config as cfg

        # GIVEN: URL with password and port
        url = "postgresql://user:password@localhost:5432/db"

        # WHEN: Mask URL
        masked = cfg.masked_url(url)

        # THEN: Password masked, port preserved
        assert masked == "postgresql://user:****@localhost:5432/db"

    def test_masked_url_without_port(self):
        """
        GIVEN: Database URL with password but no explicit port
        WHEN: masked_url is called
        THEN: Password should be masked without port handling
        """
        import src.config as cfg

        # GIVEN: URL with password, no port
        url = "postgresql://user:pass@hostname/database"

        # WHEN: Mask URL
        masked = cfg.masked_url(url)

        # THEN: Password masked
        assert masked == "postgresql://user:****@hostname/database"


@pytest.mark.analysis
class TestINIFileHandling:
    """Tests for INI file parsing and validation."""

    def test_ini_missing_required_database_key(self, monkeypatch, tmp_path):
        """
        GIVEN: INI file missing required 'database' key
        WHEN: database_url_and_source is called
        THEN: RuntimeError should be raised
        """
        # GIVEN: INI with missing database key
        ini = tmp_path / "config.local.ini"
        ini.write_text(textwrap.dedent("""\
            [db]
            host = localhost
            port = 5432
            user = alice
            password = secret
        """), encoding="utf-8")

        import src.config as cfg
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setattr(cfg, "_ini_path", lambda fname: ini)

        # WHEN/THEN: Should raise for missing database
        cfg = _reload_config()
        with pytest.raises(RuntimeError, match="'database' and 'user' are required"):
            cfg.database_url_and_source()

    def test_ini_missing_required_user_key(self, monkeypatch, tmp_path):
        """
        GIVEN: INI file missing required 'user' key
        WHEN: database_url_and_source is called
        THEN: RuntimeError should be raised
        """
        # GIVEN: INI with missing user key
        ini = tmp_path / "config.local.ini"
        ini.write_text(textwrap.dedent("""\
            [db]
            host = localhost
            port = 5432
            database = testdb
            password = secret
        """), encoding="utf-8")

        import src.config as cfg
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setattr(cfg, "_ini_path", lambda fname: ini)

        # WHEN/THEN: Should raise for missing user
        cfg = _reload_config()
        with pytest.raises(RuntimeError, match="'database' and 'user' are required"):
            cfg.database_url_and_source()

    def test_ini_with_empty_values(self, monkeypatch, tmp_path):
        """
        GIVEN: INI file with empty values for required keys
        WHEN: database_url_and_source is called
        THEN: RuntimeError should be raised
        """
        # GIVEN: INI with empty values
        ini = tmp_path / "config.local.ini"
        ini.write_text(textwrap.dedent("""\
            [db]
            host = localhost
            port = 5432
            database = 
            user = 
            password = secret
        """), encoding="utf-8")

        import src.config as cfg
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setattr(cfg, "_ini_path", lambda fname: ini)

        # WHEN/THEN: Should raise for empty values
        cfg = _reload_config()
        with pytest.raises(RuntimeError, match="'database' and 'user' are required"):
            cfg.database_url_and_source()

    def test_ini_path_resolution(self):
        """
        GIVEN: A filename for INI configuration
        WHEN: _ini_path is called
        THEN: Path should be resolved relative to module_4 directory
        """
        import src.config as cfg

        # GIVEN: Filename
        filename = "test_config.ini"

        # WHEN: Get path
        result = cfg._ini_path(filename)

        # THEN: Should resolve correctly
        assert result.name == filename
        assert isinstance(result, pathlib.Path)


@pytest.mark.analysis
class TestConfigurationIntegration:
    """Integration tests for complete configuration workflows."""

    def test_env_overrides_ini(self, monkeypatch, tmp_path):
        """
        GIVEN: Both environment variable and INI file present
        WHEN: database_url is called
        THEN: Environment variable should take precedence
        """
        # GIVEN: Both ENV and INI
        monkeypatch.setenv("DATABASE_URL", "postgresql://env_user:env_pass@env_host:5432/env_db")

        ini = tmp_path / "config.local.ini"
        ini.write_text(textwrap.dedent("""\
            [db]
            host = ini_host
            port = 5432
            database = ini_db
            user = ini_user
            password = ini_pass
        """), encoding="utf-8")

        import src.config as cfg
        monkeypatch.setattr(cfg, "_ini_path", lambda fname: ini)

        # WHEN: Get URL
        cfg = _reload_config()
        url = cfg.database_url()

        # THEN: ENV should win
        assert url == "postgresql://env_user:env_pass@env_host:5432/env_db"

        # Clean up
        monkeypatch.delenv("DATABASE_URL", raising=False)

    def test_fallback_to_second_ini_file(self, monkeypatch, tmp_path):
        """
        GIVEN: First INI file missing, second INI file present
        WHEN: database_url_and_source is called
        THEN: Should fallback to second INI file
        """
        # GIVEN: Only config.ini exists (not config.local.ini)
        ini = tmp_path / "config.ini"
        ini.write_text(textwrap.dedent("""\
            [db]
            host = fallback_host
            port = 5432
            database = fallback_db
            user = fallback_user
            password = fallback_pass
        """), encoding="utf-8")

        import src.config as cfg
        monkeypatch.delenv("DATABASE_URL", raising=False)

        # Mock to return non-existent for config.local.ini, existing for config.ini
        def mock_ini_path(fname):
            if fname == "config.local.ini":
                return tmp_path / "nonexistent.ini"
            return ini

        monkeypatch.setattr(cfg, "_ini_path", mock_ini_path)

        # WHEN: Get URL and source
        cfg = _reload_config()
        url, source = cfg.database_url_and_source()

        # THEN: Should use fallback INI
        assert url == "postgresql://fallback_user:fallback_pass@fallback_host:5432/fallback_db"
        assert source == "ini:config.ini"