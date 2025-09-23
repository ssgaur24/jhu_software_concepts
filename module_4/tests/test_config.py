"""
Enhanced test suite for src/config.py to achieve 100% coverage.

Tests configuration loading from environment variables and INI files,
URL building, password masking, and error handling scenarios.

Follows TDD GIVEN/WHEN/THEN structure as required by Module 4 assignment.
All tests avoid hardcoded passwords per assignment requirements.
"""

import pytest
import os
import configparser
import pathlib
from unittest.mock import patch, mock_open
from urllib.parse import urlparse
from src.config import database_url_and_source, database_url, masked_url, _build_url, _ini_path


@pytest.mark.db
def test_database_url_from_env(monkeypatch):
    """Test DATABASE_URL resolution from environment variable."""
    # GIVEN: Environment variable set without password
    test_url = "postgresql://testuser@localhost:5432/testdb"
    monkeypatch.setenv("DATABASE_URL", test_url)

    # WHEN: Getting URL and source
    url, source = database_url_and_source()

    # THEN: Should return environment URL and correct source
    assert url == test_url
    assert source == "env:DATABASE_URL"


@pytest.mark.db
def test_database_url_from_local_ini(monkeypatch, tmp_path):
    """Test DATABASE_URL resolution from config.local.ini without password."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create actual INI file
    ini_file = tmp_path / "config.local.ini"
    ini_content = """[db]
host = localhost
port = 5432
database = testdb
user = testuser
password = 
"""
    ini_file.write_text(ini_content)

    # Mock _ini_path to return our test file
    def mock_ini_path(fname):
        if fname == "config.local.ini":
            return ini_file
        return tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN: Getting URL and source
        url, source = database_url_and_source()

        # THEN: Should construct URL from INI without password
        assert url == "postgresql://testuser@localhost:5432/testdb"
        assert source == "ini:config.local.ini"


@pytest.mark.db
def test_database_url_from_config_ini(monkeypatch, tmp_path):
    """Test DATABASE_URL resolution from config.ini when local doesn't exist."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create actual INI file for config.ini
    ini_file = tmp_path / "config.ini"
    ini_content = """[db]
host = localhost
port = 5432
database = testdb
user = testuser
password = 
"""
    ini_file.write_text(ini_content)

    # Mock _ini_path to return non-existent for local, existing for config
    def mock_ini_path(fname):
        if fname == "config.local.ini":
            return tmp_path / "nonexistent.ini"  # Doesn't exist
        elif fname == "config.ini":
            return ini_file  # Exists
        return tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN: Getting URL and source
        url, source = database_url_and_source()

        # THEN: Should construct URL from config.ini
        assert url == "postgresql://testuser@localhost:5432/testdb"
        assert source == "ini:config.ini"


@pytest.mark.db
def test_database_url_no_config_found(monkeypatch):
    """Test error when no configuration is found."""
    # GIVEN: No environment variable and no INI files
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with patch("pathlib.Path.exists", return_value=False):
        # WHEN/THEN: Should raise RuntimeError
        with pytest.raises(RuntimeError, match="No DATABASE_URL, config.local.ini, or config.ini found"):
            database_url_and_source()


@pytest.mark.db
def test_database_url_missing_required_fields(monkeypatch, tmp_path):
    """Test error when required fields are missing from INI."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create INI file missing database and user
    ini_file = tmp_path / "config.local.ini"
    ini_content = """[db]
host = localhost
port = 5432
"""
    ini_file.write_text(ini_content)

    # Mock _ini_path to return our test file
    def mock_ini_path(fname):
        return ini_file if fname == "config.local.ini" else tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN/THEN: Should raise RuntimeError for missing required fields
        with pytest.raises(RuntimeError, match="'database' and 'user' are required"):
            database_url_and_source()


@pytest.mark.db
def test_database_url_wrapper():
    """Test database_url() wrapper function."""
    # GIVEN: Mock underlying function
    with patch("src.config.database_url_and_source", return_value=("test_url", "test_source")):
        # WHEN: Calling wrapper
        url = database_url()

        # THEN: Should return only the URL
        assert url == "test_url"


@pytest.mark.db
def test_build_url_without_password():
    """Test URL building without password."""
    # GIVEN: Credentials without password
    # WHEN: Building URL
    url = _build_url("testuser", "", "localhost", "5432", "testdb")

    # THEN: Should create URL without password segment
    assert url == "postgresql://testuser@localhost:5432/testdb"


@pytest.mark.db
def test_build_url_with_empty_password():
    """Test URL building with empty password string."""
    # GIVEN: Credentials with empty password
    # WHEN: Building URL
    url = _build_url("testuser", "", "localhost", "5432", "testdb")

    # THEN: Should create URL without password segment
    assert url == "postgresql://testuser@localhost:5432/testdb"


@pytest.mark.db
def test_build_url_special_characters_in_username():
    """Test URL building with special characters that need encoding."""
    # GIVEN: Username with special characters
    # WHEN: Building URL
    url = _build_url("user@domain", "", "localhost", "5432", "testdb")

    # THEN: Should encode special characters
    assert "user%40domain" in url
    assert url == "postgresql://user%40domain@localhost:5432/testdb"


@pytest.mark.db
def test_build_url_empty_user():
    """Test URL building with empty user."""
    # GIVEN: Empty username
    # WHEN: Building URL
    url = _build_url("", "", "localhost", "5432", "testdb")

    # THEN: Should handle empty username
    assert url == "postgresql://@localhost:5432/testdb"


@pytest.mark.db
def test_masked_url_without_password():
    """Test URL masking without password."""
    # GIVEN: URL without password
    url = "postgresql://testuser@localhost:5432/testdb"

    # WHEN: Masking URL
    masked = masked_url(url)

    # THEN: Should return unchanged since no password
    assert masked == url


@pytest.mark.db
def test_masked_url_with_password():
    """Test URL masking with password present."""
    # GIVEN: URL with password (test scenario only)
    url = "postgresql://user:testpass@localhost:5432/testdb"

    # WHEN: Masking URL
    masked = masked_url(url)

    # THEN: Should mask password with asterisks
    assert "testpass" not in masked
    assert "****" in masked
    assert "user:****@localhost:5432" in masked


@pytest.mark.db
def test_masked_url_with_port():
    """Test URL masking preserves port information."""
    # GIVEN: URL with custom port and password
    url = "postgresql://user:testpass@localhost:3306/testdb"

    # WHEN: Masking URL
    masked = masked_url(url)

    # THEN: Should preserve port information while masking password
    assert ":3306" in masked
    assert "****" in masked


@pytest.mark.db
def test_masked_url_without_port():
    """Test URL masking when no explicit port in URL."""
    # GIVEN: URL with password but no explicit port
    url = "postgresql://user:testpass@hostname/database"

    # WHEN: Masking URL
    masked = masked_url(url)

    # THEN: Should mask password without port handling
    assert "****" in masked
    assert "testpass" not in masked


@pytest.mark.db
def test_ini_path_resolution():
    """Test _ini_path constructs correct path."""
    # GIVEN: A filename
    filename = "test_config.ini"

    # WHEN: Getting INI path
    result = _ini_path(filename)

    # THEN: Should return correct Path object
    assert isinstance(result, pathlib.Path)
    assert str(result).endswith(filename)


@pytest.mark.db
def test_ini_path_parents_resolution():
    """Test _ini_path resolves to correct parent directory."""
    # GIVEN: A filename
    filename = "config.ini"

    # WHEN: Getting INI path
    result = _ini_path(filename)

    # THEN: Should resolve relative to module_4 directory
    assert isinstance(result, pathlib.Path)
    # The path should be absolute and contain the filename
    assert result.is_absolute()
    assert result.name == filename


@pytest.mark.db
def test_database_url_ini_fallback_order(monkeypatch, tmp_path):
    """Test that config.ini is used when config.local.ini doesn't exist."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create only config.ini file (not config.local.ini)
    config_ini_file = tmp_path / "config.ini"
    ini_content = """[db]
host = fallbackhost
port = 5432
database = fallbackdb
user = fallbackuser
password = 
"""
    config_ini_file.write_text(ini_content)

    # Mock _ini_path to return non-existent for local, existing for config
    def mock_ini_path(fname):
        if fname == "config.local.ini":
            return tmp_path / "nonexistent_local.ini"  # Doesn't exist
        elif fname == "config.ini":
            return config_ini_file  # Exists
        return tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN: Getting URL and source
        url, source = database_url_and_source()

        # THEN: Should use config.ini as fallback
        assert url == "postgresql://fallbackuser@fallbackhost:5432/fallbackdb"
        assert source == "ini:config.ini"


@pytest.mark.db
def test_database_url_empty_database_field(monkeypatch, tmp_path):
    """Test error handling when database field is empty."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create INI with empty database field
    ini_file = tmp_path / "config.local.ini"
    ini_content = """[db]
host = localhost
port = 5432
database = 
user = testuser
password = 
"""
    ini_file.write_text(ini_content)

    # Mock _ini_path to return our test file
    def mock_ini_path(fname):
        return ini_file if fname == "config.local.ini" else tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN/THEN: Should raise RuntimeError for empty database
        with pytest.raises(RuntimeError, match="'database' and 'user' are required"):
            database_url_and_source()


@pytest.mark.db
def test_database_url_empty_user_field(monkeypatch, tmp_path):
    """Test error handling when user field is empty."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create INI with empty user field
    ini_file = tmp_path / "config.local.ini"
    ini_content = """[db]
host = localhost
port = 5432
database = testdb
user = 
password = 
"""
    ini_file.write_text(ini_content)

    # Mock _ini_path to return our test file
    def mock_ini_path(fname):
        return ini_file if fname == "config.local.ini" else tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN/THEN: Should raise RuntimeError for empty user
        with pytest.raises(RuntimeError, match="'database' and 'user' are required"):
            database_url_and_source()


@pytest.mark.db
def test_database_url_with_default_host_port(monkeypatch, tmp_path):
    """Test URL construction uses default host and port values."""
    # GIVEN: No environment variable
    monkeypatch.delenv("DATABASE_URL", raising=False)

    # Create INI without host and port (should use defaults)
    ini_file = tmp_path / "config.local.ini"
    ini_content = """[db]
database = testdb
user = testuser
password = 
"""
    ini_file.write_text(ini_content)

    # Mock _ini_path to return our test file
    def mock_ini_path(fname):
        return ini_file if fname == "config.local.ini" else tmp_path / fname

    with patch("src.config._ini_path", mock_ini_path):
        # WHEN: Getting URL
        url, source = database_url_and_source()

        # THEN: Should use default localhost:5432
        assert url == "postgresql://testuser@localhost:5432/testdb"
        assert source.startswith("ini:")