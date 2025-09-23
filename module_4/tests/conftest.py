# module_4/tests/conftest.py
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock as Mock

# Import app factory from the real package path under module_4/src
from src.flask_app import create_app

@pytest.fixture()
def app():
    # ensure BUSY=False by default so tests can toggle it
    return create_app({"TESTING": True, "BUSY": False})

@pytest.fixture()
def client(app):
    return app.test_client()

@pytest.fixture(autouse=True)
def fake_db(monkeypatch):
    """
    Autouse: monkeypatch src.dal.pool.get_conn to return a context-managed
    connection + cursor so query_data calls don't touch a real DB.
    """
    cur = Mock()
    # sensible defaults for SELECTs used by query_data
    cur.fetchone.return_value = (0,)
    cur.fetchall.return_value = []

    cur_cm = Mock()
    cur_cm.__enter__.return_value = cur
    cur_cm.__exit__.return_value = False

    conn = Mock()
    conn.cursor.return_value = cur_cm
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False

    # Monkeypatch the path that flask_app/query_data actually import from
    monkeypatch.setattr("src.dal.pool.get_conn", lambda: conn)
