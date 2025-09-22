import pytest
from types import SimpleNamespace

@pytest.mark.db
def test_get_conn_and_close_pool(monkeypatch):
    # fake psycopg_pool.ConnectionPool and connection
    class _FakeConn:
        def close(self): pass
    class _FakePool:
        def __init__(self, dsn, **kw): self.conn = _FakeConn()
        def getconn(self): return self.conn
        def putconn(self, conn): pass
        def close(self): pass

    # patch module under test
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setattr("src.dal.pool.ConnectionPool", _FakePool, raising=True)

    # import after patches so module picks them up
    import importlib, src.dal.pool as pool
    importlib.reload(pool)

    # first call builds pool + returns conn
    conn1 = pool.get_conn()
    assert conn1 is not None

    # second call reuses pool
    conn2 = pool.get_conn()
    assert conn2 is not None

    # close_pool path
    pool.close_pool()
