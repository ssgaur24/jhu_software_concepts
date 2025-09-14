# resolve DB URL: first ENV variable, then config.local.ini, then committed config.ini
import os, configparser, pathlib
from urllib.parse import quote

def _ini_path(fname: str) -> pathlib.Path:
    # locate ini next to module_3/
    return pathlib.Path(__file__).resolve().parents[1] / fname

def _build_url(user: str, password: str, host: str, port: str, database: str) -> str:
    # build postgresql://user[:pass]@host:port/db (omit :pass if blank)
    u = quote(user or "", safe="")
    p = f":{quote(password, safe='')}" if password else ""
    return f"postgresql://{u}{p}@{host}:{port}/{database}"

def database_url() -> str:
    # prefer DATABASE_URL if set
    env = os.getenv("DATABASE_URL")
    if env:
        return env

    # prefer developer's local override if present
    for fname in ("config.local.ini", "config.ini"):
        ini = _ini_path(fname)
        if ini.exists():
            cfg = configparser.ConfigParser()
            cfg.read(ini)
            db = cfg["db"]
            host = db.get("host", "localhost")
            port = db.get("port", "5432")
            database = db.get("database", "")
            user = db.get("user", "")
            password = db.get("password", "")
            if not database or not user:
                raise RuntimeError(f"{fname}: 'database' and 'user' are required")
            return _build_url(user, password, host, port, database)

    raise RuntimeError("DATABASE_URL not set and no config.local.ini or config.ini found")
