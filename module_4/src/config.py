"""Configuration helpers for Module 3.

Resolution order for the PostgreSQL URL:
1) ENV var DATABASE_URL (if set),
2) module_4/config.local.ini,
3) module_4/config.ini.

Utility functions also expose the selected source for debugging.
"""

import os
import configparser
import pathlib
from urllib.parse import quote, urlparse, urlunparse


def _ini_path(fname: str) -> pathlib.Path:
    """Absolute path to an INI filename adjacent to module_4/."""
    return pathlib.Path(__file__).resolve().parents[1] / fname  # locate file next to module_4/


def _build_url(user: str, password: str, host: str, port: str, database: str) -> str:
    """postgresql://user[:pass]@host:port/db (omit password segment if blank)."""
    # percent-encode credentials safely
    u = quote(user or "", safe="")  # encode username
    p = f":{quote(password, safe='')}" if password else ""  # encode password if present
    return f"postgresql://{u}{p}@{host}:{port}/{database}"  # build final URL


def database_url_and_source() -> tuple[str, str]:
    """Return (url, source) where source is 'env:DATABASE_URL' or 'ini:<file>'."""
    # 1) prefer ENV for production-style overrides
    env = os.getenv("DATABASE_URL")
    if env:
        return env, "env:DATABASE_URL"
    # 2) fallback to developer/local INI files
    for fname in ("config.local.ini", "config.ini"):
        ini = _ini_path(fname)
        if ini.exists():
            cfg = configparser.ConfigParser()  # parse INI
            cfg.read(ini)
            db = cfg["db"]
            host = db.get("host", "localhost")
            port = db.get("port", "5432")
            database = db.get("database", "")
            user = db.get("user", "")
            password = db.get("password", "")
            if not database or not user:
                raise RuntimeError(f"{fname}: 'database' and 'user' are required")
            return _build_url(user, password, host, port, database), f"ini:{fname}"
    # 3) nothing found — fail clearly
    raise RuntimeError("No DATABASE_URL, config.local.ini, or config.ini found")


def database_url() -> str:
    """Return only the resolved URL."""
    url, _ = database_url_and_source()  # get URL and ignore source
    return url  # provide the URL used by the DAL


def masked_url(url: str) -> str:
    """Return URL with password redacted for display."""
    # redact only the password for safe printing
    p = urlparse(url)
    if p.password:
        netloc = f"{p.username}:****@{p.hostname}"
        if p.port:
            netloc += f":{p.port}"
        return urlunparse((p.scheme, netloc, p.path, "", "", ""))
    return url
