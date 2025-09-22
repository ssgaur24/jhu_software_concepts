"""
Incremental LLM standardizer (STRICT; no auto-pip; host can run in a separate venv).

- Requires DB URL from env OR module_3/config.local.ini OR module_3/config.ini.
  Supports either:
    [database] url=postgresql://user:pass@host:5432/db
  or:
    [db] host=..., port=..., database=..., user=..., password=...

- Computes NEW rows by comparing p_id against public.applicants.
- Runs the LLM host (llm_hosting/app.py) with a selectable Python:
    env LLM_PY = path to the Python interpreter to run the LLM host.
    If not set, defaults to the current interpreter.

- No dependency installation here. Instead, preflight-checks that the host
  interpreter can import the required modules and prints the exact pip command
  to fix it in THAT interpreter.

- Converts JSONL -> JSON array: llm_extend_applicant_data.json.
- Prints rc/stdout/stderr to the terminal.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg  # psycopg[binary]==3.2.10

BASE = Path(__file__).parent            # .../module_3/module_2_ref
CFG_ROOT = BASE.parent                  # .../module_3
APP_PY = BASE / "llm_hosting" / "app.py"

INPUT_JSON = BASE / "applicant_data.json"
INCR_JSON = BASE / "llm_input.json"     # temp (overwritten)
JSONL_OUT = BASE / "llm_output.jsonl"   # temp (overwritten)
OUT_JSON = BASE / "llm_extend_applicant_data.json"

_ID_RE = re.compile(r"/result/(\d+)")


def _stable_id(rec: Dict[str, Any]) -> Optional[int]:
    """Return stable numeric id from p_id or entry_url '/result/<id>'."""
    if isinstance(rec.get("p_id"), (int, float)) and int(rec["p_id"]) > 0:
        return int(rec["p_id"])
    u = rec.get("entry_url") or rec.get("url") or ""
    m = _ID_RE.search(str(u))
    return int(m.group(1)) if m else None


def _mask_url(url: str) -> str:
    """Mask password for display: postgresql://user@host:port/db."""
    try:
        m = re.match(r"^(\w+?)://([^:@/]+)(?::[^@]*)?@([^/]+?)/(.+)$", url)
        if m:
            proto, user, hostport, db = m.groups()
            return f"{proto}://{user}@{hostport}/{db}"
    except Exception:
        pass
    return url


def _read_db_url_from_ini(path: Path) -> Optional[str]:
    """Support [database] url=... OR [db] split keys."""
    if not path.exists():
        return None
    txt = path.read_text(encoding="utf-8", errors="ignore")
    section = None
    kv: Dict[str, str] = {}
    for line in txt.splitlines():
        s = line.strip()
        if not s or s.startswith(("#", ";")):
            continue
        if s.startswith("[") and s.endswith("]"):
            section = s[1:-1].strip().lower()
            continue
        if "=" in s and section:
            k, v = s.split("=", 1)
            k = k.strip().lower()
            v = v.strip()
            if section == "database" and k == "url":
                return v
            if section == "db" and k in {"host", "port", "database", "user", "password"}:
                kv[k] = v
    if kv:
        host = kv.get("host", "localhost")
        port = kv.get("port", "5432")
        db = kv.get("database", "")
        user = kv.get("user", "")
        pwd = kv.get("password", "")
        if not db or not user:
            return None
        auth = f"{user}:{pwd}@" if pwd != "" else f"{user}@"
        return f"postgresql://{auth}{host}:{port}/{db}"
    return None


def _get_db_url_required() -> str:
    """Probe env + common paths; print what succeeded and where it looked."""
    env = os.getenv("DATABASE_URL")
    if env:
        print(f"[LLM] using Python for RUNNER: {sys.executable}", flush=True)
        print(f"[LLM] config source: env | {_mask_url(env)}", flush=True)
        return env

    candidates: List[Path] = [
        CFG_ROOT / "config.local.ini",
        CFG_ROOT / "config.ini",
        Path.cwd() / "module_3" / "config.local.ini",
        Path.cwd() / "module_3" / "config.ini",
        Path.cwd() / "config.local.ini",
        Path.cwd() / "config.ini",
    ]
    print(f"[LLM] using Python for RUNNER: {sys.executable}", flush=True)
    print(f"[LLM] probing config paths (run.py at: {BASE})", flush=True)
    for p in candidates:
        status = "exists" if p.exists() else "missing"
        print(f"[LLM]  - try {p} [{status}]", flush=True)
        if not p.exists():
            continue
        url = _read_db_url_from_ini(p)
        if url:
            print(f"[LLM] config source: {p.name} | {_mask_url(url)}", flush=True)
            return url

    print("[LLM] ERROR: DATABASE_URL not set and no usable config found in the probed paths.", flush=True)
    sys.exit(2)


def _existing_ids(db_url: str) -> set[int]:
    """Fetch current p_id set from public.applicants; exit on failure."""
    try:
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT p_id FROM public.applicants;")
            return {int(r[0]) for r in cur.fetchall()}
    except Exception as e:
        print(f"[LLM] ERROR: cannot connect to DB or query p_id: {e}", flush=True)
        sys.exit(2)


def _require_import(host_py: str, module: str, pip_pkg: Optional[str] = None) -> None:
    """Ensure the HOST interpreter can import a module; print exact pip cmd otherwise."""
    test = subprocess.run(
        [host_py, "-c", f"import {module}"],
        capture_output=True, text=True, encoding="utf-8"
    )
    if test.returncode != 0:
        pkg = pip_pkg or module
        print(f"[LLM] ERROR: host Python cannot import '{module}'.", flush=True)
        print(f"[LLM] Host Python: {host_py}", flush=True)
        print(f'[LLM] Install into HOST venv:\n  "{host_py}" -m pip install {pkg}', flush=True)
        if module == "llama_cpp":
            print("[LLM] Note: llama-cpp-python may not have wheels for Python 3.13 on Windows. "
                  "Use a Python 3.11 host venv and set LLM_PY to its python.exe.", flush=True)
        sys.exit(2)


def main() -> None:
    # Sanity checks
    if not APP_PY.exists():
        print("LLM host app missing at module_3/module_2_ref/llm_hosting/app.py", flush=True)
        sys.exit(2)
    if not INPUT_JSON.exists():
        print("Scraped input missing at module_3/module_2_ref/applicant_data.json", flush=True)
        sys.exit(2)

    # DB config + current ids
    db_url = _get_db_url_required()
    known = _existing_ids(db_url)

    # Strict incremental set
    raw = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    new_rows = []
    for rec in raw:
        sid = _stable_id(rec)
        if sid is not None and sid not in known:
            new_rows.append(rec)
    print(f"[LLM] raw={len(raw)} known_ids={len(known)} new_rows={len(new_rows)}", flush=True)

    if not new_rows:
        OUT_JSON.write_text("[]", encoding="utf-8")
        print("[LLM] nothing new; wrote empty array", flush=True)
        sys.exit(0)

    # Choose HOST interpreter (can be a different venv)
    host_py = os.getenv("LLM_PY") or sys.executable
    print(f"[LLM] using Python for HOST: {host_py}", flush=True)

    # Preflight: ensure host env has needed imports
    _require_import(host_py, "huggingface_hub", '"huggingface_hub>=0.23,<1"')
    _require_import(host_py, "llama_cpp", "llama-cpp-python<0.3.0,>=0.2.90")

    # Prepare input for host
    INCR_JSON.write_text(json.dumps(new_rows, ensure_ascii=False), encoding="utf-8")
    if JSONL_OUT.exists():
        try:
            JSONL_OUT.unlink()
        except Exception:
            pass

    # Run host under the HOST interpreter
    proc = subprocess.run(
        [host_py, str(APP_PY), "--file", str(INCR_JSON), "--out", str(JSONL_OUT)],
        capture_output=True, text=True, encoding="utf-8"
    )
    print(f"[LLM] host rc={proc.returncode}", flush=True)
    if proc.stdout:
        print(f"[LLM] host stdout:\n{proc.stdout}", flush=True)
    if proc.stderr:
        print(f"[LLM] host stderr:\n{proc.stderr}", flush=True)

    # Validate output
    if proc.returncode != 0 or not JSONL_OUT.exists():
        print("[LLM] ERROR: host failed or no JSONL produced", flush=True)
        sys.exit(3)

    rows: List[Dict[str, Any]] = []
    with JSONL_OUT.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception as e:
                print(f"[LLM] JSONL parse error: {e} :: {s[:200]}", flush=True)

    if not rows:
        print("[LLM] ERROR: host produced 0 standardized rows", flush=True)
        sys.exit(4)

    OUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[LLM] wrote standardized rows={len(rows)} -> {OUT_JSON}", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()
