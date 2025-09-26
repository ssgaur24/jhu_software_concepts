# -*- coding: utf-8 -*-
"""
app.py —  Flask UI for Module 3 (scrape → clean → LLM → load)

What this does
--------------
- "Pull Data" runs your Module 2 pipeline:
    1) python module_2/scrape.py
    2) python module_2/clean.py
    3) python module_2/llm_hosting/app.py --file module_2/applicant_data.json
       (stdout is saved to module_3/data/module2_llm_extend_applicant_data.json)
    4) python module_3/load_data.py module_3/data/module2_llm_extend_applicant_data.json
- "Update Analysis" simply refreshes results unless a pull is running.
- Queries are reused from query_data.get_rows().

Note
----
This file assumes a tiny enhancement has been added to module_2/llm_hosting/app.py:
- support flags: --only-new (process only new items) and --prev <existing_extended_json>
I pass these flags if the previous extended file exists.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import List, Tuple

from flask import Flask, render_template, redirect, url_for, request
from query_data import get_rows

app = Flask(__name__)

# ---- simple file lock so Update does nothing during a pull ----
LOCK_PATH = os.path.join(os.path.dirname(__file__), "pull.lock")

def is_pull_running() -> bool:
    return os.path.exists(LOCK_PATH)

def start_pull_lock() -> None:
    with open(LOCK_PATH, "w", encoding="utf-8") as f:
        f.write("running")

def clear_pull_lock() -> None:
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except Exception:
        pass


# ---- tiny subprocess helper ----
def _run(cmd: list[str], cwd: str, env: dict | None = None) -> tuple[int, str]:
    """
    Run a subprocess and return (rc, tail_of_stdout_and_stderr).
    We include stderr so failures are visible in the UI.
    """
    p = subprocess.run(
        cmd, cwd=cwd, env=env,
        capture_output=True, text=True, encoding="utf-8", shell=False
    )
    out = p.stdout or ""
    err = p.stderr or ""
    tail = (out + ("\n" if out and err else "") + err)[-800:]
    return p.returncode, tail



# ------------------------------- routes ---------------------------------------

@app.route("/")
def index():
    rows: List[Tuple[str, str]] = get_rows()
    return render_template(
        "index.html",
        rows=rows,
        pull_running=is_pull_running(),
        status_msg=request.args.get("msg", ""),
        status_level=request.args.get("level", "info"),
        report_exists=False,
    )


@app.route("/pull-data", methods=["POST"])
def pull_data():
    """
    Pipeline:
      module_2/scrape.py  -> module_2/clean.py ->
      module_2/llm_hosting/app.py --file module_2/applicant_data.json [--only-new --prev ...]
        (stdout -> module_3/data/module2_llm_extend_applicant_data.json)
      module_3/load_data.py module_3/data/module2_llm_extend_applicant_data.json
    """
    if is_pull_running():
        return redirect(url_for("index", msg="A pull is already running. Please wait.", level="warn"))

    start_pull_lock()
    try:
        here = os.path.dirname(__file__)  # .../module_3
        repo = os.path.abspath(os.path.join(here, ".."))
        mod2_dir = os.path.join(repo, "module_2")
        llm_dir = os.path.join(mod2_dir, "llm_hosting")

        # Make local imports inside module_2 work even if they do "import x" relative to repo
        env = os.environ.copy()
        env["PYTHONPATH"] = repo + os.pathsep + env.get("PYTHONPATH", "")

        # 1) SCRAPE (cwd = module_2)
        scrape_py = os.path.join(mod2_dir, "scrape.py")
        if os.path.exists(scrape_py):
            rc, tail = _run([sys.executable, "-u", "scrape.py"], cwd=mod2_dir, env=env)
            if rc != 0:
                clear_pull_lock()
                return redirect(url_for("index", msg=f"Pull step failed in scrape (rc={rc}).\n{tail}", level="error"))

        # 2) CLEAN (cwd = module_2)
        clean_py = os.path.join(mod2_dir, "clean.py")
        if os.path.exists(clean_py):
            rc, tail = _run([sys.executable, "-u", "clean.py"], cwd=mod2_dir, env=env)
            if rc != 0:
                clear_pull_lock()
                return redirect(url_for("index", msg=f"Pull step failed in clean (rc={rc}).\n{tail}", level="error"))

        # Expect Module 2 to write applicant_data.json in module_2/
        applicant_json = os.path.join(mod2_dir, "applicant_data.json")
        if not os.path.exists(applicant_json):
            clear_pull_lock()
            return redirect(url_for(
                "index",
                msg=("Pull failed: module_2/applicant_data.json not found after scrape/clean. "
                     "Make sure scrape.py/clean.py write to that path (or adjust app.py)."),
                level="error",
            ))

        # 3) LLM standardizer (cwd = module_2/llm_hosting)
        llm_app = os.path.join(llm_dir, "app.py")
        out_dir = os.path.join(here, "data")
        os.makedirs(out_dir, exist_ok=True)
        extended_json = os.path.join(out_dir, "module2_llm_extend_applicant_data.json")

        llm_cmd = [sys.executable, "-u", "app.py", "--file", os.path.join(mod2_dir, "applicant_data.json")]
        if os.path.exists(extended_json):
            llm_cmd += ["--only-new", "--prev", extended_json]

        # run and capture stdout explicitly (we need the JSON body)
        p = subprocess.run(llm_cmd, cwd=llm_dir, env=env, capture_output=True, text=True, encoding="utf-8")
        if p.returncode != 0:
            clear_pull_lock()
            tail = (p.stdout or "") + ("\n" if p.stdout and p.stderr else "") + (p.stderr or "")
            tail = tail[-800:]
            return redirect(url_for("index", msg=f"LLM step failed (rc={p.returncode}).\n{tail}", level="error"))

        # Save stdout -> module_3/data/module2_llm_extend_applicant_data.json
        data = (p.stdout or "").strip()
        if not data.startswith("{") and not data.startswith("["):
            clear_pull_lock()
            return redirect(url_for("index", msg="LLM step output is not valid JSON.", level="error"))
        with open(extended_json, "w", encoding="utf-8") as f:
            f.write(data)

        # 4) LOAD into DB (cwd = module_3; pass explicit file path)
        rc, tail = _run([sys.executable, "-u", "load_data.py", extended_json], cwd=here, env=env)
        if rc != 0:
            clear_pull_lock()
            return redirect(url_for("index", msg=f"Load step failed (rc={rc}).\n{tail}", level="error"))

        clear_pull_lock()
        return redirect(url_for("index", msg="Pull complete. New data (if any) added.", level="success"))

    except Exception as e:
        clear_pull_lock()
        return redirect(url_for("index", msg=f"Pull failed: {e}", level="error"))


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    if is_pull_running():
        return redirect(url_for(
            "index",
            msg="Update ignored: a data pull is currently running. Try again after it finishes.",
            level="warn"
        ))
    return redirect(url_for("index", msg="Analysis updated with the latest data.", level="success"))


@app.route("/health")
def health():
    return {"ok": True, "pull_running": is_pull_running()}

if __name__ == "__main__":
    # Starts the Flask dev server on http://127.0.0.1:5000/
    # Change host/port if needed (e.g., host="0.0.0.0", port=5057)
    app.run(host="127.0.0.1", port=5000, debug=False)
