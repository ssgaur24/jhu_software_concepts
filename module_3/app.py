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
def _run(cmd: list[str], cwd: str) -> tuple[int, str]:
    """
    Run a subprocess and return (rc, tail_of_stdout).
    We capture output so the UI can show a short note.
    """
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, encoding="utf-8")
    tail = (p.stdout or "")[-300:]
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
        here = os.path.dirname(__file__)                  # .../module_3
        repo = os.path.abspath(os.path.join(here, ".."))  # repo root

        # 1) SCRAPE
        scrape_py = os.path.join(repo, "module_2", "scrape.py")
        if os.path.exists(scrape_py):
            rc, tail = _run([sys.executable, "-u", scrape_py], cwd=here)
            if rc != 0:
                clear_pull_lock()
                return redirect(url_for("index", msg=f"Pull step failed in scrape (rc={rc}).\n{tail}", level="error"))

        # 2) CLEAN
        clean_py = os.path.join(repo, "module_2", "clean.py")
        if os.path.exists(clean_py):
            rc, tail = _run([sys.executable, "-u", clean_py], cwd=here)
            if rc != 0:
                clear_pull_lock()
                return redirect(url_for("index", msg=f"Pull step failed in clean (rc={rc}).\n{tail}", level="error"))

        # Paths for LLM standardizer
        llm_app = os.path.join(repo, "module_2", "llm_hosting", "app.py")
        applicant_json = os.path.join(repo, "module_2", "applicant_data.json")

        # Output (Module 3 expects/uses this path by default)
        out_dir = os.path.join(here, "data")
        os.makedirs(out_dir, exist_ok=True)
        extended_json = os.path.join(out_dir, "module2_llm_extend_applicant_data.json")

        # 3) LLM HOSTING — standardize only new data if previous extended exists
        llm_cmd = [sys.executable, "-u", llm_app, "--file", applicant_json]

        # If you implement --only-new / --prev, use them:
        if os.path.exists(extended_json):
            llm_cmd += ["--only-new", "--prev", extended_json]

        # Run llm_hosting and capture stdout (the extended JSON)
        p = subprocess.run(llm_cmd, cwd=here, capture_output=True, text=True, encoding="utf-8")
        if p.returncode != 0:
            clear_pull_lock()
            tail = (p.stdout or "")[-300:]
            return redirect(url_for("index", msg=f"LLM step failed (rc={p.returncode}).\n{tail or p.stderr}", level="error"))

        # Save stdout to extended_json
        try:
            data = p.stdout.strip()
            # sanity: must be a JSON array/object
            if not data.startswith("{") and not data.startswith("["):
                raise ValueError("LLM output is not valid JSON")
            with open(extended_json, "w", encoding="utf-8") as f:
                f.write(data)
        except Exception as e:
            clear_pull_lock()
            return redirect(url_for("index", msg=f"Failed to write extended JSON: {e}", level="error"))

        # 4) LOAD into DB (explicitly pass the file path)
        rc, tail = _run([sys.executable, "-u", os.path.join(here, "load_data.py"), extended_json], cwd=here)
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
