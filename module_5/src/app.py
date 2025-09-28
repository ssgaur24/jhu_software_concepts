# -*- coding: utf-8 -*-
"""
app.py â€"  Flask UI for Module 3 (scrape â†' clean â†' LLM â†' load)

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
    """Check if a data pull operation is currently running."""
    return os.path.exists(LOCK_PATH)


def start_pull_lock() -> None:
    """Create a lock file to indicate a pull operation is starting."""
    with open(LOCK_PATH, "w", encoding="utf-8") as f:
        f.write("running")


def clear_pull_lock() -> None:
    """Remove the pull lock file."""
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except OSError:
        pass


# ---- tiny subprocess helper ----
def _run(cmd: list[str], cwd: str, env: dict | None = None) -> tuple[int, str]:
    """
    Run a subprocess and return (rc, tail_of_stdout_and_stderr).
    We include stderr so failures are visible in the UI.
    """
    p = subprocess.run(
        cmd, cwd=cwd, env=env,
        capture_output=True, text=True, encoding="utf-8", shell=False, check=False
    )
    out = p.stdout or ""
    err = p.stderr or ""
    tail = (out + ("\n" if out and err else "") + err)[-800:]
    return p.returncode, tail


def _build_environment(repo_path: str) -> dict:
    """Build environment with PYTHONPATH for subprocess execution."""
    env = os.environ.copy()
    env["PYTHONPATH"] = repo_path + os.pathsep + env.get("PYTHONPATH", "")
    return env


def _validate_json_output(data: str) -> bool:
    """Check if string appears to be valid JSON."""
    data = data.strip()
    return data.startswith("{") or data.startswith("[")


# ------------------------------- routes ---------------------------------------

@app.route("/")
def index():
    """Main page showing applicant data and status."""
    rows: List[Tuple[str, str]] = get_rows()
    return render_template(
        "index.html",
        rows=rows,
        pull_running=is_pull_running(),
        status_msg=request.args.get("msg", ""),
        status_level=request.args.get("level", "info"),
        report_exists=False,
    )


def _run_pipeline_step(script_name: str, cwd: str, env: dict) -> tuple[bool, str]:
    """Run a single pipeline step and return (success, error_message)."""
    script_path = os.path.join(cwd, script_name)
    if not os.path.exists(script_path):
        return True, ""  # Skip if script doesn't exist

    rc, tail = _run([sys.executable, "-u", script_name], cwd=cwd, env=env)
    if rc != 0:
        return False, f"Pull step failed in {script_name} (rc={rc}).\n{tail}"
    return True, ""


def _run_llm_step(mod2_dir: str, llm_dir: str, extended_json: str, env: dict) -> tuple[bool, str]:
    """Run LLM processing step and return (success, error_message)."""
    llm_cmd = [
        sys.executable, "-u", "app.py",
        "--file", os.path.join(mod2_dir, "applicant_data.json")
    ]
    if os.path.exists(extended_json):
        llm_cmd += ["--only-new", "--prev", extended_json]

    p = subprocess.run(
        llm_cmd, cwd=llm_dir, env=env,
        capture_output=True, text=True, encoding="utf-8", check=False
    )
    if p.returncode != 0:
        tail = (p.stdout or "") + ("\n" if p.stdout and p.stderr else "") + (p.stderr or "")
        tail = tail[-800:]
        return False, f"LLM step failed (rc={p.returncode}).\n{tail}"

    data = (p.stdout or "").strip()
    if not _validate_json_output(data):
        return False, "LLM step output is not valid JSON."

    with open(extended_json, "w", encoding="utf-8") as f:
        f.write(data)
    return True, ""


def _execute_full_pipeline(here: str, mod2_dir: str,
                           llm_dir: str, env: dict) -> tuple[bool, str]:
    """Execute the complete data pipeline and return (success, message)."""
    # 1) SCRAPE
    success, error_msg = _run_pipeline_step("scrape.py", mod2_dir, env)
    if not success:
        return False, error_msg

    # 2) CLEAN
    success, error_msg = _run_pipeline_step("clean.py", mod2_dir, env)
    if not success:
        return False, error_msg

    # Check for required input file
    applicant_json = os.path.join(mod2_dir, "applicant_data.json")
    if not os.path.exists(applicant_json):
        return False, ("Pull failed: module_2/applicant_data.json not found after scrape/clean. "
                       "Make sure scrape.py/clean.py write to that path (or adjust app.py).")

    # 3) LLM processing
    out_dir = os.path.join(here, "data")
    os.makedirs(out_dir, exist_ok=True)
    extended_json = os.path.join(out_dir, "module2_llm_extend_applicant_data.json")

    success, error_msg = _run_llm_step(mod2_dir, llm_dir, extended_json, env)
    if not success:
        return False, error_msg

    # 4) LOAD into DB
    rc, tail = _run(
        [sys.executable, "-u", "load_data.py", extended_json],
        cwd=here, env=env
    )
    if rc != 0:
        return False, f"Load step failed (rc={rc}).\n{tail}"

    return True, "Pull complete. New data (if any) added."


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
        return redirect(url_for(
            "index",
            msg="A pull is already running. Please wait.",
            level="warn"
        ))

    start_pull_lock()
    try:
        here = os.path.dirname(__file__)
        repo = os.path.abspath(os.path.join(here, "../.."))
        mod2_dir = os.path.join(repo, "module_2")
        llm_dir = os.path.join(mod2_dir, "llm_hosting")
        env = _build_environment(repo)

        success, message = _execute_full_pipeline(here, mod2_dir, llm_dir, env)
        clear_pull_lock()

        level = "success" if success else "error"
        return redirect(url_for("index", msg=message, level=level))

    except OSError as e:
        clear_pull_lock()
        return redirect(url_for("index", msg=f"Pull failed: {e}", level="error"))


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    """Update analysis if no pull is running."""
    if is_pull_running():
        return redirect(url_for(
            "index",
            msg="Update ignored: a data pull is currently running. Try again after it finishes.",
            level="warn"
        ))
    return redirect(url_for(
        "index",
        msg="Analysis updated with the latest data.",
        level="success"
    ))


@app.route("/health")
def health():
    """Health check endpoint."""
    return {"ok": True, "pull_running": is_pull_running()}


if __name__ == "__main__":
    # Starts the Flask dev server on http://127.0.0.1:5000/
    # Change host/port if needed (e.g., host="0.0.0.0", port=5057)
    app.run(host="127.0.0.1", port=5000, debug=False)
