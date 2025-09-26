# -*- coding: utf-8 -*-
"""
app.py — Flask UI for Module 3 (uses query_data.get_rows)

What this file does
-------------------
- Serves one page with two buttons:
    1) Pull Data        -> runs load_data.py (reads JSON and inserts into DB)
    2) Update Analysis  -> re-runs queries and refreshes the page
- Reuses the queries from query_data.get_rows() so there is no duplication.
- Keeps the variables your templates expect: rows, pull_running, status_msg, status_level, report_exists.
"""

from __future__ import annotations

import subprocess
import sys
from flask import Flask, render_template, redirect, url_for, request

# Reuse the single source of truth for Q1–Q12
from query_data import get_rows

app = Flask(__name__)


@app.route("/")
def index():
    # On GET, compute rows fresh via query_data.get_rows()
    return render_template(
        "index.html",
        rows=get_rows(),
        pull_running=False,                 # no async work in this minimal version
        status_msg=request.args.get("msg", ""),
        status_level=request.args.get("level", "info"),
        report_exists=False,                # flip to True if you later write a load_report.json
    )


@app.route("/pull-data", methods=["POST"])
def pull_data():
    """
    'Pull Data': just run load_data.py as a subprocess.
    - Relies on config.ini and default JSON path inside load_data.py.
    """
    try:
        proc = subprocess.run(
            [sys.executable, "load_data.py"],
            cwd="module_3",
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        rc = proc.returncode
        note = "Pull complete." if rc == 0 else "Pull failed."
        extra = (proc.stdout or proc.stderr or "")[-400:]  # show tail for quick debugging
        level = "success" if rc == 0 else "error"
        return redirect(url_for("index", msg=f"{note} (rc={rc})\n{extra}", level=level))
    except Exception as e:
        return redirect(url_for("index", msg=f"Pull failed: {e}", level="error"))


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    """
    Minimal 'Update Analysis': nothing to compute here;
    index() will recompute rows via get_rows() on redirect.
    """
    return redirect(url_for("index", msg="Analysis updated.", level="success"))


@app.route("/health")
def health():
    return {"ok": True}


if __name__ == "__main__":
    # Default Flask dev server (run from repo root or module_3/)
    # Example:
    #   set FLASK_APP=module_3/app.py && flask run  (Windows PowerShell)
    #   export FLASK_APP=module_3/app.py && flask run  (macOS/Linux)
    app.run(debug=True)
