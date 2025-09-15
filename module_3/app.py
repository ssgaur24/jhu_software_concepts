"""Flask UI for Module 3 with Part B buttons.

Buttons (per Part B):
  1) Pull Data        -> scrape + clean + LLM standardize + load DB (with lock)
  2) Update Analysis  -> recompute/refresh answers ONLY if no pull is running

Notes:
- Uses LLM fields for university/program logic; caps: GPA<=5, GRE<=400.
- Installs module_2_ref minimal deps (bs4, urllib3) on first Pull Data click if missing.
- Nicer status alerts: level = info | success | error with short, user-friendly text.
"""

from __future__ import annotations

import re
import sys
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

from flask import Flask, render_template, jsonify, request, redirect, url_for

from src.dal.pool import close_pool  # close threads on shutdown
from query_data import (
    q1_count_fall_2025,
    q2_pct_international,
    q3_avgs,
    q4_avg_gpa_american_fall2025,
    q5_pct_accept_fall2025,
    q6_avg_gpa_accept_fall2025,
    q7_count_jhu_masters_cs,
    q8_count_2025_georgetown_phd_cs_accept,
    q9_top5_accept_unis_2025,
    q10_avg_gre_by_status_year,
    q10_avg_gre_by_status_last_n_years,
)

app = Flask(__name__)  # create app

# Paths: everything self-contained under module_3/
M3_DIR = Path(__file__).resolve().parent
M2_REF_DIR = M3_DIR / "module_2_ref"  # reuse of Module-2 pieces for buttons
M3_DATA = M3_DIR / "data"
ART_DIR = M3_DIR / "artifacts"
LOCK_FILE = ART_DIR / "pull.lock"
M3_DATA.mkdir(parents=True, exist_ok=True)
ART_DIR.mkdir(parents=True, exist_ok=True)


def _fmt_float(x: Optional[float], nd: int = 2) -> str:
    """Format float/None to fixed decimals or NA (student helper)."""
    return f"{x:.{nd}f}" if isinstance(x, (float, int)) else "NA"


def _format_q3(gpa: Optional[float], gre: Optional[float],
               gre_v: Optional[float], gre_aw: Optional[float]) -> str:
    """Build labeled average metrics string with NA suppression (student helper)."""
    parts: List[str] = []
    if gpa is not None:
        parts.append(f"Average GPA: {_fmt_float(gpa)}")
    if gre is not None:
        parts.append(f"Average GRE: {_fmt_float(gre)}")
    if gre_v is not None:
        parts.append(f"Average GRE V: {_fmt_float(gre_v)}")
    if gre_aw is not None:
        parts.append(f"Average GRE AW: {_fmt_float(gre_aw)}")
    return "NA" if not parts else ", ".join(parts)


def _format_status_avgs(rows: Iterable[Tuple[str, Optional[float]]]) -> str:
    """Format (status, avg) rows with NA suppression (student helper)."""
    parts = [f"{status}: {_fmt_float(avg)}" for status, avg in rows if avg is not None]
    return "NA" if not parts else ", ".join(parts)


def _run(cmd: list[str], cwd: Path) -> tuple[int, str, str]:
    """Run a subprocess and return (rc, stdout, stderr)."""
    proc = subprocess.run(
        cmd, cwd=str(cwd), capture_output=True, text=True, encoding="utf-8"
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _ensure_m2_deps() -> str:
    """Install tiny scrape deps on demand (student helper)."""
    req = M2_REF_DIR / "requirements.txt"
    if req.exists():
        rc, out, err = _run([sys.executable, "-m", "pip", "install", "-r", str(req)], cwd=M3_DIR)
        return f"deps_rc={rc}"
    rc, out, err = _run([sys.executable, "-m", "pip", "install", "urllib3>=2.2.0", "beautifulsoup4>=4.12.0"], cwd=M3_DIR)
    return f"deps_rc={rc}"


def _build_rows() -> List[Tuple[str, str]]:
    """Compute all Q&A rows for rendering (student helper)."""
    q1 = q1_count_fall_2025()
    q2 = q2_pct_international()
    gpa, gre, gre_v, gre_aw = q3_avgs()
    q4 = q4_avg_gpa_american_fall2025()
    q5 = q5_pct_accept_fall2025()
    q6 = q6_avg_gpa_accept_fall2025()
    q7 = q7_count_jhu_masters_cs()
    q8 = q8_count_2025_georgetown_phd_cs_accept()
    q9 = q9_top5_accept_unis_2025()
    q10_2024 = q10_avg_gre_by_status_year(2024)
    q10_last3 = q10_avg_gre_by_status_last_n_years(3)

    rows = [
        ("How many entries do you have in your database who have applied for Fall 2025?",
         f"Applicant count: {q1}"),
        ("What percentage of entries are from International students (not American or Other) (to two decimal places)?",
         f"Percent International: {_fmt_float(q2)}%"),
        ("What is the average GPA, GRE, GRE V, GRE AW of applicants who provided these metrics?",
         _format_q3(gpa, gre, gre_v, gre_aw)),
        ("What is the average GPA of American students in Fall 2025?",
         f"Average GPA American: {_fmt_float(q4)}"),
        ("What percent of entries for Fall 2025 are Acceptances (to two decimal places)?",
         f"Acceptance percent: {_fmt_float(q5)}%"),
        ("What is the average GPA of applicants who applied for Fall 2025 who are Acceptances?",
         f"Average GPA Acceptance: {_fmt_float(q6)}"),
        ("How many entries are from applicants who applied to JHU for a masters degrees in Computer Science?",
         f"Count: {q7}"),
        ("How many entries from 2025 are acceptances from applicants who applied to Georgetown University for a PhD in Computer Science?",
         f"Count: {q8}"),
        ("Top 5 universities by acceptances in 2025 (count).",
         (", ".join([f"{u}={c}" for (u, c) in q9]) if q9 else "NA")),
        ("Average GRE by Status (2024).",
         _format_status_avgs(q10_2024)),
        ("Average GRE by Status (last 3 calendar years).",
         _format_status_avgs(q10_last3)),
    ]
    return rows


def _go(level: str, msg: str):
    """Redirect to index with a status level (student helper)."""
    return redirect(url_for("index", msg=msg, level=level))


@app.route("/")
def index():
    """Render Analysis with status message and footnote if audit file exists."""
    report_exists = (M3_DIR / "artifacts" / "load_report.json").exists()
    msg = request.args.get("msg", "")
    level = request.args.get("level", "info")
    lock = LOCK_FILE.exists()
    return render_template("index.html",
                           rows=_build_rows(),
                           report_exists=report_exists,
                           status_msg=msg,
                           status_level=level,
                           pull_running=lock)


@app.route("/pull-data", methods=["POST"])
def pull_data():
    """Button 1: scrape + clean + LLM + load (guarded by lock)."""
    if LOCK_FILE.exists():
        return _go("info", "A data pull is already running—please wait until it finishes.")

    # create lock
    LOCK_FILE.write_text("running", encoding="utf-8")
    try:
        _ensure_m2_deps()

        # 1) scrape (creates applicant_data.json in module_2_ref)
        rc1, out1, err1 = _run([sys.executable, "scrape.py"], cwd=M2_REF_DIR)
        data_json = M2_REF_DIR / "applicant_data.json"
        if rc1 != 0 or not data_json.exists():
            return _go(
                "error",
                "Pull failed while fetching new posts. The scraper did not produce "
                "module_3/module_2_ref/applicant_data.json. Check network access and try again.",
            )

        # 2) clean
        rc2, out2, err2 = _run([sys.executable, "clean.py"], cwd=M2_REF_DIR)
        if rc2 != 0:
            return _go("error", "Pull failed at the clean step. See console for details.")

        # 3) LLM standardize -> llm_extend_applicant_data.json
        llm_dir = M2_REF_DIR / "llm_hosting"
        if not llm_dir.exists():
            return _go("error", "LLM files missing at module_3/module_2_ref/llm_hosting.")
        rc3, out3, err3 = _run([sys.executable, "run.py"], cwd=M2_REF_DIR)
        out_json = M2_REF_DIR / "llm_extend_applicant_data.json"
        if rc3 != 0 or not out_json.exists():
            return _go("error", "LLM step completed with errors—could not produce standardized JSON.")

        # 4) copy to module_3/data and load
        dest = M3_DATA / "module_2llm_extend_applicant_data.json"
        dest.write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
        rc4, out4, err4 = _run(
            [sys.executable, "load_data.py", "--init", "--load", str(dest), "--batch", "2000", "--count"],
            cwd=M3_DIR,
        )
        if rc4 != 0:
            return _go("error", "Database load failed. Check database connectivity and config.")

        # success summary
        row_count = None
        m = re.search(r"row_count=(\d+)", out4 or "")
        if m:
            row_count = int(m.group(1))
        msg = "Pull complete. Analysis now includes the newest rows."
        if row_count is not None:
            msg += f" Current row count: {row_count}."
        return _go("success", msg)

    finally:
        try:
            LOCK_FILE.unlink(missing_ok=True)
        except Exception:
            pass


@app.route("/update-analysis", methods=["POST"])
def update_analysis():
    """Button 2: refresh answers only if no pull is running."""
    if LOCK_FILE.exists():
        return _go("info", "A data pull is running; Update Analysis is temporarily disabled.")
    return _go("success", "Analysis refreshed with the most recent data in the database.")


@app.route("/health")
def health():
    """Tiny health endpoint."""
    return jsonify(ok=True)


@app.teardown_appcontext
def _shutdown(exc):
    """Close the pool threads when the app stops (student cleanup)."""
    close_pool()


if __name__ == "__main__":
    # run simple dev server
    app.run(host="127.0.0.1", port=5000)
