"""Flask UI for Module 3 with Part B buttons.

Buttons (per Part B):
  1) Pull Data        -> scrape + clean + LLM standardize + load DB (with lock)
  2) Update Analysis  -> recompute/refresh answers ONLY if no pull is running

Notes:
- Uses LLM fields for university/program logic; caps: GPA<=5, GRE<=400 (handled in queries).
- Prints detailed subprocess stdout/stderr/return codes to the TERMINAL for every step,
  so LLM errors are visible when Pull Data fails.
"""

from __future__ import annotations

import re
import sys
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

from flask import Flask, render_template, jsonify, request, redirect, url_for

from src.dal.pool import close_pool  # clean shutdown
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
    # Optional extras; if not present in query_data.py, remove from imports.
    q11_top_unis_fall_2025,
    q12_status_breakdown_fall_2025,
)

app = Flask(__name__)

# Paths (all under module_3/)
M3_DIR = Path(__file__).resolve().parent
M2_REF_DIR = M3_DIR / "module_2_ref"
M3_DATA = M3_DIR / "data"
ART_DIR = M3_DIR / "artifacts"
LOCK_FILE = ART_DIR / "pull.lock"
M3_DATA.mkdir(parents=True, exist_ok=True)
ART_DIR.mkdir(parents=True, exist_ok=True)


def _fmt_float(x: Optional[float], nd: int = 2) -> str:
    return f"{x:.{nd}f}" if isinstance(x, (float, int)) else "NA"


def _format_q3(gpa: Optional[float], gre: Optional[float],
               gre_v: Optional[float], gre_aw: Optional[float]) -> str:
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
    parts = [f"{status}: {_fmt_float(avg)}" for status, avg in rows if avg is not None]
    return "NA" if not parts else ", ".join(parts)


def _run(cmd: list[str], cwd: Path) -> tuple[int, str, str]:
    """Run a subprocess and return (rc, stdout, stderr)."""
    proc = subprocess.run(
        cmd, cwd=str(cwd), capture_output=True, text=True, encoding="utf-8"
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _dump(step: str, rc: int, out: str, err: str) -> None:
    """Print debug info for a pipeline step to the terminal."""
    print(f"\n[PULL DEBUG] step={step} rc={rc}", flush=True)
    if out:
        print(f"[PULL DEBUG] {step} stdout:\n{out}\n", flush=True)
    if err:
        print(f"[PULL DEBUG] {step} stderr:\n{err}\n", flush=True)


def _build_rows() -> List[Tuple[str, str]]:
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

    # Optional extra questions if present in query_data.py
    try:
        q11 = q11_top_unis_fall_2025(limit=10)
        rows.append((
            "Top 10 universities by number of entries for Fall 2025.",
            (", ".join([f"{u}={c}" for (u, c) in q11]) if q11 else "NA"),
        ))
    except Exception:
        pass

    try:
        q12 = q12_status_breakdown_fall_2025()
        rows.append((
            "Status breakdown for Fall 2025 (percent of entries).",
            (", ".join([f"{s}={pct:.2f}%" for (s, pct) in q12]) if q12 else "NA"),
        ))
    except Exception:
        pass

    return rows


def _go(level: str, msg: str):
    return redirect(url_for("index", msg=msg, level=level))


@app.route("/")
def index():
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
    """Button 1: scrape + clean + LLM + load (guarded by lock, with terminal debug)."""
    if LOCK_FILE.exists():
        return _go("info", "A data pull is already running—please wait until it finishes.")

    LOCK_FILE.write_text("running", encoding="utf-8")
    try:
        # 0) deps (optional)
        req = M2_REF_DIR / "requirements.txt"
        if req.exists():
            rc0, out0, err0 = _run([sys.executable, "-m", "pip", "install", "-r", str(req)], cwd=M3_DIR)
            _dump("deps", rc0, out0, err0)

        # 1) scrape
        rc1, out1, err1 = _run([sys.executable, "scrape.py"], cwd=M2_REF_DIR)
        _dump("scrape", rc1, out1, err1)
        data_json = M2_REF_DIR / "applicant_data.json"
        if rc1 != 0 or not data_json.exists():
            return _go("error", "Pull failed in scrape step. See server console for details.")

        # 2) clean
        rc2, out2, err2 = _run([sys.executable, "clean.py"], cwd=M2_REF_DIR)
        _dump("clean", rc2, out2, err2)
        if rc2 != 0:
            return _go("error", "Pull failed in clean step. See server console for details.")

        # 3) LLM standardize
        rc3, out3, err3 = _run([sys.executable, "run.py"], cwd=M2_REF_DIR)
        _dump("llm", rc3, out3, err3)
        out_json = M2_REF_DIR / "llm_extend_applicant_data.json"
        if rc3 != 0 or not out_json.exists() or out_json.stat().st_size == 0:
            return _go("error", "LLM step failed (no standardized JSON). Check the terminal for rc/stdout/stderr.")

        # 4) load
        dest = M3_DATA / "module_2llm_extend_applicant_data.json"
        dest.write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
        rc4, out4, err4 = _run(
            [sys.executable, "load_data.py", "--init", "--load", str(dest), "--batch", "2000", "--count"],
            cwd=M3_DIR,
        )
        _dump("load", rc4, out4, err4)
        if rc4 != 0:
            return _go("error", "Database load failed. See server console for details.")

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
    if LOCK_FILE.exists():
        return _go("info", "A data pull is running; Update Analysis is temporarily disabled.")
    return _go("success", "Analysis refreshed with the most recent data in the database.")


@app.route("/health")
def health():
    return jsonify(ok=True)


@app.teardown_appcontext
def _shutdown(exc):
    close_pool()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
