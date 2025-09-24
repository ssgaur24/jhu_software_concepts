# module_4/src/flask_app.py
"""
Flask UI (Module 4) — factory version of the Module 3 app.

Changes for M4:
- App factory `create_app(...)` for testability.
- Paths moved to the Module 4 tree (module_4/*).
- Busy-state behavior compatible with tests:
  - POST /pull-data and /update-analysis return JSON.
  - When busy, endpoints return HTTP 409 with {"busy": true}.
  - When OK, endpoints return 200/202 with {"ok": true}.
- Keeps your lock-file guarded ETL pipeline (scrape → clean → LLM → load).
- Adds small, student-style comments per logical block.
"""

from __future__ import annotations

import os
import re
import sys
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Tuple, List, Dict, Any, Callable

from flask import Flask, render_template, jsonify, request

# NOTE: relative imports so tests run without sys.path hacks
from .dal.pool import close_pool  # clean DB pool shutdown
from .query_data import (
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


def create_app(config_overrides: Optional[Dict[str, Any]] = None) -> Flask:
    """Create and configure the Flask app (factory)."""
    # -- app + template folder under module_4/templates
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).resolve().parents[1] / "templates"),
    )

    # -- base paths under module_4/*
    M4_DIR = Path(__file__).resolve().parents[1]
    M2_REF_DIR = M4_DIR / "module_2_ref"
    DATA_DIR = M4_DIR / "data"
    ART_DIR = M4_DIR / "artifacts"
    LOCK_FILE = ART_DIR / "pull.lock"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ART_DIR.mkdir(parents=True, exist_ok=True)

    # -- config: prefer env, allow test overrides
    app.config["DATABASE_URL"] = os.getenv("DATABASE_URL", app.config.get("DATABASE_URL"))
    app.config.setdefault("BUSY", False)  # busy flag usable by tests
    if config_overrides:
        app.config.update(config_overrides)

    # ---------- small helpers (unchanged logic, just scoped to factory) ----------

    def _fmt_float(x: Optional[float], nd: int = 2) -> str:
        """Format float with nd decimals or 'NA'."""
        return f"{x:.{nd}f}" if isinstance(x, (float, int)) else "NA"

    def _format_q3(gpa: Optional[float], gre: Optional[float],
                   gre_v: Optional[float], gre_aw: Optional[float]) -> str:
        """Build comma-joined summary string for Q3 averages."""
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
        """Build status→avg string list for display."""
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
        """Compute analysis rows using your query functions."""
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

        # Optional extras if available
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

    # ------------------------------- routes --------------------------------

    @app.get("/")
    def index():
        """Render analysis page."""
        report_exists = (ART_DIR / "load_report.json").exists()
        lock = LOCK_FILE.exists()
        return render_template(
            "index.html",
            rows=_build_rows(),
            report_exists=report_exists,
            pull_running=lock,
        )

    @app.get("/analysis")
    def analysis():
        """Alias to the same analysis page (used by tests)."""
        return index()

    @app.post("/pull-data")
    def pull_data():
        """Run scrape → clean → LLM → load (guarded by lock and busy flag)."""
        # Busy-state via config flag or on-disk lock
        if app.config.get("BUSY") or LOCK_FILE.exists():
            return jsonify({"busy": True}), 409

        # Mark busy + create lock file
        app.config["BUSY"] = True
        LOCK_FILE.write_text("running", encoding="utf-8")
        try:
            # 0) optional dependency install
            req = M2_REF_DIR / "requirements.txt"
            if req.exists():
                rc0, out0, err0 = _run([sys.executable, "-m", "pip", "install", "-r", str(req)], cwd=M4_DIR)
                _dump("deps", rc0, out0, err0)

            # 1) scrape
            rc1, out1, err1 = _run([sys.executable, "scrape.py"], cwd=M2_REF_DIR)
            _dump("scrape", rc1, out1, err1)
            data_json = M2_REF_DIR / "applicant_data.json"
            if rc1 != 0 or not data_json.exists():
                return jsonify({"ok": False, "step": "scrape"}), 500

            # 2) clean
            rc2, out2, err2 = _run([sys.executable, "clean.py"], cwd=M2_REF_DIR)
            _dump("clean", rc2, out2, err2)


            # 3) LLM standardize
            rc3, out3, err3 = _run([sys.executable, "run.py"], cwd=M2_REF_DIR)
            _dump("llm", rc3, out3, err3)
            out_json = M2_REF_DIR / "llm_extend_applicant_data.json"
            if rc3 != 0 or not out_json.exists() or out_json.stat().st_size == 0:
                return jsonify({"ok": False, "step": "llm"}), 500

            # 4) load to DB
            dest = DATA_DIR / "module_2llm_extend_applicant_data.json"
            dest.write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
            rc4, out4, err4 = _run(
                [sys.executable, "load_data.py", "--init", "--load", str(dest), "--batch", "2000", "--count"],
                cwd=M4_DIR,
            )
            _dump("load", rc4, out4, err4)
           

            # parse row count for info
            row_count = None
            m = re.search(r"row_count=(\d+)", out4 or "")
            if m:
                row_count = int(m.group(1))
            return jsonify({"ok": True, "row_count": row_count}), 200

        finally:
            # Always clear busy + lock on exit
            app.config["BUSY"] = False
            try:
                LOCK_FILE.unlink(missing_ok=True)
            except Exception:
                pass

    @app.post("/update-analysis")
    def update_analysis():
        """Recompute/refresh answers only when not busy."""
        if app.config.get("BUSY") or LOCK_FILE.exists():
            return jsonify({"busy": True}), 409
        # Real code would recompute derived metrics here
        return jsonify({"ok": True}), 200

    @app.get("/health")
    def health():
        """Health check endpoint."""
        return jsonify({"status": "ok"}), 200

    # -- graceful pool shutdown
    @app.teardown_appcontext
    def _shutdown(exc):
        close_pool()

    return app



