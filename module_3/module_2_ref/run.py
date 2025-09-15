"""
Run Module-2 (reference) LLM standardizer locally (CPU-only).

- Expects: module_3/module_2_ref/llm_hosting/app.py  (instructor files)
- Input : module_3/module_2_ref/applicant_data.json
- Output: module_3/module_2_ref/llm_extend_applicant_data.json  (JSON array)

Usage:
  python module_3/module_2_ref/run.py
"""
import os
import re
import sys
import json
import pathlib
import subprocess

BASE = pathlib.Path(__file__).parent
APP_PY = BASE / "llm_hosting" / "app.py"
INPUT_JSON = BASE / "applicant_data.json"
JSONL = BASE / "applicant_data.json.jsonl"
OUT_JSON = BASE / "llm_extend_applicant_data.json"
REQS_TXT = BASE / "llm_hosting" / "requirements.txt"

def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)

def pip_install(args: list[str]) -> None:
    run([sys.executable, "-m", "pip", "install", *args])

def main() -> None:
    assert APP_PY.exists(), "module_3/module_2_ref/llm_hosting/app.py not found"
    assert INPUT_JSON.exists(), "module_3/module_2_ref/applicant_data.json not found"

    # remove incompatible 'cache_prompt=True' flags if any future patch adds them
    txt = APP_PY.read_text(encoding="utf-8")
    new = re.sub(r",?\s*cache_prompt\s*=\s*True", "", txt)
    if new != txt:
        APP_PY.write_text(new, encoding="utf-8")
        print("Patched app.py: removed 'cache_prompt=True'")

    # deps (CPU-only) per instructor requirements
    print("Installing/Updating LLM deps…")
    pip_install(["-U", "pip", "setuptools", "wheel"])
    if REQS_TXT.exists():
        pip_install(["--no-cache-dir", "-r", str(REQS_TXT)])

    # runtime env — point canon lists at llm_hosting/
    os.environ.setdefault("N_GPU_LAYERS", "0")
    os.environ.setdefault("N_THREADS", str(os.cpu_count() or 2))
    os.environ.setdefault("N_CTX", "2048")
    os.environ.setdefault("MODEL_FILE", "tinyllama-1.1b-chat-v1.0.Q3_K_M.gguf")
    os.environ.setdefault("CANON_UNIS_PATH", str(BASE / "llm_hosting" / "canon_universities.txt"))
    os.environ.setdefault("CANON_PROGS_PATH", str(BASE / "llm_hosting" / "canon_programs.txt"))

    # run app.py in CLI mode -> JSONL
    if JSONL.exists():
        JSONL.unlink()
    print("Running LLM standardizer (CLI)…")
    run([sys.executable, str(APP_PY), "--file", str(INPUT_JSON), "--out", str(JSONL)])

    # convert JSONL -> JSON array expected by Module-3 loader
    rows = []
    with JSONL.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                # skip malformed lines; tiny models can chatter — kept minimal
                continue
    OUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Final rows: {len(rows)} -> {OUT_JSON}")

if __name__ == "__main__":
    main()
