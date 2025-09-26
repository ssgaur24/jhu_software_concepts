# -*- coding: utf-8 -*-
"""CPU-optimized LLM standardizer with resume + only-new support.

Additions:
- --only-new: process only rows not present in a previous extended file.
- --prev <path>: path to previous extended output (JSON array or JSONL).
- Always emits the FINAL combined JSON array to stdout (for Module 3).
  * Logs/progress go to stderr (safe for piping).
  * When --out is given, we still write NDJSON to that file (resume kept).
"""

from __future__ import annotations

import json
import os
import re
import sys
import difflib
from typing import Any, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

from flask import Flask, jsonify, request
from huggingface_hub import hf_hub_download
from llama_cpp import Llama

app = Flask(__name__)

# ---------------- CPU-Optimized Configuration ----------------
MODEL_REPO = os.getenv("MODEL_REPO", "TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF")
MODEL_FILE = os.getenv("MODEL_FILE", "tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf")

N_THREADS = int(os.getenv("N_THREADS", str(os.cpu_count() or 4)))
N_CTX = int(os.getenv("N_CTX", "1024"))
N_GPU_LAYERS = 0  # CPU-only
N_BATCH = int(os.getenv("N_BATCH", "1"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(min(6, os.cpu_count() or 4))))

CANON_UNIS_PATH = os.getenv("CANON_UNIS_PATH", "canon_universities.txt")
CANON_PROGS_PATH = os.getenv("CANON_PROGS_PATH", "canon_programs.txt")

_LLM: Llama | None = None
_LLM_LOCK = Lock()

JSON_OBJ_RE = re.compile(r"\{.*?\}", re.DOTALL)

# ---------------- Canonical data ----------------
def _read_lines(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        return []

CANON_UNIS = _read_lines(CANON_UNIS_PATH)
CANON_PROGS = _read_lines(CANON_PROGS_PATH)

ABBREV_UNI: Dict[str, str] = {
    r"(?i)^mcg(\.|ill)?$": "McGill University",
    r"(?i)^(ubc|u\.?b\.?c\.?)$": "University of British Columbia",
    r"(?i)^uoft$": "University of Toronto",
}

COMMON_UNI_FIXES: Dict[str, str] = {
    "McGiill University": "McGill University",
    "Mcgill University": "McGill University",
    "University Of British Columbia": "University of British Columbia",
}

COMMON_PROG_FIXES: Dict[str, str] = {
    "Mathematic": "Mathematics",
    "Info Studies": "Information Studies",
}

# ---------------- Prompt ----------------
SYSTEM_PROMPT = (
    "You are a data cleaning assistant. Standardize degree program and university names.\n\n"
    "Rules:\n"
    "- Input provides a single string under key `program` that may contain both program and university.\n"
    "- Split into (program name, university name).\n"
    "- Trim extra spaces and commas.\n"
    "- Expand obvious abbreviations (e.g., \"McG\" -> \"McGill University\", \"UBC\" -> \"University of British Columbia\").\n"
    "- Use Title Case for program; official capitalization for university.\n"
    "- If university cannot be inferred, return \"Unknown\".\n\n"
    "Return JSON ONLY with keys: standardized_program, standardized_university\n"
)

FEW_SHOTS: List[Tuple[Dict[str, str], Dict[str, str]]] = [
    (
        {"program": "Information Studies, McGill University"},
        {"standardized_program": "Information Studies", "standardized_university": "McGill University"},
    ),
    (
        {"program": "Information, McG"},
        {"standardized_program": "Information Studies", "standardized_university": "McGill University"},
    ),
    (
        {"program": "Mathematics, University Of British Columbia"},
        {"standardized_program": "Mathematics", "standardized_university": "University of British Columbia"},
    ),
]

# ---------------- LLM loading ----------------
def _load_llm() -> Llama:
    global _LLM
    if _LLM is not None:
        return _LLM

    print("Loading CPU-optimized model...", file=sys.stderr)
    model_path = hf_hub_download(
        repo_id=MODEL_REPO,
        filename=MODEL_FILE,
        local_dir="models",
        local_dir_use_symlinks=False,
        force_filename=MODEL_FILE,
    )
    _LLM = Llama(
        model_path=model_path,
        n_ctx=N_CTX,
        n_threads=N_THREADS,
        n_gpu_layers=0,
        n_batch=N_BATCH,
        verbose=False,
    )
    print("Model ready.", file=sys.stderr)
    return _LLM

# ---------------- Normalization helpers ----------------
def _split_fallback(text: str) -> Tuple[str, str]:
    s = re.sub(r"\s+", " ", (text or "")).strip().strip(",")
    parts = [p.strip() for p in re.split(r",| at | @ ", s) if p.strip()]
    prog = parts[0] if parts else ""
    uni = parts[1] if len(parts) > 1 else ""
    if re.fullmatch(r"(?i)mcg(ill)?(\.)?", uni or ""):
        uni = "McGill University"
    if re.fullmatch(r"(?i)(ubc|u\.?b\.?c\.?|university of british columbia)", uni or ""):
        uni = "University of British Columbia"
    prog = prog.title()
    if uni:
        uni = re.sub(r"\bOf\b", "of", uni.title())
    else:
        uni = "Unknown"
    return prog, uni

def _best_match(name: str, candidates: List[str], cutoff: float = 0.86) -> str | None:
    if not name or not candidates:
        return None
    m = difflib.get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return m[0] if m else None

def _post_normalize_program(prog: str) -> str:
    p = (prog or "").strip()
    p = COMMON_PROG_FIXES.get(p, p)
    p = p.title()
    if p in CANON_PROGS:
        return p
    return _best_match(p, CANON_PROGS, 0.84) or p

def _post_normalize_university(uni: str) -> str:
    u = (uni or "").strip()
    for pat, full in ABBREV_UNI.items():
        if re.fullmatch(pat, u):
            u = full
            break
    u = COMMON_UNI_FIXES.get(u, u)
    if u:
        u = re.sub(r"\bOf\b", "of", u.title())
    if u in CANON_UNIS:
        return u
    return _best_match(u, CANON_UNIS, 0.86) or u or "Unknown"

def _call_llm(program_text: str) -> Dict[str, str]:
    llm = _load_llm()
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for x_in, x_out in FEW_SHOTS:
        messages.append({"role": "user", "content": json.dumps(x_in, ensure_ascii=False)})
        messages.append({"role": "assistant", "content": json.dumps(x_out, ensure_ascii=False)})
    messages.append({"role": "user", "content": json.dumps({"program": program_text}, ensure_ascii=False)})

    with _LLM_LOCK:
        out = llm.create_chat_completion(messages=messages, temperature=0.0, max_tokens=96, top_p=1.0)

    text = (out["choices"][0]["message"]["content"] or "").strip()
    try:
        match = JSON_OBJ_RE.search(text)
        obj = json.loads(match.group(0) if match else text)
        std_prog = str(obj.get("standardized_program", "")).strip()
        std_uni = str(obj.get("standardized_university", "")).strip()
    except Exception:
        std_prog, std_uni = _split_fallback(program_text)

    return {
        "standardized_program": _post_normalize_program(std_prog),
        "standardized_university": _post_normalize_university(std_uni),
    }

def _process_single_row(row: Dict[str, Any]) -> Dict[str, Any]:
    program_text = (row or {}).get("program") or ""
    r = _call_llm(program_text)
    row["llm-generated-program"] = r["standardized_program"]
    row["llm-generated-university"] = r["standardized_university"]
    return row

# ---------------- Resume helpers (existing) ----------------
def count_existing_entries(out_path: str) -> int:
    if not os.path.exists(out_path):
        return 0
    try:
        cnt = 0
        with open(out_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and line.startswith("{"):
                    try:
                        json.loads(line)
                        cnt += 1
                    except json.JSONDecodeError:
                        continue
        return cnt
    except Exception:
        return 0

def _normalize_input(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []

# ---------------- NEW: only-new helpers ----------------
def _row_key(row: Dict[str, Any]) -> str:
    url = str((row or {}).get("url", "")).strip()
    dt  = str((row or {}).get("date_added", "")).strip()
    prg = str((row or {}).get("program", "")).strip()
    return f"{url}\t{dt}\t{prg}"

def _load_prev_rows(prev_path: str) -> List[Dict[str, Any]]:
    if not prev_path or not os.path.exists(prev_path):
        return []
    try:
        if prev_path.lower().endswith(".jsonl"):
            rows: List[Dict[str, Any]] = []
            with open(prev_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))
            return rows
        with open(prev_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return data["rows"]
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"WARNING: failed to read prev file '{prev_path}': {e}", file=sys.stderr)
        return []

def _filter_only_new(in_rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prev_keys = { _row_key(r) for r in (prev_rows or []) }
    return [r for r in (in_rows or []) if _row_key(r) not in prev_keys]

# ---------------- Flask routes ----------------
@app.get("/")
def health() -> Any:
    return jsonify({
        "ok": True,
        "hardware": "CPU",
        "threads": N_THREADS,
        "workers": MAX_WORKERS,
        "model_loaded": _LLM is not None
    })

@app.post("/standardize")
def standardize() -> Any:
    payload = request.get_json(force=True, silent=True)
    rows = _normalize_input(payload)
    if MAX_WORKERS > 1 and len(rows) > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            processed_rows = list(ex.map(_process_single_row, rows))
    else:
        processed_rows = [_process_single_row(r) for r in rows]
    return jsonify({"rows": processed_rows})

# ---------------- CLI path ----------------
def _cli_process_file(
    in_path: str,
    out_path: str | None,
    append: bool,
    to_stdout_ndjson: bool,
    *,
    only_new: bool = False,
    prev_path: str | None = None,
    stdout_array: bool = True,   # emit final combined array to stdout (for Module 3)
) -> None:

    with open(in_path, "r", encoding="utf-8") as f:
        all_rows = _normalize_input(json.load(f))

    prev_rows: List[Dict[str, Any]] = []
    rows_to_process: List[Dict[str, Any]] = all_rows

    if only_new and prev_path:
        prev_rows = _load_prev_rows(prev_path)
        rows_to_process = _filter_only_new(all_rows, prev_rows)

    total_rows = len(all_rows)
    print(f"Input rows: {total_rows:,} | To standardize now: {len(rows_to_process):,}", file=sys.stderr)

    # JSONL (resume) sink if user requested --out
    sink = None
    start_index = 0
    jsonl_path = None

    if out_path and not to_stdout_ndjson:
        jsonl_path = out_path or (in_path.replace(".json", "") + "_extended.jsonl")
        existing_count = count_existing_entries(jsonl_path)
        if not append and existing_count > 0 and not (only_new and prev_path):
            if existing_count >= len(all_rows):
                print(f"Resume: {existing_count:,}/{len(all_rows):,} already done. Nothing to do.", file=sys.stderr)
                # Still print final combined array to stdout if requested
                if stdout_array:
                    combined = (prev_rows or []) + []
                    json.dump(combined if only_new else all_rows, sys.stdout, ensure_ascii=False)
                    sys.stdout.flush()
                return
            start_index = existing_count
            print(f"Resuming at row {start_index + 1:,} into {jsonl_path}", file=sys.stderr)
        mode = "a" if (existing_count > 0 or append) else "w"
        sink = open(jsonl_path, mode, encoding="utf-8")

    # Process rows_to_process (or resume slice if no only-new)
    if not rows_to_process and not (out_path and start_index < len(all_rows)):
        # Nothing new; still emit combined array for Module 3
        if stdout_array:
            combined = (prev_rows or [])
            if not only_new:
                combined = all_rows  # no prev provided; just echo the full input standardized? (not yet)
            # If we didn't standardize anything this run and no previous array given,
            # return the previous rows as-is (empty if none).
            json.dump(combined, sys.stdout, ensure_ascii=False)
            sys.stdout.flush()
        if sink:
            sink.close()
        return

    start_time = time.time()
    processed_rows: List[Dict[str, Any]] = []

    # If resuming JSONL without --only-new, adjust the slice:
    slice_rows = rows_to_process
    if out_path and not (only_new and prev_path) and start_index > 0:
        slice_rows = all_rows[start_index:]

    print(f"Standardizing {len(slice_rows):,} rows with {MAX_WORKERS} workers [CPU]...", file=sys.stderr)

    batch_size = 25
    processed_count = 0
    try:
        for i in range(0, len(slice_rows), batch_size):
            batch = slice_rows[i:i+batch_size]
            if MAX_WORKERS > 1:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    batch_results = list(ex.map(_process_single_row, batch))
            else:
                batch_results = [_process_single_row(r) for r in batch]

            # Write NDJSON incrementally if requested
            if sink is not None:
                for row in batch_results:
                    json.dump(row, sink, ensure_ascii=False)
                    sink.write("\n")
                    sink.flush()

            processed_rows.extend(batch_results)
            processed_count += len(batch_results)
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            print(f"Progress: {processed_count:,}/{len(slice_rows):,} "
                  f"({(processed_count/len(slice_rows))*100:.1f}%) - {rate:.1f} rows/sec",
                  file=sys.stderr)
    finally:
        if sink:
            sink.close()

    elapsed = time.time() - start_time
    print(f"Done: {processed_count:,} standardized in {elapsed:.1f}s", file=sys.stderr)

    # Emit final combined JSON array to stdout for Module 3
    if stdout_array:
        if only_new and prev_rows:
            combined = prev_rows + processed_rows
        elif only_new and not prev_rows:
            combined = processed_rows
        else:
            # No only-new: when resuming NDJSON, we didn't read previous NDJSON to combine;
            # In that case, just output the newly processed slice (Module 3 always passes --prev).
            combined = processed_rows

        json.dump(combined, sys.stdout, ensure_ascii=False)
        sys.stdout.flush()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CPU-optimized LLM standardizer with resume and only-new.")
    parser.add_argument("--file", required=True, help="Path to JSON input (list of rows or {'rows': [...]})")
    parser.add_argument("--serve", action="store_true", help="Run the HTTP server instead of CLI.")
    parser.add_argument("--out", default=None, help="Output path for JSONL. Keeps resume behavior.")
    parser.add_argument("--append", action="store_true", help="Append to output file instead of resuming.")
    parser.add_argument("--stdout", action="store_true", help="Stream NDJSON to stdout (advanced).")

    # NEW flags expected by Module 3
    parser.add_argument("--only-new", action="store_true",
                        help="Process only rows not present in --prev extended file (by url+date_added+program).")
    parser.add_argument("--prev", default=None,
                        help="Path to previous extended output (JSON array or JSONL).")

    args = parser.parse_args()

    if args.serve:
        port = int(os.getenv("PORT", "8000"))
        print(f"Starting CPU server on port {port}...", file=sys.stderr)
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        # Emit final array to stdout by default (Module 3 reads stdout)
        stdout_array = not args.stdout  # if user asked for NDJSON to stdout, don't also print array
        _cli_process_file(
            in_path=args.file,
            out_path=args.out,
            append=args.append,
            to_stdout_ndjson=args.stdout,
            only_new=bool(args.only_new),
            prev_path=args.prev,
            stdout_array=stdout_array,
        )
