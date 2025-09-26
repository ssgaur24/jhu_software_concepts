# -*- coding: utf-8 -*-
"""Flask + CPU/GPU LLM standardizer with batching and final JSON output.

Added:
- --only-new / --prev: process only rows not already present in a previous extended output.
- --stdout-array: write the final combined JSON array to stdout (useful for shell redirection).
"""

from __future__ import annotations

import json
import os
import re
import sys
import difflib
from typing import Any, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

from flask import Flask, jsonify, request
from huggingface_hub import hf_hub_download
from llama_cpp import Llama

app = Flask(__name__)

# ---------------- Auto-detect GPU/CPU Configuration ----------------
MODEL_REPO = os.getenv("MODEL_REPO", "TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF")
MODEL_FILE = os.getenv("MODEL_FILE", "tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf")


# Auto-detect GPU capability
def detect_gpu_layers():
    """Auto-detect optimal GPU layers based on available hardware."""
    gpu_layers = os.getenv("N_GPU_LAYERS")
    if gpu_layers is not None:
        return int(gpu_layers)

    try:
        import subprocess
        result = subprocess.run(['nvidia-smi'], capture_output=True, timeout=3)
        if result.returncode == 0:
            print("GPU detected - using GPU acceleration", file=sys.stderr)
            return -1  # All layers to GPU
    except:
        pass

    print("No GPU detected - using CPU optimization", file=sys.stderr)
    return 0  # CPU only


N_GPU_LAYERS = detect_gpu_layers()
N_THREADS = int(os.getenv("N_THREADS", str(os.cpu_count() or 4)))
N_CTX = int(os.getenv("N_CTX", "2048"))

# CPU/GPU optimized batching
if N_GPU_LAYERS > 0:
    # GPU settings
    N_BATCH = int(os.getenv("N_BATCH", "512"))
    N_UBATCH = int(os.getenv("N_UBATCH", "256"))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
else:
    # CPU optimization - larger batches, more threads
    N_BATCH = int(os.getenv("N_BATCH", "8"))
    N_UBATCH = int(os.getenv("N_UBATCH", "8"))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", str(min(8, os.cpu_count() or 4))))

CANON_UNIS_PATH = os.getenv("CANON_UNIS_PATH", "canon_universities.txt")
CANON_PROGS_PATH = os.getenv("CANON_PROGS_PATH", "canon_programs.txt")

# Single model with thread lock
_LLM: Llama | None = None
_LLM_LOCK = Lock()

# Precompiled JSON matcher
JSON_OBJ_RE = re.compile(r"\{.*?\}", re.DOTALL)


# ---------------- Canonical lists + abbrev maps ----------------
def _read_lines(path: str) -> List[str]:
    """Read non-empty, stripped lines from a file (UTF-8)."""
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

# ---------------- Original prompt (unchanged) ----------------
SYSTEM_PROMPT = (
    "You are a data cleaning assistant. Standardize degree program and university "
    "names.\n\n"
    "Rules:\n"
    "- Input provides a single string under key `program` that may contain both "
    "program and university.\n"
    "- Split into (program name, university name).\n"
    "- Trim extra spaces and commas.\n"
    '- Expand obvious abbreviations (e.g., "McG" -> "McGill University", '
    '"UBC" -> "University of British Columbia").\n"
    "- Use Title Case for program; use official capitalization for university "
    "names (e.g., \"University of X\").\n"
    '- Ensure correct spelling (e.g., "McGill", not "McGiill").\n'
    '- If university cannot be inferred, return "Unknown".\n\n"
    "Return JSON ONLY with keys:\n"
    "  standardized_program, standardized_university\n"
)

FEW_SHOTS: List[Tuple[Dict[str, str], Dict[str, str]]] = [
    (
        {"program": "Information Studies, McGill University"},
        {
            "standardized_program": "Information Studies",
            "standardized_university": "McGill University",
        },
    ),
    (
        {"program": "Information, McG"},
        {
            "standardized_program": "Information Studies",
            "standardized_university": "McGill University",
        },
    ),
    (
        {"program": "Mathematics, University Of British Columbia"},
        {
            "standardized_program": "Mathematics",
            "standardized_university": "University of British Columbia",
        },
    ),
]


def _load_llm() -> Llama:
    """Load LLM with optimal CPU/GPU configuration."""
    global _LLM
    if _LLM is not None:
        return _LLM

    hardware_type = "GPU" if N_GPU_LAYERS > 0 else "CPU"
    print(f"Loading model optimized for {hardware_type}...", file=sys.stderr)
    print(f"  GPU Layers: {N_GPU_LAYERS}", file=sys.stderr)
    print(f"  Batch Size: {N_BATCH}", file=sys.stderr)
    print(f"  Workers: {MAX_WORKERS}", file=sys.stderr)

    model_path = hf_hub_download(
        repo_id=MODEL_REPO,
        filename=MODEL_FILE,
        local_dir="models",
        local_dir_use_symlinks=False,
        force_filename=MODEL_FILE,
    )

    # Optimized initialization for CPU/GPU
    llama_args = {
        "model_path": model_path,
        "n_ctx": N_CTX,
        "n_threads": N_THREADS,
        "n_gpu_layers": N_GPU_LAYERS,
        "verbose": N_GPU_LAYERS > 0,  # Verbose for GPU to show loading
    }

    # Add batching parameters if supported
    try:
        llama_args.update({
            "n_batch": N_BATCH,
            "n_ubatch": N_UBATCH,
        })
    except:
        # Fallback for older llama-cpp-python versions
        pass

    _LLM = Llama(**llama_args)

    print(f"Model loaded with {hardware_type} optimization!", file=sys.stderr)
    return _LLM


def _split_fallback(text: str) -> Tuple[str, str]:
    """Rules-based fallback parser."""
    s = re.sub(r"\s+", " ", (text or "")).strip().strip(",")
    parts = [p.strip() for p in re.split(r",| at | @ ", s) if p.strip()]
    prog = parts[0] if parts else ""
    uni = parts[1] if len(parts) > 1 else ""

    # High-signal expansions
    if re.fullmatch(r"(?i)mcg(ill)?(\.)?", uni or ""):
        uni = "McGill University"
    if re.fullmatch(
            r"(?i)(ubc|u\.?b\.?c\.?|university of british columbia)",
            uni or "",
    ):
        uni = "University of British Columbia"

    # Title-case program; normalize 'Of' → 'of' for universities
    prog = prog.title()
    if uni:
        uni = re.sub(r"\bOf\b", "of", uni.title())
    else:
        uni = "Unknown"
    return prog, uni


def _best_match(name: str, candidates: List[str], cutoff: float = 0.86) -> str | None:
    """Fuzzy match via difflib."""
    if not name or not candidates:
        return None
    matches = difflib.get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None


def _post_normalize_program(prog: str) -> str:
    """Apply common fixes, title case, then canonical/fuzzy mapping."""
    p = (prog or "").strip()
    p = COMMON_PROG_FIXES.get(p, p)
    p = p.title()
    if p in CANON_PROGS:
        return p
    match = _best_match(p, CANON_PROGS, cutoff=0.84)
    return match or p


def _post_normalize_university(uni: str) -> str:
    """Expand abbreviations, apply common fixes, capitalization, and canonical map."""
    u = (uni or "").strip()

    # Abbreviations
    for pat, full in ABBREV_UNI.items():
        if re.fullmatch(pat, u):
            u = full
            break

    # Common spelling fixes
    u = COMMON_UNI_FIXES.get(u, u)

    # Normalize 'Of' → 'of'
    if u:
        u = re.sub(r"\bOf\b", "of", u.title())

    # Canonical or fuzzy map
    if u in CANON_UNIS:
        return u
    match = _best_match(u, CANON_UNIS, cutoff=0.86)
    return match or u or "Unknown"


def _call_llm(program_text: str) -> Dict[str, str]:
    """Thread-safe LLM call with CPU/GPU optimization."""
    llm = _load_llm()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for x_in, x_out in FEW_SHOTS:
        messages.append(
            {"role": "user", "content": json.dumps(x_in, ensure_ascii=False)}
        )
        messages.append(
            {
                "role": "assistant",
                "content": json.dumps(x_out, ensure_ascii=False),
            }
        )
    messages.append(
        {
            "role": "user",
            "content": json.dumps({"program": program_text}, ensure_ascii=False),
        }
    )

    # Thread-safe model access
    with _LLM_LOCK:
        out = llm.create_chat_completion(
            messages=messages,
            temperature=0.0,
            max_tokens=128,
            top_p=1.0,
        )

    text = (out["choices"][0]["message"]["content"] or "").strip()
    try:
        match = JSON_OBJ_RE.search(text)
        obj = json.loads(match.group(0) if match else text)
        std_prog = str(obj.get("standardized_program", "")).strip()
        std_uni = str(obj.get("standardized_university", "")).strip()
    except Exception:
        std_prog, std_uni = _split_fallback(program_text)

    std_prog = _post_normalize_program(std_prog)
    std_uni = _post_normalize_university(std_uni)
    return {
        "standardized_program": std_prog,
        "standardized_university": std_uni,
    }


def _process_single_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single row for parallel execution."""
    program_text = (row or {}).get("program") or ""
    result = _call_llm(program_text)
    row["llm-generated-program"] = result["standardized_program"]
    row["llm-generated-university"] = result["standardized_university"]
    return row


def _normalize_input(payload: Any) -> List[Dict[str, Any]]:
    """Accept either a list of rows or {'rows': [...]}."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []


# -------------- NEW: helpers for "only-new" processing --------------

def _row_key(row: Dict[str, Any]) -> str:
    """Build a simple unique key per record (url + date_added + program)."""
    url = str((row or {}).get("url", "")).strip()
    dt  = str((row or {}).get("date_added", "")).strip()
    prg = str((row or {}).get("program", "")).strip()
    return f"{url}\t{dt}\t{prg}"

def _load_prev_rows(prev_path: str) -> List[Dict[str, Any]]:
    """Load previous extended results from JSON array or JSONL."""
    if not prev_path or not os.path.exists(prev_path):
        return []
    try:
        if prev_path.lower().endswith(".jsonl"):
            out: List[Dict[str, Any]] = []
            with open(prev_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    out.append(json.loads(line))
            return out
        # JSON array
        with open(prev_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _normalize_input(data) or (data if isinstance(data, list) else [])
    except Exception as e:
        print(f"WARNING: Failed to read prev file '{prev_path}': {e}", file=sys.stderr)
        return []

def _filter_only_new(in_rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only records whose key is not present in previous output."""
    prev_keys = { _row_key(r) for r in (prev_rows or []) }
    return [r for r in (in_rows or []) if _row_key(r) not in prev_keys]


@app.get("/")
def health() -> Any:
    """Health check with system info."""
    hardware = "GPU" if N_GPU_LAYERS > 0 else "CPU"
    return jsonify({
        "ok": True,
        "hardware": hardware,
        "gpu_layers": N_GPU_LAYERS,
        "batch_size": N_BATCH,
        "workers": MAX_WORKERS,
        "model_loaded": _LLM is not None
    })


@app.post("/standardize")
def standardize() -> Any:
    """Standardize rows with optimal CPU/GPU parallelization."""
    payload = request.get_json(force=True, silent=True)
    rows = _normalize_input(payload)

    # Optimal parallel processing
    if MAX_WORKERS > 1 and len(rows) > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            processed_rows = list(executor.map(_process_single_row, rows))
    else:
        processed_rows = [_process_single_row(row) for row in rows]

    return jsonify({"rows": processed_rows})


def _cli_process_file(
        in_path: str,
        out_path: str | None,
        append: bool,
        to_stdout: bool,
        *,
        only_new: bool = False,
        prev_path: str | None = None,
        stdout_array: bool = False,
) -> None:
    """Process JSON file and create output.

    Modes:
      - Default: write NDJSON to a file (or stdout if to_stdout), then write final JSON array file.
      - only_new+prev_path: standardize only unseen rows, then combine prev + new for final JSON.
      - stdout_array: write the final combined JSON array to stdout (single JSON, not JSONL).
    """
    with open(in_path, "r", encoding="utf-8") as f:
        all_rows = _normalize_input(json.load(f))

    prev_rows: List[Dict[str, Any]] = []
    rows: List[Dict[str, Any]] = all_rows

    if only_new and prev_path:
        prev_rows = _load_prev_rows(prev_path)
        rows = _filter_only_new(all_rows, prev_rows)

    hardware = "GPU" if N_GPU_LAYERS > 0 else "CPU"
    print(f"Processing {len(rows):,} rows with {hardware} + {MAX_WORKERS} workers...", file=sys.stderr)
    start_time = time.time()

    # If user asked for final JSON array on stdout, we buffer in memory.
    if stdout_array:
        if rows:
            # Parallel processing
            if MAX_WORKERS > 1 and len(rows) > 1:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    processed_rows = list(executor.map(_process_single_row, rows))
            else:
                processed_rows = [_process_single_row(r) for r in rows]
        else:
            processed_rows = []

        combined = (prev_rows or []) + processed_rows
        json.dump(combined, sys.stdout, ensure_ascii=False)
        sys.stdout.flush()

        elapsed = time.time() - start_time
        print(f"\nDone. Output {len(combined):,} rows as a JSON array to stdout.", file=sys.stderr)
        return

    # Setup output streams for NDJSON mode (legacy)
    sink = sys.stdout if to_stdout else None
    jsonl_path = None
    if not to_stdout:
        jsonl_path = out_path or (in_path + ".jsonl")
        mode = "a" if append else "w"
        sink = open(jsonl_path, mode, encoding="utf-8")

    assert sink is not None

    # Process in optimized batches (as before)
    batch_size = 100 if N_GPU_LAYERS > 0 else 50
    processed_rows: List[Dict[str, Any]] = []
    processed_count = 0

    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            if MAX_WORKERS > 1:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    batch_results = list(executor.map(_process_single_row, batch))
            else:
                batch_results = [_process_single_row(row) for row in batch]

            # Write JSONL incrementally
            for row in batch_results:
                json.dump(row, sink, ensure_ascii=False)
                sink.write("\n")
                sink.flush()
                processed_rows.append(row)

            processed_count += len(batch_results)
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            progress = (processed_count / len(rows)) * 100 if rows else 100.0

            print(f"Progress: {progress:.1f}% ({processed_count:,}/{len(rows):,}) - {rate:.1f} rows/sec [{hardware}]",
                  file=sys.stderr)

        elapsed = time.time() - start_time
        final_rate = (len(rows) / elapsed) if elapsed > 0 else 0
        print(f"Completed! {len(rows):,} rows in {elapsed:.1f}s ({final_rate:.1f} rows/sec)",
              file=sys.stderr)

    finally:
        if sink is not sys.stdout:
            sink.close()

    # Create final JSON array output (combine prev + new when only_new)
    combined_rows = (prev_rows or []) + processed_rows

    final_json_path = "llm_extend_applicant_data.json"
    input_dir = os.path.dirname(os.path.abspath(in_path))
    final_json_path = os.path.join(input_dir, final_json_path)

    print(f"Creating final JSON output: {final_json_path}", file=sys.stderr)
    with open(final_json_path, "w", encoding="utf-8") as f:
        json.dump(combined_rows, f, ensure_ascii=False, indent=2)

    file_size = os.path.getsize(final_json_path)
    print(f"Final JSON created: {file_size:,} bytes with {len(combined_rows):,} records",
          file=sys.stderr)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="CPU/GPU optimized LLM standardizer with batching.",
    )
    parser.add_argument(
        "--file",
        help="Path to JSON input (list of rows or {'rows': [...]})",
        default=None,
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Run the HTTP server instead of CLI.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output path for JSON Lines (ndjson). "
             "Defaults to <input>.jsonl when --file is set.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the output file instead of overwriting.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Write JSON Lines to stdout instead of a file.",
    )
    # -------- NEW CLI flags --------
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Process only rows not present in --prev extended JSON (by url+date_added+program key).",
    )
    parser.add_argument(
        "--prev",
        default=None,
        help="Path to previous extended JSON (array or .jsonl). Used with --only-new.",
    )
    parser.add_argument(
        "--stdout-array",
        action="store_true",
        help="Write the FINAL combined JSON array (not JSONL) to stdout.",
    )

    args = parser.parse_args()

    if args.serve or args.file is None:
        port = int(os.getenv("PORT", "8000"))
        hardware = "GPU" if N_GPU_LAYERS > 0 else "CPU"
        print(f"Starting {hardware}-optimized server on port {port}...", file=sys.stderr)
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        _cli_process_file(
            in_path=args.file,
            out_path=args.out,
            append=bool(args.append),
            to_stdout=bool(args.stdout),
            only_new=bool(args.only_new),
            prev_path=args.prev,
            stdout_array=bool(args.stdout_array),
        )
