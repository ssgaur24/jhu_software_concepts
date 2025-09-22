# coverage: ignore file
# trim whitespace, strip simple HTML tags, fill missing fields; write back

import json
import re
from pathlib import Path

_tag_re = re.compile(r"<[^>]+>")

EXPECTED = [
    "program","university","comments","status","acceptance_date","rejection_date",
    "start_term","degree","entry_url","US/International","GRE","GRE V","GPA","GRE AW",
]

def load_data(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def clean_data(rows):
    cleaned = []
    for r in rows:
        rec = dict(r) if isinstance(r, dict) else {}
        for k in EXPECTED:
            if rec.get(k) is None: rec[k] = ""
        for k in ("program","university","comments"):
            if isinstance(rec.get(k), str): rec[k] = rec[k].strip()
        if isinstance(rec.get("comments"), str):
            rec["comments"] = _tag_re.sub("", rec["comments"]).strip()
        cleaned.append(rec)
    return cleaned

def save_data(path: Path, rows):
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    base = Path(__file__).parent
    in_path = base / "applicant_data.json"
    if not in_path.exists():
        print(f"input missing: {in_path.as_posix()}")  # show the real path used
        return
    rows = load_data(in_path); rows = clean_data(rows); save_data(in_path, rows)
    print(f"cleaned: {in_path} ({len(rows)} records)")
    for rec in rows[:3]:
        print({k: rec.get(k, "") for k in ("program","university","comments")})

if __name__ == "__main__":
    main()
