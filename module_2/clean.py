# trim whitespace, strip simple HTML tags, fill missing fields, write back

import json
import re
from pathlib import Path

# simple tag remover
_tag_re = re.compile(r"<[^>]+>")

# fields we expect (fill if missing)
EXPECTED = [
    "program",
    "university",
    "comments",
    "status",
    "acceptance_date",
    "rejection_date",
    "start_term",
    "degree",
    "entry_url",
    "US/International",
    "GRE",
    "GRE V",
    "GPA",
    "GRE AW",
]

def load_data(path: Path):
    # load json
    return json.loads(path.read_text(encoding="utf-8"))

def clean_data(rows):
    # clean fields
    cleaned = []
    for r in rows:
        rec = dict(r) if isinstance(r, dict) else {}
        # ensure all keys exist
        for k in EXPECTED:
            if rec.get(k) is None:
                rec[k] = ""
        # trim whitespace
        for k in ("program", "university", "comments"):
            if isinstance(rec.get(k), str):
                rec[k] = rec[k].strip()
        # remove simple html tags from comments
        if isinstance(rec.get("comments"), str):
            rec["comments"] = _tag_re.sub("", rec["comments"]).strip()
        cleaned.append(rec)
    return cleaned

def save_data(path: Path, rows):
    # write json
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    # paths
    in_path = Path(__file__).parent / "applicant_data.json"
    if not in_path.exists():
        print("input missing: module_2/applicant_data.json")
        return
    # load
    rows = load_data(in_path)
    # clean
    rows = clean_data(rows)
    # save (write back)
    save_data(in_path, rows)
    # tiny output
    print(f"cleaned: {in_path} ({len(rows)} records)")
    for rec in rows[:3]:
        print({k: rec.get(k, "") for k in ("program", "university", "comments")})

if __name__ == "__main__":
    main()
