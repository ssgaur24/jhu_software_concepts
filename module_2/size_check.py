# size check: assert at least 30,000 rows

import json
from pathlib import Path

# load data
in_path = Path(__file__).parent / "applicant_data.json"
data = json.loads(in_path.read_text(encoding="utf-8"))

# assert length
n = len(data)
assert n >= 30000, f"size_check_failed: {n} < 30000"

# tiny success output
print(f"size_ok: {n}")
