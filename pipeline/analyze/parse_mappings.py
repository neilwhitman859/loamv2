#!/usr/bin/env python3
"""
Parse region/appellation mappings from Claude tool output JSON.

Usage:
    python -m pipeline.analyze.parse_mappings <json-file>
"""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.analyze.parse_mappings <json-file>")
        sys.exit(1)

    raw = Path(sys.argv[1]).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    text = parsed[0]["text"]
    match = re.search(r"\[\{.*\}\]", text, re.DOTALL)
    if not match:
        print("No JSON array found in text")
        sys.exit(1)

    data = json.loads(match.group(0))
    print(f"Total mappings with NULL appellation: {len(data)}")
    print("\n--- All entries by candidate_count ---")
    for i, d in enumerate(data):
        idx = str(i + 1).rjust(3)
        rn = (d.get("region_name") or "").ljust(45)
        co = (d.get("country") or "").ljust(18)
        cnt = str(d.get("candidate_count", 0)).rjust(5)
        res = d.get("resolved_region") or "NULL"
        ca = "catch-all" if d.get("is_catch_all") else "real"
        mt = d.get("match_type") or ""
        print(f"{idx}. {rn} | {co} | {cnt} wines | -> {res} ({ca}) | {mt}")


if __name__ == "__main__":
    main()
