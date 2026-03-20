#!/usr/bin/env python3
"""
Reads the TSV data extracted from the browser localStorage
and converts it back to JSONL format, appending to the main output file.

Input: tw_remaining.txt (pipe-separated: name, price, sku, starRating, reviews, page)
Output: appends to totalwine_lexington_green.jsonl

Usage:
    python -m pipeline.analyze.tw_extract_remaining
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

INPUT = Path(__file__).resolve().parents[2] / "tw_remaining.txt"
OUTPUT = Path(__file__).resolve().parents[2] / "totalwine_lexington_green.jsonl"


def main():
    raw = INPUT.read_text(encoding="utf-8").strip()
    lines = [l for l in raw.split("\n") if l.strip()]

    count = 0
    with open(OUTPUT, "a", encoding="utf-8") as f:
        for line in lines:
            parts = line.split("|")
            if not parts[0]:
                continue
            obj = {
                "name": parts[0].strip(),
                "sku": parts[2] if len(parts) > 2 else "",
                "size": "",
                "price": parts[1] if len(parts) > 1 else "",
                "starRating": parts[3] if len(parts) > 3 else "",
                "reviews": parts[4] if len(parts) > 4 else "",
                "wineryDirect": False,
                "categories": [],
                "page": int(parts[5]) if len(parts) > 5 and parts[5].strip().isdigit() else 0,
            }
            f.write(json.dumps(obj) + "\n")
            count += 1

    print(f"Appended {count} wines to {OUTPUT}")


if __name__ == "__main__":
    main()
