#!/usr/bin/env python3
"""
Analyze PA PLCB wine catalog Excel file.

Usage:
    python -m pipeline.analyze.analyze_pa
"""

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import openpyxl

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def main():
    wb = openpyxl.load_workbook(DATA_DIR / "pa_wine_catalog.xlsx", read_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    headers = all_rows[0]
    data = [dict(zip(headers, r)) for r in all_rows[1:]]

    groups = Counter(r.get("Group Name") or "null" for r in data)
    print("Group Names:", json.dumps(dict(groups), indent=2))
    print(f"Total rows: {len(data)}")

    has_upc = 0
    total_upcs = 0
    for r in data:
        upcs = [r[k] for k in r if k and (k == "UPC" or str(k).startswith("UPC")) and r[k]]
        if upcs:
            has_upc += 1
            total_upcs += len(upcs)
    print(f"\nRows with UPC: {has_upc} / {len(data)}")
    print(f"Total UPC values: {total_upcs}")

    has_vintage = sum(1 for r in data if r.get("Vintage") and r["Vintage"] != "Nonvintage")
    print(f"Rows with vintage: {has_vintage}")

    country_counter = Counter(r.get("Country") or "null" for r in data)
    print(f"\nCountries: {json.dumps(dict(country_counter))}")

    sample = next((r for r in data if r.get("Vintage") and r["Vintage"] != "Nonvintage" and r.get("Region") and r.get("UPC")), None)
    if sample:
        print(f"\nSample wine with UPC + vintage: {json.dumps(sample, indent=2, default=str)}")


if __name__ == "__main__":
    main()
