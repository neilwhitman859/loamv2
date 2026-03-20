#!/usr/bin/env python3
"""
Audit metadata JSONB fields across producers, wines, and wine_vintages.
Identifies structured data that should be promoted to proper columns/table links.

Usage:
    python -m pipeline.analyze.audit_metadata
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import fetch_all


def audit_table(table: str, name_col: str, label: str):
    rows = fetch_all(table, f"id,{name_col},metadata")
    keys_info: dict[str, dict] = {}
    with_meta = 0

    for r in rows:
        meta = r.get("metadata")
        if not meta or not isinstance(meta, dict) or len(meta) == 0:
            continue
        with_meta += 1
        for key in meta:
            if key not in keys_info:
                keys_info[key] = {"count": 0, "examples": []}
            keys_info[key]["count"] += 1
            if len(keys_info[key]["examples"]) < 3:
                val = meta[key]
                val_str = val[:80] if isinstance(val, str) else json.dumps(val)[:80]
                keys_info[key]["examples"].append({
                    "name": r[name_col],
                    "value": val_str,
                })

    print(f"{label} with metadata: {with_meta}/{len(rows)}")
    for key, info in sorted(keys_info.items(), key=lambda x: -x[1]["count"]):
        print(f"  {key}: {info['count']} entries")
        for ex in info["examples"]:
            print(f"    -> {ex['name']}: {ex['value']}")


def main():
    print("Auditing metadata fields...\n")
    audit_table("producers", "name", "PRODUCERS")
    print()
    audit_table("wines", "name", "WINES")
    print()

    # Wine vintages need special handling for name display
    rows = fetch_all("wine_vintages", "id,wine_id,vintage_year,metadata")
    keys_info: dict[str, dict] = {}
    with_meta = 0

    for v in rows:
        meta = v.get("metadata")
        if not meta or not isinstance(meta, dict) or len(meta) == 0:
            continue
        with_meta += 1
        for key in meta:
            if key not in keys_info:
                keys_info[key] = {"count": 0, "examples": []}
            keys_info[key]["count"] += 1
            if len(keys_info[key]["examples"]) < 2:
                val = meta[key]
                val_str = val[:80] if isinstance(val, str) else json.dumps(val)[:80]
                keys_info[key]["examples"].append({
                    "vintage_year": v["vintage_year"],
                    "value": val_str,
                })

    print(f"WINE_VINTAGES with metadata: {with_meta}/{len(rows)}")
    for key, info in sorted(keys_info.items(), key=lambda x: -x[1]["count"]):
        print(f"  {key}: {info['count']} entries")
        for ex in info["examples"]:
            print(f"    -> vintage {ex['vintage_year']}: {ex['value']}")


if __name__ == "__main__":
    main()
