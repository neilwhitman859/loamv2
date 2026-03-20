#!/usr/bin/env python3
"""
Parse PRO Platform XLSX exports from 12 US states.

Processes one state at a time to avoid memory issues with large files.
Deduplicates by COLA within each state (rows duplicate per distributor).
Cross-state dedup happens at staging table level (upsert on cola_number).

Usage:
    python -m pipeline.load.parse_pro_platform                    # parse all 12 states
    python -m pipeline.load.parse_pro_platform --state ar         # parse one state
    python -m pipeline.load.parse_pro_platform --state ar,co,il   # parse multiple
    python -m pipeline.load.parse_pro_platform --stats            # stats only, no output
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import openpyxl

PRO_STATES = ["ar", "co", "il", "ky", "la", "mn", "nm", "ny", "oh", "ok", "sc", "sd"]
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def field_count(rec: dict) -> int:
    return sum(1 for v in rec.values() if v is not None and v != "")


def parse_row(r: dict, state: str) -> dict | None:
    cola = str(r.get("Tax Trade Bureau ID") or "").strip()
    if not cola:
        return None

    return {
        "cola_number": cola,
        "brand": str(r.get("Brand Description") or "").strip() or None,
        "label_description": str(r.get("Label Description") or "").strip() or None,
        "vintage": str(r.get("Vintage") or "").strip() or None,
        "appellation": str(r.get("Appellation") or "").strip() or None,
        "abv": float(r["Percent Alcohol"]) if r.get("Percent Alcohol") else None,
        "container_type": str(r.get("Container Type") or "").strip() or None,
        "unit_size": float(r["Unit Size"]) if r.get("Unit Size") else None,
        "unit_measure": str(r.get("Unit Measure") or "").strip() or None,
        "supplier_name": str(r.get("Supplier Name") or "").strip() or None,
        "distributor_name": str(r.get("Distributor Name") or "").strip() or None,
        "approval_date": str(r.get("Inception Date") or r.get("Approval Date") or "").strip() or None,
        "end_date": str(r.get("End Date") or "").strip() or None,
        "approval_number": str(r.get("Approval Number") or "").strip() or None,
        "status": str(r.get("Status") or "").strip() or None,
        "state": state.upper(),
    }


def read_xlsx_rows(file_path: Path) -> list[dict]:
    """Read xlsx to list of dicts. For large files, uses csv fast path via openpyxl."""
    file_size_mb = file_path.stat().st_size / (1024 * 1024)

    wb = openpyxl.load_workbook(file_path, read_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h) if h else f"col_{i}" for i, h in enumerate(next(rows_iter))]
    data = [dict(zip(headers, row)) for row in rows_iter]
    wb.close()
    return data


def main():
    parser = argparse.ArgumentParser(description="Parse PRO Platform XLSX exports")
    parser.add_argument("--state", type=str, default=None)
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    states = args.state.split(",") if args.state else PRO_STATES

    print("=== PRO Platform XLSX Parser ===")
    print(f"States: {', '.join(s.upper() for s in states)}")
    print(f"Mode: {'STATS ONLY' if args.stats else 'FULL PARSE -- one JSON per state'}\n")

    grand_stats = {"totalRaw": 0, "totalUnique": 0, "totalNoCola": 0}

    for state in states:
        file_path = DATA_DIR / f"{state}_active_brands.xlsx"
        if not file_path.exists():
            file_path = DATA_DIR / f"{state}_active_brands_wine.xlsx"
            if not file_path.exists():
                print(f"  {state.upper()}: FILE NOT FOUND -- skipping")
                continue

        file_size = file_path.stat().st_size / (1024 * 1024)
        print(f"  {state.upper()}: Reading {file_size:.1f} MB...", end="", flush=True)

        rows = read_xlsx_rows(file_path)
        print(f" {len(rows):,} rows")

        # Dedup within state by COLA -- keep richest record, collect all distributors
        by_cola: dict[str, dict] = {}
        no_cola = 0

        for r in rows:
            rec = parse_row(r, state)
            if not rec:
                no_cola += 1
                continue

            existing = by_cola.get(rec["cola_number"])
            if existing:
                if rec["distributor_name"] and rec["distributor_name"] not in existing["distributors"]:
                    existing["distributors"].add(rec["distributor_name"])
                if field_count(rec) > field_count(existing["record"]):
                    existing["record"] = rec
            else:
                distributors = set()
                if rec["distributor_name"]:
                    distributors.add(rec["distributor_name"])
                by_cola[rec["cola_number"]] = {"record": rec, "distributors": distributors}

        # Build output array
        unique = []
        for cola, entry in by_cola.items():
            rec = entry["record"]
            rec["distributors"] = sorted(entry["distributors"])
            rec["distributor_count"] = len(entry["distributors"])
            del rec["distributor_name"]
            unique.append(rec)

        has_vintage = sum(1 for r in unique if r.get("vintage"))
        has_appellation = sum(1 for r in unique if r.get("appellation"))
        has_abv = sum(1 for r in unique if r.get("abv") is not None)

        grand_stats["totalRaw"] += len(rows)
        grand_stats["totalUnique"] += len(unique)
        grand_stats["totalNoCola"] += no_cola

        n = len(unique) or 1
        print(f"    Unique: {len(unique):,} | Dedup: {len(rows) - len(unique) - no_cola:,} removed | No COLA: {no_cola:,}")
        print(f"    Vintage: {has_vintage:,} ({has_vintage/n*100:.0f}%) | Appellation: {has_appellation:,} ({has_appellation/n*100:.0f}%) | ABV: {has_abv:,} ({has_abv/n*100:.0f}%)")

        if not args.stats:
            out_path = DATA_DIR / f"pro_{state}_parsed.json"
            output = {
                "metadata": {
                    "source": "PRO Platform (Sovos ShipCompliant)",
                    "state": state.upper(),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "raw_rows": len(rows),
                    "unique_colas": len(unique),
                    "no_cola": no_cola,
                },
                "records": unique,
            }
            out_path.write_text(json.dumps(output), encoding="utf-8")
            out_size = out_path.stat().st_size / (1024 * 1024)
            print(f"    Saved: {out_path} ({out_size:.1f} MB)")

        by_cola.clear()
        print()

    print("=== GRAND TOTALS ===")
    print(f"Raw rows: {grand_stats['totalRaw']:,}")
    print(f"Unique COLAs: {grand_stats['totalUnique']:,}")
    print(f"No COLA: {grand_stats['totalNoCola']:,}")
    total = grand_stats["totalRaw"] or 1
    print(f"Dedup ratio: {(1 - grand_stats['totalUnique'] / total) * 100:.1f}%")
    print(f"\nNote: cross-state dedup not shown here -- happens at staging table load (upsert on cola_number).")


if __name__ == "__main__":
    main()
