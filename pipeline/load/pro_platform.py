#!/usr/bin/env python3
"""
Load parsed PRO Platform JSON files into source_pro_platform staging table.
Cross-state dedup in memory -- one row per COLA with states[] array.

Usage:
    python -m pipeline.load.pro_platform                    # load all parsed states
    python -m pipeline.load.pro_platform --state ar,co      # load specific states
    python -m pipeline.load.pro_platform --dry-run           # count only, no insert
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_upsert

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"
PRO_STATES = ["ar", "co", "il", "ky", "la", "mn", "nm", "ny", "oh", "ok", "sc", "sd"]
BATCH_SIZE = 500


def field_count(rec: dict) -> int:
    return sum(1 for v in rec.values() if v is not None and v != "" and v != [])


def main():
    parser = argparse.ArgumentParser(description="Load PRO Platform JSON into staging")
    parser.add_argument("--state", type=str, default=None, help="Comma-separated state codes")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    states = args.state.split(",") if args.state else PRO_STATES

    print("=== PRO Platform Staging Loader ===")
    print(f"States: {', '.join(s.upper() for s in states)}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE INSERT'}\n")

    # Phase 1: Load all states into memory, merge by COLA
    print("Phase 1: Cross-state merge in memory...")
    by_cola: dict[str, dict] = {}
    total_raw = 0

    for state in states:
        file_path = DATA_DIR / f"pro_{state}_parsed.json"
        if not file_path.exists():
            print(f"  {state.upper()}: FILE NOT FOUND -- skipping")
            continue

        raw = json.loads(file_path.read_text(encoding="utf-8"))
        records = raw.get("records", raw) if isinstance(raw, dict) else raw
        total_raw += len(records)
        print(f"  {state.upper()}: {len(records):,} records")

        for r in records:
            cola = r.get("cola_number")
            if not cola:
                continue

            existing = by_cola.get(cola)
            if existing:
                if state.upper() not in existing["states"]:
                    existing["states"].append(state.upper())
                if r.get("distributors"):
                    for d in r["distributors"]:
                        if d not in existing["distributors"]:
                            existing["distributors"].append(d)
                if field_count(r) > field_count(existing):
                    saved_states = existing["states"]
                    saved_dists = existing["distributors"]
                    existing.update(r)
                    existing["states"] = saved_states
                    existing["distributors"] = saved_dists
            else:
                by_cola[cola] = {
                    **r,
                    "states": [state.upper()],
                    "distributors": r.get("distributors") or [],
                }

    unique = list(by_cola.values())
    print(f"\n  Raw records: {total_raw:,}")
    print(f"  Unique COLAs: {len(unique):,}")
    print(f"  Cross-state dedup: {total_raw - len(unique):,} removed\n")

    if args.dry_run:
        state_counts: dict[int, int] = {}
        for r in unique:
            n = len(r["states"])
            state_counts[n] = state_counts.get(n, 0) + 1
        print("State coverage:")
        for n in sorted(state_counts):
            print(f"  In {n} state(s): {state_counts[n]:,}")
        return

    # Phase 2: Insert into DB
    print("Phase 2: Loading into source_pro_platform...")
    sb = get_supabase()
    inserted = 0
    errors = 0

    for i in range(0, len(unique), BATCH_SIZE):
        batch = []
        for r in unique[i:i + BATCH_SIZE]:
            batch.append({
                "cola_number": r["cola_number"],
                "brand": r.get("brand") or None,
                "label_description": r.get("label_description") or None,
                "vintage": r.get("vintage") or None,
                "appellation": r.get("appellation") or None,
                "abv": r.get("abv") or None,
                "container_type": r.get("container_type") or None,
                "unit_size": r.get("unit_size") or None,
                "unit_measure": r.get("unit_measure") or None,
                "supplier_name": r.get("supplier_name") or None,
                "distributors": r.get("distributors") or [],
                "distributor_count": len(r.get("distributors") or []),
                "states": r["states"],
            })

        try:
            result = sb.table("source_pro_platform").upsert(
                batch, on_conflict="cola_number"
            ).execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            print(f"  Batch error at {i}: {e}")
            errors += 1

        if (i + BATCH_SIZE) % 10000 == 0 or i + BATCH_SIZE >= len(unique):
            print(f"  {min(i + BATCH_SIZE, len(unique)):,}/{len(unique):,} loaded")

    print(f"\n\n=== TOTALS ===")
    print(f"Raw records across {len(states)} states: {total_raw:,}")
    print(f"Unique COLAs loaded: {len(unique):,}")
    print(f"Errors: {errors}")

    result = sb.table("source_pro_platform").select("*", count="exact").limit(0).execute()
    print(f"DB total: {result.count:,}" if result.count else "DB total: unknown")


if __name__ == "__main__":
    main()
