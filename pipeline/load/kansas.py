#!/usr/bin/env python3
"""
Load Kansas Active Brands JSON into source_kansas_brands staging table.
Source: data/imports/kansas_active_brands.json (24.6MB, 65K records)

Usage:
    python -m pipeline.load.kansas [--dry-run]
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def parse_kansas_date(date_str: str | None) -> str | None:
    if not date_str or not date_str.strip():
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", date_str.strip())
    if not m:
        return None
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"


def map_record(r: dict) -> dict:
    return {
        "cola_number": (r.get("a") or "").strip() or None,
        "ks_license": (r.get("b") or "").strip() or None,
        "brand_name": (r.get("c") or "").strip() or None,
        "fanciful_name": (r.get("d") or "").strip() or None,
        "product_type": (r.get("e") or "").strip() or None,
        "abv": float(r["f"]) if r.get("f") else None,
        "pack_size": int(r["g"]) if r.get("g") else None,
        "container_size": float(r["h"]) if r.get("h") else None,
        "container_unit": (r.get("i") or "").strip() or None,
        "vintage": (r.get("j") or "").strip() or None,
        "appellation": (r.get("k") or "").strip() or None,
        "expiration": parse_kansas_date(r.get("l")),
        "unknown_m": (r.get("m") or "").strip() or None,
        "container_type": (r.get("n") or "").strip() or None,
        "flag_o": (r.get("o") or "").strip() or None,
        "flag_p": (r.get("p") or "").strip() or None,
        "distributor1": (r.get("q") or "").strip() or None,
        "distributor2": (r.get("r") or "").strip() or None,
    }


def main():
    parser = argparse.ArgumentParser(description="Load Kansas Active Brands into staging")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("Loading Kansas Active Brands...")
    raw = json.loads((DATA_DIR / "kansas_active_brands.json").read_text(encoding="utf-8"))
    print(f"Total records: {len(raw)}")

    wines = [r for r in raw if "wine" in (r.get("e") or "").lower() or "Wine" in (r.get("e") or "")]
    print(f"Wine records: {len(wines)}")

    mapped = [map_record(r) for r in wines]

    if args.dry_run:
        print("\n--- DRY RUN ---")
        print("Sample mapped records:")
        for r in mapped[:5]:
            print(json.dumps(r, indent=2))
        print(f"\nWould insert {len(mapped)} wine records.")
        return

    sb = get_supabase()

    # Clear existing data
    sb.table("source_kansas_brands").delete().gte("id", 0).execute()
    print("Cleared existing data.")

    inserted = batch_insert("source_kansas_brands", mapped, batch_size=500)
    print(f"\nDone. Inserted {inserted} wine records into source_kansas_brands.")

    result = sb.table("source_kansas_brands").select("*", count="exact").limit(0).execute()
    print(f"Table row count: {result.count}")


if __name__ == "__main__":
    main()
