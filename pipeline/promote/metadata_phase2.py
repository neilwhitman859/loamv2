#!/usr/bin/env python3
"""
Phase 2 metadata promotion — requires new columns from Migration 5.

Promotes:
  - wine.soil -> wines.soil_description
  - wine.vine_age -> wines.vine_age_description
  - wine.vineyard_area -> wines.vineyard_area_ha
  - wine.commune -> wines.commune
  - wine.altitude_m -> wines.altitude_m_low / altitude_m_high
  - wine.aspect -> wines.aspect
  - wine.slope_pct -> wines.slope_pct
  - wine.monopole -> wines.monopole
  - producer.location -> producers.address

Usage:
    python -m pipeline.promote.metadata_phase2 [--dry-run]
"""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all


def parse_altitude_range(val) -> tuple[int | None, int | None]:
    if not val:
        return None, None
    s = str(val).replace(",", "")
    m = re.search(r"(\d+)\s*[-\u2013]\s*(\d+)", s)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"(\d+)", s)
    if m2:
        n = int(m2.group(1))
        return n, n
    return None, None


def parse_vineyard_area(val) -> float | None:
    if not val:
        return None
    s = str(val).replace(",", "").strip()
    m = re.match(r"([\d.]+)\s*(?:ha|hectares?)?", s, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None


def parse_slope_pct(val) -> float | None:
    if not val:
        return None
    s = str(val).replace("%", "").strip()
    m = re.match(r"([\d.]+)\s*[-\u2013]\s*([\d.]+)", s)
    if m:
        return float(m.group(2))  # use high end
    try:
        return float(s)
    except ValueError:
        return None


def clean_metadata(meta: dict, keys_to_delete: list[str]) -> dict | None:
    new_meta = {k: v for k, v in meta.items() if k not in keys_to_delete}
    return new_meta if new_meta else None


def main():
    parser = argparse.ArgumentParser(description="Phase 2 metadata promotion")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    dry = args.dry_run

    print(f"Phase 2 metadata promotion...{' (DRY RUN)' if dry else ''}\n")

    # Check if new columns exist
    try:
        sb.table("wines").select("soil_description").limit(1).execute()
    except Exception as e:
        print(f"ERROR: New columns not found. Run Migration 5 first.\n  {e}")
        sys.exit(1)

    wines = fetch_all(
        "wines",
        "id,name,metadata,soil_description,vine_age_description,vineyard_area_ha,"
        "commune,altitude_m_low,altitude_m_high,aspect,slope_pct,monopole"
    )
    stats = {
        "soil": 0, "vine_age": 0, "vineyard_area": 0, "commune": 0,
        "altitude": 0, "aspect": 0, "slope": 0, "monopole": 0, "address": 0,
    }

    for w in wines:
        meta = w.get("metadata")
        if not meta:
            continue
        updates = {}
        meta_deletes = []

        # Soil
        if meta.get("soil") and not w.get("soil_description"):
            updates["soil_description"] = re.sub(r"<br\s*/?>", " ", str(meta["soil"])).strip()
            meta_deletes.append("soil")
            stats["soil"] += 1

        # Vine age
        if meta.get("vine_age") and not w.get("vine_age_description"):
            updates["vine_age_description"] = str(meta["vine_age"]).strip()
            meta_deletes.append("vine_age")
            stats["vine_age"] += 1

        # Vineyard area
        if meta.get("vineyard_area") and w.get("vineyard_area_ha") is None:
            ha = parse_vineyard_area(meta["vineyard_area"])
            if ha:
                updates["vineyard_area_ha"] = ha
                meta_deletes.append("vineyard_area")
                stats["vineyard_area"] += 1

        # Commune
        if meta.get("commune") and not w.get("commune"):
            updates["commune"] = meta["commune"]
            meta_deletes.append("commune")
            stats["commune"] += 1

        # Altitude
        if meta.get("altitude_m") and w.get("altitude_m_low") is None:
            low, high = parse_altitude_range(meta["altitude_m"])
            if low is not None:
                updates["altitude_m_low"] = low
                updates["altitude_m_high"] = high
                meta_deletes.append("altitude_m")
                stats["altitude"] += 1

        # Aspect
        if meta.get("aspect") and not w.get("aspect"):
            updates["aspect"] = meta["aspect"]
            meta_deletes.append("aspect")
            stats["aspect"] += 1

        # Slope
        if meta.get("slope_pct") and w.get("slope_pct") is None:
            slope = parse_slope_pct(meta["slope_pct"])
            if slope is not None:
                updates["slope_pct"] = slope
                meta_deletes.append("slope_pct")
                stats["slope"] += 1

        # Monopole
        if meta.get("monopole") and not w.get("monopole"):
            updates["monopole"] = meta["monopole"] is True or meta["monopole"] == "true"
            meta_deletes.append("monopole")
            stats["monopole"] += 1

        if not updates:
            continue

        new_meta = clean_metadata(meta, meta_deletes)
        updates["metadata"] = new_meta

        if dry:
            total = sum(stats.values())
            if total <= 10:
                fields = [k for k in updates if k != "metadata"]
                print(f"  [DRY] {w['name']}: {', '.join(fields)}")
        else:
            result = sb.table("wines").update(updates).eq("id", w["id"]).execute()
            if not result.data:
                print(f"  Warning: {w['name']}: update may have failed")

    # Producer addresses
    producers = fetch_all("producers", "id,name,metadata,address")
    for p in producers:
        meta = p.get("metadata") or {}
        if not meta.get("location") or p.get("address"):
            continue
        addr = str(meta["location"]).strip()
        if not addr:
            continue

        if dry:
            if stats["address"] < 5:
                print(f"  [DRY] {p['name']}: address -> {addr[:60]}")
        else:
            new_meta = clean_metadata(meta, ["location"])
            result = sb.table("producers").update({
                "address": addr,
                "metadata": new_meta,
            }).eq("id", p["id"]).execute()
            if not result.data:
                print(f"  Warning: {p['name']}: update may have failed")
        stats["address"] += 1

    print("\n" + "=" * 40)
    print("PHASE 2 SUMMARY")
    print("=" * 40)
    for key, count in stats.items():
        print(f"  {key}: {count}")


if __name__ == "__main__":
    main()
