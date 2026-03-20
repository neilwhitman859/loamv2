#!/usr/bin/env python3
"""
Promote structured data from metadata JSONB to proper columns.
Only moves data to columns that already exist — no DDL required.

Usage:
    python -m pipeline.promote.metadata [--dry-run]
"""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all

MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def parse_date(val: str | None) -> str | None:
    if not val:
        return None
    s = str(val).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    m = re.match(r"^(\w+)\s+(\d{1,2}),?\s+(\d{4})$", s)
    if m:
        mm = MONTHS.get(m.group(1).lower())
        if mm:
            return f"{m.group(3)}-{mm}-{m.group(2).zfill(2)}"
    m2 = re.match(r"^(\w+)\s+(\d{4})$", s)
    if m2:
        mm = MONTHS.get(m2.group(1).lower())
        if mm:
            return f"{m2.group(2)}-{mm}-01"
    return None


def clean_metadata(meta: dict, keys_to_delete: list[str]) -> dict | None:
    new_meta = {k: v for k, v in meta.items() if k not in keys_to_delete}
    return new_meta if new_meta else None


def main():
    parser = argparse.ArgumentParser(description="Promote metadata to proper columns")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    dry = args.dry_run

    print(f"Promoting metadata to proper columns...{' (DRY RUN)' if dry else ''}\n")

    # 1. WINES: metadata.vinification -> wines.vinification_notes
    print("=== Wine vinification notes ===")
    wines = fetch_all("wines", "id,name,vinification_notes,metadata")
    vinification_promoted = 0
    for w in wines:
        if w.get("vinification_notes"):
            continue
        meta = w.get("metadata") or {}
        vinif = meta.get("vinification") or meta.get("vinification_notes")
        if not vinif:
            continue
        cleaned = re.sub(r"^[•]\s*", "", str(vinif), flags=re.MULTILINE)
        cleaned = re.sub(r"\n+", " ", cleaned).strip()
        if not cleaned:
            continue

        if dry:
            if vinification_promoted < 5:
                print(f"  [DRY] {w['name']}: {cleaned[:80]}...")
            vinification_promoted += 1
            continue

        new_meta = clean_metadata(meta, ["vinification", "vinification_notes"])
        result = sb.table("wines").update({
            "vinification_notes": cleaned,
            "metadata": new_meta,
        }).eq("id", w["id"]).execute()
        if result.data is not None:
            vinification_promoted += 1
        else:
            print(f"  Warning: error for {w['name']}")

    print(f"  Promoted: {vinification_promoted}\n")

    # 2. WINES: metadata.style -> wines.style
    print("=== Wine style ===")
    print("  (Already promoted in prior session)\n")

    # 3. WINE_VINTAGES: metadata.release_date -> wine_vintages.release_date
    print("=== Vintage release dates ===")
    vintages = fetch_all("wine_vintages", "id,wine_id,vintage_year,release_date,metadata")
    release_date_promoted = 0
    for v in vintages:
        if v.get("release_date"):
            continue
        meta = v.get("metadata") or {}
        rd = meta.get("release_date")
        if not rd:
            continue
        parsed = parse_date(rd)
        if not parsed:
            if release_date_promoted == 0:
                print(f"  Warning: could not parse: \"{rd}\"")
            continue

        if dry:
            if release_date_promoted < 5:
                print(f"  [DRY] vintage {v['vintage_year']}: {rd} -> {parsed}")
            release_date_promoted += 1
            continue

        new_meta = clean_metadata(meta, ["release_date"])
        result = sb.table("wine_vintages").update({
            "release_date": parsed,
            "metadata": new_meta,
        }).eq("id", v["id"]).execute()
        if result.data is not None:
            release_date_promoted += 1
        else:
            print(f"  Warning: error updating vintage")

    print(f"  Promoted: {release_date_promoted}\n")

    # 4. WINES: metadata.first_vintage -> wines.first_vintage_year
    print("=== Wine first_vintage_year ===")
    first_vintage_promoted = 0
    for w in wines:
        meta = w.get("metadata") or {}
        fv = meta.get("first_vintage")
        if not fv:
            continue
        try:
            year = int(str(fv))
        except ValueError:
            continue
        if year < 1800 or year > 2030:
            continue

        if dry:
            if first_vintage_promoted < 5:
                print(f"  [DRY] {w['name']}: first_vintage -> {year}")
            first_vintage_promoted += 1
            continue

        new_meta = clean_metadata(meta, ["first_vintage"])
        result = sb.table("wines").update({
            "first_vintage_year": year,
            "metadata": new_meta,
        }).eq("id", w["id"]).execute()
        if result.data is not None:
            first_vintage_promoted += 1
        else:
            print(f"  Warning: error for {w['name']}")

    print(f"  Promoted: {first_vintage_promoted}\n")

    # 5. PRODUCERS: metadata.annual_production -> producers.total_production_cases
    print("=== Producer annual production ===")
    producers = fetch_all("producers", "id,name,total_production_cases,metadata")
    prod_promoted = 0
    for p in producers:
        if p.get("total_production_cases"):
            continue
        meta = p.get("metadata") or {}
        ap = meta.get("annual_production") or meta.get("annual_production_cases") or meta.get("production_cases")
        if not ap:
            continue
        num_str = re.sub(r",", "", str(ap))
        num_str = re.sub(r"\s*cases?", "", num_str, flags=re.IGNORECASE).strip()
        try:
            num = int(num_str)
        except ValueError:
            continue
        if num <= 0:
            continue

        if dry:
            if prod_promoted < 5:
                print(f"  [DRY] {p['name']}: {ap} -> {num}")
            prod_promoted += 1
            continue

        new_meta = clean_metadata(meta, ["annual_production", "annual_production_cases", "production_cases"])
        result = sb.table("producers").update({
            "total_production_cases": num,
            "metadata": new_meta,
        }).eq("id", p["id"]).execute()
        if result.data is not None:
            prod_promoted += 1
        else:
            print(f"  Warning: error for {p['name']}")

    print(f"  Promoted: {prod_promoted}\n")

    # 6. PRODUCERS: metadata.philosophy -> producers.philosophy
    print("=== Producer philosophy ===")
    phil_promoted = 0
    for p in producers:
        meta = p.get("metadata") or {}
        phil = meta.get("philosophy")
        if not phil:
            continue
        phil_promoted += 1
    print(f"  {phil_promoted} still in metadata (may already be in column -- verify)\n")

    print("=" * 40)
    print("SUMMARY")
    print("=" * 40)
    print(f"  Vinification notes promoted: {vinification_promoted}")
    print(f"  Release dates promoted: {release_date_promoted}")
    print(f"  First vintage years promoted: {first_vintage_promoted}")
    print(f"  Production cases promoted: {prod_promoted}")
    print()
    print("Remaining metadata requiring new columns/tables:")
    print("  wine.soil: ~1,489 entries (needs vineyard_soils or wine.soil_description)")
    print("  wine.vine_age: ~1,469 entries (needs wines.vine_age_description)")
    print("  wine.vineyard_area: ~1,468 entries (needs wines.vineyard_area_ha)")
    print("  wine.classification: ~77 entries (needs entity_classifications links)")
    print("  wine.commune: ~53 entries (needs wines.commune)")
    print("  wine.vdp_level: ~28 entries (needs entity_classifications links)")
    print("  producer.winemaker: ~195 entries (needs producer_winemakers links)")
    print("  producer.location: ~198 entries (needs producer address fields)")


if __name__ == "__main__":
    main()
