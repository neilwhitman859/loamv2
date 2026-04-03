"""
Phase B: Create new canonical producers from TTB brand names.

Reads from _phase_b_producers helper table (pre-filtered candidates).
Creates producer records, then links TTB records to the new producers.

Usage:
    python -m pipeline.promote.phase_b_producers [--min-count 50] [--dry-run]
"""

import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify


def title_case_brand(brand):
    """Convert TTB ALL-CAPS brand to proper title case."""
    name = brand.title()
    # Fix common title-case issues
    name = re.sub(r"\bLlc\b", "LLC", name)
    name = re.sub(r"\bInc\.?\b", "Inc.", name)
    name = re.sub(r"\bL\.?P\.?\b", "LP", name)
    name = re.sub(r"\bNv\b", "NV", name)
    # Lowercase small words (except at start)
    small = ['And', 'Of', 'The', 'De', 'Du', 'Des', 'La', 'Le', 'Les',
             'Di', 'Da', 'Del', 'Della', 'Et', 'En', 'El', 'Los', 'Von', 'Van']
    for w in small:
        name = re.sub(rf'\b{w}\b', w.lower(), name)
    # Re-capitalize first letter
    if name:
        name = name[0].upper() + name[1:]
    # Fix D' prefix (D'Arenberg, D'Oliveira)
    name = re.sub(r"\bD '", "D'", name)
    name = re.sub(r"\bD'([a-z])", lambda m: "d'" + m.group(1).upper(), name)
    return name


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load candidates
    print(f"Loading Phase B candidates (min {args.min_count} records)...")
    candidates = []
    offset = 0
    while True:
        result = (sb.table("_phase_b_producers")
                  .select("brand_name,record_count,best_origin,country_id")
                  .gte("record_count", args.min_count)
                  .order("record_count", desc=True)
                  .range(offset, offset + 999)
                  .execute())
        candidates.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(candidates)} candidates")

    # Load US country_id
    us_result = sb.table("countries").select("id").eq("name", "United States").limit(1).execute()
    us_id = us_result.data[0]["id"] if us_result.data else None

    # Load existing producer name_normalized for dedup
    print("Loading existing producers for dedup...")
    existing_norms = set()
    offset = 0
    while True:
        result = (sb.table("producers")
                  .select("name_normalized")
                  .is_("deleted_at", "null")
                  .range(offset, offset + 999)
                  .execute())
        for p in result.data:
            existing_norms.add(p["name_normalized"])
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_norms)} existing")

    # Create producers
    created = 0
    skipped = 0
    errors = 0
    brand_to_id = {}

    for i, c in enumerate(candidates):
        brand = c["brand_name"]
        norm = normalize(brand)

        if norm in existing_norms:
            skipped += 1
            continue

        display = title_case_brand(brand)
        country_id = c.get("country_id") or (us_id if c.get("best_origin") == "D" else None)

        if args.dry_run:
            brand_to_id[brand] = "dry-run"
            created += 1
            continue

        try:
            result = sb.table("producers").insert({
                "name": display,
                "name_normalized": norm,
                "slug": slugify(brand)[:200],
                "country_id": country_id,
                "producer_type": "estate",
            }).execute()
            if result.data:
                pid = result.data[0]["id"]
                brand_to_id[brand] = pid
                existing_norms.add(norm)
                created += 1
        except Exception as e:
            errors += 1
            if "duplicate" in str(e).lower():
                skipped += 1
            elif errors <= 5:
                print(f"  Error creating {brand}: {e}")

        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(candidates)} processed, {created} created, {skipped} skipped")

    print(f"\nProducers created: {created}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Errors: {errors}")

    if args.dry_run:
        print("\nDRY RUN — no writes")
        return

    # Link TTB records to new producers
    print(f"\nLinking TTB records to {len(brand_to_id)} new producers...")
    total_linked = 0
    for i, (brand, pid) in enumerate(brand_to_id.items()):
        try:
            result = (sb.table("source_ttb_colas")
                      .update({"canonical_producer_id": pid})
                      .eq("brand_name", brand)
                      .is_("canonical_producer_id", "null")
                      .execute())
            cnt = len(result.data) if result.data else 0
            total_linked += cnt
        except Exception:
            pass

        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(brand_to_id)} brands, {total_linked} records linked")

    print(f"  Total TTB records linked: {total_linked}")


if __name__ == "__main__":
    main()
