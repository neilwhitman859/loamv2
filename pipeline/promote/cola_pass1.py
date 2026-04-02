"""
COLA Pass 1: Deterministic matching of TTB COLA records to LWIN canonical data.

Steps:
  1b. Link producers: exact brand_name match to LWIN producers (per-brand UPDATE)
  1c. Link wines: exact fanciful_name match within matched producers (non-ambiguous only)
  1d. Create wine_vintages and external_ids for matched wines

Usage:
  python -m pipeline.promote.cola_pass1 [--dry-run] [--step 1b|1c|1d|all]
"""

import argparse
import time
from pipeline.lib.db import get_supabase, batch_insert


WINE_CLASS_TYPES = ['80', '81', '80A', '84', '88']


def load_producer_map(sb):
    """Load all LWIN producers into an UPPER(name) -> id map."""
    print("Loading producers...")
    producers = {}
    offset = 0
    while True:
        result = sb.table("producers").select("id,name").range(offset, offset + 999).execute()
        for p in result.data:
            producers[p["name"].upper()] = p["id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  Loaded {len(producers):,} producers")
    return producers


def load_wine_map(sb):
    """Load all LWIN wines into (producer_id, UPPER(name)) -> wine_id map.
    Excludes ambiguous wines (same name under same producer)."""
    print("Loading wines...")
    wines = {}
    ambiguous = set()
    offset = 0
    while True:
        result = sb.table("wines").select("id,name,producer_id").range(offset, offset + 999).execute()
        for w in result.data:
            key = (w["producer_id"], w["name"].upper())
            if key in wines:
                ambiguous.add(key)
            else:
                wines[key] = w["id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    for key in ambiguous:
        wines.pop(key, None)
    print(f"  Loaded {len(wines) + len(ambiguous):,} wines, {len(ambiguous):,} ambiguous (excluded)")
    return wines


def step_1b(sb, producer_map, dry_run=False):
    """Link TTB COLA records to LWIN producers via exact brand_name match.
    Updates one brand at a time using the brand_name index."""
    print("\n=== Step 1b: Link producers ===")

    total_linked = 0
    brands_linked = 0
    errors = 0

    brand_names = list(producer_map.keys())
    print(f"  Testing {len(brand_names):,} producer names against TTB brand_name...")

    for i, brand_upper in enumerate(brand_names):
        producer_id = producer_map[brand_upper]

        try:
            if dry_run:
                result = (sb.table("source_ttb_colas")
                          .select("ttb_id", count="exact")
                          .eq("brand_name", brand_upper)
                          .is_("canonical_producer_id", "null")
                          .execute())
                count = result.count or 0
            else:
                result = (sb.table("source_ttb_colas")
                          .update({"canonical_producer_id": producer_id})
                          .eq("brand_name", brand_upper)
                          .is_("canonical_producer_id", "null")
                          .execute())
                count = len(result.data) if result.data else 0
            if count > 0:
                total_linked += count
                brands_linked += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error on {brand_upper}: {e}", flush=True)

        if (i + 1) % 1000 == 0:
            print(f"  {i + 1:,}/{len(brand_names):,} brands checked... "
                  f"({brands_linked:,} matched, {total_linked:,} COLAs linked)", flush=True)

    print(f"\nResults:")
    print(f"  {brands_linked:,} brands matched")
    print(f"  {total_linked:,} COLA records linked")
    if errors:
        print(f"  {errors} errors")
    if dry_run:
        print("  (DRY RUN — no updates written)")

    return total_linked


def load_tmp_wine_match(sb):
    """Load the _tmp_wine_match table into a producer_id -> {name_upper: wine_id} map."""
    print("Loading _tmp_wine_match...")
    producer_wines = {}
    offset = 0
    while True:
        result = (sb.table("_tmp_wine_match")
                  .select("producer_id,name_upper,wine_id")
                  .range(offset, offset + 999)
                  .execute())
        for row in result.data:
            pid = row["producer_id"]
            if pid not in producer_wines:
                producer_wines[pid] = {}
            producer_wines[pid][row["name_upper"]] = row["wine_id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    total_wines = sum(len(v) for v in producer_wines.values())
    print(f"  Loaded {total_wines:,} non-ambiguous wines across {len(producer_wines):,} producers")
    return producer_wines


def step_1c(sb, producer_wines, dry_run=False):
    """Link TTB COLA records to LWIN wines via exact fanciful_name match.
    Uses _tmp_wine_match pre-computed table. Iterates per-producer via REST API."""
    print("\n=== Step 1c: Link wines ===")

    producer_ids = list(producer_wines.keys())
    print(f"  {len(producer_ids):,} producers with non-ambiguous wines")

    total_linked = 0
    producers_with_matches = 0
    errors = 0

    for i, producer_id in enumerate(producer_ids):
        wines_for_producer = producer_wines[producer_id]

        # For each wine name under this producer, try to update matching COLAs
        for name_upper, wine_id in wines_for_producer.items():
            try:
                if dry_run:
                    result = (sb.table("source_ttb_colas")
                              .select("ttb_id", count="exact")
                              .eq("canonical_producer_id", producer_id)
                              .eq("fanciful_name", name_upper)
                              .is_("canonical_wine_id", "null")
                              .execute())
                    count = result.count or 0
                else:
                    result = (sb.table("source_ttb_colas")
                              .update({"canonical_wine_id": wine_id})
                              .eq("canonical_producer_id", producer_id)
                              .eq("fanciful_name", name_upper)
                              .is_("canonical_wine_id", "null")
                              .execute())
                    count = len(result.data) if result.data else 0
                if count > 0:
                    total_linked += count
                    producers_with_matches += 1
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  Error on {producer_id}/{name_upper}: {e}", flush=True)

        if (i + 1) % 2000 == 0:
            print(f"  {i + 1:,}/{len(producer_ids):,} producers... "
                  f"({total_linked:,} COLAs linked)", flush=True)

    print(f"\nResults:")
    print(f"  {total_linked:,} COLA records linked to wines")
    if errors:
        print(f"  {errors} errors")
    if dry_run:
        print("  (DRY RUN — no updates written)")

    return total_linked


def step_1d(sb, dry_run=False):
    """Create wine_vintages and external_ids for wine-linked COLAs."""
    print("\n=== Step 1d: Create wine_vintages + external_ids ===")

    # Load existing wine_vintages
    print("Loading existing wine_vintages...")
    existing_vintages = set()
    offset = 0
    while True:
        result = sb.table("wine_vintages").select("wine_id,vintage_year").range(offset, offset + 999).execute()
        for v in result.data:
            existing_vintages.add((v["wine_id"], v["vintage_year"]))
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_vintages):,} existing vintages")

    # Load existing COLA external_ids
    print("Loading existing external_ids (cola)...")
    existing_ext = set()
    offset = 0
    while True:
        result = (sb.table("external_ids")
                  .select("entity_id,external_id")
                  .eq("system", "cola")
                  .range(offset, offset + 999)
                  .execute())
        for e in result.data:
            existing_ext.add((e["entity_id"], e["external_id"]))
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_ext):,} existing cola links")

    # Scan wine-linked COLAs
    print("Scanning wine-linked COLAs...")
    vintage_inserts = []
    ext_id_inserts = []
    seen_vintages = set()
    seen_ext = set()
    offset = 0

    while True:
        result = (sb.table("source_ttb_colas")
                  .select("ttb_id,canonical_wine_id,wine_vintage,abv")
                  .not_.is_("canonical_wine_id", "null")
                  .in_("class_type", WINE_CLASS_TYPES)
                  .range(offset, offset + 999)
                  .execute())

        if not result.data:
            break

        for row in result.data:
            wine_id = row["canonical_wine_id"]
            ttb_id = row["ttb_id"]

            # External ID
            ext_key = (wine_id, ttb_id)
            if ext_key not in existing_ext and ext_key not in seen_ext:
                ext_id_inserts.append({
                    "entity_type": "wine",
                    "entity_id": wine_id,
                    "system": "cola",
                    "external_id": ttb_id,
                })
                seen_ext.add(ext_key)

            # Wine vintage
            vintage_str = row.get("wine_vintage")
            if vintage_str:
                try:
                    vy = int(vintage_str)
                    if 1900 <= vy <= 2030:
                        vkey = (wine_id, vy)
                        if vkey not in existing_vintages and vkey not in seen_vintages:
                            vrow = {"wine_id": wine_id, "vintage_year": vy}
                            if row.get("abv"):
                                try:
                                    vrow["abv"] = float(row["abv"])
                                except (ValueError, TypeError):
                                    pass
                            vintage_inserts.append(vrow)
                            seen_vintages.add(vkey)
                except (ValueError, TypeError):
                    pass

        offset += len(result.data)
        if len(result.data) < 1000:
            break

    print(f"\nTo create:")
    print(f"  {len(vintage_inserts):,} new wine_vintages")
    print(f"  {len(ext_id_inserts):,} new external_ids (cola)")

    if not dry_run:
        if vintage_inserts:
            print("Inserting wine_vintages...")
            count = batch_insert("wine_vintages", vintage_inserts, batch_size=200)
            print(f"  Inserted {count:,}")

        if ext_id_inserts:
            print("Inserting external_ids...")
            count = batch_insert("external_ids", ext_id_inserts, batch_size=200)
            print(f"  Inserted {count:,}")
    else:
        print("(DRY RUN — no inserts)")

    return len(vintage_inserts), len(ext_id_inserts)


def main():
    parser = argparse.ArgumentParser(description="COLA Pass 1: Deterministic LWIN matching")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--step", default="all", choices=["1b", "1c", "1d", "all"])
    args = parser.parse_args()

    sb = get_supabase()
    producer_map = load_producer_map(sb)

    if args.step in ("1b", "all"):
        step_1b(sb, producer_map, dry_run=args.dry_run)

    if args.step in ("1c", "all"):
        producer_wines = load_tmp_wine_match(sb)
        step_1c(sb, producer_wines, dry_run=args.dry_run)

    if args.step in ("1d", "all"):
        step_1d(sb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
