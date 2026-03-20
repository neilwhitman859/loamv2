"""
Rebuild the regions table from data/regions_rebuild.json.

Two modes:
  --dry-run   Validate JSON, check countries exist in DB, report what would be inserted (default)
  --insert    Actually insert new regions into the DB (does NOT delete old regions)

Inserts in order: catch-all -> L1 -> L2 (respecting parent_id references).

Usage:
    python -m pipeline.reference.rebuild_regions
    python -m pipeline.reference.rebuild_regions --insert
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


def fetch_all(sb, table: str, columns: str = "*") -> list[dict]:
    """Paginate through a Supabase table."""
    rows: list[dict] = []
    offset = 0
    batch_size = 1000
    while True:
        result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size
    return rows


def main():
    parser = argparse.ArgumentParser(description="Rebuild regions from JSON")
    parser.add_argument("--insert", action="store_true", help="Insert new regions (default is dry-run)")
    parser.add_argument("--file", default="data/regions_rebuild.json", help="Path to regions JSON")
    args = parser.parse_args()

    sb = get_supabase()

    # ── Step 1: Load and validate JSON ────────────────────────────
    print("Step 1: Loading and validating data/regions_rebuild.json...\n")

    filepath = Path(args.file)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath

    if not filepath.exists():
        print(f"  File not found: {filepath}")
        sys.exit(1)

    regions = json.loads(filepath.read_text(encoding="utf-8"))

    catch_all = [r for r in regions if r.get("is_catch_all")]
    l1 = [r for r in regions if not r.get("is_catch_all") and not r.get("parent")]
    l2 = [r for r in regions if not r.get("is_catch_all") and r.get("parent")]

    print(f"  Total entries: {len(regions)}")
    print(f"  Catch-all: {len(catch_all)}")
    print(f"  L1: {len(l1)}")
    print(f"  L2: {len(l2)}")

    # Check duplicate slugs
    slugs = [r["slug"] for r in regions]
    seen: set[str] = set()
    dupes = []
    for s in slugs:
        if s in seen:
            dupes.append(s)
        seen.add(s)
    if dupes:
        print(f"\n  ERROR: Duplicate slugs found: {', '.join(dupes)}")
        sys.exit(1)
    print("  Duplicate slugs: none")

    # Check L2 parents reference valid L1 slugs
    l1_slugs = {r["slug"] for r in l1}
    bad_parents = [r for r in l2 if r["parent"] not in l1_slugs]
    if bad_parents:
        print("\n  ERROR: L2 regions with invalid parents:")
        for r in bad_parents:
            print(f"    {r['slug']} -> {r['parent']}")
        sys.exit(1)
    print("  L2 parent references: all valid")

    # Check each country has exactly one catch-all
    countries_in_data = {r["country"] for r in regions}
    ca_by_country: dict[str, int] = {}
    for r in catch_all:
        ca_by_country[r["country"]] = ca_by_country.get(r["country"], 0) + 1

    for c in countries_in_data:
        if not ca_by_country.get(c):
            print(f"\n  ERROR: Country '{c}' has no catch-all region")
            sys.exit(1)
        if ca_by_country[c] > 1:
            print(f"\n  ERROR: Country '{c}' has {ca_by_country[c]} catch-all regions")
            sys.exit(1)
    print("  Catch-all per country: exactly one each")

    # ── Step 2: Check countries exist in DB ───────────────────────
    print("\nStep 2: Verifying countries exist in database...\n")

    all_countries = fetch_all(sb, "countries", "id,name,slug")
    country_by_name = {c["name"]: c for c in all_countries}

    missing = [c for c in countries_in_data if c not in country_by_name]
    if missing:
        print(f"  ERROR: Countries not found in DB: {', '.join(missing)}")
        sys.exit(1)
    print(f"  All {len(countries_in_data)} countries found in DB")

    # ── Step 3: Check for existing regions with same slugs ────────
    print("\nStep 3: Checking for slug conflicts with existing regions...\n")

    existing_regions = fetch_all(sb, "regions", "id,name,slug,country_id,is_catch_all,parent_id")
    print(f"  Existing regions in DB: {len(existing_regions)}")

    existing_slug_set = {r["slug"] for r in existing_regions}
    conflicts = [r for r in regions if r["slug"] in existing_slug_set]

    if conflicts:
        ca_conflicts = [r for r in conflicts if r.get("is_catch_all")]
        non_ca_conflicts = [r for r in conflicts if not r.get("is_catch_all")]
        print(f"  Slug conflicts: {len(conflicts)} ({len(ca_conflicts)} catch-all, {len(non_ca_conflicts)} non-catch-all)")
        if non_ca_conflicts:
            print("  Non-catch-all conflicts:")
            for r in non_ca_conflicts[:10]:
                print(f"    {r['slug']} ({r['country']}/{r['name']})")
            if len(non_ca_conflicts) > 10:
                print(f"    ... and {len(non_ca_conflicts) - 10} more")
    else:
        print("  No slug conflicts")

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Would insert: {len(regions) - len(conflicts)} new regions")
    print(f"  Would skip (slug exists): {len(conflicts)}")

    # Per-country breakdown
    print("\n  Per-country breakdown:")
    for c in sorted(countries_in_data):
        regs = [r for r in regions if r["country"] == c and not r.get("is_catch_all")]
        if not regs:
            continue
        new_regs = [r for r in regs if r["slug"] not in existing_slug_set]
        existing_regs = [r for r in regs if r["slug"] in existing_slug_set]
        print(f"    {c}: {len(regs)} regions ({len(new_regs)} new, {len(existing_regs)} existing)")

    if not args.insert:
        print("\n  Mode: DRY RUN -- no changes made")
        print("  Run with --insert to insert new regions into DB")
        return

    # ── Step 4: Insert new regions ───────────────────────────────
    print(f"\n{'=' * 60}")
    print("INSERTING NEW REGIONS")
    print(f"{'=' * 60}")

    country_id_by_name = {c["name"]: c["id"] for c in all_countries}

    # Insert catch-all regions first
    inserted = 0
    skipped = 0

    print("\n  Inserting catch-all regions...")
    for r in catch_all:
        if r["slug"] in existing_slug_set:
            skipped += 1
            continue
        result = sb.table("regions").insert({
            "name": r["name"],
            "slug": r["slug"],
            "country_id": country_id_by_name[r["country"]],
            "is_catch_all": True,
            "parent_id": None,
        }).execute()
        if result.data:
            inserted += 1
        else:
            print(f"    ERROR inserting catch-all {r['slug']}")
    print(f"    Inserted: {inserted}, Skipped (existing): {skipped}")

    # Insert L1 regions
    print("\n  Inserting L1 regions...")
    l1_inserted = 0
    l1_skipped = 0
    new_l1_id_by_slug: dict[str, str] = {}

    for r in l1:
        if r["slug"] in existing_slug_set:
            existing = next((e for e in existing_regions if e["slug"] == r["slug"]), None)
            if existing:
                new_l1_id_by_slug[r["slug"]] = existing["id"]
            l1_skipped += 1
            continue
        result = sb.table("regions").insert({
            "name": r["name"],
            "slug": r["slug"],
            "country_id": country_id_by_name[r["country"]],
            "is_catch_all": False,
            "parent_id": None,
        }).select("id").execute()
        if result.data:
            new_l1_id_by_slug[r["slug"]] = result.data[0]["id"]
            l1_inserted += 1
        else:
            print(f"    ERROR inserting L1 {r['slug']}")
    print(f"    Inserted: {l1_inserted}, Skipped (existing): {l1_skipped}")

    # Insert L2 regions
    print("\n  Inserting L2 regions...")
    l2_inserted = 0
    l2_skipped = 0
    l2_errors = 0

    for r in l2:
        if r["slug"] in existing_slug_set:
            l2_skipped += 1
            continue
        parent_id = new_l1_id_by_slug.get(r["parent"])
        if not parent_id:
            print(f"    ERROR: No parent ID found for {r['slug']} -> {r['parent']}")
            l2_errors += 1
            continue
        try:
            sb.table("regions").insert({
                "name": r["name"],
                "slug": r["slug"],
                "country_id": country_id_by_name[r["country"]],
                "is_catch_all": False,
                "parent_id": parent_id,
            }).execute()
            l2_inserted += 1
        except Exception as e:
            print(f"    ERROR inserting L2 {r['slug']}: {e}")
            l2_errors += 1
    print(f"    Inserted: {l2_inserted}, Skipped (existing): {l2_skipped}, Errors: {l2_errors}")

    # Final summary
    print(f"\n{'=' * 60}")
    print("DONE")
    print(f"{'=' * 60}")
    print(f"  Total inserted: {inserted + l1_inserted + l2_inserted}")
    print(f"  Total skipped: {skipped + l1_skipped + l2_skipped}")
    print(f"  Total errors: {l2_errors}")

    # Verify final count
    count_result = sb.table("regions").select("id", count="exact").execute()
    print(f"  Total regions in DB now: {count_result.count}")


if __name__ == "__main__":
    main()
