"""
Derives the French AOC hierarchy from spatial containment of existing
boundary polygons (Eurac municipality-level data) in PostGIS.

Approach:
  1. Load pre-computed containment pairs from cache (or query PostGIS)
  2. Prune transitive ancestors to get direct parents only
  3. Insert into appellation_containment with source='spatially_derived'

Usage:
  python -m pipeline.geo.populate_fr_containment --dry-run
  python -m pipeline.geo.populate_fr_containment --apply
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase, get_env

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_FILE = PROJECT_ROOT / "data" / "fr_containment_raw.json"


def fetch_containment_via_sql():
    """Fetch containment pairs using the Supabase SQL endpoint."""
    url = get_env("SUPABASE_URL")
    key = get_env("SUPABASE_SERVICE_ROLE")

    sql_url = f"{url}/pg"
    resp = httpx.post(
        sql_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
            "apikey": key,
        },
        json={
            "query": """
                WITH fr_bounds AS (
                  SELECT a.id, a.name, gb.boundary,
                         extensions.ST_Area(gb.boundary::extensions.geography) as area_m2
                  FROM appellations a
                  JOIN countries c ON a.country_id = c.id
                  JOIN geographic_boundaries gb ON gb.appellation_id = a.id
                  WHERE c.iso_code = 'FR' AND a.deleted_at IS NULL AND gb.boundary IS NOT NULL
                )
                SELECT p.id as parent_id, p.name as parent_name,
                       c.id as child_id, c.name as child_name,
                       p.area_m2 as parent_area, c.area_m2 as child_area
                FROM fr_bounds p
                JOIN fr_bounds c ON p.id != c.id
                WHERE extensions.ST_Contains(p.boundary, c.boundary)
                  AND p.area_m2 > c.area_m2 * 1.01
                ORDER BY p.area_m2 DESC, c.area_m2 DESC
            """
        },
        timeout=300,
    )
    if resp.is_success:
        return resp.json()
    raise Exception(f"SQL API failed: {resp.status_code} {resp.text[:200]}")


def prune_transitive(pairs):
    """Prune transitive ancestors to keep only direct parent-child relationships."""
    print(f"\nPruning transitive ancestors from {len(pairs)} pairs...")

    child_to_parents = {}
    pair_map = {}
    for p in pairs:
        child_to_parents.setdefault(p["child_id"], set()).add(p["parent_id"])
        pair_map[f"{p['parent_id']}|{p['child_id']}"] = p

    direct_pairs = []
    pruned_count = 0

    for child_id, parent_ids in child_to_parents.items():
        parents = list(parent_ids)
        for parent_id in parents:
            is_transitive = False
            for other_parent_id in parents:
                if other_parent_id == parent_id:
                    continue
                other_parents = child_to_parents.get(other_parent_id, set())
                if parent_id in other_parents:
                    is_transitive = True
                    break

            if not is_transitive:
                pair = pair_map.get(f"{parent_id}|{child_id}")
                if pair:
                    direct_pairs.append(pair)
            else:
                pruned_count += 1

    print(f"Pruned {pruned_count} transitive relationships")
    print(f"Direct parent-child relationships: {len(direct_pairs)}")
    return direct_pairs


def main():
    parser = argparse.ArgumentParser(description="France AOC spatial containment")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.apply
    sb = get_supabase()
    print(f"\n=== France AOC Spatial Containment {'(DRY RUN)' if dry_run else ''} ===\n")

    # Load or compute containment pairs
    if CACHE_FILE.exists():
        pairs = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        print(f"Loaded {len(pairs)} pre-computed containment pairs from cache")
    else:
        pairs = fetch_containment_via_sql()
        print(f"Fetched {len(pairs)} containment pairs from PostGIS")

    # Prune transitive
    direct_pairs = prune_transitive(pairs)

    # Multi-parent analysis
    child_parent_count = {}
    for r in direct_pairs:
        child_parent_count[r["child_name"]] = child_parent_count.get(r["child_name"], 0) + 1
    multi_parent = [(n, c) for n, c in child_parent_count.items() if c > 1]
    if multi_parent:
        print(f"\nMulti-parent appellations ({len(multi_parent)}):")
        for name, count in multi_parent[:10]:
            parents = [r["parent_name"] for r in direct_pairs if r["child_name"] == name]
            print(f"  {name} -> {' + '.join(parents)} ({count} parents)")
        if len(multi_parent) > 10:
            print(f"  ... and {len(multi_parent) - 10} more")

    # Sample hierarchies
    print("\nSample hierarchies:")
    for region in ["Bordeaux", "Bourgogne", "Côtes du Rhône", "Languedoc", "Champagne", "Alsace / Vin d'Alsace"]:
        children = [r for r in direct_pairs if r["parent_name"] == region]
        if children:
            child_names = sorted(r["child_name"] for r in children)
            sample = ", ".join(child_names[:8])
            suffix = "..." if len(children) > 8 else ""
            print(f"  {region} ({len(children)} children): {sample}{suffix}")

    # Depth analysis
    print("\nHierarchy depth analysis:")
    top_level = {r["parent_name"] for r in direct_pairs}
    child_set = {r["child_name"] for r in direct_pairs}
    roots = sorted(n for n in top_level if n not in child_set)
    print(f"  Root appellations (no parent): {len(roots)}")
    print(f"    {', '.join(roots)}")
    leaves = [n for n in child_set if n not in top_level]
    print(f"  Leaf appellations (no children): {len(leaves)}")

    if dry_run:
        print(f"\n=== DRY RUN SUMMARY ===")
        print(f"Direct containment relationships: {len(direct_pairs)}")
        print("Source: spatially_derived (Eurac municipality boundaries)")
        print("\n[DRY RUN] No changes made.")
        return

    # Insert
    existing = sb.table("appellation_containment").select("parent_id, child_id").execute().data
    existing_set = {f"{r['parent_id']}|{r['child_id']}" for r in (existing or [])}
    to_insert = [
        {"parent_id": r["parent_id"], "child_id": r["child_id"], "source": "spatially_derived"}
        for r in direct_pairs
        if f"{r['parent_id']}|{r['child_id']}" not in existing_set
    ]

    if not to_insert:
        print("\nAll relationships already exist in DB. Nothing to insert.")
        return

    print(f"\nInserting {len(to_insert)} new containment rows (source: spatially_derived)...")
    BATCH = 500
    inserted = 0
    for i in range(0, len(to_insert), BATCH):
        batch = to_insert[i:i + BATCH]
        sb.table("appellation_containment").insert(batch).execute()
        inserted += len(batch)
        print(f"  Inserted {inserted}/{len(to_insert)}")

    print(f"\nDone! Inserted {inserted} French AOC containment relationships.")


if __name__ == "__main__":
    main()
