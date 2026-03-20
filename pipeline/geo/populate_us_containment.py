"""
Parses UC Davis AVA GeoJSON within/contains fields to populate
appellation_containment with direct parent-child relationships for US AVAs.

Key logic:
  - The 'within' field lists ALL ancestors (transitive closure)
  - We compute direct parents by pruning transitive ancestors
  - 5 AVAs have 2 direct parents (DAG cases)
  - 5 pairs of overlapping AVAs are skipped

Usage:
  python -m pipeline.geo.populate_us_containment --dry-run
  python -m pipeline.geo.populate_us_containment --apply
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import fetch_all_paginated

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data quality fixes for the UC Davis 'within' field
WITHIN_FIXES = {
    "Lake Wisconsin": lambda w: w.replace("Upper Mississippi|River Valley", "Upper Mississippi River Valley"),
    "Wild Horse Valley": lambda w: w.replace("Solano County|Green Valley", "Solano County Green Valley"),
    "Green Valley of Russian River Valley": lambda w: w.replace("Northern Sonoma Valley", "Northern Sonoma"),
    "West Sonoma Coast": lambda w: w.replace("Sonoma Coast, North Coast", "Sonoma Coast|North Coast"),
}

OVERLAP_PAIRS = {
    "Sonoma Coast|Northern Sonoma",
    "Northern Sonoma|Sonoma Coast",
    "Russian River Valley|Alexander Valley",
    "Alexander Valley|Russian River Valley",
    "Mendocino Ridge|Anderson Valley",
    "Anderson Valley|Mendocino Ridge",
    "Alexander Valley|Pine Mountain-Cloverdale Peak",
    "Pine Mountain-Cloverdale Peak|Alexander Valley",
    "Dry Creek Valley|Rockpile",
    "Rockpile|Dry Creek Valley",
}

GEO_TO_DB_NAME = {
    "Mt. Veeder": "Mount Veeder",
    "Moon Mountain District Sonoma County": "Moon Mountain District",
    "San Luis Obispo Coast": "San Luis Obispo",
    "San Benito": "San Benito County",
    "Contra Costa": "Contra Costa County",
}


def main():
    parser = argparse.ArgumentParser(description="US AVA containment import")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.apply
    sb = get_supabase()
    print(f"\n=== US AVA Containment Import {'(DRY RUN)' if dry_run else ''} ===\n")

    # 1. Load GeoJSON
    geojson_path = PROJECT_ROOT / "avas_ucdavis.geojson"
    geojson = json.loads(geojson_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(geojson['features'])} AVAs from GeoJSON")

    geo_by_name = {f["properties"]["name"]: f["properties"] for f in geojson["features"]}

    # 2. Load all US AVAs from DB
    us_apps = []
    page = 0
    page_size = 1000
    while True:
        result = (
            sb.table("appellations")
            .select("id, name, designation_type, country_id")
            .eq("designation_type", "AVA")
            .range(page * page_size, (page + 1) * page_size - 1)
            .execute()
        )
        us_apps.extend(result.data)
        if len(result.data) < page_size:
            break
        page += 1
    print(f"Loaded {len(us_apps)} US AVAs from DB")

    db_by_name = {a["name"]: a["id"] for a in us_apps}
    db_by_name_lower = {a["name"].lower(): a["id"] for a in us_apps}

    def resolve_db_id(geo_name):
        if geo_name in db_by_name:
            return {"id": db_by_name[geo_name], "matched_name": geo_name}
        mapped = GEO_TO_DB_NAME.get(geo_name)
        if mapped and mapped in db_by_name:
            return {"id": db_by_name[mapped], "matched_name": mapped}
        lower = geo_name.lower()
        if lower in db_by_name_lower:
            return {"id": db_by_name_lower[lower], "matched_name": geo_name}
        return None

    # 3. Parse 'within' fields and compute direct parents
    direct_relationships = []
    unmatched = []
    skipped_overlaps = 0

    for f in geojson["features"]:
        props = f["properties"]
        within_str = props.get("within")
        if within_str is None:
            continue

        if props["name"] in WITHIN_FIXES:
            within_str = WITHIN_FIXES[props["name"]](within_str)

        ancestors = [s.strip() for s in within_str.split("|") if s.strip()]
        if not ancestors:
            continue

        # Prune transitive ancestors
        direct_parents = []
        for ancestor in ancestors:
            is_transitive = False
            for other_ancestor in ancestors:
                if other_ancestor == ancestor:
                    continue
                other_props = geo_by_name.get(other_ancestor)
                if other_props and other_props.get("within"):
                    other_within = other_props["within"]
                    if other_ancestor in WITHIN_FIXES:
                        other_within = WITHIN_FIXES[other_ancestor](other_within)
                    other_ancestors = [s.strip() for s in other_within.split("|")]
                    if ancestor in other_ancestors:
                        is_transitive = True
                        break
            if not is_transitive:
                direct_parents.append(ancestor)

        child_resolved = resolve_db_id(props["name"])
        if not child_resolved:
            unmatched.append({"name": props["name"], "type": "child"})
            continue

        for parent_name in direct_parents:
            if f"{parent_name}|{props['name']}" in OVERLAP_PAIRS:
                skipped_overlaps += 1
                continue

            parent_resolved = resolve_db_id(parent_name)
            if not parent_resolved:
                unmatched.append({"name": parent_name, "type": "parent", "referenced_by": props["name"]})
                continue

            direct_relationships.append({
                "parent_name": parent_resolved["matched_name"],
                "child_name": child_resolved["matched_name"],
                "parent_id": parent_resolved["id"],
                "child_id": child_resolved["id"],
            })

    # Deduplicate
    seen = set()
    unique = []
    for r in direct_relationships:
        key = f"{r['parent_id']}|{r['child_id']}"
        if key not in seen:
            seen.add(key)
            unique.append(r)

    print(f"\nDirect parent-child relationships: {len(unique)}")
    print(f"Skipped overlapping pairs: {skipped_overlaps}")

    if unmatched:
        print(f"\nUnmatched names ({len(unmatched)}):")
        for u in unmatched:
            ref = f" (referenced by {u['referenced_by']})" if u.get("referenced_by") else ""
            print(f"  [{u['type']}] {u['name']}{ref}")

    # Multi-parent AVAs
    child_parent_count = {}
    for r in unique:
        child_parent_count[r["child_name"]] = child_parent_count.get(r["child_name"], 0) + 1
    multi_parent = [(n, c) for n, c in child_parent_count.items() if c > 1]
    if multi_parent:
        print(f"\nMulti-parent AVAs (DAG cases):")
        for name, count in multi_parent:
            parents = [r["parent_name"] for r in unique if r["child_name"] == name]
            print(f"  {name} -> {' + '.join(parents)} ({count} parents)")

    # Sample
    napa_children = [r for r in unique if r["parent_name"] == "Napa Valley"]
    print(f"\nSample relationships:")
    print(f"  Napa Valley has {len(napa_children)} children: {', '.join(sorted(r['child_name'] for r in napa_children))}")

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    # Insert
    existing = sb.table("appellation_containment").select("parent_id, child_id").execute().data
    existing_set = {f"{r['parent_id']}|{r['child_id']}" for r in (existing or [])}
    to_insert = [
        {"parent_id": r["parent_id"], "child_id": r["child_id"], "source": "explicit"}
        for r in unique
        if f"{r['parent_id']}|{r['child_id']}" not in existing_set
    ]

    if not to_insert:
        print("\nAll relationships already exist in DB. Nothing to insert.")
        return

    print(f"\nInserting {len(to_insert)} new containment rows...")
    BATCH = 500
    inserted = 0
    for i in range(0, len(to_insert), BATCH):
        batch = to_insert[i:i + BATCH]
        sb.table("appellation_containment").insert(batch).execute()
        inserted += len(batch)
        print(f"  Inserted {inserted}/{len(to_insert)}")

    print(f"\nDone! Inserted {inserted} US AVA containment relationships.")


if __name__ == "__main__":
    main()
