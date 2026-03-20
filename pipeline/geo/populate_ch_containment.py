"""
Imports the Swiss wine AOC hierarchy into Loam:
  1. Soft-deletes invalid entries (Trois Lacs)
  2. Updates existing canton-level appellations with classification_level
  3. Creates missing canton-level AOCs
  4. Creates sub-cantonal AOCs (Vaud's 8 AOCs including 2 Grand Crus)
  5. Populates appellation_containment with the nesting hierarchy

Usage:
  python -m pipeline.geo.populate_ch_containment --dry-run
  python -m pipeline.geo.populate_ch_containment --apply
"""

import sys
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import geo_slugify

SOFT_DELETE = ["Trois Lacs"]

CANTON_AOCS = {
    "Valais":     {"region": "Valais",      "existsInDb": True},
    "Vaud":       {"region": "Vaud",        "existsInDb": True},
    "Gen\u00e8ve":{"region": "Geneva",      "existsInDb": True},
    "Ticino":     {"region": "Ticino",      "existsInDb": True},
    "Neuch\u00e2tel": {"region": "Switzerland", "existsInDb": True},
    "Fribourg":   {"region": "Switzerland"},
    "Bern":       {"region": "Switzerland"},
    "Z\u00fcrich":         {"region": "Switzerland"},
    "Schaffhausen":        {"region": "Switzerland"},
    "Aargau":              {"region": "Switzerland"},
    "Graub\u00fcnden":     {"region": "Switzerland"},
    "Thurgau":             {"region": "Switzerland"},
    "St. Gallen":          {"region": "Switzerland"},
    "Basel-Landschaft":    {"region": "Switzerland"},
    "Luzern":              {"region": "Switzerland"},
    "Schwyz":              {"region": "Switzerland"},
    "Zug":                 {"region": "Switzerland"},
    "Basel-Stadt":         {"region": "Switzerland"},
    "Glarus":              {"region": "Switzerland"},
    "Obwalden":            {"region": "Switzerland"},
    "Nidwalden":           {"region": "Switzerland"},
    "Appenzell Ausserrhoden": {"region": "Switzerland"},
    "Appenzell Innerrhoden":  {"region": "Switzerland"},
    "Uri":                 {"region": "Switzerland"},
}

VAUD_SUB_AOCS = [
    {"name": "La C\u00f4te",          "level": "aoc",       "parent": "Vaud"},
    {"name": "Lavaux",                "level": "aoc",       "parent": "Vaud"},
    {"name": "Chablais",              "level": "aoc",       "parent": "Vaud"},
    {"name": "Bonvillars",            "level": "aoc",       "parent": "Vaud"},
    {"name": "C\u00f4tes de l'Orbe",  "level": "aoc",       "parent": "Vaud"},
    {"name": "Vully",                 "level": "aoc",       "parent": "Vaud"},
    {"name": "Calamin",               "level": "grand_cru", "parent": "Lavaux"},
    {"name": "D\u00e9zaley",          "level": "grand_cru", "parent": "Lavaux"},
]

THREE_LAKES_SUB_AOCS = [
    {"name": "Cheyres",       "level": "aoc", "parent": "Fribourg"},
    {"name": "Lac de Bienne", "level": "aoc", "parent": "Bern"},
]


def main():
    parser = argparse.ArgumentParser(description="Switzerland AOC containment import")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.apply
    sb = get_supabase()
    print(f"\n=== Switzerland AOC Containment Import {'(DRY RUN)' if dry_run else ''} ===\n")

    # 1. Get Switzerland country ID
    ch = sb.table("countries").select("id").eq("iso_code", "CH").single().execute().data
    CH_ID = ch["id"]
    print(f"CH country ID: {CH_ID}")

    # 2. Load existing CH regions
    existing_regions = sb.table("regions").select("id, name").eq("country_id", CH_ID).execute().data
    region_by_name = {r["name"]: r["id"] for r in existing_regions}
    print(f"CH regions: {', '.join(r['name'] for r in existing_regions)}")

    # 3. Load existing CH appellations
    existing_apps = sb.table("appellations").select("id, name, slug, classification_level, deleted_at").eq("country_id", CH_ID).execute().data
    app_by_name = {a["name"]: a for a in existing_apps}
    print(f"Existing CH appellations: {len(existing_apps)}")

    # 4. Soft-delete invalid entries
    print("\n--- Soft-delete invalid entries ---")
    for name in SOFT_DELETE:
        app = app_by_name.get(name)
        if not app:
            print(f'  "{name}" not found in DB -- skipping')
            continue
        if app.get("deleted_at"):
            print(f'  "{name}" already soft-deleted')
            continue
        print(f'  Soft-deleting "{name}" (id: {app["id"]})')
        if not dry_run:
            sb.table("appellations").update({"deleted_at": datetime.now(timezone.utc).isoformat()}).eq("id", app["id"]).execute()

    # 5. Create/update canton-level AOCs
    print("\n--- Canton-level AOCs ---")
    canton_to_create = []
    canton_level_updates = []

    for name, info in CANTON_AOCS.items():
        region_id = region_by_name.get(info["region"])
        if not region_id:
            print(f'  Warning: region "{info["region"]}" not found for {name}')
            continue

        existing = app_by_name.get(name)
        if existing:
            if existing.get("classification_level") != "canton":
                canton_level_updates.append({"id": existing["id"], "name": name})
            if existing.get("deleted_at"):
                print(f'  "{name}" is soft-deleted, will restore')
                if not dry_run:
                    sb.table("appellations").update({"deleted_at": None}).eq("id", existing["id"]).execute()
        else:
            canton_to_create.append({
                "name": name,
                "slug": geo_slugify(f"{name}-switzerland"),
                "designation_type": "AOC",
                "classification_level": "canton",
                "country_id": CH_ID,
                "region_id": region_id,
                "hemisphere": "north",
            })

    print(f"Canton AOCs to create: {len(canton_to_create)}")
    if canton_to_create:
        print(f"  {', '.join(c['name'] for c in canton_to_create)}")
    print(f"Canton classification_level updates: {len(canton_level_updates)}")

    if not dry_run and canton_to_create:
        result = sb.table("appellations").insert(canton_to_create).select("id, name").execute()
        for c in result.data:
            app_by_name[c["name"]] = {"id": c["id"], "name": c["name"], "classification_level": "canton"}
        print(f"  Created {len(result.data)} canton appellations")
    elif dry_run and canton_to_create:
        for c in canton_to_create:
            app_by_name[c["name"]] = {"id": f"dry-{geo_slugify(c['name'])}", "name": c["name"], "classification_level": "canton"}

    if not dry_run and canton_level_updates:
        for u in canton_level_updates:
            sb.table("appellations").update({"classification_level": "canton"}).eq("id", u["id"]).execute()
        print(f"  Updated {len(canton_level_updates)} classification levels")

    for u in canton_level_updates:
        if app_by_name.get(u["name"]):
            app_by_name[u["name"]]["classification_level"] = "canton"

    # 6. Create sub-cantonal AOCs
    print("\n--- Sub-cantonal AOCs ---")
    all_sub_aocs = VAUD_SUB_AOCS + THREE_LAKES_SUB_AOCS
    sub_to_create = []

    for sub in all_sub_aocs:
        if app_by_name.get(sub["name"]):
            existing = app_by_name[sub["name"]]
            print(f'  "{sub["name"]}" already exists')
            if existing.get("classification_level") != sub["level"]:
                print(f'    Updating classification: {existing.get("classification_level")} -> {sub["level"]}')
                if not dry_run:
                    sb.table("appellations").update({"classification_level": sub["level"]}).eq("id", existing["id"]).execute()
            continue

        parent_canton = CANTON_AOCS.get(sub["parent"], {})
        region_name = parent_canton.get("region")
        if not region_name:
            grand_parent = next((s for s in all_sub_aocs if s["name"] == sub["parent"]), None)
            if grand_parent:
                region_name = CANTON_AOCS.get(grand_parent["parent"], {}).get("region")
        region_id = region_by_name.get(region_name or "Switzerland")

        sub_to_create.append({
            "name": sub["name"],
            "slug": geo_slugify(f"{sub['name']}-switzerland"),
            "designation_type": "AOC",
            "classification_level": sub["level"],
            "country_id": CH_ID,
            "region_id": region_id,
            "hemisphere": "north",
        })

    print(f"Sub-cantonal AOCs to create: {len(sub_to_create)}")
    if sub_to_create:
        print(f"  {', '.join(c['name'] for c in sub_to_create)}")

    if not dry_run and sub_to_create:
        result = sb.table("appellations").insert(sub_to_create).select("id, name").execute()
        for c in result.data:
            sub_info = next((s for s in all_sub_aocs if s["name"] == c["name"]), None)
            app_by_name[c["name"]] = {"id": c["id"], "name": c["name"], "classification_level": sub_info["level"] if sub_info else None}
        print(f"  Created {len(result.data)} sub-cantonal appellations")
    elif dry_run and sub_to_create:
        for c in sub_to_create:
            sub_info = next((s for s in all_sub_aocs if s["name"] == c["name"]), None)
            app_by_name[c["name"]] = {"id": f"dry-{geo_slugify(c['name'])}", "name": c["name"], "classification_level": sub_info["level"] if sub_info else None}

    # 7. Build containment relationships
    print("\n--- Containment relationships ---")
    containment_rows = []

    def add_containment(parent_name, child_name):
        parent = app_by_name.get(parent_name)
        child = app_by_name.get(child_name)
        if not parent:
            print(f"  Warning: parent not in DB: {parent_name}")
            return
        if not child:
            print(f"  Warning: child not in DB: {child_name}")
            return
        if parent.get("deleted_at") or child.get("deleted_at"):
            return
        containment_rows.append({
            "parent_id": parent["id"],
            "child_id": child["id"],
            "source": "explicit",
            "_parentName": parent_name,
            "_childName": child_name,
        })

    for sub in all_sub_aocs:
        add_containment(sub["parent"], sub["name"])

    # Vully shared between Vaud and Fribourg
    add_containment("Fribourg", "Vully")

    # Deduplicate
    seen = set()
    unique_rows = []
    for r in containment_rows:
        key = f"{r['parent_id']}|{r['child_id']}"
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    print(f"\nContainment relationships: {len(unique_rows)}")

    canton_to_sub = [r for r in unique_rows if app_by_name.get(r["_parentName"], {}).get("classification_level") == "canton"]
    print(f"\n  Canton -> sub-cantonal AOC:")
    for r in canton_to_sub:
        print(f"    {r['_parentName']} -> {r['_childName']}")

    gc_rows = [r for r in unique_rows if app_by_name.get(r["_childName"], {}).get("classification_level") == "grand_cru"]
    print(f"\n  Sub-AOC -> Grand Cru:")
    for r in gc_rows:
        print(f"    {r['_parentName']} -> {r['_childName']}")

    if dry_run:
        print(f"\n=== DRY RUN SUMMARY ===")
        print(f"Soft-deletes: {len([n for n in SOFT_DELETE if app_by_name.get(n) and not app_by_name[n].get('deleted_at')])}")
        print(f"Canton AOCs to create: {len(canton_to_create)}")
        print(f"Canton classification_level updates: {len(canton_level_updates)}")
        print(f"Sub-cantonal AOCs to create: {len(sub_to_create)}")
        print(f"Containment rows to insert: {len(unique_rows)}")
        print(f"\nTotal new appellations: {len(canton_to_create) + len(sub_to_create)}")
        print("\n[DRY RUN] No changes made.")
        return

    # 8. Insert containment rows
    existing = sb.table("appellation_containment").select("parent_id, child_id").execute().data
    existing_set = {f"{r['parent_id']}|{r['child_id']}" for r in (existing or [])}
    to_insert = [
        {"parent_id": r["parent_id"], "child_id": r["child_id"], "source": r["source"]}
        for r in unique_rows
        if f"{r['parent_id']}|{r['child_id']}" not in existing_set
    ]

    if not to_insert:
        print("\nAll containment relationships already exist. Nothing to insert.")
    else:
        print(f"\nInserting {len(to_insert)} new containment rows...")
        sb.table("appellation_containment").insert(to_insert).execute()
        print(f"  Inserted {len(to_insert)} containment rows")

    print("\nDone!")


if __name__ == "__main__":
    main()
