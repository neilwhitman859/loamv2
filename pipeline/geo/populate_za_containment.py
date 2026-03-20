#!/usr/bin/env python3
"""
South Africa Wine of Origin (WO) containment hierarchy importer.

Creates missing GU/Region/District/Ward appellations, sets classification_level,
and populates appellation_containment with the full SAWIS nesting.

Hierarchy: Geographical Unit (GU) → Region → District → Ward

Usage:
    python -m pipeline.geo.populate_za_containment              # full run
    python -m pipeline.geo.populate_za_containment --dry-run    # preview only
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.normalize import slugify

# ---------------------------------------------------------------------------
# Complete SAWIS Wine of Origin Hierarchy
# ---------------------------------------------------------------------------

HIERARCHY = {
    "Western Cape": {
        "regions": {
            "Breede River Valley": {
                "districts": {
                    "Breedekloof": ["Goudini", "Slanghoek"],
                    "Robertson": [
                        "Agterkliphoogte", "Ashton", "Boesmansrivier", "Bonnievale",
                        "Eilandia", "Goedemoed", "Goree", "Goudmyn", "Hoopsrivier",
                        "Klaasvoogds", "Le Chasseur", "McGregor", "Vinkrivier", "Zandrivier",
                    ],
                    "Worcester": [
                        "Hex River Valley", "Keeromsberg", "Moordkuil", "Nuy",
                        "Rooikrans", "Scherpenheuwel", "Stettyn",
                    ],
                },
                "standaloneWards": [],
            },
            "Cape South Coast": {
                "districts": {
                    "Cape Agulhas": ["Elim"],
                    "Elgin": [],
                    "Lower Duivenhoks River": ["Napier"],
                    "Overberg": ["Elandskloof", "Greyton", "Klein River", "Theewater"],
                    "Plettenberg Bay": ["Still Bay East"],
                    "Swellendam": ["Buffeljags", "Malgas", "Stormsvlei"],
                    "Walker Bay": [
                        "Bot River", "Hemel-en-Aarde Valley", "Upper Hemel-en-Aarde Valley",
                        "Hemel-en-Aarde Ridge", "Sunday's Glen", "Springfontein Rim",
                        "Stanford Foothills",
                    ],
                },
                "standaloneWards": ["Herbertsdale"],
            },
            "Coastal Region": {
                "districts": {
                    "Cape Town": ["Constantia", "Durbanville", "Hout Bay", "Philadelphia"],
                    "Darling": ["Groenekloof"],
                    "Franschhoek": [],
                    "Lutzville Valley": ["Koekenaap"],
                    "Paarl": ["Agter-Paarl", "Simonsberg-Paarl", "Voor-Paardeberg"],
                    "Stellenbosch": [
                        "Banghoek", "Bottelary", "Devon Valley", "Jonkershoek Valley",
                        "Papegaaiberg", "Polkadraai Hills", "Simonsberg-Stellenbosch",
                        "Vlottenburg",
                    ],
                    "Swartland": [
                        "Malmesbury", "Paardeberg", "Paardeberg South", "Piket-Bo-Berg",
                        "Porseleinberg", "Riebeekberg", "Riebeeksrivier",
                    ],
                    "Tulbagh": [],
                    "Wellington": ["Blouvlei", "Bovlei", "Groenberg", "Limietberg", "Mid-Berg River"],
                },
                "standaloneWards": ["Bamboes Bay", "Lamberts Bay", "St Helena Bay"],
            },
            "Klein Karoo": {
                "districts": {
                    "Calitzdorp": ["Groenfontein"],
                    "Langeberg-Garcia": ["Montagu", "Outeniqua", "Tradouw", "Tradouw Highlands", "Upper Langkloof"],
                },
                "standaloneWards": ["Cango Valley", "Koo Plateau"],
            },
            "Olifants River": {
                "districts": {
                    "Citrusdal Mountain": ["Piekenierskloof"],
                    "Citrusdal Valley": ["Spruitdrift", "Vredendal"],
                },
                "standaloneWards": [],
            },
        },
        "standaloneDistricts": {
            "Ceres Plateau": ["Ceres"],
            "Prince Albert": ["Kweekvallei", "Prince Albert Valley", "Swartberg"],
        },
        "standaloneWards": ["Cederberg", "Leipoldtville-Sandveld", "Nieuwoudtville"],
    },
    "Northern Cape": {
        "regions": {
            "Karoo-Hoogland": {
                "districts": {
                    "Sutherland-Karoo": [],
                    "Central Orange River": ["Groblershoop", "Grootdrink", "Kakamas", "Keimoes", "Upington"],
                },
                "standaloneWards": [],
            },
        },
        "standaloneDistricts": {
            "Douglas": [],
        },
        "standaloneWards": ["Hartswater", "Prieska"],
    },
    "Eastern Cape": {
        "regions": {},
        "standaloneDistricts": {},
        "standaloneWards": ["St Francis Bay"],
    },
    "KwaZulu-Natal": {
        "regions": {},
        "standaloneDistricts": {
            "Central Drakensberg": [],
            "Lions River": [],
        },
        "standaloneWards": [],
    },
    "Free State": {
        "regions": {},
        "standaloneDistricts": {},
        "standaloneWards": ["Rietrivier FS"],
    },
}

NO_GU_WARDS = ["Lanseria"]


def main():
    parser = argparse.ArgumentParser(description="Populate SA WO containment hierarchy")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    sb = get_supabase()
    dry_run = args.dry_run

    print(f"\n=== South Africa WO Containment Import {'(DRY RUN)' if dry_run else ''} ===\n")

    # 1. Get ZA country ID and catch-all region
    za_country = sb.table("countries").select("id").eq("iso_code", "ZA").single().execute()
    za_country_id = za_country.data["id"]
    print(f"ZA country ID: {za_country_id}")

    za_regions = sb.table("regions").select("id,name").eq("country_id", za_country_id).execute()
    catch_all = next((r for r in za_regions.data if r["name"] == "South Africa"), None)
    za_region_id = catch_all["id"] if catch_all else None
    print(f"Catch-all region ID: {za_region_id}")

    # 2. Load existing ZA appellations (paginate)
    existing_apps = []
    offset = 0
    while True:
        result = (sb.table("appellations")
                  .select("id,name,slug,classification_level")
                  .eq("country_id", za_country_id)
                  .range(offset, offset + 999)
                  .execute())
        existing_apps.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    print(f"Existing ZA appellations: {len(existing_apps)}")
    app_by_name = {a["name"]: a for a in existing_apps}

    # 3. Collect all entries with classification levels
    all_entries = []

    for gu_name in HIERARCHY:
        all_entries.append({"name": gu_name, "level": "geographical_unit"})

    for gu_data in HIERARCHY.values():
        for region_name, region_data in (gu_data.get("regions") or {}).items():
            all_entries.append({"name": region_name, "level": "region"})
            for district_name, wards in (region_data.get("districts") or {}).items():
                all_entries.append({"name": district_name, "level": "district"})
                for ward in wards:
                    all_entries.append({"name": ward, "level": "ward"})
            for ward in (region_data.get("standaloneWards") or []):
                all_entries.append({"name": ward, "level": "ward"})

        for district_name, wards in (gu_data.get("standaloneDistricts") or {}).items():
            all_entries.append({"name": district_name, "level": "district"})
            for ward in wards:
                all_entries.append({"name": ward, "level": "ward"})

        for ward in (gu_data.get("standaloneWards") or []):
            all_entries.append({"name": ward, "level": "ward"})

    for ward in NO_GU_WARDS:
        all_entries.append({"name": ward, "level": "ward"})

    entry_by_name = {e["name"]: e for e in all_entries}

    gus = [e for e in all_entries if e["level"] == "geographical_unit"]
    regions = [e for e in all_entries if e["level"] == "region"]
    districts = {e["name"] for e in all_entries if e["level"] == "district"}
    wards = {e["name"] for e in all_entries if e["level"] == "ward"}

    print(f"Total entries in hierarchy: {len(entry_by_name)}")
    print(f"  GUs: {len(gus)}")
    print(f"  Regions: {len(regions)}")
    print(f"  Districts: {len(districts)}")
    print(f"  Wards: {len(wards)}")

    # 4. Identify entries to create
    to_create = []
    for name, entry in entry_by_name.items():
        if name not in app_by_name:
            to_create.append({
                "name": name,
                "slug": slugify(f"{name} south africa"),
                "designation_type": "WO",
                "classification_level": entry["level"],
                "country_id": za_country_id,
                "region_id": za_region_id,
                "hemisphere": "south",
            })

    print(f"\nAppellations to create: {len(to_create)}")
    if to_create:
        by_level: dict[str, list[str]] = {}
        for c in to_create:
            by_level.setdefault(c["classification_level"], []).append(c["name"])
        for level, names in sorted(by_level.items()):
            print(f"  {level} ({len(names)}): {', '.join(sorted(names))}")

    # 5. Create missing appellations
    if not dry_run and to_create:
        for i in range(0, len(to_create), 500):
            batch = to_create[i:i + 500]
            result = sb.table("appellations").insert(batch).select("id,name").execute()
            for c in result.data:
                app_by_name[c["name"]] = c
            print(f"  Created batch {i // 500 + 1}: {len(result.data)} appellations")
    elif dry_run and to_create:
        for c in to_create:
            app_by_name[c["name"]] = {"id": f"dry-run-{slugify(c['name'])}", "name": c["name"]}

    # 6. Set classification_level on all entries
    level_updates = []
    for name, entry in entry_by_name.items():
        app = app_by_name.get(name)
        if not app:
            print(f"  Warning: {name} not found in DB")
            continue
        if app.get("classification_level") != entry["level"]:
            level_updates.append({"id": app["id"], "name": name, "level": entry["level"]})

    print(f"\nClassification level updates needed: {len(level_updates)}")
    if not dry_run and level_updates:
        for u in level_updates:
            sb.table("appellations").update({"classification_level": u["level"]}).eq("id", u["id"]).execute()
        print(f"  Updated {len(level_updates)} classification levels")

    # 7. Build containment relationships
    containment_rows = []

    def add_containment(parent_name: str, child_name: str):
        parent = app_by_name.get(parent_name)
        child = app_by_name.get(child_name)
        if not parent:
            print(f"  Warning: parent not in DB: {parent_name}")
            return
        if not child:
            print(f"  Warning: child not in DB: {child_name}")
            return
        containment_rows.append({
            "parent_id": parent["id"],
            "child_id": child["id"],
            "source": "explicit",
            "_parent": parent_name,
            "_child": child_name,
        })

    for gu_name, gu_data in HIERARCHY.items():
        for region_name, region_data in (gu_data.get("regions") or {}).items():
            add_containment(gu_name, region_name)
            for district_name, wards_list in (region_data.get("districts") or {}).items():
                add_containment(region_name, district_name)
                for ward in wards_list:
                    add_containment(district_name, ward)
            for ward in (region_data.get("standaloneWards") or []):
                add_containment(region_name, ward)

        for district_name, wards_list in (gu_data.get("standaloneDistricts") or {}).items():
            add_containment(gu_name, district_name)
            for ward in wards_list:
                add_containment(district_name, ward)

        for ward in (gu_data.get("standaloneWards") or []):
            add_containment(gu_name, ward)

    # Deduplicate
    seen = set()
    unique_rows = []
    for r in containment_rows:
        key = f"{r['parent_id']}|{r['child_id']}"
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    print(f"\nContainment relationships: {len(unique_rows)}")

    # Summary by type
    def count_type(p_level, c_level):
        return sum(1 for r in unique_rows
                   if entry_by_name.get(r["_parent"], {}).get("level") == p_level
                   and entry_by_name.get(r["_child"], {}).get("level") == c_level)

    print(f"  GU → Region: {count_type('geographical_unit', 'region')}")
    print(f"  GU → District (standalone): {count_type('geographical_unit', 'district')}")
    print(f"  GU → Ward (standalone): {count_type('geographical_unit', 'ward')}")
    print(f"  Region → District: {count_type('region', 'district')}")
    print(f"  Region → Ward (standalone): {count_type('region', 'ward')}")
    print(f"  District → Ward: {count_type('district', 'ward')}")

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    # 8. Insert containment rows
    existing_containment = sb.table("appellation_containment").select("parent_id,child_id").execute()
    existing_set = {f"{r['parent_id']}|{r['child_id']}" for r in (existing_containment.data or [])}

    to_insert = [
        {"parent_id": r["parent_id"], "child_id": r["child_id"], "source": r["source"]}
        for r in unique_rows
        if f"{r['parent_id']}|{r['child_id']}" not in existing_set
    ]

    if not to_insert:
        print("\nAll relationships already exist in DB. Nothing to insert.")
        return

    print(f"\nInserting {len(to_insert)} new containment rows...")
    inserted = batch_insert("appellation_containment", to_insert, batch_size=500)
    print(f"\nDone! Inserted {inserted} SA WO containment relationships.")


if __name__ == "__main__":
    main()
