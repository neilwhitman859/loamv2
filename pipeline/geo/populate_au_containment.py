"""
Imports Australian wine GI hierarchy into Loam:
  1. Creates zone-level appellations (currently missing from DB)
  2. Imports zone boundary polygons from wine_australia_zones.geojson
  3. Sets classification_level (zone/region/subregion) on all AU GIs
  4. Populates appellation_containment with Zone->Region->Subregion nesting

Source: Winetitles/Wine Australia GI Register hierarchy + Open Data Hub

Usage:
  python -m pipeline.geo.populate_au_containment --dry-run
  python -m pipeline.geo.populate_au_containment --apply
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import geo_slugify, compute_centroid

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Complete Australian GI Hierarchy
HIERARCHY = {
    "Barossa": {"state": "SA", "regions": {"Barossa Valley": [], "Eden Valley": ["High Eden"]}},
    "Far North": {"state": "SA", "regions": {"Southern Flinders Ranges": []}},
    "Fleurieu": {"state": "SA", "regions": {"Currency Creek": [], "Kangaroo Island": [], "Langhorne Creek": [], "McLaren Vale": [], "Southern Fleurieu": []}},
    "Limestone Coast": {"state": "SA", "regions": {"Coonawarra": [], "Mount Benson": [], "Mount Gambier": [], "Padthaway": [], "Robe": [], "Wrattonbully": []}},
    "Lower Murray": {"state": "SA", "regions": {"Riverland": []}},
    "Mount Lofty Ranges": {"state": "SA", "regions": {"Adelaide Hills": ["Lenswood", "Piccadilly Valley"], "Adelaide Plains": [], "Clare Valley": []}},
    "The Peninsulas": {"state": "SA", "regions": {}},
    "Big Rivers": {"state": "NSW", "regions": {"Murray Darling": [], "Perricoota": [], "Riverina": [], "Swan Hill": []}},
    "Central Ranges": {"state": "NSW", "regions": {"Cowra": [], "Mudgee": [], "Orange": []}},
    "Hunter Valley": {"state": "NSW", "regions": {"Hunter": ["Broke Fordwich", "Pokolbin", "Upper Hunter Valley"]}},
    "Northern Rivers": {"state": "NSW", "regions": {"Hastings River": []}},
    "Northern Slopes": {"state": "NSW", "regions": {"New England Australia": []}},
    "South Coast": {"state": "NSW", "regions": {"Shoalhaven Coast": [], "Southern Highlands": []}},
    "Southern New South Wales": {"state": "NSW", "regions": {"Canberra District": [], "Gundagai": [], "Hilltops": [], "Tumbarumba": []}},
    "Western Plains": {"state": "NSW", "regions": {}},
    "Central Victoria": {"state": "VIC", "regions": {"Bendigo": [], "Goulburn Valley": ["Nagambie Lakes"], "Heathcote": [], "Strathbogie Ranges": [], "Upper Goulburn": []}},
    "Gippsland": {"state": "VIC", "regions": {}},
    "North East Victoria": {"state": "VIC", "regions": {"Alpine Valleys": [], "Beechworth": [], "Glenrowan": [], "King Valley": [], "Rutherglen": []}},
    "North West Victoria": {"state": "VIC", "regions": {"Murray Darling": [], "Swan Hill": []}},
    "Port Phillip": {"state": "VIC", "regions": {"Geelong": [], "Macedon Ranges": [], "Mornington Peninsula": [], "Sunbury": [], "Yarra Valley": []}},
    "Western Victoria": {"state": "VIC", "regions": {"Grampians": ["Great Western"], "Henty": [], "Pyrenees": []}},
    "Greater Perth": {"state": "WA", "regions": {"Peel": [], "Perth Hills": [], "Swan District": ["Swan Valley"]}},
    "South West Australia": {"state": "WA", "regions": {"Blackwood Valley": [], "Geographe": [], "Great Southern": ["Albany", "Denmark", "Frankland River", "Mount Barker", "Porongurup"], "Manjimup": [], "Margaret River": [], "Pemberton": []}},
    "Central Western Australia": {"state": "WA", "regions": {}},
    "Eastern Plains, Inland And North Of Western Australia": {"state": "WA", "regions": {}},
    "West Australian South East Coastal": {"state": "WA", "regions": {}},
    "Queensland": {"state": "QLD", "regions": {"Granite Belt": [], "South Burnett": []}},
    "Tasmania": {"state": "TAS", "regions": {}},
}

SUPER_ZONES = {
    "Adelaide": ["Barossa", "Fleurieu", "Mount Lofty Ranges"],
}

MULTI_ZONE_REGIONS = {
    "Murray Darling": ["Big Rivers", "North West Victoria"],
    "Swan Hill": ["Big Rivers", "North West Victoria"],
}


def main():
    parser = argparse.ArgumentParser(description="Australia GI containment import")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.apply
    sb = get_supabase()
    print(f"\n=== Australia GI Containment Import {'(DRY RUN)' if dry_run else ''} ===\n")

    # 1. Get Australia country ID
    au = sb.table("countries").select("id").eq("iso_code", "AU").single().execute().data
    AU_ID = au["id"]

    # 2. Load existing AU appellations
    existing_apps = sb.table("appellations").select("id, name, classification_level").eq("country_id", AU_ID).execute().data
    app_by_name = {a["name"]: a for a in existing_apps}
    print(f"Existing AU appellations: {len(existing_apps)}")

    # 3. Load existing AU regions
    existing_regions = sb.table("regions").select("id, name").eq("country_id", AU_ID).execute().data
    region_by_name = {r["name"]: r["id"] for r in existing_regions}

    STATE_REGIONS = {
        "SA": region_by_name.get("South Australia"),
        "NSW": region_by_name.get("New South Wales"),
        "VIC": region_by_name.get("Victoria"),
        "WA": region_by_name.get("Western Australia"),
        "TAS": region_by_name.get("Tasmania"),
        "QLD": region_by_name.get("Australia"),
    }

    # 4. Load zone GeoJSON
    geo_path = PROJECT_ROOT / "data" / "geo" / "wine_australia_zones.geojson"
    zone_boundaries = {}
    if geo_path.exists():
        zones_geo = json.loads(geo_path.read_text(encoding="utf-8"))
        for f in zones_geo["features"]:
            zone_boundaries[f["properties"]["GI_NAME"]] = f["geometry"]

    # 5. Create zone appellations
    all_zone_names = list(HIERARCHY.keys()) + list(SUPER_ZONES.keys())
    zones_to_create = []
    for zone_name in all_zone_names:
        if app_by_name.get(zone_name):
            print(f"  Zone already exists: {zone_name}")
            continue
        zone_info = HIERARCHY.get(zone_name, {"state": "SA"})
        region_id = STATE_REGIONS.get(zone_info["state"]) or region_by_name.get("Australia")

        lat, lng = None, None
        geom = zone_boundaries.get(zone_name)
        if geom:
            centroid = compute_centroid(geom)
            if centroid:
                lat = round(centroid["lat"], 6)
                lng = round(centroid["lng"], 6)

        zones_to_create.append({
            "name": zone_name,
            "slug": geo_slugify(f"{zone_name} australia"),
            "designation_type": "GI",
            "classification_level": "zone",
            "country_id": AU_ID,
            "region_id": region_id,
            "hemisphere": "south",
            "latitude": lat,
            "longitude": lng,
        })

    # Also create Adelaide super-zone if not exists
    if "Adelaide" not in app_by_name and not any(z["name"] == "Adelaide" for z in zones_to_create):
        zones_to_create.append({
            "name": "Adelaide",
            "slug": "adelaide-australia",
            "designation_type": "GI",
            "classification_level": "zone",
            "country_id": AU_ID,
            "region_id": STATE_REGIONS["SA"],
            "hemisphere": "south",
            "latitude": -34.9285,
            "longitude": 138.6007,
        })

    print(f"Zones to create: {len(zones_to_create)}")
    if zones_to_create:
        print(f"  {', '.join(z['name'] for z in zones_to_create)}")

    if not dry_run and zones_to_create:
        result = sb.table("appellations").insert(zones_to_create).select("id, name").execute()
        for c in result.data:
            app_by_name[c["name"]] = {"id": c["id"], "name": c["name"], "classification_level": "zone"}
        print(f"  Created {len(result.data)} zone appellations")

    # 6. Import zone boundaries
    if not dry_run:
        boundary_count = 0
        for zone_name in all_zone_names:
            app = app_by_name.get(zone_name)
            if not app:
                continue
            geom = zone_boundaries.get(zone_name)
            if not geom:
                continue
            try:
                sb.rpc("upsert_appellation_boundary", {
                    "p_appellation_id": app["id"],
                    "p_geojson": json.dumps(geom),
                    "p_source_id": f"wine-australia-zone/{geo_slugify(zone_name)}",
                    "p_confidence": "official",
                }).execute()
                boundary_count += 1
            except Exception as e:
                print(f"  Warning: boundary import failed for {zone_name}: {e}")
        print(f"Imported {boundary_count} zone boundaries")

    # 7. Set classification_level
    subregion_names = set()
    region_names = set()
    for zone_data in HIERARCHY.values():
        for reg_name, subs in zone_data["regions"].items():
            region_names.add(reg_name)
            subregion_names.update(subs)

    level_updates = []
    for name, app in app_by_name.items():
        level = None
        if name in HIERARCHY or name in SUPER_ZONES:
            level = "zone"
        elif name in subregion_names:
            level = "subregion"
        elif name in region_names:
            level = "region"
        if level and app.get("classification_level") != level:
            level_updates.append({"id": app["id"], "name": name, "level": level})

    print(f"\nClassification level updates: {len(level_updates)}")
    if not dry_run and level_updates:
        for u in level_updates:
            sb.table("appellations").update({"classification_level": u["level"]}).eq("id", u["id"]).execute()
        print(f"  Updated {len(level_updates)} classification levels")

    # 8. Build containment relationships
    containment_rows = []

    for zone_name, zone_data in HIERARCHY.items():
        zone_app = app_by_name.get(zone_name)
        if not zone_app:
            print(f"  Warning: zone not in DB: {zone_name}")
            continue

        for reg_name, subs in zone_data["regions"].items():
            # Skip multi-zone regions in secondary zone
            if reg_name in MULTI_ZONE_REGIONS and MULTI_ZONE_REGIONS[reg_name][0] != zone_name:
                continue

            reg_app = app_by_name.get(reg_name)
            if not reg_app:
                print(f"  Warning: region not in DB: {reg_name}")
                continue

            containment_rows.append({"parent_id": zone_app["id"], "child_id": reg_app["id"], "source": "explicit"})

            for sub_name in subs:
                sub_app = app_by_name.get(sub_name)
                if not sub_app:
                    print(f"  Warning: subregion not in DB: {sub_name}")
                    continue
                containment_rows.append({"parent_id": reg_app["id"], "child_id": sub_app["id"], "source": "explicit"})

    # Multi-zone regions
    for reg_name, zones in MULTI_ZONE_REGIONS.items():
        reg_app = app_by_name.get(reg_name)
        if not reg_app:
            continue
        for zone_name in zones:
            zone_app = app_by_name.get(zone_name)
            if not zone_app:
                continue
            if not any(r["parent_id"] == zone_app["id"] and r["child_id"] == reg_app["id"] for r in containment_rows):
                containment_rows.append({"parent_id": zone_app["id"], "child_id": reg_app["id"], "source": "explicit"})

    # Super-zone containment
    for super_zone, child_zones in SUPER_ZONES.items():
        super_app = app_by_name.get(super_zone)
        if not super_app:
            continue
        for child_zone in child_zones:
            child_app = app_by_name.get(child_zone)
            if not child_app:
                continue
            containment_rows.append({"parent_id": super_app["id"], "child_id": child_app["id"], "source": "explicit"})

    print(f"\nContainment relationships to insert: {len(containment_rows)}")

    if not dry_run and containment_rows:
        existing = sb.table("appellation_containment").select("parent_id, child_id").execute().data
        existing_set = {f"{r['parent_id']}|{r['child_id']}" for r in (existing or [])}
        to_insert = [r for r in containment_rows if f"{r['parent_id']}|{r['child_id']}" not in existing_set]

        if to_insert:
            sb.table("appellation_containment").insert(to_insert).execute()
            print(f"  Inserted {len(to_insert)} containment rows")
        else:
            print("  All relationships already exist")

    print("\nDone!")


if __name__ == "__main__":
    main()
