"""
Imports the German wine hierarchy from the RLP Weinbergsrolle API.

4-level hierarchy:
  Anbaugebiet (region) -> Bereich (subregion) -> Grosslage (cluster) -> Einzellage (vineyard)

Creates Bereich, Grosslage, Einzellage appellations and populates containment.

Usage:
  python -m pipeline.geo.populate_de_containment --dry-run
  python -m pipeline.geo.populate_de_containment --apply
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import geo_slugify

PROJECT_ROOT = Path(__file__).resolve().parents[2]
API_BASE = "https://demo.ldproxy.net/vineyards/collections/vineyards/items"
PAGE_SIZE = 200
CACHE_FILE = PROJECT_ROOT / "data" / "de_vineyards_cache.json"

OTHER_ANBAUGEBIETE = [
    "Baden", "Württemberg", "Franken", "Rheingau",
    "Hessische Bergstraße", "Saale-Unstrut", "Sachsen",
]


def normalize_subregion(raw: str) -> str:
    if raw == "Ber. Mittelhaardt/Dt.":
        return "Ber. Mittelhaardt/Dt. Weinstraße"
    return raw


def clean_bereich_name(raw: str) -> str:
    name = raw
    if name.startswith("Bereich "):
        name = name[len("Bereich "):]
    elif name.startswith("Ber. "):
        name = name[len("Ber. "):]
    name = name.replace("Mittelhaardt/Dt. Weinstraße", "Mittelhaardt-Deutsche Weinstraße")
    name = name.replace("Südl. Weinstraße", "Südliche Weinstraße")
    return name


def fetch_all_vineyards() -> list:
    """Fetch all vineyards from ldproxy API, with caching."""
    if CACHE_FILE.exists():
        cached = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        print(f"Loaded {len(cached)} vineyards from cache")
        return cached

    print("Fetching vineyards from RLP Weinbergsrolle API...")
    client = httpx.Client(timeout=60)
    all_features = []
    offset = 0

    while True:
        print(f"  Fetching offset {offset}...")
        resp = client.get(API_BASE, params={"limit": str(PAGE_SIZE), "offset": str(offset), "f": "json"})
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        all_features.extend(features)
        if len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.5)

    client.close()
    print(f"Fetched {len(all_features)} vineyards total")

    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(all_features, ensure_ascii=False), encoding="utf-8")
    print(f"Cached to {CACHE_FILE}")
    return all_features


def build_hierarchy(features):
    """Build unique hierarchy levels from vineyard features."""
    regions = {}
    bereiche = {}
    grosslagen = {}
    einzellagen = {}

    for f in features:
        props = f.get("properties", {})
        name = props.get("name")
        region = props.get("region")
        subregion = normalize_subregion(props.get("subregion", ""))
        cluster = props.get("cluster")
        village = props.get("village")
        has_grosslage = cluster and cluster != "--"

        if region not in regions:
            regions[region] = {"bereiche": set()}
        regions[region]["bereiche"].add(subregion)

        if subregion not in bereiche:
            bereiche[subregion] = {"region": region, "grosslagen": set()}
        if has_grosslage:
            bereiche[subregion]["grosslagen"].add(cluster)

        if has_grosslage:
            if cluster not in grosslagen:
                grosslagen[cluster] = {"region": region, "bereich": subregion, "einzellagen": set()}
            grosslagen[cluster]["einzellagen"].add(name)

        key = f"{region}|{name}"
        if key not in einzellagen:
            einzellagen[key] = {
                "name": name,
                "region": region,
                "bereich": subregion,
                "grosslage": cluster if has_grosslage else None,
                "village": village,
            }

    return regions, bereiche, grosslagen, einzellagen


def main():
    parser = argparse.ArgumentParser(description="Germany vineyard hierarchy import")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.apply
    sb = get_supabase()
    print(f"\n=== Germany Vineyard Hierarchy Import {'(DRY RUN)' if dry_run else ''} ===\n")

    # 1. Fetch vineyard data
    features = fetch_all_vineyards()

    # 2. Build hierarchy
    regions, bereiche, grosslagen, einzellagen = build_hierarchy(features)

    print(f"\nHierarchy summary:")
    print(f"  Regions (Anbaugebiete): {len(regions)}")
    print(f"  Bereiche (subregions): {len(bereiche)}")
    print(f"  Grosslagen (clusters): {len(grosslagen)}")
    print(f"  Einzellagen (vineyards): {len(einzellagen)}")

    print(f"\nHierarchy tree:")
    for region_name, r_data in regions.items():
        print(f"  {region_name} ({len(r_data['bereiche'])} Bereiche)")
        for b in r_data["bereiche"]:
            b_data = bereiche[b]
            print(f"    {clean_bereich_name(b)} ({len(b_data['grosslagen'])} Grosslagen)")

    # 3. Load DB context
    de = sb.table("countries").select("id").eq("iso_code", "DE").single().execute().data
    DE_ID = de["id"]

    existing_apps = sb.table("appellations").select("id, name, slug, classification_level, designation_type").eq("country_id", DE_ID).execute().data
    app_by_name = {a["name"]: a for a in existing_apps}
    print(f"\nExisting German appellations: {len(existing_apps)}")

    de_regions = sb.table("regions").select("id, name").eq("country_id", DE_ID).execute().data
    region_by_name = {r["name"]: r["id"] for r in de_regions}
    default_region_id = region_by_name.get("Germany") or region_by_name.get("Deutschland") or (de_regions[0]["id"] if de_regions else None)

    # 4. Match Anbaugebiete
    print("\n--- Matching Anbaugebiete ---")
    anbaugebiete_map = {}
    for region_name in regions:
        existing = app_by_name.get(region_name)
        if existing:
            anbaugebiete_map[region_name] = existing
            if existing.get("classification_level") != "anbaugebiet":
                print(f"  {region_name}: updating classification_level to 'anbaugebiet'")
                if not dry_run:
                    sb.table("appellations").update({"classification_level": "anbaugebiet"}).eq("id", existing["id"]).execute()
                existing["classification_level"] = "anbaugebiet"
            else:
                print(f"  {region_name}: matched ({existing['id']})")
        else:
            print(f"  {region_name}: NOT FOUND IN DB")

    for name in OTHER_ANBAUGEBIETE:
        existing = app_by_name.get(name)
        if existing and existing.get("classification_level") != "anbaugebiet":
            print(f"  {name}: updating classification_level to 'anbaugebiet'")
            if not dry_run:
                sb.table("appellations").update({"classification_level": "anbaugebiet"}).eq("id", existing["id"]).execute()

    # 5. Create Bereiche
    print("\n--- Creating Bereiche ---")
    bereiche_to_create = []
    bereich_map = {}

    for raw_name, b_data in bereiche.items():
        clean_name = clean_bereich_name(raw_name)
        display_name = f"{clean_name} (Bereich)"

        if app_by_name.get(display_name) or app_by_name.get(clean_name):
            existing = app_by_name.get(display_name) or app_by_name.get(clean_name)
            bereich_map[raw_name] = existing["id"]
            print(f"  {display_name}: already exists")
            continue

        bereiche_to_create.append({
            "name": display_name,
            "slug": geo_slugify(f"{clean_name}-bereich-germany"),
            "designation_type": "Qualitätswein",
            "classification_level": "bereich",
            "country_id": DE_ID,
            "region_id": default_region_id,
            "hemisphere": "north",
            "_raw": raw_name,
        })

    print(f"Bereiche to create: {len(bereiche_to_create)}")

    if not dry_run and bereiche_to_create:
        insert_data = [{k: v for k, v in b.items() if k != "_raw"} for b in bereiche_to_create]
        result = sb.table("appellations").insert(insert_data).select("id, name").execute()
        for c in result.data:
            app_by_name[c["name"]] = {"id": c["id"], "name": c["name"], "classification_level": "bereich"}
            entry = next((b for b in bereiche_to_create if b["name"] == c["name"]), None)
            if entry:
                bereich_map[entry["_raw"]] = c["id"]
        print(f"  Created {len(result.data)} Bereiche")
    elif dry_run:
        for b in bereiche_to_create:
            fake_id = f"dry-bereich-{geo_slugify(b['name'])}"
            app_by_name[b["name"]] = {"id": fake_id, "name": b["name"], "classification_level": "bereich"}
            bereich_map[b["_raw"]] = fake_id

    # 6. Create Grosslagen
    print("\n--- Creating Grosslagen ---")
    grosslagen_to_create = []
    grosslage_map = {}

    for name, g_data in grosslagen.items():
        display_name = f"{name} (Grosslage)"
        if app_by_name.get(display_name) or app_by_name.get(name):
            existing = app_by_name.get(display_name) or app_by_name.get(name)
            grosslage_map[name] = existing["id"]
            continue

        grosslagen_to_create.append({
            "name": display_name,
            "slug": geo_slugify(f"{name}-grosslage-germany"),
            "designation_type": "Qualitätswein",
            "classification_level": "grosslage",
            "country_id": DE_ID,
            "region_id": default_region_id,
            "hemisphere": "north",
            "_raw": name,
        })

    print(f"Grosslagen to create: {len(grosslagen_to_create)}")

    if not dry_run and grosslagen_to_create:
        BATCH = 200
        total_created = 0
        for i in range(0, len(grosslagen_to_create), BATCH):
            batch = [{k: v for k, v in g.items() if k != "_raw"} for g in grosslagen_to_create[i:i + BATCH]]
            result = sb.table("appellations").insert(batch).select("id, name").execute()
            for c in result.data:
                app_by_name[c["name"]] = {"id": c["id"], "name": c["name"], "classification_level": "grosslage"}
                entry = next((g for g in grosslagen_to_create if g["name"] == c["name"]), None)
                if entry:
                    grosslage_map[entry["_raw"]] = c["id"]
            total_created += len(result.data)
        print(f"  Created {total_created} Grosslagen")
    elif dry_run:
        for g in grosslagen_to_create:
            fake_id = f"dry-grosslage-{geo_slugify(g['name'])}"
            app_by_name[g["name"]] = {"id": fake_id, "name": g["name"], "classification_level": "grosslage"}
            grosslage_map[g["_raw"]] = fake_id

    # 7. Create Einzellagen
    print("\n--- Creating Einzellagen ---")
    einzellagen_to_create = []
    einzellage_map = {}

    for key, e_data in einzellagen.items():
        existing = app_by_name.get(e_data["name"])
        if existing:
            einzellage_map[key] = existing["id"]
            if existing.get("classification_level") != "einzellage":
                if not dry_run:
                    sb.table("appellations").update({"classification_level": "einzellage"}).eq("id", existing["id"]).execute()
            continue

        display_name = f"{e_data['name']}, {e_data['village']}" if e_data.get("village") else e_data["name"]
        einzellagen_to_create.append({
            "name": display_name,
            "slug": geo_slugify(f"{e_data['name']}-{e_data.get('village') or e_data['region']}-einzellage"),
            "designation_type": "Qualitätswein",
            "classification_level": "einzellage",
            "country_id": DE_ID,
            "region_id": default_region_id,
            "hemisphere": "north",
            "_key": key,
        })

    print(f"Einzellagen to create: {len(einzellagen_to_create)}")

    if not dry_run and einzellagen_to_create:
        BATCH = 200
        total_created = 0
        for i in range(0, len(einzellagen_to_create), BATCH):
            batch = [{k: v for k, v in e.items() if k != "_key"} for e in einzellagen_to_create[i:i + BATCH]]
            try:
                result = sb.table("appellations").insert(batch).select("id, name").execute()
                for c in result.data:
                    app_by_name[c["name"]] = {"id": c["id"], "name": c["name"], "classification_level": "einzellage"}
                    entry = next((e for e in einzellagen_to_create if e["name"] == c["name"]), None)
                    if entry:
                        einzellage_map[entry["_key"]] = c["id"]
                total_created += len(result.data)
            except Exception as err:
                print(f"Error inserting batch {i // BATCH + 1}: {err}")
                for row in batch:
                    try:
                        result = sb.table("appellations").insert(row).select("id, name").execute()
                        if result.data:
                            app_by_name[result.data[0]["name"]] = {"id": result.data[0]["id"], "name": result.data[0]["name"], "classification_level": "einzellage"}
                            total_created += 1
                    except Exception as e2:
                        print(f"  Failed: {row['name']}: {e2}")
            print(f"  Inserted {total_created}/{len(einzellagen_to_create)} Einzellagen")
        print(f"  Created {total_created} Einzellagen total")
    elif dry_run:
        for e in einzellagen_to_create:
            fake_id = f"dry-einzellage-{geo_slugify(e['name'])}"
            app_by_name[e["name"]] = {"id": fake_id, "name": e["name"], "classification_level": "einzellage"}
            einzellage_map[e["_key"]] = fake_id

    # 8. Build containment
    print("\n--- Building containment relationships ---")
    containment_rows = []

    # Anbaugebiet -> Bereich
    for raw_name, b_data in bereiche.items():
        parent_app = anbaugebiete_map.get(b_data["region"])
        clean_name = clean_bereich_name(raw_name)
        display_name = f"{clean_name} (Bereich)"
        child_app = app_by_name.get(display_name) or app_by_name.get(clean_name)
        if parent_app and child_app:
            containment_rows.append({"parent_id": parent_app["id"], "child_id": child_app["id"], "source": "explicit"})

    # Bereich -> Grosslage
    for name, g_data in grosslagen.items():
        display_name = f"{name} (Grosslage)"
        parent_app = app_by_name.get(f"{clean_bereich_name(g_data['bereich'])} (Bereich)") or app_by_name.get(clean_bereich_name(g_data["bereich"]))
        child_app = app_by_name.get(display_name) or app_by_name.get(name)
        if parent_app and child_app:
            containment_rows.append({"parent_id": parent_app["id"], "child_id": child_app["id"], "source": "explicit"})

    # Grosslage -> Einzellage (or Bereich -> Einzellage)
    for key, e_data in einzellagen.items():
        display_name = f"{e_data['name']}, {e_data['village']}" if e_data.get("village") else e_data["name"]
        child_app = app_by_name.get(display_name) or app_by_name.get(e_data["name"])
        if e_data.get("grosslage"):
            g_display = f"{e_data['grosslage']} (Grosslage)"
            parent_app = app_by_name.get(g_display) or app_by_name.get(e_data["grosslage"])
        else:
            b_display = f"{clean_bereich_name(e_data['bereich'])} (Bereich)"
            parent_app = app_by_name.get(b_display) or app_by_name.get(clean_bereich_name(e_data["bereich"]))
        if parent_app and child_app:
            containment_rows.append({"parent_id": parent_app["id"], "child_id": child_app["id"], "source": "explicit"})

    # Deduplicate
    seen = set()
    unique_rows = []
    for r in containment_rows:
        k = f"{r['parent_id']}|{r['child_id']}"
        if k not in seen:
            seen.add(k)
            unique_rows.append(r)

    print(f"\nContainment relationships: {len(unique_rows)}")

    if dry_run:
        print(f"\n=== DRY RUN SUMMARY ===")
        print(f"Bereiche to create: {len(bereiche_to_create)}")
        print(f"Grosslagen to create: {len(grosslagen_to_create)}")
        print(f"Einzellagen to create: {len(einzellagen_to_create)}")
        print(f"Total new appellations: {len(bereiche_to_create) + len(grosslagen_to_create) + len(einzellagen_to_create)}")
        print(f"Containment rows: {len(unique_rows)}")
        print("\n[DRY RUN] No changes made.")
        return

    # 9. Insert containment
    existing = sb.table("appellation_containment").select("parent_id, child_id").execute().data
    existing_set = {f"{r['parent_id']}|{r['child_id']}" for r in (existing or [])}
    to_insert = [r for r in unique_rows if f"{r['parent_id']}|{r['child_id']}" not in existing_set]

    if not to_insert:
        print("\nAll containment relationships already exist.")
    else:
        print(f"\nInserting {len(to_insert)} new containment rows...")
        BATCH = 500
        inserted = 0
        for i in range(0, len(to_insert), BATCH):
            batch = to_insert[i:i + BATCH]
            sb.table("appellation_containment").insert(batch).execute()
            inserted += len(batch)
            print(f"  Inserted {inserted}/{len(to_insert)}")
        print(f"  Done! Inserted {inserted} containment rows.")

    print("\nDone!")


if __name__ == "__main__":
    main()
