#!/usr/bin/env python3
"""
Import LWIN database as identity backbone.

Parses the LWIN CSV (186K wine records) and imports producers + wines
with LWIN-7 codes. No vintages, scores, or prices -- LWIN is pure identity.

Modes:
  --analyze     Show match rates without writing anything
  --dry-run     Show what would be imported without writing
  --import      Actually import to DB
  --limit N     Process only first N wine rows
  --country XX  Only process wines from country XX

Usage:
    python -m pipeline.promote.import_lwin --analyze
    python -m pipeline.promote.import_lwin --analyze --country France
    python -m pipeline.promote.import_lwin --import --limit 500
"""

import argparse
import csv
import sys
import uuid
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize, slugify

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

COLOR_MAP = {"Red": "red", "White": "white", "Rose": "rose", "Mixed": None}

REGION_NAME_MAP = {
    "burgundy": "bourgogne", "rhone": "rhône valley", "loire": "loire valley",
    "champagne": "champagne", "bordeaux": "bordeaux", "alsace": "alsace",
    "languedoc": "languedoc-roussillon", "beaujolais": "beaujolais",
    "provence": "provence", "corsica": "corse", "jura": "jura", "savoie": "savoie",
    "roussillon": "languedoc-roussillon", "south west france": "southwest france",
    "piedmont": "piemonte", "tuscany": "tuscany", "sicily": "sicily",
    "sardinia": "sardinia", "trentino alto adige": "trentino-alto adige",
    "friuli venezia giulia": "friuli-venezia giulia", "emilia romagna": "emilia-romagna",
    "lombardia": "lombardy", "puglia": "puglia", "campania": "campania",
    "veneto": "veneto", "abruzzo": "abruzzo", "umbria": "umbria", "lazio": "lazio",
    "liguria": "liguria", "calabria": "calabria", "marche": "marche",
    "basilicata": "basilicata", "molise": "molise", "prosecco": "veneto",
    "mosel": "mosel", "pfalz": "pfalz", "rheingau": "rheingau",
    "rheinhessen": "rheinhessen", "nahe": "nahe", "franken": "franken",
    "wurttemberg": "württemberg",
    "castilla y leon": "castilla y león", "castilla la mancha": "castilla-la mancha",
    "catalunya": "catalunya", "andalucia": "andalucía",
    "galicia": "the north west", "murcia": "the levante",
    "douro": "douro", "dao": "dão", "alentejano": "alentejo", "porto": "douro",
    "california": "california", "washington": "washington", "oregon": "oregon",
    "new york": "new york", "virginia": "virginia",
    "south australia": "south australia", "victoria": "victoria",
    "western australia": "western australia", "new south wales": "new south wales",
    "tasmania": "tasmania",
    "marlborough": "marlborough", "central otago": "central otago",
    "coastal region": "coastal region",
    "niederosterreich": "niederösterreich", "burgenland": "burgenland",
    "steiermark": "steiermark", "wien": "wien",
    "mendoza": "mendoza", "patagonia": "patagonia", "salta": "salta",
    "central valley": "central valley region", "aconcagua": "aconcagua region",
}

CLASSIFICATION_MAP = {
    "Grand Cru": {"system_slug": "burgundy-vineyard", "level_name": "Grand Cru"},
    "Premier Cru": {"system_slug": "burgundy-vineyard", "level_name": "Premier Cru"},
    "Grand Cru Classe": {"system_slug": "saint-emilion", "level_name": "Grand Cru Classé"},
    "Premier Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Premier Cru"},
    "2eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Deuxième Cru"},
    "3eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Troisième Cru"},
    "4eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Quatrième Cru"},
    "5eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Cinquième Cru"},
    "Erste Lage": {"system_slug": "vdp-classification", "level_name": "Erste Lage"},
    "Cru Classe": {"system_slug": "graves-pessac-leognan", "level_name": "Cru Classé"},
}

ALLOWED_TYPES = {"Wine", "Fortified Wine", "Champagne"}


def map_wine_type(type_val: str, sub_type: str | None) -> dict:
    if type_val == "Wine" and sub_type == "Sparkling":
        return {"wine_type": "sparkling", "effervescence": "sparkling"}
    if type_val == "Champagne":
        return {"wine_type": "sparkling", "effervescence": "sparkling"}
    if type_val == "Fortified Wine":
        return {"wine_type": "fortified", "effervescence": "still"}
    return {"wine_type": "table", "effervescence": "still"}


def main():
    parser = argparse.ArgumentParser(description="LWIN database import")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--analyze", action="store_true")
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--import", dest="do_import", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--country", type=str, default=None)
    args = parser.parse_args()

    mode = "import" if args.do_import else "dry-run" if args.dry_run else "analyze"
    limit = args.limit or float("inf")

    print(f"Mode: {mode}" + (f", limit: {args.limit}" if args.limit else "") +
          (f", country: {args.country}" if args.country else ""))

    # Parse CSV
    print("\nParsing LWIN CSV...")
    csv_path = DATA_DIR / "lwin_database.csv"
    wine_rows = []
    skipped_non_wine = 0
    skipped_status = 0

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            type_val = row.get("TYPE", "")
            if type_val not in ALLOWED_TYPES:
                skipped_non_wine += 1
                continue
            if row.get("STATUS") == "Deleted":
                skipped_status += 1
                continue
            if args.country and row.get("COUNTRY") != args.country:
                continue

            def na(v):
                return None if not v or v == "NA" else v

            wine_rows.append({
                "lwin": row.get("LWIN", ""),
                "status": row.get("STATUS"),
                "display_name": na(row.get("DISPLAY_NAME")),
                "producer_title": na(row.get("PRODUCER_TITLE")),
                "producer_name": na(row.get("PRODUCER_NAME")),
                "wine_name": na(row.get("WINE")),
                "country": na(row.get("COUNTRY")),
                "region": na(row.get("REGION")),
                "sub_region": na(row.get("SUB_REGION")),
                "site": na(row.get("SITE")),
                "color": na(row.get("COLOUR")),
                "type": type_val,
                "sub_type": na(row.get("SUB_TYPE")),
                "designation": na(row.get("DESIGNATION")),
                "classification": na(row.get("CLASSIFICATION")),
                "vintage_config": na(row.get("VINTAGE_CONFIG")),
                "first_vintage": int(row["FIRST_VINTAGE"]) if na(row.get("FIRST_VINTAGE")) and row["FIRST_VINTAGE"].isdigit() else None,
            })
            if len(wine_rows) >= limit:
                break

    print(f"Parsed {len(wine_rows)} wine rows (skipped {skipped_non_wine} non-wine, {skipped_status} deleted)")

    # Load reference data
    print("\nLoading reference data...")
    sb = get_supabase()

    countries = fetch_all("countries", "id,name")
    country_map = {c["name"].lower(): c["id"] for c in countries}

    regions = fetch_all("regions", "id,name,country_id,parent_id,is_catch_all")
    region_map: dict[str, dict] = {}
    for r in regions:
        region_map[r["name"].lower()] = r
        region_map[normalize(r["name"])] = r
        region_map[f"{normalize(r['name'])}|{r['country_id']}"] = r

    region_aliases = fetch_all("region_aliases", "id,name,region_id")
    for ra in region_aliases:
        reg = next((r for r in regions if r["id"] == ra["region_id"]), None)
        if reg:
            region_map[normalize(ra["name"])] = reg
            region_map[f"{normalize(ra['name'])}|{reg['country_id']}"] = reg

    appellations = fetch_all("appellations", "id,name,country_id,region_id")
    appellation_map: dict[str, dict] = {}
    for a in appellations:
        appellation_map[a["name"].lower()] = a
        appellation_map[normalize(a["name"])] = a

    app_aliases = fetch_all("appellation_aliases", "id,alias,appellation_id")
    for aa in app_aliases:
        app = next((a for a in appellations if a["id"] == aa["appellation_id"]), None)
        if app:
            appellation_map[normalize(aa["alias"])] = app
            appellation_map[aa["alias"].lower()] = app

    classifications = fetch_all("classifications", "id,slug,name")
    class_map = {c["slug"]: c for c in classifications}

    class_levels = fetch_all("classification_levels", "id,classification_id,level_name,level_rank")
    class_level_map = {}
    for cl in class_levels:
        class_level_map[f"{cl['classification_id']}|{cl['level_name'].lower()}"] = cl

    print(f"  {len(countries)} countries, {len(regions)} regions, {len(appellations)} appellations")

    # Resolution functions
    def resolve_country(name):
        if not name:
            return None
        return country_map.get(name.lower())

    def resolve_region(lwin_region, country_id):
        if not lwin_region:
            return None
        lower = lwin_region.lower()
        mapped = REGION_NAME_MAP.get(lower)
        if mapped:
            norm = normalize(mapped)
            if country_id:
                r = region_map.get(f"{norm}|{country_id}")
                if r:
                    return r
            r2 = region_map.get(norm)
            if r2:
                return r2
        norm = normalize(lower)
        if country_id:
            r = region_map.get(f"{norm}|{country_id}")
            if r:
                return r
        return region_map.get(norm)

    def resolve_appellation(sub_region, site, country_id):
        if sub_region:
            a = appellation_map.get(normalize(sub_region)) or appellation_map.get(sub_region.lower())
            if a:
                return a
        if site:
            a = appellation_map.get(normalize(site)) or appellation_map.get(site.lower())
            if a:
                return a
        return None

    def resolve_classification(lwin_class):
        if not lwin_class:
            return None
        mapping = CLASSIFICATION_MAP.get(lwin_class)
        if not mapping:
            return None
        system = class_map.get(mapping["system_slug"])
        if not system:
            return None
        level = class_level_map.get(f"{system['id']}|{mapping['level_name'].lower()}")
        return {"system": system, "level": level} if level else None

    # Resolve all rows
    print("\nResolving references...")
    stats = {
        "total": len(wine_rows),
        "country_resolved": 0, "country_missing": Counter(),
        "region_resolved": 0, "region_missing": Counter(),
        "appellation_resolved": 0, "appellation_missing": Counter(),
        "classification_resolved": 0, "classification_missing": Counter(),
        "unique_producers": set(), "is_nv": 0, "has_first_vintage": 0,
    }

    resolved_rows = []
    for row in wine_rows:
        cid = resolve_country(row["country"])
        if cid:
            stats["country_resolved"] += 1
        elif row["country"]:
            stats["country_missing"][row["country"]] += 1

        reg = resolve_region(row["region"], cid)
        if reg:
            stats["region_resolved"] += 1
        elif row["region"]:
            stats["region_missing"][f"{row['country']}|{row['region']}"] += 1

        app = resolve_appellation(row["sub_region"], row["site"], cid)
        if app:
            stats["appellation_resolved"] += 1
        elif row["sub_region"]:
            stats["appellation_missing"][f"{row['country']}|{row['sub_region']}"] += 1

        cls = resolve_classification(row["classification"])
        if cls:
            stats["classification_resolved"] += 1
        elif row["classification"]:
            stats["classification_missing"][row["classification"]] += 1

        if row["first_vintage"]:
            stats["has_first_vintage"] += 1
        if row["vintage_config"] == "nonSequential":
            stats["is_nv"] += 1

        producer_key = row["producer_name"] or (row["display_name"] or "").split(",")[0].strip()
        if producer_key:
            stats["unique_producers"].add(producer_key)

        resolved_rows.append({
            **row, "_country_id": cid, "_region": reg,
            "_appellation": app, "_classification": cls,
            "_producer_key": producer_key,
        })

    # Print analysis
    pct = lambda n: f"{(n / stats['total'] * 100):.1f}%"
    print(f"\nTotal wine rows: {stats['total']}")
    print(f"Unique producers: {len(stats['unique_producers'])}")
    print(f"NV wines: {stats['is_nv']}")
    print(f"\nRESOLUTION RATES:")
    print(f"  Country:        {stats['country_resolved']}/{stats['total']} ({pct(stats['country_resolved'])})")
    print(f"  Region:         {stats['region_resolved']}/{stats['total']} ({pct(stats['region_resolved'])})")
    print(f"  Appellation:    {stats['appellation_resolved']}/{stats['total']} ({pct(stats['appellation_resolved'])})")
    print(f"  Classification: {stats['classification_resolved']}/{stats['total']} ({pct(stats['classification_resolved'])})")

    if stats["country_missing"]:
        print("\nUNRESOLVED COUNTRIES:")
        for k, v in stats["country_missing"].most_common(20):
            print(f"  {k}: {v}")
    if stats["region_missing"]:
        print("\nUNRESOLVED REGIONS (top 30):")
        for k, v in stats["region_missing"].most_common(30):
            print(f"  {k}: {v}")
    if stats["appellation_missing"]:
        print("\nUNRESOLVED APPELLATIONS (top 30):")
        for k, v in stats["appellation_missing"].most_common(30):
            print(f"  {k}: {v}")

    if mode == "analyze":
        print("\nAnalysis complete. Run with --import to write to DB.")
        return

    # Import mode
    print("\n\nStarting import...")
    producer_groups: dict[str, list] = {}
    for row in resolved_rows:
        key = row["_producer_key"]
        if not key:
            continue
        producer_groups.setdefault(key, []).append(row)

    print(f"{len(producer_groups)} producers to process")

    # Check existing LWIN wines
    existing_lwins = set()
    offset = 0
    while True:
        result = sb.table("wines").select("lwin").not_.is_("lwin", "null").range(offset, offset + 999).execute()
        for w in (result.data or []):
            existing_lwins.add(w["lwin"])
        if len(result.data or []) < 1000:
            break
        offset += 1000
    print(f"{len(existing_lwins)} wines with LWIN already in DB")

    # Check existing producers
    existing_producers: dict[str, str] = {}
    offset = 0
    while True:
        result = sb.table("producers").select("id,name,name_normalized").range(offset, offset + 999).execute()
        for p in (result.data or []):
            existing_producers[p["name_normalized"]] = p["id"]
            existing_producers[p["name"].lower()] = p["id"]
        if len(result.data or []) < 1000:
            break
        offset += 1000
    print(f"{len(existing_producers) // 2} existing producers in DB")

    producers_created = 0
    wines_created = 0
    wines_skipped = 0
    classifications_linked = 0
    errors = 0
    batch_num = 0
    BATCH_SIZE = 50

    entries = list(producer_groups.items())
    for b in range(0, len(entries), BATCH_SIZE):
        batch_num += 1
        batch = entries[b:b + BATCH_SIZE]

        producer_inserts = []
        wine_inserts = []
        class_inserts = []

        for producer_name, rows in batch:
            norm_name = normalize(producer_name)
            producer_id = existing_producers.get(norm_name) or existing_producers.get(producer_name.lower())

            if not producer_id:
                country_counts: Counter = Counter()
                region_counts: Counter = Counter()
                for r in rows:
                    if r["_country_id"]:
                        country_counts[r["_country_id"]] += 1
                    if r["_region"]:
                        region_counts[r["_region"]["id"]] += 1
                top_country = country_counts.most_common(1)[0][0] if country_counts else None
                top_region = region_counts.most_common(1)[0][0] if region_counts else None

                producer_id = str(uuid.uuid4())
                title = rows[0].get("producer_title")
                full_name = f"{title} {producer_name}" if title else producer_name

                producer_inserts.append({
                    "id": producer_id, "slug": slugify(full_name),
                    "name": full_name, "name_normalized": norm_name,
                    "country_id": top_country, "region_id": top_region,
                    "producer_type": "estate",
                })
                existing_producers[norm_name] = producer_id
                producers_created += 1

            for row in rows:
                if row["lwin"] in existing_lwins:
                    wines_skipped += 1
                    continue

                wt = map_wine_type(row["type"], row["sub_type"])
                color = COLOR_MAP.get(row["color"]) if row["color"] else None
                is_nv = row["vintage_config"] == "nonSequential"
                wine_name = row["wine_name"] or row["display_name"] or "Unknown"
                wine_slug = slugify(f"{row['_producer_key']}-{wine_name}-{row['lwin']}")

                wine = {
                    "id": str(uuid.uuid4()), "slug": wine_slug,
                    "name": wine_name, "name_normalized": normalize(wine_name),
                    "producer_id": producer_id, "country_id": row["_country_id"],
                    "region_id": row["_region"]["id"] if row["_region"] else None,
                    "appellation_id": row["_appellation"]["id"] if row["_appellation"] else None,
                    "color": color, **wt, "is_nv": is_nv,
                    "lwin": row["lwin"], "first_vintage_year": row["first_vintage"],
                }
                wine_inserts.append(wine)
                existing_lwins.add(row["lwin"])
                wines_created += 1

                if row["_classification"] and row["_classification"].get("level"):
                    class_inserts.append({
                        "id": str(uuid.uuid4()), "entity_type": "wine",
                        "entity_id": wine["id"],
                        "classification_id": row["_classification"]["system"]["id"],
                        "classification_level_id": row["_classification"]["level"]["id"],
                    })
                    classifications_linked += 1

        if mode == "import" and producer_inserts:
            try:
                sb.table("producers").upsert(producer_inserts, on_conflict="slug").execute()
            except Exception as e:
                print(f"  Batch {batch_num} producer error: {e}")
                errors += 1

        if mode == "import" and wine_inserts:
            for w in range(0, len(wine_inserts), 200):
                chunk = wine_inserts[w:w + 200]
                try:
                    sb.table("wines").upsert(chunk, on_conflict="slug").execute()
                except Exception as e:
                    print(f"  Batch {batch_num} wine chunk error: {e}")
                    errors += 1

        if mode == "import" and class_inserts:
            try:
                sb.table("entity_classifications").insert(class_inserts).execute()
            except Exception as e:
                if "duplicate" not in str(e):
                    print(f"  Batch {batch_num} classification error: {e}")
                    errors += 1

        if batch_num % 20 == 0 or b + BATCH_SIZE >= len(entries):
            print(f"  Batch {batch_num}/{(len(entries) + BATCH_SIZE - 1) // BATCH_SIZE} -- producers: +{len(producer_inserts)}, wines: +{len(wine_inserts)}")

    print(f"\n{'=' * 50}")
    print("  IMPORT COMPLETE")
    print("=" * 50)
    print(f"Producers created: {producers_created}")
    print(f"Wines created:     {wines_created} (skipped {wines_skipped} existing LWIN)")
    print(f"Classifications:   {classifications_linked}")
    print(f"Errors:            {errors}")


if __name__ == "__main__":
    main()
