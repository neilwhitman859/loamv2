#!/usr/bin/env python3
"""
Import LWIN database as identity backbone.

Parses the LWIN CSV (186K wine records) and imports producers + wines
with LWIN-7 codes. No vintages, scores, or prices -- LWIN is pure identity.

Modes:
    --analyze     Show match rates without writing anything
    --dry-run     Show what would be imported without writing
    --import      Actually import to DB
    --limit N     Process only first N wine rows (for testing)
    --country XX  Only process wines from country XX (e.g., "France")

Usage:
    python -m pipeline.load.import_lwin --analyze
    python -m pipeline.load.import_lwin --analyze --country France
    python -m pipeline.load.import_lwin --import --limit 500
    python -m pipeline.load.import_lwin --import
"""

import argparse
import csv
import io
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
BATCH_SIZE = 1000

# ── LWIN Field Maps ──────────────────────────────────────────

COLOR_MAP = {"Red": "red", "White": "white", "Rose": "rose", "Mixed": None}

REGION_NAME_MAP = {
    # France
    "burgundy": "bourgogne", "rhone": "rhône valley", "loire": "loire valley",
    "champagne": "champagne", "bordeaux": "bordeaux", "alsace": "alsace",
    "languedoc": "languedoc-roussillon", "beaujolais": "beaujolais",
    "provence": "provence", "corsica": "corse", "jura": "jura", "savoie": "savoie",
    "roussillon": "languedoc-roussillon", "south west france": "southwest france",
    # Italy
    "piedmont": "piemonte", "tuscany": "tuscany", "sicily": "sicily",
    "sardinia": "sardinia", "trentino alto adige": "trentino-alto adige",
    "friuli venezia giulia": "friuli-venezia giulia", "emilia romagna": "emilia-romagna",
    "lombardia": "lombardy", "puglia": "puglia", "campania": "campania",
    "veneto": "veneto", "abruzzo": "abruzzo", "umbria": "umbria", "lazio": "lazio",
    "liguria": "liguria", "calabria": "calabria", "marche": "marche",
    "basilicata": "basilicata", "molise": "molise", "prosecco": "veneto",
    # Germany
    "mosel": "mosel", "pfalz": "pfalz", "rheingau": "rheingau",
    "rheinhessen": "rheinhessen", "nahe": "nahe", "franken": "franken",
    "wurttemberg": "württemberg", "mittelrhein": "mittelrhein",
    "sachsen": "sachsen", "saale unstrut": "saale-unstrut", "ahr": "ahr",
    # Spain
    "castilla y leon": "castilla y león", "castilla la mancha": "castilla-la mancha",
    "catalunya": "catalunya", "andalucia": "andalucía", "aragon": "aragón",
    "pais vasco": "país vasco", "extremadura": "extremadura",
    "galicia": "the north west", "murcia": "the levante",
    "navarra": "navarra", "cava": "catalunya",
    # Portugal
    "douro": "douro", "dao": "dão", "alentejano": "alentejo", "porto": "douro",
    # US
    "california": "california", "washington": "washington", "oregon": "oregon",
    "new york": "new york", "virginia": "virginia",
    "walla walla valley": "washington", "arizona": "arizona", "texas": "texas",
    "michigan": "michigan", "colorado": "colorado", "pennsylvania": "pennsylvania",
    "idaho": "idaho", "north carolina": "north carolina",
    # Australia
    "south australia": "south australia", "victoria": "victoria",
    "western australia": "western australia", "new south wales": "new south wales",
    "tasmania": "tasmania", "south eastern australia": "south eastern australia",
    "queensland": "queensland",
    # New Zealand
    "marlborough": "marlborough", "hawke's bay": "hawke's bay",
    "central otago": "central otago", "wairarapa": "martinborough",
    "canterbury": "canterbury", "auckland": "north island",
    "nelson": "nelson", "gisborne": "gisborne",
    # South Africa
    "coastal region": "coastal region", "cape south coast": "cape south coast",
    "breede river valley": "breede river valley", "olifants river": "olifants river",
    "klein karoo": "klein karoo",
    # Austria
    "niederosterreich": "niederösterreich", "burgenland": "burgenland",
    "steiermark": "steiermark", "wien": "wien",
    # Argentina
    "mendoza": "mendoza", "patagonia": "patagonia", "salta": "salta",
    # Chile
    "central valley": "central valley region", "aconcagua": "aconcagua region",
    "sur": "southern region", "coquimbo": "coquimbo region",
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
    "Premier Grand Cru Classe A": {"system_slug": "saint-emilion", "level_name": "Premier Grand Cru Classé A"},
    "Premier Grand Cru Classe B": {"system_slug": "saint-emilion", "level_name": "Premier Grand Cru Classé B"},
    "Premier Cru Superieur": {"system_slug": "bordeaux-1855-sauternes", "level_name": "Premier Cru Supérieur"},
    "Erste Lage": {"system_slug": "vdp-classification", "level_name": "Erste Lage"},
    "Cru Classe": {"system_slug": "graves-pessac-leognan", "level_name": "Cru Classé"},
}


def map_wine_type(wine_type: str | None, sub_type: str | None) -> dict:
    if wine_type == "Wine" and sub_type == "Still":
        return {"wine_type": "table", "effervescence": "still"}
    if wine_type == "Wine" and sub_type == "Sparkling":
        return {"wine_type": "sparkling", "effervescence": "sparkling"}
    if wine_type == "Champagne":
        return {"wine_type": "sparkling", "effervescence": "sparkling"}
    if wine_type == "Fortified Wine":
        if sub_type in ("Vin Doux Naturel", "Moscatel de Setubal"):
            return {"wine_type": "dessert", "effervescence": "still"}
        return {"wine_type": "fortified", "effervescence": "still"}
    return {"wine_type": "table", "effervescence": "still"}


def map_vintage_config(vc: str | None) -> bool:
    """Returns True if wine is NV."""
    return vc == "nonSequential"


def fetch_all_sync(sb, table: str, columns: str = "*") -> list[dict]:
    all_rows: list[dict] = []
    offset = 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + BATCH_SIZE - 1).execute()
        all_rows.extend(result.data)
        if len(result.data) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    return all_rows


def parse_csv_line(line: str) -> list[str]:
    """Parse a CSV line handling quoted fields."""
    reader = csv.reader(io.StringIO(line))
    return next(reader)


def main():
    parser = argparse.ArgumentParser(description="Import LWIN database as identity backbone")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--analyze", action="store_true", help="Show match rates only")
    group.add_argument("--dry-run", action="store_true", help="Show what would happen")
    group.add_argument("--import", dest="do_import", action="store_true", help="Actually import")
    parser.add_argument("--limit", type=int, default=0, help="Max wine rows to process")
    parser.add_argument("--country", type=str, help="Filter by country name")
    args = parser.parse_args()

    mode = "import" if args.do_import else ("dry-run" if args.dry_run else "analyze")
    limit = args.limit or float("inf")

    sb = get_supabase()
    print(f"Mode: {mode}" + (f", limit: {args.limit}" if args.limit else "") +
          (f", country: {args.country}" if args.country else ""))

    # ── Parse LWIN CSV ───────────────────────────────────────
    print("\nParsing LWIN CSV...")
    csv_path = DATA_DIR / "lwin_database.csv"
    csv_text = csv_path.read_text(encoding="utf-8")
    lines = csv_text.split("\n")
    header = parse_csv_line(lines[0])

    wine_rows: list[dict] = []
    skipped_non_wine = 0
    skipped_status = 0

    for i in range(1, len(lines)):
        line = lines[i].strip()
        if not line:
            continue
        cols = parse_csv_line(line)

        wine_type = cols[12] if len(cols) > 12 else ""
        if wine_type not in ("Wine", "Fortified Wine", "Champagne"):
            skipped_non_wine += 1
            continue

        status = cols[1] if len(cols) > 1 else ""
        if status == "Deleted":
            skipped_status += 1
            continue

        def col_or_none(idx):
            val = cols[idx] if len(cols) > idx else ""
            return val if val and val != "NA" else None

        row = {
            "lwin": cols[0],
            "status": cols[1],
            "display_name": col_or_none(2),
            "producer_title": col_or_none(3),
            "producer_name": col_or_none(4),
            "wine_name": col_or_none(5),
            "country": col_or_none(6),
            "region": col_or_none(7),
            "sub_region": col_or_none(8),
            "site": col_or_none(9),
            "parcel": col_or_none(10),
            "color": col_or_none(11),
            "type": cols[12],
            "sub_type": col_or_none(13),
            "designation": col_or_none(14),
            "classification": col_or_none(15),
            "vintage_config": col_or_none(16),
            "first_vintage": None,
            "final_vintage": None,
        }

        if col_or_none(17):
            try:
                row["first_vintage"] = int(cols[17])
            except ValueError:
                pass
        if col_or_none(18):
            try:
                row["final_vintage"] = int(cols[18])
            except ValueError:
                pass

        if args.country and row["country"] != args.country:
            continue

        wine_rows.append(row)
        if len(wine_rows) >= limit:
            break

    print(f"Parsed {len(wine_rows)} wine rows (skipped {skipped_non_wine} non-wine, {skipped_status} deleted)")

    # ── Load Reference Data ──────────────────────────────────
    print("\nLoading reference data...")

    countries = fetch_all_sync(sb, "countries", "id,name")
    regions = fetch_all_sync(sb, "regions", "id,name,country_id,parent_id,is_catch_all")
    appellations = fetch_all_sync(sb, "appellations", "id,name,country_id,region_id")
    app_aliases = fetch_all_sync(sb, "appellation_aliases", "id,alias,appellation_id")
    region_aliases = fetch_all_sync(sb, "region_aliases", "id,name,region_id")
    classifications = fetch_all_sync(sb, "classifications", "id,slug,name")
    classification_levels = fetch_all_sync(sb, "classification_levels", "id,classification_id,level_name,level_rank")

    # Build lookup maps
    country_map: dict[str, str] = {}
    for c in countries:
        country_map[c["name"].lower()] = c["id"]
    us_id = country_map.get("united states")
    if us_id:
        country_map["usa"] = us_id

    region_map: dict[str, dict] = {}
    for r in regions:
        lower = r["name"].lower()
        norm = normalize(r["name"])
        region_map[f"{lower}|{r['country_id']}"] = r
        region_map[f"{norm}|{r['country_id']}"] = r
        region_map[lower] = r
    for ra in region_aliases:
        region = next((r for r in regions if r["id"] == ra["region_id"]), None)
        if region:
            norm = normalize(ra["name"])
            region_map[f"{norm}|{region['country_id']}"] = region
            region_map[norm] = region

    appellation_map: dict[str, dict] = {}
    for a in appellations:
        appellation_map[a["name"].lower()] = a
        appellation_map[normalize(a["name"])] = a
    for aa in app_aliases:
        app = next((a for a in appellations if a["id"] == aa["appellation_id"]), None)
        if app:
            norm = normalize(aa["alias"])
            appellation_map[norm] = app
            appellation_map[aa["alias"].lower()] = app

    class_map: dict[str, dict] = {c["slug"]: c for c in classifications}
    class_level_map: dict[str, dict] = {}
    for cl in classification_levels:
        class_level_map[f"{cl['classification_id']}|{cl['level_name'].lower()}"] = cl

    print(f"  {len(countries)} countries, {len(regions)} regions, {len(appellations)} appellations")
    print(f"  {len(app_aliases)} appellation aliases, {len(region_aliases)} region aliases")
    print(f"  {len(classifications)} classifications, {len(classification_levels)} classification levels")

    # ── Resolution Functions ─────────────────────────────────

    def resolve_country(name):
        return country_map.get(name.lower()) if name else None

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

    def resolve_appellation(sub_region, site, _country_id):
        if sub_region:
            norm = normalize(sub_region)
            a = appellation_map.get(norm) or appellation_map.get(sub_region.lower())
            if a:
                return a
        if site:
            norm = normalize(site)
            a = appellation_map.get(norm) or appellation_map.get(site.lower())
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

    # ── Analysis ─────────────────────────────────────────────
    print("\nResolving references...")

    stats = {
        "total": len(wine_rows),
        "country_resolved": 0, "country_missing": {},
        "region_resolved": 0, "region_missing": {},
        "appellation_resolved": 0, "appellation_missing": {},
        "classification_resolved": 0, "classification_missing": {},
        "unique_producers": set(),
        "producer_names_blank": 0,
        "color_mapped": 0, "type_mapped": 0,
        "has_first_vintage": 0, "is_nv": 0,
    }

    resolved_rows: list[dict] = []

    for row in wine_rows:
        country_id = resolve_country(row["country"])
        if country_id:
            stats["country_resolved"] += 1
        elif row["country"]:
            stats["country_missing"][row["country"]] = stats["country_missing"].get(row["country"], 0) + 1

        region = resolve_region(row["region"], country_id)
        if region:
            stats["region_resolved"] += 1
        elif row["region"]:
            key = f"{row['country']}|{row['region']}"
            stats["region_missing"][key] = stats["region_missing"].get(key, 0) + 1

        appellation = resolve_appellation(row["sub_region"], row["site"], country_id)
        if appellation:
            stats["appellation_resolved"] += 1
        elif row["sub_region"]:
            key = f"{row['country']}|{row['sub_region']}"
            stats["appellation_missing"][key] = stats["appellation_missing"].get(key, 0) + 1

        classification = resolve_classification(row["classification"])
        if classification:
            stats["classification_resolved"] += 1
        elif row["classification"]:
            stats["classification_missing"][row["classification"]] = \
                stats["classification_missing"].get(row["classification"], 0) + 1

        if row["color"] and row["color"] in COLOR_MAP:
            stats["color_mapped"] += 1
        stats["type_mapped"] += 1
        if row["first_vintage"]:
            stats["has_first_vintage"] += 1
        if map_vintage_config(row["vintage_config"]):
            stats["is_nv"] += 1

        producer_key = row["producer_name"]
        if not producer_key and row.get("display_name"):
            producer_key = row["display_name"].split(",")[0].strip()
        if producer_key:
            stats["unique_producers"].add(producer_key)
        else:
            stats["producer_names_blank"] += 1

        resolved_rows.append({
            **row,
            "_country_id": country_id,
            "_region": region,
            "_appellation": appellation,
            "_classification": classification,
            "_producer_key": producer_key,
        })

    # ── Print Analysis Report ────────────────────────────────
    total = stats["total"]

    def pct(n):
        return f"{(n / total * 100):.1f}%" if total > 0 else "0%"

    print(f"\n{'=' * 51}")
    print("  LWIN IMPORT ANALYSIS REPORT")
    print(f"{'=' * 51}\n")

    print(f"Total wine rows: {total}")
    print(f"Unique producers: {len(stats['unique_producers'])}")
    print(f"Producer names blank: {stats['producer_names_blank']}")
    print(f"NV wines: {stats['is_nv']}")
    print(f"Has first_vintage: {stats['has_first_vintage']}\n")

    print("RESOLUTION RATES:")
    print(f"  Country:        {stats['country_resolved']}/{total} ({pct(stats['country_resolved'])})")
    print(f"  Region:         {stats['region_resolved']}/{total} ({pct(stats['region_resolved'])})")
    print(f"  Appellation:    {stats['appellation_resolved']}/{total} ({pct(stats['appellation_resolved'])})")
    print(f"  Classification: {stats['classification_resolved']}/{total} ({pct(stats['classification_resolved'])})")

    if stats["country_missing"]:
        print("\nUNRESOLVED COUNTRIES:")
        for k, v in sorted(stats["country_missing"].items(), key=lambda x: -x[1])[:20]:
            print(f"  {k}: {v}")

    if stats["region_missing"]:
        print("\nUNRESOLVED REGIONS (top 30):")
        for k, v in sorted(stats["region_missing"].items(), key=lambda x: -x[1])[:30]:
            print(f"  {k}: {v}")

    if stats["appellation_missing"]:
        print("\nUNRESOLVED APPELLATIONS (top 30):")
        for k, v in sorted(stats["appellation_missing"].items(), key=lambda x: -x[1])[:30]:
            print(f"  {k}: {v}")

    if stats["classification_missing"]:
        print("\nUNRESOLVED CLASSIFICATIONS:")
        for k, v in sorted(stats["classification_missing"].items(), key=lambda x: -x[1]):
            print(f"  {k}: {v}")

    if mode == "analyze":
        print("\nAnalysis complete. Run with --import to write to DB.")
        return

    # ── Import Mode ──────────────────────────────────────────
    print("\n\nStarting import...")

    # Group by producer
    producer_groups: dict[str, list[dict]] = {}
    for row in resolved_rows:
        key = row["_producer_key"]
        if not key:
            continue
        producer_groups.setdefault(key, []).append(row)

    print(f"{len(producer_groups)} producers to process")

    # Check for existing LWIN wines
    existing_lwins: set[str] = set()
    offset = 0
    while True:
        result = sb.table("wines").select("lwin").not_.is_("lwin", "null") \
            .range(offset, offset + 999).execute()
        for w in result.data:
            existing_lwins.add(w["lwin"])
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"{len(existing_lwins)} wines with LWIN already in DB")

    # Check for existing producers
    existing_producers: dict[str, str] = {}
    offset = 0
    while True:
        result = sb.table("producers").select("id,name,name_normalized") \
            .range(offset, offset + 999).execute()
        for p in result.data:
            existing_producers[p["name_normalized"]] = p["id"]
            existing_producers[p["name"].lower()] = p["id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"{len(existing_producers) // 2} existing producers in DB")

    producers_created = 0
    producers_skipped = 0
    wines_created = 0
    wines_skipped = 0
    classifications_linked = 0
    errors = 0
    batch_num = 0

    producer_entries = list(producer_groups.items())
    PRODUCER_BATCH = 50

    for b in range(0, len(producer_entries), PRODUCER_BATCH):
        batch_num += 1
        batch = producer_entries[b:b + PRODUCER_BATCH]

        producer_inserts: list[dict] = []
        wine_inserts: list[dict] = []
        class_inserts: list[dict] = []

        for producer_name, rows in batch:
            norm_name = normalize(producer_name)
            producer_id = existing_producers.get(norm_name) or \
                existing_producers.get(producer_name.lower())

            if not producer_id:
                # Determine producer country from most common wine country
                country_counts: dict[str, int] = {}
                for r in rows:
                    if r["_country_id"]:
                        country_counts[r["_country_id"]] = country_counts.get(r["_country_id"], 0) + 1
                top_country_entry = sorted(country_counts.items(), key=lambda x: -x[1])
                top_country = top_country_entry[0][0] if top_country_entry else None

                region_counts: dict[str, int] = {}
                for r in rows:
                    if r["_region"]:
                        region_counts[r["_region"]["id"]] = region_counts.get(r["_region"]["id"], 0) + 1
                top_region_entry = sorted(region_counts.items(), key=lambda x: -x[1])
                top_region = top_region_entry[0][0] if top_region_entry else None

                producer_id = str(uuid.uuid4())
                producer_title = rows[0].get("producer_title")
                full_name = f"{producer_title} {producer_name}" if producer_title else producer_name

                producer_inserts.append({
                    "id": producer_id,
                    "slug": slugify(full_name),
                    "name": full_name,
                    "name_normalized": norm_name,
                    "country_id": top_country,
                    "region_id": top_region,
                    "producer_type": "estate",
                })

                existing_producers[norm_name] = producer_id
                producers_created += 1
            else:
                producers_skipped += 1

            # Create wines
            for row in rows:
                if row["lwin"] in existing_lwins:
                    wines_skipped += 1
                    continue

                type_info = map_wine_type(row["type"], row["sub_type"])
                color = COLOR_MAP.get(row["color"]) if row["color"] else None
                is_nv = map_vintage_config(row["vintage_config"])

                wine_name = row["wine_name"] or row.get("display_name") or "Unknown"
                wine_slug = slugify(f"{row['_producer_key']}-{wine_name}-{row['lwin']}")

                wine_id = str(uuid.uuid4())
                wine_inserts.append({
                    "id": wine_id,
                    "slug": wine_slug,
                    "name": wine_name,
                    "name_normalized": normalize(wine_name),
                    "producer_id": producer_id,
                    "country_id": row["_country_id"],
                    "region_id": row["_region"]["id"] if row["_region"] else None,
                    "appellation_id": row["_appellation"]["id"] if row["_appellation"] else None,
                    "color": color,
                    "wine_type": type_info["wine_type"],
                    "effervescence": type_info["effervescence"],
                    "lwin": row["lwin"],
                    "first_vintage_year": row["first_vintage"],
                })

                existing_lwins.add(row["lwin"])
                wines_created += 1

                # Classification
                if row["_classification"] and row["_classification"].get("level"):
                    class_inserts.append({
                        "id": str(uuid.uuid4()),
                        "entity_type": "wine",
                        "entity_id": wine_id,
                        "classification_id": row["_classification"]["system"]["id"],
                        "classification_level_id": row["_classification"]["level"]["id"],
                    })
                    classifications_linked += 1

        # Insert batch
        if mode == "import" and producer_inserts:
            try:
                sb.table("producers").upsert(
                    producer_inserts, on_conflict="slug"
                ).execute()
            except Exception as e:
                print(f"  Batch {batch_num} producer error: {e}")
                errors += 1

        if mode == "import" and wine_inserts:
            for w in range(0, len(wine_inserts), 200):
                chunk = wine_inserts[w:w + 200]
                try:
                    sb.table("wines").upsert(
                        chunk, on_conflict="slug"
                    ).execute()
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

        total_batches = (len(producer_entries) + PRODUCER_BATCH - 1) // PRODUCER_BATCH
        if batch_num % 20 == 0 or b + PRODUCER_BATCH >= len(producer_entries):
            print(f"  Batch {batch_num}/{total_batches} -- "
                  f"producers: +{len(producer_inserts)}, wines: +{len(wine_inserts)}")

    print(f"\n{'=' * 51}")
    print("  IMPORT COMPLETE")
    print(f"{'=' * 51}\n")
    print(f"Producers created: {producers_created} (skipped {producers_skipped} existing)")
    print(f"Wines created:     {wines_created} (skipped {wines_skipped} existing LWIN)")
    print(f"Classifications:   {classifications_linked}")
    print(f"Errors:            {errors}")


if __name__ == "__main__":
    main()
