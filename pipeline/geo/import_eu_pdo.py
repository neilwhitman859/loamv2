"""
Imports European wine PDO boundaries from the Eurac Research EU Wine
Geospatial Inventory (EWaGI) into the Loam database.

Three phases:
  Phase 0: Load & join GeoPackage (boundaries) + CSV (metadata)
  Phase 1: Match PDOs against existing appellations or create new ones
  Phase 2: Import boundary polygons (reprojected from EPSG:3035 to WGS84)

Requires: fiona, pyproj, shapely

Usage:
  python -m pipeline.geo.import_eu_pdo [--dry-run] [--country FR] [--boundaries-only]
"""

import sys
import re
import csv
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify as base_slugify

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# --- Greek transliteration ---
GREEK_TO_LATIN = {
    "\u0391": "A", "\u0392": "V", "\u0393": "G", "\u0394": "D", "\u0395": "E",
    "\u0396": "Z", "\u0397": "I", "\u0398": "Th", "\u0399": "I", "\u039a": "K",
    "\u039b": "L", "\u039c": "M", "\u039d": "N", "\u039e": "X", "\u039f": "O",
    "\u03a0": "P", "\u03a1": "R", "\u03a3": "S", "\u03a4": "T", "\u03a5": "Y",
    "\u03a6": "F", "\u03a7": "Ch", "\u03a8": "Ps", "\u03a9": "O",
    "\u03b1": "a", "\u03b2": "v", "\u03b3": "g", "\u03b4": "d", "\u03b5": "e",
    "\u03b6": "z", "\u03b7": "i", "\u03b8": "th", "\u03b9": "i", "\u03ba": "k",
    "\u03bb": "l", "\u03bc": "m", "\u03bd": "n", "\u03be": "x", "\u03bf": "o",
    "\u03c0": "p", "\u03c1": "r", "\u03c3": "s", "\u03c2": "s", "\u03c4": "t",
    "\u03c5": "y", "\u03c6": "f", "\u03c7": "ch", "\u03c8": "ps", "\u03c9": "o",
}

GREEK_NAME_MAP = {
    "\u0391\u03b3\u03c7\u03af\u03b1\u03bb\u03bf\u03c2": "Anchialos",
    "\u0391\u03bc\u03cd\u03bd\u03c4\u03b1\u03b9\u03bf": "Amyntaio",
    "\u0391\u03c1\u03c7\u03ac\u03bd\u03b5\u03c2": "Archanes",
    "\u0393\u03bf\u03c5\u03bc\u03ad\u03bd\u03b9\u03c3\u03c3\u03b1": "Goumenissa",
    "\u0394\u03b1\u03c6\u03bd\u03ad\u03c2": "Dafnes",
    "\u0396\u03af\u03c4\u03c3\u03b1": "Zitsa",
    "\u039b\u03ae\u03bc\u03bd\u03bf\u03c2": "Lemnos",
    "\u039c\u03b1\u03bd\u03c4\u03b9\u03bd\u03b5\u03af\u03b1": "Mantinia",
    "\u039d\u03ac\u03bf\u03c5\u03c3\u03b1": "Naoussa",
    "\u039d\u03b5\u03bc\u03ad\u03b1": "Nemea",
    "\u03a0\u03ac\u03c1\u03bf\u03c2": "Paros",
    "\u03a0\u03ac\u03c4\u03c1\u03b1": "Patra",
    "\u03a0\u03b5\u03b6\u03ac": "Peza",
    "\u03a1\u03b1\u03c8\u03ac\u03bd\u03b7": "Rapsani",
    "\u03a1\u03cc\u03b4\u03bf\u03c2": "Rhodes",
    "\u03a3\u03ac\u03bc\u03bf\u03c2": "Samos",
    "\u03a3\u03b1\u03bd\u03c4\u03bf\u03c1\u03af\u03bd\u03b7": "Santorini",
    "\u03a3\u03b7\u03c4\u03b5\u03af\u03b1": "Sitia",
}

# --- Cyrillic transliteration ---
CYRILLIC_TO_LATIN = {
    "\u0410": "A", "\u0411": "B", "\u0412": "V", "\u0413": "G", "\u0414": "D",
    "\u0415": "E", "\u0416": "Zh", "\u0417": "Z", "\u0418": "I", "\u0419": "Y",
    "\u041a": "K", "\u041b": "L", "\u041c": "M", "\u041d": "N", "\u041e": "O",
    "\u041f": "P", "\u0420": "R", "\u0421": "S", "\u0422": "T", "\u0423": "U",
    "\u0424": "F", "\u0425": "Kh", "\u0426": "Ts", "\u0427": "Ch", "\u0428": "Sh",
    "\u0429": "Sht", "\u042a": "a", "\u042c": "", "\u042e": "Yu", "\u042f": "Ya",
    "\u0430": "a", "\u0431": "b", "\u0432": "v", "\u0433": "g", "\u0434": "d",
    "\u0435": "e", "\u0436": "zh", "\u0437": "z", "\u0438": "i", "\u0439": "y",
    "\u043a": "k", "\u043b": "l", "\u043c": "m", "\u043d": "n", "\u043e": "o",
    "\u043f": "p", "\u0440": "r", "\u0441": "s", "\u0442": "t", "\u0443": "u",
    "\u0444": "f", "\u0445": "kh", "\u0446": "ts", "\u0447": "ch", "\u0448": "sh",
    "\u0449": "sht", "\u044a": "a", "\u044c": "", "\u044e": "yu", "\u044f": "ya",
}

COUNTRY_DESIGNATION = {
    "FR": "AOC", "IT": "DOC", "ES": "DO", "PT": "DOC", "DE": "Qualit\u00e4tswein",
    "AT": "DAC", "HU": "OEM", "RO": "DOC", "CZ": "VOC", "GR": "PDO",
    "HR": "PDO", "SI": "PDO", "SK": "PDO", "BG": "PDO", "BE": "PDO",
    "NL": "PDO", "GB": "PDO", "MT": "PDO", "CY": "PDO", "DK": "PDO",
}


def transliterate_greek(s: str) -> str:
    return "".join(GREEK_TO_LATIN.get(c, c) for c in s)


def transliterate_cyrillic(s: str) -> str:
    return "".join(CYRILLIC_TO_LATIN.get(c, c) for c in s)


def transliterate_all(s: str) -> str:
    return transliterate_cyrillic(transliterate_greek(s))


def eu_slugify(s: str) -> str:
    import unicodedata
    t = transliterate_all(s).lower()
    t = unicodedata.normalize("NFD", t)
    t = re.sub(r"[\u0300-\u036f]", "", t)
    t = re.sub(r"['\u2019]", "", t)
    t = re.sub(r"[^a-z0-9]+", "-", t)
    return t.strip("-")


def normalize_name(s: str) -> str:
    import unicodedata
    t = transliterate_all(s)
    t = unicodedata.normalize("NFD", t)
    t = re.sub(r"[\u0300-\u036f]", "", t)
    return t.lower().strip()


def simplify_precision(geojson: dict) -> dict:
    def round_coords(coords):
        if isinstance(coords[0], (int, float)):
            return [round(coords[0], 5), round(coords[1], 5)]
        return [round_coords(c) for c in coords]
    return {"type": geojson["type"], "coordinates": round_coords(geojson["coordinates"])}


def compute_centroid(geojson: dict) -> dict | None:
    total_lat = 0.0
    total_lng = 0.0
    count = 0

    def extract(coords):
        nonlocal total_lat, total_lng, count
        if isinstance(coords[0], (int, float)):
            total_lng += coords[0]
            total_lat += coords[1]
            count += 1
        else:
            for c in coords:
                extract(c)

    extract(geojson["coordinates"])
    return {"lat": total_lat / count, "lng": total_lng / count} if count > 0 else None


def main():
    parser = argparse.ArgumentParser(description="Import EU PDO boundaries from Eurac Research")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--country", type=str, default=None)
    parser.add_argument("--boundaries-only", action="store_true")
    args = parser.parse_args()

    try:
        import fiona
        from pyproj import Transformer
    except ImportError:
        print("ERROR: This script requires fiona and pyproj.")
        print("Install with: pip install fiona pyproj")
        sys.exit(1)

    sb = get_supabase()
    print("=== EU PDO Boundary Import ===")
    if args.dry_run:
        print("[DRY RUN MODE]")
    if args.country:
        print(f"[COUNTRY FILTER: {args.country}]")

    # Phase 0: Load data
    print("\n--- Phase 0: Loading data ---")

    gpkg_path = PROJECT_ROOT / "data" / "geo" / "EU_PDO.gpkg"
    csv_path = PROJECT_ROOT / "data" / "geo" / "PDO_EU_id.csv"

    # Load GeoPackage using fiona
    transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True)
    gpkg_features = {}
    with fiona.open(str(gpkg_path)) as src:
        for feature in src:
            pdo_id = feature["properties"].get("PDOid")
            if pdo_id:
                geom = feature["geometry"]
                gpkg_features[str(pdo_id)] = geom
    print(f"  GeoPackage: {len(gpkg_features)} features loaded")

    # Load CSV metadata
    csv_rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_rows.append(row)
    print(f"  CSV: {len(csv_rows)} rows loaded")

    # Build PDO lookup
    pdo_map = {}
    for row in csv_rows:
        pdo_id = row.get("PDOid", "").strip()
        if not pdo_id:
            continue
        if args.country and row.get("Country") != args.country:
            continue
        if pdo_id not in pdo_map:
            max_yield_hl = row.get("Maximum_yield_hl", "na")
            pdo_map[pdo_id] = {
                "pdo_id": pdo_id,
                "country": row.get("Country", ""),
                "name": row.get("PDOnam", ""),
                "registration": row.get("Registration", ""),
                "varieties_oiv": row.get("Varieties_OIV", ""),
                "max_yield_hl": float(max_yield_hl) if max_yield_hl != "na" else None,
                "pdo_info_url": row.get("PDOinfo", ""),
                "shape_geom": gpkg_features.get(pdo_id),
            }
    print(f"  Joined: {len(pdo_map)} PDOs to process")

    def reproject_geometry(geom: dict) -> dict:
        """Reproject GeoJSON geometry from EPSG:3035 to WGS84."""
        def reproject_coords(coords):
            if isinstance(coords[0], (int, float)):
                x, y = transformer.transform(coords[0], coords[1])
                return [x, y]
            return [reproject_coords(c) for c in coords]
        return {"type": geom["type"], "coordinates": reproject_coords(geom["coordinates"])}

    # Load DB reference data
    print("\n--- Loading DB reference data ---")
    countries_result = sb.table("countries").select("id, name, iso_code").execute()
    country_by_iso = {c["iso_code"]: c for c in countries_result.data}

    catch_all_result = sb.table("regions").select("id, name, country_id").eq(
        "is_catch_all", True
    ).execute()
    catch_all_by_country = {r["country_id"]: r for r in catch_all_result.data}

    existing_apps = []
    offset = 0
    while True:
        result = sb.table("appellations").select(
            "id, name, slug, country_id, region_id, designation_type, max_yield_hl_ha, allowed_grapes_description, regulatory_url"
        ).range(offset, offset + 999).execute()
        existing_apps.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_apps)} existing appellations loaded")

    app_lookup = {}
    app_slug_lookup = {}
    for app in existing_apps:
        key = f"{app['country_id']}::{normalize_name(app['name'])}"
        app_lookup[key] = app
        slug_key = f"{app['country_id']}::{eu_slugify(app['name'])}"
        app_slug_lookup[slug_key] = app

    # Phase 1: Match/Create Appellations
    stats = {"matched": 0, "created": 0, "skipped": 0, "boundaries_updated": 0, "enriched": 0, "errors": 0}
    pdo_to_app_id = {}

    if not args.boundaries_only:
        print("\n--- Phase 1: Matching appellations ---")

    for pdo_id, pdo in pdo_map.items():
        country = country_by_iso.get(pdo["country"])
        if not country:
            stats["skipped"] += 1
            continue

        resolved_name = GREEK_NAME_MAP.get(pdo["name"])
        if not resolved_name:
            if re.search(r"[\u0410-\u044f]", pdo["name"]):
                resolved_name = transliterate_cyrillic(pdo["name"])
            else:
                resolved_name = pdo["name"]

        norm_name = normalize_name(resolved_name)
        lookup_key = f"{country['id']}::{norm_name}"
        slug_key = f"{country['id']}::{eu_slugify(resolved_name)}"
        orig_norm = normalize_name(pdo["name"])
        orig_key = f"{country['id']}::{orig_norm}"
        orig_slug_key = f"{country['id']}::{eu_slugify(pdo['name'])}"

        app = (app_lookup.get(lookup_key) or app_slug_lookup.get(slug_key)
               or app_lookup.get(orig_key) or app_slug_lookup.get(orig_slug_key))

        if app:
            pdo_to_app_id[pdo_id] = app["id"]
            stats["matched"] += 1

            if not args.boundaries_only and not args.dry_run:
                updates = {}
                if not app.get("max_yield_hl_ha") and pdo["max_yield_hl"]:
                    updates["max_yield_hl_ha"] = pdo["max_yield_hl"]
                if not app.get("allowed_grapes_description") and pdo["varieties_oiv"] and pdo["varieties_oiv"] != "na":
                    updates["allowed_grapes_description"] = ", ".join(
                        re.sub(r" [BNRG]r?s?g?$", "", v).strip()
                        for v in pdo["varieties_oiv"].split("/") if v.strip()
                    )
                if not app.get("regulatory_url") and pdo["pdo_info_url"]:
                    updates["regulatory_url"] = pdo["pdo_info_url"]
                if not app.get("designation_type"):
                    updates["designation_type"] = COUNTRY_DESIGNATION.get(pdo["country"], "PDO")
                if updates:
                    sb.table("appellations").update(updates).eq("id", app["id"]).execute()
                    stats["enriched"] += 1

        elif not args.boundaries_only:
            catch_all = catch_all_by_country.get(country["id"])
            if not catch_all:
                stats["skipped"] += 1
                continue

            lat, lng = None, None
            if pdo["shape_geom"]:
                try:
                    wgs84 = reproject_geometry(pdo["shape_geom"])
                    centroid = compute_centroid(wgs84)
                    if centroid:
                        lat, lng = centroid["lat"], centroid["lng"]
                except Exception:
                    pass

            hemisphere = "south" if lat and lat < 0 else "north"
            designation_type = COUNTRY_DESIGNATION.get(pdo["country"], "PDO")
            grapes_desc = None
            if pdo["varieties_oiv"] and pdo["varieties_oiv"] != "na":
                grapes_desc = ", ".join(
                    re.sub(r" [BNRG]r?s?g?$", "", v).strip()
                    for v in pdo["varieties_oiv"].split("/") if v.strip()
                )

            new_app = {
                "name": resolved_name, "slug": eu_slugify(resolved_name),
                "country_id": country["id"], "region_id": catch_all["id"],
                "designation_type": designation_type,
                "latitude": round(lat, 5) if lat else None,
                "longitude": round(lng, 5) if lng else None,
                "hemisphere": hemisphere,
                "max_yield_hl_ha": pdo["max_yield_hl"],
                "allowed_grapes_description": grapes_desc,
                "regulatory_url": pdo["pdo_info_url"] or None,
            }

            if args.dry_run:
                print(f"  [DRY] Would create: {resolved_name} ({country['name']})")
                stats["created"] += 1
                continue

            try:
                result = sb.table("appellations").insert(new_app).select("id").single().execute()
                pdo_to_app_id[pdo_id] = result.data["id"]
                app_lookup[lookup_key] = {"id": result.data["id"], **new_app}
                stats["created"] += 1
            except Exception as e:
                if "23505" in str(e):
                    new_app["slug"] = eu_slugify(f"{resolved_name}-{country['name']}")
                    try:
                        result = sb.table("appellations").insert(new_app).select("id").single().execute()
                        pdo_to_app_id[pdo_id] = result.data["id"]
                        stats["created"] += 1
                    except Exception as e2:
                        print(f"  [ERROR] Creating {pdo['name']}: {e2}")
                        stats["errors"] += 1
                else:
                    print(f"  [ERROR] Creating {pdo['name']}: {e}")
                    stats["errors"] += 1

    if not args.boundaries_only:
        print(f"\n  Phase 1: {stats['matched']} matched, {stats['created']} created, "
              f"{stats['enriched']} enriched, {stats['errors']} errors")

    # Phase 2: Import boundaries
    print("\n--- Phase 2: Importing boundaries ---")
    processed = 0

    for pdo_id, pdo in pdo_map.items():
        app_id = pdo_to_app_id.get(pdo_id)
        if not app_id:
            country = country_by_iso.get(pdo["country"])
            if country:
                resolved = GREEK_NAME_MAP.get(pdo["name"], pdo["name"])
                key = f"{country['id']}::{normalize_name(resolved)}"
                slug_key = f"{country['id']}::{eu_slugify(resolved)}"
                existing = app_lookup.get(key) or app_slug_lookup.get(slug_key)
                if existing:
                    app_id = existing["id"]
                    pdo_to_app_id[pdo_id] = app_id

        if not app_id:
            continue
        if not pdo["shape_geom"]:
            continue

        try:
            wgs84 = reproject_geometry(pdo["shape_geom"])
            simplified = simplify_precision(wgs84)

            if args.dry_run:
                centroid = compute_centroid(simplified)
                print(f"  [DRY] Would import boundary for {pdo['name']} ({simplified['type']})")
                stats["boundaries_updated"] += 1
                processed += 1
                continue

            sb.rpc("upsert_appellation_boundary", {
                "p_appellation_id": app_id,
                "p_geojson": json.dumps(simplified),
                "p_source_id": f"eu-pdo/{pdo_id}",
                "p_confidence": "approximate",
            }).execute()
            stats["boundaries_updated"] += 1
            processed += 1

            if processed % 50 == 0:
                print(f"  Processed {processed}/{len(pdo_map)} boundaries...")
            if processed % 20 == 0:
                time.sleep(0.1)

        except Exception as e:
            print(f"  [ERROR] {pdo['name']}: {e}")
            stats["errors"] += 1

    print(f"\n=== Import Complete ===")
    print(f"  Matched: {stats['matched']}")
    print(f"  Created: {stats['created']}")
    print(f"  Enriched: {stats['enriched']}")
    print(f"  Boundaries: {stats['boundaries_updated']}")
    print(f"  Errors: {stats['errors']}")


if __name__ == "__main__":
    main()
