#!/usr/bin/env python3
"""
Kermit Lynch bulk catalog import.

Imports ~1,468 wines from ~193 growers as extracted from kermitlynch.com API.
Multi-producer portfolio import -- tests importers, bulk producer creation,
grape/region/appellation resolution, farming certifications.

Usage:
    python -m pipeline.promote.import_kl [--dry-run] [--replace]
"""

import argparse
import json
import re
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize, slugify

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"

FARMING_MAP = {
    "Biodynamic (certified)": "Biodynamic",
    "Biodynamic (practicing)": "Biodynamic",
    "Organic (certified)": "Organic",
    "Organic (practicing)": "Organic",
    "Sustainable": None,
    "Lutte Raisonnée": None,
    "Traditional": None,
    "N/A": None,
    "Haute Valeur Environnementale (certified)": "HVE",
}

REGION_MAP = {
    "Alsace": "Alsace",
    "Alto Adige": "Alto Adige",
    "Beaujolais": "Beaujolais",
    "Bordeaux": "Bordeaux",
    "Burgundy": "Burgundy",
    "Campania": "Campania",
    "Champagne": "Champagne",
    "Corsica": "Corsica",
    "Emilia-Romagna": "Emilia-Romagna",
    "Friuli": "Friuli-Venezia Giulia",
    "Jura": "Jura",
    "Languedoc-Roussillon": "Languedoc",
    "Liguria": "Liguria",
    "Loire": "Loire Valley",
    "Marche": "Marche",
    "Molise": "Molise",
    "Northern Rhône": "Northern Rhône",
    "Piedmont": "Piemonte",
    "Provence": "Provence",
    "Puglia": "Puglia",
    "Sardinia": "Sardinia",
    "Savoie, Bugey, Hautes-Alpes": "Savoie",
    "Sicily": "Sicily",
    "Southern Rhône": "Southern Rhône",
    "Southwest": "Southwest France",
    "Tuscany": "Tuscany",
    "Valle d'Aosta": "Valle d'Aosta",
    "Veneto": "Veneto",
}


def infer_color(wine_type: str | None) -> str | None:
    if not wine_type:
        return None
    t = wine_type.lower()
    if t == "red":
        return "red"
    if t == "white":
        return "white"
    if t == "rosé":
        return "rose"
    return None


def parse_blend(blend_str: str | None) -> list[dict]:
    if not blend_str or blend_str == "N/A":
        return []
    if re.match(r"^varies", blend_str, re.IGNORECASE):
        return []
    if re.match(r"^see below", blend_str, re.IGNORECASE):
        return []
    s = re.sub(r"^approximately\s+", "", blend_str, flags=re.IGNORECASE)
    s = re.sub(r"\s+and\s+", ", ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*&\s*", ", ", s)
    s = re.sub(r"\([^)]*\)", "", s)
    parts = [p.strip() for p in re.split(r"[,/]", s) if p.strip()]
    grapes = []
    for part in parts:
        if re.match(r"^\d+$", part):
            continue
        if re.match(r"^see\s", part, re.IGNORECASE):
            continue
        pct_match = re.match(r"^(\d+)%?\s+(.+)", part)
        if pct_match:
            name = pct_match.group(2).strip()
            if len(name) > 1:
                grapes.append({"name": name, "percentage": int(pct_match.group(1))})
        else:
            name = part.strip()
            if len(name) > 2 and not re.match(r"^\d", name):
                grapes.append({"name": name, "percentage": None})
    return grapes


def main():
    parser = argparse.ArgumentParser(description="Kermit Lynch bulk import")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    dry = args.dry_run

    print("=" * 60)
    print("  KERMIT LYNCH BULK IMPORT")
    mode_str = "(DRY RUN)" if dry else "(REPLACE MODE)" if args.replace else "(INSERT MODE)"
    print(f"  {mode_str}")
    print("=" * 60 + "\n")

    # Load catalog
    catalog = json.loads((DATA_DIR / "kermit_lynch_catalog.json").read_text(encoding="utf-8"))
    print(f"Catalog: {len(catalog['wines'])} wines, {len(catalog['growers'])} growers\n")

    # Load reference data
    print("Loading reference data...")

    countries = fetch_all("countries", "id,name,iso_code")
    country_map: dict[str, str] = {}
    for c in countries:
        country_map[c["name"].lower()] = c["id"]
        if c.get("iso_code"):
            country_map[c["iso_code"].lower()] = c["id"]

    regions = fetch_all("regions", "id,name,country_id,is_catch_all")
    region_map: dict[str, dict] = {}
    for r in regions:
        region_map[r["name"].lower()] = r
        region_map[f"{r['name'].lower()}|{r['country_id']}"] = r

    appellations = fetch_all("appellations", "id,name,country_id,region_id")
    appellation_map: dict[str, dict] = {}
    for a in appellations:
        appellation_map[a["name"].lower()] = a
        appellation_map[normalize(a["name"])] = a

    aliases = fetch_all("appellation_aliases", "appellation_id,alias_normalized")
    for al in aliases:
        app = next((a for a in appellations if a["id"] == al["appellation_id"]), None)
        if app and al["alias_normalized"] not in appellation_map:
            appellation_map[al["alias_normalized"]] = app
    print(f"  Appellation aliases loaded: {len(aliases)}")

    grapes = fetch_all("grapes", "id,name,display_name,color")
    grape_map: dict[str, dict] = {}
    for g in grapes:
        if g.get("display_name"):
            grape_map[g["display_name"].lower()] = g
        grape_map[g["name"].lower()] = g

    synonyms = fetch_all("grape_synonyms", "grape_id,synonym")
    syn_map = {s["synonym"].lower(): s["grape_id"] for s in synonyms}

    farming_certs = fetch_all("farming_certifications", "id,name")
    farming_cert_map = {f["name"].lower(): f["id"] for f in farming_certs}

    source_types = fetch_all("source_types", "id,slug")
    source_type_map = {s["slug"]: s["id"] for s in source_types}
    importer_source_id = source_type_map.get("importer-website") or source_type_map.get("producer-website")

    varietal_categories = fetch_all("varietal_categories", "id,name,slug")
    vc_map: dict[str, str] = {}
    for vc in varietal_categories:
        vc_map[vc["name"].lower()] = vc["id"]
        vc_map[vc["slug"]] = vc["id"]

    print(f"  Countries: {len(countries)}, Regions: {len(regions)}")
    print(f"  Appellations: {len(appellations)}, Grapes: {len(grapes)}")
    print(f"  Farming certs: {len(farming_certs)}, Synonyms: {len(synonyms)}\n")

    def resolve_grape(name: str) -> str | None:
        lower = name.lower().strip()
        g = grape_map.get(lower)
        if g:
            return g["id"]
        syn_id = syn_map.get(lower)
        if syn_id:
            return syn_id
        stripped = normalize(name)
        if stripped != lower:
            g2 = grape_map.get(stripped)
            if g2:
                return g2["id"]
            syn_id2 = syn_map.get(stripped)
            if syn_id2:
                return syn_id2
        return None

    stats = {
        "producers": 0, "wines": 0, "wine_grapes": 0,
        "farming_certs": 0, "importer_links": 0,
        "warnings": [], "region_misses": set(), "grape_misses": set(),
        "appellation_hits": 0, "appellation_misses": 0,
    }

    # Create/find Kermit Lynch as importer
    print("Setting up Kermit Lynch importer...")
    result = sb.table("importers").select("id").eq("slug", "kermit-lynch").execute()
    if result.data:
        importer_id = result.data[0]["id"]
        print(f"  Importer exists: {importer_id}")
    elif not dry:
        importer_id = str(uuid.uuid4())
        sb.table("importers").insert({
            "id": importer_id,
            "name": "Kermit Lynch Wine Merchant",
            "slug": "kermit-lynch",
            "country_id": country_map.get("united states"),
            "website_url": "https://kermitlynch.com",
            "metadata": {"founded": 1972, "location": "Berkeley, CA", "type": "importer-retailer"},
        }).execute()
        print(f"  Created importer: Kermit Lynch ({importer_id})")
    else:
        print("  [DRY RUN] Would create importer: Kermit Lynch")
        importer_id = "dry-run-id"

    # Import growers as producers
    print("\nImporting growers as producers...\n")
    producer_id_map: dict[str, str] = {}

    for grower in catalog["growers"]:
        slug = slugify(grower["name"])
        country_id = country_map.get((grower.get("country") or "").lower())
        if not country_id:
            stats["warnings"].append(f"Country not found for grower: {grower['name']} ({grower.get('country')})")
            continue

        loam_region = REGION_MAP.get(grower.get("region", ""), grower.get("region"))
        region_data = None
        if loam_region:
            region_data = region_map.get(f"{loam_region.lower()}|{country_id}") or region_map.get(loam_region.lower())
        region_id = region_data["id"] if region_data else None
        if not region_id and grower.get("region"):
            stats["region_misses"].add(grower["region"])

        # Check existing
        result = sb.table("producers").select("id").eq("slug", slug).execute()
        if result.data:
            producer_id_map[grower["kl_id"]] = result.data[0]["id"]
            continue

        producer_id = str(uuid.uuid4())
        producer_id_map[grower["kl_id"]] = producer_id

        founded_year = None
        fy = grower.get("founded_year")
        if isinstance(fy, int):
            founded_year = fy
        elif isinstance(fy, str):
            ym = re.search(r"\d{4}", str(fy))
            if ym:
                founded_year = int(ym.group(0))

        row = {
            "id": producer_id,
            "slug": slug,
            "name": grower["name"],
            "name_normalized": normalize(grower["name"]),
            "country_id": country_id,
            "region_id": region_id,
            "website_url": grower.get("website"),
            "year_established": founded_year,
            "producer_type": "estate",
            "philosophy": (grower.get("viticulture_notes") or "")[:1000] or None,
            "metadata": {
                "kl_id": grower["kl_id"],
                "kl_slug": grower.get("slug"),
                "winemaker": grower.get("winemaker"),
                "annual_production": grower.get("annual_production"),
                "location": grower.get("location"),
                "source": "kermitlynch.com",
            },
        }

        if not dry:
            try:
                sb.table("producers").insert(row).execute()
            except Exception as e:
                stats["warnings"].append(f"Producer insert error for \"{grower['name']}\": {e}")
                continue

        stats["producers"] += 1

        # Link to KL as importer
        if not dry and importer_id != "dry-run-id":
            try:
                sb.table("producer_importers").insert({
                    "producer_id": producer_id,
                    "importer_id": importer_id,
                }).execute()
                stats["importer_links"] += 1
            except Exception as e:
                if "duplicate" not in str(e):
                    stats["warnings"].append(f"Importer link error for \"{grower['name']}\": {e}")

        # Farming certifications
        for farm_name in (grower.get("farming") or []):
            loam_name = FARMING_MAP.get(farm_name)
            if not loam_name:
                continue
            cert_id = farming_cert_map.get(loam_name.lower())
            if not cert_id:
                stats["warnings"].append(f"Farming cert not found: {loam_name}")
                continue
            if not dry:
                try:
                    sb.table("producer_farming_certifications").insert({
                        "producer_id": producer_id,
                        "farming_certification_id": cert_id,
                        "source_id": importer_source_id,
                    }).execute()
                    stats["farming_certs"] += 1
                except Exception as e:
                    if "duplicate" not in str(e):
                        stats["warnings"].append(f"Farming cert error: {e}")

        if stats["producers"] % 20 == 0:
            print(f"  Created {stats['producers']} producers...")

    print(f"\nCreated {stats['producers']} producers, {stats['importer_links']} importer links, {stats['farming_certs']} farming certs")

    # Import wines
    print("\nImporting wines...\n")
    existing_slugs: set[str] = set()

    for wine in catalog["wines"]:
        producer_id = producer_id_map.get(wine.get("grower_kl_id"))
        if not producer_id:
            stats["warnings"].append(f"No producer for wine: {wine.get('wine_name')} (grower KL ID: {wine.get('grower_kl_id')})")
            continue

        country_id = country_map.get((wine.get("country") or "").lower())
        if not country_id:
            continue

        loam_region = REGION_MAP.get(wine.get("region", ""), wine.get("region"))
        region_data = None
        if loam_region:
            region_data = region_map.get(f"{loam_region.lower()}|{country_id}") or region_map.get(loam_region.lower())
        region_id = region_data["id"] if region_data else None

        color = infer_color(wine.get("wine_type"))
        wt = wine.get("wine_type", "")
        wine_type = "dessert" if wt == "Dessert" else "sparkling" if wt == "Sparkling" else "table"
        effervescence = "sparkling" if wt == "Sparkling" else "still"

        slug = slugify(f"{wine.get('grower_name', '')} {wine.get('wine_name', '')}")
        if slug in existing_slugs:
            slug = f"{slug}-{wine.get('sku', '').lower()}"
        existing_slugs.add(slug)

        # Check existing
        result = sb.table("wines").select("id").eq("slug", slug).execute()
        if result.data:
            wine_id = result.data[0]["id"]
        else:
            wine_id = str(uuid.uuid4())
            wine_name = wine.get("wine_name", "")

            # 5-strategy appellation resolution
            appellation_id = None
            app = appellation_map.get(normalize(wine_name))

            if not app:
                before_quote = re.split(r'["\u201c\u201d\u201e]', wine_name)[0].strip()
                app = appellation_map.get(normalize(before_quote))

            if not app:
                no_color = re.sub(r'["\u201c\u201d\u201e].*$', "", wine_name).strip()
                no_color = re.sub(r"\s+(Rouge|Blanc|Ros[eé]|Rosato|Rosso|Bianco|Clairet)\s*$", "", no_color, flags=re.IGNORECASE).strip()
                app = appellation_map.get(normalize(no_color))

            if not app:
                no_cru = re.sub(r'["\u201c\u201d\u201e].*$', "", wine_name).strip()
                no_cru = re.sub(r"\s+(Rouge|Blanc|Ros[eé]|Rosato|Rosso|Bianco|Clairet)\s*$", "", no_cru, flags=re.IGNORECASE)
                no_cru = re.sub(r"\s+(1er\s+Cru|Premier\s+Cru|Grand\s+Cru|Cru)\b.*$", "", no_cru, flags=re.IGNORECASE).strip()
                app = appellation_map.get(normalize(no_cru))

            if not app:
                words = re.sub(r'["\u201c\u201d\u201e].*$', "", wine_name).strip().split()
                for length in range(len(words), 0, -1):
                    candidate = " ".join(words[:length])
                    app = appellation_map.get(normalize(candidate))
                    if app:
                        break

            if app:
                appellation_id = app["id"]
                stats["appellation_hits"] += 1
            else:
                stats["appellation_misses"] += 1

            # Varietal category resolution
            varietal_category_id = None
            grape_entries = parse_blend(wine.get("blend"))
            if len(grape_entries) == 1:
                gn = grape_entries[0]["name"]
                varietal_category_id = vc_map.get(gn.lower()) or vc_map.get(slugify(gn))
            if not varietal_category_id and grape_entries:
                if color == "red":
                    varietal_category_id = vc_map.get("red-blend") or vc_map.get("red blend")
                elif color == "white":
                    varietal_category_id = vc_map.get("white-blend") or vc_map.get("white blend")
                elif color == "rose":
                    varietal_category_id = vc_map.get("rosé") or vc_map.get("rose")
                else:
                    varietal_category_id = vc_map.get("red-blend")
            if not varietal_category_id:
                if wine_type == "sparkling":
                    varietal_category_id = vc_map.get("sparkling-blend") or vc_map.get("champagne-blend")
                elif color == "rose":
                    varietal_category_id = vc_map.get("rosé blend") or vc_map.get("rose-blend")
                elif color == "white":
                    varietal_category_id = vc_map.get("white-blend") or vc_map.get("white blend")
                else:
                    varietal_category_id = vc_map.get("red-blend") or vc_map.get("red blend")

            metadata = {
                "kl_id": wine.get("kl_id"),
                "kl_sku": wine.get("sku"),
                "soil": wine.get("soil"),
                "vine_age": wine.get("vine_age"),
                "vineyard_area": wine.get("vineyard_area"),
                "vinification": wine.get("vinification"),
                "source": "kermitlynch.com",
            }

            row = {
                "id": wine_id,
                "slug": slug,
                "name": wine_name,
                "name_normalized": normalize(wine_name),
                "producer_id": producer_id,
                "country_id": country_id,
                "region_id": region_id,
                "appellation_id": appellation_id,
                "color": color,
                "wine_type": wine_type,
                "effervescence": effervescence,
                "varietal_category_id": varietal_category_id,
                "metadata": metadata,
            }

            if not dry:
                try:
                    sb.table("wines").insert(row).execute()
                except Exception as e:
                    stats["warnings"].append(f"Wine insert error for \"{wine_name}\": {e}")
                    continue
            stats["wines"] += 1

        # Insert grape composition
        grape_entries_to_insert = parse_blend(wine.get("blend"))
        for entry in grape_entries_to_insert:
            grape_id = resolve_grape(entry["name"])
            if not grape_id:
                stats["grape_misses"].add(entry["name"])
                continue
            if not dry:
                try:
                    sb.table("wine_grapes").insert({
                        "wine_id": wine_id,
                        "grape_id": grape_id,
                        "percentage": entry["percentage"],
                    }).execute()
                    stats["wine_grapes"] += 1
                except Exception as e:
                    if "duplicate" not in str(e):
                        stats["warnings"].append(f"Wine grape error: {e}")
            else:
                stats["wine_grapes"] += 1

        if stats["wines"] % 100 == 0 and stats["wines"] > 0:
            print(f"  Imported {stats['wines']} wines, {stats['wine_grapes']} grape links...")

    # Summary
    print(f"\n{'=' * 60}")
    print("  IMPORT SUMMARY")
    print("=" * 60)
    print(f"  Producers created: {stats['producers']}")
    print(f"  Wines created: {stats['wines']}")
    print(f"  Wine grape links: {stats['wine_grapes']}")
    print(f"  Importer links: {stats['importer_links']}")
    print(f"  Farming certs: {stats['farming_certs']}")
    total_app = stats["appellation_hits"] + stats["appellation_misses"]
    pct = round(100 * stats["appellation_hits"] / total_app) if total_app else 0
    print(f"  Appellation resolved: {stats['appellation_hits']}/{total_app} ({pct}%)")
    if stats["region_misses"]:
        print(f"  Region misses: {', '.join(sorted(stats['region_misses']))}")
    if stats["grape_misses"]:
        misses = sorted(stats["grape_misses"])
        print(f"  Grape misses ({len(misses)}): {', '.join(misses[:20])}")
    if stats["warnings"]:
        unique = list(dict.fromkeys(stats["warnings"]))
        print(f"\n  Warnings ({len(unique)}):")
        for w in unique[:20]:
            print(f"    - {w}")
        if len(unique) > 20:
            print(f"    ... and {len(unique) - 20} more")
    print(f"\n{'=' * 60}\n")


if __name__ == "__main__":
    main()
