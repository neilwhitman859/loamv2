"""
Create wine, wine_vintage, and wine_grapes records from wine_candidates table.

Phase 1: Load reference data (producers, grapes, varietal_categories, countries, regions, region_name_mappings)
Phase 2: Process wine_candidates -> deduplicated wine records with FKs
Phase 3: Batch insert wines, wine_vintages, wine_grapes

Usage:
    python -m pipeline.vivino.create_wines_legacy --dry-run
    python -m pipeline.vivino.create_wines_legacy
"""

import sys
import argparse
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.normalize import normalize, slugify

# Elaborate -> Varietal Category name mapping
ELABORATE_BLEND_MAP = {
    "Assemblage/Bordeaux Red Blend": "Bordeaux Blend",
    "Assemblage/Rhone Red Blend": "Rhone Blend",
    "Assemblage/Valpolicella Red Blend": "Valpolicella Blend",
    "Assemblage/Champagne Blend": "Champagne Blend",
    "Assemblage/Portuguese Red Blend": "Douro Blend",
    "Assemblage/Port Blend": "Port",
    "Assemblage/Provence Rose Blend": "Provence Blend",
    "Assemblage/Meritage Red Blend": "Meritage",
    "Assemblage/Portuguese White Blend": "White Douro Blend",
    "Assemblage/Rioja Red Blend": "Rioja Blend",
    "Assemblage/Cava Blend": "Cava Blend",
    "Assemblage/Tuscan Red Blend": "Super Tuscan",
    "Assemblage/Priorat Red Blend": "Priorat Blend",
    "Assemblage/Chianti Red Blend": "Chianti Blend",
    "Assemblage/Meritage White Blend": "White Meritage",
    "Assemblage/Bourgogne Red Blend": None,
    "Assemblage/Bourgogne White Blend": None,
    "Assemblage/Soave White Blend": None,
    "Assemblage/Rioja White Blend": None,
}

WINE_TYPE_GENERIC_BLEND = {
    "Red": "Red Blend", "White": "White Blend", "Rose": "Rose Blend",
    "Sparkling": "Sparkling Blend", "Dessert": "Dessert Blend", "Dessert/Port": "Port",
}


def target_color_for_wine_type(wine_type, grape_color):
    mapping = {
        "Red": "red", "White": "white", "Rose": "rose",
        "Sparkling": grape_color or "white", "Dessert": grape_color or "white",
        "Dessert/Port": "red",
    }
    return mapping.get(wine_type, grape_color or "red")


def fetch_all(table, columns="*", batch_size=1000):
    sb = get_supabase()
    rows, offset = [], 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < batch_size: break
        offset += batch_size
    return rows


def resolve_grape(grape_name, grape_map):
    if not grape_name:
        return None
    cleaned = grape_name.strip('"')
    if not cleaned:
        return None
    return grape_map.get(cleaned.lower())


def resolve_varietal_category(elaborate, primary_grape, wine_type, grape_map, vcat_by_name, vcat_by_grape_color, vcat_by_grape):
    # 1. Named blend from elaborate field
    if elaborate and elaborate.startswith("Assemblage/"):
        mapped = ELABORATE_BLEND_MAP.get(elaborate)
        if mapped is not None:
            return vcat_by_name.get(mapped)
        if mapped is None and elaborate in ELABORATE_BLEND_MAP:
            return vcat_by_name.get(WINE_TYPE_GENERIC_BLEND.get(wine_type))
        if elaborate == "Assemblage/Blend":
            return vcat_by_name.get(WINE_TYPE_GENERIC_BLEND.get(wine_type))
        return vcat_by_name.get(WINE_TYPE_GENERIC_BLEND.get(wine_type))

    # 2. Single varietal
    grape = resolve_grape(primary_grape, grape_map)
    if grape:
        target_color = target_color_for_wine_type(wine_type, grape.get("color"))
        vc_id = vcat_by_grape_color.get(f"{grape['id']}|{target_color}")
        if vc_id:
            return vc_id
        if target_color == "rose":
            fallback = vcat_by_grape_color.get(f"{grape['id']}|{grape.get('color')}")
            if fallback:
                return fallback
        any_color = vcat_by_grape.get(grape["id"])
        if any_color:
            return any_color

    # 3. Fallback
    return vcat_by_name.get(WINE_TYPE_GENERIC_BLEND.get(wine_type)) or vcat_by_name.get("Red Blend")


def main():
    parser = argparse.ArgumentParser(description="Create wines from wine_candidates table")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    print("Phase 1: Loading reference data...")

    countries = fetch_all("countries", "id,name")
    country_map = {c["name"]: c["id"] for c in countries}
    print(f"  {len(countries)} countries")

    producers = fetch_all("producers", "id,name,name_normalized,country_id,slug")
    producer_map = {}
    producer_by_id = {}
    for p in producers:
        producer_map[f"{p['name']}|{p['country_id']}"] = {"id": p["id"], "slug": p.get("slug")}
        producer_by_id[p["id"]] = p
    print(f"  {len(producers)} producers")

    aliases = fetch_all("producer_aliases", "name,producer_id")
    alias_map = {}
    for a in aliases:
        prod = producer_by_id.get(a["producer_id"])
        if prod:
            alias_map[f"{a['name']}|{prod['country_id']}"] = {"id": prod["id"], "slug": prod.get("slug")}
    print(f"  {len(aliases)} producer aliases")

    grapes = fetch_all("grapes", "id,name,color")
    grape_map = {}
    for g in grapes:
        grape_map[g["name"].lower()] = {"id": g["id"], "color": g.get("color")}
    print(f"  {len(grapes)} grapes ({len(grape_map)} names)")

    vcats = fetch_all("varietal_categories", "id,name,color,type,grape_id")
    vcat_by_name = {v["name"]: v["id"] for v in vcats}
    vcat_by_grape_color = {}
    vcat_by_grape = {}
    for v in vcats:
        if v.get("grape_id"):
            vcat_by_grape_color[f"{v['grape_id']}|{v.get('color')}"] = v["id"]
            if v["grape_id"] not in vcat_by_grape:
                vcat_by_grape[v["grape_id"]] = v["id"]
    print(f"  {len(vcats)} varietal categories")

    regions = fetch_all("regions", "id,country_id,is_catch_all")
    catch_all_region = {r["country_id"]: r["id"] for r in regions if r.get("is_catch_all")}
    print(f"  {len(regions)} regions ({len(catch_all_region)} catch-alls)")

    try:
        rnm = fetch_all("region_name_mappings", "region_name,country,region_id,appellation_id")
        region_mapping = {f"{r['region_name']}|{r['country']}": {"region_id": r["region_id"], "appellation_id": r.get("appellation_id")} for r in rnm}
        print(f"  {len(rnm)} region name mappings")
    except Exception:
        region_mapping = {}

    # Phase 2: Process wine_candidates
    print("\nPhase 2: Processing wine_candidates...")
    all_candidates = fetch_all("wine_candidates", "id,producer_name,wine_name,wine_type,grapes,primary_grape,elaborate,abv,country,region_name,vintage_years")
    print(f"  {len(all_candidates)} wine_candidates fetched")

    unresolved_producers: dict[str, int] = {}
    resolved_count = 0

    def resolve_producer(producer_name, country_name):
        country_id = country_map.get(country_name)
        if not country_id:
            return None
        key = f"{producer_name}|{country_id}"
        return producer_map.get(key) or alias_map.get(key)

    wine_dedup: dict[str, dict] = {}

    for wc in all_candidates:
        country_id = country_map.get(wc.get("country"))
        if not country_id:
            k = f"COUNTRY:{wc.get('country')}"
            unresolved_producers[k] = unresolved_producers.get(k, 0) + 1
            continue

        prod = resolve_producer(wc["producer_name"], wc["country"])
        if not prod:
            u_key = f"{wc['producer_name']}|{wc['country']}"
            unresolved_producers[u_key] = unresolved_producers.get(u_key, 0) + 1
            continue
        resolved_count += 1

        wine_name_norm = normalize(wc["wine_name"])
        dedup_key = f"{prod['id']}|{wine_name_norm}"

        if dedup_key in wine_dedup:
            existing = wine_dedup[dedup_key]
            if wc.get("vintage_years"):
                vint_set = set(existing.get("vintage_years") or [])
                for v in wc["vintage_years"]:
                    vint_set.add(v)
                existing["vintage_years"] = sorted(vint_set)
            if wc.get("grapes") and (not existing.get("grapes") or len(wc["grapes"]) > len(existing["grapes"])):
                existing["grapes"] = wc["grapes"]
                existing["primary_grape"] = wc.get("primary_grape")
                existing["elaborate"] = wc.get("elaborate")
            existing["candidate_ids"].append(wc["id"])
        else:
            wine_dedup[dedup_key] = {
                "producer_id": prod["id"], "producer_slug": prod.get("slug"),
                "wine_name": wc["wine_name"], "wine_name_norm": wine_name_norm,
                "wine_type": wc.get("wine_type"), "grapes": wc.get("grapes") or [],
                "primary_grape": wc.get("primary_grape"), "elaborate": wc.get("elaborate"),
                "abv": wc.get("abv"), "country": wc.get("country"), "country_id": country_id,
                "region_name": wc.get("region_name"), "vintage_years": wc.get("vintage_years") or [],
                "candidate_ids": [wc["id"]],
            }

    print(f"  {resolved_count}/{len(all_candidates)} candidates resolved to producers")
    print(f"  {len(wine_dedup)} unique wines after dedup")

    if unresolved_producers:
        print(f"\n  Top unresolved producers:")
        for name, count in sorted(unresolved_producers.items(), key=lambda x: -x[1])[:15]:
            print(f"    {name} ({count} wines)")

    # Build records
    wines_list, vintages, wine_grapes = [], [], []
    slug_counts: dict[str, int] = {}
    unresolved_grapes: dict[str, int] = {}

    base_slugs = []
    for wd in wine_dedup.values():
        base_slug = f"{wd.get('producer_slug', '')}-{slugify(wd['wine_name'])}"[:120] or wd.get("producer_slug", "")
        base_slugs.append(base_slug)
        slug_counts[base_slug] = slug_counts.get(base_slug, 0) + 1

    slug_used: dict[str, int] = {}
    for idx, (_, wd) in enumerate(wine_dedup.items()):
        wine_id = str(uuid.uuid4())
        base_slug = base_slugs[idx]

        if slug_counts.get(base_slug, 0) > 1:
            num = slug_used.get(base_slug, 1)
            slug_used[base_slug] = num + 1
            slug = base_slug if num == 1 else f"{base_slug}-{num}"
        else:
            slug = base_slug

        # Resolve region
        region_id, appellation_id = None, None
        if wd.get("region_name"):
            rm = region_mapping.get(f"{wd['region_name']}|{wd['country']}")
            if rm:
                region_id = rm["region_id"]
                appellation_id = rm.get("appellation_id")
        if not region_id:
            region_id = catch_all_region.get(wd["country_id"])

        vcat_id = resolve_varietal_category(
            wd.get("elaborate"), wd.get("primary_grape"), wd.get("wine_type"),
            grape_map, vcat_by_name, vcat_by_grape_color, vcat_by_grape,
        )

        effervescence = "sparkling" if wd.get("wine_type") == "Sparkling" else None

        wines_list.append({
            "id": wine_id, "slug": slug, "name": wd["wine_name"],
            "name_normalized": wd["wine_name_norm"], "producer_id": wd["producer_id"],
            "country_id": wd["country_id"], "region_id": region_id,
            "appellation_id": appellation_id, "varietal_category_id": vcat_id,
            "effervescence": effervescence, "is_nv": False,
        })

        for year in wd.get("vintage_years", []):
            vintages.append({
                "wine_id": wine_id, "vintage_year": year,
                "abv": float(wd["abv"]) if wd.get("abv") else None,
            })

        seen_grape_ids = set()
        for grape_name in wd.get("grapes", []):
            grape = resolve_grape(grape_name, grape_map)
            if grape and grape["id"] not in seen_grape_ids:
                seen_grape_ids.add(grape["id"])
                wine_grapes.append({"wine_id": wine_id, "grape_id": grape["id"]})
            elif not grape:
                unresolved_grapes[grape_name] = unresolved_grapes.get(grape_name, 0) + 1

    collisions = [(s, c) for s, c in slug_counts.items() if c > 1]
    print(f"\n  {len(collisions)} slug collisions disambiguated")

    if unresolved_grapes:
        total_skipped = sum(unresolved_grapes.values())
        print(f"\n  {len(unresolved_grapes)} unresolved grape names ({total_skipped} wine_grapes rows skipped):")
        for name, count in sorted(unresolved_grapes.items(), key=lambda x: -x[1])[:20]:
            print(f'    "{name}" ({count})')

    print(f"\n-- Summary --")
    print(f"  Wines: {len(wines_list)}")
    print(f"  Wine vintages: {len(vintages)}")
    print(f"  Wine grapes: {len(wine_grapes)}")

    if args.dry_run:
        print(f"\nDRY RUN -- no database changes made.")
        print(f"\nSample wines:")
        for w in wines_list[:10]:
            print(f"  {w['name']} ({w['slug']}) -- producer_id={w['producer_id'][:8]}...")
        return

    # Phase 3: Batch insert
    print("\nPhase 3: Inserting wines...")
    batch_insert("wines", wines_list, batch_size=500)

    print("\nPhase 3b: Inserting wine_vintages...")
    batch_insert("wine_vintages", vintages, batch_size=2000)

    print("\nPhase 3c: Inserting wine_grapes...")
    batch_insert("wine_grapes", wine_grapes, batch_size=2000)

    print("\nDone!")


if __name__ == "__main__":
    main()
