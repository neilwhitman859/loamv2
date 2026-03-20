#!/usr/bin/env python3
"""
Last Bottle Wines bulk import.

Imports wines from Last Bottle's Shopify JSON catalog.
Multi-producer portfolio import -- tests title parsing, score extraction
from marketing copy, appellation/grape resolution with minimal structured data.

Usage:
    python -m pipeline.promote.import_last_bottle [--dry-run] [--replace]
"""

import argparse
import json
import re
import sys
import uuid
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize, slugify

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"

GRAPE_PATTERNS = [
    "Cabernet Sauvignon", "Sauvignon Blanc", "Pinot Noir", "Pinot Grigio",
    "Pinot Gris", "Chenin Blanc", "Petite Sirah", "Petit Verdot",
    "Cabernet Franc", "Ribolla Gialla", "Tinta de Toro", "Sangiovese Grosso",
    "Chardonnay", "Zinfandel", "Merlot", "Syrah", "Shiraz", "Malbec",
    "Grenache", "Nebbiolo", "Sangiovese", "Tempranillo", "Primitivo",
    "Carignan", "Viognier", "Riesling", "Aglianico", "Barbera", "Loureiro",
    "Red Blend", "Proprietary Red", "Bordeaux Blend",
]

SKIP_TITLES = {
    "Cool-Pack Shipping", "Last Bottle Gift Card",
    "Upsell Product 2", "Last Bottle Harvest 2022 T-Shirt (M)",
}

PUB_ALIASES = {
    "robert parker": "Wine Advocate",
    "robert parker's wine advocate": "Wine Advocate",
    "wine advocate": "Wine Advocate",
    "james suckling": "James Suckling",
    "wine spectator": "Wine Spectator",
    "wine enthusiast": "Wine Enthusiast",
    "decanter": "Decanter",
    "vinous": "Vinous",
    "jancis robinson": "Jancis Robinson",
    "jeb dunnuck": "Jeb Dunnuck",
    "wine & spirits": "Wine & Spirits",
}

COUNTRY_PATTERNS = [
    (re.compile(r"napa|sonoma|california|oregon|washington|willamette|paso robles|russian river|carneros|columbia valley|walla walla|lodi|mendocino|central coast", re.I), "United States"),
    (re.compile(r"tuscany|barolo|brunello|montalcino|chianti|piedmont|langhe|barbaresco|valpolicella|salento", re.I), "Italy"),
    (re.compile(r"bordeaux|burgundy|champagne|rh[oô]ne|chablis|sancerre|loire|languedoc|ch[aâ]teauneuf|hermitage|gigondas|sauternes", re.I), "France"),
    (re.compile(r"rioja|toro|jumilla", re.I), "Spain"),
    (re.compile(r"marlborough|canterbury", re.I), "New Zealand"),
    (re.compile(r"barossa|mclaren vale|yarra valley|south australia|clare valley", re.I), "Australia"),
    (re.compile(r"stellenbosch|cape point", re.I), "South Africa"),
    (re.compile(r"mendoza|uco valley", re.I), "Argentina"),
    (re.compile(r"kremstal|wagram", re.I), "Austria"),
    (re.compile(r"tokaji", re.I), "Hungary"),
    (re.compile(r"vinho verde|douro", re.I), "Portugal"),
    (re.compile(r"mosel|pfalz|rheingau", re.I), "Germany"),
]


def should_skip(product: dict) -> bool:
    title = product.get("title", "")
    if title in SKIP_TITLES:
        return True
    if re.match(r"^(TEST|DBG)", title):
        return True
    if re.search(r"T-Shirt|Gift Card|Cool-Pack|Shipping", title, re.I):
        return True
    if not product.get("product_type") and not re.search(r"\d{4}|NV", title):
        return True
    return False


def infer_color(product_type: str | None) -> str | None:
    if not product_type:
        return None
    t = product_type.lower()
    if "red" in t:
        return "red"
    if "white" in t:
        return "white"
    if "ros" in t:
        return "rose"
    return None


def extract_scores(body_html: str | None) -> list[dict]:
    if not body_html:
        return []
    text = re.sub(r"<[^>]+>", " ", body_html).replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text)
    scores = []
    for m in re.finditer(r"(\d{2,3})\s*(?:POINTS?|points?)", text):
        score = int(m.group(1))
        if 80 <= score <= 100:
            ctx = text[max(0, m.start() - 200):m.start() + 200]
            pub = None
            if re.search(r"Robert Parker|Wine Advocate", ctx, re.I):
                pub = "Wine Advocate"
            elif re.search(r"James Suckling", ctx, re.I):
                pub = "James Suckling"
            elif re.search(r"Wine Spectator", ctx, re.I):
                pub = "Wine Spectator"
            elif re.search(r"Wine Enthusiast", ctx, re.I):
                pub = "Wine Enthusiast"
            elif re.search(r"Decanter", ctx, re.I):
                pub = "Decanter"
            elif re.search(r"Vinous|Antonio Galloni", ctx, re.I):
                pub = "Vinous"
            elif re.search(r"Jancis Robinson", ctx, re.I):
                pub = "Jancis Robinson"
            elif re.search(r"Jeb Dunnuck", ctx, re.I):
                pub = "Jeb Dunnuck"
            if not any(s["score"] == score and s["publication"] == pub for s in scores):
                scores.append({"score": score, "publication": pub})
    return scores


def extract_bottle_format(title: str) -> tuple[str, int, str]:
    formats = [
        (re.compile(r"\(6\s*Liter\)", re.I), "6L", 6000),
        (re.compile(r"\(3\s*Liter\)", re.I), "Jeroboam", 3000),
        (re.compile(r"\(Magnum\s*1\.5L\)", re.I), "Magnum", 1500),
        (re.compile(r"\(Magnum\)", re.I), "Magnum", 1500),
        (re.compile(r"Magnum", re.I), "Magnum", 1500),
        (re.compile(r"\(Half\s*Bottle\s*375\s*m[Ll]\)", re.I), "Half Bottle", 375),
        (re.compile(r"\(500\s*m[Ll]\)", re.I), "500ml", 500),
        (re.compile(r"375\s*m[Ll]", re.I), "Half Bottle", 375),
    ]
    for pat, name, ml in formats:
        if pat.search(title):
            return name, ml, pat.sub("", title).strip()
    return "Standard", 750, title


def parse_title(raw_title: str, appellation_map: dict, region_map: dict) -> dict:
    _, _, clean_title = extract_bottle_format(raw_title)
    vintage = None
    title = clean_title
    vm = re.search(r"\b((?:19|20)\d{2})\s*$", title)
    if vm:
        vintage = int(vm.group(1))
        title = title[:vm.start()].strip()
    elif re.search(r"\bNV\s*$", title, re.I):
        title = re.sub(r"\bNV\s*$", "", title, flags=re.I).strip()

    grape = None
    grape_start = -1
    grape_end = -1
    for gp in GRAPE_PATTERNS:
        pattern = re.compile(r"\b" + re.escape(gp) + r"\b", re.I)
        gm = pattern.search(title)
        if gm:
            grape = gp
            grape_start = gm.start()
            grape_end = gm.end()
            break

    appellation = None
    region = None
    geo_section = title[grape_end:].strip() if grape else title
    geo_words = geo_section.split()
    for start in range(len(geo_words)):
        candidate = " ".join(geo_words[start:])
        app = appellation_map.get(normalize(candidate))
        if app:
            appellation = app
            break
        reg = region_map.get(normalize(candidate))
        if reg and not reg.get("is_catch_all"):
            region = reg
            break

    producer_name = None
    if grape and grape_start > 0:
        producer_name = title[:grape_start].strip()
    elif appellation:
        pass  # complex logic simplified
    else:
        producer_name = title

    display_name = raw_title
    if vintage:
        display_name = re.sub(r"\s*\b\d{4}\s*$", "", display_name).strip()
    for pat in [r"\(Magnum\s*1?\.?5?L?\)", r"\(Half\s*Bottle\s*375\s*m[Ll]\)", r"\(500\s*m[Ll]\)", r"\(6\s*Liter\)", r"\(3\s*Liter\)"]:
        display_name = re.sub(pat, "", display_name, flags=re.I).strip()

    return {
        "producer_name": producer_name or "Unknown Producer",
        "display_name": display_name,
        "grape": grape,
        "vintage": vintage,
        "appellation": appellation,
        "region": region,
    }


def main():
    parser = argparse.ArgumentParser(description="Last Bottle Wines import")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    dry = args.dry_run

    print("=" * 60)
    print("  LAST BOTTLE WINES IMPORT")
    print(f"  {'(DRY RUN)' if dry else '(INSERT MODE)'}")
    print("=" * 60 + "\n")

    catalog = json.loads((DATA_DIR / "last_bottle_raw.json").read_text(encoding="utf-8"))
    wine_products = [p for p in catalog if not should_skip(p)]
    print(f"Catalog: {len(catalog)} products, {len(wine_products)} wines\n")

    # Load reference data
    print("Loading reference data...")
    countries = fetch_all("countries", "id,name,iso_code")
    country_map = {}
    for c in countries:
        country_map[c["name"].lower()] = c["id"]
        if c.get("iso_code"):
            country_map[c["iso_code"].lower()] = c["id"]

    regions = fetch_all("regions", "id,name,country_id,parent_id,is_catch_all")
    region_map = {}
    for r in regions:
        region_map[normalize(r["name"])] = r

    appellations = fetch_all("appellations", "id,name,designation_type,country_id,region_id")
    appellation_map = {}
    for a in appellations:
        appellation_map[normalize(a["name"])] = a

    aliases = fetch_all("appellation_aliases", "appellation_id,alias_normalized")
    for al in aliases:
        app = next((a for a in appellations if a["id"] == al["appellation_id"]), None)
        if app and al["alias_normalized"] not in appellation_map:
            appellation_map[al["alias_normalized"]] = app

    grapes = fetch_all("grapes", "id,name,display_name,color")
    grape_map = {}
    for g in grapes:
        if g.get("display_name"):
            grape_map[g["display_name"].lower()] = g
        grape_map[g["name"].lower()] = g

    synonyms = fetch_all("grape_synonyms", "grape_id,synonym")
    syn_map = {s["synonym"].lower(): s["grape_id"] for s in synonyms}

    publications = fetch_all("publications", "id,name,slug")
    pub_map = {}
    for p in publications:
        pub_map[p["name"].lower()] = p["id"]
        pub_map[p["slug"]] = p["id"]
    for alias, canonical in PUB_ALIASES.items():
        pid = pub_map.get(canonical.lower())
        if pid:
            pub_map[alias.lower()] = pid

    source_types = fetch_all("source_types", "id,slug")
    source_type_map = {s["slug"]: s["id"] for s in source_types}
    retailer_source_id = source_type_map.get("retailer-website") or source_type_map.get("producer-website")

    print(f"  Countries: {len(countries)}, Regions: {len(regions)}")
    print(f"  Appellations: {len(appellations)}, Grapes: {len(grapes)}\n")

    def resolve_grape(name):
        if not name:
            return None
        lower = name.lower().strip()
        if lower in ("red blend", "proprietary red", "bordeaux blend"):
            return None
        g = grape_map.get(lower)
        if g:
            return g["id"]
        sid = syn_map.get(lower)
        if sid:
            return sid
        stripped = normalize(name)
        g2 = grape_map.get(stripped)
        return g2["id"] if g2 else None

    stats = {
        "producers": 0, "wines": 0, "vintages": 0, "scores": 0,
        "wine_grapes": 0, "prices": 0,
        "appellation_hits": 0, "appellation_misses": 0,
        "grape_hits": 0, "grape_misses": 0,
        "producer_reuses": 0,
        "warnings": [], "grape_miss_names": set(),
    }

    producer_id_map: dict[str, str] = {}
    processed = 0

    for product in wine_products:
        processed += 1
        parsed = parse_title(product.get("title", ""), appellation_map, region_map)
        color = infer_color(product.get("product_type")) or (
            "red" if parsed.get("grape") in ("Red Blend", "Proprietary Red") else None
        )
        scores = extract_scores(product.get("body_html"))

        wine_type = "table"
        effervescence = None
        title = product.get("title", "")
        if product.get("product_type") == "Sparkling" or re.search(r"Brut|Cremant|Champagne|Sparkling|P[eé]t-Nat", title, re.I):
            wine_type = "sparkling"
            effervescence = "sparkling"
        if re.search(r"Sauternes|Late Harvest|Tokaji", title, re.I):
            wine_type = "dessert"
        if re.search(r"Port|Sherry|Madeira|Marsala|Vermouth", title, re.I):
            wine_type = "fortified"

        country_id = (parsed.get("appellation") or {}).get("country_id") or (parsed.get("region") or {}).get("country_id")
        if not country_id:
            for pat, cname in COUNTRY_PATTERNS:
                if pat.search(title):
                    country_id = country_map.get(cname.lower())
                    break
        if not country_id:
            country_id = country_map.get("united states")
            stats["warnings"].append(f"Country defaulted to US for: \"{title}\"")

        region_id = (parsed.get("region") or {}).get("id")
        if not region_id and parsed.get("appellation"):
            region_id = parsed["appellation"].get("region_id")

        producer_name = parsed["producer_name"]
        producer_slug = slugify(producer_name)

        if producer_slug in producer_id_map:
            producer_id = producer_id_map[producer_slug]
            stats["producer_reuses"] += 1
        else:
            result = sb.table("producers").select("id").eq("slug", producer_slug).execute()
            if result.data:
                producer_id = result.data[0]["id"]
                producer_id_map[producer_slug] = producer_id
                stats["producer_reuses"] += 1
            elif not dry:
                producer_id = str(uuid.uuid4())
                try:
                    sb.table("producers").insert({
                        "id": producer_id, "name": producer_name,
                        "slug": producer_slug, "name_normalized": normalize(producer_name),
                        "country_id": country_id, "region_id": region_id,
                        "metadata": {"source": "last-bottle-wines"},
                    }).execute()
                except Exception as e:
                    stats["warnings"].append(f"Producer error \"{producer_name}\": {e}")
                    continue
                producer_id_map[producer_slug] = producer_id
                stats["producers"] += 1
            else:
                producer_id = f"dry-{producer_slug}"
                producer_id_map[producer_slug] = producer_id
                stats["producers"] += 1

        wine_slug = slugify(parsed["display_name"])
        wine_id = str(uuid.uuid4())

        if not dry:
            existing = sb.table("wines").select("id").eq("slug", wine_slug).execute()
            if existing.data:
                continue
            try:
                sb.table("wines").insert({
                    "id": wine_id, "producer_id": producer_id,
                    "slug": wine_slug, "name": parsed["display_name"],
                    "name_normalized": normalize(parsed["display_name"]),
                    "color": color, "wine_type": wine_type, "effervescence": effervescence,
                    "appellation_id": (parsed.get("appellation") or {}).get("id"),
                    "country_id": country_id, "region_id": region_id,
                    "metadata": {"source": "last-bottle-wines", "shopify_id": product.get("shopify_id")},
                }).execute()
            except Exception as e:
                stats["warnings"].append(f"Wine error \"{parsed['display_name']}\": {e}")
                continue
        stats["wines"] += 1

        if parsed.get("appellation"):
            stats["appellation_hits"] += 1
        else:
            stats["appellation_misses"] += 1

        if parsed.get("grape"):
            gid = resolve_grape(parsed["grape"])
            if gid:
                if not dry:
                    try:
                        sb.table("wine_grapes").insert({
                            "wine_id": wine_id, "grape_id": gid, "percentage": 100,
                        }).execute()
                    except Exception:
                        pass
                stats["wine_grapes"] += 1
                stats["grape_hits"] += 1
            else:
                stats["grape_misses"] += 1
                stats["grape_miss_names"].add(parsed["grape"])

        vintage_year = parsed.get("vintage") or 0
        if not dry:
            try:
                sb.table("wine_vintages").insert({
                    "id": str(uuid.uuid4()), "wine_id": wine_id,
                    "vintage_year": vintage_year,
                    "metadata": {"source": "last-bottle-wines"},
                }).execute()
            except Exception:
                pass
        stats["vintages"] += 1

        for sc in scores:
            pub_id = pub_map.get((sc["publication"] or "").lower()) if sc.get("publication") else None
            if not dry:
                try:
                    sb.table("wine_vintage_scores").insert({
                        "wine_id": wine_id, "vintage_year": vintage_year,
                        "publication_id": pub_id, "score": sc["score"],
                        "score_scale": "100-point", "source_id": retailer_source_id,
                    }).execute()
                    stats["scores"] += 1
                except Exception:
                    pass
            else:
                stats["scores"] += 1

        if product.get("price"):
            if not dry:
                try:
                    sb.table("wine_vintage_prices").insert({
                        "wine_id": wine_id, "vintage_year": vintage_year,
                        "price_usd": product["price"], "price_type": "retail",
                        "merchant_name": "Last Bottle Wines",
                        "price_date": date.today().isoformat(),
                    }).execute()
                    stats["prices"] += 1
                except Exception:
                    pass
            else:
                stats["prices"] += 1

        if processed % 25 == 0:
            print(f"  Processed {processed}/{len(wine_products)}...")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  IMPORT SUMMARY{'  (DRY RUN)' if dry else ''}")
    print("=" * 60)
    print(f"  Producers created:  {stats['producers']} ({stats['producer_reuses']} reused)")
    print(f"  Wines created:      {stats['wines']}")
    print(f"  Vintages created:   {stats['vintages']}")
    print(f"  Scores inserted:    {stats['scores']}")
    print(f"  Wine grapes linked: {stats['wine_grapes']}")
    print(f"  Prices recorded:    {stats['prices']}")
    app_total = stats["appellation_hits"] + stats["appellation_misses"]
    print(f"  Appellation hits:   {stats['appellation_hits']}/{app_total} ({round(stats['appellation_hits'] / app_total * 100) if app_total else 0}%)")
    grape_total = stats["grape_hits"] + stats["grape_misses"]
    print(f"  Grape hits:         {stats['grape_hits']}/{grape_total} ({round(stats['grape_hits'] / grape_total * 100) if grape_total else 0}%)")
    if stats["grape_miss_names"]:
        print(f"\n  Unresolved grapes: {', '.join(sorted(stats['grape_miss_names']))}")
    if stats["warnings"]:
        print(f"\n  Warnings ({len(stats['warnings'])}):")
        for w in stats["warnings"][:20]:
            print(f"    - {w}")
    print("\n  Done!\n")


if __name__ == "__main__":
    main()
