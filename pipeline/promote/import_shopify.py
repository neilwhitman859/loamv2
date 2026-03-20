#!/usr/bin/env python3
"""
Generic Shopify wine catalog importer.

Imports wines from any Shopify wine retailer's JSON catalog.
Handles multiple tag formats: flat, key:value, operational (ignored).

Usage:
    python -m pipeline.promote.import_shopify <catalog.json> <source-name> [--dry-run] [--max-price N]
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

# Known flat-tag grapes
FLAT_GRAPES = {
    "cabernet sauvignon", "pinot noir", "chardonnay", "merlot", "sauvignon blanc",
    "zinfandel", "syrah", "shiraz", "malbec", "pinot grigio", "pinot gris",
    "riesling", "gewurztraminer", "grenache", "tempranillo", "sangiovese",
    "nebbiolo", "barbera", "primitivo", "petit verdot", "petite sirah",
    "cabernet franc", "viognier", "muscat", "moscato", "prosecco",
    "chenin blanc", "verdejo", "albarino", "gruner veltliner",
    "mourvedre", "carignan", "gamay", "verdicchio", "trebbiano",
    "montepulciano", "aglianico", "lambrusco", "touriga nacional",
    "red blend", "white blend", "bordeaux blend red", "ribolla gialla",
    "loureiro", "elbling", "trousseau", "poulsard", "ploussard", "savagnin",
    "melon de bourgogne", "cinsault", "counoise", "rolle", "vermentino",
    "tibouren", "furmint", "blaufränkisch", "zweigelt", "st. laurent",
    "grüner veltliner", "welschriesling", "corvina", "rondinella",
    "garganega", "glera", "arneis", "cortese", "grillo", "nero d'avola",
    "nerello mascalese", "carricante", "frappato", "dolcetto", "freisa",
    "schiava", "lagrein", "teroldego", "nosiola", "pecorino",
    "falanghina", "fiano", "greco", "piedirosso", "coda di volpe",
    "pallagrello", "casavecchia", "godello", "mencia", "treixadura",
    "bobal", "monastrell", "macabeo", "xarel-lo", "parellada", "garnacha",
    "pais", "aligoté", "aligote", "jacquère", "jacquere", "altesse",
    "pinot meunier", "meunier", "chasselas", "moscatel", "malvasia",
    "grechetto", "ciliegiolo", "colorino",
}

FLAT_COUNTRIES = {
    "usa": "United States", "united states": "United States",
    "france": "France", "italy": "Italy", "spain": "Spain",
    "australia": "Australia", "argentina": "Argentina", "chile": "Chile",
    "germany": "Germany", "portugal": "Portugal", "new zealand": "New Zealand",
    "south africa": "South Africa", "austria": "Austria", "greece": "Greece",
    "hungary": "Hungary", "slovenia": "Slovenia", "croatia": "Croatia",
    "georgia": "Georgia", "uruguay": "Uruguay", "brazil": "Brazil",
    "canada": "Canada", "switzerland": "Switzerland", "lebanon": "Lebanon",
    "uk": "United Kingdom", "england": "United Kingdom",
}

FLAT_REGIONS = {
    "california", "napa valley", "sonoma county", "sonoma coast", "carneros",
    "paso robles", "central coast", "russian river valley", "dry creek valley",
    "alexander valley", "mendocino", "willamette valley", "columbia valley",
    "walla walla valley", "barossa valley", "mclaren vale", "marlborough",
    "piedmont", "tuscany", "sicily", "veneto", "bordeaux", "burgundy",
    "champagne", "rhone", "loire", "alsace", "languedoc", "provence",
    "rioja", "stellenbosch", "mendoza", "mosel", "beaujolais", "oregon",
    "alto adige", "friuli venezia giulia", "abruzzo", "campania",
    "douro", "alentejo", "vinho verde",
}

GRAPE_PATTERNS = [
    "Cabernet Sauvignon", "Sauvignon Blanc", "Pinot Noir", "Pinot Grigio",
    "Pinot Gris", "Chenin Blanc", "Petite Sirah", "Petit Verdot",
    "Cabernet Franc", "Chardonnay", "Zinfandel", "Merlot", "Syrah",
    "Shiraz", "Malbec", "Grenache", "Nebbiolo", "Sangiovese",
    "Tempranillo", "Riesling", "Viognier", "Barbera", "Gamay",
    "Red Blend", "Proprietary Red", "Bordeaux Blend",
]

COUNTRY_PATTERNS = [
    (re.compile(r"napa|sonoma|california|oregon|washington|willamette|paso robles", re.I), "United States"),
    (re.compile(r"tuscany|barolo|brunello|chianti|piedmont|langhe|valpolicella", re.I), "Italy"),
    (re.compile(r"bordeaux|burgundy|champagne|rhône|rhone|chablis|sancerre|loire", re.I), "France"),
    (re.compile(r"rioja|toro|jumilla", re.I), "Spain"),
    (re.compile(r"marlborough|hawkes bay", re.I), "New Zealand"),
    (re.compile(r"barossa|mclaren vale|yarra valley", re.I), "Australia"),
    (re.compile(r"stellenbosch", re.I), "South Africa"),
    (re.compile(r"mendoza|uco valley", re.I), "Argentina"),
    (re.compile(r"mosel|pfalz|rheingau", re.I), "Germany"),
    (re.compile(r"douro|alentejo|vinho verde", re.I), "Portugal"),
]

PUB_ALIASES = {
    "wine advocate": "Wine Advocate",
    "james suckling": "James Suckling",
    "wine spectator": "Wine Spectator",
    "wine enthusiast": "Wine Enthusiast",
    "decanter": "Decanter",
    "vinous": "Vinous",
    "jeb dunnuck": "Jeb Dunnuck",
}


def should_skip(product: dict) -> bool:
    title = product.get("title", "")
    if re.search(r"Gift Card|T-Shirt|Cool-Pack|Shipping|Opener|Corkscrew|Glass Set|Tote Bag|Decanter", title, re.I):
        return True
    if re.match(r"^(TEST|DBG)", title, re.I):
        return True
    if re.search(r"DO NOT SELL", title, re.I):
        return True
    pt = product.get("product_type", "")
    if pt and re.search(r"Spirit|Accessory|Merch|Gift|Box|Subscription", pt, re.I):
        return True
    if re.search(r"\d+-Pack|\bBundle\b|\bSampler\b|\bCollection\b|\bDuo\b|\bTrio\b", title, re.I):
        return True
    if re.search(r"Gift Box|Wine Club|Subscription", title, re.I):
        return True
    if pt and re.match(r"^(Cider|Piquette|Verjus|Beer|Spirits?|Sake)$", pt, re.I):
        return True
    return False


def parse_tags(tags: list[str]) -> dict:
    result = {
        "grapes": [], "countries": [], "regions": [], "color": None,
        "vintage": None, "abv": None, "is_sparkling": False, "effervescence": None,
    }
    for tag in tags:
        lower = tag.lower().strip()
        if lower.startswith("country:"):
            result["countries"].append(tag[8:].strip())
            continue
        if lower.startswith("grape:"):
            result["grapes"].append(tag[6:].strip())
            continue
        if lower.startswith("region:"):
            result["regions"].append(tag[7:].strip())
            continue
        if lower.startswith("type:"):
            t = tag[5:].strip().lower()
            if t == "red":
                result["color"] = "red"
            elif t == "white":
                result["color"] = "white"
            elif t in ("rose", "rosé"):
                result["color"] = "rose"
            elif t == "orange":
                result["color"] = "orange"
            elif t == "sparkling":
                result["is_sparkling"] = True
            continue
        if lower.startswith("vintage:"):
            try:
                result["vintage"] = int(tag[8:].strip())
            except ValueError:
                pass
            continue
        if re.match(r"^(freeship|status|display|category|profile|staffpick|importer|past wine)", lower, re.I):
            continue
        abv_m = re.match(r"^(\d{1,2}(?:\.\d+)?)\s*%$", lower)
        if abv_m:
            result["abv"] = float(abv_m.group(1))
            continue
        if lower in ("pet nat", "petnat", "pet'nat"):
            result["is_sparkling"] = True
            result["effervescence"] = "petillant_naturel"
            continue
        if lower.startswith("vintage "):
            try:
                yr = int(lower.replace("vintage ", ""))
                if 1990 <= yr <= 2030:
                    result["vintage"] = yr
            except ValueError:
                pass
            continue
        if lower in FLAT_COUNTRIES:
            result["countries"].append(FLAT_COUNTRIES[lower])
            continue
        if lower in FLAT_REGIONS:
            result["regions"].append(tag)
            continue
        if lower in FLAT_GRAPES:
            result["grapes"].append(tag)
            continue
        if lower in ("rose", "rosé"):
            result["color"] = "rose"
        elif lower == "sparkling":
            result["is_sparkling"] = True
        elif lower == "organic":
            pass  # tracked but not used here
    return result


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
            elif re.search(r"Jeb Dunnuck", ctx, re.I):
                pub = "Jeb Dunnuck"
            if not any(s["score"] == score and s["publication"] == pub for s in scores):
                scores.append({"score": score, "publication": pub})
    return scores


def extract_bottle_format(title: str) -> tuple[str, int, str]:
    formats = [
        (re.compile(r"\(6\s*Liter\)", re.I), "6L", 6000),
        (re.compile(r"\(3\s*Liter\)", re.I), "Jeroboam", 3000),
        (re.compile(r"\(Magnum\s*1?\.?5?L?\)", re.I), "Magnum", 1500),
        (re.compile(r"Magnum\s*1\.5L", re.I), "Magnum", 1500),
        (re.compile(r"\(Half\s*Bottle\s*375\s*m[Ll]\)", re.I), "Half Bottle", 375),
        (re.compile(r"375\s*m[Ll]", re.I), "Half Bottle", 375),
        (re.compile(r"500\s*m[Ll]", re.I), "500ml", 500),
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
        candidate_norm = normalize(candidate)
        app = appellation_map.get(candidate_norm)
        if app:
            appellation = app
            break
        reg = region_map.get(candidate_norm)
        if reg and not reg.get("is_catch_all"):
            region = reg
            break

    producer_name = None
    if grape and grape_start > 0:
        producer_name = title[:grape_start].strip()
    else:
        producer_name = title

    display_name = raw_title
    if vintage:
        display_name = re.sub(r"\s*\b\d{4}\s*$", "", display_name).strip()
    display_name = re.sub(r"\(Magnum\s*1?\.?5?L?\)", "", display_name, flags=re.I)
    display_name = re.sub(r"\(Half\s*Bottle\s*375\s*m[Ll]\)", "", display_name, flags=re.I)
    display_name = re.sub(r"\s*\d+\s*(Pack|pk)\s*$", "", display_name, flags=re.I).strip()

    return {
        "producer_name": producer_name or "Unknown",
        "display_name": display_name,
        "grape": grape,
        "vintage": vintage,
        "appellation": appellation,
        "region": region,
    }


def main():
    parser = argparse.ArgumentParser(description="Shopify wine catalog importer")
    parser.add_argument("catalog_json", help="Path to catalog JSON")
    parser.add_argument("source_name", help="Source name (e.g. 'The Best Wine Store')")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-price", type=float, default=float("inf"))
    args = parser.parse_args()

    sb = get_supabase()
    dry = args.dry_run
    source_slug = slugify(args.source_name)

    print("=" * 60)
    print(f"  SHOPIFY WINE IMPORT: {args.source_name}")
    max_str = f" | Max price: ${args.max_price}" if args.max_price < float("inf") else ""
    print(f"  {'(DRY RUN)' if dry else '(INSERT MODE)'}{max_str}")
    print("=" * 60 + "\n")

    catalog = json.loads(Path(args.catalog_json).read_text(encoding="utf-8"))
    wine_products = [p for p in catalog if not should_skip(p)]
    if args.max_price < float("inf"):
        wine_products = [p for p in wine_products if (p.get("price") or 0) <= args.max_price]
    print(f"Catalog: {len(catalog)} total, {len(wine_products)} wines to import\n")

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
    retailer_source_id = source_type_map.get("retailer-website")

    print(f"  Countries: {len(countries)}, Regions: {len(regions)}")
    print(f"  Appellations: {len(appellations)}, Grapes: {len(grapes)}\n")

    def resolve_grape(name):
        if not name:
            return None
        lower = name.lower().strip()
        if lower in ("red blend", "proprietary red", "bordeaux blend", "white blend"):
            return None
        if lower == "pinot grigio":
            g = grape_map.get("pinot gris")
            return g["id"] if g else None
        if lower == "prosecco":
            g = grape_map.get("glera")
            return g["id"] if g else None
        g = grape_map.get(lower)
        if g:
            return g["id"]
        sid = syn_map.get(lower)
        if sid:
            return sid
        stripped = normalize(name)
        g2 = grape_map.get(stripped)
        if g2:
            return g2["id"]
        return syn_map.get(stripped)

    stats = {
        "producers": 0, "wines": 0, "vintages": 0, "scores": 0,
        "wine_grapes": 0, "prices": 0,
        "appellation_hits": 0, "appellation_misses": 0,
        "grape_hits": 0, "grape_misses": 0,
        "producer_reuses": 0, "skipped": 0,
        "warnings": [], "grape_miss_names": set(),
    }

    producer_id_map: dict[str, str] = {}
    processed = 0

    for product in wine_products:
        processed += 1
        tag_data = parse_tags(product.get("tags") or [])
        parsed = parse_title(product.get("title", ""), appellation_map, region_map)
        scores = extract_scores(product.get("body_html"))

        color = tag_data["color"]
        if not color:
            pt = (product.get("product_type") or "").lower()
            if "red" in pt:
                color = "red"
            elif "white" in pt:
                color = "white"
            elif "ros" in pt:
                color = "rose"
            elif re.search(r"Ros[eé]", product.get("title", ""), re.I):
                color = "rose"

        wine_type = "table"
        effervescence = "still"
        title = product.get("title", "")
        if tag_data["is_sparkling"] or re.search(r"Brut|Cremant|Champagne|Sparkling|P[eé]t-Nat|Prosecco|Cava|Franciacorta", title, re.I):
            wine_type = "sparkling"
            effervescence = tag_data.get("effervescence") or "sparkling"
        if re.search(r"P[eé]t-Nat|Pet[\s'-]?Nat|Ancestral", title, re.I):
            effervescence = "petillant_naturel"
        if re.search(r"Sauternes|Late Harvest|Tokaji|Dessert", title, re.I):
            wine_type = "dessert"
        if re.search(r"\bPort\b|Sherry|Madeira|Marsala", title, re.I):
            wine_type = "fortified"

        vintage = tag_data["vintage"] or parsed["vintage"]
        grape_names = tag_data["grapes"] if tag_data["grapes"] else ([parsed["grape"]] if parsed.get("grape") else [])

        country_id = None
        if tag_data["countries"]:
            country_id = country_map.get(tag_data["countries"][0].lower())
        if not country_id:
            app = parsed.get("appellation")
            reg = parsed.get("region")
            country_id = (app or {}).get("country_id") or (reg or {}).get("country_id")
        if not country_id:
            for pat, cname in COUNTRY_PATTERNS:
                if pat.search(title):
                    country_id = country_map.get(cname.lower())
                    break

        region_id = (parsed.get("region") or {}).get("id")
        if not region_id and parsed.get("appellation"):
            region_id = parsed["appellation"].get("region_id")
        if not region_id and tag_data["regions"]:
            for rn in tag_data["regions"]:
                r = region_map.get(normalize(rn))
                if r and not r.get("is_catch_all"):
                    region_id = r["id"]
                    break

        producer_name = product.get("vendor") if product.get("vendor") and product.get("vendor") != args.source_name else parsed["producer_name"]
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
                        "metadata": {"source": source_slug},
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
                stats["skipped"] += 1
                continue
            try:
                sb.table("wines").insert({
                    "id": wine_id, "producer_id": producer_id,
                    "slug": wine_slug, "name": parsed["display_name"],
                    "name_normalized": normalize(parsed["display_name"]),
                    "color": color, "wine_type": wine_type, "effervescence": effervescence,
                    "appellation_id": (parsed.get("appellation") or {}).get("id"),
                    "country_id": country_id, "region_id": region_id,
                    "metadata": {"source": source_slug, "shopify_id": product.get("shopify_id")},
                }).execute()
            except Exception as e:
                stats["warnings"].append(f"Wine error \"{parsed['display_name']}\": {e}")
                continue
        stats["wines"] += 1

        if parsed.get("appellation"):
            stats["appellation_hits"] += 1
        else:
            stats["appellation_misses"] += 1

        for gn in grape_names:
            gid = resolve_grape(gn)
            if gid:
                if not dry:
                    try:
                        sb.table("wine_grapes").insert({
                            "wine_id": wine_id, "grape_id": gid,
                            "percentage": 100 if len(grape_names) == 1 else None,
                        }).execute()
                    except Exception:
                        pass
                stats["wine_grapes"] += 1
                stats["grape_hits"] += 1
            else:
                stats["grape_misses"] += 1
                stats["grape_miss_names"].add(gn)

        vintage_year = vintage or 0
        if not dry:
            try:
                sb.table("wine_vintages").insert({
                    "id": str(uuid.uuid4()), "wine_id": wine_id,
                    "vintage_year": vintage_year, "abv": tag_data.get("abv"),
                    "metadata": {"source": source_slug},
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
                        "merchant_name": args.source_name,
                        "price_date": date.today().isoformat(),
                    }).execute()
                except Exception:
                    pass
            stats["prices"] += 1

        if processed % 50 == 0:
            print(f"  Processed {processed}/{len(wine_products)}...")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  IMPORT SUMMARY{'  (DRY RUN)' if dry else ''}: {args.source_name}")
    print("=" * 60)
    print(f"  Producers created:  {stats['producers']} ({stats['producer_reuses']} reused)")
    print(f"  Wines created:      {stats['wines']} ({stats['skipped']} skipped)")
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
        for w in stats["warnings"][:15]:
            print(f"    - {w}")
    print("\n  Done!\n")


if __name__ == "__main__":
    main()
