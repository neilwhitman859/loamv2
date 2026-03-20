"""
Create new Loam wines from unmatched Vivino listings.

Phase 0: Second-chance matching -- recover false negatives (suffix stripping + Haiku)
Phase 1: Create new producers for truly unmatched wineries
Phase 2: Classify varietal categories (grape parsing + regional + Haiku + generic)
Phase 3: Resolve regions / appellations
Phase 4: Create wine records
Phase 5: Create vintages, scores, prices, grape links

Usage:
    python -m pipeline.vivino.create_wines --dry-run
    python -m pipeline.vivino.create_wines --skip-rematch
    python -m pipeline.vivino.create_wines --input file.json
    python -m pipeline.vivino.create_wines
"""

import sys
import json
import re
import time
import argparse
import uuid
from pathlib import Path
from datetime import date

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, get_env, batch_insert
from pipeline.lib.normalize import normalize, slugify

VIVINO_PUBLICATION_ID = "ed228eae-c3bf-41e6-9a90-d78c8efaf97e"
TODAY = date.today().isoformat()

# Vivino wine_type_id -> Loam wine type string
VIVINO_TYPE_MAP = {
    1: "Red", 2: "White", 3: "Sparkling", 4: "Rose",
    7: "Dessert", 24: "Dessert/Port",
}

WINE_TYPE_GENERIC_BLEND = {
    "Red": "Red Blend", "White": "White Blend", "Rose": "Rose Blend",
    "Sparkling": "Sparkling Blend", "Dessert": "Dessert Blend", "Dessert/Port": "Port",
}

REGIONAL_DESIGNATION_MAP = {
    "champagne": "Champagne Blend", "port": "Port", "porto": "Port",
    "prosecco": "Prosecco", "cava": "Cava Blend", "chianti": "Chianti Blend",
    "barolo": "Nebbiolo", "barbaresco": "Nebbiolo", "beaujolais": "Beaujolais",
    "cotes du rhone": "Rhone Blend", "chateauneuf du pape": "Rhone Blend",
    "bordeaux": "Bordeaux Blend", "rioja": "Rioja Blend", "sauternes": "Sauternes",
    "valpolicella": "Valpolicella Blend", "amarone": "Valpolicella Blend",
    "cremant": "Sparkling Blend", "asti": "Moscato", "brunello": "Sangiovese",
    "priorat": "Priorat Blend",
}

COUNTRY_ALIASES = {
    "united states": "United States", "usa": "United States", "us": "United States",
    "uk": "United Kingdom", "great britain": "United Kingdom",
}

PRODUCER_SUFFIXES = [
    "vineyards", "vineyard", "winery", "estate", "wines", "wine",
    "family", "cellars", "cellar", "estates", "bodegas", "bodega",
    "domaine", "champagne", "chateau", "château", "casa", "cantina",
    "tenuta", "fattoria", "azienda", "weingut",
]


def normalize_country_name(name):
    if not name:
        return name
    return COUNTRY_ALIASES.get(name.lower(), name)


def strip_producer_suffixes(name):
    n = normalize(name)
    for suffix in PRODUCER_SUFFIXES:
        n = re.sub(rf"\b{suffix}\b", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def target_color_for_wine_type(wine_type, grape_color):
    mapping = {
        "Red": "red", "White": "white", "Rose": "rose",
        "Sparkling": grape_color or "white", "Dessert": grape_color or "white",
        "Dessert/Port": "red",
    }
    return mapping.get(wine_type, grape_color or "red")


def levenshtein(a, b):
    m, n = len(a), len(b)
    if m == 0: return n
    if n == 0: return m
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1): dp[i][0] = i
    for j in range(n + 1): dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            dp[i][j] = dp[i - 1][j - 1] if a[i - 1] == b[j - 1] else 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
    return dp[m][n]


def fetch_all(table, columns="*", batch_size=1000):
    sb = get_supabase()
    rows, offset = [], 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < batch_size: break
        offset += batch_size
    return rows


def call_haiku(messages, api_key, max_tokens=2000):
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens, "messages": messages},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "text": data["content"][0].get("text", "") if data.get("content") else "",
        "inputTokens": data.get("usage", {}).get("input_tokens", 0),
        "outputTokens": data.get("usage", {}).get("output_tokens", 0),
    }


def main():
    parser = argparse.ArgumentParser(description="Create wines from Vivino unmatched")
    parser.add_argument("--input", default="vivino_unmatched.json")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-rematch", action="store_true")
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY", required=False)
    sb = get_supabase()

    print("=== Create Wines from Vivino Unmatched ===\n")

    # 1. Load & dedup input
    input_file = args.input
    if input_file.endswith(".jsonl"):
        with open(input_file, "r", encoding="utf-8") as f:
            raw_listings = [json.loads(line) for line in f if line.strip()]
    else:
        raw_listings = json.loads(Path(input_file).read_text(encoding="utf-8"))
    print(f"Loaded {len(raw_listings)} listings from {input_file}")

    by_wine_id: dict[int, dict] = {}
    for l in raw_listings:
        key = l.get("vivino_wine_id")
        if key not in by_wine_id or (l.get("rating_count") or 0) > (by_wine_id[key]["listing"].get("rating_count") or 0):
            if key not in by_wine_id:
                by_wine_id[key] = {"listing": l, "vintages": set(), "prices": []}
            else:
                by_wine_id[key]["listing"] = l
        entry = by_wine_id[key]
        if l.get("vintage_year"):
            entry["vintages"].add(l["vintage_year"])
        if l.get("price_usd") is not None:
            entry["prices"].append({
                "price_usd": l["price_usd"], "price_raw": l.get("price_raw"),
                "merchant_name": l.get("merchant_name"), "source_url": l.get("source_url"),
                "vintage_year": l.get("vintage_year"),
            })

    unique_wines = list(by_wine_id.values())
    print(f"Deduped to {len(unique_wines)} unique wines\n")

    # 2. Load reference data
    print("Loading reference data...")
    countries = fetch_all("countries", "id,name")
    country_map = {c["name"]: c["id"] for c in countries}

    producers = fetch_all("producers", "id,name,name_normalized,country_id,slug")
    producer_by_norm: dict[str, list] = {}
    producer_by_id = {}
    for p in producers:
        producer_by_id[p["id"]] = p
        norm = normalize(p["name"])
        producer_by_norm.setdefault(norm, []).append(p)

    aliases = fetch_all("producer_aliases", "name,producer_id")
    alias_norm_map = {}
    for a in aliases:
        prod = producer_by_id.get(a["producer_id"])
        if prod:
            alias_norm_map[normalize(a["name"])] = prod

    grapes = fetch_all("grapes", "id,name,color")
    grape_map = {}
    grape_names = []
    for g in grapes:
        grape_map[g["name"].lower()] = {"id": g["id"], "color": g.get("color"), "name": g["name"]}
        grape_names.append(g["name"].lower())
    grape_names.sort(key=lambda x: -len(x))

    vcats = fetch_all("varietal_categories", "id,name,color,type,grape_id")
    vcat_by_name = {v["name"]: v["id"] for v in vcats}
    vcat_by_grape_color = {}
    vcat_by_grape = {}
    for v in vcats:
        if v.get("grape_id"):
            vcat_by_grape_color[f"{v['grape_id']}|{v.get('color')}"] = v["id"]
            if v["grape_id"] not in vcat_by_grape:
                vcat_by_grape[v["grape_id"]] = v["id"]

    regions = fetch_all("regions", "id,country_id,is_catch_all")
    catch_all_region = {r["country_id"]: r["id"] for r in regions if r.get("is_catch_all")}

    try:
        rnm = fetch_all("region_name_mappings", "region_name,country,region_id,appellation_id")
        region_mapping = {f"{r['region_name']}|{r['country']}": {"region_id": r["region_id"], "appellation_id": r.get("appellation_id")} for r in rnm}
        print(f"  {len(rnm)} region name mappings")
    except Exception:
        region_mapping = {}
        print("  region_name_mappings not available")

    existing_wines = fetch_all("wines", "id,name,name_normalized,producer_id")
    existing_wine_map = {}
    wines_by_producer: dict[str, list] = {}
    for w in existing_wines:
        existing_wine_map[f"{w['producer_id']}||{w.get('name_normalized', normalize(w['name']))}"] = w
        wines_by_producer.setdefault(w["producer_id"], []).append(w)

    slug_rows = fetch_all("wines", "slug")
    existing_slugs = {r["slug"] for r in slug_rows}
    print(f"  {len(existing_slugs)} existing wine slugs\n")

    all_producer_norms = list(producer_by_norm.keys())

    # Pre-compute suffix-stripped producer names
    stripped_producer_map: dict[str, list] = {}
    for norm, prods in producer_by_norm.items():
        stripped = strip_producer_suffixes(norm)
        if stripped != norm and len(stripped) > 2:
            stripped_producer_map.setdefault(stripped, []).extend(prods)

    total_haiku_input = 0
    total_haiku_output = 0

    # ── PHASE 0: Second-chance matching ──
    rematched = []
    truly_new = []

    if args.skip_rematch:
        print("--- Phase 0: SKIPPED (--skip-rematch) ---\n")
        truly_new.extend(unique_wines)
    else:
        print("--- Phase 0: Second-chance matching ---")
        for idx, entry in enumerate(unique_wines):
            if (idx + 1) % 1000 == 0:
                print(f"\r  Phase 0: {idx + 1}/{len(unique_wines)}", end="", flush=True)
            l = entry["listing"]
            if not l.get("winery_name"):
                truly_new.append(entry)
                continue

            norm_winery = normalize(l["winery_name"])
            norm_country = normalize_country_name(l.get("country_name"))
            country_id = country_map.get(norm_country)

            matched_producer = None

            # 0a. Exact normalized
            candidates = producer_by_norm.get(norm_winery)
            if candidates:
                matched_producer = next((p for p in candidates if p.get("country_id") == country_id), candidates[0]) if country_id else candidates[0]

            # 0b. Alias
            if not matched_producer:
                matched_producer = alias_norm_map.get(norm_winery)

            # 0c. Suffix-stripped
            if not matched_producer:
                stripped = strip_producer_suffixes(l["winery_name"])
                if stripped != norm_winery and len(stripped) > 2:
                    cands = producer_by_norm.get(stripped)
                    if cands:
                        matched_producer = next((p for p in cands if p.get("country_id") == country_id), cands[0]) if country_id else cands[0]
                    if not matched_producer:
                        matched_producer = alias_norm_map.get(stripped)

            # 0c2. Reverse stripped lookup
            if not matched_producer:
                cands = stripped_producer_map.get(norm_winery) or stripped_producer_map.get(strip_producer_suffixes(l["winery_name"]))
                if cands:
                    matched_producer = next((p for p in cands if p.get("country_id") == country_id), cands[0]) if country_id else cands[0]

            if matched_producer:
                norm_wine = normalize(l.get("wine_name", ""))
                wine_key = f"{matched_producer['id']}||{norm_wine}"
                matched_wine = existing_wine_map.get(wine_key)

                if not matched_wine:
                    prod_wines = wines_by_producer.get(matched_producer["id"], [])
                    matched_wine = next(
                        (w for w in prod_wines if (w.get("name_normalized") or normalize(w["name"])) in norm_wine or norm_wine in (w.get("name_normalized") or normalize(w["name"]))),
                        None,
                    )

                if matched_wine:
                    rematched.append({"entry": entry, "producerId": matched_producer["id"], "wineId": matched_wine["id"], "winery": l["winery_name"]})
                else:
                    entry["_resolvedProducer"] = matched_producer
                    truly_new.append(entry)
            else:
                truly_new.append(entry)

        print(f"\n  Rematched: {len(rematched)} wines (false negatives recovered)")
        print(f"  Truly new: {len(truly_new)} wines\n")

    # ── Insert data for rematched wines ──
    if rematched and not args.dry_run:
        print(f"--- Inserting data for {len(rematched)} rematched wines ---")
        vintage_rows, vintage_keys = [], set()
        for m in rematched:
            for year in m["entry"]["vintages"]:
                key = f"{m['wineId']}||{year}"
                if key not in vintage_keys:
                    vintage_keys.add(key)
                    vintage_rows.append({"wine_id": m["wineId"], "vintage_year": year})
        if vintage_rows:
            try:
                sb.table("wine_vintages").upsert(vintage_rows, on_conflict="wine_id,vintage_year").execute()
                print(f"  Upserted {len(vintage_rows)} vintages")
            except Exception as e:
                print(f"  Vintage upsert error: {e}")

        score_rows, score_keys = [], set()
        for m in rematched:
            l = m["entry"]["listing"]
            if not l.get("rating_average") or not l.get("rating_count"):
                continue
            key = f"{m['wineId']}||{l.get('vintage_year', 'nv')}"
            if key in score_keys: continue
            score_keys.add(key)
            score_rows.append({
                "wine_id": m["wineId"], "vintage_year": l.get("vintage_year"),
                "score": l["rating_average"], "score_scale": "5",
                "publication_id": VIVINO_PUBLICATION_ID, "critic": "Vivino Community",
                "is_community": True, "rating_count": l["rating_count"],
                "review_date": TODAY, "url": f"https://www.vivino.com/w/{l.get('vivino_wine_id')}",
            })
        if score_rows:
            inserted = 0
            for row in score_rows:
                try:
                    sb.table("wine_vintage_scores").insert(row).execute()
                    inserted += 1
                except Exception:
                    pass
            print(f"  Inserted {inserted}/{len(score_rows)} scores")

    if not truly_new:
        print("No truly new wines to create. Done!")
        return

    # ── PHASE 1: Create new producers ──
    print(f"--- Phase 1: Resolve/create producers for {len(truly_new)} wines ---")
    new_producers = []
    producer_slugs_used = {p.get("slug") or slugify(p["name"]) for p in producers}
    created_producer_map = {}

    for entry in truly_new:
        if entry.get("_resolvedProducer"):
            continue
        l = entry["listing"]
        if not l.get("winery_name") or not l.get("wine_name"):
            continue
        norm_country = normalize_country_name(l.get("country_name"))
        country_id = country_map.get(norm_country)
        if not country_id:
            continue

        dedup_key = f"{normalize(l['winery_name'])}|{country_id}"
        if dedup_key in created_producer_map:
            entry["_resolvedProducer"] = created_producer_map[dedup_key]
            continue

        prod_id = str(uuid.uuid4())
        prod_slug = slugify(l["winery_name"])
        if prod_slug in producer_slugs_used:
            prod_slug = f"{prod_slug}-{slugify(norm_country)}"
        if prod_slug in producer_slugs_used:
            prod_slug = f"{prod_slug}-{prod_id[:6]}"
        producer_slugs_used.add(prod_slug)

        new_prod = {
            "id": prod_id, "slug": prod_slug, "name": l["winery_name"],
            "name_normalized": normalize(l["winery_name"]), "country_id": country_id,
        }
        new_producers.append(new_prod)
        resolved = {"id": prod_id, "slug": prod_slug, "name": l["winery_name"], "country_id": country_id}
        entry["_resolvedProducer"] = resolved
        created_producer_map[dedup_key] = resolved

    print(f"  New producers to create: {len(new_producers)}")
    if not args.dry_run and new_producers:
        batch_insert("producers", new_producers)

    # ── PHASE 2: Classify varietal categories ──
    print(f"\n--- Phase 2: Classify varietal categories ---")
    needs_haiku_vcat = []

    for entry in truly_new:
        l = entry["listing"]
        wine_type = VIVINO_TYPE_MAP.get(l.get("wine_type_id"), "Red")
        norm_wine_name = normalize(l.get("wine_name", "")).lower()

        vcat_id = None
        grape_id = None
        method = None

        # 2a. Parse grape from wine name
        for grape_name in grape_names:
            if grape_name in norm_wine_name:
                grape = grape_map.get(grape_name)
                if grape:
                    target_color = target_color_for_wine_type(wine_type, grape.get("color"))
                    vcat_id = vcat_by_grape_color.get(f"{grape['id']}|{target_color}") or vcat_by_grape.get(grape["id"])
                    if vcat_id:
                        grape_id = grape["id"]
                        method = f"grape:{grape['name']}"
                        break

        # 2b. Regional designation
        if not vcat_id:
            norm_region = normalize(l.get("region_name", "")).lower()
            for region_key, vc_name in REGIONAL_DESIGNATION_MAP.items():
                pattern = rf"\b{re.escape(region_key)}\b"
                if re.search(pattern, norm_wine_name) or re.search(pattern, norm_region):
                    vcat_id = vcat_by_name.get(vc_name)
                    if vcat_id:
                        method = f"regional:{region_key}->{vc_name}"
                        break

        if not vcat_id:
            needs_haiku_vcat.append(entry)

        entry["_vcatId"] = vcat_id
        entry["_grapeId"] = grape_id
        entry["_wineType"] = wine_type
        entry["_vcatMethod"] = method

    print(f"  Resolved by grape parsing: {sum(1 for e in truly_new if (e.get('_vcatMethod') or '').startswith('grape:'))}")
    print(f"  Resolved by regional: {sum(1 for e in truly_new if (e.get('_vcatMethod') or '').startswith('regional:'))}")
    print(f"  Needs Haiku: {len(needs_haiku_vcat)}")

    # 2c. Haiku classification
    if needs_haiku_vcat and api_key:
        vcat_names_str = ", ".join(v["name"] for v in vcats)
        prompt_items = "\n".join(
            f'[{idx}] "{e["listing"].get("wine_name", "")}" -- Type: {e["_wineType"]}, Region: {e["listing"].get("region_name", "?")}({e["listing"].get("country_name", "")})'
            for idx, e in enumerate(needs_haiku_vcat)
        )
        try:
            result = call_haiku([
                {"role": "user", "content": f"You are a wine classification expert. For each wine, determine the most likely varietal category.\n\nAvailable categories: {vcat_names_str}\n\nFor each wine, return JSON array with: {{\"index\": N, \"category\": \"exact name from list\"}}\n\n{prompt_items}"},
                {"role": "assistant", "content": "["},
            ], api_key)
            total_haiku_input += result["inputTokens"]
            total_haiku_output += result["outputTokens"]
            cleaned = ("[" + result["text"]).replace("```json", "").replace("```", "").strip()
            results = json.loads(cleaned)
            for r in results:
                if r.get("index") is not None and r["index"] < len(needs_haiku_vcat):
                    vc_id = vcat_by_name.get(r.get("category"))
                    if vc_id:
                        needs_haiku_vcat[r["index"]]["_vcatId"] = vc_id
                        needs_haiku_vcat[r["index"]]["_vcatMethod"] = f"haiku:{r['category']}"
            print(f"  Haiku resolved: {sum(1 for e in needs_haiku_vcat if e.get('_vcatId'))}/{len(needs_haiku_vcat)}")
        except Exception as err:
            print(f"  Haiku classification error: {err}")

    # 2d. Generic fallback
    for entry in truly_new:
        if not entry.get("_vcatId"):
            generic_name = WINE_TYPE_GENERIC_BLEND.get(entry.get("_wineType", ""), "Red Blend")
            entry["_vcatId"] = vcat_by_name.get(generic_name) or vcat_by_name.get("Red Blend")
            entry["_vcatMethod"] = f"fallback:{generic_name}"

    # ── PHASE 3: Resolve regions ──
    print(f"\n--- Phase 3: Resolve regions ---")
    region_hits = 0
    catch_all_hits = 0
    for entry in truly_new:
        l = entry["listing"]
        norm_country = normalize_country_name(l.get("country_name"))
        country_id = country_map.get(norm_country)
        region_id = None
        appellation_id = None

        if l.get("region_name"):
            rm = region_mapping.get(f"{l['region_name']}|{norm_country}")
            if rm:
                region_id = rm["region_id"]
                appellation_id = rm.get("appellation_id")
                region_hits += 1

        if not region_id and country_id:
            region_id = catch_all_region.get(country_id)
            if region_id:
                catch_all_hits += 1

        entry["_regionId"] = region_id
        entry["_appellationId"] = appellation_id
        entry["_countryId"] = country_id

    print(f"  Region mapped: {region_hits}")
    print(f"  Catch-all fallback: {catch_all_hits}")

    # ── PHASE 4: Create wine records ──
    print(f"\n--- Phase 4: Create wine records ---")
    new_wines = []
    skipped_dupes = []

    for entry in truly_new:
        l = entry["listing"]
        prod = entry.get("_resolvedProducer")
        if not prod or not entry.get("_countryId") or not entry.get("_vcatId"):
            continue

        norm_wine = normalize(l.get("wine_name", ""))
        dedup_key = f"{prod['id']}||{norm_wine}"
        if dedup_key in existing_wine_map:
            skipped_dupes.append(l)
            continue

        slug = f"{prod.get('slug') or slugify(prod['name'])}-{slugify(l.get('wine_name', ''))}".strip("-")[:120]
        if not slug:
            slug = prod.get("slug") or slugify(prod["name"])
        if slug in existing_slugs:
            slug = f"{slug}-vivino"
        if slug in existing_slugs:
            slug = f"{slug}-{uuid.uuid4().hex[:6]}"
        existing_slugs.add(slug)

        wine_id = str(uuid.uuid4())
        wine_type = entry.get("_wineType", "Red")

        new_wines.append({
            "id": wine_id, "slug": slug, "name": l.get("wine_name", ""),
            "name_normalized": norm_wine, "producer_id": prod["id"],
            "country_id": entry["_countryId"], "region_id": entry.get("_regionId"),
            "appellation_id": entry.get("_appellationId"),
            "varietal_category_id": entry["_vcatId"],
            "effervescence": "sparkling" if wine_type == "Sparkling" else None,
            "is_nv": not l.get("vintage_year") and len(entry.get("vintages", set())) == 0,
        })
        entry["_wineId"] = wine_id
        existing_wine_map[dedup_key] = {"id": wine_id}

    print(f"  New wines to create: {len(new_wines)}")
    print(f"  Skipped (dedup): {len(skipped_dupes)}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would create:")
        print(f"  {len(new_producers)} producers")
        print(f"  {len(new_wines)} wines")
        haiku_cost = (total_haiku_input * 0.8 + total_haiku_output * 4) / 1_000_000
        print(f"\n  Haiku cost: ${haiku_cost:.4f}")
        return

    if new_wines:
        batch_insert("wines", new_wines, batch_size=500)

    # ── PHASE 5: Vintages, scores, prices, grape links ──
    print(f"\n--- Phase 5: Vintages, scores, prices, grape links ---")
    vintage_rows, score_rows, price_rows, grape_rows = [], [], [], []
    vintage_keys, score_keys, price_key_set = set(), set(), set()

    for entry in truly_new:
        if not entry.get("_wineId"):
            continue
        l = entry["listing"]

        for year in entry.get("vintages", set()):
            key = f"{entry['_wineId']}||{year}"
            if key not in vintage_keys:
                vintage_keys.add(key)
                vintage_rows.append({"wine_id": entry["_wineId"], "vintage_year": year})

        if l.get("rating_average") and l.get("rating_count", 0) > 0:
            key = f"{entry['_wineId']}||{l.get('vintage_year', 'nv')}"
            if key not in score_keys:
                score_keys.add(key)
                score_rows.append({
                    "wine_id": entry["_wineId"], "vintage_year": l.get("vintage_year"),
                    "score": l["rating_average"], "score_scale": "5",
                    "publication_id": VIVINO_PUBLICATION_ID, "critic": "Vivino Community",
                    "is_community": True, "rating_count": l["rating_count"],
                    "review_date": TODAY, "url": f"https://www.vivino.com/w/{l.get('vivino_wine_id')}",
                })

        for p in entry.get("prices", []):
            key = f"{entry['_wineId']}||{p.get('vintage_year')}||{p['price_usd']}||{p.get('merchant_name')}"
            if key not in price_key_set:
                price_key_set.add(key)
                price_rows.append({
                    "wine_id": entry["_wineId"], "vintage_year": p.get("vintage_year"),
                    "price_usd": p["price_usd"], "price_original": p.get("price_raw"),
                    "currency": "USD", "price_type": "retail",
                    "source_url": p.get("source_url"),
                    "merchant_name": p.get("merchant_name") or "Vivino Marketplace",
                    "price_date": TODAY,
                })

        if entry.get("_grapeId"):
            grape_rows.append({"wine_id": entry["_wineId"], "grape_id": entry["_grapeId"]})

    if vintage_rows:
        try:
            sb.table("wine_vintages").upsert(vintage_rows, on_conflict="wine_id,vintage_year").execute()
            print(f"  Upserted {len(vintage_rows)} vintages")
        except Exception as e:
            print(f"  Vintage upsert error: {e}")

    if score_rows:
        inserted = 0
        for row in score_rows:
            try:
                sb.table("wine_vintage_scores").insert(row).execute()
                inserted += 1
            except Exception:
                pass
        print(f"  Inserted {inserted}/{len(score_rows)} scores")

    if price_rows:
        inserted = 0
        for row in price_rows:
            try:
                sb.table("wine_vintage_prices").insert(row).execute()
                inserted += 1
            except Exception:
                pass
        print(f"  Inserted {inserted}/{len(price_rows)} prices")

    if grape_rows:
        batch_insert("wine_grapes", grape_rows)

    # ── Final Summary ──
    haiku_cost = (total_haiku_input * 0.8 + total_haiku_output * 4) / 1_000_000
    print("\n=== SUMMARY ===")
    print(f"  Rematched (false negatives recovered): {len(rematched)}")
    print(f"  New producers created: {len(new_producers)}")
    print(f"  New wines created: {len(new_wines)}")
    print(f"  Vintages upserted: {len(vintage_rows)}")
    print(f"  Scores inserted: {len(score_rows)}")
    print(f"  Prices inserted: {len(price_rows)}")
    print(f"  Grape links: {len(grape_rows)}")
    print(f"  Skipped (dedup): {len(skipped_dupes)}")
    print(f"  Haiku cost: ${haiku_cost:.4f}")


if __name__ == "__main__":
    main()
