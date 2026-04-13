"""
Vivino Producer Depth Crawl -- fetches complete wine catalogs for all producers.

Phase 1: Resolve Vivino winery IDs from producer names (web page scrape + slug matching)
Phase 2: Fetch all wines per winery via /api/wineries/{id}/wines
Phase 2b: Fetch prices & per-vintage scores via /api/explore/explore
Phase 3: Match & create new wines in the Loam DB

Usage:
    python -m pipeline.vivino.fetch_producer_wines --phase 1
    python -m pipeline.vivino.fetch_producer_wines --phase 2
    python -m pipeline.vivino.fetch_producer_wines --phase 2b
    python -m pipeline.vivino.fetch_producer_wines --phase 3 --dry-run
    python -m pipeline.vivino.fetch_producer_wines --stats
"""

import sys
import json
import re
import time
import uuid
import argparse
from pathlib import Path
from datetime import date

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_supabase, fetch_all, batch_insert
from pipeline.lib.normalize import normalize, slugify

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)

WINERY_MAP_FILE = "producer_winery_map.jsonl"
WINES_DATA_FILE = "producer_wines_data.jsonl"
PRICES_DATA_FILE = "producer_wines_prices.jsonl"
VIVINO_PUBLICATION_ID = "ed228eae-c3bf-41e6-9a90-d78c8efaf97e"
TODAY = date.today().isoformat()

VIVINO_TYPE_MAP = {1: "Red", 2: "White", 3: "Sparkling", 4: "Rose", 7: "Dessert", 24: "Dessert/Port"}
WINE_TYPE_GENERIC_BLEND = {
    "Red": "Red Blend", "White": "White Blend", "Rose": "Rose Blend",
    "Sparkling": "Sparkling Blend", "Dessert": "Dessert Blend", "Dessert/Port": "Port",
}
REGIONAL_DESIGNATION_MAP = {
    "champagne": "Champagne Blend", "port": "Port", "porto": "Port", "prosecco": "Prosecco",
    "cava": "Cava Blend", "chianti": "Chianti Blend", "barolo": "Nebbiolo",
    "barbaresco": "Nebbiolo", "beaujolais": "Beaujolais", "cotes du rhone": "Rhone Blend",
    "chateauneuf du pape": "Rhone Blend", "bordeaux": "Bordeaux Blend",
    "rioja": "Rioja Blend", "sauternes": "Sauternes", "valpolicella": "Valpolicella Blend",
    "amarone": "Valpolicella Blend", "cremant": "Sparkling Blend", "asti": "Moscato",
    "brunello": "Sangiovese", "priorat": "Priorat Blend",
}

PRODUCER_STRIP_WORDS = [
    "winery", "wines", "wine", "vineyards", "vineyard", "estate", "estates",
    "cellars", "cellar", "family", "bodegas", "bodega", "domaine", "dom",
    "chateau", "chateau", "casa", "cantina", "tenuta", "fattoria",
    "azienda", "weingut", "cave", "caves", "maison", "champagne",
]
SLUG_SUFFIXES = ["", "-winery", "-wines", "-vineyards"]


def strip_producer_suffixes(name: str) -> str:
    n = normalize(name)
    for suffix in PRODUCER_STRIP_WORDS:
        n = re.sub(rf"\b{suffix}\b", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def load_jsonl_map(filepath: str, key_field: str) -> dict:
    result = {}
    p = Path(filepath)
    if not p.exists():
        return result
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if obj.get(key_field):
                result[obj[key_field]] = obj
        except Exception:
            pass
    return result


def load_jsonl_array(filepath: str) -> list:
    arr = []
    p = Path(filepath)
    if not p.exists():
        return arr
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            arr.append(json.loads(line))
        except Exception:
            pass
    return arr


def call_haiku(api_key: str, messages: list, max_tokens: int = 2000) -> dict:
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": max_tokens,
            "messages": messages,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "text": (data.get("content") or [{}])[0].get("text", ""),
        "input_tokens": (data.get("usage") or {}).get("input_tokens", 0),
        "output_tokens": (data.get("usage") or {}).get("output_tokens", 0),
    }


# ── PHASE 1 ──────────────────────────────────────────────────

def phase1(args):
    print("=== PHASE 1: Resolve Vivino Winery IDs ===\n")
    sb = get_supabase()
    from pipeline.lib.db import get_env
    producers = fetch_all("producers", "id,name,name_normalized,country_id,slug")
    print(f"  {len(producers)} producers loaded")

    countries = fetch_all("countries", "id,name,iso_code")
    country_by_id = {c["id"]: c for c in countries}

    existing_map = load_jsonl_map(WINERY_MAP_FILE, "producer_id")
    print(f"  {len(existing_map)} existing mappings loaded (resume)\n")

    to_process = producers[:args.limit] if args.limit else producers
    resolved = not_found = skipped = errors = 0
    current_delay = args.delay_ms / 1000.0
    start_time = time.time()

    client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30, follow_redirects=True)
    try:
        for i, producer in enumerate(to_process):
            if producer["id"] in existing_map:
                skipped += 1
                continue

            country = country_by_id.get(producer.get("country_id")) or {}
            country_code = (country.get("iso_code") or "").lower()

            base_slug = slugify(producer["name"])
            stripped_slug = slugify(strip_producer_suffixes(producer["name"]))
            slugs_to_try = set()
            for sfx in SLUG_SUFFIXES:
                slugs_to_try.add(base_slug + sfx)
                if stripped_slug != base_slug and len(stripped_slug) > 2:
                    slugs_to_try.add(stripped_slug + sfx)

            winery_id = winery_name = winery_seo = None
            match_confidence = "none"
            wines_count = 0

            for slug in slugs_to_try:
                if not slug or len(slug) < 2:
                    continue
                try:
                    url = f"https://www.vivino.com/wineries/{slug}"
                    resp = client.get(url)
                    if resp.status_code == 200:
                        html = resp.text
                        id_match = (
                            re.search(r'"winery":\{"id":(\d+)', html) or
                            re.search(r'data-winery-id="(\d+)"', html) or
                            re.search(r'"id":(\d+),"name":"[^"]+","seo_name":"[^"]+"', html)
                        )
                        if id_match:
                            winery_id = int(id_match.group(1))
                            name_m = re.search(r'"winery":\{"id":\d+,"name":"([^"]+)"', html)
                            winery_name = name_m.group(1) if name_m else slug
                            seo_m = re.search(r'"seo_name":"([^"]+)"', html)
                            winery_seo = seo_m.group(1) if seo_m else slug
                            cnt_m = re.search(r'"wines_count":(\d+)', html)
                            wines_count = int(cnt_m.group(1)) if cnt_m else 0

                            nv = normalize(winery_name)
                            np = normalize(producer["name"])
                            if nv == np:
                                match_confidence = "exact"
                            elif nv in np or np in nv:
                                match_confidence = "substring"
                            elif strip_producer_suffixes(winery_name) == strip_producer_suffixes(producer["name"]):
                                match_confidence = "suffix_stripped"
                            else:
                                match_confidence = "slug_match"
                            break
                    time.sleep(0.15)
                except Exception as err:
                    if "429" in str(err):
                        current_delay = min(current_delay * 2, 30)
                        print(f"\n  Rate limited. Backing off to {current_delay*1000:.0f}ms")
                        time.sleep(current_delay)

            record = {
                "producer_id": producer["id"],
                "producer_name": producer["name"],
                "country_code": country_code,
                "vivino_winery_id": winery_id,
                "vivino_winery_name": winery_name,
                "vivino_seo_name": winery_seo,
                "match_confidence": match_confidence,
                "wines_count": wines_count,
            }
            with open(WINERY_MAP_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

            if winery_id:
                resolved += 1
            else:
                not_found += 1

            elapsed = (time.time() - start_time) / 60
            processed = resolved + not_found
            total = len(to_process) - skipped
            print(f"\r  {processed}/{total} ({resolved} resolved, {not_found} not found) [{elapsed:.1f}m]",
                  end="", flush=True)
            time.sleep(max(0.5, current_delay))
    finally:
        client.close()

    print(f"\n\n=== PHASE 1 COMPLETE ===")
    print(f"  Resolved: {resolved}")
    print(f"  Not found: {not_found}")
    print(f"  Skipped (already resolved): {skipped}")
    print(f"  Output: {WINERY_MAP_FILE}\n")

    all_m = load_jsonl_map(WINERY_MAP_FILE, "producer_id")
    conf_dist: dict[str, int] = {}
    for m in all_m.values():
        c = m.get("match_confidence", "none")
        conf_dist[c] = conf_dist.get(c, 0) + 1
    print("  Confidence distribution:", conf_dist)


# ── PHASE 2 ──────────────────────────────────────────────────

def phase2(args):
    print("=== PHASE 2: Fetch Wine Catalogs ===\n")
    winery_map = load_jsonl_map(WINERY_MAP_FILE, "producer_id")
    print(f"  {len(winery_map)} winery mappings loaded")

    resolved = [m for m in winery_map.values() if m.get("vivino_winery_id")]
    print(f"  {len(resolved)} with Vivino winery IDs")

    existing = load_jsonl_array(WINES_DATA_FILE)
    done_ids = {w["vivino_winery_id"] for w in existing if w.get("vivino_winery_id")}
    print(f"  {len(done_ids)} wineries already fetched (resume)\n")

    to_process = [m for m in resolved if m["vivino_winery_id"] not in done_ids]
    if args.limit:
        to_process = to_process[:args.limit]

    fetched = total_wines = errors = 0
    current_delay = args.delay_ms / 1000.0
    start_time = time.time()

    client = httpx.Client(headers={"User-Agent": USER_AGENT, "Accept": "application/json"}, timeout=30)
    try:
        for i, mapping in enumerate(to_process):
            winery_id = mapping["vivino_winery_id"]
            try:
                url = f"https://www.vivino.com/api/wineries/{winery_id}/wines?start_from=0&limit=500"
                resp = client.get(url)
                if resp.status_code == 429:
                    current_delay = min(current_delay * 2, 30)
                    print(f"\n  Rate limited. Backing off to {current_delay*1000:.0f}ms")
                    time.sleep(current_delay)
                    continue
                if not resp.is_success:
                    errors += 1
                    continue

                data = resp.json()
                wines = data.get("wines") or []
                vintages = data.get("vintages") or []

                vintage_by_wine = {}
                for v in vintages:
                    wid = (v.get("wine") or {}).get("id")
                    if not wid:
                        continue
                    existing_v = vintage_by_wine.get(wid)
                    if not existing_v or (v.get("statistics") or {}).get("ratings_count", 0) > (existing_v.get("statistics") or {}).get("ratings_count", 0):
                        vintage_by_wine[wid] = v

                for wine in wines:
                    vintage = vintage_by_wine.get(wine.get("id"))
                    stats = wine.get("statistics") or {}
                    v_stats = (vintage or {}).get("statistics") or {}
                    region = wine.get("region") or {}
                    country = region.get("country") or {}
                    grapes = (vintage or {}).get("grapes") or []

                    record = {
                        "vivino_winery_id": winery_id,
                        "producer_id": mapping["producer_id"],
                        "producer_name": mapping["producer_name"],
                        "vivino_wine_id": wine.get("id"),
                        "wine_name": wine.get("name"),
                        "wine_seo_name": wine.get("seo_name"),
                        "type_id": wine.get("type_id"),
                        "is_natural": wine.get("is_natural") or False,
                        "region_name": region.get("name"),
                        "country_code": country.get("code") or mapping.get("country_code"),
                        "country_name": country.get("name"),
                        "rating_average": stats.get("ratings_average") or v_stats.get("ratings_average"),
                        "rating_count": stats.get("ratings_count") or v_stats.get("ratings_count") or 0,
                        "grapes": [g.get("name") or (g.get("grape") or {}).get("name") for g in grapes if g.get("name") or (g.get("grape") or {}).get("name")],
                        "vintage_year": vintage["year"] if vintage and (vintage.get("year") or 0) > 1900 else None,
                        "wines_count_on_winery": len(wines),
                    }
                    with open(WINES_DATA_FILE, "a", encoding="utf-8") as f:
                        f.write(json.dumps(record) + "\n")
                    total_wines += 1

                fetched += 1
                current_delay = args.delay_ms / 1000.0
                elapsed = (time.time() - start_time) / 60
                print(f"\r  {fetched}/{len(to_process)} wineries, {total_wines} wines [{elapsed:.1f}m]",
                      end="", flush=True)

            except Exception as err:
                errors += 1
                print(f"\n  Error for winery {winery_id}: {err}")

            time.sleep(current_delay)
    finally:
        client.close()

    print(f"\n\n=== PHASE 2 COMPLETE ===")
    print(f"  Wineries fetched: {fetched}")
    print(f"  Total wines found: {total_wines}")
    print(f"  Errors: {errors}")
    print(f"  Output: {WINES_DATA_FILE}\n")


# ── PHASE 2b ─────────────────────────────────────────────────

def phase2b(args):
    print("=== PHASE 2b: Fetch Prices & Per-Vintage Scores ===\n")
    winery_map = load_jsonl_map(WINERY_MAP_FILE, "producer_id")
    resolved = [m for m in winery_map.values() if m.get("vivino_winery_id")]
    print(f"  {len(resolved)} resolved wineries")

    winery_to_producer = {m["vivino_winery_id"]: m["producer_id"] for m in resolved}

    existing = load_jsonl_array(PRICES_DATA_FILE)
    done_ids = {p["vivino_winery_id"] for p in existing if p.get("vivino_winery_id")}
    print(f"  {len(done_ids)} wineries already fetched (resume)\n")

    to_process = [m for m in resolved if m["vivino_winery_id"] not in done_ids]
    if args.limit:
        to_process = to_process[:args.limit]

    batch_size = 5
    total_prices = total_scores = batches_done = errors = 0
    current_delay = args.delay_ms / 1000.0
    start_time = time.time()

    client = httpx.Client(headers={"User-Agent": USER_AGENT, "Accept": "application/json"}, timeout=30)
    try:
        for i in range(0, len(to_process), batch_size):
            batch = to_process[i:i+batch_size]
            winery_ids = [m["vivino_winery_id"] for m in batch]

            try:
                page = 1
                has_more = True
                batch_wineries_written = set()

                while has_more:
                    params = {
                        "min_rating": "1", "order_by": "ratings_count", "order": "desc",
                        "per_page": "50", "page": str(page), "currency_code": "USD", "language": "en",
                    }
                    # httpx handles repeated params differently; build URL manually
                    qs = "&".join(
                        [f"{k}={v}" for k, v in params.items()] +
                        [f"winery_ids[]={wid}" for wid in winery_ids]
                    )
                    url = f"https://www.vivino.com/api/explore/explore?{qs}"
                    resp = client.get(url)

                    if resp.status_code == 429:
                        current_delay = min(current_delay * 2, 30)
                        print(f"\n  Rate limited. Backing off to {current_delay*1000:.0f}ms")
                        time.sleep(current_delay)
                        continue

                    if not resp.is_success:
                        errors += 1
                        break

                    data = resp.json()
                    matches = (data.get("explore_vintage") or {}).get("matches") or []

                    for m in matches:
                        v = m.get("vintage") or {}
                        w = (v.get("wine") or {})
                        if not w.get("id"):
                            continue
                        wid = (w.get("winery") or {}).get("id")
                        producer_id = winery_to_producer.get(wid) if wid else None

                        record = {
                            "vivino_winery_id": wid,
                            "producer_id": producer_id,
                            "vivino_wine_id": w["id"],
                            "wine_name": w.get("name"),
                            "vintage_year": v["year"] if (v.get("year") or 0) > 1900 else None,
                            "rating_average": (v.get("statistics") or {}).get("ratings_average"),
                            "rating_count": (v.get("statistics") or {}).get("ratings_count") or 0,
                            "wine_rating_average": (v.get("statistics") or {}).get("wine_ratings_average"),
                            "wine_rating_count": (v.get("statistics") or {}).get("wine_ratings_count") or 0,
                            "price_usd": (m.get("price") or {}).get("amount"),
                            "price_currency": ((m.get("price") or {}).get("currency") or {}).get("code", "USD"),
                            "price_url": (m.get("price") or {}).get("url"),
                            "price_bottle_type": ((m.get("price") or {}).get("bottle_type") or {}).get("short_name", "bottle"),
                            "price_bottle_ml": ((m.get("price") or {}).get("bottle_type") or {}).get("volume_ml", 750),
                        }
                        with open(PRICES_DATA_FILE, "a", encoding="utf-8") as f:
                            f.write(json.dumps(record) + "\n")
                        if wid:
                            batch_wineries_written.add(wid)
                        if record["price_usd"]:
                            total_prices += 1
                        if record["vintage_year"]:
                            total_scores += 1

                    has_more = len(matches) == 50
                    page += 1
                    if has_more:
                        time.sleep(max(0.5, current_delay / 2))

                batches_done += 1
                current_delay = args.delay_ms / 1000.0

                # Write sentinels for wineries with no data
                for m in batch:
                    if m["vivino_winery_id"] not in done_ids:
                        if m["vivino_winery_id"] not in batch_wineries_written:
                            sentinel = {
                                "vivino_winery_id": m["vivino_winery_id"],
                                "producer_id": m["producer_id"],
                                "vivino_wine_id": None, "wine_name": None, "_no_prices": True,
                            }
                            with open(PRICES_DATA_FILE, "a", encoding="utf-8") as f:
                                f.write(json.dumps(sentinel) + "\n")
                        done_ids.add(m["vivino_winery_id"])

                elapsed = (time.time() - start_time) / 60
                cnt = min(i + batch_size, len(to_process))
                print(f"\r  {cnt}/{len(to_process)} wineries, {total_prices} prices, {total_scores} vintage scores [{elapsed:.1f}m]",
                      end="", flush=True)

            except Exception as err:
                errors += 1
                print(f"\n  Error for batch {winery_ids}: {err}")

            time.sleep(current_delay)
    finally:
        client.close()

    print(f"\n\n=== PHASE 2b COMPLETE ===")
    print(f"  Batches processed: {batches_done}")
    print(f"  Prices found: {total_prices}")
    print(f"  Vintage scores found: {total_scores}")
    print(f"  Errors: {errors}")
    print(f"  Output: {PRICES_DATA_FILE}\n")


# ── PHASE 3 ──────────────────────────────────────────────────

def phase3(args):
    print("=== PHASE 3: Match & Create Wines ===\n")
    from pipeline.lib.db import get_env
    api_key = get_env("ANTHROPIC_API_KEY")
    sb = get_supabase()
    total_haiku_in = total_haiku_out = 0

    raw_listings = load_jsonl_array(WINES_DATA_FILE)
    print(f"Loaded {len(raw_listings)} Vivino wine listings")

    by_wine_id: dict[int, dict] = {}
    for l in raw_listings:
        key = l.get("vivino_wine_id")
        if not key:
            continue
        if key not in by_wine_id or (l.get("rating_count") or 0) > (by_wine_id[key].get("rating_count") or 0):
            by_wine_id[key] = l
    unique_listings = list(by_wine_id.values())
    print(f"Deduped to {len(unique_listings)} unique wines\n")

    print("Loading reference data...")
    countries = fetch_all("countries", "id,name,iso_code")
    country_by_name = {c["name"]: c["id"] for c in countries}
    country_by_code = {c["iso_code"].lower(): c["id"] for c in countries if c.get("iso_code")}

    producers = fetch_all("producers", "id,name,name_normalized,country_id,slug")
    producer_by_id = {p["id"]: p for p in producers}
    print(f"  {len(producers)} producers")

    grapes = fetch_all("grapes", "id,name,aliases,color")
    grape_map: dict[str, dict] = {}
    grape_names: list[str] = []
    for g in grapes:
        grape_map[g["name"].lower()] = {"id": g["id"], "color": g["color"], "name": g["name"]}
        grape_names.append(g["name"].lower())
        for alias in (g.get("aliases") or []):
            grape_map[alias.lower()] = {"id": g["id"], "color": g["color"], "name": g["name"]}
            grape_names.append(alias.lower())
    grape_names.sort(key=len, reverse=True)
    print(f"  {len(grapes)} grapes ({len(grape_map)} names)")

    vcats = fetch_all("varietal_categories", "id,name,color,type,grape_id")
    vcat_by_name = {v["name"]: v["id"] for v in vcats}
    vcat_by_grape_color: dict[str, str] = {}
    vcat_by_grape: dict[str, str] = {}
    for v in vcats:
        if v.get("grape_id"):
            vcat_by_grape_color[f"{v['grape_id']}|{v['color']}"] = v["id"]
            vcat_by_grape.setdefault(v["grape_id"], v["id"])
    print(f"  {len(vcats)} varietal categories")

    regions = fetch_all("regions", "id,country_id,is_catch_all")
    catch_all = {r["country_id"]: r["id"] for r in regions if r.get("is_catch_all")}

    rnm = fetch_all("region_name_mappings", "region_name,country,region_id,appellation_id")
    region_mapping = {f"{r['region_name']}|{r['country']}": {"region_id": r["region_id"], "appellation_id": r.get("appellation_id")} for r in rnm}
    print(f"  {len(rnm)} region name mappings")

    existing_wines = fetch_all("wines", "id,name,name_normalized,producer_id")
    existing_wine_map = {}
    wines_by_producer: dict[str, list] = {}
    for w in existing_wines:
        existing_wine_map[f"{w['producer_id']}||{w['name_normalized']}"] = w
        wines_by_producer.setdefault(w["producer_id"], []).append(w)
    print(f"  {len(existing_wines)} existing wines")

    slug_rows = fetch_all("wines", "slug")
    existing_slugs = {r["slug"] for r in slug_rows}
    print(f"  {len(existing_slugs)} existing wine slugs\n")

    # Match against existing wines
    print("--- Matching against existing catalog ---")
    matched = []
    new_wine_listings = []

    for listing in unique_listings:
        producer = producer_by_id.get(listing.get("producer_id"))
        if not producer:
            continue
        norm_wine = normalize(listing.get("wine_name") or "")
        dedup_key = f"{producer['id']}||{norm_wine}"

        if dedup_key in existing_wine_map:
            matched.append({"listing": listing, "wine_id": existing_wine_map[dedup_key]["id"], "producer_id": producer["id"]})
        else:
            found = False
            for w in (wines_by_producer.get(producer["id"]) or []):
                if w["name_normalized"] in norm_wine or norm_wine in w["name_normalized"]:
                    matched.append({"listing": listing, "wine_id": w["id"], "producer_id": producer["id"]})
                    found = True
                    break
            if not found:
                new_wine_listings.append({"listing": listing, "producer": producer})

    print(f"  Matched to existing wines: {len(matched)}")
    print(f"  New wines to create: {len(new_wine_listings)}\n")

    # Classify varietal categories
    print("--- Classifying varietal categories ---")
    needs_haiku = []

    for entry in new_wine_listings:
        l = entry["listing"]
        wine_type = VIVINO_TYPE_MAP.get(l.get("type_id"), "Red")
        norm_name = normalize(l.get("wine_name") or "").lower()
        entry["wine_type"] = wine_type

        vcat_id = grape_id = None
        method = None

        # From grape array
        if l.get("grapes"):
            primary = l["grapes"][0].lower()
            grape = grape_map.get(primary)
            if grape:
                tc = "red" if wine_type == "Red" else "white" if wine_type == "White" else "rose" if wine_type == "Rose" else grape["color"]
                vcat_id = vcat_by_grape_color.get(f"{grape['id']}|{tc}") or vcat_by_grape.get(grape["id"])
                if vcat_id:
                    grape_id = grape["id"]
                    method = f"grape_array:{grape['name']}"

        # From wine name
        if not vcat_id:
            for gn in grape_names:
                if gn in norm_name:
                    grape = grape_map[gn]
                    tc = "red" if wine_type == "Red" else "white" if wine_type == "White" else "rose" if wine_type == "Rose" else grape["color"]
                    vcat_id = vcat_by_grape_color.get(f"{grape['id']}|{tc}") or vcat_by_grape.get(grape["id"])
                    if vcat_id:
                        grape_id = grape["id"]
                        method = f"grape_name:{grape['name']}"
                        break

        # Regional designation
        if not vcat_id:
            norm_region = normalize(l.get("region_name") or "").lower()
            for rk, vcn in REGIONAL_DESIGNATION_MAP.items():
                if re.search(rf"\b{rk}\b", norm_name) or re.search(rf"\b{rk}\b", norm_region):
                    vcat_id = vcat_by_name.get(vcn)
                    if vcat_id:
                        method = f"regional:{rk}->{vcn}"
                        break

        if not vcat_id:
            needs_haiku.append(entry)

        entry["vcat_id"] = vcat_id
        entry["grape_id"] = grape_id
        entry["vcat_method"] = method

    print(f"  Resolved by grape array: {sum(1 for e in new_wine_listings if (e.get('vcat_method') or '').startswith('grape_array:'))}")
    print(f"  Resolved by grape name: {sum(1 for e in new_wine_listings if (e.get('vcat_method') or '').startswith('grape_name:'))}")
    print(f"  Resolved by regional: {sum(1 for e in new_wine_listings if (e.get('vcat_method') or '').startswith('regional:'))}")
    print(f"  Needs Haiku: {len(needs_haiku)}")

    # Haiku classification
    if needs_haiku:
        vcat_names_str = ", ".join(v["name"] for v in vcats)
        BATCH = 40
        for bi in range(0, len(needs_haiku), BATCH):
            batch = needs_haiku[bi:bi+BATCH]
            prompt_lines = []
            for idx, entry in enumerate(batch):
                l = entry["listing"]
                prompt_lines.append(f'[{idx}] "{l.get("wine_name")}" -- Type: {entry["wine_type"]}, Region: {l.get("region_name") or "?"}({l.get("country_name") or "?"})')

            try:
                result = call_haiku(api_key, [
                    {"role": "user", "content": (
                        "You are a wine classification expert. For each wine, determine the most likely varietal category.\n\n"
                        f"Available categories: {vcat_names_str}\n\n"
                        'Reply JSON array: [{"index":N,"category":"exact name from list"}]\n\n' +
                        "\n".join(prompt_lines)
                    )},
                    {"role": "assistant", "content": "["},
                ])
                total_haiku_in += result["input_tokens"]
                total_haiku_out += result["output_tokens"]

                cleaned = ("[" + result["text"]).replace("```json", "").replace("```", "").strip()
                results = json.loads(cleaned)
                for r in results:
                    idx = r.get("index")
                    if idx is None or idx >= len(batch):
                        continue
                    vc_id = vcat_by_name.get(r.get("category"))
                    if vc_id:
                        batch[idx]["vcat_id"] = vc_id
                        batch[idx]["vcat_method"] = f"haiku:{r['category']}"
            except Exception as err:
                print(f"\n  Haiku error: {err}")

            print(f"\r  Haiku: {min(bi+BATCH, len(needs_haiku))}/{len(needs_haiku)}", end="", flush=True)
            time.sleep(0.2)
        print(f"\n  Haiku resolved: {sum(1 for e in needs_haiku if e.get('vcat_id'))}/{len(needs_haiku)}")

    # Fallback
    for entry in new_wine_listings:
        if not entry.get("vcat_id"):
            generic = WINE_TYPE_GENERIC_BLEND.get(entry.get("wine_type"), "Red Blend")
            entry["vcat_id"] = vcat_by_name.get(generic) or vcat_by_name.get("Red Blend")
            entry["vcat_method"] = f"fallback:{generic}"

    print(f"  Fallback: {sum(1 for e in new_wine_listings if (e.get('vcat_method') or '').startswith('fallback:'))}\n")

    # Resolve regions
    print("--- Resolving regions ---")
    region_hits = catch_all_hits = 0
    for entry in new_wine_listings:
        l = entry["listing"]
        country_id = country_by_code.get((l.get("country_code") or "").lower()) or entry["producer"].get("country_id")
        entry["country_id"] = country_id

        region_id = appellation_id = None
        if l.get("region_name"):
            country_name = l.get("country_name") or next((c["name"] for c in countries if c["id"] == country_id), None)
            rm = region_mapping.get(f"{l['region_name']}|{country_name}")
            if rm:
                region_id = rm["region_id"]
                appellation_id = rm.get("appellation_id")
                region_hits += 1
        if not region_id and country_id:
            region_id = catch_all.get(country_id)
            if region_id:
                catch_all_hits += 1

        entry["region_id"] = region_id
        entry["appellation_id"] = appellation_id

    print(f"  Region mapped: {region_hits}")
    print(f"  Catch-all fallback: {catch_all_hits}\n")

    # Create wine records
    print("--- Creating wine records ---")
    new_wines = []
    for entry in new_wine_listings:
        l = entry["listing"]
        producer = entry["producer"]
        if not entry.get("country_id") or not entry.get("vcat_id"):
            continue

        norm_wine = normalize(l.get("wine_name") or "")
        dedup_key = f"{producer['id']}||{norm_wine}"
        if dedup_key in existing_wine_map:
            continue

        slug = f"{producer.get('slug') or slugify(producer['name'])}-{slugify(l.get('wine_name') or '')}"[:120] or producer.get("slug") or slugify(producer["name"])
        if slug in existing_slugs:
            slug = f"{slug}-vivino"
        if slug in existing_slugs:
            slug = f"{slug}-{str(uuid.uuid4())[:6]}"
        existing_slugs.add(slug)

        wine_id = str(uuid.uuid4())
        wt = entry.get("wine_type", "Red")
        new_wines.append({
            "id": wine_id, "slug": slug, "name": l.get("wine_name"),
            "name_normalized": norm_wine, "producer_id": producer["id"],
            "country_id": entry["country_id"], "region_id": entry.get("region_id"),
            "appellation_id": entry.get("appellation_id"), "varietal_category_id": entry["vcat_id"],
            "effervescence": "sparkling" if wt == "Sparkling" else None,
        })
        entry["wine_id"] = wine_id
        existing_wine_map[dedup_key] = {"id": wine_id}

    print(f"  New wines to create: {len(new_wines)}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would create {len(new_wines)} wines")
        for entry in new_wine_listings[:20]:
            if entry.get("wine_id"):
                l = entry["listing"]
                print(f"    {l.get('producer_name')} -- {l.get('wine_name')} [{entry.get('wine_type')}] vcat: {entry.get('vcat_method')}")
        cost = (total_haiku_in * 0.8 + total_haiku_out * 4) / 1_000_000
        print(f"\n  Haiku cost: ${cost:.4f}")
        return

    if new_wines:
        batch_insert("wines", new_wines, batch_size=500)

    print("\nPhase 3 DB writes complete.")
    cost = (total_haiku_in * 0.8 + total_haiku_out * 4) / 1_000_000
    print(f"\n=== PHASE 3 COMPLETE ===")
    print(f"  Matched to existing: {len(matched)}")
    print(f"  New wines created: {len(new_wines)}")
    print(f"  Haiku cost: ${cost:.4f}")


# ── STATS ────────────────────────────────────────────────────

def show_stats():
    print("=== Pipeline Stats ===\n")
    if Path(WINERY_MAP_FILE).exists():
        mappings = load_jsonl_array(WINERY_MAP_FILE)
        resolved = [m for m in mappings if m.get("vivino_winery_id")]
        conf_dist: dict[str, int] = {}
        for m in mappings:
            c = m.get("match_confidence", "none")
            conf_dist[c] = conf_dist.get(c, 0) + 1
        print(f"Phase 1: {len(mappings)} producers processed")
        print(f"  Resolved: {len(resolved)} ({round(len(resolved)/len(mappings)*100) if mappings else 0}%)")
        print(f"  Confidence: {conf_dist}\n")
    else:
        print("Phase 1: No data yet\n")

    if Path(WINES_DATA_FILE).exists():
        wines = load_jsonl_array(WINES_DATA_FILE)
        winery_ids = {w.get("vivino_winery_id") for w in wines}
        wine_ids = {w.get("vivino_wine_id") for w in wines}
        print(f"Phase 2: {len(wines)} wine records")
        print(f"  Unique wineries: {len(winery_ids)}")
        print(f"  Unique wines: {len(wine_ids)}\n")
    else:
        print("Phase 2: No data yet\n")

    if Path(PRICES_DATA_FILE).exists():
        prices = [p for p in load_jsonl_array(PRICES_DATA_FILE) if not p.get("_no_prices")]
        with_price = [p for p in prices if p.get("price_usd")]
        with_vintage = [p for p in prices if p.get("vintage_year")]
        winery_ids = {p.get("vivino_winery_id") for p in prices}
        print(f"Phase 2b: {len(prices)} explore records")
        print(f"  Unique wineries: {len(winery_ids)}")
        print(f"  With prices: {len(with_price)}")
        print(f"  With vintage year: {len(with_vintage)}\n")
    else:
        print("Phase 2b: No data yet\n")


def main():
    parser = argparse.ArgumentParser(description="Vivino Producer Depth Crawl")
    parser.add_argument("--phase", default="0", help="Phase to run: 1, 2, 2b, 3, or 0 (all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay-ms", type=int, default=1500)
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    phase_raw = args.phase
    print(f"=== Vivino Producer Depth Crawl ===")
    print(f"  Phase: {phase_raw or 'all'}")
    print(f"  Limit: {args.limit or 'none'}")
    print(f"  Delay: {args.delay_ms}ms")
    print(f"  Resume: {args.resume}")
    print(f"  Dry run: {args.dry_run}\n")

    if phase_raw in ("0", "1"):
        phase1(args)
    if phase_raw in ("0", "2"):
        phase2(args)
    if phase_raw in ("0", "2b"):
        phase2b(args)
    if phase_raw in ("0", "3"):
        phase3(args)

    print("\nDone!")


if __name__ == "__main__":
    main()
