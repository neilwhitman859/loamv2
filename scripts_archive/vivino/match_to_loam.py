"""
Two-pass matching of Vivino listings against the Loam wine catalog.
  Pass 1: Exact normalized name matching (free)
  Pass 2: Haiku fuzzy matching for unmatched listings

After matching, inserts:
  - New vintages into wine_vintages (upsert)
  - Community scores into wine_vintage_scores (Vivino ratings)
  - Retail prices into wine_vintage_prices

Usage:
    python -m pipeline.vivino.match_to_loam
    python -m pipeline.vivino.match_to_loam --input vivino_full.json
    python -m pipeline.vivino.match_to_loam --dry-run
    python -m pipeline.vivino.match_to_loam --skip-haiku
"""

import sys
import json
import time
import argparse
from pathlib import Path
from datetime import date

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, get_env, batch_upsert, batch_insert
from pipeline.lib.normalize import normalize

VIVINO_PUBLICATION_ID = "ed228eae-c3bf-41e6-9a90-d78c8efaf97e"
TODAY = date.today().isoformat()

# ── Country aliases ──────────────────────────────────────────
COUNTRY_ALIASES = {
    "united states": "united states",
    "usa": "united states",
    "us": "united states",
    "uk": "united kingdom",
    "great britain": "united kingdom",
}


def normalize_country(name: str) -> str:
    if not name:
        return ""
    n = normalize(name)
    return COUNTRY_ALIASES.get(n, n)


def levenshtein(a: str, b: str) -> int:
    m, n = len(a), len(b)
    if m == 0:
        return n
    if n == 0:
        return m
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
    return dp[m][n]


def fetch_all(table: str, columns: str = "*", batch_size: int = 1000) -> list[dict]:
    sb = get_supabase()
    rows = []
    offset = 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size
    return rows


def haiku_match(batch: list[dict], api_key: str) -> dict:
    prompt_parts = []
    for i, item in enumerate(batch):
        vivino = item["vivino"]
        lines = [f'[{i}] Vivino: "{vivino.get("winery_name", "")}" — "{vivino.get("wine_name", "")}" ({vivino.get("country_name", "?")})']
        lines.append("    Candidates:")
        for j, c in enumerate(item["candidates"]):
            letter = chr(65 + j)
            lines.append(f'      {letter}) Producer: "{c["producerName"]}" — Wine: "{c["wineName"]}" ({c["countryName"]})')
        prompt_parts.append("\n".join(lines))

    prompt = "\n\n".join(prompt_parts)

    system_msg = """You are a wine catalog matcher. For each Vivino listing, determine if any Loam candidate is the same wine. Account for:
- Different transliterations (Château vs Chateau, ü vs u)
- Abbreviated vs full names (Dr. vs Doktor, Dom. vs Domaine)
- Minor name variations (adding/dropping "Estate", "Wines", "Winery")
- The wine name may be a subset (e.g., Vivino "Pinot Noir" matches Loam "Pinot Noir Reserve" if same producer)

Reply with JSON array. For each listing index, return the candidate letter (A/B/C) or "none".
Example: [{"index":0,"match":"A"},{"index":1,"match":"none"}]"""

    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1000,
            "messages": [
                {"role": "user", "content": system_msg + "\n\n" + prompt},
                {"role": "assistant", "content": "["},
            ],
        },
        timeout=60,
    )
    resp.raise_for_status()
    msg = resp.json()
    text = "[" + (msg["content"][0].get("text", "") if msg.get("content") else "")
    cleaned = text.replace("```json", "").replace("```", "").strip()

    return {
        "results": json.loads(cleaned),
        "inputTokens": msg.get("usage", {}).get("input_tokens", 0),
        "outputTokens": msg.get("usage", {}).get("output_tokens", 0),
    }


def main():
    parser = argparse.ArgumentParser(description="Match Vivino listings to Loam catalog")
    parser.add_argument("--input", default="vivino_full.json", help="Input file (JSON or JSONL)")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert anything")
    parser.add_argument("--skip-haiku", action="store_true", help="Pass 1 only")
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY", required=not args.skip_haiku)
    sb = get_supabase()

    print("=== Vivino -> Loam Catalog Matcher ===\n")

    # 1. Load Vivino listings
    input_file = args.input
    if input_file.endswith(".jsonl"):
        with open(input_file, "r", encoding="utf-8") as f:
            listings = [json.loads(line) for line in f if line.strip()]
    else:
        listings = json.loads(Path(input_file).read_text(encoding="utf-8"))
    print(f"Loaded {len(listings)} Vivino listings from {input_file}")

    # 2. Load Loam producers + wines
    print("Loading Loam catalog...")
    producers = fetch_all("producers", "id, name, country:countries(name)")
    print(f"  {len(producers)} producers")

    wines = fetch_all("wines", "id, name, producer_id, country:countries(name)")
    print(f"  {len(wines)} wines")

    # 3. Build lookup maps
    producer_map: dict[str, list[dict]] = {}
    for p in producers:
        key = normalize(p["name"])
        producer_map.setdefault(key, []).append({
            "id": p["id"],
            "name": p["name"],
            "countryName": (p.get("country") or {}).get("name", ""),
        })

    wine_map: dict[str, dict] = {}
    wines_by_producer: dict[str, list[dict]] = {}
    for w in wines:
        norm_name = normalize(w["name"])
        key = f"{w['producer_id']}||{norm_name}"
        wine_map[key] = w
        wines_by_producer.setdefault(w["producer_id"], []).append({
            "id": w["id"],
            "name": w["name"],
            "normalized": norm_name,
            "countryName": (w.get("country") or {}).get("name", ""),
        })

    all_producer_names = list(producer_map.keys())

    print(f"  Producer lookup: {len(producer_map)} normalized names")
    print(f"  Wine lookup: {len(wine_map)} (producer, wine) pairs\n")

    # ── Pass 1: Exact normalized match ──────────────────────
    print("--- Pass 1: Exact normalized matching ---")
    matched = []
    unmatched = []

    for listing in listings:
        if not listing.get("winery_name") or not listing.get("wine_name"):
            unmatched.append(listing)
            continue

        norm_winery = normalize(listing["winery_name"])
        norm_wine = normalize(listing["wine_name"])
        norm_country = normalize_country(listing.get("country_name", ""))

        candidates = producer_map.get(norm_winery)
        if not candidates:
            unmatched.append(listing)
            continue

        producer = candidates[0]
        if len(candidates) > 1 and norm_country:
            country_match = next((p for p in candidates if normalize_country(p["countryName"]) == norm_country), None)
            if country_match:
                producer = country_match

        wine_key = f"{producer['id']}||{norm_wine}"
        wine = wine_map.get(wine_key)

        if not wine:
            producer_wines = wines_by_producer.get(producer["id"], [])
            wine = next(
                (w for w in producer_wines if w["normalized"] in norm_wine or norm_wine in w["normalized"]),
                None,
            )

        if wine:
            matched.append({"listing": listing, "producerId": producer["id"], "wineId": wine["id"]})
        else:
            unmatched.append(listing)

    pct = round(len(matched) / len(listings) * 100) if listings else 0
    print(f"  Exact matches: {len(matched)}/{len(listings)} ({pct}%)")
    print(f"  Unmatched: {len(unmatched)}\n")

    # ── Pass 2: Haiku fuzzy match ────────────────────────────
    haiku_matched = []
    haiku_tokens = {"input": 0, "output": 0}

    if not args.skip_haiku and unmatched:
        print("--- Pass 2: Haiku fuzzy matching ---")
        unmatched_with_candidates = []

        for listing in unmatched:
            if not listing.get("winery_name"):
                continue
            norm_winery = normalize(listing["winery_name"])

            scored = [
                {"name": name, "dist": levenshtein(norm_winery, name)}
                for name in all_producer_names
            ]
            scored = [s for s in scored if s["dist"] <= max(len(norm_winery) * 0.4, 5)]
            scored.sort(key=lambda s: s["dist"])
            scored = scored[:3]

            if not scored:
                continue

            candidates = []
            for s in scored:
                prods = producer_map[s["name"]]
                prod = prods[0]
                prod_wines = wines_by_producer.get(prod["id"], [])
                norm_wine = normalize(listing.get("wine_name", ""))
                best_wine = None
                if prod_wines:
                    best_wine = min(prod_wines, key=lambda w: levenshtein(norm_wine, w["normalized"]))

                candidates.append({
                    "producerName": prod["name"],
                    "producerId": prod["id"],
                    "wineName": best_wine["name"] if best_wine else "(no matching wine)",
                    "wineId": best_wine["id"] if best_wine else None,
                    "countryName": prod["countryName"],
                })

            unmatched_with_candidates.append({"vivino": listing, "candidates": candidates})

        print(f"  Listings with candidates: {len(unmatched_with_candidates)}")

        BATCH_SIZE = 10
        haiku_processed = 0

        for i in range(0, len(unmatched_with_candidates), BATCH_SIZE):
            batch = unmatched_with_candidates[i:i + BATCH_SIZE]
            try:
                result = haiku_match(batch, api_key)
                haiku_tokens["input"] += result["inputTokens"]
                haiku_tokens["output"] += result["outputTokens"]

                for r in result["results"]:
                    if r.get("match") == "none" or not r.get("match"):
                        continue
                    idx = r.get("index")
                    if idx is None or idx >= len(batch):
                        continue
                    item = batch[idx]
                    cand_idx = ord(r["match"][0]) - 65
                    if cand_idx < 0 or cand_idx >= len(item["candidates"]):
                        continue
                    cand = item["candidates"][cand_idx]
                    if cand["wineId"]:
                        haiku_matched.append({
                            "listing": item["vivino"],
                            "producerId": cand["producerId"],
                            "wineId": cand["wineId"],
                        })

                haiku_processed += len(batch)
                print(f"\r  Haiku: {haiku_processed}/{len(unmatched_with_candidates)} processed, {len(haiku_matched)} matched", end="", flush=True)

                if i + BATCH_SIZE < len(unmatched_with_candidates):
                    time.sleep(0.2)
            except Exception as err:
                print(f"\n  Haiku batch error at {i}: {err}")

        haiku_cost = (haiku_tokens["input"] * 0.8 + haiku_tokens["output"] * 4) / 1_000_000
        print(f"\n  Haiku matches: {len(haiku_matched)}")
        print(f"  Haiku tokens: {haiku_tokens['input']:,} in / {haiku_tokens['output']:,} out -- ${haiku_cost:.4f}")

    # ── Summary ──────────────────────────────────────────────
    all_matched = matched + haiku_matched
    final_unmatched = [l for l in unmatched if not any(h["listing"] is l for h in haiku_matched)]

    print("\n=== RESULTS ===")
    print(f"  Total listings: {len(listings)}")
    print(f"  Pass 1 (exact): {len(matched)}")
    print(f"  Pass 2 (Haiku): {len(haiku_matched)}")
    total_pct = round(len(all_matched) / len(listings) * 100) if listings else 0
    print(f"  Total matched: {len(all_matched)} ({total_pct}%)")
    print(f"  Unmatched: {len(final_unmatched)}")

    matched_with_price = [m for m in all_matched if m["listing"].get("price_usd") is not None]
    if matched_with_price:
        prices = sorted(m["listing"]["price_usd"] for m in matched_with_price)
        print(f"\n  Matched with price: {len(matched_with_price)}")
        print(f"  Price range: ${prices[0]} -- ${prices[-1]}")
        print(f"  Median: ${prices[len(prices) // 2]}")

    print("\n  Sample matches:")
    for m in all_matched[:10]:
        p = m["listing"]
        price_str = f"${p['price_usd']}" if p.get("price_usd") else "no price"
        print(f"    {p.get('winery_name')} -- {p.get('wine_name')} -> matched ({price_str})")

    print("\n  Top unmatched (by rating count):")
    final_unmatched.sort(key=lambda l: l.get("rating_count", 0), reverse=True)
    for l in final_unmatched[:10]:
        print(f"    {l.get('winery_name')} -- {l.get('wine_name')} ({l.get('country_name')}, {l.get('rating_count', 0)} ratings)")

    # ── DB Writes ──────────────────────────────────────────────
    if args.dry_run:
        print("\n[DRY RUN] Skipping all DB inserts.")
        with_vintage = [m for m in all_matched if m["listing"].get("vintage_year")]
        with_rating = [m for m in all_matched if m["listing"].get("rating_average")]
        print(f"  Would upsert {len(with_vintage)} vintages")
        print(f"  Would insert {len(with_rating)} scores")
        print(f"  Would insert {len(matched_with_price)} prices")
    else:
        # 1. Upsert vintages
        vintage_rows = []
        vintage_keys = set()
        for m in all_matched:
            if not m["listing"].get("vintage_year"):
                continue
            key = f"{m['wineId']}||{m['listing']['vintage_year']}"
            if key not in vintage_keys:
                vintage_keys.add(key)
                vintage_rows.append({
                    "wine_id": m["wineId"],
                    "vintage_year": m["listing"]["vintage_year"],
                })

        if vintage_rows:
            print(f"\n--- Upserting {len(vintage_rows)} vintages ---")
            count = batch_upsert("wine_vintages", vintage_rows, on_conflict="wine_id,vintage_year", batch_size=500)
            print(f"  Vintages upserted: {count}")

        # 2. Insert scores
        score_rows = []
        score_keys = set()
        for m in all_matched:
            if not m["listing"].get("rating_average") or not m["listing"].get("rating_count"):
                continue
            key = f"{m['wineId']}||{m['listing'].get('vintage_year')}||{VIVINO_PUBLICATION_ID}"
            if key not in score_keys:
                score_keys.add(key)
                score_rows.append({
                    "wine_id": m["wineId"],
                    "vintage_year": m["listing"].get("vintage_year"),
                    "score": m["listing"]["rating_average"],
                    "score_scale": "5",
                    "publication_id": VIVINO_PUBLICATION_ID,
                    "critic": "Vivino Community",
                    "is_community": True,
                    "rating_count": m["listing"]["rating_count"],
                    "review_date": TODAY,
                    "url": f"https://www.vivino.com/w/{m['listing'].get('vivino_wine_id')}",
                })

        if score_rows:
            print(f"\n--- Inserting {len(score_rows)} community scores ---")
            count = batch_insert("wine_vintage_scores", score_rows, batch_size=500)
            print(f"  Scores inserted: {count}")

        # 3. Insert prices
        price_rows_raw = []
        for m in matched_with_price:
            price_rows_raw.append({
                "wine_id": m["wineId"],
                "vintage_year": m["listing"].get("vintage_year"),
                "price_usd": m["listing"]["price_usd"],
                "price_original": m["listing"].get("price_raw"),
                "currency": "USD",
                "price_type": "retail",
                "source_url": m["listing"].get("source_url"),
                "merchant_name": m["listing"].get("merchant_name") or "Vivino Marketplace",
                "price_date": TODAY,
            })

        price_rows = []
        price_keys = set()
        for p in price_rows_raw:
            key = f"{p['wine_id']}||{p['vintage_year']}||{p['price_usd']}||{p['merchant_name']}"
            if key not in price_keys:
                price_keys.add(key)
                price_rows.append(p)

        if price_rows:
            print(f"\n--- Inserting {len(price_rows)} price records (deduped from {len(price_rows_raw)}) ---")
            count = batch_insert("wine_vintage_prices", price_rows, batch_size=500)
            print(f"  Prices inserted: {count}")

        print("\n--- DB Summary ---")
        print(f"  Vintages: {len(vintage_rows)} upserted")
        print(f"  Scores: {len(score_rows)} inserted")
        print(f"  Prices: {len(price_rows)} inserted")

    # Save unmatched
    Path("vivino_unmatched.json").write_text(json.dumps(final_unmatched, indent=2), encoding="utf-8")
    print(f"\nSaved {len(final_unmatched)} unmatched listings to vivino_unmatched.json")


if __name__ == "__main__":
    main()
