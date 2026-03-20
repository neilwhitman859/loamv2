#!/usr/bin/env python3
"""
Compares Total Wine Lexington Green inventory against Loam wine database.
Read-only -- no DB writes.

Usage:
    python -m pipeline.analyze.tw_compare
"""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx

from pipeline.lib.db import get_env

SUPABASE_URL = get_env("SUPABASE_URL")
SUPABASE_KEY = get_env("SUPABASE_SERVICE_ROLE")
JSONL_FILE = Path(__file__).resolve().parents[2] / "totalwine_lexington_green.jsonl"


def pg_query(endpoint: str, params: dict | None = None) -> dict:
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    r = httpx.get(url, params=params or {}, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact",
    }, timeout=30)
    return {"data": r.json(), "count": r.headers.get("content-range")}


def normalize_tw(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[,.''\-\u2013\u2014]", " ", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b(20[12]\d|19\d{2})\b", "", s)
    return s.strip()


def trigrams(s: str) -> set[str]:
    padded = f"  {s} "
    return {padded[i:i + 3] for i in range(len(padded) - 2)}


def main():
    # Load TW data
    lines = JSONL_FILE.read_text(encoding="utf-8").strip().split("\n")
    tw_wines = [json.loads(l) for l in lines]
    print(f"Total Wine inventory: {len(tw_wines)} wines\n")

    # Load Loam producers
    print("Loading Loam producers...")
    all_producers = []
    offset = 0
    PAGE = 1000
    while True:
        result = pg_query("producers", {
            "select": "id,name,name_normalized",
            "deleted_at": "is.null", "order": "name",
            "offset": str(offset), "limit": str(PAGE),
        })
        all_producers.extend(result["data"])
        if len(result["data"]) < PAGE:
            break
        offset += PAGE
    print(f"Loaded {len(all_producers)} Loam producers")

    # Build lookup
    producer_by_norm = {}
    for p in all_producers:
        producer_by_norm[normalize_tw(p["name"])] = p

    producer_names = sorted(producer_by_norm.keys(), key=len, reverse=True)

    # Match TW wines to producers
    print("Matching TW wines to Loam producers...\n")
    producer_matches = 0
    producer_misses = 0
    matched_producer_ids = set()
    unmatched_producers: dict[str, int] = {}
    tw_with_producer = []

    for tw in tw_wines:
        tw_norm = normalize_tw(tw["name"])
        matched = False
        for pn in producer_names:
            if tw_norm.startswith(pn + " ") or tw_norm == pn:
                producer = producer_by_norm[pn]
                wine_part = tw_norm[len(pn):].strip()
                tw_with_producer.append({"tw": tw, "producer": producer, "wine_part": wine_part})
                matched_producer_ids.add(producer["id"])
                producer_matches += 1
                matched = True
                break
        if not matched:
            producer_misses += 1
            prefix = " ".join(tw["name"].split()[:2])
            unmatched_producers[prefix] = unmatched_producers.get(prefix, 0) + 1

    print(f"Producer matching:")
    print(f"  Matched: {producer_matches} / {len(tw_wines)} ({producer_matches / len(tw_wines) * 100:.1f}%)")
    print(f"  Unmatched: {producer_misses}")
    print(f"  Unique Loam producers matched: {len(matched_producer_ids)}")

    top_unmatched = sorted(unmatched_producers.items(), key=lambda x: -x[1])[:20]
    print(f"\n  Top unmatched producer prefixes:")
    for prefix, count in top_unmatched:
        print(f'    "{prefix}" -- {count} wines')

    # Match wines within producers
    print("\nMatching wines within matched producers...")
    loam_wines_by_producer: dict[str, list] = {}
    producer_id_list = list(matched_producer_ids)

    for i in range(0, len(producer_id_list), 50):
        batch = producer_id_list[i:i + 50]
        id_filter = f"in.({','.join(batch)})"
        result = pg_query("wines", {
            "select": "name,name_normalized,producer_id",
            "deleted_at": "is.null", "producer_id": id_filter, "limit": "10000",
        })
        for w in result["data"]:
            loam_wines_by_producer.setdefault(w["producer_id"], []).append(w)
        print(f"  Fetched wines for {min(i + 50, len(producer_id_list))}/{len(producer_id_list)} producers", end="\r", flush=True)
    print()

    full_matches = 0
    fuzzy_matches = 0
    wine_only_misses = 0
    full_match_list = []
    fuzzy_match_list = []
    missed_wines = []

    for entry in tw_with_producer:
        tw = entry["tw"]
        producer = entry["producer"]
        wine_part = entry["wine_part"]
        producer_wines = loam_wines_by_producer.get(producer["id"], [])

        exact_match = next(
            (w for w in producer_wines
             if normalize_tw(w["name"]) == wine_part or w.get("name_normalized") == wine_part),
            None,
        )
        if exact_match:
            full_matches += 1
            full_match_list.append({"tw": tw["name"], "loam": f"{producer['name']} -- {exact_match['name']}", "price": tw.get("price")})
            continue

        best_fuzzy = None
        best_score = 0.0
        for w in producer_wines:
            w_norm = normalize_tw(w["name"])
            trg_a = trigrams(wine_part)
            trg_b = trigrams(w_norm)
            intersection = len(trg_a & trg_b)
            union = len(trg_a | trg_b)
            sim = intersection / union if union else 0
            if sim > best_score:
                best_score = sim
                best_fuzzy = w

        if best_score >= 0.4:
            fuzzy_matches += 1
            fuzzy_match_list.append({
                "tw": tw["name"], "loam": f"{producer['name']} -- {best_fuzzy['name']}",
                "score": f"{best_score:.2f}", "price": tw.get("price"),
            })
        else:
            wine_only_misses += 1
            missed_wines.append({"name": tw["name"], "producer": producer["name"], "wine_part": wine_part, "price": tw.get("price")})

    total_matched = full_matches + fuzzy_matches
    total_unmatched = producer_misses + wine_only_misses

    print("\n" + "=" * 40)
    print("   TOTAL WINE vs LOAM COMPARISON")
    print("=" * 40 + "\n")
    print(f"Total Wine Lexington Green: {len(tw_wines)} wines")
    print(f"Loam Database: {len(all_producers)} producers\n")
    print(f"MATCHING RESULTS:")
    print(f"  Exact matches:       {full_matches} ({full_matches / len(tw_wines) * 100:.1f}%)")
    print(f"  Fuzzy matches:       {fuzzy_matches} ({fuzzy_matches / len(tw_wines) * 100:.1f}%)")
    print(f"  Total matched:       {total_matched} ({total_matched / len(tw_wines) * 100:.1f}%)")
    print(f"  No match (wine):     {wine_only_misses}")
    print(f"  No match (producer): {producer_misses}")
    print(f"  Total unmatched:     {total_unmatched} ({total_unmatched / len(tw_wines) * 100:.1f}%)\n")

    matched_prices = [float(str(m.get("price", "").replace("$", ""))) for m in full_match_list + fuzzy_match_list if m.get("price")]
    matched_prices = [p for p in matched_prices if p > 0]
    if matched_prices:
        matched_prices.sort()
        avg = sum(matched_prices) / len(matched_prices)
        median = matched_prices[len(matched_prices) // 2]
        print(f"PRICE ANALYSIS:")
        print(f"  Matched wines avg price:   ${avg:.2f} (median ${median:.2f})")

    print(f"\nSAMPLE UNMATCHED WINES (producer not in Loam):")
    for prefix, count in top_unmatched[:15]:
        print(f'  "{prefix}..." -- {count} wines')

    print(f"\nSAMPLE FUZZY MATCHES (review quality):")
    for m in fuzzy_match_list[:15]:
        print(f'  TW: "{m["tw"]}" -> Loam: "{m["loam"]}" (score: {m["score"]}) {m.get("price", "")}')


if __name__ == "__main__":
    main()
