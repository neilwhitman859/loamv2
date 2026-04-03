"""
Mystery Shopper Readiness Test

Samples random wines from retail staging tables and checks how well
our canonical data can answer a user's query. Measures:
  - Producer findability (can we identify the producer?)
  - Wine findability (can we find the exact wine?)
  - Depth score (vintages, scores, grapes, prices for found wines)
  - Overall readiness (weighted composite)

Usage:
    python -m pipeline.analyze.readiness_test [--size 50] [--source specs]
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


def sample_wines(sb, source, size):
    """Get random wine samples from a retail staging table."""
    import random
    if source == "specs":
        all_ids = []
        offset = 0
        while True:
            r = (sb.table("source_specs")
                 .select("id,name,upc,canonical_wine_id,canonical_producer_id")
                 .range(offset, offset + 999)
                 .execute())
            all_ids.extend(r.data)
            if len(r.data) < 1000:
                break
            offset += 1000
        return random.sample(all_ids, min(size, len(all_ids)))
    elif source == "lcbo":
        all_ids = []
        offset = 0
        while True:
            r = (sb.table("source_lcbo")
                 .select("id,name,upc,canonical_wine_id,canonical_producer_id")
                 .range(offset, offset + 999)
                 .execute())
            all_ids.extend(r.data)
            if len(r.data) < 1000:
                break
            offset += 1000
        return random.sample(all_ids, min(size, len(all_ids)))
    return []


def check_depth(sb, wine_id):
    """Check depth metrics for a canonical wine."""
    if not wine_id:
        return {"vintages": 0, "scores": 0, "grapes": 0, "prices": 0}

    vintages = sb.table("wine_vintages").select("id", count="exact").eq("wine_id", wine_id).execute()
    scores = sb.table("wine_vintage_scores").select("id", count="exact").eq("wine_id", wine_id).execute()
    grapes = sb.table("wine_grapes").select("wine_id", count="exact").eq("wine_id", wine_id).execute()
    prices = sb.table("wine_vintage_prices").select("id", count="exact").eq("wine_id", wine_id).execute()

    return {
        "vintages": vintages.count or 0,
        "scores": scores.count or 0,
        "grapes": grapes.count or 0,
        "prices": prices.count or 0,
    }


def run_test(sb, source="specs", size=50):
    """Run the mystery shopper test."""
    print(f"\nMYSTERY SHOPPER READINESS TEST")
    print(f"Source: {source}, Sample size: {size}")
    print("=" * 60)

    samples = sample_wines(sb, source, size)
    if not samples:
        print("No samples found!")
        return

    producer_found = 0
    wine_found = 0
    has_vintages = 0
    has_scores = 0
    has_grapes = 0
    has_prices = 0
    total_depth_score = 0

    for i, s in enumerate(samples):
        has_producer = s.get("canonical_producer_id") is not None
        has_wine = s.get("canonical_wine_id") is not None

        if has_producer:
            producer_found += 1
        if has_wine:
            wine_found += 1
            depth = check_depth(sb, s["canonical_wine_id"])
            if depth["vintages"] > 0:
                has_vintages += 1
            if depth["scores"] > 0:
                has_scores += 1
            if depth["grapes"] > 0:
                has_grapes += 1
            if depth["prices"] > 0:
                has_prices += 1
            # Depth score: 0-4 points
            ds = min(1, depth["vintages"]) + min(1, depth["scores"]) + min(1, depth["grapes"]) + min(1, depth["prices"])
            total_depth_score += ds

    n = len(samples)
    producer_pct = producer_found / n * 100
    wine_pct = wine_found / n * 100
    avg_depth = total_depth_score / n if n > 0 else 0

    # Readiness score: weighted composite (0-100)
    # 40% wine findability + 20% producer findability + 40% depth
    readiness = (wine_pct * 0.4 + producer_pct * 0.2 + (avg_depth / 4 * 100) * 0.4)

    print(f"\n  Producer found:  {producer_found}/{n} ({producer_pct:.1f}%)")
    print(f"  Wine found:      {wine_found}/{n} ({wine_pct:.1f}%)")
    print(f"  Has vintages:    {has_vintages}/{n} ({has_vintages/n*100:.1f}%)")
    print(f"  Has scores:      {has_scores}/{n} ({has_scores/n*100:.1f}%)")
    print(f"  Has grapes:      {has_grapes}/{n} ({has_grapes/n*100:.1f}%)")
    print(f"  Has prices:      {has_prices}/{n} ({has_prices/n*100:.1f}%)")
    print(f"  Avg depth:       {avg_depth:.2f}/4.0")
    print(f"\n  READINESS SCORE: {readiness:.0f}/100")
    print(f"  (previous: ~8/100 on 2026-04-02)")

    return readiness


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=50)
    parser.add_argument("--source", default="specs", choices=["specs", "lcbo"])
    args = parser.parse_args()

    sb = get_supabase()
    run_test(sb, args.source, args.size)


if __name__ == "__main__":
    main()
