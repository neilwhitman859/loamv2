"""
Create wines for Phase B producers from TTB fanciful names.

Processes per-producer (uses the indexed canonical_producer_id column).
Much faster than full-table scans.

Usage:
    python -m pipeline.promote.phase_b_wines [--dry-run]
"""

import sys
import time
import re
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load Phase B producer IDs (recently created, producer_type='estate')
    print("Loading Phase B producers...")
    new_producers = []
    offset = 0
    while True:
        result = (sb.table("producers")
                  .select("id,name,country_id")
                  .eq("producer_type", "estate")
                  .is_("deleted_at", "null")
                  .gte("created_at", "2026-04-03")
                  .range(offset, offset + 999)
                  .execute())
        new_producers.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(new_producers)} Phase B producers")

    # Load existing wines for these producers (for dedup)
    print("Loading existing wines for Phase B producers...")
    existing_wines = set()
    producer_ids = [p["id"] for p in new_producers]
    for i in range(0, len(producer_ids), 50):
        batch = producer_ids[i:i + 50]
        result = (sb.table("wines")
                  .select("producer_id,name_normalized")
                  .in_("producer_id", batch)
                  .is_("deleted_at", "null")
                  .execute())
        for w in result.data:
            existing_wines.add((w["producer_id"], w["name_normalized"]))
    print(f"  {len(existing_wines)} existing wines")

    # Process per-producer: get fanciful names from TTB
    wines_created = 0
    wines_skipped = 0
    errors = 0
    slug_counter = 0
    t0 = time.time()

    for pi, producer in enumerate(new_producers):
        pid = producer["id"]
        pname = producer["name"]
        country_id = producer.get("country_id")

        # Get unique fanciful names for this producer
        result = (sb.table("source_ttb_colas")
                  .select("fanciful_name")
                  .eq("canonical_producer_id", pid)
                  .is_("canonical_wine_id", "null")
                  .not_.is_("fanciful_name", "null")
                  .limit(200)
                  .execute())

        fanciful_names = set()
        for r in result.data:
            fn = (r.get("fanciful_name") or "").strip()
            if fn:
                fanciful_names.add(fn)

        # Create wines for each unique fanciful name
        for fn in fanciful_names:
            name_norm = normalize(fn)
            if not name_norm or len(name_norm) < 2:
                continue
            if (pid, name_norm) in existing_wines:
                wines_skipped += 1
                continue

            slug_counter += 1
            display_name = fn.title() if fn == fn.upper() else fn

            if args.dry_run:
                wines_created += 1
                existing_wines.add((pid, name_norm))
                continue

            try:
                result = sb.table("wines").insert({
                    "name": display_name,
                    "name_normalized": name_norm,
                    "slug": slugify(f"{pname} {fn}")[:190] + f"-pb-{slug_counter}",
                    "producer_id": pid,
                    "country_id": country_id,
                    "wine_type": "table",
                    "effervescence": "still",
                }).execute()
                if result.data:
                    wines_created += 1
                    existing_wines.add((pid, name_norm))
            except Exception as e:
                errors += 1
                if errors <= 5:
                    safe = str(e)[:100].encode('ascii', 'replace').decode('ascii')
                    print(f"  Error: {safe}")

        if (pi + 1) % 200 == 0:
            elapsed = time.time() - t0
            print(f"  {pi+1}/{len(new_producers)} producers, "
                  f"{wines_created} wines created ({elapsed:.0f}s)")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Wines created: {wines_created}")
    print(f"  Wines skipped (existing): {wines_skipped}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    main()
