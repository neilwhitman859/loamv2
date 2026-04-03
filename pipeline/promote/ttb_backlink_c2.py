"""
TTB Back-link: Link unmatched TTB records to existing canonical wines.

Uses _tier_c2_pending helper table (pre-computed producer+fanciful_name → wine_id pairs).
Processes per-producer to use the indexed canonical_producer_id column.

Usage:
    python -m pipeline.promote.ttb_backlink_c2 [--dry-run]
"""

import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load the pre-computed matching pairs from _tier_c2_pending
    print("Loading _tier_c2_pending...")
    pairs = defaultdict(dict)  # producer_id -> {fanciful_name: wine_id}
    offset = 0
    while True:
        result = sb.table("_tier_c2_pending").select("producer_id,fanciful_name,wine_id").range(offset, offset + 999).execute()
        for r in result.data:
            pairs[r["producer_id"]][r["fanciful_name"]] = r["wine_id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    total_pairs = sum(len(v) for v in pairs.values())
    print(f"  {total_pairs:,} pairs across {len(pairs):,} producers")

    if args.dry_run:
        print("DRY RUN — no writes")
        return

    # Process per-producer: for each (producer_id, fanciful_name), update matching TTB records
    total_linked = 0
    errors = 0
    t0 = time.time()

    producer_list = list(pairs.keys())
    for i, pid in enumerate(producer_list):
        wine_map = pairs[pid]

        for fanciful_name, wine_id in wine_map.items():
            try:
                result = (sb.table("source_ttb_colas")
                          .update({"canonical_wine_id": wine_id})
                          .eq("canonical_producer_id", pid)
                          .eq("fanciful_name", fanciful_name)
                          .is_("canonical_wine_id", "null")
                          .execute())
                cnt = len(result.data) if result.data else 0
                total_linked += cnt
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error: {e}")

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            rate = total_linked / elapsed if elapsed > 0 else 0
            print(f"  {i + 1:,}/{len(producer_list):,} producers "
                  f"({total_linked:,} records linked, {rate:.0f}/s)")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  {total_linked:,} TTB records linked to wines")
    print(f"  {errors} errors")


if __name__ == "__main__":
    main()
