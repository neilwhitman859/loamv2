"""
Link TTB COLA records to canonical wines via exact fanciful_name match.
V2: Pages through source_ttb_colas directly (using producer+fanciful index),
    matches in-memory wine map, batches updates.

Usage:
    python -m pipeline.promote.ttb_wine_link_v2 [--dry-run]
"""

import sys
import time
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load _tmp_wine_match into memory: (producer_id, name_upper) -> wine_id
    print("Loading _tmp_wine_match...", flush=True)
    wine_map = {}  # (producer_id, name_upper) -> wine_id
    offset = 0
    while True:
        result = (sb.table("_tmp_wine_match")
                  .select("producer_id,name_upper,wine_id")
                  .range(offset, offset + 999)
                  .execute())
        for row in result.data:
            wine_map[(row["producer_id"], row["name_upper"])] = row["wine_id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(wine_map):,} wine entries", flush=True)

    # Get distinct (canonical_producer_id, fanciful_name) pairs from unlinked TTB records
    # This uses the idx_ttb_unlinked_by_producer partial index
    print("Loading unlinked TTB producer-fanciful pairs...", flush=True)
    pairs = {}  # (producer_id, fanciful_upper) -> fanciful_original
    offset = 0
    pages = 0
    while True:
        for attempt in range(3):
            try:
                result = (sb.table("source_ttb_colas")
                          .select("canonical_producer_id,fanciful_name")
                          .not_.is_("canonical_producer_id", "null")
                          .is_("canonical_wine_id", "null")
                          .not_.is_("fanciful_name", "null")
                          .range(offset, offset + 999)
                          .execute())
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                print(f"  Error at offset {offset}: {e}", flush=True)
                result = type('R', (), {'data': []})()
                break

        for row in result.data:
            pid = row["canonical_producer_id"]
            fn = (row.get("fanciful_name") or "").strip()
            if fn:
                key = (pid, fn.upper())
                if key not in pairs:
                    pairs[key] = fn

        pages += 1
        if pages % 100 == 0:
            print(f"  Loaded {offset + len(result.data):,} rows, {len(pairs):,} unique pairs...",
                  flush=True)

        if len(result.data) < 1000:
            break
        offset += 1000

    print(f"  {len(pairs):,} unique (producer, fanciful) pairs from {offset + len(result.data):,} records",
          flush=True)

    # Match against wine_map
    matches = []
    for (pid, fn_upper), fn_original in pairs.items():
        wine_id = wine_map.get((pid, fn_upper))
        if wine_id:
            matches.append((pid, fn_original, wine_id))

    print(f"  {len(matches):,} matchable pairs", flush=True)

    if args.dry_run:
        print("  DRY RUN — no updates", flush=True)
        return

    # Apply updates
    print(f"\nApplying {len(matches):,} updates...", flush=True)
    t0 = time.time()
    updated = 0
    errors = 0

    for i, (pid, fn_original, wine_id) in enumerate(matches):
        for attempt in range(3):
            try:
                sb.table("source_ttb_colas").update({
                    "canonical_wine_id": wine_id
                }).eq("canonical_producer_id", pid).eq(
                    "fanciful_name", fn_original
                ).is_("canonical_wine_id", "null").execute()
                updated += 1
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                errors += 1
                if errors <= 10:
                    print(f"  Error: {e}", flush=True)
                break

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(matches) - i - 1) / rate if rate > 0 else 0
            print(f"  {i+1:,}/{len(matches):,} ({updated:,} updated, {errors} errors) "
                  f"[{rate:.0f}/s, ~{remaining/60:.0f}m left]", flush=True)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s", flush=True)
    print(f"  Wine-name pairs updated: {updated:,}", flush=True)
    print(f"  Errors: {errors}", flush=True)


if __name__ == "__main__":
    main()
