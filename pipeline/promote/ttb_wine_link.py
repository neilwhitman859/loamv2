"""
Link TTB COLA records to canonical wines via exact fanciful_name match.

Uses _tmp_wine_match table (must be rebuilt first) and processes per-producer
to avoid full-table scans that timeout.

Usage:
    python -m pipeline.promote.ttb_wine_link [--dry-run]
"""

import sys
import time
from pathlib import Path

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
    wine_map = {}  # producer_id -> {name_upper: wine_id}
    offset = 0
    while True:
        result = (sb.table("_tmp_wine_match")
                  .select("producer_id,name_upper,wine_id")
                  .range(offset, offset + 999)
                  .execute())
        for row in result.data:
            pid = row["producer_id"]
            if pid not in wine_map:
                wine_map[pid] = {}
            wine_map[pid][row["name_upper"]] = row["wine_id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    total_wines = sum(len(v) for v in wine_map.values())
    print(f"  {total_wines:,} wines across {len(wine_map):,} producers", flush=True)

    # Use wine_map keys as the producer list — only these can match
    producer_ids = list(wine_map.keys())
    print(f"  {len(producer_ids):,} producers to check", flush=True)

    total_linked = 0
    producers_matched = 0
    errors = 0
    t0 = time.time()

    for pi, pid in enumerate(producer_ids):
        wines_for_producer = wine_map[pid]

        # Get unlinked TTB records for this producer (paginate to get all fanciful names)
        fanciful_names = {}  # upper -> original
        try:
            ttb_offset = 0
            while True:
                for attempt in range(3):
                    try:
                        result = (sb.table("source_ttb_colas")
                                  .select("fanciful_name")
                                  .eq("canonical_producer_id", pid)
                                  .is_("canonical_wine_id", "null")
                                  .not_.is_("fanciful_name", "null")
                                  .range(ttb_offset, ttb_offset + 999)
                                  .execute())
                        break
                    except Exception:
                        if attempt < 2:
                            time.sleep(2 ** attempt)
                            continue
                        raise
                for row in result.data:
                    fn = (row.get("fanciful_name") or "").strip()
                    if fn:
                        fanciful_names[fn.upper()] = fn
                if len(result.data) < 1000:
                    break
                ttb_offset += 1000
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error loading TTB for {pid}: {e}", flush=True)
            continue

        if not fanciful_names:
            continue

        # Match each unique fanciful name to a wine
        matched_in_producer = 0
        for fn_upper, fn_original in fanciful_names.items():
            wine_id = wines_for_producer.get(fn_upper)
            if not wine_id:
                continue

            if args.dry_run:
                matched_in_producer += 1
                continue

            for attempt in range(3):
                try:
                    sb.table("source_ttb_colas").update({
                        "canonical_wine_id": wine_id
                    }).eq("canonical_producer_id", pid).eq(
                        "fanciful_name", fn_original
                    ).is_("canonical_wine_id", "null").execute()
                    matched_in_producer += 1
                    break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    errors += 1
                    if errors <= 10:
                        print(f"  Error updating {pid}/{fn_upper}: {e}", flush=True)
                    break

        if matched_in_producer > 0:
            total_linked += matched_in_producer
            producers_matched += 1

        if (pi + 1) % 2000 == 0:
            elapsed = time.time() - t0
            rate = (pi + 1) / elapsed if elapsed > 0 else 0
            print(f"  {pi+1:,}/{len(producer_ids):,} producers "
                  f"({producers_matched:,} with matches, {total_linked:,} wine-name pairs linked) "
                  f"[{rate:.0f} prod/s, {elapsed:.0f}s]", flush=True)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s", flush=True)
    print(f"  Producers with matches: {producers_matched:,}", flush=True)
    print(f"  Wine-name pairs linked: {total_linked:,}", flush=True)
    print(f"  Errors: {errors}", flush=True)
    if args.dry_run:
        print("  (DRY RUN — no updates written)", flush=True)


if __name__ == "__main__":
    main()
