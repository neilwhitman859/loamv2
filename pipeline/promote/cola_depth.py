"""
Create wine_vintages and external_ids (COLA) from wine-linked TTB records.

Uses cursor-based pagination (ttb_id > last_seen) instead of OFFSET
to avoid statement timeouts on the 3.28M row table.

Usage:
    python -m pipeline.promote.cola_depth [--dry-run]
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load existing wine_vintages
    print("Loading existing wine_vintages...", flush=True)
    existing_vintages = set()
    offset = 0
    while True:
        result = sb.table("wine_vintages").select("wine_id,vintage_year").range(offset, offset + 999).execute()
        for v in result.data:
            existing_vintages.add((v["wine_id"], v["vintage_year"]))
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_vintages):,} existing vintages", flush=True)

    # Load existing COLA external_ids — track wine IDs that already have a COLA
    # Constraint: (entity_type, entity_id, system) is UNIQUE — only 1 COLA per wine
    print("Loading existing external_ids (cola)...", flush=True)
    wines_with_cola = set()
    offset = 0
    while True:
        result = (sb.table("external_ids")
                  .select("entity_id")
                  .eq("system", "cola")
                  .range(offset, offset + 999)
                  .execute())
        for e in result.data:
            wines_with_cola.add(e["entity_id"])
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(wines_with_cola):,} wines already have COLA IDs", flush=True)

    # Scan wine-linked COLAs using cursor-based pagination on ttb_id
    print("Scanning wine-linked COLAs (cursor pagination)...", flush=True)
    vintage_inserts = []
    ext_id_inserts = []
    seen_vintages = set()
    seen_ext = set()
    total_scanned = 0
    last_ttb_id = ""
    t0 = time.time()

    while True:
        for attempt in range(3):
            try:
                result = (sb.table("source_ttb_colas")
                          .select("ttb_id,canonical_wine_id,wine_vintage,abv")
                          .not_.is_("canonical_wine_id", "null")
                          .gt("ttb_id", last_ttb_id)
                          .order("ttb_id")
                          .limit(1000)
                          .execute())
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                print(f"  Error at ttb_id>{last_ttb_id}: {e}", flush=True)
                result = type('R', (), {'data': []})()
                break

        if not result.data:
            break

        for row in result.data:
            wine_id = row["canonical_wine_id"]
            ttb_id = row["ttb_id"]

            # External ID — only first COLA per wine (unique constraint)
            if wine_id not in wines_with_cola and wine_id not in seen_ext:
                ext_id_inserts.append({
                    "entity_type": "wine",
                    "entity_id": wine_id,
                    "system": "cola",
                    "external_id": ttb_id,
                })
                seen_ext.add(wine_id)

            # Wine vintage
            vintage_str = row.get("wine_vintage")
            if vintage_str:
                try:
                    vy = int(vintage_str)
                    if 1900 <= vy <= 2030:
                        vkey = (wine_id, vy)
                        if vkey not in existing_vintages and vkey not in seen_vintages:
                            vrow = {"wine_id": wine_id, "vintage_year": vy}
                            if row.get("abv"):
                                try:
                                    vrow["abv"] = float(row["abv"])
                                except (ValueError, TypeError):
                                    pass
                            vintage_inserts.append(vrow)
                            seen_vintages.add(vkey)
                except (ValueError, TypeError):
                    pass

        last_ttb_id = result.data[-1]["ttb_id"]
        total_scanned += len(result.data)

        if total_scanned % 10000 == 0:
            elapsed = time.time() - t0
            rate = total_scanned / elapsed if elapsed > 0 else 0
            print(f"  {total_scanned:,} records scanned, "
                  f"{len(vintage_inserts):,} vintages, {len(ext_id_inserts):,} COLA IDs "
                  f"[{rate:.0f}/s]", flush=True)

        # Periodic flush to avoid memory pressure
        if not args.dry_run and len(vintage_inserts) >= 10000:
            print(f"  Flushing {len(vintage_inserts):,} vintages...", flush=True)
            count = batch_insert("wine_vintages", vintage_inserts, batch_size=200)
            print(f"  Inserted {count:,}", flush=True)
            vintage_inserts = []

        if not args.dry_run and len(ext_id_inserts) >= 10000:
            print(f"  Flushing {len(ext_id_inserts):,} COLA IDs...", flush=True)
            count = batch_insert("external_ids", ext_id_inserts, batch_size=200)
            print(f"  Inserted {count:,}", flush=True)
            ext_id_inserts = []

    elapsed = time.time() - t0
    print(f"\nScan complete in {elapsed:.0f}s", flush=True)
    print(f"  Total records scanned: {total_scanned:,}", flush=True)
    print(f"  New vintages to insert: {len(vintage_inserts):,}", flush=True)
    print(f"  New COLA IDs to insert: {len(ext_id_inserts):,}", flush=True)

    if args.dry_run:
        print("\n  DRY RUN — no inserts", flush=True)
        return

    if vintage_inserts:
        print(f"\nInserting {len(vintage_inserts):,} vintages...", flush=True)
        count = batch_insert("wine_vintages", vintage_inserts, batch_size=200)
        print(f"  Inserted {count:,}", flush=True)

    if ext_id_inserts:
        print(f"\nInserting {len(ext_id_inserts):,} COLA IDs...", flush=True)
        count = batch_insert("external_ids", ext_id_inserts, batch_size=200)
        print(f"  Inserted {count:,}", flush=True)


if __name__ == "__main__":
    main()
