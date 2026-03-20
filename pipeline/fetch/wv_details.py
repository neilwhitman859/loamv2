#!/usr/bin/env python3
"""
WV ABCA detail fetcher — batch fetch appellation/varietal/vineyard for wine labels.

The list endpoint gives brand name, class, ABV, vintage, winery.
The detail endpoint adds: appellation, varietal, vineyard, origin, supplier DBA.

Usage:
    python -m pipeline.fetch.wv_details              # run (resume-safe)
    python -m pipeline.fetch.wv_details --stats      # show progress
    python -m pipeline.fetch.wv_details --limit 100  # fetch only 100 labels
    python -m pipeline.fetch.wv_details --rate 2     # requests per second (default: 1)
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

WV_API_KEY = "2BB0C528-219F-49EE-A8B8-A5A2271BEF9D"
WV_DETAIL_URL = "https://api.wvabca.com/API.svc/GetWineLabelDetails"
DB_BATCH_SIZE = 50


def show_stats(sb, rate: float = 1.0):
    total = sb.table("source_wv_abca").select("*", count="exact", head=True).execute().count or 0
    fetched = sb.table("source_wv_abca").select("*", count="exact", head=True).not_.is_("detail_fetched_at", "null").execute().count or 0
    with_app = sb.table("source_wv_abca").select("*", count="exact", head=True).not_.is_("appellation", "null").execute().count or 0
    with_var = sb.table("source_wv_abca").select("*", count="exact", head=True).not_.is_("varietal", "null").execute().count or 0
    remaining = total - fetched
    eta_hours = remaining / rate / 3600 if rate > 0 else 0

    print(f"source_wv_abca: {total:,} total")
    print(f"  Detail fetched: {fetched:,} ({remaining:,} remaining, ~{eta_hours:.1f}h at {rate} req/s)")
    print(f"  With appellation: {with_app:,}")
    print(f"  With varietal: {with_var:,}")
    return total, fetched, remaining


def fetch_detail(client: httpx.Client, label_id: int) -> dict:
    """Fetch detail data for a single label ID."""
    resp = client.post(
        WV_DETAIL_URL,
        json={"id": str(label_id)},
        headers={"api_key": WV_API_KEY},
    )
    if resp.status_code == 429:
        raise Exception("RATE_LIMITED")
    resp.raise_for_status()
    if not resp.content:
        return {}
    return resp.json()


def extract_fields(detail: dict) -> dict:
    """Extract appellation/varietal/vineyard from detail response."""
    return {
        "appellation": detail.get("Appellation") or detail.get("appellation"),
        "origin": detail.get("Origin") or detail.get("origin") or detail.get("CountryOfOrigin"),
        "varietal": detail.get("Varietal") or detail.get("varietal") or detail.get("GrapeVarietal"),
        "vineyard": detail.get("Vineyard") or detail.get("vineyard"),
        "supplier_dba": detail.get("SupplierDBA") or detail.get("supplierDba") or detail.get("ApplicantDBA"),
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch WV ABCA label details")
    parser.add_argument("--stats", action="store_true", help="Show progress only")
    parser.add_argument("--limit", type=int, help="Max labels to fetch")
    parser.add_argument("--rate", type=float, default=1.0, help="Requests per second")
    args = parser.parse_args()

    sb = get_supabase()
    delay = 1.0 / args.rate

    if args.stats:
        show_stats(sb, args.rate)
        return

    print("=== WV ABCA Detail Fetcher ===\n")
    show_stats(sb, args.rate)

    # Paginate through unfetched rows
    all_rows = []
    offset = 0
    max_rows = args.limit or float("inf")

    print("\nLoading unfetched label IDs...")
    while len(all_rows) < max_rows:
        fetch_limit = min(1000, int(max_rows - len(all_rows)))
        result = (sb.table("source_wv_abca")
                  .select("id, label_id")
                  .is_("detail_fetched_at", "null")
                  .order("label_id")
                  .range(offset, offset + fetch_limit - 1)
                  .execute())
        if not result.data:
            break
        all_rows.extend(result.data)
        offset += 1000
        if len(result.data) < fetch_limit:
            break

    if not all_rows:
        print("\nAll details already fetched!")
        return

    print(f"Fetching details for {len(all_rows):,} labels at {args.rate} req/s...\n")

    fetched = 0
    errors = 0
    with_data = 0
    update_buffer = []
    start_time = time.time()

    with httpx.Client(timeout=30.0) as client:
        for i, row in enumerate(all_rows):
            try:
                detail = fetch_detail(client, row["label_id"])
                fields = extract_fields(detail)

                update_fields = {"detail_fetched_at": datetime.now(timezone.utc).isoformat()}
                for k, v in fields.items():
                    if v:
                        update_fields[k] = v

                if fields.get("appellation") or fields.get("varietal") or fields.get("vineyard"):
                    with_data += 1

                update_buffer.append({"id": row["id"], "fields": update_fields})
                fetched += 1

                # Flush buffer
                if len(update_buffer) >= DB_BATCH_SIZE:
                    for upd in update_buffer:
                        sb.table("source_wv_abca").update(upd["fields"]).eq("id", upd["id"]).execute()
                    update_buffer = []

                # Progress
                if fetched % 100 == 0 or fetched == len(all_rows):
                    elapsed = time.time() - start_time
                    rate = fetched / elapsed if elapsed > 0 else 0
                    remaining = len(all_rows) - fetched
                    eta_min = remaining / rate / 60 if rate > 0 else 0
                    print(f"\r  {fetched:,}/{len(all_rows):,} | {with_data} with data | "
                          f"{errors} errors | {rate:.1f} req/s | ETA: {eta_min:.1f}m", end="", flush=True)

                time.sleep(delay)

            except Exception as e:
                if "RATE_LIMITED" in str(e):
                    print("\n  Rate limited! Waiting 60s...")
                    time.sleep(60)
                    continue

                errors += 1
                update_buffer.append({
                    "id": row["id"],
                    "fields": {"detail_fetched_at": datetime.now(timezone.utc).isoformat()},
                })

                if errors <= 10:
                    print(f"\n  Error fetching label {row['label_id']}: {e}")
                elif errors == 11:
                    print("\n  (suppressing further error messages)")

                if errors > 100 and fetched < 10:
                    print("\n\nToo many errors, aborting.")
                    break

    # Final flush
    for upd in update_buffer:
        sb.table("source_wv_abca").update(upd["fields"]).eq("id", upd["id"]).execute()

    elapsed = time.time() - start_time
    print(f"\n\n=== Done ===")
    print(f"Fetched: {fetched:,}, With data: {with_data}, Errors: {errors}")
    print(f"Time: {elapsed / 60:.1f} minutes ({fetched / elapsed:.1f} req/s avg)")
    show_stats(sb, args.rate)


if __name__ == "__main__":
    main()
