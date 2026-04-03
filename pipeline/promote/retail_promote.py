"""
Promote linked retail/monopoly staging data to canonical tables.

After batch_matcher links staging rows to canonical wines, this script promotes:
  - UPC barcodes → external_ids
  - Prices → wine_vintage_prices (via wine_vintages)
  - Vintages → wine_vintages
  - ABV → wine_vintages.abv

Usage:
    python -m pipeline.promote.retail_promote --source flatiron,lcbo,systembolaget,bc_liquor [--dry-run]
"""

import argparse
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.normalize import normalize, slugify, parse_vintage


def get_or_create_vintage(sb, wine_id, vintage_year, abv=None, existing_vintages=None):
    """Get or create a wine_vintage row. Returns vintage id."""
    if existing_vintages is None:
        existing_vintages = {}

    key = (wine_id, vintage_year)
    if key in existing_vintages:
        vid = existing_vintages[key]
        # Update ABV if we have it and the vintage doesn't
        if abv and vid.get("needs_abv"):
            try:
                sb.table("wine_vintages").update({"abv": float(abv)}).eq("id", vid["id"]).execute()
            except Exception:
                pass
        return vid["id"]

    # Check DB
    result = (sb.table("wine_vintages")
              .select("id,abv")
              .eq("wine_id", wine_id)
              .eq("vintage_year", vintage_year)
              .limit(1)
              .execute())
    if result.data:
        v = result.data[0]
        existing_vintages[key] = {"id": v["id"], "needs_abv": v.get("abv") is None}
        if abv and v.get("abv") is None:
            try:
                sb.table("wine_vintages").update({"abv": float(abv)}).eq("id", v["id"]).execute()
            except Exception:
                pass
        return v["id"]

    # Create
    row = {"wine_id": wine_id, "vintage_year": vintage_year}
    if abv:
        try:
            abv_f = float(abv)
            if 3.0 <= abv_f <= 25.0:
                row["abv"] = abv_f
        except (ValueError, TypeError):
            pass
    try:
        result = sb.table("wine_vintages").insert(row).execute()
        if result.data:
            vid = result.data[0]["id"]
            existing_vintages[key] = {"id": vid, "needs_abv": False}
            return vid
    except Exception as e:
        # Likely duplicate
        result = (sb.table("wine_vintages")
                  .select("id")
                  .eq("wine_id", wine_id)
                  .eq("vintage_year", vintage_year)
                  .limit(1)
                  .execute())
        if result.data:
            vid = result.data[0]["id"]
            existing_vintages[(wine_id, vintage_year)] = {"id": vid, "needs_abv": False}
            return vid
    return None


def promote_upcs(sb, source_table, source_system, dry_run=False):
    """Promote UPC barcodes from matched staging rows to external_ids."""
    print(f"\n  Promoting UPCs from {source_table}...")

    # Load matched rows with UPCs
    upc_col = "upc"
    rows = []
    offset = 0
    while True:
        result = (sb.table(source_table)
                  .select(f"id,canonical_wine_id,{upc_col}")
                  .not_.is_("canonical_wine_id", "null")
                  .not_.is_(upc_col, "null")
                  .range(offset, offset + 999)
                  .execute())
        rows.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    if not rows:
        print("    No UPC data to promote")
        return 0

    # Load existing UPCs to avoid duplicates
    existing = set()
    offset = 0
    while True:
        result = (sb.table("external_ids")
                  .select("entity_id,external_id")
                  .eq("system", "upc")
                  .eq("entity_type", "wine")
                  .range(offset, offset + 999)
                  .execute())
        for r in result.data:
            existing.add((r["entity_id"], r["external_id"]))
        if len(result.data) < 1000:
            break
        offset += 1000

    # Build insert batch
    to_insert = []
    seen = set()
    for r in rows:
        wine_id = r["canonical_wine_id"]
        upc = str(r[upc_col]).strip()
        if not upc or len(upc) < 8:
            continue
        key = (wine_id, upc)
        if key in existing or key in seen:
            continue
        seen.add(key)
        to_insert.append({
            "entity_type": "wine",
            "entity_id": wine_id,
            "system": "upc",
            "external_id": upc,
            "notes": f"from {source_system}",
        })

    print(f"    {len(rows)} matched rows with UPC, {len(to_insert)} new UPCs to insert")
    if dry_run or not to_insert:
        return len(to_insert)

    inserted = batch_insert("external_ids", to_insert, batch_size=200)
    print(f"    Inserted: {inserted}")
    return inserted


def promote_prices(sb, source_table, source_config, dry_run=False):
    """Promote prices from matched staging rows to wine_vintage_prices."""
    print(f"\n  Promoting prices from {source_table}...")

    price_col = source_config["price_col"]
    currency = source_config["currency"]
    vintage_col = source_config.get("vintage_col", "vintage")
    abv_col = source_config.get("abv_col", "abv")
    price_divisor = source_config.get("price_divisor", 1)
    source_type = source_config["source_type"]

    # Load matched rows with prices
    select_cols = f"id,canonical_wine_id,{price_col}"
    if vintage_col:
        select_cols += f",{vintage_col}"
    if abv_col:
        select_cols += f",{abv_col}"

    rows = []
    offset = 0
    while True:
        result = (sb.table(source_table)
                  .select(select_cols)
                  .not_.is_("canonical_wine_id", "null")
                  .not_.is_(price_col, "null")
                  .range(offset, offset + 999)
                  .execute())
        rows.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    if not rows:
        print("    No price data to promote")
        return 0, 0

    existing_vintages = {}
    prices_inserted = 0
    vintages_created = 0
    errors = 0

    # Load existing prices to deduplicate
    existing_prices = set()
    offset = 0
    while True:
        result = (sb.table("wine_vintage_prices")
                  .select("wine_id,vintage_year,currency,merchant_name")
                  .range(offset, offset + 999)
                  .execute())
        for r in result.data:
            existing_prices.add((r["wine_id"], r.get("vintage_year"), r["currency"], r.get("merchant_name")))
        if len(result.data) < 1000:
            break
        offset += 1000

    price_rows = []
    for r in rows:
        wine_id = r["canonical_wine_id"]
        try:
            price_raw = r[price_col]
            if price_raw is None:
                continue
            price = float(price_raw) / price_divisor
            if price <= 0 or price > 50000:
                continue
        except (ValueError, TypeError):
            continue

        # Parse vintage
        vintage_year = 0  # default NV
        if vintage_col and r.get(vintage_col):
            v = parse_vintage(str(r[vintage_col]))
            if v:
                vintage_year = int(v)
            elif str(r[vintage_col]).strip().upper() in ("NV", "N/V", ""):
                vintage_year = 0

        # Get or create vintage
        abv = r.get(abv_col) if abv_col else None
        vintage_id = get_or_create_vintage(sb, wine_id, vintage_year, abv, existing_vintages)
        if not vintage_id:
            errors += 1
            continue

        # Check if price already exists
        dedup_key = (wine_id, vintage_year, currency, source_type)
        if dedup_key in existing_prices:
            continue

        price_rows.append({
            "wine_id": wine_id,
            "vintage_year": vintage_year,
            "wine_vintage_id": vintage_id,
            "price_usd": round(price, 2) if currency == "USD" else None,
            "price_original": round(price, 2),
            "currency": currency,
            "merchant_name": source_type,
            "price_type": "retail",
        })
        existing_prices.add(dedup_key)

    print(f"    {len(rows)} matched rows with prices, {len(price_rows)} new prices")
    if dry_run or not price_rows:
        return len(price_rows), 0

    inserted = batch_insert("wine_vintage_prices", price_rows, batch_size=100)
    print(f"    Prices inserted: {inserted}, errors: {errors}")
    return inserted, vintages_created


def promote_vintages_only(sb, source_table, vintage_col="vintage", abv_col="abv", dry_run=False):
    """Promote vintage + ABV data even for rows without prices."""
    print(f"\n  Promoting vintages from {source_table}...")

    select_cols = f"id,canonical_wine_id"
    if vintage_col:
        select_cols += f",{vintage_col}"
    if abv_col:
        select_cols += f",{abv_col}"

    rows = []
    offset = 0
    while True:
        result = (sb.table(source_table)
                  .select(select_cols)
                  .not_.is_("canonical_wine_id", "null")
                  .range(offset, offset + 999)
                  .execute())
        rows.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    if not rows:
        print("    No vintage data to promote")
        return 0

    existing_vintages = {}
    created = 0

    for r in rows:
        wine_id = r["canonical_wine_id"]
        vintage_year = None
        if vintage_col and r.get(vintage_col):
            v = parse_vintage(str(r[vintage_col]))
            if v:
                vintage_year = int(v)
        if vintage_year is None:
            continue

        abv = r.get(abv_col) if abv_col else None
        key = (wine_id, vintage_year)
        if key in existing_vintages:
            continue

        vid = get_or_create_vintage(sb, wine_id, vintage_year, abv, existing_vintages)
        if vid:
            created += 1

    print(f"    Processed: {len(rows)}, vintages ensured: {created}")
    return created


def main():
    parser = argparse.ArgumentParser(description="Promote retail staging data to canonical tables")
    parser.add_argument("--source", default="flatiron,lcbo,systembolaget,bc_liquor")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(",")]
    sb = get_supabase()
    dry_run = args.dry_run

    totals = {"upcs": 0, "prices": 0, "vintages": 0}

    if "flatiron" in sources:
        print("\n" + "=" * 60)
        print("FLATIRON PROMOTION")
        print("=" * 60)
        p, v = promote_prices(sb, "source_flatiron", {
            "price_col": "price", "currency": "USD", "vintage_col": "vintage",
            "abv_col": None, "price_divisor": 1, "source_type": "flatiron_wines",
        }, dry_run)
        totals["prices"] += p
        vt = promote_vintages_only(sb, "source_flatiron", "vintage", None, dry_run)
        totals["vintages"] += vt

    if "lcbo" in sources:
        print("\n" + "=" * 60)
        print("LCBO PROMOTION")
        print("=" * 60)
        u = promote_upcs(sb, "source_lcbo", "lcbo", dry_run)
        totals["upcs"] += u
        p, v = promote_prices(sb, "source_lcbo", {
            "price_col": "price_cad_cents", "currency": "CAD", "vintage_col": None,
            "abv_col": "abv", "price_divisor": 100, "source_type": "lcbo",
        }, dry_run)
        totals["prices"] += p

    if "bc_liquor" in sources:
        print("\n" + "=" * 60)
        print("BC LIQUOR PROMOTION")
        print("=" * 60)
        u = promote_upcs(sb, "source_bc_liquor", "bc_liquor", dry_run)
        totals["upcs"] += u
        p, v = promote_prices(sb, "source_bc_liquor", {
            "price_col": "price", "currency": "CAD", "vintage_col": None,
            "abv_col": "abv", "price_divisor": 1, "source_type": "bc_liquor",
        }, dry_run)
        totals["prices"] += p

    if "systembolaget" in sources:
        print("\n" + "=" * 60)
        print("SYSTEMBOLAGET PROMOTION")
        print("=" * 60)
        p, v = promote_prices(sb, "source_systembolaget", {
            "price_col": "price_sek", "currency": "SEK", "vintage_col": "vintage",
            "abv_col": "abv", "price_divisor": 1, "source_type": "systembolaget",
        }, dry_run)
        totals["prices"] += p
        vt = promote_vintages_only(sb, "source_systembolaget", "vintage", "abv", dry_run)
        totals["vintages"] += vt

    print("\n" + "=" * 60)
    print("PROMOTION SUMMARY")
    print("=" * 60)
    print(f"  UPCs inserted: {totals['upcs']}")
    print(f"  Prices inserted: {totals['prices']}")
    print(f"  Vintages created: {totals['vintages']}")
    if dry_run:
        print("\n  ** DRY RUN — no writes performed **")


if __name__ == "__main__":
    main()
