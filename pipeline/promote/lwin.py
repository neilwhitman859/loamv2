#!/usr/bin/env python3
"""
LWIN staging -> canonical promoter.

LWIN records are the fine wine identity backbone. Each record maps to a producer + wine
in canonical. This script handles the matching and promotion logic specific to LWIN data.

Usage:
    python -m pipeline.promote.lwin --analyze              # show match stats without changing anything
    python -m pipeline.promote.lwin --dry-run [--limit 100] # preview matches
    python -m pipeline.promote.lwin --promote [--limit 100] # promote to canonical
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify
from pipeline.lib.resolve import ReferenceResolver


def analyze(sb):
    """Show LWIN staging stats and match potential."""
    total = sb.table("source_lwin").select("*", count="exact", head=True).execute().count or 0
    processed = sb.table("source_lwin").select("*", count="exact", head=True).not_.is_("processed_at", "null").execute().count or 0
    with_producer = sb.table("source_lwin").select("*", count="exact", head=True).not_.is_("canonical_producer_id", "null").execute().count or 0
    with_wine = sb.table("source_lwin").select("*", count="exact", head=True).not_.is_("canonical_wine_id", "null").execute().count or 0

    print(f"=== LWIN Staging Analysis ===")
    print(f"  Total rows:        {total:>10,}")
    print(f"  Processed:         {processed:>10,}")
    print(f"  Linked to producer:{with_producer:>10,}")
    print(f"  Linked to wine:    {with_wine:>10,}")
    print(f"  Unprocessed:       {total - processed:>10,}")

    # Sample unprocessed
    sample = sb.table("source_lwin").select("*").is_("processed_at", "null").limit(10).execute()
    if sample.data:
        print(f"\n  Sample unprocessed rows:")
        for row in sample.data:
            print(f"    LWIN {row.get('lwin', '?')}: {row.get('producer_name', '?')} — {row.get('wine_name', '?')} ({row.get('country', '?')})")


def promote_lwin(dry_run: bool = False, limit: int | None = None):
    """Promote LWIN staging records to canonical producers and wines."""
    sb = get_supabase()
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Fetch unprocessed LWIN rows
    query = sb.table("source_lwin").select("*").is_("processed_at", "null").order("lwin")
    if limit:
        query = query.limit(limit)
    result = query.execute()
    rows = result.data or []

    print(f"\n=== LWIN Promotion: {len(rows)} unprocessed rows ===\n")
    if not rows:
        return

    stats = {
        "producer_matched": 0, "producer_created": 0,
        "wine_matched": 0, "wine_created": 0,
        "external_ids_created": 0,
        "errors": 0, "skipped": 0,
    }

    # Cache: normalized producer name -> canonical producer
    producer_cache: dict[str, dict] = {}

    for i, row in enumerate(rows):
        producer_name = row.get("producer_name")
        wine_name = row.get("wine_name")
        lwin = row.get("lwin")
        country = row.get("country")

        if not producer_name:
            stats["skipped"] += 1
            continue

        # Resolve country
        country_id = resolver.resolve_country(country)
        region = resolver.resolve_region(row.get("region"), country_id)

        # Match or create producer
        prod_key = f"{normalize(producer_name)}|{country_id or ''}"
        producer = producer_cache.get(prod_key)

        if not producer:
            # Try exact match
            norm = normalize(producer_name)
            match = sb.table("producers").select("id,name,country_id").eq("name_normalized", norm)
            if country_id:
                match = match.eq("country_id", country_id)
            match_result = match.is_("deleted_at", "null").limit(1).execute()

            if match_result.data:
                producer = match_result.data[0]
                stats["producer_matched"] += 1
            elif not dry_run:
                new_producer = {
                    "name": producer_name,
                    "name_normalized": norm,
                    "slug": slugify(producer_name),
                    "country_id": country_id,
                    "region_id": region["id"] if region else None,
                }
                create_result = sb.table("producers").insert(new_producer).execute()
                if create_result.data:
                    producer = create_result.data[0]
                    stats["producer_created"] += 1
                else:
                    stats["errors"] += 1
                    continue
            else:
                print(f"  [DRY] Would create producer: {producer_name} ({country})")
                stats["producer_created"] += 1
                continue

            producer_cache[prod_key] = producer

        # Match or create wine (if wine_name exists)
        wine = None
        if wine_name:
            wine_norm = normalize(wine_name)
            wine_match = (sb.table("wines")
                          .select("id,name")
                          .eq("producer_id", producer["id"])
                          .eq("name_normalized", wine_norm)
                          .is_("deleted_at", "null")
                          .limit(1)
                          .execute())

            if wine_match.data:
                wine = wine_match.data[0]
                stats["wine_matched"] += 1
            elif not dry_run:
                appellation = resolver.resolve_appellation(row.get("appellation"), country_id)
                new_wine = {
                    "name": wine_name,
                    "name_normalized": wine_norm,
                    "slug": slugify(wine_name),
                    "producer_id": producer["id"],
                    "country_id": country_id,
                    "region_id": region["id"] if region else (appellation["region_id"] if appellation else None),
                    "appellation_id": appellation["id"] if appellation else None,
                    "color": row.get("color"),
                    "wine_type": "table",
                    "effervescence": "still",
                }
                create_result = sb.table("wines").insert(new_wine).execute()
                if create_result.data:
                    wine = create_result.data[0]
                    stats["wine_created"] += 1
                else:
                    stats["errors"] += 1
                    continue
            else:
                print(f"  [DRY] Would create wine: {producer_name} — {wine_name}")
                stats["wine_created"] += 1
                continue

        # Create LWIN external ID
        if not dry_run and lwin and wine:
            try:
                sb.table("external_ids").upsert({
                    "entity_type": "wine",
                    "entity_id": wine["id"],
                    "source": "lwin",
                    "external_id": str(lwin),
                }, on_conflict="entity_type,entity_id,source,external_id").execute()
                stats["external_ids_created"] += 1
            except Exception:
                pass  # Dupe is fine

        # Link staging row
        if not dry_run:
            now = datetime.now(timezone.utc).isoformat()
            update_fields = {"processed_at": now}
            if producer:
                update_fields["canonical_producer_id"] = producer["id"]
            if wine:
                update_fields["canonical_wine_id"] = wine["id"]
            sb.table("source_lwin").update(update_fields).eq("id", row["id"]).execute()

        # Progress
        if (i + 1) % 500 == 0:
            print(f"  {i + 1}/{len(rows)} | P: {stats['producer_matched']}m/{stats['producer_created']}c | "
                  f"W: {stats['wine_matched']}m/{stats['wine_created']}c | E: {stats['errors']}")

    print(f"\n=== Results ===")
    print(f"  Producers: {stats['producer_matched']} matched, {stats['producer_created']} created")
    print(f"  Wines: {stats['wine_matched']} matched, {stats['wine_created']} created")
    print(f"  External IDs: {stats['external_ids_created']}")
    print(f"  Errors: {stats['errors']}, Skipped: {stats['skipped']}")
    if dry_run:
        print("  (dry-run — no changes made)")


def main():
    parser = argparse.ArgumentParser(description="Promote LWIN staging to canonical")
    parser.add_argument("--analyze", action="store_true", help="Show stats only")
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    parser.add_argument("--promote", action="store_true", help="Actually promote")
    parser.add_argument("--limit", type=int, help="Max rows to process")
    args = parser.parse_args()

    sb = get_supabase()

    if args.analyze:
        analyze(sb)
    elif args.promote or args.dry_run:
        promote_lwin(dry_run=args.dry_run, limit=args.limit)
    else:
        print("Usage: --analyze, --dry-run, or --promote")


if __name__ == "__main__":
    main()
