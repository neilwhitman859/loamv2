"""
TTB Tier C Slice 2: Create new canonical wines from TTB records.

Phase A: Create wines for TTB records that have a linked producer but no wine match.
         Uses fanciful_name as wine identity. Resolves appellation (incl. new aliases).
         For region-name strings (CALIFORNIA, MENDOZA), sets region_id without appellation.

Phase B: Create new producers from high-frequency TTB brand names that don't match
         existing producers. Then create wines for those producers.

Usage:
    python -m pipeline.promote.ttb_tier_c2 --phase a [--dry-run] [--limit 50000]
    python -m pipeline.promote.ttb_tier_c2 --phase b [--dry-run] [--min-count 10]
"""

import argparse
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.normalize import normalize, slugify, normalize_producer


def load_appellation_map(sb):
    """Load appellations + aliases into a lowercase name -> {id, region_id, country_id} map."""
    print("Loading appellations + aliases...")
    apps = {}
    offset = 0
    while True:
        result = sb.table("appellations").select("id,name,region_id,country_id").range(offset, offset + 999).execute()
        for a in result.data:
            apps[a["name"].lower()] = a
        if len(result.data) < 1000:
            break
        offset += 1000

    # Load aliases
    alias_count = 0
    offset = 0
    while True:
        result = sb.table("appellation_aliases").select("appellation_id,alias_normalized").range(offset, offset + 999).execute()
        for al in result.data:
            if al["alias_normalized"] not in apps:
                # Find the appellation for this alias
                app = next((a for a in apps.values() if a["id"] == al["appellation_id"]), None)
                if app:
                    apps[al["alias_normalized"]] = app
                    alias_count += 1
        if len(result.data) < 1000:
            break
        offset += 1000

    print(f"  {len(apps)} appellation entries ({alias_count} from aliases)")
    return apps


def load_region_map(sb):
    """Load regions into a lowercase name -> {id, country_id} map."""
    print("Loading regions...")
    regions = {}
    offset = 0
    while True:
        result = sb.table("regions").select("id,name,country_id,is_catch_all").range(offset, offset + 999).execute()
        for r in result.data:
            regions[r["name"].lower()] = r
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(regions)} regions")
    return regions


def load_country_map(sb):
    """Load countries into a lowercase name -> id map."""
    countries = {}
    offset = 0
    while True:
        result = sb.table("countries").select("id,name").range(offset, offset + 999).execute()
        for c in result.data:
            countries[c["name"].lower()] = c["id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    countries["american"] = countries.get("united states")
    return countries


def resolve_ttb_appellation(ttb_str, app_map, region_map, country_map):
    """Resolve a TTB wine_appellation string.
    Returns: {appellation_id, region_id, country_id} — all may be None."""
    if not ttb_str:
        return {"appellation_id": None, "region_id": None, "country_id": None}

    lower = ttb_str.lower().strip()

    # 1. Direct appellation match (includes aliases)
    app = app_map.get(lower)
    if app:
        return {
            "appellation_id": app["id"],
            "region_id": app.get("region_id"),
            "country_id": app.get("country_id"),
        }

    # 2. Region match
    region = region_map.get(lower)
    if region:
        # Special case: GEORGIA in TTB context = US state
        if lower == "georgia":
            us_id = country_map.get("united states")
            us_georgia = next((r for r in region_map.values()
                               if r["name"].lower() == "georgia" and r["country_id"] == us_id), None)
            if us_georgia:
                region = us_georgia
        return {
            "appellation_id": None,
            "region_id": region["id"],
            "country_id": region["country_id"],
        }

    # 3. Country match (AMERICAN, FRANCE, SPAIN, etc.)
    country_id = country_map.get(lower)
    if country_id:
        return {
            "appellation_id": None,
            "region_id": None,
            "country_id": country_id,
        }

    return {"appellation_id": None, "region_id": None, "country_id": None}


def phase_a(sb, app_map, region_map, country_map, dry_run=False, limit=None):
    """Create wines from TTB records with linked producers but no wine match."""
    print("\n" + "=" * 60)
    print("PHASE A: Create wines for linked producers")
    print("=" * 60)

    # Load existing wines to avoid duplicates: (producer_id, name_normalized) -> wine_id
    print("Loading existing wines...")
    existing_wines = {}
    offset = 0
    while True:
        result = (sb.table("wines")
                  .select("id,producer_id,name_normalized")
                  .is_("deleted_at", "null")
                  .range(offset, offset + 999)
                  .execute())
        for w in result.data:
            key = (w["producer_id"], w["name_normalized"])
            existing_wines[key] = w["id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_wines):,} existing wines loaded")

    # Load producer info for country/region inheritance
    print("Loading producer details...")
    producers = {}
    offset = 0
    while True:
        result = (sb.table("producers")
                  .select("id,name,country_id,region_id")
                  .is_("deleted_at", "null")
                  .range(offset, offset + 999)
                  .execute())
        for p in result.data:
            producers[p["id"]] = p
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(producers):,} producers loaded")

    # Get distinct producer IDs that have unmatched TTB records
    print("Finding producers with unmatched TTB records...")
    producer_ids_with_unmatched = set()
    offset = 0
    while True:
        result = (sb.table("source_ttb_colas")
                  .select("canonical_producer_id")
                  .not_.is_("canonical_producer_id", "null")
                  .is_("canonical_wine_id", "null")
                  .not_.is_("fanciful_name", "null")
                  .range(offset, offset + 999)
                  .execute())
        for r in result.data:
            producer_ids_with_unmatched.add(r["canonical_producer_id"])
        if len(result.data) < 1000:
            break
        offset += 1000
        if offset % 100000 == 0:
            print(f"  ... scanned {offset:,} rows, {len(producer_ids_with_unmatched)} producers so far")
    print(f"  {len(producer_ids_with_unmatched):,} producers with unmatched TTB records")

    # Load TTB records per-producer (avoids full-table scan timeout)
    print("Loading TTB records per producer...")
    wine_groups = defaultdict(list)
    producer_list = list(producer_ids_with_unmatched)
    total_rows = 0

    for pi, pid in enumerate(producer_list):
        result = (sb.table("source_ttb_colas")
                  .select("ttb_id,canonical_producer_id,fanciful_name,wine_appellation,grape_varietals,wine_vintage,abv")
                  .eq("canonical_producer_id", pid)
                  .is_("canonical_wine_id", "null")
                  .not_.is_("fanciful_name", "null")
                  .limit(500)
                  .execute())
        for r in result.data:
            fn = (r.get("fanciful_name") or "").strip()
            if fn:
                key = (r["canonical_producer_id"], fn)
                wine_groups[key].append(r)
                total_rows += 1

        if (pi + 1) % 500 == 0:
            print(f"  ... {pi + 1:,}/{len(producer_list):,} producers, "
                  f"{total_rows:,} rows, {len(wine_groups):,} groups")

    print(f"  {total_rows:,} TTB records loaded")
    print(f"  {len(wine_groups):,} unique (producer, wine_name) groups")

    # Apply limit
    groups_to_process = list(wine_groups.items())
    if limit:
        groups_to_process = groups_to_process[:limit]
        print(f"  Limited to {limit:,} groups")

    # Create wines
    wines_created = 0
    wines_skipped = 0
    wine_inserts = []
    ttb_updates = []  # (ttb_id, wine_id)
    slug_set = set()  # track used slugs

    for i, ((producer_id, fanciful_name), rows) in enumerate(groups_to_process):
        name_norm = normalize(fanciful_name)
        if not name_norm or len(name_norm) < 2:
            wines_skipped += 1
            continue

        # Check if wine already exists
        key = (producer_id, name_norm)
        if key in existing_wines:
            # Wine exists — just link the TTB records
            wine_id = existing_wines[key]
            for r in rows:
                ttb_updates.append((r["ttb_id"], wine_id))
            wines_skipped += 1
            continue

        # Resolve appellation from the most common wine_appellation in this group
        app_strs = [r.get("wine_appellation") for r in rows if r.get("wine_appellation")]
        app_str = max(set(app_strs), key=app_strs.count) if app_strs else None
        geo = resolve_ttb_appellation(app_str, app_map, region_map, country_map)

        # Get producer info for country/region inheritance
        producer = producers.get(producer_id, {})
        country_id = geo["country_id"] or producer.get("country_id")
        region_id = geo["region_id"] or producer.get("region_id")
        appellation_id = geo["appellation_id"]

        # Generate slug
        producer_name = producer.get("name", "")
        base_slug = slugify(f"{producer_name} {fanciful_name}")
        if not base_slug:
            base_slug = slugify(fanciful_name)
        slug = base_slug
        suffix = 1
        while slug in slug_set:
            slug = f"{base_slug}-{suffix}"
            suffix += 1
        slug_set.add(slug)

        wine_record = {
            "name": fanciful_name.title() if fanciful_name == fanciful_name.upper() else fanciful_name,
            "name_normalized": name_norm,
            "slug": slug[:200],  # slug column limit
            "producer_id": producer_id,
            "country_id": country_id,
            "region_id": region_id,
            "appellation_id": appellation_id,
            "wine_type": "table",
            "effervescence": "still",
        }

        wine_inserts.append({
            "record": wine_record,
            "ttb_ids": [r["ttb_id"] for r in rows],
            "key": key,
        })
        wines_created += 1

        if (i + 1) % 10000 == 0:
            print(f"  ... processed {i + 1:,}/{len(groups_to_process):,} groups, "
                  f"{wines_created:,} new wines")

    print(f"\nResults:")
    print(f"  {wines_created:,} new wines to create")
    print(f"  {wines_skipped:,} already existed (will link TTB records)")
    print(f"  {len(ttb_updates):,} TTB records to link to existing wines")

    if dry_run:
        print("  (DRY RUN — no writes)")
        return wines_created

    # Insert wines in batches
    print("\nInserting wines...")
    created_count = 0
    for i in range(0, len(wine_inserts), 200):
        batch = wine_inserts[i:i + 200]
        records = [w["record"] for w in batch]
        try:
            result = sb.table("wines").insert(records).execute()
            if result.data:
                for j, row in enumerate(result.data):
                    wine_id = row["id"]
                    existing_wines[batch[j]["key"]] = wine_id
                    for ttb_id in batch[j]["ttb_ids"]:
                        ttb_updates.append((ttb_id, wine_id))
                    created_count += 1
        except Exception as e:
            # Fall back to one by one
            for w in batch:
                try:
                    result = sb.table("wines").insert(w["record"]).execute()
                    if result.data:
                        wine_id = result.data[0]["id"]
                        existing_wines[w["key"]] = wine_id
                        for ttb_id in w["ttb_ids"]:
                            ttb_updates.append((ttb_id, wine_id))
                        created_count += 1
                except Exception as e2:
                    pass  # skip duplicate or invalid

        if (i + 200) % 5000 < 200:
            print(f"  ... inserted {created_count:,}/{wines_created:,}")

    print(f"  Wines inserted: {created_count:,}")

    # Link TTB records to new wines (batch updates)
    print(f"\nLinking {len(ttb_updates):,} TTB records to wines...")
    linked = 0
    for i in range(0, len(ttb_updates), 100):
        batch = ttb_updates[i:i + 100]
        for ttb_id, wine_id in batch:
            try:
                sb.table("source_ttb_colas").update({"canonical_wine_id": wine_id}).eq("ttb_id", ttb_id).execute()
                linked += 1
            except Exception:
                pass
        if (i + 100) % 10000 < 100:
            print(f"  ... linked {linked:,}/{len(ttb_updates):,}")

    print(f"  TTB records linked: {linked:,}")
    return created_count


def phase_b(sb, app_map, region_map, country_map, dry_run=False, min_count=10):
    """Create new producers from high-frequency TTB brand names, then create wines."""
    print("\n" + "=" * 60)
    print("PHASE B: Create new producers from TTB brand names")
    print("=" * 60)

    # Load existing producers for dedup
    print("Loading existing producers...")
    existing_by_norm = {}
    existing_by_stripped = {}
    offset = 0
    while True:
        result = (sb.table("producers")
                  .select("id,name,name_normalized,country_id")
                  .is_("deleted_at", "null")
                  .range(offset, offset + 999)
                  .execute())
        for p in result.data:
            norm = p.get("name_normalized") or normalize(p["name"])
            existing_by_norm[norm] = p["id"]
            stripped = normalize_producer(p["name"])
            if stripped:
                existing_by_stripped[stripped] = p["id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing_by_norm):,} producers by normalized name")

    # Find unlinked brand names with enough records
    print(f"Finding brand names with {min_count}+ records and no producer match...")
    brand_counts = defaultdict(int)
    offset = 0
    batch_count = 0
    while True:
        result = (sb.table("source_ttb_colas")
                  .select("brand_name")
                  .is_("canonical_producer_id", "null")
                  .in_("class_type", ["80", "81", "80A", "84", "88"])
                  .range(offset, offset + 999)
                  .execute())
        for r in result.data:
            bn = r.get("brand_name")
            if bn:
                brand_counts[bn] += 1
        if len(result.data) < 1000:
            break
        offset += 1000
        batch_count += 1
        if batch_count % 200 == 0:
            print(f"  ... scanned {offset:,} records")

    # Filter to brands with enough records that don't match existing producers
    new_brands = {}
    for brand, cnt in brand_counts.items():
        if cnt < min_count:
            continue
        norm = normalize(brand)
        stripped = normalize_producer(brand)
        if norm in existing_by_norm or stripped in existing_by_stripped:
            continue
        new_brands[brand] = cnt

    # Sort by count descending
    sorted_brands = sorted(new_brands.items(), key=lambda x: -x[1])
    total_records = sum(cnt for _, cnt in sorted_brands)

    print(f"  {len(sorted_brands):,} new brand names ({total_records:,} TTB records)")
    print("\n  Top 20:")
    for brand, cnt in sorted_brands[:20]:
        print(f"    {brand:40s} {cnt:,} records")

    if dry_run:
        print("\n  (DRY RUN — no writes)")
        return len(sorted_brands)

    # Create producers
    print(f"\nCreating {len(sorted_brands):,} new producers...")
    producers_created = 0
    brand_to_producer = {}  # brand_name -> producer_id
    us_country_id = country_map.get("united states")

    for brand, cnt in sorted_brands:
        # Normalize the name for display (title case from ALL CAPS)
        display_name = brand.title()
        # Fix common title-case issues
        display_name = re.sub(r"\bLlc\b", "LLC", display_name)
        display_name = re.sub(r"\bInc\b", "Inc.", display_name)
        display_name = re.sub(r"\b(And|Of|The|De|Du|La|Le|Les|Di|Da|Del)\b",
                              lambda m: m.group(0).lower(), display_name)
        display_name = display_name[0].upper() + display_name[1:]  # Re-capitalize first

        producer_record = {
            "name": display_name,
            "name_normalized": normalize(brand),
            "slug": slugify(brand)[:200],
            "country_id": us_country_id,  # TTB = US regulatory, default to US
            "producer_type": "winery",
        }

        try:
            result = sb.table("producers").insert(producer_record).execute()
            if result.data:
                pid = result.data[0]["id"]
                brand_to_producer[brand] = pid
                producers_created += 1
        except Exception as e:
            if "duplicate" not in str(e).lower():
                print(f"  Error creating {brand}: {e}")

        if producers_created % 1000 == 0 and producers_created > 0:
            print(f"  ... created {producers_created:,} producers")

    print(f"  Producers created: {producers_created:,}")

    # Link TTB records to new producers
    print(f"\nLinking TTB records to new producers...")
    total_linked = 0
    for brand, pid in brand_to_producer.items():
        try:
            result = (sb.table("source_ttb_colas")
                      .update({"canonical_producer_id": pid})
                      .eq("brand_name", brand)
                      .is_("canonical_producer_id", "null")
                      .execute())
            cnt = len(result.data) if result.data else 0
            total_linked += cnt
        except Exception:
            pass

    print(f"  TTB records linked: {total_linked:,}")
    return producers_created


def main():
    parser = argparse.ArgumentParser(description="TTB Tier C Slice 2: new wines + producers")
    parser.add_argument("--phase", default="a", choices=["a", "b", "ab"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, help="Limit groups to process (Phase A)")
    parser.add_argument("--min-count", type=int, default=10, help="Min TTB records for new producer (Phase B)")
    args = parser.parse_args()

    sb = get_supabase()

    app_map = load_appellation_map(sb)
    region_map = load_region_map(sb)
    country_map = load_country_map(sb)

    if "a" in args.phase:
        phase_a(sb, app_map, region_map, country_map, dry_run=args.dry_run, limit=args.limit)

    if "b" in args.phase:
        phase_b(sb, app_map, region_map, country_map, dry_run=args.dry_run, min_count=args.min_count)


if __name__ == "__main__":
    main()
