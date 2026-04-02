"""
Promote importer staging data to canonical tables.

Step 1: Create new canonical wines from unmatched importer wines (AI said "NONE")
Step 2: Promote enrichment data for ALL linked wines (matched + new)

Usage:
    python -m pipeline.promote.importer_promote [--dry-run] [--step 1|2|all]
"""

import argparse
import re
import uuid
from collections import defaultdict

from pipeline.lib.db import get_supabase


SOURCES = {
    "skurnik": {"table": "source_skurnik", "wine_col": "name", "producer_col": "producer"},
    "kl": {"table": "source_kermit_lynch", "wine_col": "wine_name", "producer_col": "grower_name"},
    "empson": {"table": "source_empson", "wine_col": "name", "producer_col": "producer"},
    "ec": {"table": "source_european_cellars", "wine_col": "name", "producer_col": "producer"},
    "winebow": {"table": "source_winebow", "wine_col": "name", "producer_col": "producer"},
}


def make_slug(producer_name, wine_name):
    """Generate a URL slug from producer + wine name."""
    raw = f"{producer_name} {wine_name}"
    slug = raw.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug[:200]


def normalize_name(name):
    """Normalize a wine name for matching."""
    import unicodedata
    n = unicodedata.normalize('NFKD', name)
    n = ''.join(c for c in n if not unicodedata.combining(c))
    return n.upper().strip()


def resolve_color(staging_row, source_key):
    """Try to extract color from staging data."""
    if source_key in ("skurnik", "ec"):
        color = staging_row.get("color")
        if color:
            c = color.lower().strip()
            if c in ("red", "white", "rose"):
                return c
            if c in ("rosé", "pink"):
                return "rose"
    return None


def step_1_create_wines(sb, dry_run=False):
    """Create new canonical wine records from unmatched importer wines."""
    print("\n=== STEP 1: Create new canonical wines ===\n")

    # Load producer data for country/region inheritance
    all_new = []

    for source_key, cfg in SOURCES.items():
        table = cfg["table"]
        wine_col = cfg["wine_col"]

        # Fetch unmatched, producer-linked wines
        has_color = source_key in ("skurnik", "ec")
        select_cols = f"id, {wine_col}, canonical_producer_id"
        if has_color:
            select_cols += ", color"

        offset = 0
        rows = []
        while True:
            resp = (
                sb.table(table)
                .select(select_cols)
                .not_.is_("canonical_producer_id", "null")
                .is_("canonical_wine_id", "null")
                .not_.is_(wine_col, "null")
                .range(offset, offset + 999)
                .execute()
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            if len(resp.data) < 1000:
                break
            offset += 1000

        # Group by unique (producer, wine_name)
        seen = {}
        for row in rows:
            pid = row["canonical_producer_id"]
            wname = row[wine_col].strip()
            key = (pid, wname.upper())
            if key not in seen:
                seen[key] = {
                    "producer_id": pid,
                    "wine_name": wname,
                    "source": source_key,
                    "staging_ids": [row["id"]],
                    "color": resolve_color(row, source_key),
                }
            else:
                seen[key]["staging_ids"].append(row["id"])

        print(f"  {source_key}: {len(seen)} unique new wines ({len(rows)} staging rows)")
        all_new.extend(seen.values())

    print(f"\nTotal unique new wines to create: {len(all_new)}")

    if not all_new:
        print("Nothing to create.")
        return

    # Load producer details for country/region/name
    producer_ids = list(set(w["producer_id"] for w in all_new))
    producer_map = {}
    for i in range(0, len(producer_ids), 50):
        batch = producer_ids[i:i+50]
        resp = sb.table("producers").select("id, name, country_id, region_id").in_("id", batch).execute()
        for p in resp.data:
            producer_map[p["id"]] = p

    # Check for slug conflicts with existing wines
    existing_slugs = set()
    offset = 0
    while True:
        resp = sb.table("wines").select("slug").range(offset, offset + 4999).execute()
        if not resp.data:
            break
        existing_slugs.update(r["slug"] for r in resp.data)
        if len(resp.data) < 5000:
            break
        offset += 5000
    print(f"Loaded {len(existing_slugs)} existing slugs for dedup")

    # Create wine records
    created = 0
    linked = 0
    errors = 0

    for wine_data in all_new:
        producer = producer_map.get(wine_data["producer_id"], {})
        producer_name = producer.get("name", "Unknown")
        wine_name = wine_data["wine_name"]

        # Generate unique slug
        slug = make_slug(producer_name, wine_name)
        base_slug = slug
        counter = 1
        while slug in existing_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        existing_slugs.add(slug)

        wine_record = {
            "slug": slug,
            "name": wine_name,
            "name_normalized": normalize_name(wine_name),
            "producer_id": wine_data["producer_id"],
            "country_id": producer.get("country_id"),
            "region_id": producer.get("region_id"),
            "wine_type": "table",
            "effervescence": "still",
        }

        # Add color if available
        if wine_data["color"]:
            wine_record["color"] = wine_data["color"]

        if dry_run:
            created += 1
            continue

        try:
            resp = sb.table("wines").insert(wine_record).execute()
            new_wine_id = resp.data[0]["id"]
            created += 1

            # Link all staging rows to the new wine
            source_cfg = SOURCES[wine_data["source"]]
            for sid in wine_data["staging_ids"]:
                try:
                    sb.table(source_cfg["table"]).update({
                        "canonical_wine_id": new_wine_id,
                        "match_confidence": 1.0,  # created, not matched
                    }).eq("id", sid).execute()
                    linked += 1
                except Exception as e:
                    print(f"    ERROR linking {sid}: {e}")
                    errors += 1

        except Exception as e:
            print(f"  ERROR creating wine '{producer_name} / {wine_name}': {e}")
            errors += 1

        if created % 100 == 0:
            print(f"  Created {created}/{len(all_new)} wines, {linked} staging rows linked")

    print(f"\nStep 1 complete: {created} wines created, {linked} staging rows linked, {errors} errors")
    return created


def step_2_promote_vintages(sb, dry_run=False):
    """Promote vintage data from linked staging wines to canonical wine_vintages."""
    print("\n=== STEP 2: Promote vintages + ABV ===\n")

    # Load existing wine_vintages to avoid duplicates
    existing = set()
    offset = 0
    while True:
        resp = sb.table("wine_vintages").select("wine_id, vintage_year").range(offset, offset + 4999).execute()
        if not resp.data:
            break
        for r in resp.data:
            existing.add((r["wine_id"], r["vintage_year"]))
        if len(resp.data) < 5000:
            break
        offset += 5000
    print(f"Loaded {len(existing)} existing wine_vintages")

    total_created = 0
    total_skipped = 0

    # Only sources with vintage data can create wine_vintages
    source_cols = {
        "skurnik": {"has_vintage": True, "has_abv": False},
        "winebow": {"has_vintage": True, "has_abv": True},
        # KL, empson, EC have no vintage column — skip
    }

    for source_key, col_info in source_cols.items():
        cfg = SOURCES[source_key]
        table = cfg["table"]

        select = "canonical_wine_id, vintage"
        if col_info["has_abv"]:
            select += ", abv"

        # Fetch linked wines with vintage data
        offset = 0
        rows = []
        while True:
            resp = (
                sb.table(table)
                .select(select)
                .not_.is_("canonical_wine_id", "null")
                .range(offset, offset + 999)
                .execute()
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            if len(resp.data) < 1000:
                break
            offset += 1000

        created = 0
        skipped = 0

        for row in rows:
            wine_id = row["canonical_wine_id"]
            vintage_str = row.get("vintage")

            if not vintage_str:
                continue

            # Parse vintage year
            vintage_str = str(vintage_str).strip()
            if vintage_str.upper() == "NV":
                vintage_year = 0
            elif re.match(r'^\d{4}$', vintage_str):
                vintage_year = int(vintage_str)
            else:
                continue

            if (wine_id, vintage_year) in existing:
                skipped += 1
                continue

            vintage_record = {
                "wine_id": wine_id,
                "vintage_year": vintage_year,
            }

            # Parse ABV
            abv_str = row.get("abv")
            if abv_str:
                abv_str = str(abv_str).strip().rstrip('%')
                try:
                    abv_val = float(abv_str)
                    if 5 <= abv_val <= 25:
                        vintage_record["abv"] = abv_val
                except (ValueError, TypeError):
                    pass

            if dry_run:
                created += 1
                existing.add((wine_id, vintage_year))
                continue

            try:
                sb.table("wine_vintages").insert(vintage_record).execute()
                existing.add((wine_id, vintage_year))
                created += 1
            except Exception as e:
                if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                    skipped += 1
                else:
                    print(f"    ERROR: {e}")

        print(f"  {source_key}: {created} vintages created, {skipped} skipped (existing)")
        total_created += created
        total_skipped += skipped

    print(f"\nStep 2 complete: {total_created} vintages created, {total_skipped} skipped")
    return total_created


def main():
    parser = argparse.ArgumentParser(description="Promote importer data to canonical")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--step", default="all", choices=["1", "2", "all"])
    args = parser.parse_args()

    sb = get_supabase()

    if args.step in ("1", "all"):
        step_1_create_wines(sb, dry_run=args.dry_run)

    if args.step in ("2", "all"):
        step_2_promote_vintages(sb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
