#!/usr/bin/env python3
"""
Staging table promoter — matches staging rows against canonical, creates or links records.

For each staging row:
  1. Parse producer name + wine name from source data (via per-source adapter)
  2. Resolve country/region/appellation/grape from reference data
  3. Match producer against canonical (exact normalized → fuzzy pg_trgm)
  4. Match wine against canonical (within matched producer)
  5. If matched: link staging row, merge any new fields
  6. If no match: create new canonical producer/wine, link staging row
  7. Log match decisions for audit

Usage:
    python -m pipeline.promote.staging --source polaner [--dry-run]
    python -m pipeline.promote.staging --source skurnik [--dry-run] [--limit 100]
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify, normalize_producer
from pipeline.lib.resolve import ReferenceResolver


# ── Source Adapters ─────────────────────────────────────────
# Each adapter extracts a normalized record from source-specific staging row.

def normalize_wine_type(raw_type: str | None) -> str:
    if not raw_type:
        return "table"
    t = raw_type.lower().strip()
    if t in ("sparkling", "dessert", "fortified", "aromatized"):
        return t
    return "table"


def extract_color_from_type(raw_type: str | None) -> str | None:
    if not raw_type:
        return None
    t = raw_type.lower().strip()
    return {"red": "red", "white": "white", "rosé": "rose", "rose": "rose"}.get(t)


def parse_polaner(row: dict) -> dict:
    return {
        "producer_name": row.get("producer"),  # Filled by Haiku title parser
        "wine_name": row.get("wine_name") or row.get("title"),
        "country": row.get("country"),
        "region": row.get("region"),
        "appellation": row.get("appellation"),
        "color": None,
        "wine_type": None,
        "grape": None,
        "certifications": row.get("certifications"),
        "source_table": "source_polaner",
        "source_id": row["id"],
    }


def parse_kl(row: dict) -> dict:
    return {
        "producer_name": row.get("grower_name"),
        "wine_name": row.get("wine_name"),
        "country": row.get("country"),
        "region": row.get("region"),
        "appellation": None,
        "color": extract_color_from_type(row.get("wine_type")),
        "wine_type": normalize_wine_type(row.get("wine_type")),
        "grape": row.get("blend"),
        "soil": row.get("soil"),
        "vine_age": row.get("vine_age"),
        "vineyard_area": row.get("vineyard_area"),
        "vinification": row.get("vinification"),
        "farming": row.get("farming"),
        "source_table": "source_kermit_lynch",
        "source_id": row["id"],
        "external_ids": [{"system": "kermit_lynch", "id": row["kl_id"]}] if row.get("kl_id") else [],
    }


def parse_skurnik(row: dict) -> dict:
    return {
        "producer_name": row.get("producer"),
        "wine_name": row.get("name"),
        "country": row.get("country"),
        "region": row.get("region"),
        "appellation": row.get("appellation"),
        "color": (row.get("color") or "").lower() or None,
        "wine_type": None,
        "grape": row.get("grape"),
        "vintage": row.get("vintage"),
        "source_table": "source_skurnik",
        "source_id": row["id"],
        "external_ids": [{"system": "skurnik_sku", "id": row["sku"]}] if row.get("sku") else [],
    }


def parse_winebow(row: dict) -> dict:
    return {
        "producer_name": row.get("producer"),
        "wine_name": row.get("name"),
        "country": None,
        "region": None,
        "appellation": row.get("appellation"),
        "color": None,
        "wine_type": None,
        "grape": row.get("grape"),
        "vintage": row.get("vintage"),
        "soil": row.get("soil"),
        "abv": row.get("abv"),
        "ph": row.get("ph"),
        "acidity": row.get("acidity"),
        "residual_sugar": row.get("residual_sugar"),
        "scores": row.get("scores"),
        "source_table": "source_winebow",
        "source_id": row["id"],
    }


def parse_empson(row: dict) -> dict:
    return {
        "producer_name": row.get("producer"),
        "wine_name": row.get("name"),
        "country": "Italy",  # Empson is Italy-only
        "region": None,
        "appellation": None,
        "color": None,
        "wine_type": None,
        "grape": row.get("grape"),
        "soil": row.get("soil"),
        "winemaker": row.get("winemaker"),
        "altitude": row.get("altitude"),
        "source_table": "source_empson",
        "source_id": row["id"],
    }


def parse_ec(row: dict) -> dict:
    return {
        "producer_name": row.get("producer"),
        "wine_name": row.get("name"),
        "country": None,
        "region": None,
        "appellation": row.get("appellation"),
        "color": (row.get("color") or "").lower() or None,
        "wine_type": None,
        "grape": row.get("grape"),
        "soil": row.get("soil"),
        "certifications": row.get("certifications"),
        "scores": row.get("scores"),
        "source_table": "source_european_cellars",
        "source_id": row["id"],
    }


ADAPTERS = {
    # "polaner" removed — title parsing too fragile, low metadata value (2026-03-20)
    "kl": {"table": "source_kermit_lynch", "parse": parse_kl},
    "skurnik": {"table": "source_skurnik", "parse": parse_skurnik},
    "winebow": {"table": "source_winebow", "parse": parse_winebow},
    "empson": {"table": "source_empson", "parse": parse_empson},
    "ec": {"table": "source_european_cellars", "parse": parse_ec},
}


# ── Match Engine ────────────────────────────────────────────

def match_producer(sb, resolver: ReferenceResolver, name: str, country_id: str | None = None) -> dict | None:
    """
    3-tier producer matching:
    1. Exact normalized name match
    2. Fuzzy pg_trgm via RPC
    """
    if not name:
        return None

    norm = normalize(name)
    # Tier 1: exact normalized match
    query = sb.table("producers").select("id,name,country_id").eq("name_normalized", norm).is_("deleted_at", "null")
    if country_id:
        query = query.eq("country_id", country_id)
    result = query.limit(1).execute()
    if result.data:
        return result.data[0]

    # Tier 2: fuzzy pg_trgm
    try:
        rpc_result = sb.rpc("match_producer_fuzzy", {"query_name": norm, "min_similarity": 0.4}).execute()
        if rpc_result.data:
            best = rpc_result.data[0]
            if country_id and best.get("country_id") != country_id:
                # Country mismatch — check if there's a better match
                for candidate in rpc_result.data:
                    if candidate.get("country_id") == country_id:
                        return candidate
            return best
    except Exception:
        pass  # RPC may not exist yet

    return None


def match_wine(sb, producer_id: str, wine_name: str) -> dict | None:
    """Match a wine within a producer by normalized name."""
    if not wine_name:
        return None

    norm = normalize(wine_name)
    result = (sb.table("wines")
              .select("id,name,name_normalized")
              .eq("producer_id", producer_id)
              .eq("name_normalized", norm)
              .is_("deleted_at", "null")
              .limit(1)
              .execute())
    if result.data:
        return result.data[0]

    # Fuzzy fallback
    try:
        rpc_result = sb.rpc("match_wine_fuzzy", {
            "query_name": norm,
            "query_producer_id": producer_id,
            "min_similarity": 0.5,
        }).execute()
        if rpc_result.data:
            return rpc_result.data[0]
    except Exception:
        pass

    return None


# ── Promote ─────────────────────────────────────────────────

def promote_source(source_name: str, dry_run: bool = False, limit: int | None = None):
    adapter = ADAPTERS.get(source_name)
    if not adapter:
        print(f"Unknown source: {source_name}. Available: {', '.join(ADAPTERS.keys())}")
        return

    sb = get_supabase()
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Fetch unprocessed rows
    query = sb.table(adapter["table"]).select("*").is_("processed_at", "null")
    if limit:
        query = query.limit(limit)
    result = query.execute()
    rows = result.data or []

    print(f"\n=== Promoting {source_name}: {len(rows)} unprocessed rows ===\n")
    if not rows:
        return

    stats = {
        "producer_matched": 0, "producer_created": 0,
        "wine_matched": 0, "wine_created": 0,
        "errors": 0, "skipped": 0,
    }

    for i, row in enumerate(rows):
        try:
            parsed = adapter["parse"](row)
        except Exception as e:
            print(f"  Parse error row {i}: {e}")
            stats["errors"] += 1
            continue

        producer_name = parsed.get("producer_name")
        wine_name = parsed.get("wine_name")

        if not producer_name or not wine_name:
            stats["skipped"] += 1
            continue

        # Resolve reference data
        country_id = resolver.resolve_country(parsed.get("country"))
        region = resolver.resolve_region(parsed.get("region"), country_id)
        appellation = resolver.resolve_appellation(parsed.get("appellation"), country_id)

        # Match or create producer
        producer = match_producer(sb, resolver, producer_name, country_id)
        if producer:
            stats["producer_matched"] += 1
        elif not dry_run:
            # Create new producer
            new_producer = {
                "name": producer_name,
                "name_normalized": normalize(producer_name),
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
            print(f"  [DRY] Would create producer: {producer_name}")
            stats["producer_created"] += 1
            continue

        # Match or create wine
        wine = match_wine(sb, producer["id"], wine_name)
        if wine:
            stats["wine_matched"] += 1
        elif not dry_run:
            new_wine = {
                "name": wine_name,
                "name_normalized": normalize(wine_name),
                "slug": slugify(wine_name),
                "producer_id": producer["id"],
                "country_id": country_id,
                "region_id": region["id"] if region else (appellation["region_id"] if appellation else None),
                "appellation_id": appellation["id"] if appellation else None,
                "color": parsed.get("color"),
                "wine_type": parsed.get("wine_type") or "table",
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

        # Link staging row to canonical
        if not dry_run and wine:
            now = datetime.now(timezone.utc).isoformat()
            sb.table(adapter["table"]).update({
                "canonical_producer_id": producer["id"],
                "canonical_wine_id": wine["id"],
                "processed_at": now,
            }).eq("id", row["id"]).execute()

        # Progress
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(rows)} processed | "
                  f"P: {stats['producer_matched']} matched, {stats['producer_created']} new | "
                  f"W: {stats['wine_matched']} matched, {stats['wine_created']} new")

    print(f"\n=== Results ===")
    print(f"  Producers: {stats['producer_matched']} matched, {stats['producer_created']} created")
    print(f"  Wines: {stats['wine_matched']} matched, {stats['wine_created']} created")
    print(f"  Errors: {stats['errors']}, Skipped: {stats['skipped']}")
    if dry_run:
        print("  (dry-run — no changes made)")


def main():
    parser = argparse.ArgumentParser(description="Promote staging rows to canonical tables")
    parser.add_argument("--source", required=True, help="Source adapter name")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    parser.add_argument("--limit", type=int, help="Max rows to process")
    args = parser.parse_args()

    promote_source(args.source, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
