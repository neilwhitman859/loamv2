"""
SQL-based wine cleanup using FK table cascade deletion.

Reads producer_winery_map.jsonl to identify bad producer IDs, finds wines
created during the crawl for those producers, then deletes from all FK tables
before deleting the wines themselves.

Usage:
    python -m pipeline.vivino.cleanup_wines --dry-run
    python -m pipeline.vivino.cleanup_wines
"""

import sys
import json
import argparse
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


def normalize(s):
    s = s.replace("\\u0026", "&").replace("&amp;", "&")
    return unicodedata.normalize("NFC", s).lower().strip()


# All FK tables that reference wines.id
FK_TABLES = [
    ("wine_vintage_scores", "wine_id"),
    ("wine_vintage_prices", "wine_id"),
    ("wine_vintage_grapes", "wine_id"),
    ("wine_vintage_documents", "wine_id"),
    ("wine_vintage_insights", "wine_id"),
    ("wine_vintages", "wine_id"),
    ("wine_grapes", "wine_id"),
    ("wine_regions", "wine_id"),
    ("wine_soils", "wine_id"),
    ("wine_insights", "wine_id"),
    ("wine_biodiversity_certifications", "wine_id"),
    ("wine_farming_certifications", "wine_id"),
    ("wine_water_bodies", "wine_id"),
    ("wine_candidates", "wines_id"),
]


def main():
    parser = argparse.ArgumentParser(description="Cleanup wines via FK cascade")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    mode = "(DRY RUN)" if args.dry_run else "(LIVE)"
    print(f"=== CLEANUP WINES SQL {mode} ===\n")

    # Step 1: Identify bad producer IDs
    bad_producer_ids = set()
    with open("producer_winery_map.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not j.get("vivino_winery_id"):
                continue

            if j.get("match_confidence") == "slug_match":
                if normalize(j["producer_name"]) != normalize(j["vivino_winery_name"]):
                    bad_producer_ids.add(j["producer_id"])
            if j.get("match_confidence") == "suffix_stripped":
                bad_producer_ids.add(j["producer_id"])
            if j.get("match_confidence") == "substring":
                a = normalize(j["producer_name"])
                b = normalize(j["vivino_winery_name"])
                if not (a in b or b in a):
                    bad_producer_ids.add(j["producer_id"])

    producer_array = list(bad_producer_ids)
    print(f"Bad producer IDs: {len(producer_array)}")

    # Step 2: Get wine IDs to delete
    wine_ids_to_delete = []
    for i in range(0, len(producer_array), 10):
        batch = producer_array[i:i + 10]
        try:
            result = (
                sb.table("wines").select("id")
                .in_("producer_id", batch)
                .gte("created_at", "2026-03-09T00:00:00Z")
                .execute()
            )
            if result.data:
                wine_ids_to_delete.extend(w["id"] for w in result.data)
        except Exception as e:
            print(f"Error fetching wines batch {i}: {e}")

    print(f"Total wines to delete: {len(wine_ids_to_delete)}")

    if args.dry_run:
        print("[DRY RUN] Exiting without changes.")
        return

    # Step 3: Delete from all FK tables first, then wines
    BATCH_SIZE = 10
    deleted_wines = 0
    errors = 0

    for i in range(0, len(wine_ids_to_delete), BATCH_SIZE):
        batch = wine_ids_to_delete[i:i + BATCH_SIZE]

        # Delete from all FK tables first
        for table, column in FK_TABLES:
            try:
                sb.table(table).delete().in_(column, batch).execute()
            except Exception:
                pass

        # Clear self-referencing FK
        try:
            sb.table("wines").update({"duplicate_of": None}).in_("duplicate_of", batch).execute()
        except Exception:
            pass

        # Delete the wines
        try:
            sb.table("wines").delete().in_("id", batch).execute()
            deleted_wines += len(batch)
        except Exception as e:
            print(f"  Wine delete error batch {i}: {e}")
            errors += 1

        if (i + BATCH_SIZE) % 200 == 0 or i + BATCH_SIZE >= len(wine_ids_to_delete):
            print(f"  Progress: {min(i + BATCH_SIZE, len(wine_ids_to_delete))}/{len(wine_ids_to_delete)} wines processed, {deleted_wines} deleted, {errors} errors")

    print(f"\nDone. Deleted {deleted_wines} wines ({errors} errors)")

    # Step 4: Final counts
    for table in ["wines", "wine_vintage_scores", "wine_vintage_prices", "wine_vintages"]:
        result = sb.table(table).select("*", count="exact", head=True).execute()
        print(f"  {table}: {result.count}")


if __name__ == "__main__":
    main()
