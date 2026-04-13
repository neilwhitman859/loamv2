"""
Identifies and deletes bad producer->winery mappings and their cascading data.

Reads producer_winery_map.jsonl to find false positive matches (slug_match,
suffix_stripped, risky substring), then deletes wines created during the crawl
for those bad producers, plus bad scores/prices on pre-existing wines.

Usage:
    python -m pipeline.vivino.cleanup_matches --dry-run
    python -m pipeline.vivino.cleanup_matches
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

VIVINO_PUB_ID = "ed228eae-c3bf-41e6-9a90-d78c8efaf97e"
VIVINO_SOURCE_TYPE_ID = "f4c5a61d-3921-4cd0-a32c-9363a4549f70"


def normalize(s):
    import unicodedata
    s = s.replace("\\u0026", "&").replace("&amp;", "&")
    return unicodedata.normalize("NFC", s).lower().strip()


def main():
    parser = argparse.ArgumentParser(description="Cleanup bad matches")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    mode = "(DRY RUN)" if args.dry_run else "(LIVE)"
    print(f"=== CLEANUP BAD MATCHES {mode} ===\n")

    # Step 1: Identify bad producer mappings
    bad_producers = []
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
                    bad_producers.append(j)
            if j.get("match_confidence") == "suffix_stripped":
                bad_producers.append(j)
            if j.get("match_confidence") == "substring":
                a = normalize(j["producer_name"])
                b = normalize(j["vivino_winery_name"])
                if not (a in b or b in a):
                    bad_producers.append(j)

    print(f"Bad producers identified: {len(bad_producers)}")
    bad_producer_ids = list({p["producer_id"] for p in bad_producers})
    bad_winery_ids = list({p["vivino_winery_id"] for p in bad_producers})
    print(f"Unique bad producer IDs: {len(bad_producer_ids)}")
    print(f"Unique bad Vivino winery IDs: {len(bad_winery_ids)}")

    # Step 2: Load Phase 2 data for bad wineries
    print("\nLoading Phase 2 data for bad wineries...")
    phase2_bad_count = 0
    try:
        with open("producer_wines_data.jsonl", "r", encoding="utf-8") as f:
            for line in f:
                try:
                    j = json.loads(line.strip())
                    if j.get("winery_id") in bad_winery_ids:
                        phase2_bad_count += 1
                except (json.JSONDecodeError, KeyError):
                    pass
    except FileNotFoundError:
        pass
    print(f"Phase 2 wine records from bad wineries: {phase2_bad_count}")

    # Step 3: Find wines created by the crawl for bad producers
    print("\nQuerying DB for wines from bad producers...")
    wine_ids_to_delete = []

    for i in range(0, len(bad_producer_ids), 20):
        batch = bad_producer_ids[i:i + 20]
        try:
            result = (
                sb.table("wines").select("id, name, producer_id, created_at")
                .in_("producer_id", batch)
                .gte("created_at", "2026-03-09T00:00:00Z")
                .order("created_at")
                .execute()
            )
            if result.data:
                wine_ids_to_delete.extend(w["id"] for w in result.data)
        except Exception as e:
            print(f"Error fetching wines for batch {i}: {e}")

        if i % 100 == 0 and i > 0:
            print(f"\r  Checked {i}/{len(bad_producer_ids)} producers, found {len(wine_ids_to_delete)} wines to delete", end="", flush=True)

    print(f"\nWines to delete (created >= 2026-03-09 for bad producers): {len(wine_ids_to_delete)}")

    # Step 4: Find bad scores/prices on existing wines
    print("\nChecking for bad scores on existing wines...")
    existing_wine_ids = []
    for i in range(0, len(bad_producer_ids), 20):
        batch = bad_producer_ids[i:i + 20]
        try:
            result = sb.table("wines").select("id").in_("producer_id", batch).lt("created_at", "2026-03-09T00:00:00Z").execute()
            if result.data:
                existing_wine_ids.extend(w["id"] for w in result.data)
        except Exception:
            pass

    print(f"Pre-existing wines for bad producers: {len(existing_wine_ids)}")

    score_ids_to_delete = []
    for i in range(0, len(existing_wine_ids), 50):
        batch = existing_wine_ids[i:i + 50]
        try:
            result = (
                sb.table("wine_vintage_scores").select("id, wine_id, vintage_year")
                .in_("wine_id", batch).eq("publication_id", VIVINO_PUB_ID)
                .gte("created_at", "2026-03-09T00:00:00Z").execute()
            )
            if result.data:
                score_ids_to_delete.extend(s["id"] for s in result.data)
        except Exception:
            pass
    print(f"Bad Vivino scores on pre-existing wines: {len(score_ids_to_delete)}")

    price_ids_to_delete = []
    for i in range(0, len(existing_wine_ids), 50):
        batch = existing_wine_ids[i:i + 50]
        try:
            result = (
                sb.table("wine_vintage_prices").select("id, wine_id")
                .in_("wine_id", batch).eq("source_id", VIVINO_SOURCE_TYPE_ID)
                .gte("created_at", "2026-03-09T00:00:00Z").execute()
            )
            if result.data:
                price_ids_to_delete.extend(p["id"] for p in result.data)
        except Exception:
            pass
    print(f"Bad Vivino prices on pre-existing wines: {len(price_ids_to_delete)}")

    # Step 5: Summary
    print("\n=== CLEANUP SUMMARY ===")
    print(f"Wines to delete (new wines from wrong winery): {len(wine_ids_to_delete)}")
    print(f"Scores to delete on pre-existing wines: {len(score_ids_to_delete)}")
    print(f"Prices to delete on pre-existing wines: {len(price_ids_to_delete)}")

    if args.dry_run:
        print("\n[DRY RUN] No changes made. Run without --dry-run to execute.")
        return

    # Step 6: Execute deletions
    print("\nExecuting deletions...")

    if score_ids_to_delete:
        deleted = 0
        for i in range(0, len(score_ids_to_delete), 100):
            batch = score_ids_to_delete[i:i + 100]
            try:
                sb.table("wine_vintage_scores").delete().in_("id", batch).execute()
                deleted += len(batch)
            except Exception as e:
                print(f"Score delete error: {e}")
        print(f"  Deleted {deleted} bad scores on pre-existing wines")

    if price_ids_to_delete:
        deleted = 0
        for i in range(0, len(price_ids_to_delete), 100):
            batch = price_ids_to_delete[i:i + 100]
            try:
                sb.table("wine_vintage_prices").delete().in_("id", batch).execute()
                deleted += len(batch)
            except Exception as e:
                print(f"Price delete error: {e}")
        print(f"  Deleted {deleted} bad prices on pre-existing wines")

    if wine_ids_to_delete:
        deleted_scores = deleted_prices = deleted_grapes = deleted_vintages = deleted_wines = 0
        for i in range(0, len(wine_ids_to_delete), 100):
            batch = wine_ids_to_delete[i:i + 100]
            for table in ["wine_vintage_scores", "wine_vintage_prices", "wine_grapes", "wine_vintages"]:
                try:
                    sb.table(table).delete().in_("wine_id", batch).execute()
                except Exception:
                    pass
            try:
                sb.table("wines").delete().in_("id", batch).execute()
                deleted_wines += len(batch)
            except Exception as e:
                print(f"  Wine delete error batch {i}: {e}")

            if i % 500 == 0 and i > 0:
                print(f"  Progress: {i}/{len(wine_ids_to_delete)} wines processed")

        print(f"  Deleted {deleted_wines} wines (with cascading data)")

    # Step 7: Final counts
    print("\nFetching final DB stats...")
    for table in ["wines", "wine_vintage_scores", "wine_vintage_prices"]:
        result = sb.table(table).select("*", count="exact", head=True).execute()
        print(f"  {table}: {result.count}")


if __name__ == "__main__":
    main()
