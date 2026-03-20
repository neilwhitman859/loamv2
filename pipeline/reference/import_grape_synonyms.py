"""
Import grape synonyms from VIVC cache into grape_synonyms table.

Source: data/vivc_grapes_cache.json (synonyms arrays per grape)
Target: grape_synonyms table (grape_id, synonym, source, synonym_type)

Joins VIVC cache -> DB grapes via vivc_number.
Imports all synonyms for grapes that exist in our DB.

Usage:
    python -m pipeline.reference.import_grape_synonyms [--cache <path>]
"""

import argparse
import json
import sys
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all, batch_insert

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE = PROJECT_ROOT / "data" / "vivc_grapes_cache.json"
BATCH_SIZE = 500


def main():
    parser = argparse.ArgumentParser(description="Import grape synonyms from VIVC cache")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE),
                        help="Path to VIVC grapes cache JSON")
    args = parser.parse_args()

    sb = get_supabase()

    # Load VIVC cache
    cache = json.loads(Path(args.cache).read_text(encoding="utf-8"))
    vivc_grapes = list(cache["grapes"].values())
    print(f"VIVC cache: {len(vivc_grapes)} grapes")

    # Filter to grapes with synonyms
    with_synonyms = [g for g in vivc_grapes if g.get("synonyms") and len(g["synonyms"]) > 0]
    print(f"Grapes with synonyms: {len(with_synonyms)}")

    # Load all grape IDs from DB, keyed by vivc_number
    all_grapes = fetch_all("grapes", "id,vivc_number,name,display_name")
    grape_map: dict[str, dict] = {}
    for g in all_grapes:
        if g.get("vivc_number"):
            grape_map[str(g["vivc_number"])] = g

    print(f"DB grapes with VIVC numbers: {len(grape_map)}")

    # Build synonym rows
    rows = []
    skipped_no_match = 0
    skipped_self_name = 0

    for vivc_grape in with_synonyms:
        db_grape = grape_map.get(str(vivc_grape["vivc_number"]))
        if not db_grape:
            skipped_no_match += 1
            continue

        for syn in vivc_grape["synonyms"]:
            trimmed = syn.strip()
            if not trimmed:
                continue

            # Skip if synonym is identical to the grape's own name (case-insensitive)
            if trimmed.upper() == vivc_grape["name"].upper():
                skipped_self_name += 1
                continue

            rows.append({
                "grape_id": db_grape["id"],
                "synonym": trimmed,
                "synonym_type": "vivc_synonym",
                "source": "VIVC",
            })

    print(f"Synonym rows to insert: {len(rows)}")
    print(f"Skipped (no DB match): {skipped_no_match}")
    print(f"Skipped (self-name): {skipped_self_name}")

    # Check for existing synonyms
    result = sb.table("grape_synonyms").select("*", count="exact").limit(0).execute()
    existing_count = result.count or 0

    if existing_count > 0:
        print(f"\nWARNING: grape_synonyms already has {existing_count} rows.")
        print("Clearing existing VIVC synonyms before re-import...")

        # Delete in batches to avoid timeout
        deleted = 0
        while True:
            result = sb.table("grape_synonyms").delete().eq("source", "VIVC").limit(2000).execute()
            if not result.data or len(result.data) == 0:
                break
            deleted += len(result.data)
            print(f"  Deleted {deleted}...")

    # Insert in batches
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            sb.table("grape_synonyms").insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            print(f"Error at batch {i // BATCH_SIZE}: {e}")
            # Try one by one
            for row in batch:
                try:
                    sb.table("grape_synonyms").insert(row).execute()
                    inserted += 1
                except Exception as single_err:
                    print(f"  Failed: {row['synonym']} for grape {row['grape_id']}: {single_err}")

        if (i // BATCH_SIZE) % 10 == 0:
            print(f"  Inserted {inserted}/{len(rows)}...")

    print(f"\nDone! Inserted {inserted} synonyms.")

    # Verify with some well-known examples
    examples = ["SYRAH", "TEMPRANILLO TINTO", "PINOT NOIR"]
    for name in examples:
        grape = next((g for g in grape_map.values() if g["name"] == name), None)
        if not grape:
            continue

        result = sb.table("grape_synonyms").select("synonym").eq("grape_id", grape["id"]).limit(10).execute()
        syns = [s["synonym"] for s in (result.data or [])]
        print(f"\n{grape.get('display_name', grape['name'])}: {', '.join(syns)}")


if __name__ == "__main__":
    main()
