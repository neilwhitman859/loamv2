"""
Uses Claude Haiku to classify region_name_mappings with NULL appellation_id:
  - formal_appellation -> insert into appellations table + link mapping
  - not_appellation -> leave mapping as-is (broad region / IGP / sub-zone)

Then updates wines by tracing back through wine_candidates to get the
original region_name and applying the new appellation_id.

Usage:
    python -m pipeline.vivino.classify_appellations --dry-run
    python -m pipeline.vivino.classify_appellations
"""

import sys
import json
import re
import time
import uuid
import argparse
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, get_env
from pipeline.lib.normalize import normalize, slugify


def fetch_all(table, columns="*", batch_size=1000):
    sb = get_supabase()
    rows, offset = [], 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < batch_size: break
        offset += batch_size
    return rows


def call_haiku(messages, api_key, max_tokens=4096):
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens, "messages": messages},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["content"][0].get("text", "") if data.get("content") else ""


def main():
    parser = argparse.ArgumentParser(description="Classify appellations via Haiku")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    sb = get_supabase()

    print(f"\n{'=' * 70}")
    print(f"  APPELLATION CLASSIFICATION & INSERTION")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"{'=' * 70}\n")

    # Phase 1: Load reference data
    print("Phase 1: Loading reference data...")

    regions = fetch_all("regions", "id,name,country_id,is_catch_all")
    region_map = {}
    region_by_id = {}
    for r in regions:
        region_map[f"{r['name']}|{r['country_id']}"] = r
        region_by_id[r["id"]] = r
    print(f"  {len(regions)} regions")

    countries = fetch_all("countries", "id,name")
    country_by_name = {c["name"]: c for c in countries}
    country_by_id = {c["id"]: c for c in countries}
    print(f"  {len(countries)} countries")

    appellations = fetch_all("appellations", "id,name,slug,designation_type,region_id,country_id")
    appellation_lookup = {}
    for a in appellations:
        appellation_lookup[f"{normalize(a['name'])}|{a['country_id']}"] = a
    print(f"  {len(appellations)} existing appellations")

    mappings = fetch_all("region_name_mappings", "region_name,country,region_id,appellation_id,match_type")
    null_app_mappings = [m for m in mappings if not m.get("appellation_id")]
    print(f"  {len(mappings)} total mappings, {len(null_app_mappings)} with NULL appellation_id")

    # Wine candidate counts
    print("  Fetching wine_candidate counts per region_name...")
    all_candidates = fetch_all("wine_candidates", "region_name,country")
    wc_count_map: dict[str, int] = {}
    for wc in all_candidates:
        if not wc.get("region_name"):
            continue
        key = f"{wc['region_name']}|{wc['country']}"
        wc_count_map[key] = wc_count_map.get(key, 0) + 1
    print(f"  Wine candidate counts computed for {len(wc_count_map)} region_name combos")

    # Phase 2: Quick backfill
    print("\nPhase 2: Quick backfill -- matching existing appellations...")
    quick_backfills = []
    needs_classification = []

    for m in null_app_mappings:
        country = country_by_name.get(m["country"])
        if not country:
            continue
        norm_name = normalize(m["region_name"])
        existing = appellation_lookup.get(f"{norm_name}|{country['id']}")
        if existing:
            quick_backfills.append({
                "region_name": m["region_name"], "country": m["country"],
                "appellation_id": existing["id"], "appellation_name": existing["name"],
            })
        else:
            needs_classification.append({
                "region_name": m["region_name"], "country": m["country"],
                "country_id": country["id"], "region_id": m.get("region_id"),
                "resolved_region": region_by_id.get(m.get("region_id"), {}).get("name", "UNKNOWN"),
                "is_catch_all": region_by_id.get(m.get("region_id"), {}).get("is_catch_all", False),
                "match_type": m.get("match_type"),
                "candidate_count": wc_count_map.get(f"{m['region_name']}|{m['country']}", 0),
            })

    print(f"  {len(quick_backfills)} mappings matched existing appellations (quick backfill)")
    print(f"  {len(needs_classification)} mappings need Haiku classification")

    if quick_backfills and not args.dry_run:
        print("  Applying quick backfills...")
        backfilled = 0
        for bf in quick_backfills:
            try:
                sb.table("region_name_mappings").update({"appellation_id": bf["appellation_id"]}).eq("region_name", bf["region_name"]).eq("country", bf["country"]).execute()
                backfilled += 1
            except Exception as e:
                print(f'    ERROR backfilling "{bf["region_name"]}|{bf["country"]}": {e}')
        print(f"  Backfilled {backfilled}/{len(quick_backfills)} mappings")
    elif quick_backfills:
        print("  [DRY RUN] Would backfill:")
        for bf in quick_backfills:
            print(f'    "{bf["region_name"]}" ({bf["country"]}) -> appellation "{bf["appellation_name"]}"')

    # Phase 3: Haiku classification
    print("\nPhase 3: Haiku classification...")
    needs_classification.sort(key=lambda x: -x["candidate_count"])
    existing_designation_types = sorted({a.get("designation_type") for a in appellations if a.get("designation_type")})

    BATCH_SIZE = 40
    all_classifications = []

    for i in range(0, len(needs_classification), BATCH_SIZE):
        batch = needs_classification[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(needs_classification) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} entries)...")

        items_list = "\n".join(
            f'{idx + 1}. region_name="{m["region_name"]}" | country="{m["country"]}" | resolved_region="{m["resolved_region"]}" | is_catch_all={m["is_catch_all"]} | wines={m["candidate_count"]}'
            for idx, m in enumerate(batch)
        )

        prompt = f"""You are a wine geography expert. Classify each region_name below as either a FORMAL APPELLATION or NOT an appellation.

A FORMAL APPELLATION is a legally defined wine production area with an official designation (AOC, DOC, DOCG, DO, AVA, GI, WO, DAC, VQA, PDO, IGP, IGT, etc.).

NOT an appellation includes broad administrative regions, sub-zones without formal designation, country-level catch-alls, US states that are regions not AVAs.

Existing designation types in our DB: {', '.join(existing_designation_types)}

For each item, respond with a JSON object. Return a JSON array:
{{"index": <1-based>, "region_name": "<name>", "is_appellation": true/false, "designation_type": "<type or null>", "canonical_name": "<proper name>", "confidence": "high"|"medium"|"low", "reason": "<brief>"}}

Items to classify:
{items_list}

Respond with ONLY the JSON array, no other text."""

        try:
            response = call_haiku([{"role": "user", "content": prompt}], api_key, 8192)
            json_match = re.search(r"\[[\s\S]*\]", response)
            if not json_match:
                print(f"    WARNING: Could not parse JSON from Haiku response")
                continue
            classifications = json.loads(json_match.group(0))
            apps_count = sum(1 for c in classifications if c.get("is_appellation"))
            print(f"    Got {len(classifications)} classifications: {apps_count} appellations, {len(classifications) - apps_count} not")

            for cls in classifications:
                orig = batch[cls.get("index", 0) - 1] if cls.get("index") and cls["index"] <= len(batch) else None
                if not orig:
                    continue
                all_classifications.append({**orig, **cls, "country_id": orig["country_id"], "region_id": orig.get("region_id")})
        except Exception as err:
            print(f"    ERROR in batch {batch_num}: {err}")

        if i + BATCH_SIZE < len(needs_classification):
            time.sleep(0.5)

    # Phase 4: Process classifications
    print("\nPhase 4: Processing Haiku classifications...")

    DESIGNATION_SUFFIXES = [
        "DOCG", "DOCa", "DOC", "AOC", "AOP", "AVA", "VQA", "DAC",
        "GI", "WO", "DO", "PGI", "PDO", "IGP", "IGT", "IG",
    ]
    designation_pattern = re.compile(
        r"\s+(?:" + "|".join(DESIGNATION_SUFFIXES) + r")\s*$", re.IGNORECASE
    )

    for c in all_classifications:
        if c.get("canonical_name"):
            c["canonical_name"] = designation_pattern.sub("", c["canonical_name"]).strip()
        if c.get("region_name") == "Oloroso":
            c["is_appellation"] = False
            c["reason"] = "Oloroso is a sherry style, not a geographic appellation"

    new_appellations = [c for c in all_classifications if c.get("is_appellation")]
    not_appellations = [c for c in all_classifications if not c.get("is_appellation")]

    print(f"  {len(new_appellations)} classified as formal appellations")
    print(f"  {len(not_appellations)} classified as not appellations")

    print("\n  APPELLATIONS TO ADD:")
    total_app_wines = 0
    for a in new_appellations:
        name = a.get("canonical_name") or a["region_name"]
        dt = (a.get("designation_type") or "?").ljust(12)
        country = a["country"].ljust(18)
        wines = str(a.get("candidate_count", 0)).rjust(5)
        print(f"    {name.ljust(45)} | {dt} | {country} | {wines} wines | {a.get('confidence', '?')} | {a.get('reason', '')}")
        total_app_wines += a.get("candidate_count", 0)
    print(f"  Total wines affected: {total_app_wines}")

    print("\n  NOT APPELLATIONS (top 30):")
    not_appellations.sort(key=lambda x: -x.get("candidate_count", 0))
    for na in not_appellations[:30]:
        name = na["region_name"].ljust(45)
        country = na["country"].ljust(18)
        wines = str(na.get("candidate_count", 0)).rjust(5)
        print(f"    {name} | {country} | {wines} wines | {na.get('reason', '')}")

    if args.dry_run:
        print("\n[DRY RUN] Would insert appellations and update mappings. Exiting.")
        return

    # Phase 5: Insert new appellations
    print("\nPhase 5: Inserting new appellations...")
    appellation_inserts = []
    slugs_seen = {a["slug"] for a in appellations}

    for a in new_appellations:
        name = a.get("canonical_name") or a["region_name"]
        norm_key = f"{normalize(name)}|{a['country_id']}"
        if norm_key in appellation_lookup:
            print(f'  SKIP (already exists): "{name}" in {a["country"]}')
            existing = appellation_lookup[norm_key]
            try:
                sb.table("region_name_mappings").update({"appellation_id": existing["id"]}).eq("region_name", a["region_name"]).eq("country", a["country"]).execute()
            except Exception:
                pass
            continue

        slug = slugify(name)
        if slug in slugs_seen:
            country_slug = slugify(country_by_id.get(a["country_id"], {}).get("name", "unknown"))
            slug = f"{slug}-{country_slug}"
        if slug in slugs_seen:
            slug = f"{slug}-{uuid.uuid4().hex[:6]}"
        slugs_seen.add(slug)

        app_id = str(uuid.uuid4())
        appellation_inserts.append({
            "id": app_id, "slug": slug, "name": name,
            "designation_type": a.get("designation_type") or "Appellation",
            "country_id": a["country_id"], "region_id": a.get("region_id"),
            "_original_region_name": a["region_name"], "_original_country": a["country"],
        })

    print(f"  Inserting {len(appellation_inserts)} new appellations...")
    inserted_count = 0
    IBATCH = 50
    for i in range(0, len(appellation_inserts), IBATCH):
        batch = appellation_inserts[i:i + IBATCH]
        clean_batch = [{k: v for k, v in item.items() if not k.startswith("_")} for item in batch]
        try:
            sb.table("appellations").insert(clean_batch).execute()
            inserted_count += len(batch)
        except Exception as e:
            print(f"    ERROR inserting batch at {i}: {e}")
            for item in clean_batch:
                try:
                    sb.table("appellations").insert(item).execute()
                    inserted_count += 1
                except Exception as e2:
                    print(f'      SKIP "{item["name"]}": {e2}')

    print(f"  Inserted {inserted_count} new appellations")

    # Phase 6: Update region_name_mappings
    print("\nPhase 6: Updating region_name_mappings with new appellation_ids...")
    mapping_updates = 0
    for ai in appellation_inserts:
        try:
            sb.table("region_name_mappings").update({"appellation_id": ai["id"]}).eq("region_name", ai["_original_region_name"]).eq("country", ai["_original_country"]).execute()
            mapping_updates += 1
        except Exception as e:
            print(f'    ERROR updating mapping "{ai["_original_region_name"]}|{ai["_original_country"]}": {e}')
    print(f"  Updated {mapping_updates} mappings")

    # Phase 7: Update wines via wine_candidates
    print("\nPhase 7: Updating wines with new appellations via wine_candidates...")
    print("  Loading producers and candidates for matching...")

    producers = fetch_all("producers", "id,name")
    producer_by_name = {p["name"].lower().strip(): p["id"] for p in producers}

    aliases = fetch_all("producer_aliases", "producer_id,name")
    for a in aliases:
        producer_by_name[a["name"].lower().strip()] = a["producer_id"]
    print(f"  {len(producers)} producers + {len(aliases)} aliases loaded")

    mapping_appellation: dict[str, str] = {}
    for bf in quick_backfills:
        mapping_appellation[f"{bf['region_name']}|{bf['country']}"] = bf["appellation_id"]
    for ai in appellation_inserts:
        mapping_appellation[f"{ai['_original_region_name']}|{ai['_original_country']}"] = ai["id"]

    updated_mapping_keys = set(mapping_appellation.keys())
    print(f"  {len(updated_mapping_keys)} region_name|country combos to process")

    print("  Loading wine_candidates...")
    all_wc = fetch_all("wine_candidates", "producer_name,wine_name,region_name,country")
    print(f"  {len(all_wc)} wine_candidates loaded")

    relevant_candidates = [wc for wc in all_wc if wc.get("region_name") and f"{wc['region_name']}|{wc['country']}" in updated_mapping_keys]
    print(f"  {len(relevant_candidates)} candidates match updated mappings")

    updates_by_appellation: dict[str, set] = {}
    skipped_no_producer = 0
    for wc in relevant_candidates:
        prod_id = producer_by_name.get(wc["producer_name"].lower().strip())
        if not prod_id:
            skipped_no_producer += 1
            continue
        app_id = mapping_appellation.get(f"{wc['region_name']}|{wc['country']}")
        if not app_id:
            continue
        norm_wine_name = normalize(wc["wine_name"])
        updates_by_appellation.setdefault(app_id, set()).add(f"{prod_id}|||{norm_wine_name}")

    if skipped_no_producer:
        print(f"  {skipped_no_producer} candidates skipped (producer not found)")

    wine_updates = 0
    wine_errors = 0
    for app_idx, (app_id, wine_key_set) in enumerate(updates_by_appellation.items()):
        wine_keys = list(wine_key_set)
        if (app_idx + 1) % 10 == 0 or app_idx == 0:
            print(f"  Appellation {app_idx + 1}/{len(updates_by_appellation)}: {len(wine_keys)} wine keys...")

        by_producer: dict[str, list] = {}
        for key in wine_keys:
            pid, norm_name = key.split("|||", 1)
            by_producer.setdefault(pid, []).append(norm_name)

        for pid, norm_names in by_producer.items():
            for ni in range(0, len(norm_names), 50):
                batch = norm_names[ni:ni + 50]
                try:
                    result = (
                        sb.table("wines").update({"appellation_id": app_id})
                        .eq("producer_id", pid)
                        .in_("name_normalized", batch)
                        .is_("appellation_id", "null")
                        .execute()
                    )
                    if result.data:
                        wine_updates += len(result.data)
                except Exception:
                    wine_errors += 1

    print(f"  Updated {wine_updates} wines with new appellation_ids ({wine_errors} errors)")

    # Summary
    print(f"\n{'=' * 70}")
    print("  SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Quick backfills (existing appellation matched): {len(quick_backfills)}")
    print(f"  Haiku classifications: {len(all_classifications)}")
    print(f"    - Formal appellations: {len(new_appellations)}")
    print(f"    - Not appellations: {len(not_appellations)}")
    print(f"  New appellations inserted: {inserted_count}")
    print(f"  Region_name_mappings updated: {mapping_updates + len(quick_backfills)}")
    print(f"  Wines updated: {wine_updates}")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
