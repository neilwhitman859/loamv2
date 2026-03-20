#!/usr/bin/env python3
"""
Promote LWIN staging records to canonical tables.

Reads from source_lwin staging table, cross-matches against existing
canonical producers/wines, then creates new canonical records for unmatched.
Updates staging rows with canonical_wine_id/canonical_producer_id links.
Stores LWIN-7 codes in external_ids.

Usage:
    python -m pipeline.promote.promote_lwin_legacy --analyze
    python -m pipeline.promote.promote_lwin_legacy --dry-run
    python -m pipeline.promote.promote_lwin_legacy --import
    python -m pipeline.promote.promote_lwin_legacy --import --limit 500
    python -m pipeline.promote.promote_lwin_legacy --import --country France
"""

import argparse
import re
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify

BATCH_SIZE = 1000
WRITE_BATCH = 200

# ── LWIN Field Maps ──────────────────────────────────────────

COLOR_MAP = {"Red": "red", "White": "white", "Rose": "rose", "Mixed": None}

REGION_NAME_MAP = {
    "burgundy": "bourgogne", "rhone": "rhône valley", "loire": "loire valley",
    "languedoc": "languedoc-roussillon", "corsica": "corse", "roussillon": "languedoc-roussillon",
    "south west france": "southwest france",
    "piedmont": "piemonte", "trentino alto adige": "trentino-alto adige",
    "friuli venezia giulia": "friuli-venezia giulia", "emilia romagna": "emilia-romagna",
    "lombardia": "lombardy", "prosecco": "veneto",
    "wurttemberg": "württemberg", "saale unstrut": "saale-unstrut",
    "castilla y leon": "castilla y león", "castilla la mancha": "castilla-la mancha",
    "andalucia": "andalucía", "aragon": "aragón", "pais vasco": "país vasco",
    "galicia": "the north west", "murcia": "the levante", "cava": "catalunya",
    "dao": "dão", "alentejano": "alentejo", "porto": "douro",
    "walla walla valley": "washington",
    "south eastern australia": "south eastern australia",
    "wairarapa": "martinborough", "auckland": "north island",
    "niederosterreich": "niederösterreich",
    "central valley": "central valley region", "aconcagua": "aconcagua region",
    "sur": "southern region", "coquimbo": "coquimbo region",
}

CLASSIFICATION_MAP = {
    "Grand Cru": {"system_slug": "burgundy-vineyard", "level_name": "Grand Cru"},
    "Premier Cru": {"system_slug": "burgundy-vineyard", "level_name": "Premier Cru"},
    "Grand Cru Classe": {"system_slug": "saint-emilion", "level_name": "Grand Cru Classé"},
    "Premier Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Premier Cru"},
    "2eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Deuxième Cru"},
    "3eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Troisième Cru"},
    "4eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Quatrième Cru"},
    "5eme Cru Classe": {"system_slug": "bordeaux-1855-medoc", "level_name": "Cinquième Cru"},
    "Premier Grand Cru Classe A": {"system_slug": "saint-emilion", "level_name": "Premier Grand Cru Classé A"},
    "Premier Grand Cru Classe B": {"system_slug": "saint-emilion", "level_name": "Premier Grand Cru Classé B"},
    "Premier Cru Superieur": {"system_slug": "bordeaux-1855-sauternes", "level_name": "Premier Cru Supérieur"},
    "Erste Lage": {"system_slug": "vdp-classification", "level_name": "Erste Lage"},
    "Cru Classe": {"system_slug": "graves-pessac-leognan", "level_name": "Cru Classé"},
}


def strip_corporate_suffix(name: str | None) -> str:
    """Strip corporate suffixes from producer names."""
    if not name:
        return name or ""
    return re.sub(
        r"\s*,?\s*\b(Inc\.?|LLC|Ltd\.?|S\.?A\.?S\.?|S\.?r\.?l\.?|GmbH|S\.?A\.?|S\.?L\.?|AG|Co\.?|Corp\.?|Pty\.?|Ltda\.?)\s*$",
        "", name, flags=re.IGNORECASE
    ).strip()


def map_wine_type(wine_type: str | None) -> dict:
    if not wine_type:
        return {"wine_type": "table", "effervescence": "still"}
    t = wine_type.lower()
    if t in ("sparkling", "champagne"):
        return {"wine_type": "sparkling", "effervescence": "sparkling"}
    if t == "fortified":
        return {"wine_type": "fortified", "effervescence": "still"}
    return {"wine_type": "table", "effervescence": "still"}


def fetch_all_sync(sb, table: str, columns: str = "*", filter_fn=None) -> list[dict]:
    """Paginated fetch from Supabase table."""
    all_rows: list[dict] = []
    offset = 0
    while True:
        query = sb.table(table).select(columns).range(offset, offset + BATCH_SIZE - 1)
        if filter_fn:
            query = filter_fn(query)
        result = query.execute()
        all_rows.extend(result.data)
        if len(result.data) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    return all_rows


def main():
    parser = argparse.ArgumentParser(description="Promote LWIN staging to canonical")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--analyze", action="store_true", help="Match stats only")
    group.add_argument("--dry-run", action="store_true", help="Show what would happen")
    group.add_argument("--import", dest="do_import", action="store_true", help="Actually promote")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process")
    parser.add_argument("--country", type=str, help="Filter by country name")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    mode = "import" if args.do_import else ("dry-run" if args.dry_run else "analyze")
    limit = args.limit or float("inf")
    verbose = args.verbose

    sb = get_supabase()
    print(f"Mode: {mode}" + (f", limit: {args.limit}" if args.limit else "") +
          (f", country: {args.country}" if args.country else ""))

    # ── Load Reference Data ──────────────────────────────────
    print("\nLoading reference data...")

    countries = fetch_all_sync(sb, "countries", "id,name")
    regions = fetch_all_sync(sb, "regions", "id,name,country_id,parent_id,is_catch_all")
    appellations = fetch_all_sync(sb, "appellations", "id,name,country_id,region_id")
    app_aliases = fetch_all_sync(sb, "appellation_aliases", "id,alias,appellation_id")
    region_aliases = fetch_all_sync(sb, "region_aliases", "id,name,region_id")
    classifications = fetch_all_sync(sb, "classifications", "id,slug,name")
    classification_levels = fetch_all_sync(sb, "classification_levels", "id,classification_id,level_name,level_rank")
    existing_producers = fetch_all_sync(sb, "producers", "id,name,name_normalized,country_id,region_id",
                                        lambda q: q.is_("deleted_at", "null"))
    existing_wines = fetch_all_sync(sb, "wines", "id,name,name_normalized,producer_id,country_id,lwin,identity_confidence",
                                    lambda q: q.is_("deleted_at", "null"))

    print(f"  {len(countries)} countries, {len(regions)} regions, {len(appellations)} appellations")
    print(f"  {len(app_aliases)} app aliases, {len(region_aliases)} region aliases")
    print(f"  {len(existing_producers)} existing producers, {len(existing_wines)} existing wines")

    # ── Build Lookup Maps ────────────────────────────────────

    # Country
    country_map: dict[str, str] = {}
    for c in countries:
        country_map[c["name"].lower()] = c["id"]
    us_id = country_map.get("united states")
    if us_id:
        country_map["usa"] = us_id

    # Region
    region_map: dict[str, dict] = {}
    for r in regions:
        region_map[f"{normalize(r['name'])}|{r['country_id']}"] = r
        region_map[normalize(r["name"])] = r
    for ra in region_aliases:
        region = next((r for r in regions if r["id"] == ra["region_id"]), None)
        if region:
            region_map[f"{normalize(ra['name'])}|{region['country_id']}"] = region
            region_map[normalize(ra["name"])] = region

    # Appellation
    appellation_map: dict[str, dict] = {}
    for a in appellations:
        appellation_map[normalize(a["name"])] = a
        appellation_map[a["name"].lower()] = a
    for aa in app_aliases:
        app = next((a for a in appellations if a["id"] == aa["appellation_id"]), None)
        if app:
            appellation_map[normalize(aa["alias"])] = app
            appellation_map[aa["alias"].lower()] = app

    # Classification
    class_slug_map: dict[str, dict] = {c["slug"]: c for c in classifications}
    class_level_map: dict[str, dict] = {}
    for cl in classification_levels:
        class_level_map[f"{cl['classification_id']}|{cl['level_name'].lower()}"] = cl

    # Existing producers: normalized name -> producer
    producer_by_norm: dict[str, dict] = {}
    producer_by_id: dict[str, dict] = {}
    for p in existing_producers:
        producer_by_norm[p["name_normalized"]] = p
        producer_by_norm[normalize(p["name"])] = p
        stripped = normalize(strip_corporate_suffix(p["name"]))
        if stripped != p["name_normalized"]:
            producer_by_norm[stripped] = p
        producer_by_id[p["id"]] = p

    # Existing wines
    wine_by_lwin: dict[str, dict] = {}
    for w in existing_wines:
        if w.get("lwin"):
            wine_by_lwin[w["lwin"]] = w

    # ── Resolution Functions ─────────────────────────────────

    def resolve_country(name):
        return country_map.get(name.lower()) if name else None

    def resolve_region(lwin_region, country_id):
        if not lwin_region:
            return None
        lower = lwin_region.lower()
        mapped = REGION_NAME_MAP.get(lower)
        if mapped:
            norm = normalize(mapped)
            if country_id:
                r = region_map.get(f"{norm}|{country_id}")
                if r:
                    return r
            r2 = region_map.get(norm)
            if r2:
                return r2
        norm = normalize(lower)
        if country_id:
            r = region_map.get(f"{norm}|{country_id}")
            if r:
                return r
        return region_map.get(norm)

    def resolve_appellation(sub_region, _country_id):
        if not sub_region:
            return None
        norm = normalize(sub_region)
        return appellation_map.get(norm) or appellation_map.get(sub_region.lower())

    def resolve_classification(lwin_class):
        if not lwin_class:
            return None
        mapping = CLASSIFICATION_MAP.get(lwin_class)
        if not mapping:
            return None
        system = class_slug_map.get(mapping["system_slug"])
        if not system:
            return None
        level = class_level_map.get(f"{system['id']}|{mapping['level_name'].lower()}")
        return {"system": system, "level": level} if level else None

    # ── Fetch LWIN Staging Rows ──────────────────────────────
    print("\nFetching LWIN staging rows...")

    lwin_rows: list[dict] = []
    offset = 0
    while True:
        query = sb.table("source_lwin").select("*").is_("canonical_wine_id", "null")
        if args.country:
            query = query.eq("country", args.country)
        query = query.range(offset, offset + BATCH_SIZE - 1)
        result = query.execute()
        lwin_rows.extend(result.data)
        if len(result.data) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
        if len(lwin_rows) >= limit:
            break

    if limit < float("inf") and len(lwin_rows) > limit:
        lwin_rows = lwin_rows[:int(limit)]

    print(f"  {len(lwin_rows)} unprocessed LWIN rows to promote")

    # ── Process ──────────────────────────────────────────────
    print("\nProcessing...")

    stats = {
        "total": len(lwin_rows),
        "country_resolved": 0, "country_missing": {},
        "region_resolved": 0, "region_missing": {},
        "appellation_resolved": 0,
        "classification_resolved": 0,
        "producer_exact_match": 0,
        "producer_created": 0,
        "producer_no_name": 0,
        "wine_lwin_match": 0,
        "wine_created": 0,
        "errors": 0,
    }

    # Batch accumulators
    producer_insert_batch: list[dict] = []
    wine_insert_batch: list[dict] = []
    classification_insert_batch: list[dict] = []
    external_id_batch: list[dict] = []
    alias_insert_batch: list[dict] = []

    def flush_producers():
        nonlocal producer_insert_batch
        if not producer_insert_batch:
            return
        if mode == "import":
            for p in producer_insert_batch:
                try:
                    sb.table("producers").insert(p).execute()
                except Exception as e:
                    err_msg = str(e)
                    if "duplicate" in err_msg or "unique" in err_msg:
                        result = sb.table("producers").select("id,name") \
                            .eq("slug", p["slug"]).is_("deleted_at", "null").limit(1).execute()
                        if result.data:
                            old_id = p["id"]
                            real_id = result.data[0]["id"]
                            producer_by_norm[normalize(p["name"])] = {
                                "id": real_id, "name": result.data[0]["name"],
                                "country_id": p.get("country_id")
                            }
                            producer_by_id[real_id] = {"id": real_id, "name": result.data[0]["name"]}
                            for w in wine_insert_batch:
                                if w["producer_id"] == old_id:
                                    w["producer_id"] = real_id
                            if verbose:
                                print(f'  Slug conflict: "{p["name"]}" -> existing "{result.data[0]["name"]}"')
                    else:
                        print(f"  Producer insert error ({p['name']}): {err_msg}")
                        stats["errors"] += 1
        producer_insert_batch = []

    def flush_wines():
        nonlocal wine_insert_batch
        if not wine_insert_batch:
            return
        if mode == "import":
            for i in range(0, len(wine_insert_batch), WRITE_BATCH):
                chunk = wine_insert_batch[i:i + WRITE_BATCH]
                try:
                    sb.table("wines").insert(chunk).execute()
                except Exception:
                    for w in chunk:
                        try:
                            sb.table("wines").insert(w).execute()
                        except Exception as e2:
                            err_msg = str(e2)
                            if "duplicate" in err_msg or "unique" in err_msg:
                                result = sb.table("wines").select("id") \
                                    .eq("slug", w["slug"]).is_("deleted_at", "null").limit(1).execute()
                                if result.data:
                                    old_id = w["id"]
                                    real_id = result.data[0]["id"]
                                    for ext in external_id_batch:
                                        if ext["entity_id"] == old_id:
                                            ext["entity_id"] = real_id
                                    for cl in classification_insert_batch:
                                        if cl["entity_id"] == old_id:
                                            cl["entity_id"] = real_id
                            elif "foreign key" not in err_msg:
                                print(f"  Wine insert error: {err_msg}")
                                stats["errors"] += 1
                            else:
                                print(f"  Wine FK error (producer_id={w['producer_id']}): {w['name']}")
                                stats["errors"] += 1
        wine_insert_batch = []

    def flush_classifications():
        nonlocal classification_insert_batch
        if not classification_insert_batch:
            return
        if mode == "import":
            for c in classification_insert_batch:
                try:
                    sb.table("entity_classifications").insert(c).execute()
                except Exception as e:
                    if "duplicate" not in str(e):
                        print(f"  Classification error: {e}")
                        stats["errors"] += 1
        classification_insert_batch = []

    def flush_external_ids():
        nonlocal external_id_batch
        if not external_id_batch:
            return
        if mode == "import":
            for i in range(0, len(external_id_batch), WRITE_BATCH):
                chunk = external_id_batch[i:i + WRITE_BATCH]
                try:
                    sb.table("external_ids").insert(chunk).execute()
                except Exception as e:
                    if "duplicate" not in str(e):
                        print(f"  External ID batch error: {e}")
                        stats["errors"] += 1
        external_id_batch = []

    def flush_all():
        flush_producers()
        flush_wines()
        flush_classifications()
        flush_external_ids()

    # ── Main Loop ────────────────────────────────────────────
    # Group by producer
    producer_groups: dict[str, list[dict]] = {}
    for row in lwin_rows:
        producer_name = row.get("producer_name")
        if not producer_name:
            dn = row.get("display_name") or ""
            producer_name = dn.split(",")[0].strip() if "," in dn else None
        if not producer_name:
            stats["producer_no_name"] += 1
            continue
        producer_groups.setdefault(producer_name, []).append(row)

    print(f"  {len(producer_groups)} unique producers to process")

    processed = 0

    for producer_name, rows in producer_groups.items():
        # Determine most common country
        country_counts: dict[str, int] = {}
        for r in rows:
            cid = resolve_country(r.get("country"))
            if cid:
                country_counts[cid] = country_counts.get(cid, 0) + 1
        top_country = max(country_counts.items(), key=lambda x: x[1])[0] if country_counts else None

        norm_name = normalize(producer_name)
        producer_id = None

        existing = producer_by_norm.get(norm_name)
        if existing:
            producer_id = existing["id"]
            stats["producer_exact_match"] += 1
        else:
            producer_id = str(uuid.uuid4())
            region_counts: dict[str, int] = {}
            for r in rows:
                reg = resolve_region(r.get("region"), top_country)
                if reg:
                    region_counts[reg["id"]] = region_counts.get(reg["id"], 0) + 1
            top_region = max(region_counts.items(), key=lambda x: x[1])[0] if region_counts else None

            clean_name = strip_corporate_suffix(producer_name)
            slug = slugify(clean_name)

            producer_insert_batch.append({
                "id": producer_id,
                "slug": slug,
                "name": clean_name,
                "name_normalized": norm_name,
                "country_id": top_country,
                "region_id": top_region,
                "producer_type": "estate",
            })

            producer_by_norm[norm_name] = {"id": producer_id, "name": clean_name, "country_id": top_country}
            producer_by_id[producer_id] = {"id": producer_id, "name": clean_name}
            stats["producer_created"] += 1

            if clean_name != producer_name:
                alias_insert_batch.append({
                    "id": str(uuid.uuid4()),
                    "producer_id": producer_id,
                    "name": producer_name,
                    "name_normalized": normalize(producer_name),
                    "alias_type": "source_variant",
                })

            flush_producers()

            cached = producer_by_norm.get(norm_name)
            if cached and cached["id"] != producer_id:
                producer_id = cached["id"]

        # ── Process wines for this producer ──
        for row in rows:
            country_id = resolve_country(row.get("country"))
            if country_id:
                stats["country_resolved"] += 1
            elif row.get("country"):
                stats["country_missing"][row["country"]] = stats["country_missing"].get(row["country"], 0) + 1

            region = resolve_region(row.get("region"), country_id)
            if region:
                stats["region_resolved"] += 1
            elif row.get("region"):
                key = f"{row.get('country')}|{row['region']}"
                stats["region_missing"][key] = stats["region_missing"].get(key, 0) + 1

            appellation = resolve_appellation(row.get("sub_region"), country_id)
            if appellation:
                stats["appellation_resolved"] += 1

            classification = resolve_classification(row.get("classification"))
            if classification:
                stats["classification_resolved"] += 1

            lwin7 = row.get("lwin_7") or row.get("lwin")

            # Build wine name
            wine_name = None
            if row.get("display_name"):
                dn = row["display_name"]
                first_comma = dn.find(",")
                if first_comma > 0:
                    wine_name = dn[first_comma + 1:].strip().rstrip(",").strip()
                else:
                    wine_name = row.get("wine_name") or dn
            else:
                wine_name = row.get("wine_name") or "Unknown"
            if not wine_name or not wine_name.strip():
                wine_name = row.get("wine_name") or "Unknown"

            # Check if LWIN already exists
            wine_match = wine_by_lwin.get(lwin7)
            if wine_match:
                stats["wine_lwin_match"] += 1
                processed += 1
                continue

            # Create new wine
            type_info = map_wine_type(row.get("wine_type"))
            color = COLOR_MAP.get(row.get("colour")) if row.get("colour") else None

            wine_id = str(uuid.uuid4())
            wine_slug = slugify(f"{producer_name}-{wine_name}-{lwin7}")
            wine_norm = normalize(wine_name)

            wine_insert_batch.append({
                "id": wine_id,
                "slug": wine_slug,
                "name": wine_name,
                "name_normalized": wine_norm,
                "producer_id": producer_id,
                "country_id": country_id,
                "region_id": region["id"] if region else None,
                "appellation_id": appellation["id"] if appellation else None,
                "color": color,
                "wine_type": type_info["wine_type"],
                "effervescence": type_info["effervescence"],
                "lwin": lwin7,
                "identity_confidence": "unverified",
            })

            wine_by_lwin[lwin7] = {"id": wine_id, "name": wine_name, "lwin": lwin7}

            external_id_batch.append({
                "entity_type": "wine", "entity_id": wine_id,
                "system": "lwin", "external_id": lwin7,
            })

            if classification:
                classification_insert_batch.append({
                    "id": str(uuid.uuid4()),
                    "entity_type": "wine",
                    "entity_id": wine_id,
                    "classification_level_id": classification["level"]["id"],
                })

            stats["wine_created"] += 1
            processed += 1

            if processed % 1000 == 0:
                flush_all()
                print(f"  {processed}/{len(lwin_rows)} processed "
                      f"({stats['producer_created']} new producers, "
                      f"{stats['wine_created']} new wines, "
                      f"{stats['wine_lwin_match']} matched)")

    # Final flush
    flush_all()

    # Flush aliases
    if mode == "import" and alias_insert_batch:
        for i in range(0, len(alias_insert_batch), WRITE_BATCH):
            chunk = alias_insert_batch[i:i + WRITE_BATCH]
            try:
                sb.table("producer_aliases").upsert(
                    chunk, on_conflict="producer_id,name_normalized"
                ).execute()
            except Exception as e:
                if "duplicate" not in str(e):
                    print(f"  Alias batch error: {e}")

    # ── Report ───────────────────────────────────────────────
    total = stats["total"]

    def pct(n, t):
        return f"{(n / t * 100):.1f}%" if t > 0 else "0%"

    print(f"\n{'=' * 51}")
    print("  LWIN PROMOTION REPORT")
    print(f"{'=' * 51}\n")

    print(f"Total LWIN rows: {total}")
    print(f"Producers with no name: {stats['producer_no_name']}\n")

    print("RESOLUTION RATES:")
    print(f"  Country:        {stats['country_resolved']}/{total} ({pct(stats['country_resolved'], total)})")
    print(f"  Region:         {stats['region_resolved']}/{total} ({pct(stats['region_resolved'], total)})")
    print(f"  Appellation:    {stats['appellation_resolved']}/{total} ({pct(stats['appellation_resolved'], total)})")
    print(f"  Classification: {stats['classification_resolved']}/{total} ({pct(stats['classification_resolved'], total)})")

    print("\nPRODUCER RESULTS:")
    print(f"  Existing match: {stats['producer_exact_match']}")
    print(f"  Created new:    {stats['producer_created']}")

    print("\nWINE RESULTS:")
    print(f"  Already existed: {stats['wine_lwin_match']}")
    print(f"  Created new:     {stats['wine_created']}")

    print(f"\nErrors: {stats['errors']}")

    if stats["country_missing"]:
        print("\nUNRESOLVED COUNTRIES (top 20):")
        for k, v in sorted(stats["country_missing"].items(), key=lambda x: -x[1])[:20]:
            print(f"  {k}: {v}")

    if stats["region_missing"]:
        print("\nUNRESOLVED REGIONS (top 20):")
        for k, v in sorted(stats["region_missing"].items(), key=lambda x: -x[1])[:20]:
            print(f"  {k}: {v}")

    if mode != "import":
        label = "Analysis" if mode == "analyze" else "Dry run"
        print(f"\n{label} complete. Run with --import to write to DB.")

    print("\nDone.")


if __name__ == "__main__":
    main()
