#!/usr/bin/env python3
"""LWIN long-tail producer promotion sweep (Session 13).

Promotes LWIN producers not already covered by the 30K Plan's 20+ cutoff:

  - ALL US producers (any wine count), minus Haiku-filtered junk
  - International producers with >= 8 LWIN wines

Each eligible producer gets a canonical `producers` row (creating or matching),
and every LWIN wine under that producer gets a canonical `wines` row (creating
or matching by name_normalized). Every wine gets an `external_ids.lwin_7` entry.
source_lwin is updated with canonical_producer_id + canonical_wine_id + processed_at.

The script is resume-safe: rows already marked `processed_at` are skipped.

Usage:
    python -m pipeline.promote.lwin_long_tail --analyze
    python -m pipeline.promote.lwin_long_tail --dry-run
    python -m pipeline.promote.lwin_long_tail --dry-run --sample 10
    python -m pipeline.promote.lwin_long_tail --execute
    python -m pipeline.promote.lwin_long_tail --execute --limit-producers 50
    python -m pipeline.promote.lwin_long_tail --execute --progress-file data/stats/lwin_long_tail_log.txt

Thresholds:
    --intl-min-wines N  (default 8)  — INTL producers with fewer than N wines are skipped
    US producers are ALL processed (except Haiku-flagged junk)

Junk filter:
    Reads data/stats/lwin_us_junk_classifications.json produced by
    `pipeline/promote/lwin_junk_filter.py`. If absent, no US producers are filtered.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify
from pipeline.lib.resolve import ReferenceResolver


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JUNK_FILE = PROJECT_ROOT / "data" / "stats" / "lwin_us_junk_classifications.json"


# ── Helpers ─────────────────────────────────────────────────

def load_junk_set(apply: bool = False) -> set[str]:
    """Load US junk producer names from the Haiku classification file.

    By default, returns empty set (junk filter DISABLED). LWIN is a well-curated
    trade identifier system; we prefer inclusive promotion over risking cuts of
    real producers. Pass apply=True to re-enable the filter if needed.
    """
    if not apply:
        print("  Junk filter: DISABLED (LWIN inclusive mode — all US producers processed)")
        return set()
    if not JUNK_FILE.exists():
        print(f"  [warn] Junk file not found: {JUNK_FILE} — no US producers filtered")
        return set()
    try:
        payload = json.loads(JUNK_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  [warn] Could not parse {JUNK_FILE}: {e}")
        return set()
    classifications = payload.get("classifications", {})
    junk = {name for name, v in classifications.items() if not v.get("real", True)}
    print(f"  Junk filter: {len(junk)} US producer names flagged (from {len(classifications)} classified)")
    return junk


def fetch_eligible_producers(conn, intl_min_wines: int, us_junk: set[str]) -> list[tuple[str, str, int]]:
    """Return list of (producer_name, country, wine_count) for producers to process.

    - US: all producers, excluding junk set
    - INTL: producers with >= intl_min_wines
    """
    sql = """
        WITH buckets AS (
            SELECT producer_name, country, COUNT(*) AS wines
            FROM source_lwin
            WHERE producer_name IS NOT NULL
              AND TRIM(producer_name) != ''
            GROUP BY producer_name, country
        )
        SELECT producer_name, country, wines
        FROM buckets
        WHERE (country = 'United States')
           OR (country != 'United States' AND wines >= %s)
        ORDER BY
            CASE WHEN country = 'United States' THEN 0 ELSE 1 END,
            wines DESC,
            producer_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, (intl_min_wines,))
        rows = cur.fetchall()

    eligible = []
    skipped_junk = 0
    for name, country, wines in rows:
        if country == "United States" and name in us_junk:
            skipped_junk += 1
            continue
        eligible.append((name, country, int(wines)))

    print(f"  Eligible: {len(eligible):,} producers ({skipped_junk} US junk filtered)")
    return eligible


def fetch_lwin_rows_for_producer(conn, producer_name: str, country: str) -> list[dict]:
    """Fetch all source_lwin rows for a producer+country combo, unprocessed only.

    Note: source_lwin has no numeric `id` — the primary key is `lwin` (text).
    """
    sql = """
        SELECT lwin, lwin_7, producer_name, wine_name, country,
               region, sub_region, appellation, colour, wine_type, designation
        FROM source_lwin
        WHERE producer_name = %s
          AND country = %s
          AND processed_at IS NULL
        ORDER BY wine_name, lwin
    """
    with conn.cursor() as cur:
        cur.execute(sql, (producer_name, country))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def normalize_color(raw: str | None) -> str | None:
    """LWIN colour text -> canonical wines.color enum."""
    if not raw:
        return None
    c = raw.strip().lower()
    if c in ("red", "r"):
        return "red"
    if c in ("white", "w", "wt"):
        return "white"
    if c in ("rose", "rosé", "rose wine", "rosado", "rosato", "pink"):
        return "rose"
    return None


# Sparkling detection: LWIN region names + wine-name keywords
SPARKLING_REGIONS = {
    "champagne",           # France
    "franciacorta",        # Italy
    "prosecco",            # Italy (also a grape, but the region is a sparkling name)
}
SPARKLING_NAME_TOKENS = (
    "champagne", "brut", "extra brut", "extra-brut", "extra sec", "demi-sec", "sec",
    "prosecco", "franciacorta", "cava", "cremant", "crémant", "cremante",
    "sekt", "asti spumante", "moscato d'asti", "blanquette",
    "cap classique", "mcc", "spumante", "metodo classico",
)


def classify_wine_type_effervescence(
    wine_name: str | None,
    region: str | None,
    sub_region: str | None,
    appellation_tier: str | None,
    lwin_wine_type: str | None,
) -> tuple[str, str]:
    """Infer (wine_type, effervescence) for the canonical wines row.

    Returns canonical enum values: wine_type in {'table','sparkling','fortified'},
    effervescence in {'still','sparkling','frizzante'}.
    """
    # Fortified: LWIN explicitly sets wine_type='Fortified Wine' for 3,073 rows
    if lwin_wine_type and lwin_wine_type.strip().lower() == "fortified wine":
        return "fortified", "still"

    # Sparkling detection
    name_low = (wine_name or "").lower()
    region_low = (region or "").lower()
    sub_region_low = (sub_region or "").lower()

    # Region-based match
    if region_low in SPARKLING_REGIONS:
        return "sparkling", "sparkling"
    if any(s in region_low for s in SPARKLING_REGIONS):
        return "sparkling", "sparkling"
    if any(s in sub_region_low for s in SPARKLING_REGIONS):
        return "sparkling", "sparkling"

    # Name-token match (word-boundary-aware for short tokens)
    for tok in SPARKLING_NAME_TOKENS:
        if f" {tok} " in f" {name_low} " or name_low.startswith(f"{tok} ") or name_low.endswith(f" {tok}") or name_low == tok:
            return "sparkling", "sparkling"

    # Default: still table wine
    return "table", "still"


def match_or_create_producer(
    conn, producer_name: str, country_id: str | None, region_id: str | None,
    dry_run: bool,
) -> tuple[str | None, str]:
    """Return (producer_id, action). action in {'matched', 'created', 'dry', 'error'}.

    Does NOT commit — caller is responsible for committing the transaction.
    """
    norm = normalize(producer_name)
    if not norm:
        return None, "error"

    # Match within country if we have one, else global
    with conn.cursor() as cur:
        if country_id:
            cur.execute(
                """
                SELECT id FROM producers
                WHERE name_normalized = %s
                  AND country_id = %s
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                (norm, country_id),
            )
        else:
            cur.execute(
                """
                SELECT id FROM producers
                WHERE name_normalized = %s
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                (norm,),
            )
        row = cur.fetchone()
        if row:
            return str(row[0]), "matched"

    if dry_run:
        return None, "dry"

    base_slug = slugify(producer_name)
    country_suffix = (country_id[:8] if country_id else "x")
    slug_attempts = [
        base_slug,
        f"{base_slug}-{country_suffix}",
        f"{base_slug}-{country_suffix}-{norm[:6]}",
    ]
    with conn.cursor() as cur:
        last_err = None
        for attempt_slug in slug_attempts:
            cur.execute("SAVEPOINT prod_ins")
            try:
                cur.execute(
                    """
                    INSERT INTO producers (name, name_normalized, slug, country_id, region_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (producer_name, norm, attempt_slug, country_id, region_id),
                )
                new_id = str(cur.fetchone()[0])
                cur.execute("RELEASE SAVEPOINT prod_ins")
                return new_id, "created"
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT prod_ins")
                last_err = e
        print(f"      [err] producer insert failed for {producer_name!r}: {last_err}")
        return None, "error"


def fetch_existing_wines_for_producer(conn, producer_id: str) -> dict[str, str]:
    """Return {name_normalized: wine_id} for all active canonical wines under this producer."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name_normalized, id FROM wines
            WHERE producer_id = %s
              AND deleted_at IS NULL
              AND duplicate_of IS NULL
            """,
            (producer_id,),
        )
        return {norm: str(wid) for norm, wid in cur.fetchall() if norm}


def match_or_create_wine(
    conn, producer_id: str, wine_name: str, country_id: str | None,
    region_id: str | None, appellation_id: str | None, color: str | None,
    wine_type: str, effervescence: str,
    dry_run: bool,
    existing_wines_cache: dict[str, str] | None = None,
) -> tuple[str | None, str]:
    """Return (wine_id, action). action in {'matched', 'created', 'dry', 'error'}.

    If existing_wines_cache is provided (pre-fetched {name_normalized: wine_id}),
    uses it instead of querying. Successfully inserted wines are added to the cache
    so same-producer duplicates within a batch are de-duplicated too.

    Does NOT commit — caller is responsible for committing the transaction.
    """
    if not wine_name:
        return None, "error"
    norm = normalize(wine_name)
    if not norm:
        return None, "error"

    # Cache path
    if existing_wines_cache is not None:
        cached = existing_wines_cache.get(norm)
        if cached:
            return cached, "matched"
    else:
        # Fallback single-row lookup
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM wines
                WHERE producer_id = %s
                  AND name_normalized = %s
                  AND deleted_at IS NULL
                  AND duplicate_of IS NULL
                LIMIT 1
                """,
                (producer_id, norm),
            )
            row = cur.fetchone()
            if row:
                return str(row[0]), "matched"

    if dry_run:
        return None, "dry"

    base_slug = slugify(wine_name)
    # Try INSERT with savepoint isolation so slug collisions don't abort the outer tx
    with conn.cursor() as cur:
        for attempt_slug in (base_slug, f"{base_slug}-{str(producer_id)[:8]}", f"{base_slug}-{norm[:6]}-{str(producer_id)[:8]}"):
            cur.execute("SAVEPOINT wine_ins")
            try:
                cur.execute(
                    """
                    INSERT INTO wines (
                        name, name_normalized, slug, producer_id,
                        country_id, region_id, appellation_id, color,
                        wine_type, effervescence
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        wine_name, norm, attempt_slug, producer_id,
                        country_id, region_id, appellation_id, color,
                        wine_type, effervescence,
                    ),
                )
                new_id = str(cur.fetchone()[0])
                cur.execute("RELEASE SAVEPOINT wine_ins")
                if existing_wines_cache is not None:
                    existing_wines_cache[norm] = new_id
                return new_id, "created"
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT wine_ins")
                # Continue trying next slug variant
                last_err = e
        # All slug variants exhausted
        print(f"      [err] wine insert failed for {wine_name!r}: {last_err}")
        return None, "error"


def bulk_insert_external_ids(conn, rows: list[tuple[str, str]]) -> int:
    """Bulk insert LWIN external_ids. rows = [(wine_id, lwin_value), ...].

    Returns rows inserted (conflicts skipped). Does NOT commit.
    Uses a savepoint so failure rolls back only this batch.
    """
    if not rows:
        return 0
    values_sql = ",".join(
        f"('wine', '{wine_id}'::uuid, 'lwin', %s)" for wine_id, _ in rows
    )
    params = [lwin_val for _, lwin_val in rows]
    sql = f"""
        INSERT INTO external_ids (entity_type, entity_id, system, external_id)
        VALUES {values_sql}
        ON CONFLICT (entity_id, entity_type, system, external_id) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT ext_ins")
        try:
            cur.execute(sql, params)
            count = cur.rowcount or 0
            cur.execute("RELEASE SAVEPOINT ext_ins")
            return count
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT ext_ins")
            print(f"      [err] bulk external_ids insert failed ({len(rows)} rows): {e}")
            return 0


def bulk_update_source_lwin(
    conn, updates: list[tuple[str, str | None, str | None]]
) -> int:
    """Bulk update source_lwin rows. updates = [(lwin, wine_id, producer_id), ...].

    Uses a VALUES-list UPDATE for efficiency and a savepoint for isolation.
    Does NOT commit.
    """
    if not updates:
        return 0
    placeholders = ",".join(["(%s, %s::uuid, %s::uuid)"] * len(updates))
    params: list = []
    for lwin, wine_id, producer_id in updates:
        params.extend([lwin, wine_id, producer_id])
    sql = f"""
        UPDATE source_lwin sl
        SET canonical_wine_id = v.wine_id,
            canonical_producer_id = v.producer_id,
            processed_at = NOW()
        FROM (VALUES {placeholders}) AS v(lwin, wine_id, producer_id)
        WHERE sl.lwin = v.lwin
    """
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT sl_upd")
        try:
            cur.execute(sql, params)
            count = cur.rowcount or 0
            cur.execute("RELEASE SAVEPOINT sl_upd")
            return count
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT sl_upd")
            print(f"      [err] bulk source_lwin update failed ({len(updates)} rows): {e}")
            return 0


# ── Main sweep ──────────────────────────────────────────────

def run_sweep(
    intl_min_wines: int,
    dry_run: bool,
    sample_count: int | None = None,
    limit_producers: int | None = None,
    progress_file: Path | None = None,
    apply_junk_filter: bool = False,
):
    """Run the LWIN long-tail promotion sweep."""
    print("=" * 70)
    print(f"  LWIN Long-Tail Sweep — Session 13")
    junk_label = "w/ junk filter" if apply_junk_filter else "all (inclusive)"
    print(f"  US: {junk_label}  |  INTL: >= {intl_min_wines} wines")
    print(f"  Mode: {'DRY-RUN' if dry_run else 'EXECUTE'}")
    if sample_count:
        print(f"  SAMPLE MODE: {sample_count} random producers per bucket")
    print("=" * 70)

    conn = get_conn()
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    us_junk = load_junk_set(apply=apply_junk_filter)
    eligible = fetch_eligible_producers(conn, intl_min_wines, us_junk)

    if sample_count:
        # Sample producers across several buckets for dry-run inspection
        import random
        random.seed(42)

        def _bucket(wines: int) -> str:
            if wines >= 20: return "20+"
            if wines >= 10: return "10-19"
            if wines >= 8:  return "8-9"
            if wines >= 5:  return "5-7"
            if wines >= 3:  return "3-4"
            if wines >= 2:  return "2"
            return "1"

        by_bucket_region: dict[tuple, list] = {}
        for prod in eligible:
            key = (_bucket(prod[2]), "US" if prod[1] == "United States" else "INTL")
            by_bucket_region.setdefault(key, []).append(prod)

        sampled = []
        for key in sorted(by_bucket_region.keys()):
            pool = by_bucket_region[key]
            chosen = random.sample(pool, min(sample_count, len(pool)))
            sampled.extend(chosen)
            print(f"  Sample {key}: {len(chosen)} of {len(pool)} producers")
        eligible = sampled

    if limit_producers:
        eligible = eligible[:limit_producers]

    print(f"\nProcessing {len(eligible):,} producers\n")

    stats = {
        "producers_matched": 0,
        "producers_created": 0,
        "producers_failed": 0,
        "wines_matched": 0,
        "wines_created": 0,
        "wines_failed": 0,
        "external_ids_created": 0,
        "source_lwin_updated": 0,
        "lwin_rows_processed": 0,
        "start_time": time.time(),
    }

    producers_seen = 0
    log_lines = []

    def _log(line: str):
        print(line)
        if progress_file:
            log_lines.append(line)
            if len(log_lines) >= 20:
                _flush_log()

    def _flush_log():
        if progress_file and log_lines:
            with progress_file.open("a", encoding="utf-8") as f:
                f.write("\n".join(log_lines) + "\n")
            log_lines.clear()

    for producer_name, country, wine_count in eligible:
        producers_seen += 1

        # Fetch all LWIN rows for this producer
        lwin_rows = fetch_lwin_rows_for_producer(conn, producer_name, country)
        if not lwin_rows:
            continue

        # Resolve country at producer level
        country_id = resolver.resolve_country(country)

        # Resolve the most common region across the producer's LWIN rows.
        # LWIN's `region` column is the proper region name (Champagne, Napa Valley, Burgundy).
        region_counts: dict[str, int] = {}
        for r in lwin_rows:
            if r.get("region"):
                region_counts[r["region"]] = region_counts.get(r["region"], 0) + 1
        region_name = max(region_counts.items(), key=lambda kv: kv[1])[0] if region_counts else None
        region_obj = resolver.resolve_region(region_name, country_id) if region_name else None
        region_id = region_obj["id"] if region_obj else None

        # Producer match/create
        producer_id, prod_action = match_or_create_producer(
            conn, producer_name, country_id, region_id, dry_run
        )

        if prod_action == "matched":
            stats["producers_matched"] += 1
        elif prod_action == "created":
            stats["producers_created"] += 1
        elif prod_action == "dry":
            stats["producers_created"] += 1
        else:
            stats["producers_failed"] += 1
            continue

        # Progress print every producer (verbose) or every 25 (sweep)
        if sample_count or producers_seen <= 10:
            _log(f"  [{producers_seen:4d}] {producer_name!r} ({country}, {wine_count} wines) "
                 f"-> producer {prod_action} | {len(lwin_rows)} unprocessed lwin rows")

        # Pre-fetch all existing wines under this producer (1 round-trip vs N)
        existing_wines_cache: dict[str, str] = {}
        if producer_id and not dry_run:
            existing_wines_cache = fetch_existing_wines_for_producer(conn, producer_id)
        elif producer_id and dry_run:
            # In dry-run, still pre-fetch so the "matched" counts are accurate
            existing_wines_cache = fetch_existing_wines_for_producer(conn, producer_id)

        # Accumulate per-producer writes for batching
        pending_external_ids: list[tuple[str, str]] = []   # (wine_id, lwin_value)
        pending_source_lwin: list[tuple[str, str | None, str | None]] = []  # (lwin, wine_id, producer_id)

        # Process each LWIN row for this producer
        for row in lwin_rows:
            stats["lwin_rows_processed"] += 1
            wine_name = row.get("wine_name")
            if not wine_name:
                continue

            # LWIN's `sub_region` is the ACTUAL appellation name (Meursault, Chablis,
            # Rutherford, Gevrey-Chambertin). LWIN's `appellation` column is just the
            # classification tier (AOP, AVA, DOCG) and is not useful for lookup.
            sub_region_name = row.get("sub_region")
            lwin_region_name = row.get("region")
            tier = row.get("appellation")  # AOP/AVA/DOCG classification tier

            appellation_obj = resolver.resolve_appellation(sub_region_name, country_id)
            appellation_id = appellation_obj["id"] if appellation_obj else None

            # Per-wine region: prefer producer-level, fall back to row region, then appellation region
            effective_region_id = region_id
            if not effective_region_id and lwin_region_name:
                row_region_obj = resolver.resolve_region(lwin_region_name, country_id)
                if row_region_obj:
                    effective_region_id = row_region_obj["id"]
            if not effective_region_id and appellation_obj and appellation_obj.get("region_id"):
                effective_region_id = appellation_obj["region_id"]

            color = normalize_color(row.get("colour"))
            wine_type, effervescence = classify_wine_type_effervescence(
                wine_name, lwin_region_name, sub_region_name, tier, row.get("wine_type")
            )

            wine_id, wine_action = match_or_create_wine(
                conn, producer_id, wine_name, country_id, effective_region_id,
                appellation_id, color, wine_type, effervescence, dry_run,
                existing_wines_cache=existing_wines_cache,
            )

            if wine_action == "matched":
                stats["wines_matched"] += 1
            elif wine_action == "created":
                stats["wines_created"] += 1
            elif wine_action == "dry":
                stats["wines_created"] += 1
            else:
                stats["wines_failed"] += 1
                continue

            if not dry_run and wine_id:
                lwin_value = row.get("lwin_7") or row.get("lwin")
                if lwin_value:
                    pending_external_ids.append((wine_id, str(lwin_value)))
                pending_source_lwin.append((row["lwin"], wine_id, producer_id))

            if sample_count and producers_seen <= 10:
                _log(f"        -> {wine_name!r} [{wine_action}] "
                     f"appel={sub_region_name or '-'}({'ok' if appellation_id else 'miss'}) "
                     f"color={color or '-'} type={wine_type}")

        # Bulk-flush this producer's pending writes + commit once
        if not dry_run:
            if pending_external_ids:
                inserted = bulk_insert_external_ids(conn, pending_external_ids)
                stats["external_ids_created"] += inserted
            if pending_source_lwin:
                updated = bulk_update_source_lwin(conn, pending_source_lwin)
                stats["source_lwin_updated"] += updated
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"      [err] commit failed after producer {producer_name!r}: {e}")

        # Bulk sweep progress
        if not sample_count and producers_seen % 25 == 0:
            elapsed = time.time() - stats["start_time"]
            rate = producers_seen / elapsed if elapsed > 0 else 0
            remaining = len(eligible) - producers_seen
            eta = remaining / rate if rate > 0 else 0
            _log(
                f"  [{producers_seen:5d}/{len(eligible):,}] "
                f"P:{stats['producers_matched']}m/{stats['producers_created']}c  "
                f"W:{stats['wines_matched']}m/{stats['wines_created']}c  "
                f"ext:{stats['external_ids_created']}  "
                f"rate={rate:.1f}/s  eta={eta/60:.1f}min"
            )

    _flush_log()

    elapsed = time.time() - stats["start_time"]
    print("\n" + "=" * 70)
    print("  LWIN Long-Tail Sweep — Results")
    print("=" * 70)
    print(f"  Producers:    {stats['producers_matched']:>6,} matched, {stats['producers_created']:>6,} created, {stats['producers_failed']} failed")
    print(f"  Wines:        {stats['wines_matched']:>6,} matched, {stats['wines_created']:>6,} created, {stats['wines_failed']} failed")
    print(f"  LWIN rows:    {stats['lwin_rows_processed']:>6,} processed")
    print(f"  External IDs: {stats['external_ids_created']:>6,} upserted")
    print(f"  source_lwin:  {stats['source_lwin_updated']:>6,} updated")
    print(f"  Elapsed:      {elapsed/60:.1f} min ({stats['lwin_rows_processed']/elapsed:.1f} rows/s)")
    if dry_run:
        print("\n  (dry-run — no writes)")


def main():
    parser = argparse.ArgumentParser(description="LWIN long-tail promotion sweep")
    parser.add_argument("--analyze", action="store_true", help="Show bucket stats and exit")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writes")
    parser.add_argument("--execute", action="store_true", help="Execute the sweep")
    parser.add_argument("--intl-min-wines", type=int, default=8, help="Minimum wines for INTL producers (default 8)")
    parser.add_argument("--sample", type=int, help="Dry-run sample N producers per bucket")
    parser.add_argument("--limit-producers", type=int, help="Cap number of producers processed")
    parser.add_argument("--progress-file", type=str, help="Append progress lines to this file")
    parser.add_argument("--apply-junk-filter", action="store_true",
                       help="Apply Haiku junk filter to US producers (default: inclusive, no filter)")
    args = parser.parse_args()

    if not any([args.analyze, args.dry_run, args.execute]):
        parser.error("Specify --analyze, --dry-run, or --execute")

    if args.analyze:
        conn = get_conn()
        us_junk = load_junk_set(apply=args.apply_junk_filter)
        eligible = fetch_eligible_producers(conn, args.intl_min_wines, us_junk)
        total_wines = sum(p[2] for p in eligible)
        us_count = sum(1 for p in eligible if p[1] == "United States")
        intl_count = len(eligible) - us_count
        us_wines = sum(p[2] for p in eligible if p[1] == "United States")
        intl_wines = total_wines - us_wines
        print("\n=== Eligible totals ===")
        print(f"  US:       {us_count:>6,} producers   {us_wines:>7,} wines")
        print(f"  INTL>={args.intl_min_wines}: {intl_count:>6,} producers   {intl_wines:>7,} wines")
        print(f"  TOTAL:    {len(eligible):>6,} producers   {total_wines:>7,} wines")
        conn.close()
        return

    progress_file = Path(args.progress_file) if args.progress_file else None
    if progress_file:
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        progress_file.write_text(
            f"LWIN long-tail sweep started {datetime.now(timezone.utc).isoformat()}\n",
            encoding="utf-8",
        )

    run_sweep(
        intl_min_wines=args.intl_min_wines,
        dry_run=args.dry_run,
        sample_count=args.sample,
        limit_producers=args.limit_producers,
        progress_file=progress_file,
        apply_junk_filter=args.apply_junk_filter,
    )


if __name__ == "__main__":
    main()
