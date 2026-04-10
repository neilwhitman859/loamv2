"""
Josh Test: measure Loam's ability to find wines Americans actually encounter.

Loads the sample from data/josh_test_sample.json and tests each wine against
the canonical database. Measures find rate, data quality, and coverage by
price tier and country.

Usage:
    python -m pipeline.analyze.josh_test [--sample data/josh_test_sample.json]
    python -m pipeline.analyze.josh_test --tier "$30-100"  # single tier
    python -m pipeline.analyze.josh_test --staging  # check staging coverage
    python -m pipeline.analyze.josh_test --save  # save to data/stats/josh_test_latest.json
"""
import argparse
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pipeline.lib.db import get_conn

LATEST_RESULTS_PATH = Path("data/stats/josh_test_latest.json")


SAMPLE_PATH = Path("data/josh_test_sample.json")

# Staging tables and the columns to search for producer names
STAGING_SOURCES = [
    ("source_lwin", "producer"),
    ("source_ttb_colas", "brand_name"),
    ("source_skurnik", "producer"),
    ("source_empson", "producer"),
    ("source_winebow", "producer"),
    ("source_european_cellars", "producer"),
    ("source_kermit_lynch", "producer"),
    ("source_flatiron", "producer"),
    ("source_specs", "brand"),
    ("source_bc_liquor", "producer"),
    ("source_systembolaget", "producer"),
    ("source_texsom", "producer"),
    ("source_berliner", "producer"),
    ("source_enofile", "producer"),
    ("source_wallys", "title"),  # producer column is NULL; title has it embedded
    ("source_domestique", "producer"),
    ("source_best_wine_store", "producer"),
    ("source_firstleaf", "producer"),
    ("source_claude_knowledge", "producer"),
    ("source_pro_platform", "brand_name"),
    ("source_tabc", "brand_name"),
    ("source_kansas_brands", "brand_name"),
    ("source_wv_abca", "brand_name"),
]


def _normalize(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s.lower().strip())


def load_sample(path: Path = SAMPLE_PATH) -> list[dict]:
    """Load the Josh Test wine sample."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data["wines"]


def _calc_depth(cur, wine_id: str) -> int:
    """Calculate depth score 0-8 for a found wine."""
    score = 0

    cur.execute("""
        SELECT color, appellation_id, region_id, country_id
        FROM wines WHERE id = %s
    """, (wine_id,))
    row = cur.fetchone()
    if not row:
        return 0
    color, app_id, region_id, country_id = row

    if color:
        score += 1
    if app_id or region_id:
        score += 1
    if country_id:
        score += 1

    # Grapes
    cur.execute("SELECT 1 FROM wine_grapes WHERE wine_id = %s LIMIT 1", (wine_id,))
    if cur.fetchone():
        score += 1

    # Vintages
    cur.execute("SELECT 1 FROM wine_vintages WHERE wine_id = %s LIMIT 1", (wine_id,))
    if cur.fetchone():
        score += 1

    # Prices
    cur.execute("""
        SELECT 1 FROM wine_vintage_prices p
        JOIN wine_vintages v ON p.wine_vintage_id = v.id
        WHERE v.wine_id = %s LIMIT 1
    """, (wine_id,))
    if cur.fetchone():
        score += 1

    # Scores
    cur.execute("""
        SELECT 1 FROM wine_vintage_scores s
        JOIN wine_vintages v ON s.wine_vintage_id = v.id
        WHERE v.wine_id = %s LIMIT 1
    """, (wine_id,))
    if cur.fetchone():
        score += 1

    # COLA or LWIN backbone
    cur.execute("""
        SELECT 1 FROM external_ids
        WHERE entity_type = 'wine' AND entity_id = %s AND system IN ('cola', 'lwin_7')
        LIMIT 1
    """, (wine_id,))
    if cur.fetchone():
        score += 1

    return score


def test_findability(wines: list[dict]) -> dict:
    """
    Honest findability test using the real search_catalog RPC.

    For each wine, calls search_catalog(search_term, 10, ['wine']) —
    the exact same search a user would trigger. A wine is "found" only if
    a result from the correct producer appears in the top 10.
    """
    conn = get_conn()
    cur = conn.cursor()

    found_wines = []
    missing_wines = []
    tier_counts = defaultdict(lambda: {"total": 0, "found": 0})
    country_counts = defaultdict(lambda: {"total": 0, "found": 0})
    depth_scores = []
    confirmation_scores = []  # ints: A=4, B=3, C=2, D=1, None=0
    completeness_scores = []
    enrichment_scores = []  # ints: 2=B, 1=C, 0=F/D
    CONF_TO_INT = {'A': 4, 'B': 3, 'C': 2, 'D': 1, None: 0}
    INT_TO_CONF = {4: 'A', 3: 'B', 2: 'C', 1: 'D', 0: 'NULL'}

    for w in wines:
        tier = w["price_tier"]
        cc = w.get("country", "??")
        tier_counts[tier]["total"] += 1
        country_counts[cc]["total"] += 1

        found = False
        matched_wine_id = None
        search_term = w["search_term"]

        # Call the real search_catalog RPC
        try:
            cur.execute("""
                SELECT entity_type, id, name, slug, subtitle, score
                FROM search_catalog(%s, 10, ARRAY['wine'])
            """, (search_term,))
            results = cur.fetchall()
        except Exception:
            conn.rollback()
            results = []

        if results:
            # Check if any result is from the right producer
            producer_norm = _normalize(w["producer"])
            # Build producer variants for matching
            producer_variants = {producer_norm}
            for prefix in ["chateau ", "domaine ", "bodegas ", "maison ",
                           "tenuta ", "cantina ", "weingut "]:
                if producer_norm.startswith(prefix):
                    producer_variants.add(producer_norm[len(prefix):])
            for suffix in [" vineyards", " vineyard", " winery", " estate",
                           " cellars", " wines"]:
                if producer_norm.endswith(suffix):
                    producer_variants.add(producer_norm[:-len(suffix)])

            for etype, rid, rname, rslug, subtitle, score in results:
                # subtitle is producer name for wine results
                subtitle_norm = _normalize(subtitle) if subtitle else ""
                name_norm = _normalize(rname) if rname else ""

                # Check if result producer matches expected producer
                producer_match = False
                for variant in producer_variants:
                    if variant and (variant in subtitle_norm or subtitle_norm in variant):
                        producer_match = True
                        break
                    # Also check if producer name appears in the wine name/slug
                    if variant and variant in name_norm:
                        producer_match = True
                        break

                if producer_match:
                    found = True
                    matched_wine_id = str(rid)
                    break

        if found:
            found_wines.append(w)
            tier_counts[tier]["found"] += 1
            country_counts[cc]["found"] += 1

            if matched_wine_id:
                depth = _calc_depth(cur, matched_wine_id)
                depth_scores.append(depth)
                w["_depth"] = depth
                w["_wine_id"] = matched_wine_id

                # Pull confirmation, completeness, data_grade for the matched wine
                cur.execute("""
                    SELECT confirmation, completeness, data_grade
                    FROM wines WHERE id = %s
                """, (matched_wine_id,))
                row = cur.fetchone()
                if row:
                    conf, comp, dg = row
                    confirmation_scores.append(CONF_TO_INT.get(conf, 0))
                    completeness_scores.append(comp or 0)
                    # Map data_grade to enrichment level: B=2, C=1, else=0
                    if dg == 'B':
                        enrichment_scores.append(2)
                    elif dg == 'C':
                        enrichment_scores.append(1)
                    else:
                        enrichment_scores.append(0)
        else:
            missing_wines.append(w)

    cur.close()
    conn.close()

    total = len(wines)
    found = len(found_wines)

    for d in tier_counts.values():
        d["rate"] = d["found"] / d["total"] if d["total"] else 0
    for d in country_counts.values():
        d["rate"] = d["found"] / d["total"] if d["total"] else 0

    avg_depth = sum(depth_scores) / len(depth_scores) if depth_scores else 0
    avg_conf_int = sum(confirmation_scores) / len(confirmation_scores) if confirmation_scores else 0
    avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
    avg_enrichment_int = sum(enrichment_scores) / len(enrichment_scores) if enrichment_scores else 0
    # Round avg confirmation to nearest int letter
    avg_confirmation_letter = INT_TO_CONF.get(round(avg_conf_int), 'NULL')
    if avg_enrichment_int >= 1.5:
        avg_enrichment_letter = 'B'
    elif avg_enrichment_int >= 0.5:
        avg_enrichment_letter = 'C'
    else:
        avg_enrichment_letter = 'F/D'

    return {
        "total": total,
        "found": found,
        "find_rate": found / total if total else 0,
        "avg_depth": avg_depth,
        "avg_confirmation": avg_confirmation_letter,
        "avg_completeness": round(avg_completeness, 1),
        "avg_enrichment": avg_enrichment_letter,
        "by_tier": dict(tier_counts),
        "by_country": dict(country_counts),
        "found_wines": found_wines,
        "missing": missing_wines,
    }


def test_staging_coverage(wines: list[dict]) -> dict:
    """
    Check how many Josh Test wines exist in staging tables (pre-promotion).

    For each wine, searches staging tables for the producer name.
    A wine is "found" if its producer appears in at least one staging source.

    Strategy: one batched query per staging table (efficient), skip full-scan
    on huge tables (TTB 3.28M) in favor of sampled check.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Deduplicate producers to minimize queries
    producers = {}
    for w in wines:
        key = _normalize(w["producer"])
        if key not in producers:
            producers[key] = {"orig": w["producer"], "patterns": set()}
        # Primary pattern
        producers[key]["patterns"].add(key)
        # Also try without common prefixes
        for prefix in ["chateau ", "domaine ", "bodegas ", "caves ",
                        "maison ", "tenuta ", "cantina "]:
            if key.startswith(prefix):
                producers[key]["patterns"].add(key[len(prefix):])

    producer_found = {k: False for k in producers}

    # For each staging table, run ONE query checking all producers
    for table, col in STAGING_SOURCES:
        # Build a single query: check all producer patterns at once
        all_patterns = []
        pattern_to_producers = {}
        for pkey, pdata in producers.items():
            if producer_found[pkey]:
                continue  # already found, skip
            for pat in pdata["patterns"]:
                all_patterns.append(f"%{pat}%")
                pattern_to_producers.setdefault(f"%{pat}%", []).append(pkey)

        if not all_patterns:
            break  # all found

        # For huge tables, use a different approach: check a sample
        try:
            cur.execute(f"SELECT reltuples::bigint FROM pg_class WHERE relname = %s",
                        (table,))
            row = cur.fetchone()
            row_count = row[0] if row else 0
        except Exception:
            conn.rollback()
            row_count = 0

        try:
            if row_count > 500_000:
                # Large table: check each unfound producer individually with LIMIT
                for pkey, pdata in producers.items():
                    if producer_found[pkey]:
                        continue
                    for pat in pdata["patterns"]:
                        cur.execute(
                            f"SELECT 1 FROM {table} WHERE lower({col}) LIKE %s LIMIT 1",
                            (f"%{pat}%",)
                        )
                        if cur.fetchone():
                            producer_found[pkey] = True
                            break
            else:
                # Small table: pull all distinct producer values, match in Python
                cur.execute(f"SELECT DISTINCT lower({col}) FROM {table} WHERE {col} IS NOT NULL")
                values = {row[0] for row in cur.fetchall()}
                for pkey, pdata in producers.items():
                    if producer_found[pkey]:
                        continue
                    for pat in pdata["patterns"]:
                        if any(pat in v for v in values):
                            producer_found[pkey] = True
                            break
        except Exception:
            conn.rollback()
            continue

    cur.close()
    conn.close()

    # Map back to wines
    found_wines = []
    missing_wines = []
    tier_counts = defaultdict(lambda: {"total": 0, "found": 0})
    country_counts = defaultdict(lambda: {"total": 0, "found": 0})

    for w in wines:
        key = _normalize(w["producer"])
        tier = w["price_tier"]
        cc = w.get("country", "??")
        tier_counts[tier]["total"] += 1
        country_counts[cc]["total"] += 1

        if producer_found.get(key, False):
            found_wines.append(w)
            tier_counts[tier]["found"] += 1
            country_counts[cc]["found"] += 1
        else:
            missing_wines.append(w)

    total = len(wines)
    found = len(found_wines)

    # Add rates to tier/country dicts
    for d in tier_counts.values():
        d["rate"] = d["found"] / d["total"] if d["total"] else 0
    for d in country_counts.values():
        d["rate"] = d["found"] / d["total"] if d["total"] else 0

    return {
        "total": total,
        "found": found,
        "find_rate": found / total if total else 0,
        "by_tier": dict(tier_counts),
        "by_country": dict(country_counts),
        "missing": missing_wines,
    }


def print_report(results: dict) -> None:
    """Print formatted Josh Test results."""
    mode = "FINDABILITY" if "found_wines" in results else "STAGING COVERAGE"
    found_key = "found"

    print(f"\n{'='*60}")
    print(f"  JOSH TEST — {mode}")
    print(f"{'='*60}")
    print(f"  Overall: {results[found_key]}/{results['total']}"
          f" ({results['find_rate']:.0%})")
    if "avg_depth" in results:
        print(f"  Avg depth (found wines): {results['avg_depth']:.1f}/8")
    print()

    print("  By price tier:")
    for tier in ["$0-10", "$10-30", "$30-100", "$100-250", "$250+"]:
        data = results.get("by_tier", {}).get(tier)
        if data:
            print(f"    {tier:>10}: {data['found']:>3}/{data['total']:<3}"
                  f" ({data['rate']:.0%})")
    print()

    print("  By country:")
    by_cc = results.get("by_country", {})
    for cc in sorted(by_cc.keys(), key=lambda k: -by_cc[k]["total"]):
        data = by_cc[cc]
        print(f"    {cc:>4}: {data['found']:>3}/{data['total']:<3}"
              f" ({data['rate']:.0%})")
    print()

    missing = results.get("missing", [])
    if missing:
        print(f"  Missing ({len(missing)}):")
        for w in missing:
            print(f"    [{w['price_tier']}] {w['search_term']} ({w['producer']})")

    print(f"{'='*60}")

    # Threshold check
    rate = results["find_rate"]
    if "found_wines" in results:
        # Findability test
        if rate >= 0.50:
            print(f"  [PASS] Findability {rate:.0%} >= 50% target")
        else:
            print(f"  [FAIL] Findability {rate:.0%} < 50% target")
    else:
        # Staging coverage
        if rate >= 0.50:
            print(f"  [PASS] S2.3: staging coverage {rate:.0%} >= 50%")
        else:
            print(f"  [FAIL] S2.3: staging coverage {rate:.0%} < 50%")


def save_results(results: dict, session: int | None = None, notes: str = "") -> None:
    """Persist findability results in the schema the dashboards expect."""
    LATEST_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "run_date": datetime.now(timezone.utc).date().isoformat(),
        "session": session,
        "method": "search_catalog RPC (v2, honest test)",
        "total_tested": results["total"],
        "found": results["found"],
        "find_rate": round(results["find_rate"], 2),
        "avg_depth_found": round(results.get("avg_depth", 0), 1),
        "avg_confirmation": results.get("avg_confirmation", "—"),
        "avg_completeness": results.get("avg_completeness", 0),
        "avg_enrichment": results.get("avg_enrichment", "—"),
        "by_tier": {
            tier: {
                "found": data["found"],
                "total": data["total"],
                "rate": round(data["rate"], 2),
            }
            for tier, data in results.get("by_tier", {}).items()
        },
        "by_country": {
            cc: {
                "found": data["found"],
                "total": data["total"],
                "rate": round(data["rate"], 2),
            }
            for cc, data in results.get("by_country", {}).items()
        },
    }
    if notes:
        payload["notes"] = notes

    LATEST_RESULTS_PATH.write_text(json.dumps(payload, indent=2))
    print(f"\n  Saved to {LATEST_RESULTS_PATH}")


def main():
    parser = argparse.ArgumentParser(description="Josh Test")
    parser.add_argument("--sample", default=str(SAMPLE_PATH))
    parser.add_argument("--tier", help="Test single price tier only")
    parser.add_argument("--staging", action="store_true",
                        help="Check staging coverage instead of canonical")
    parser.add_argument("--save", action="store_true",
                        help="Save results to data/stats/josh_test_latest.json")
    parser.add_argument("--session", type=int, help="Session number for save metadata")
    parser.add_argument("--notes", default="", help="Notes to embed in saved JSON")
    args = parser.parse_args()

    wines = load_sample(Path(args.sample))
    if args.tier:
        wines = [w for w in wines if w["price_tier"] == args.tier]

    if args.staging:
        results = test_staging_coverage(wines)
    else:
        results = test_findability(wines)

    print_report(results)

    if args.save and not args.staging:
        save_results(results, session=args.session, notes=args.notes)


if __name__ == "__main__":
    main()
