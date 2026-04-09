"""
Josh Test: measure Loam's ability to find wines Americans actually encounter.

Loads the sample from data/josh_test_sample.json and tests each wine against
the canonical database. Measures find rate, data quality, and coverage by
price tier and country.

Usage:
    python -m pipeline.analyze.josh_test [--sample data/josh_test_sample.json]
    python -m pipeline.analyze.josh_test --tier "$30-100"  # single tier
    python -m pipeline.analyze.josh_test --staging  # check staging coverage
"""
import argparse
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

from pipeline.lib.db import get_conn


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


def test_findability(wines: list[dict]) -> dict:
    """
    Test if each wine can be found in the canonical database.

    Search strategy per wine:
    1. Find producer by normalized name match (name_normalized or stripped prefix/suffix)
    2. If producer found, check if a matching wine exists (by name or grape)
    3. Also try search_vector full-text search as fallback
    """
    conn = get_conn()
    cur = conn.cursor()

    # Pre-load all producers with normalized names for fast matching
    cur.execute("""
        SELECT id, name, name_normalized, slug
        FROM producers WHERE deleted_at IS NULL
    """)
    all_producers = cur.fetchall()

    # Build producer lookup: normalized_name -> (id, name)
    producer_index = {}
    for pid, pname, pnorm, pslug in all_producers:
        key = _normalize(pname) if pname else ""
        if key:
            producer_index[key] = (str(pid), pname)
        # Also index without prefixes/suffixes
        for prefix in ["chateau ", "domaine ", "bodegas ", "maison ", "tenuta ",
                        "cantina ", "casa ", "weingut ", "caves "]:
            if key.startswith(prefix):
                producer_index[key[len(prefix):]] = (str(pid), pname)
        for suffix in [" vineyards", " vineyard", " winery", " estate",
                        " cellars", " wines", " wine", " estates"]:
            if key.endswith(suffix):
                producer_index[key[:-len(suffix)]] = (str(pid), pname)

    found_wines = []
    missing_wines = []
    tier_counts = defaultdict(lambda: {"total": 0, "found": 0})
    country_counts = defaultdict(lambda: {"total": 0, "found": 0})
    depth_scores = []

    for w in wines:
        tier = w["price_tier"]
        cc = w.get("country", "??")
        tier_counts[tier]["total"] += 1
        country_counts[cc]["total"] += 1

        found = False
        matched_wine_id = None

        # Strategy 1: Producer name lookup
        search_producer = _normalize(w["producer"])
        producer_match = producer_index.get(search_producer)

        # Try without prefix/suffix
        if not producer_match:
            for prefix in ["chateau ", "domaine ", "bodegas ", "maison "]:
                if search_producer.startswith(prefix):
                    producer_match = producer_index.get(search_producer[len(prefix):])
                    if producer_match:
                        break
        if not producer_match:
            for suffix in [" vineyards", " vineyard", " winery", " estate",
                            " cellars", " wines"]:
                if search_producer.endswith(suffix):
                    producer_match = producer_index.get(search_producer[:-len(suffix)])
                    if producer_match:
                        break

        if producer_match:
            pid, pname = producer_match
            # Get wines for this producer
            cur.execute("""
                SELECT id, name, display_name, color
                FROM wines WHERE producer_id = %s AND deleted_at IS NULL
            """, (pid,))
            producer_wines = cur.fetchall()

            # Try to match the specific wine
            search_norm = _normalize(w["search_term"])
            grape_norm = _normalize(w.get("grape", "")) if w.get("grape") else ""

            for wid, wname, display, color in producer_wines:
                wname_norm = _normalize(wname) if wname else ""
                display_norm = _normalize(display) if display else ""

                # Exact name match (either direction)
                if wname_norm and (wname_norm in search_norm or search_norm in wname_norm):
                    found = True
                    matched_wine_id = str(wid)
                    break
                if display_norm and (display_norm in search_norm or search_norm in display_norm):
                    found = True
                    matched_wine_id = str(wid)
                    break

                # Grape match (e.g., "Barefoot Cabernet Sauvignon" -> wine named "Cabernet Sauvignon")
                if grape_norm and wname_norm and grape_norm == wname_norm:
                    found = True
                    matched_wine_id = str(wid)
                    break

                # Search term contains wine name tokens
                if wname_norm and len(wname_norm) > 3:
                    # Check if all words in wine name appear in search term
                    wname_words = set(wname_norm.split())
                    search_words = set(search_norm.split())
                    if wname_words and wname_words.issubset(search_words):
                        found = True
                        matched_wine_id = str(wid)
                        break

                # Producer-is-wine match (Bordeaux estates, single-wine producers)
                # If wine has no cuvée name and producer name matches search
                if not wname and _normalize(pname) in search_norm:
                    found = True
                    matched_wine_id = str(wid)
                    break

            # If producer found but no specific wine matched, check if it's a
            # "producer-only" wine (e.g., "Apothic Red" where wine is "Red")
            if not found and producer_wines:
                # Try matching just the non-producer part of the search term
                remainder = search_norm.replace(_normalize(w["producer"]), "").strip()
                if remainder:
                    for wid, wname, display, color in producer_wines:
                        wname_norm = _normalize(wname) if wname else ""
                        if wname_norm and (wname_norm == remainder or remainder == wname_norm):
                            found = True
                            matched_wine_id = str(wid)
                            break

        # Strategy 2: Full-text search fallback
        if not found:
            search_terms = w["search_term"].split()
            tsquery = " & ".join(t for t in search_terms if len(t) > 2)
            if tsquery:
                try:
                    cur.execute("""
                        SELECT id FROM wines
                        WHERE deleted_at IS NULL
                          AND search_vector @@ to_tsquery('simple', %s)
                        LIMIT 5
                    """, (tsquery,))
                    rows = cur.fetchall()
                    if rows:
                        found = True
                        matched_wine_id = str(rows[0][0])
                except Exception:
                    conn.rollback()

        if found:
            found_wines.append(w)
            tier_counts[tier]["found"] += 1
            country_counts[cc]["found"] += 1

            # Calculate depth score for found wines
            if matched_wine_id:
                depth = _calc_depth(cur, matched_wine_id)
                depth_scores.append(depth)
                w["_depth"] = depth
                w["_wine_id"] = matched_wine_id
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

    return {
        "total": total,
        "found": found,
        "find_rate": found / total if total else 0,
        "avg_depth": avg_depth,
        "by_tier": dict(tier_counts),
        "by_country": dict(country_counts),
        "found_wines": found_wines,
        "missing": missing_wines,
    }


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


def main():
    parser = argparse.ArgumentParser(description="Josh Test")
    parser.add_argument("--sample", default=str(SAMPLE_PATH))
    parser.add_argument("--tier", help="Test single price tier only")
    parser.add_argument("--staging", action="store_true",
                        help="Check staging coverage instead of canonical")
    args = parser.parse_args()

    wines = load_sample(Path(args.sample))
    if args.tier:
        wines = [w for w in wines if w["price_tier"] == args.tier]

    if args.staging:
        results = test_staging_coverage(wines)
    else:
        results = test_findability(wines)

    print_report(results)


if __name__ == "__main__":
    main()
