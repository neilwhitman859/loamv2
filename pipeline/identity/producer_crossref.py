"""
Cross-reference a producer across staging tables.

Given a producer name, search all staging sources and return matches with
confidence scores. Used to validate producer identity before canonical insertion.

Usage:
    python -m pipeline.identity.producer_crossref --name "Ridge Vineyards"
"""
import re
from typing import Optional
from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, normalize_producer


# Staging source definitions: (table, producer_column, extra_columns)
STAGING_SOURCES = [
    ("source_lwin", "producer_name", ["wine_name", "country", "region", "sub_region", "colour", "wine_type", "classification", "lwin_7"]),
    ("source_ttb_colas", "brand_name", ["fanciful_name", "wine_appellation", "grape_varietals", "class_type_desc", "origin_desc", "wine_vintage", "abv", "ttb_id"]),
    ("source_skurnik", "producer", ["wine_name", "appellation", "grapes", "color", "country"]),
    ("source_empson", "producer", ["wine_name", "appellation", "grapes", "color", "country"]),
    ("source_kermit_lynch", "producer", ["wine_name", "appellation", "grape", "color", "country"]),
    ("source_winebow", "producer", ["wine_name", "appellation", "varietal", "color"]),
    ("source_european_cellars", "producer", ["wine_name", "appellation", "grape", "color"]),
    ("source_specs", "brand", ["title", "upc"]),
    ("source_flatiron", "producer", ["title", "varietal", "appellation"]),
    ("source_texsom", "producer", ["wine_name", "appellation", "vintage", "medal"]),
    ("source_berliner", "producer", ["wine_name", "country", "vintage", "medal"]),
    ("source_bc_liquor", "producer", ["wine_name", "appellation", "grape", "upc"]),
    ("source_systembolaget", "producer", ["wine_name", "country", "grape"]),
    ("source_enofile", "producer", ["wine_name", "appellation", "varietal"]),
]


def normalize_producer_name(name: str) -> str:
    """Normalize producer name for matching. Delegates to lib."""
    return normalize_producer(name)


def is_junk_producer(name: str) -> bool:
    """Check if a producer name matches junk criteria (IDENTITY_RULES.md Section 7)."""
    n = name.strip()
    if len(n) < 3:
        return True
    if n.isdigit():
        return True
    if n.upper() in {
        'LLC', 'INC', 'CORP', 'LTD', 'PTY', 'GMBH', 'SA', 'SL', 'SRL',
        'SPA', 'AG', 'NV', 'BV', 'CO', 'COMPANY',
    }:
        return True
    if n.upper() in {
        'N/A', 'NA', 'UNKNOWN', 'VARIOUS', 'PRIVATE LABEL', 'NONE',
        'TBD', 'TEST', 'SAMPLE', 'OTHER', 'MISC', 'GENERIC',
    }:
        return True
    if n == n.upper() and len(n) > 2 and not any(c in n.upper() for c in 'AEIOU'):
        return True
    if any(p in n.upper() for p in ['DBA ', 'DOING BUSINESS AS', 'FORMERLY KNOWN AS', 'AKA ', 'BRAND REGISTRATION']):
        return True
    if len(n) > 80:
        return True
    return False


def search_staging_for_producer(name: str, conn=None) -> dict:
    """
    Search all staging tables for a producer name.
    Returns dict keyed by source name with list of matching rows.
    Uses normalized exact matching to avoid substring false positives.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
    cur = conn.cursor()

    norm_name = normalize_producer(name)
    results = {}

    for table, col, extra_cols in STAGING_SOURCES:
        try:
            # Check table exists
            cur.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = 'public'", (table,))
            if not cur.fetchone():
                continue

            # Check column exists
            cur.execute(f"SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s", (table, col))
            if not cur.fetchone():
                continue

            # For LWIN and importers: exact producer name match (case-insensitive)
            # For TTB: match brand_name exactly (they're already the brand)
            cols_str = ", ".join([col] + [c for c in extra_cols if _column_exists(cur, table, c)])
            available_cols = [col] + [c for c in extra_cols if _column_exists(cur, table, c)]
            cols_str = ", ".join(available_cols)

            # Build all name variants to search (exact + alternatives)
            all_names = [name] + _build_alt_names(name)
            if table == "source_ttb_colas":
                all_names = _build_ttb_patterns(name)

            # Search with all variants, merge results
            seen_keys = set()
            rows = []
            for search_name in all_names:
                cur.execute(f"SELECT {cols_str} FROM {table} WHERE UPPER({col}) = UPPER(%s) LIMIT 500", (search_name,))
                for row in cur.fetchall():
                    key = str(row)  # Simple dedup
                    if key not in seen_keys:
                        seen_keys.add(key)
                        rows.append(row)

            if rows:
                results[table] = [dict(zip(available_cols, row)) for row in rows]
        except Exception as e:
            pass  # Skip inaccessible tables silently

    if own_conn:
        conn.close()
    return results


def _column_exists(cur, table: str, col: str) -> bool:
    """Check if a column exists in a table."""
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s", (table, col))
    return cur.fetchone() is not None


def _build_ttb_patterns(name: str) -> list[str]:
    """Build TTB brand_name search patterns from a producer name."""
    patterns = [name]
    # Add with/without common prefixes
    for prefix in ["Chateau ", "Château ", "Domaine ", "Bodegas ", "Weingut ",
                    "Tenuta ", "Bodega ", "Maison "]:
        if name.lower().startswith(prefix.lower()):
            patterns.append(name[len(prefix):])
        else:
            patterns.append(prefix + name)
    # Add with/without common suffixes
    for suffix in [" Winery", " Vineyards", " Vineyard", " Estate", " Cellars",
                   " Wines", " Wine Company"]:
        if name.lower().endswith(suffix.lower()):
            patterns.append(name[:-len(suffix)])
        else:
            patterns.append(name + suffix)
    return list(set(patterns))


def _build_alt_names(name: str) -> list[str]:
    """Build alternative name forms for searching."""
    alts = []
    # Strip prefix (LWIN uses short names like "Margaux" not "Château Margaux")
    for prefix in ["Château ", "Chateau ", "Domaine ", "Domaine de la ",
                    "Domaine du ", "Domaine de ", "Bodegas ", "Weingut ",
                    "Tenuta ", "Bodega ", "Maison ", "Cantina ", "Casa "]:
        if name.lower().startswith(prefix.lower()):
            alts.append(name[len(prefix):])
    # Strip suffix
    for suffix in [" Winery", " Vineyards", " Vineyard", " Estate", " Cellars",
                   " Wines"]:
        if name.lower().endswith(suffix.lower()):
            alts.append(name[:-len(suffix)])
    return alts


def resolve_producer_country(staging_results: dict, conn=None) -> Optional[dict]:
    """
    Resolve country/region for a producer from staging data.
    Returns {country_id, region_id, country_code, region_name} or None.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_conn()
    cur = conn.cursor()

    # Try LWIN first (best structured data)
    lwin_rows = staging_results.get("source_lwin", [])
    if lwin_rows:
        country_name = lwin_rows[0].get("country")
        region_name = lwin_rows[0].get("region")
        if country_name:
            result = _resolve_geo(cur, country_name, region_name)
            if own_conn:
                conn.close()
            return result

    # Try TTB origin_desc
    ttb_rows = staging_results.get("source_ttb_colas", [])
    if ttb_rows:
        origin = ttb_rows[0].get("origin_desc")
        if origin:
            result = _resolve_geo(cur, origin, None)
            if own_conn:
                conn.close()
            return result

    # Try importers
    for source in ["source_skurnik", "source_empson", "source_kermit_lynch",
                    "source_european_cellars", "source_berliner", "source_systembolaget"]:
        rows = staging_results.get(source, [])
        if rows:
            country = rows[0].get("country")
            if country:
                result = _resolve_geo(cur, country, None)
                if own_conn:
                    conn.close()
                return result

    if own_conn:
        conn.close()
    return None


def _resolve_geo(cur, country_text: str, region_text: Optional[str]) -> Optional[dict]:
    """Resolve country and region text to canonical IDs."""
    # Country lookup
    cur.execute("""
        SELECT id, iso_code FROM countries
        WHERE UPPER(name) = UPPER(%s) OR UPPER(iso_code) = UPPER(%s)
        LIMIT 1
    """, (country_text, country_text))
    row = cur.fetchone()
    if not row:
        # Try partial match for things like "FRANCE" in origin_desc
        cur.execute("SELECT id, iso_code FROM countries WHERE UPPER(name) LIKE UPPER(%s) LIMIT 1",
                    (f"%{country_text}%",))
        row = cur.fetchone()
    if not row:
        return None

    country_id, country_code = row

    # Region lookup
    region_id = None
    if region_text:
        cur.execute("""
            SELECT id FROM regions
            WHERE UPPER(name) = UPPER(%s) AND country_id = %s
            LIMIT 1
        """, (region_text, str(country_id)))
        rrow = cur.fetchone()
        if rrow:
            region_id = rrow[0]

    return {
        "country_id": str(country_id),
        "country_code": country_code,
        "region_id": str(region_id) if region_id else None,
        "region_name": region_text,
    }


def main():
    import sys
    name = sys.argv[1] if len(sys.argv) > 1 else "Ridge Vineyards"
    print(f"Searching staging for: {name}\n")

    results = search_staging_for_producer(name)
    for source, rows in results.items():
        print(f"  {source}: {len(rows)} rows")
        for row in rows[:3]:
            print(f"    {row}")

    geo = resolve_producer_country(results)
    print(f"\n  Resolved geography: {geo}")


if __name__ == "__main__":
    main()
