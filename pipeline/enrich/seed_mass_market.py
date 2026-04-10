"""
Seed canonical wines from TTB COLA records for mass-market wine brands.

For brands that have a producer entry but 0 wines, queries source_ttb_colas
by brand_name, groups records into unique wine identities (fanciful_name +
primary grape), and creates:
  - Canonical wines in the wines table
  - Wine vintages with ABV
  - COLA external_ids linking wines to TTB records
  - Wine_grapes linking to the grapes reference table

Usage:
    python -m pipeline.enrich.seed_mass_market --dry-run
    python -m pipeline.enrich.seed_mass_market --dry-run --brands "JOSH CELLARS,BAREFOOT"
    python -m pipeline.enrich.seed_mass_market --execute
    python -m pipeline.enrich.seed_mass_market --execute --all
"""

import argparse
import io
import re
import sys
import uuid
from collections import defaultdict

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify


# ── Brand Configuration ─────────────────────────────────────────────────────

US_COUNTRY_ID = "f30a9c62-b8df-41fe-b226-563dbcd56023"

MASS_MARKET_BRANDS = [
    ("JOSH CELLARS", "b923f95e-b643-4d73-8ef3-8a0a6e2bb3d5"),
    ("BOTA BOX", "5ce6765c-8cee-43f3-9434-11987c11060d"),
    ("BLACK BOX", "6d453207-27aa-4253-950a-f3bb48c082e5"),
    ("FRANZIA", "424e9bbf-25fa-4179-87c7-8abf423a4c85"),
    ("DECOY", "ea5d1e15-d0e0-41a5-85b7-19149aef375c"),
    ("MEIOMI", "b6d3fd75-4bde-46fb-9e07-a90487ca8045"),
    ("MARK WEST", "81a05f54-c5f1-4bf2-8a85-119cd169779c"),
    ("BREAD & BUTTER", "f4866b3e-0e65-4f5d-af17-2872219d135b"),
    ("LA MARCA", "882bb7cc-0961-4d4e-98e7-1b94d782da9b"),
    ("WOODBRIDGE", "6e5b0ee4-c939-4938-9f30-a9420bbdd586"),
    ("SUTTER HOME", "e34c4911-cb49-49b1-adbf-cf906a9b9d0f"),
    ("BAREFOOT", "b2b5573d-77d0-4713-9caa-0221d7e70d66"),
    ("YELLOW TAIL", "edfcd5a2-453b-46e1-8689-50ce6868df6c"),
    ("APOTHIC", "e64d4d8f-3ca7-40a2-8e3b-ecbf20af14e3"),
    ("CUPCAKE", "019f7bc8-b931-4357-b530-142fd344dcef"),
]

# These were previously needing producer creation, now have IDs
# (created in Session 8 Data Quality Gate)
MASS_MARKET_BRANDS_EXTRA = [
    ("STELLA ROSA", "3810fe16-99c1-4d03-be29-4229301f8aca"),
    ("14 HANDS", "69485a10-6f97-4da5-9144-2d9b5074a59f"),
    ("BONTERRA", "97a18ab6-e0e8-43a9-b593-39542addce56"),
]

# Legacy — kept for reference, no longer needed
BRANDS_NEEDING_PRODUCER = []


# ── Grape / Color Mapping ───────────────────────────────────────────────────

GRAPE_COLOR_MAP = {
    "cabernet sauvignon": "red",
    "merlot": "red",
    "pinot noir": "red",
    "zinfandel": "red",
    "syrah": "red",
    "shiraz": "red",
    "malbec": "red",
    "tempranillo": "red",
    "sangiovese": "red",
    "grenache": "red",
    "petite sirah": "red",
    "petit verdot": "red",
    "cabernet franc": "red",
    "red blend": "red",
    "red": "red",
    "chardonnay": "white",
    "sauvignon blanc": "white",
    "pinot grigio": "white",
    "pinot gris": "white",
    "riesling": "white",
    "moscato": "white",
    "muscat": "white",
    "viognier": "white",
    "gewurztraminer": "white",
    "chenin blanc": "white",
    "albarino": "white",
    "white blend": "white",
    "white zinfandel": "rosé",
    "rose": "rosé",
    "rosé": "rosé",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_first_grape(grape_field: str | None) -> str | None:
    """Extract the first (primary) grape name from TTB's comma-separated grape_varietals field."""
    if not grape_field:
        return None
    grape_field = grape_field.strip()
    if not grape_field:
        return None
    # TTB format: "Cabernet Sauvignon, Merlot" or "CABERNET SAUVIGNON,MERLOT"
    first = grape_field.split(",")[0].strip()
    # Strip percentage if present: "Cabernet Sauvignon (85%)"
    first = re.sub(r"\s*\([\d.]+%?\)\s*$", "", first).strip()
    if not first:
        return None
    return first


def grape_to_title(grape: str) -> str:
    """Convert TTB uppercase grape to title case, handling multi-word names."""
    if not grape:
        return ""
    return grape.strip().title()


def infer_color(grape_name: str | None) -> str | None:
    """Infer wine color from the primary grape name."""
    if not grape_name:
        return None
    key = grape_name.lower().strip()
    return GRAPE_COLOR_MAP.get(key)


def infer_wine_type(class_type_desc: str | None) -> str:
    """Infer wine_type from TTB class_type_desc. Default 'table'."""
    if not class_type_desc:
        return "table"
    desc_upper = class_type_desc.upper()
    if "SPARKLING" in desc_upper or "CHAMPAGNE" in desc_upper or "CARBONATED" in desc_upper:
        return "sparkling"
    if "DESSERT" in desc_upper:
        return "dessert"
    if "VERMOUTH" in desc_upper:
        return "fortified"
    return "table"


def make_display_name(
    producer_name: str,
    cuvee: str | None,
    grape_title: str | None,
    appellation: str | None,
) -> str:
    """Build a display_name following the existing pattern:
    "{Producer} {Cuvée} {Grape}, {Appellation}"
    """
    parts = [producer_name]
    if cuvee:
        parts.append(cuvee)
    if grape_title:
        parts.append(grape_title)
    name = " ".join(parts)
    if appellation:
        name = f"{name}, {appellation}"
    return name


def to_title_case(s: str | None) -> str | None:
    """Convert a TTB all-caps name to Title Case, preserving NV / NULL."""
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    # If already mixed case, leave it alone
    if s != s.upper():
        return s
    return s.title()


def parse_abv(abv_raw: str | None) -> float | None:
    """Parse ABV from TTB string, validate range 3-25%."""
    if not abv_raw:
        return None
    try:
        # Handle formats like "13.5%", "13.5", "13.5 %"
        cleaned = re.sub(r"[%\s]", "", str(abv_raw).strip())
        val = float(cleaned)
        if 3.0 <= val <= 25.0:
            return val
    except (ValueError, TypeError):
        pass
    return None


def parse_vintage_year(vintage_raw: str | None) -> int | None:
    """Parse a 4-digit vintage year from TTB vintage field."""
    if not vintage_raw:
        return None
    match = re.match(r"^(19|20)\d{2}$", str(vintage_raw).strip())
    if match:
        return int(match.group(0))
    return None


def load_grape_lookup(cur) -> dict:
    """Load grapes table into a normalized-name -> (id, name, color) lookup."""
    cur.execute("SELECT id, name, color FROM grapes WHERE deleted_at IS NULL")
    lookup = {}
    for gid, gname, gcolor in cur.fetchall():
        key = normalize(gname)
        if key:
            lookup[key] = (str(gid), gname, gcolor)
    # Also load synonyms
    cur.execute("""
        SELECT gs.grape_id, gs.synonym, g.color
        FROM grape_synonyms gs
        JOIN grapes g ON g.id = gs.grape_id
        WHERE g.deleted_at IS NULL
    """)
    for gid, syn, gcolor in cur.fetchall():
        key = normalize(syn)
        if key and key not in lookup:
            lookup[key] = (str(gid), syn, gcolor)
    return lookup


def match_grape(grape_name: str, grape_lookup: dict) -> tuple | None:
    """Match a grape name to the canonical grapes table.
    Returns (grape_id, canonical_name, color) or None."""
    if not grape_name:
        return None
    key = normalize(grape_name)
    if not key:
        return None
    return grape_lookup.get(key)


# ── Wine Identity ────────────────────────────────────────────────────────────

class WineIdentity:
    """A unique wine identity derived from TTB records."""

    def __init__(self, fanciful_name: str | None, primary_grape: str | None):
        self.fanciful_name = fanciful_name  # raw from TTB
        self.primary_grape = primary_grape  # raw first grape from TTB
        self.ttb_records: list[dict] = []
        self.vintages: set[int] = set()
        self.appellations: set[str] = set()
        self.class_type_descs: set[str] = set()

    @property
    def identity_key(self) -> str:
        fn = normalize(self.fanciful_name) if self.fanciful_name else ""
        gn = normalize(self.primary_grape) if self.primary_grape else ""
        return f"{fn}||{gn}"

    def add_record(self, rec: dict):
        self.ttb_records.append(rec)
        vintage = parse_vintage_year(rec.get("wine_vintage"))
        if vintage:
            self.vintages.add(vintage)
        appellation = rec.get("wine_appellation")
        if appellation and appellation.strip():
            self.appellations.add(appellation.strip())
        ctd = rec.get("class_type_desc")
        if ctd:
            self.class_type_descs.add(ctd.strip())

    @property
    def best_appellation(self) -> str | None:
        """Pick the most common appellation across TTB records, or None."""
        if not self.appellations:
            return None
        # Count occurrences and pick most frequent
        counts = defaultdict(int)
        for rec in self.ttb_records:
            app = rec.get("wine_appellation")
            if app and app.strip():
                counts[app.strip()] += 1
        if not counts:
            return None
        return max(counts, key=counts.get)


# ── Main Pipeline ────────────────────────────────────────────────────────────

def ensure_producer(cur, brand_name: str, dry_run: bool) -> str | None:
    """Create a producer for a brand that doesn't have one yet. Returns producer_id."""
    # Title-case the brand for the producer name
    display_name = brand_name.strip().title()
    name_norm = normalize(display_name)
    slug = slugify(display_name)

    # Check if it already exists
    cur.execute(
        "SELECT id FROM producers WHERE name_normalized = %s AND deleted_at IS NULL",
        (name_norm,),
    )
    existing = cur.fetchone()
    if existing:
        print(f"  Producer already exists for {brand_name}: {existing[0]}")
        return str(existing[0])

    if dry_run:
        fake_id = str(uuid.uuid4())
        print(f"  [DRY RUN] Would create producer: {display_name} → {fake_id}")
        return fake_id

    producer_id = str(uuid.uuid4())
    short_id = uuid.uuid4().hex[:8]
    slug_unique = f"{slug}-{short_id}"

    cur.execute(
        """INSERT INTO producers (id, name, name_normalized, slug, country_id, producer_type)
           VALUES (%s, %s, %s, %s, %s, 'corporate')
           RETURNING id""",
        (producer_id, display_name, name_norm, slug_unique, US_COUNTRY_ID),
    )
    result = cur.fetchone()
    if result:
        print(f"  Created producer: {display_name} → {result[0]}")
        return str(result[0])
    return None


def process_brand(
    conn,
    cur,
    brand_name: str,
    producer_id: str,
    producer_name: str,
    grape_lookup: dict,
    dry_run: bool,
) -> dict:
    """Process one brand: query TTB, group into wine identities, create wines."""

    stats = defaultdict(int)

    # ── 1. Query TTB by brand_name ────────────────────────────────────────
    cur.execute(
        """SELECT ttb_id, fanciful_name, wine_appellation, grape_varietals,
                  class_type_desc, wine_vintage, abv, label_image_urls
           FROM source_ttb_colas
           WHERE upper(brand_name) = upper(%s)""",
        (brand_name,),
    )
    ttb_rows = cur.fetchall()
    col_names = ["ttb_id", "fanciful_name", "wine_appellation", "grape_varietals",
                 "class_type_desc", "wine_vintage", "abv", "label_image_urls"]
    ttb_records = [dict(zip(col_names, row)) for row in ttb_rows]

    stats["ttb_records"] = len(ttb_records)
    if not ttb_records:
        print(f"  0 TTB records — skipping")
        return dict(stats)

    print(f"  {len(ttb_records)} TTB records found")

    # ── 2. Group into wine identities ─────────────────────────────────────
    identities: dict[str, WineIdentity] = {}
    skipped_no_identity = 0

    for rec in ttb_records:
        fanciful = rec["fanciful_name"]
        primary_grape = parse_first_grape(rec["grape_varietals"])

        # Skip if we have neither fanciful name nor grape — can't form identity
        if not fanciful and not primary_grape:
            skipped_no_identity += 1
            continue

        identity = WineIdentity(fanciful, primary_grape)
        key = identity.identity_key
        if key not in identities:
            identities[key] = identity
        identities[key].add_record(rec)

    stats["skipped_no_identity"] = skipped_no_identity
    stats["unique_wines"] = len(identities)
    print(f"  {len(identities)} unique wine identities ({skipped_no_identity} TTB records skipped — no name or grape)")

    # ── 3. Check for existing wines under this producer ───────────────────
    cur.execute(
        "SELECT id, name, name_normalized FROM wines WHERE producer_id = %s AND deleted_at IS NULL",
        (producer_id,),
    )
    existing_wines = {}
    for wid, wname, wnorm in cur.fetchall():
        if wnorm:
            existing_wines[wnorm] = str(wid)

    # ── 4. Create wines, vintages, external_ids, wine_grapes ──────────────
    for identity in identities.values():
        fanciful = identity.fanciful_name
        grape_raw = identity.primary_grape

        # Build wine name
        cuvee = to_title_case(fanciful)  # Title-cased cuvée (or None for varietal wines)
        grape_title = grape_to_title(grape_raw) if grape_raw else None

        # Determine canonical wine name — this is the wines.name field (the cuvée)
        # For varietal wines with no fanciful name, name is NULL
        wine_name = cuvee

        # Build the display_name
        appellation = identity.best_appellation
        appellation_display = to_title_case(appellation) if appellation else None
        display_name = make_display_name(producer_name, cuvee, grape_title, appellation_display)

        # Dedup check: normalize and compare
        name_for_dedup = normalize(display_name)
        if name_for_dedup in existing_wines:
            stats["wines_already_exist"] += 1
            # Still link TTB records to existing wine
            wine_id = existing_wines[name_for_dedup]
            _link_ttb_records(cur, wine_id, identity, dry_run, stats)
            _create_vintages(cur, wine_id, identity, dry_run, stats)
            continue

        # Also check by just the cuvée name if we have one
        if wine_name:
            cuvee_norm = normalize(wine_name)
            if cuvee_norm and cuvee_norm in existing_wines:
                stats["wines_already_exist"] += 1
                wine_id = existing_wines[cuvee_norm]
                _link_ttb_records(cur, wine_id, identity, dry_run, stats)
                _create_vintages(cur, wine_id, identity, dry_run, stats)
                continue

        # Infer color from grape or class_type_desc
        color = infer_color(grape_raw)
        if not color:
            # Try from class_type_desc
            for ctd in identity.class_type_descs:
                ctd_upper = ctd.upper()
                if "RED" in ctd_upper:
                    color = "red"
                    break
                elif "WHITE" in ctd_upper:
                    color = "white"
                    break
                elif "ROSE" in ctd_upper or "ROSÉ" in ctd_upper:
                    color = "rosé"
                    break

        # Map 'rosé' to DB enum value 'rose'
        db_color = "rose" if color == "rosé" else color

        # Wine type
        wine_type = "table"
        for ctd in identity.class_type_descs:
            inferred = infer_wine_type(ctd)
            if inferred != "table":
                wine_type = inferred
                break

        # Slug
        slug_base = slugify(display_name) if display_name else slugify(producer_name)
        short_id = uuid.uuid4().hex[:8]
        slug = f"{slug_base[:180]}-mm-{short_id}"

        # Name normalized (for dedup matching)
        name_norm = normalize(wine_name) if wine_name else None

        if dry_run:
            stats["wines_created"] += 1
            print(f"    [DRY RUN] Wine: {display_name} | color={color} | type={wine_type} | "
                  f"vintages={sorted(identity.vintages)[:5]}{'...' if len(identity.vintages) > 5 else ''} | "
                  f"TTB records={len(identity.ttb_records)}")
            # Track for dedup within this brand during dry run
            if name_for_dedup:
                existing_wines[name_for_dedup] = "dry-run-id"
            continue

        # INSERT wine
        wine_id = str(uuid.uuid4())
        try:
            cur.execute(
                """INSERT INTO wines (id, name, name_normalized, display_name, slug,
                                     producer_id, country_id, color, wine_type,
                                     identity_confidence, data_grade)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'cola_matched', 'F')
                   RETURNING id""",
                (wine_id, wine_name, name_norm, display_name, slug,
                 producer_id, US_COUNTRY_ID, db_color, wine_type),
            )
            result = cur.fetchone()
            if not result:
                stats["wine_errors"] += 1
                continue
            wine_id = str(result[0])
            stats["wines_created"] += 1

            # Track for dedup within this brand
            if name_for_dedup:
                existing_wines[name_for_dedup] = wine_id
            if wine_name:
                cuvee_norm = normalize(wine_name)
                if cuvee_norm:
                    existing_wines[cuvee_norm] = wine_id

        except Exception as e:
            stats["wine_errors"] += 1
            print(f"    ERROR creating wine '{display_name}': {e}")
            conn.rollback()
            continue

        # Link grape
        if grape_raw:
            grape_match = match_grape(grape_raw, grape_lookup)
            if grape_match:
                grape_id, _, _ = grape_match
                try:
                    cur.execute(
                        """INSERT INTO wine_grapes (wine_id, grape_id)
                           VALUES (%s, %s)
                           ON CONFLICT (wine_id, grape_id) DO NOTHING""",
                        (wine_id, grape_id),
                    )
                    if cur.rowcount > 0:
                        stats["grapes_linked"] += 1
                except Exception as e:
                    print(f"    ERROR linking grape for '{display_name}': {e}")
                    conn.rollback()

        # Create COLA external_ids + vintages
        _link_ttb_records(cur, wine_id, identity, dry_run, stats)
        _create_vintages(cur, wine_id, identity, dry_run, stats)

        # Also update source_ttb_colas.canonical_wine_id for these records
        ttb_ids = [rec["ttb_id"] for rec in identity.ttb_records if rec.get("ttb_id")]
        if ttb_ids:
            for i in range(0, len(ttb_ids), 500):
                batch = ttb_ids[i : i + 500]
                placeholders = ",".join(["%s"] * len(batch))
                try:
                    cur.execute(
                        f"""UPDATE source_ttb_colas
                            SET canonical_wine_id = %s
                            WHERE ttb_id IN ({placeholders})
                              AND canonical_wine_id IS NULL""",
                        [wine_id] + batch,
                    )
                    stats["ttb_rows_linked"] += cur.rowcount
                except Exception as e:
                    print(f"    ERROR linking TTB rows: {e}")
                    conn.rollback()

    return dict(stats)


def _link_ttb_records(cur, wine_id: str, identity, dry_run: bool, stats: dict):
    """Create COLA external_ids for TTB records."""
    for rec in identity.ttb_records:
        ttb_id = rec.get("ttb_id")
        if not ttb_id:
            continue
        if dry_run:
            stats["cola_ids_created"] += 1
            continue
        try:
            cur.execute(
                """INSERT INTO external_ids (entity_type, entity_id, system, external_id)
                   VALUES ('wine', %s, 'cola', %s)
                   ON CONFLICT (entity_type, entity_id, system, external_id) DO NOTHING""",
                (wine_id, str(ttb_id)),
            )
            if cur.rowcount > 0:
                stats["cola_ids_created"] += 1
        except Exception as e:
            stats["cola_errors"] += 1


def _create_vintages(cur, wine_id: str, identity, dry_run: bool, stats: dict):
    """Create wine_vintages for each vintage year found in TTB, with ABV and label image."""
    # Collect best ABV and label per vintage
    vintage_data: dict[int, dict] = {}
    for rec in identity.ttb_records:
        year = parse_vintage_year(rec.get("wine_vintage"))
        if not year:
            continue
        if year not in vintage_data:
            vintage_data[year] = {"abv": None, "label_url": None}
        abv = parse_abv(rec.get("abv"))
        if abv and not vintage_data[year]["abv"]:
            vintage_data[year]["abv"] = abv
        label_urls = rec.get("label_image_urls")
        if label_urls and not vintage_data[year]["label_url"]:
            if isinstance(label_urls, list) and label_urls:
                vintage_data[year]["label_url"] = label_urls[0]
            elif isinstance(label_urls, str) and label_urls.startswith("http"):
                vintage_data[year]["label_url"] = label_urls

    for year, data in vintage_data.items():
        if dry_run:
            stats["vintages_created"] += 1
            continue
        try:
            # Check if vintage already exists
            cur.execute(
                "SELECT id FROM wine_vintages WHERE wine_id = %s AND vintage_year = %s",
                (wine_id, year),
            )
            if cur.fetchone():
                continue  # already exists

            vintage_id = str(uuid.uuid4())
            cur.execute(
                """INSERT INTO wine_vintages (id, wine_id, vintage_year, abv, label_image_url)
                   VALUES (%s, %s, %s, %s, %s)""",
                (vintage_id, wine_id, year, data["abv"], data["label_url"]),
            )
            if cur.rowcount > 0:
                stats["vintages_created"] += 1
        except Exception as e:
            stats["vintage_errors"] += 1


def run(dry_run: bool = True, brands: list[str] | None = None, use_all: bool = False):
    """Main entry point."""
    # Fix Windows stdout encoding
    if sys.stdout.encoding != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    conn = get_conn()
    cur = conn.cursor()

    # Load grape lookup once
    print("Loading grape reference table...")
    grape_lookup = load_grape_lookup(cur)
    print(f"  {len(grape_lookup)} grape names loaded")

    # Build brand list
    all_brands = list(MASS_MARKET_BRANDS) + list(MASS_MARKET_BRANDS_EXTRA)
    all_brand_names = {b[0] for b in all_brands}

    # Filter to requested brands
    if brands:
        requested = {b.strip().upper() for b in brands}
        selected = [(bn, pid) for bn, pid in all_brands if bn.upper() in requested]
        missing = requested - {bn.upper() for bn, _ in selected}
        if missing:
            print(f"WARNING: Unknown brands: {', '.join(sorted(missing))}")
        all_brands = selected
    elif not use_all:
        # Default: process all configured brands
        pass  # all_brands already has everything

    if not all_brands:
        print("No brands to process.")
        return

    mode = "DRY RUN" if dry_run else "EXECUTE"
    print(f"\n{'='*70}")
    print(f"  MASS-MARKET WINE SEEDER — {mode}")
    print(f"  {len(all_brands)} brands to process")
    print(f"{'='*70}\n")

    totals = defaultdict(int)
    totals["brands_total"] = len(all_brands)

    for i, (brand_name, producer_id) in enumerate(all_brands):
        print(f"\n[{i+1}/{len(all_brands)}] {brand_name}")
        print(f"  {'—'*50}")

        # Resolve or create producer
        if producer_id:
            # Verify producer exists
            cur.execute("SELECT name FROM producers WHERE id = %s AND deleted_at IS NULL", (producer_id,))
            row = cur.fetchone()
            if not row:
                print(f"  ERROR: Producer {producer_id} not found — skipping {brand_name}")
                totals["brands_skipped"] += 1
                continue
            producer_name = row[0]
        else:
            # Need to create producer
            producer_id = ensure_producer(cur, brand_name, dry_run)
            if not producer_id:
                print(f"  ERROR: Could not create producer for {brand_name} — skipping")
                totals["brands_skipped"] += 1
                continue
            producer_name = brand_name.strip().title()
            totals["producers_created"] += 1
            if not dry_run:
                conn.commit()

        # Check if producer already has wines
        cur.execute(
            "SELECT count(*) FROM wines WHERE producer_id = %s AND deleted_at IS NULL",
            (producer_id,),
        )
        existing_count = cur.fetchone()[0]
        if existing_count > 0:
            print(f"  Producer '{producer_name}' already has {existing_count} wines — processing anyway (will dedup)")

        # Process the brand
        brand_stats = process_brand(conn, cur, brand_name, producer_id, producer_name, grape_lookup, dry_run)

        # Commit per-producer
        if not dry_run:
            conn.commit()

        # Accumulate
        for k, v in brand_stats.items():
            totals[k] += v
        if brand_stats.get("wines_created", 0) > 0:
            totals["brands_with_wines"] += 1

        print(f"  Summary: {brand_stats.get('wines_created', 0)} wines, "
              f"{brand_stats.get('vintages_created', 0)} vintages, "
              f"{brand_stats.get('cola_ids_created', 0)} COLA IDs, "
              f"{brand_stats.get('grapes_linked', 0)} grapes linked")

    # ── Final Report ──────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  FINAL RESULTS — {mode}")
    print(f"{'='*70}")
    print(f"  Brands processed:      {totals['brands_total']}")
    print(f"  Brands with new wines: {totals['brands_with_wines']}")
    print(f"  Brands skipped:        {totals['brands_skipped']}")
    print(f"  Producers created:     {totals['producers_created']}")
    print(f"  TTB records scanned:   {totals['ttb_records']}")
    print(f"  Skipped (no identity): {totals['skipped_no_identity']}")
    print(f"  Unique wine identities:{totals['unique_wines']}")
    print(f"  Wines already existed: {totals['wines_already_exist']}")
    print(f"  Wines created:         {totals['wines_created']}")
    print(f"  Wine errors:           {totals['wine_errors']}")
    print(f"  Vintages created:      {totals['vintages_created']}")
    print(f"  COLA IDs created:      {totals['cola_ids_created']}")
    print(f"  Grapes linked:         {totals['grapes_linked']}")
    print(f"  TTB rows linked:       {totals['ttb_rows_linked']}")
    print(f"{'='*70}\n")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed canonical wines from TTB COLA records for mass-market brands"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Preview only, no DB writes")
    mode.add_argument("--execute", action="store_true", help="Write to database")
    parser.add_argument(
        "--brands",
        type=str,
        default=None,
        help="Comma-separated brand names to process (e.g., 'JOSH CELLARS,BAREFOOT')",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="use_all",
        help="Process all configured brands including those needing producer creation",
    )
    args = parser.parse_args()

    brand_list = None
    if args.brands:
        brand_list = [b.strip() for b in args.brands.split(",") if b.strip()]

    run(dry_run=args.dry_run, brands=brand_list, use_all=args.use_all)
