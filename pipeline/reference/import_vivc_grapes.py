"""
Rebuild the grapes table from VIVC (Vitis International Variety Catalogue).

Scrapes passport pages for all wine grapes, including synonyms and planting
area data.

Data source: VIVC (vivc.de) -- JKI Federal Research Centre for Cultivated Plants

Phases:
  Phase 1: Crawl passport pages (IDs 1-25000), filter wine grapes, extract all
           fields + synonyms in a single pass, cache to JSON
  Phase 2: Fetch area/planting sub-pages + EU catalog for each wine grape
  Phase 3: Import into Supabase (grapes, grape_synonyms, grape_plantings)
  Phase 4: Resolve parentage (second pass after all grapes inserted)
  Phase 5: Reconnect varietal_categories.grape_id from saved mappings

Usage:
    python -m pipeline.reference.import_vivc_grapes                    # full run (resume-safe)
    python -m pipeline.reference.import_vivc_grapes --phase 1          # crawl only
    python -m pipeline.reference.import_vivc_grapes --phase 2          # enrich only
    python -m pipeline.reference.import_vivc_grapes --phase 3          # import only
    python -m pipeline.reference.import_vivc_grapes --phase 4          # resolve parentage
    python -m pipeline.reference.import_vivc_grapes --phase 5          # reconnect varietal categories
    python -m pipeline.reference.import_vivc_grapes --start 5000       # resume crawl from ID 5000
    python -m pipeline.reference.import_vivc_grapes --dry-run          # preview without DB writes
"""

import argparse
import json
import re
import sys
import time
import urllib.parse
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import slugify

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VIVC_BASE = "https://www.vivc.de/index.php"
MAX_ID = 25000
CRAWL_DELAY_MS = 0.3       # polite delay between requests (seconds)
ENRICH_DELAY_MS = 0.5      # delay for sub-page fetches
CACHE_FILE = PROJECT_ROOT / "data" / "vivc_grapes_cache.json"
SAVE_INTERVAL = 50          # save cache every N grapes found
BATCH_SIZE = 100

# Wine grape utilization values to include (lowercased)
WINE_UTILIZATIONS = {
    "wine grape",
    "wine and table grape",
    "table and wine grape",
    "wine/table grape",
    "table/wine grape",
}

# ---------------------------------------------------------------------------
# Display name logic -- three-tier strategy
# ---------------------------------------------------------------------------

# Tier 1: Explicit overrides -- VIVC prime name -> industry-standard display name
DISPLAY_NAME_OVERRIDES = {
    "MERLOT NOIR": "Merlot",
    "CHARDONNAY BLANC": "Chardonnay",
    "RIESLING WEISS": "Riesling",
    "TEMPRANILLO TINTO": "Tempranillo",
    "GAMAY NOIR": "Gamay",
    "BARBERA NERA": "Barbera",
    "GARNACHA TINTA": "Grenache",
    "COT": "Malbec",
    "CALABRESE": "Nero d'Avola",
    "MONASTRELL": "Mourvèdre",
    "VELTLINER GRUEN": "Grüner Veltliner",
    "VERDOT PETIT": "Petit Verdot",
    "ZWEIGELTREBE BLAU": "Zweigelt",
    "NEGRO AMARO": "Negramaro",
    "UVA DI TROIA": "Nero di Troia",
    "GOUVEIO": "Godello",
    "XYNOMAVRO": "Xinomavro",
    "BLAUFRAENKISCH": "Blaufränkisch",
    "GEWUERZTRAMINER": "Gewürztraminer",
    "SILVANER GRUEN": "Silvaner",
    "VERDEJO BLANCO": "Verdejo",
    "GARNACHA BLANCA": "Grenache Blanc",
    "MUELLER THURGAU WEISS": "Müller-Thurgau",
    "HARSLEVELUE": "Hárslevelű",
    "MUSCAT A PETITS GRAINS BLANCS": "Muscat Blanc à Petits Grains",
    "ALVARINHO": "Albariño",
}

# Tier 2: Grape families where suffix MUST be kept
KEEP_SUFFIX_FAMILIES = {
    "PINOT", "SAUVIGNON", "CABERNET", "MUSCAT", "CHENIN", "MOSCATO",
    "MALVASIA", "TREBBIANO", "TOCAI", "ARAMON", "GRIGNOLINO",
}

# Color suffixes that can be stripped for Tier 3
COLOR_SUFFIXES_RE = re.compile(
    r"\s+(NOIR|BLANC|BLANCHE|BLANCO|BLANCA|BIANCO|BIANCA|WEISS|ROUGE|ROSE|"
    r"ROSSO|ROSSA|TINTO|TINTA|NERO|NERA|GRIS|GRIGIO|GRIGIA|GRUEN|BLAU|ROT)$",
    re.IGNORECASE,
)

# VIVC ISO3 -> our ISO2 country code mapping
ISO3_TO_ISO2 = {
    "FRA": "FR", "ITA": "IT", "ESP": "ES", "PRT": "PT", "DEU": "DE",
    "AUT": "AT", "GRC": "GR", "HUN": "HU", "HRV": "HR", "SVN": "SI",
    "GEO": "GE", "USA": "US", "AUS": "AU", "NZL": "NZ", "ZAF": "ZA",
    "ARG": "AR", "CHL": "CL", "BRA": "BR", "URY": "UY", "CHE": "CH",
    "BGR": "BG", "ROU": "RO", "SRB": "RS", "MKD": "MK", "MDA": "MD",
    "CAN": "CA", "JPN": "JP", "CHN": "CN", "ISR": "IL", "TUR": "TR",
    "MAR": "MA", "CZE": "CZ", "SVK": "SK", "RUS": "RU", "UKR": "UA",
    "IND": "IN", "MEX": "MX", "LBN": "LB", "CYP": "CY", "TUN": "TN",
    "DZA": "DZ", "ARM": "AM", "AZE": "AZ", "GBR": "GB", "PER": "PE",
    "POL": "PL", "BEL": "BE", "LUX": "LU", "NLD": "NL", "DNK": "DK",
    "SWE": "SE", "MNE": "ME", "ALB": "AL", "MLT": "MT", "BOL": "BO",
    "COL": "CO", "JOR": "JO", "LIE": "LI", "SMR": "SM", "SYR": "SY",
    "THA": "TH", "MMR": "MM", "BLR": "BY",
}

# VIVC country name -> our country name mapping
COUNTRY_NAME_MAP = {
    "UNITED STATES OF AMERICA": "UNITED STATES", "USA": "UNITED STATES",
    "RUSSIAN FEDERATION": "RUSSIA", "MACEDONIA": "NORTH MACEDONIA",
    "ENGLAND": "UNITED KINGDOM", "GREAT BRITAIN": "UNITED KINGDOM",
    "REPUBLIC OF KOREA": "SOUTH KOREA", "KOREA": "SOUTH KOREA",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return {"lastScannedId": 0, "grapes": {}, "stats": {"scanned": 0, "wineGrapes": 0, "skipped": 0, "errors": 0}}


def save_cache(cache: dict):
    CACHE_FILE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


def input_value(html: str, field_id: str) -> str | None:
    """Extract value from a hidden input field by ID."""
    m = re.search(rf'id="{field_id}"[^>]*value="([^"]*)"', html, re.IGNORECASE)
    return m.group(1).strip() if m else None


def kv_attribute(html: str, label: str) -> str | None:
    """Extract text from kv-attribute div after a th label."""
    escaped = re.escape(label)
    m = re.search(rf">{escaped}</th>[\s\S]*?kv-attribute\">\s*(?:<a[^>]*>)?([^<]*)", html, re.IGNORECASE)
    return m.group(1).strip() if m and m.group(1).strip() else None


def title_case(s: str) -> str:
    particles = {"de", "di", "du", "da", "do", "des", "del", "della", "delle", "\u00e0"}
    parts = re.split(r"(\s+|-)", s.lower())
    result = []
    for part in parts:
        if re.match(r"^\s+$", part) or part == "-":
            result.append(part)
        elif part in particles:
            result.append(part)
        else:
            result.append(part[0].upper() + part[1:] if part else part)
    return "".join(result)


def derive_display_name(vivc_name: str, all_names: list[str]) -> str:
    """Three-tier display name derivation."""
    # Tier 1: Explicit override
    if vivc_name in DISPLAY_NAME_OVERRIDES:
        return DISPLAY_NAME_OVERRIDES[vivc_name]

    # Tier 2: Multi-variant family
    first_word = vivc_name.split()[0] if vivc_name.split() else ""
    if first_word in KEEP_SUFFIX_FAMILIES:
        return title_case(vivc_name)

    # Tier 3: Strip color suffix if single-variant
    suffix_match = COLOR_SUFFIXES_RE.search(vivc_name)
    if suffix_match:
        base_name = COLOR_SUFFIXES_RE.sub("", vivc_name).strip()
        siblings = [n for n in all_names if n != vivc_name and n.startswith(base_name + " ")]
        if siblings:
            return title_case(vivc_name)
        return title_case(base_name)

    return title_case(vivc_name)


def clean_species(raw: str | None) -> str | None:
    if not raw:
        return None
    upper = raw.upper().strip()
    if "VINIFERA" in upper and "X " not in upper and " X" not in upper:
        return "vinifera"
    if "LABRUSCA" in upper and "X " not in upper and " X" not in upper:
        return "labrusca"
    if "RIPARIA" in upper and "X " not in upper and " X" not in upper:
        return "riparia"
    if "RUPESTRIS" in upper and "X " not in upper and " X" not in upper:
        return "rupestris"
    if "INTERSPECIFIC CROSSING" in upper or " X " in upper:
        return "hybrid"
    if "COMPLEX CROSSING" in upper or "COMPLEX HYBRID" in upper:
        return "complex_hybrid"
    if len(upper) < 5 or "VITIS" not in upper:
        return None
    return "vinifera"


def map_color(berry_skin: str | None) -> str | None:
    if not berry_skin:
        return None
    c = berry_skin.lower()
    if c in ("black", "dark"):
        return "red"
    if c in ("white", "green-yellow", "green"):
        return "white"
    if c in ("grey", "gray"):
        return "white"
    if c in ("rose", "pink", "red"):
        return "red"
    return None


def map_grape_type(utilization: str | None) -> str:
    if not utilization:
        return "wine"
    if "table" in utilization:
        return "dual"
    return "wine"


# ---------------------------------------------------------------------------
# Phase 1: Crawl passport pages
# ---------------------------------------------------------------------------
def crawl_passports(cache: dict, start_id: int):
    print(f"\n=== PHASE 1: Crawling VIVC passport pages ===")
    actual_start = max(start_id, (cache.get("lastScannedId") or 0) + 1)
    print(f"Starting from ID {actual_start}, scanning to {MAX_ID}")
    print(f"Current cache: {len(cache['grapes'])} wine grapes found\n")

    new_found = 0
    consecutive_not_found = 0

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for id_ in range(actual_start, MAX_ID + 1):
            try:
                url = f"{VIVC_BASE}?r=passport%2Fview&id={id_}"
                resp = client.get(url)

                if resp.status_code != 200:
                    cache["stats"]["errors"] += 1
                    if id_ % 500 == 0:
                        print(f"  [{id_}] HTTP {resp.status_code}")
                    cache["lastScannedId"] = id_
                    time.sleep(CRAWL_DELAY_MS)
                    continue

                html = resp.text

                # Check for non-existent ID
                if "return to the initial search" in html or "Please return to" in html:
                    consecutive_not_found += 1
                    cache["stats"]["skipped"] += 1
                    cache["lastScannedId"] = id_
                    if consecutive_not_found >= 500:
                        print(f"\n  500 consecutive not-found IDs -- stopping at {id_}")
                        break
                    time.sleep(0.05)
                    continue

                consecutive_not_found = 0
                cache["stats"]["scanned"] += 1

                # Extract prime name
                prime_name = input_value(html, "passport-leitname")
                if not prime_name:
                    cache["lastScannedId"] = id_
                    time.sleep(CRAWL_DELAY_MS)
                    continue

                # Extract utilization
                util_match = re.search(r"utilization22\]=([^&\"]+)", html, re.IGNORECASE)
                utilization = urllib.parse.unquote(util_match.group(1)).strip().lower() if util_match else None

                # Filter: only wine grapes
                if not utilization or utilization not in WINE_UTILIZATIONS:
                    cache["stats"]["skipped"] += 1
                    cache["lastScannedId"] = id_
                    if id_ % 1000 == 0:
                        print(f"  [{id_}] scanned={cache['stats']['scanned']} wine={len(cache['grapes'])} skip={cache['stats']['skipped']}")
                    time.sleep(0.05)
                    continue

                # ===== This is a wine grape -- extract everything =====
                grape = {
                    "vivc_number": str(id_),
                    "name": prime_name,
                    "utilization": utilization,
                }

                # Berry skin color
                grape["berry_skin_color"] = input_value(html, "passport-b_farbe") or None

                # Country of origin
                origin_code = input_value(html, "passport-landescode")
                origin_display = kv_attribute(html, "Country or region of origin of the variety")
                grape["origin_country_code"] = origin_code or None
                grape["origin_country"] = origin_display or None

                # Species
                species_match = re.search(r">Species</th>[\s\S]*?kv-attribute\">\s*<a[^>]*>([^<]*)", html, re.IGNORECASE)
                grape["species"] = species_match.group(1).strip() if species_match else None

                # Pedigree
                grape["pedigree_text"] = kv_attribute(html, "Pedigree as given by breeder/bibliography") or None
                grape["pedigree_confirmed_text"] = kv_attribute(html, "Pedigree confirmed by markers") or None

                # Parent IDs
                p1_id = input_value(html, "passport-kenn_nr_e1")
                p2_id = input_value(html, "passport-kenn_nr_e2")
                grape["parent1_vivc_id"] = int(p1_id) if p1_id else None
                grape["parent2_vivc_id"] = int(p2_id) if p2_id else None

                # Parent names
                if grape["parent1_vivc_id"]:
                    p1_re = re.compile(rf"passport%2Fview&amp;id={grape['parent1_vivc_id']}\">([^<]+)", re.IGNORECASE)
                    p1m = p1_re.search(html)
                    grape["parent1_name"] = p1m.group(1).strip() if p1m else None
                else:
                    grape["parent1_name"] = None
                if grape["parent2_vivc_id"]:
                    p2_re = re.compile(rf"passport%2Fview&amp;id={grape['parent2_vivc_id']}\">([^<]+)", re.IGNORECASE)
                    p2m = p2_re.search(html)
                    grape["parent2_name"] = p2m.group(1).strip() if p2m else None
                else:
                    grape["parent2_name"] = None

                # Full pedigree confirmed
                full_ped = kv_attribute(html, "Full pedigree")
                grape["parentage_confirmed"] = full_ped == "YES"

                # Breeder
                grape["breeder"] = kv_attribute(html, "Breeder") or None
                grape["breeding_institute"] = kv_attribute(html, "Breeder institute code") or None

                # Year of crossing
                year_str = kv_attribute(html, "Year of crossing")
                grape["crossing_year"] = int(year_str) if year_str and re.match(r"^\d{4}$", year_str) else None

                # Synonyms
                syn_regex = re.compile(r"%5Bsname%5D=([^&\"]+)", re.IGNORECASE)
                synonyms = set()
                for syn_match in syn_regex.finditer(html):
                    syn = urllib.parse.unquote(syn_match.group(1)).strip()
                    if syn and syn != prime_name:
                        synonyms.add(syn)
                grape["synonyms"] = list(synonyms)

                # Synonym count from header
                syn_count_match = re.search(r"Synonyms:\s*(\d+)", html, re.IGNORECASE)
                grape["synonym_count"] = int(syn_count_match.group(1)) if syn_count_match else 0

                # Area data available?
                grape["has_area_data"] = "arealisting" in html or "Area tabular listing" in html

                # EU catalog available?
                grape["has_eu_catalog"] = "europ-catalogue" in html or "European Catalogue" in html

                cache["grapes"][str(id_)] = grape
                cache["stats"]["wineGrapes"] += 1
                new_found += 1

                color_str = grape["berry_skin_color"] or "?"
                origin_str = grape["origin_country"] or "?"
                print(f"  [{id_}] + {prime_name} ({color_str}) -- {origin_str} -- {len(grape['synonyms'])} syn")

                cache["lastScannedId"] = id_

                if new_found % SAVE_INTERVAL == 0:
                    save_cache(cache)
                    print(f"    >> Cache saved ({len(cache['grapes'])} wine grapes)")

                time.sleep(CRAWL_DELAY_MS)

            except Exception as err:
                print(f"  [{id_}] ERROR: {err}")
                cache["stats"]["errors"] += 1
                cache["lastScannedId"] = id_
                time.sleep(1)

    save_cache(cache)
    print(f"\nPhase 1 complete:")
    print(f"  Scanned: {cache['stats']['scanned']}")
    print(f"  Wine grapes found: {len(cache['grapes'])}")
    print(f"  Skipped (non-wine/not found): {cache['stats']['skipped']}")
    print(f"  Errors: {cache['stats']['errors']}")


# ---------------------------------------------------------------------------
# Phase 2: Enrich with area data + EU catalog
# ---------------------------------------------------------------------------
def enrich_grapes(cache: dict):
    print(f"\n=== PHASE 2: Enriching wine grapes with area data + EU catalog ===")

    grape_ids = [
        id_ for id_, g in cache["grapes"].items()
        if not g.get("areas_fetched") or not g.get("eu_catalog_fetched")
    ]
    print(f"{len(grape_ids)} grapes need enrichment\n")

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for i, id_ in enumerate(grape_ids):
            grape = cache["grapes"][id_]

            # Fetch area/planting data
            if not grape.get("areas_fetched") and grape.get("has_area_data"):
                try:
                    name_enc = urllib.parse.quote(grape["name"])
                    url = f"{VIVC_BASE}?r=flaechen%2Farealisting&FlaechenSearch%5Bleitname2%5D={name_enc}&FlaechenSearch%5Bkenn_nr2%5D={id_}"
                    resp = client.get(url)
                    html = resp.text

                    areas = []
                    row_regex = re.compile(
                        r"<td[^>]*>([^<]+)</td>\s*<td[^>]*>([\d,.]+)\s*</td>\s*<td[^>]*>(\d{4})\s*</td>",
                        re.IGNORECASE,
                    )
                    for m in row_regex.finditer(html):
                        country = m.group(1).strip()
                        area = float(m.group(2).replace(",", ""))
                        year = int(m.group(3))
                        if country and not (area != area) and year:  # NaN check
                            areas.append({"country": country, "area_ha": area, "year": year})

                    # Keep only most recent entry per country
                    latest_by_country: dict[str, dict] = {}
                    for a in areas:
                        if a["country"] not in latest_by_country or a["year"] > latest_by_country[a["country"]]["year"]:
                            latest_by_country[a["country"]] = a
                    grape["areas"] = list(latest_by_country.values())
                    grape["areas_fetched"] = True

                    time.sleep(ENRICH_DELAY_MS)
                except Exception as err:
                    print(f"  [{id_}] Area fetch error: {err}")
            elif not grape.get("areas_fetched"):
                grape["areas"] = []
                grape["areas_fetched"] = True

            # Fetch EU catalog countries
            if not grape.get("eu_catalog_fetched") and grape.get("has_eu_catalog"):
                try:
                    url = f"{VIVC_BASE}?r=www-europ-catalogue%2Fpassportresult&WwwEuropCatalogueSearch%5Bvivc_var_id%5D={id_}"
                    resp = client.get(url)
                    html = resp.text

                    eu_countries: set[str] = set()
                    country_regex = re.compile(r"<td[^>]*>([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*</td>")
                    for m in country_regex.finditer(html):
                        c = m.group(1).strip()
                        if len(c) > 2 and c not in ("YES", "NO", "Showing"):
                            eu_countries.add(c)
                    grape["eu_catalog_countries"] = list(eu_countries)
                    grape["eu_catalog_fetched"] = True

                    time.sleep(ENRICH_DELAY_MS)
                except Exception as err:
                    print(f"  [{id_}] EU catalog fetch error: {err}")
            elif not grape.get("eu_catalog_fetched"):
                grape["eu_catalog_countries"] = []
                grape["eu_catalog_fetched"] = True

            if (i + 1) % 25 == 0 or i + 1 == len(grape_ids):
                save_cache(cache)
                area_count = len(grape.get("areas") or [])
                eu_count = len(grape.get("eu_catalog_countries") or [])
                print(f"  Enriched {i + 1}/{len(grape_ids)}: {grape['name']} ({area_count} area entries, {eu_count} EU countries)")

    save_cache(cache)
    print(f"\nPhase 2 complete: {len(grape_ids)} grapes enriched")


# ---------------------------------------------------------------------------
# Phase 3: Import into Supabase
# ---------------------------------------------------------------------------
def import_to_supabase(cache: dict, dry_run: bool):
    print(f"\n=== PHASE 3: Importing to Supabase ===")
    if dry_run:
        print("  [DRY RUN -- no DB writes]\n")

    sb = get_supabase()
    grapes = list(cache["grapes"].values())
    all_vivc_names = [g["name"] for g in grapes]
    print(f"{len(grapes)} wine grapes to import\n")

    # Load countries for origin matching
    all_countries = fetch_all("countries", "id,name,iso_code")
    country_by_name: dict[str, str] = {}
    country_by_iso2: dict[str, str] = {}
    for c in all_countries:
        country_by_name[c["name"].upper()] = c["id"]
        if c.get("iso_code"):
            country_by_iso2[c["iso_code"].upper()] = c["id"]

    def resolve_country_id(vivc_country: str | None, vivc_iso3: str | None) -> str | None:
        if vivc_iso3:
            iso2 = ISO3_TO_ISO2.get(vivc_iso3.upper())
            if iso2 and iso2 in country_by_iso2:
                return country_by_iso2[iso2]
        if not vivc_country:
            return None
        upper = vivc_country.upper().strip()
        if upper in country_by_name:
            return country_by_name[upper]
        mapped = COUNTRY_NAME_MAP.get(upper)
        if mapped and mapped in country_by_name:
            return country_by_name[mapped]
        return None

    # Pre-compute display names
    print("  Computing display names...")
    display_names: dict[str, str] = {}
    tier1 = tier2 = tier3 = 0
    for g in grapes:
        dn = derive_display_name(g["name"], all_vivc_names)
        display_names[g["vivc_number"]] = dn
        if g["name"] in DISPLAY_NAME_OVERRIDES:
            tier1 += 1
        elif g["name"].split()[0] in KEEP_SUFFIX_FAMILIES if g["name"].split() else False:
            tier2 += 1
        else:
            tier3 += 1

    # Fix collisions
    dn_counts: dict[str, list[str]] = {}
    for vivc, dn in display_names.items():
        dn_counts.setdefault(dn, []).append(vivc)
    collisions_fixed = 0
    for dn, ids in dn_counts.items():
        if len(ids) <= 1:
            continue
        for vivc in ids:
            grape = cache["grapes"].get(vivc)
            if grape and display_names[vivc] != title_case(grape["name"]):
                display_names[vivc] = title_case(grape["name"])
                collisions_fixed += 1

    print(f"  Display names: {tier1} Tier 1 overrides, {tier2} Tier 2 family-kept, {tier3} Tier 3 auto")
    if collisions_fixed > 0:
        print(f"  Fixed {collisions_fixed} display name collisions (reverted to full name)")

    # Remaining collisions
    dn_counts2: dict[str, list[str]] = {}
    for vivc, dn in display_names.items():
        dn_counts2.setdefault(dn, []).append(vivc)
    remaining = [(dn, ids) for dn, ids in dn_counts2.items() if len(ids) > 1]
    if remaining:
        print(f"  {len(remaining)} remaining collisions (true VIVC duplicates)")

    # Detect duplicate prime names for unique slugs
    name_counts: dict[str, int] = {}
    for g in grapes:
        name_counts[g["name"]] = name_counts.get(g["name"], 0) + 1
    dupe_names = {n for n, c in name_counts.items() if c > 1}
    if dupe_names:
        print(f"  {len(dupe_names)} duplicate prime names -- slugs will include VIVC number")

    # Insert grapes in batches
    inserted = 0
    errors = 0
    unmapped_countries: dict[str, int] = {}
    vivc_to_uuid: dict[str, str] = {}

    for i in range(0, len(grapes), BATCH_SIZE):
        batch = grapes[i:i + BATCH_SIZE]
        rows = []
        for g in batch:
            country_id = resolve_country_id(g.get("origin_country"), g.get("origin_country_code"))
            if not country_id and (g.get("origin_country") or g.get("origin_country_code")):
                key = g.get("origin_country") or g.get("origin_country_code")
                unmapped_countries[key] = unmapped_countries.get(key, 0) + 1
            slug = (
                slugify(g["name"]) + "-vivc-" + g["vivc_number"]
                if g["name"] in dupe_names
                else slugify(g["name"])
            )
            rows.append({
                "slug": slug,
                "name": g["name"],
                "display_name": display_names[g["vivc_number"]],
                "color": map_color(g.get("berry_skin_color")),
                "berry_skin_color": g.get("berry_skin_color") or None,
                "origin_country_id": country_id,
                "origin_region": g.get("origin_country") or None,
                "vivc_number": g["vivc_number"],
                "species": clean_species(g.get("species")),
                "grape_type": map_grape_type(g.get("utilization")),
                "crossing_year": g.get("crossing_year") or None,
                "breeder": g.get("breeder") or None,
                "breeding_institute": g.get("breeding_institute") or None,
                "origin_type": "cross" if (g.get("pedigree_text") or g.get("pedigree_confirmed_text")) else None,
                "eu_catalog_countries": g.get("eu_catalog_countries") if g.get("eu_catalog_countries") else None,
                "parentage_confirmed": g.get("parentage_confirmed", False),
            })

        if not dry_run:
            try:
                result = sb.table("grapes").insert(rows).execute()
                if result.data:
                    for d in result.data:
                        vivc_to_uuid[d["vivc_number"]] = d["id"]
                    inserted += len(result.data)
            except Exception as e:
                print(f"  Batch {i // BATCH_SIZE + 1} error: {e}")
                # Try one by one
                for row in rows:
                    try:
                        result = sb.table("grapes").insert(row).execute()
                        if result.data and result.data[0]:
                            vivc_to_uuid[result.data[0]["vivc_number"]] = result.data[0]["id"]
                            inserted += 1
                    except Exception as row_err:
                        print(f"    x {row['name']}: {row_err}")
                        errors += 1
        else:
            inserted += len(rows)

        if (i + BATCH_SIZE) % 500 == 0 or i + BATCH_SIZE >= len(grapes):
            print(f"  Inserted {inserted}/{len(grapes)} grapes ({errors} errors)")

    print(f"\nGrapes inserted: {inserted} ({errors} errors)")

    if unmapped_countries:
        print(f"\n  Unmapped origin countries:")
        for c, n in sorted(unmapped_countries.items(), key=lambda x: -x[1]):
            print(f"    {c}: {n} grapes")

    if dry_run:
        print("  [DRY RUN -- skipping synonyms and areas]")
        return

    # Load UUID mapping if needed
    if not vivc_to_uuid:
        print("  Loading grape UUID mapping from DB...")
        all_db_grapes = fetch_all("grapes", "id,vivc_number")
        for g in all_db_grapes:
            if g.get("vivc_number"):
                vivc_to_uuid[g["vivc_number"]] = g["id"]
        print(f"  Loaded {len(vivc_to_uuid)} grape UUIDs")

    # Insert synonyms
    print("\n  Inserting synonyms...")
    syn_inserted = 0
    syn_errors = 0
    syn_batch = []

    for g in grapes:
        grape_id = vivc_to_uuid.get(g["vivc_number"])
        if not grape_id or not g.get("synonyms") or len(g["synonyms"]) == 0:
            continue
        for syn in g["synonyms"]:
            syn_batch.append({
                "grape_id": grape_id,
                "synonym": syn,
                "source": "vivc",
                "synonym_type": "synonym",
            })

    for i in range(0, len(syn_batch), BATCH_SIZE):
        batch = syn_batch[i:i + BATCH_SIZE]
        try:
            sb.table("grape_synonyms").insert(batch).execute()
            syn_inserted += len(batch)
        except Exception:
            for row in batch:
                try:
                    sb.table("grape_synonyms").insert(row).execute()
                    syn_inserted += 1
                except Exception:
                    syn_errors += 1

        if (i + BATCH_SIZE) % 2000 == 0 or i + BATCH_SIZE >= len(syn_batch):
            print(f"    Synonyms: {syn_inserted}/{len(syn_batch)} ({syn_errors} dupes/errors)")

    print(f"  Synonyms inserted: {syn_inserted} ({syn_errors} dupes/errors)")

    # Insert planting area data
    print("\n  Inserting planting area data...")
    area_inserted = 0
    area_batch = []

    for g in grapes:
        grape_id = vivc_to_uuid.get(g["vivc_number"])
        if not grape_id or not g.get("areas") or len(g["areas"]) == 0:
            continue
        for a in g["areas"]:
            country_id = resolve_country_id(a["country"], None)
            if not country_id:
                continue
            area_batch.append({
                "grape_id": grape_id,
                "country_id": country_id,
                "area_ha": a["area_ha"],
                "survey_year": a["year"],
                "source": "VIVC",
            })

    for i in range(0, len(area_batch), BATCH_SIZE):
        batch = area_batch[i:i + BATCH_SIZE]
        try:
            sb.table("grape_plantings").insert(batch).execute()
            area_inserted += len(batch)
        except Exception as e:
            print(f"    Area batch error: {e}")

    print(f"  Area entries inserted: {area_inserted}")


# ---------------------------------------------------------------------------
# Phase 4: Resolve parentage
# ---------------------------------------------------------------------------
def resolve_parentage(cache: dict, dry_run: bool):
    print(f"\n=== PHASE 4: Resolving parentage ===")
    if dry_run:
        print("  [DRY RUN]")
        return

    sb = get_supabase()
    all_grapes = fetch_all("grapes", "id,vivc_number,name")

    by_vivc: dict[str, str] = {}
    for g in all_grapes:
        if g.get("vivc_number"):
            by_vivc[g["vivc_number"]] = g["id"]

    resolved = 0
    unresolved = 0

    for id_, grape in cache["grapes"].items():
        grape_id = by_vivc.get(grape["vivc_number"])
        if not grape_id:
            continue

        updates = {}
        if grape.get("parent1_vivc_id") and str(grape["parent1_vivc_id"]) in by_vivc:
            updates["parent1_grape_id"] = by_vivc[str(grape["parent1_vivc_id"])]
        if grape.get("parent2_vivc_id") and str(grape["parent2_vivc_id"]) in by_vivc:
            updates["parent2_grape_id"] = by_vivc[str(grape["parent2_vivc_id"])]

        if updates:
            try:
                sb.table("grapes").update(updates).eq("id", grape_id).execute()
                resolved += 1
            except Exception as e:
                print(f"  x {grape['name']}: {e}")
        elif grape.get("parent1_vivc_id") or grape.get("parent2_vivc_id"):
            unresolved += 1

    print(f"\nParentage resolved: {resolved}")
    print(f"Unresolved (parent not a wine grape): {unresolved}")


# ---------------------------------------------------------------------------
# Phase 5: Reconnect varietal categories
# ---------------------------------------------------------------------------
def reconnect_varietal_categories(dry_run: bool):
    print(f"\n=== PHASE 5: Reconnecting varietal categories ===")
    if dry_run:
        print("  [DRY RUN]")
        return

    sb = get_supabase()
    mapping_file = PROJECT_ROOT / "data" / "varietal_category_grape_mappings.json"
    if not mapping_file.exists():
        print(f"  x Mapping file not found: {mapping_file}")
        return

    mappings = json.loads(mapping_file.read_text(encoding="utf-8"))

    # Load all grapes by name
    all_grapes = fetch_all("grapes", "id,name")
    grape_by_name: dict[str, str] = {}
    for g in all_grapes:
        grape_by_name[g["name"].upper()] = g["id"]

    result = sb.table("varietal_categories").select("id,name").execute()
    categories = result.data

    matched = 0
    unmatched = 0

    for mapping in mappings:
        category = next((c for c in categories if c["name"] == mapping["category"]), None)
        if not category:
            print(f"  x Category not found: {mapping['category']}")
            unmatched += 1
            continue

        # Try VIVC UPPERCASE name match
        grape_id = grape_by_name.get(mapping["grape"].upper())

        # Try exact case match
        if not grape_id:
            g = next((g for g in all_grapes if g["name"] == mapping["grape"]), None)
            if g:
                grape_id = g["id"]

        # Try synonym lookup
        if not grape_id:
            try:
                syn_result = sb.table("grape_synonyms").select("grape_id").ilike("synonym", mapping["grape"]).limit(1).execute()
                if syn_result.data and len(syn_result.data) > 0:
                    grape_id = syn_result.data[0]["grape_id"]
            except Exception:
                pass

        if grape_id:
            try:
                sb.table("varietal_categories").update({"grape_id": grape_id}).eq("id", category["id"]).execute()
                matched += 1
            except Exception as e:
                print(f"  x {mapping['category']}: {e}")
                unmatched += 1
        else:
            print(f"  x No grape match for: {mapping['grape']} (category: {mapping['category']})")
            unmatched += 1

    print(f"\nVarietal categories reconnected: {matched}/{len(mappings)}")
    if unmatched > 0:
        print(f"Unmatched: {unmatched}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="VIVC Grape Rebuild")
    parser.add_argument("--phase", type=int, choices=[1, 2, 3, 4, 5],
                        help="Run only a specific phase")
    parser.add_argument("--start", type=int, default=1,
                        help="Resume crawl from this VIVC ID (Phase 1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without DB writes")
    args = parser.parse_args()

    print("+" + "=" * 46 + "+")
    print("|  VIVC Grape Rebuild                         |")
    print("|  Source: vivc.de (JKI)                      |")
    print("+" + "=" * 46 + "+")
    if args.dry_run:
        print("\n*** DRY RUN MODE ***\n")

    cache = load_cache()

    def should_run(phase: int) -> bool:
        return args.phase is None or args.phase == phase

    if should_run(1):
        crawl_passports(cache, args.start)
    if should_run(2):
        enrich_grapes(cache)
    if should_run(3):
        import_to_supabase(cache, args.dry_run)
    if should_run(4):
        resolve_parentage(cache, args.dry_run)
    if should_run(5):
        reconnect_varietal_categories(args.dry_run)

    print("\n+ Done")


if __name__ == "__main__":
    main()
