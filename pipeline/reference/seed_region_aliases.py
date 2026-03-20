"""
Seed region_aliases table with common alternative names.

Sources:
- WSET Level 3 naming conventions (English/local pairs)
- Wine trade standard names (Wine-Searcher, Decanter conventions)
- Import friction encountered during 13+ producer imports

Usage:
    python -m pipeline.reference.seed_region_aliases [--dry-run]
"""

import argparse
import sys
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize


# Format: (alias, canonical_region_name, alias_type, language_code)
ALIASES = [
    # Italian regions -- English <-> Italian
    ("Piedmont", "Piemonte", "translation", "en"),
    ("Tuscany", "Toscana", "translation", "en"),
    ("Lombardy", "Lombardia", "translation", "en"),
    ("Sicily", "Sicilia", "translation", "en"),
    ("Sardinia", "Sardegna", "translation", "en"),
    ("Apulia", "Puglia", "translation", "en"),
    ("Friuli", "Friuli-Venezia Giulia", "abbreviation", None),
    ("FVG", "Friuli-Venezia Giulia", "abbreviation", None),
    ("Friuli Venezia Giulia", "Friuli-Venezia Giulia", "alternate_name", None),
    ("Trentino", "Trentino-Alto Adige", "abbreviation", None),
    ("Alto Adige", "Trentino-Alto Adige", "abbreviation", None),
    ("Südtirol", "Trentino-Alto Adige", "translation", "de"),
    ("South Tyrol", "Trentino-Alto Adige", "translation", "en"),
    ("Trentino Alto Adige", "Trentino-Alto Adige", "alternate_name", None),

    # Italian L2 sub-regions
    ("Langhe", "Langhe", "alternate_name", None),
    ("Monferrato", "Monferrato", "alternate_name", None),
    ("Chianti region", "Chianti", "alternate_name", "en"),
    ("Etna region", "Etna", "alternate_name", "en"),

    # French regions -- English <-> French
    ("Bourgogne", "Burgundy", "translation", "fr"),
    ("Rhône", "Rhône Valley", "abbreviation", None),
    ("Rhone", "Rhône Valley", "abbreviation", None),
    ("Rhone Valley", "Rhône Valley", "alternate_name", None),
    ("Northern Rhone", "Northern Rhône", "alternate_name", None),
    ("Southern Rhone", "Southern Rhône", "alternate_name", None),
    ("Loire", "Loire Valley", "abbreviation", None),
    ("Val de Loire", "Loire Valley", "translation", "fr"),
    ("Languedoc-Roussillon", "Languedoc", "historical_name", None),
    ("South West France", "Southwest France", "alternate_name", "en"),
    ("The Dordogne and South West France", "Southwest France", "alternate_name", "en"),
    ("Southern France", "Southern France", "alternate_name", None),
    ("Midi", "Southern France", "alternate_name", "fr"),

    # Spanish regions
    ("Catalonia", "Catalunya", "translation", "en"),
    ("Castile and León", "Castilla y León", "translation", "en"),
    ("Castile and Leon", "Castilla y León", "translation", "en"),
    ("Castilla y Leon", "Castilla y León", "alternate_name", None),
    ("Andalusia", "Andalucía", "translation", "en"),
    ("Andalucia", "Andalucía", "alternate_name", None),
    ("Upper Ebro", "Upper Ebro", "alternate_name", None),

    # German regions
    ("Moselle", "Mosel", "translation", "en"),
    ("Palatinate", "Pfalz", "translation", "en"),
    ("Rhine", "Rheingau", "abbreviation", "en"),
    ("Franconia", "Franken", "translation", "en"),
    ("Baden", "Baden", "alternate_name", None),
    ("Württemberg", "Württemberg", "alternate_name", None),
    ("Wuerttemberg", "Württemberg", "alternate_name", None),

    # Portuguese regions
    ("Dão", "Dão", "alternate_name", None),
    ("Dao", "Dão", "alternate_name", None),
    ("Minho", "Vinho Verde", "alternate_name", "pt"),
    ("Porto", "Douro", "alternate_name", None),

    # Austrian regions
    ("Lower Austria", "Niederösterreich", "translation", "en"),
    ("Niederosterreich", "Niederösterreich", "alternate_name", None),
    ("Burgenland", "Burgenland", "alternate_name", None),
    ("Styria", "Steiermark", "translation", "en"),
    ("Steiermark", "Steiermark", "alternate_name", None),
    ("Vienna", "Wien", "translation", "en"),

    # New World
    ("Hawkes Bay", "Hawke's Bay", "alternate_name", None),
    ("Hawke's Bay", "Hawke's Bay", "alternate_name", None),
    ("Barossa", "Barossa", "alternate_name", None),
    ("McLaren Vale", "McLaren Vale", "alternate_name", None),
    ("Margaret River", "Margaret River", "alternate_name", None),
    ("Napa", "Napa Valley", "abbreviation", None),
    ("Sonoma", "Sonoma County", "abbreviation", None),
    ("Willamette", "Willamette Valley", "abbreviation", None),
    ("Maipo", "Maipo Valley", "abbreviation", None),
    ("Colchagua", "Colchagua Valley", "abbreviation", None),
    ("Casablanca", "Casablanca Valley", "abbreviation", None),

    # South Africa
    ("Stellenbosch", "Stellenbosch", "alternate_name", None),
    ("Constantia", "Constantia", "alternate_name", None),
    ("Swartland", "Swartland", "alternate_name", None),
]


def main():
    parser = argparse.ArgumentParser(description="Seed region_aliases table")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()

    sb = get_supabase()

    # Load regions
    all_regions = fetch_all("regions", "id,name,slug")

    region_by_name: dict[str, dict] = {}
    region_by_norm: dict[str, dict] = {}
    for r in all_regions:
        region_by_name[r["name"].lower()] = r
        region_by_norm[normalize(r["name"])] = r

    inserted = 0
    skipped = 0
    not_found = 0

    for alias_text, canonical, alias_type, lang in ALIASES:
        region = region_by_name.get(canonical.lower()) or region_by_norm.get(normalize(canonical))
        if not region:
            print(f"  Warning: Region not found: \"{canonical}\" (alias: \"{alias_text}\")")
            not_found += 1
            continue

        alias_norm = normalize(alias_text)
        # Skip if alias is same as canonical (normalized)
        if alias_norm == normalize(region["name"]):
            skipped += 1
            continue

        if args.dry_run:
            print(f"  [DRY RUN] \"{alias_text}\" -> {region['name']} ({alias_type})")
            inserted += 1
            continue

        try:
            sb.table("region_aliases").upsert(
                {
                    "region_id": region["id"],
                    "alias": alias_text,
                    "alias_normalized": alias_norm,
                    "alias_type": alias_type,
                    "language_code": lang,
                    "source": "wset-l3-conventions",
                },
                on_conflict="alias_normalized",
            ).execute()
            inserted += 1
        except Exception as e:
            print(f"  Warning: Error for \"{alias_text}\": {e}")

    print(f"\nDone. Inserted: {inserted}, Skipped (same as canonical): {skipped}, Not found: {not_found}")


if __name__ == "__main__":
    main()
