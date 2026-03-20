"""
Populate appellation_aliases from primary sources + mechanical generation.

Sources:
  1. INAO OpenDataSoft API -- French AOC/IGP product variants (color, cru, style)
  2. Tier 1 mechanical generation -- color suffixes, accent-stripped, designation types
  3. Tier 2 known translations -- English <-> local name pairs

Usage:
    python -m pipeline.reference.seed_appellation_aliases [--dry-run]
"""

import argparse
import sys
import time
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx

from pipeline.lib.db import get_supabase, fetch_all
from pipeline.lib.normalize import normalize


# ---- Color suffix maps per country ----
COLOR_SUFFIXES = {
    "FR": ["rouge", "blanc", "rosé", "clairet"],
    "IT": ["rosso", "bianco", "rosato"],
    "ES": ["tinto", "blanco", "rosado"],
    "PT": ["tinto", "branco", "rosado", "rosé"],
    "DE": ["rot", "weiß", "weiss", "rosé"],
    "AT": ["rot", "weiß", "weiss", "rosé"],
    "US": ["red", "white", "rosé", "rose"],
    "AU": ["red", "white", "rosé", "rose"],
    "NZ": ["red", "white", "rosé", "rose"],
    "ZA": ["red", "white", "rosé", "rose"],
    "CL": ["tinto", "blanco", "rosado"],
    "AR": ["tinto", "blanco", "rosado"],
}

# ---- Known English <-> local translations ----
# Format: (english_variant, local_name, iso_code)
TRANSLATIONS = [
    # French
    ("Burgundy", "Bourgogne", "FR"),
    ("Rhone Valley", "Vallée du Rhône", "FR"),
    ("Rhone", "Rhône", "FR"),
    ("Cotes du Rhone", "Côtes du Rhône", "FR"),
    ("Cotes du Rhone Villages", "Côtes du Rhône Villages", "FR"),
    ("Beaujolais Villages", "Beaujolais-Villages", "FR"),
    ("Cote de Beaune", "Côte de Beaune", "FR"),
    ("Cote de Nuits", "Côte de Nuits", "FR"),
    ("Cotes de Provence", "Côtes de Provence", "FR"),
    ("Cotes du Roussillon", "Côtes du Roussillon", "FR"),
    ("Coteaux du Layon", "Coteaux du Layon", "FR"),
    ("Saint-Emilion", "Saint-Émilion", "FR"),
    ("Pouilly-Fume", "Pouilly-Fumé", "FR"),
    ("Pouilly-Fuisse", "Pouilly-Fuissé", "FR"),
    ("Chateauneuf-du-Pape", "Châteauneuf-du-Pape", "FR"),
    ("Cote Rotie", "Côte Rôtie", "FR"),
    ("Cote-Rotie", "Côte Rôtie", "FR"),
    ("Gevrey-Chambertin", "Gevrey-Chambertin", "FR"),
    ("Cremant d'Alsace", "Crémant d'Alsace", "FR"),
    ("Cremant de Bourgogne", "Crémant de Bourgogne", "FR"),
    ("Cremant de Loire", "Crémant de Loire", "FR"),
    ("Cremant de Limoux", "Crémant de Limoux", "FR"),
    ("Corbieres", "Corbières", "FR"),
    ("Fitou", "Fitou", "FR"),
    ("Medoc", "Médoc", "FR"),
    ("Haut-Medoc", "Haut-Médoc", "FR"),
    ("Premieres Cotes de Bordeaux", "Premières Côtes de Bordeaux", "FR"),
    ("Entre-Deux-Mers", "Entre-deux-Mers", "FR"),
    ("Cotes de Bourg", "Côtes de Bourg", "FR"),
    ("Cotes de Blaye", "Côtes de Blaye", "FR"),
    # Italian
    ("Piedmont", "Piemonte", "IT"),
    ("Tuscany", "Toscana", "IT"),
    ("Brunello di Montalcino", "Brunello di Montalcino", "IT"),
    ("Vino Nobile di Montepulciano", "Vino Nobile di Montepulciano", "IT"),
    # Spanish
    ("Rioja", "Rioja", "ES"),
    ("Sherry", "Jerez-Xérès-Sherry", "ES"),
    ("Jerez", "Jerez-Xérès-Sherry", "ES"),
    ("Cava", "Cava", "ES"),
    ("Priorat", "Priorat", "ES"),
    ("Priorato", "Priorat", "ES"),
    # Portuguese
    ("Port", "Porto", "PT"),
    ("Oporto", "Porto", "PT"),
    ("Douro", "Douro", "PT"),
    ("Madeira", "Madeira", "PT"),
    ("Vinho Verde", "Vinho Verde", "PT"),
    ("Dao", "Dão", "PT"),
    # German
    ("Mosel", "Mosel", "DE"),
    ("Moselle", "Mosel", "DE"),
    ("Rhine", "Rheingau", "DE"),
    ("Pfalz", "Pfalz", "DE"),
    ("Palatinate", "Pfalz", "DE"),
    # Austrian
    ("Wachau", "Wachau", "AT"),
    # Hungarian
    ("Tokaj", "Tokaji", "HU"),
    ("Tokay", "Tokaji", "HU"),
    # Greek
    ("Santorini", "\u03a3\u03b1\u03bd\u03c4\u03bf\u03c1\u03af\u03bd\u03b7", "GR"),
    ("Naoussa", "\u039d\u03ac\u03bf\u03c5\u03c3\u03b1", "GR"),
    ("Nemea", "\u039d\u03b5\u03bc\u03ad\u03b1", "GR"),
]


def main():
    parser = argparse.ArgumentParser(description="Seed appellation_aliases from primary sources")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()

    print(f"\n{'=' * 60}")
    print("  APPELLATION ALIASES -- SEED FROM PRIMARY SOURCES")
    print(f"  {'(DRY RUN)' if args.dry_run else '(LIVE)'}")
    print(f"{'=' * 60}\n")

    sb = get_supabase()

    # Load all appellations with country info
    appellations = fetch_all("appellations", "id,name,designation_type,country_id")
    countries = fetch_all("countries", "id,name,iso_code")
    country_map = {c["id"]: c for c in countries}
    iso_to_country_id = {c["iso_code"]: c["id"] for c in countries}

    # Build appellation lookup by normalized name
    appellation_by_name: dict[str, dict] = {}
    appellation_by_norm: dict[str, dict] = {}
    for a in appellations:
        appellation_by_name[a["name"].lower()] = a
        appellation_by_norm[normalize(a["name"])] = a

    print(f"Loaded {len(appellations)} appellations from {len(countries)} countries\n")

    # Collect all aliases to insert
    aliases: list[dict] = []
    seen: set[str] = set()  # dedupe by alias_normalized

    def add_alias(appellation_id: str, alias: str, alias_type: str, source: str):
        norm = normalize(alias)
        # Skip if same as the canonical name
        appellation = next((a for a in appellations if a["id"] == appellation_id), None)
        if appellation and normalize(appellation["name"]) == norm:
            return
        # Skip duplicates
        if norm in seen:
            return
        seen.add(norm)
        aliases.append({
            "appellation_id": appellation_id,
            "alias": alias,
            "alias_normalized": norm,
            "alias_type": alias_type,
            "source": source,
        })

    # ====================================================================
    # SOURCE 1: INAO OpenDataSoft API -- French product variants
    # ====================================================================
    print("1. Fetching INAO product variants...")

    INAO_API = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/aires-et-produits-aocaop-et-igp/records"
    inao_products: list[dict] = []
    inao_offset = 0
    INAO_LIMIT = 100
    INAO_WHERE = "signe_fr LIKE 'AOC%' OR signe_fr LIKE 'IGP%'"

    with httpx.Client(timeout=30) as client:
        while True:
            params = {
                "select": "aire_geographique,produit,signe_fr,idproduit",
                "group_by": "aire_geographique,produit,signe_fr,idproduit",
                "where": INAO_WHERE,
                "limit": INAO_LIMIT,
                "offset": inao_offset,
            }
            resp = client.get(INAO_API, params=params)
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break
            inao_products.extend(results)
            inao_offset += INAO_LIMIT
            if len(results) < INAO_LIMIT:
                break
            time.sleep(0.2)

    print(f"  Fetched {len(inao_products)} INAO wine product entries")

    # Match INAO products to our appellations
    inao_matched = 0
    inao_unmatched = 0
    inao_missed_set: set[str] = set()

    for p in inao_products:
        aire_norm = normalize(p["aire_geographique"])
        produit = p["produit"]
        produit_norm = normalize(produit)

        # Skip if produit is same as aire_geographique (no variant)
        if aire_norm == produit_norm:
            continue

        # Find matching appellation by aire_geographique
        appellation = appellation_by_norm.get(aire_norm)

        # If no direct match, try progressively shorter prefixes
        if not appellation:
            words = p["aire_geographique"].split(" ")
            for length in range(len(words) - 1, 0, -1):
                prefix = " ".join(words[:length])
                appellation = appellation_by_norm.get(normalize(prefix))
                if appellation:
                    break

        if appellation:
            # Determine alias type based on what's different
            alias_type = "synonym"
            diff = produit.replace(p["aire_geographique"], "").strip().lower()
            if diff in ("rouge", "blanc", "rosé", "clairet"):
                alias_type = "with_color"
            elif any(kw in diff for kw in (
                "vendanges tardives", "sélection de grains nobles",
                "vin jaune", "vin de paille", "mousseux", "primeur",
                "supérieur", "grand cru", "premier cru",
            )):
                alias_type = "with_designation"

            add_alias(appellation["id"], produit, alias_type, "inao-opendatasoft")
            inao_matched += 1
        else:
            inao_unmatched += 1
            inao_missed_set.add(p["aire_geographique"])

    print(f"  Matched: {inao_matched}, Unmatched: {inao_unmatched} ({len(inao_missed_set)} unique aires)")
    if inao_missed_set:
        sample = list(inao_missed_set)[:10]
        print(f"  Sample unmatched: {', '.join(sample)}")

    # ====================================================================
    # SOURCE 2: Tier 1 -- Mechanical color suffix generation
    # ====================================================================
    print("\n2. Generating Tier 1 color suffix aliases...")

    color_count = 0
    for a in appellations:
        country = country_map.get(a["country_id"])
        if not country or not country.get("iso_code"):
            continue
        suffixes = COLOR_SUFFIXES.get(country["iso_code"])
        if not suffixes:
            continue

        for suffix in suffixes:
            add_alias(a["id"], f"{a['name']} {suffix}", "with_color", "mechanical-color-suffix")
            color_count += 1

    pre_color = len(aliases) - color_count
    print(f"  Generated {color_count} color suffix candidates ({pre_color} already existed from INAO)")

    # ====================================================================
    # SOURCE 3: Tier 1 -- Accent-stripped variants
    # ====================================================================
    print("\n3. Generating accent-stripped aliases...")

    import unicodedata
    accent_count = 0
    for a in appellations:
        nfkd = unicodedata.normalize("NFD", a["name"])
        stripped = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
        if stripped != a["name"]:
            add_alias(a["id"], stripped, "synonym", "mechanical-accent-strip")
            accent_count += 1

    print(f"  Generated {accent_count} accent-stripped aliases")

    # ====================================================================
    # SOURCE 4: Tier 1 -- Designation type suffixes
    # ====================================================================
    print("\n4. Generating designation type suffix aliases...")

    designation_count = 0
    for a in appellations:
        dt = a.get("designation_type")
        if not dt:
            continue
        # Skip if the name already contains the designation
        if dt.lower() in a["name"].lower():
            continue
        add_alias(a["id"], f"{a['name']} {dt}", "with_designation", "mechanical-designation-suffix")
        designation_count += 1

    print(f"  Generated {designation_count} designation suffix aliases")

    # ====================================================================
    # SOURCE 5: Tier 2 -- Known translations
    # ====================================================================
    print("\n5. Adding known translation aliases...")

    translation_count = 0
    for english, local, iso in TRANSLATIONS:
        # Find the canonical appellation -- could be either the english or local name
        appellation = (
            appellation_by_name.get(local.lower())
            or appellation_by_name.get(english.lower())
            or appellation_by_norm.get(normalize(local))
            or appellation_by_norm.get(normalize(english))
        )
        if appellation:
            # Add the variant that ISN'T the canonical name
            if normalize(english) != normalize(appellation["name"]):
                add_alias(appellation["id"], english, "synonym", "known-translation")
                translation_count += 1
            if normalize(local) != normalize(appellation["name"]):
                add_alias(appellation["id"], local, "local_name", "known-translation")
                translation_count += 1

    print(f"  Added {translation_count} translation aliases")

    # ====================================================================
    # INSERT INTO DATABASE
    # ====================================================================
    print(f"\n{'-' * 60}")
    print(f"Total aliases to insert: {len(aliases)}")

    if args.dry_run:
        print("\n(DRY RUN -- not inserting)\n")
        # Show sample
        print("Sample aliases:")
        samples = [a for a in aliases if a["source"] == "inao-opendatasoft"][:10]
        for s in samples:
            app = next((a for a in appellations if a["id"] == s["appellation_id"]), None)
            print(f'  "{s["alias"]}" -> {app["name"] if app else "?"} [{s["alias_type"]}] ({s["source"]})')
        print("...")
        color_samples = [a for a in aliases if a["source"] == "mechanical-color-suffix"][:5]
        for s in color_samples:
            app = next((a for a in appellations if a["id"] == s["appellation_id"]), None)
            print(f'  "{s["alias"]}" -> {app["name"] if app else "?"} [{s["alias_type"]}] ({s["source"]})')

        # Stats by source
        by_src: dict[str, int] = {}
        for a in aliases:
            by_src[a["source"]] = by_src.get(a["source"], 0) + 1
        print("\nBy source:")
        for src, count in sorted(by_src.items(), key=lambda x: -x[1]):
            print(f"  {src}: {count}")
        return

    # Batch insert
    BATCH = 500
    inserted = 0
    for i in range(0, len(aliases), BATCH):
        batch = aliases[i:i + BATCH]
        try:
            sb.table("appellation_aliases").upsert(
                batch, on_conflict="alias_normalized"
            ).execute()
            inserted += len(batch)
        except Exception as e:
            print(f"  Batch error at {i}: {e}")
            # Try one by one for this batch
            for a in batch:
                try:
                    sb.table("appellation_aliases").upsert(
                        a, on_conflict="alias_normalized"
                    ).execute()
                    inserted += 1
                except Exception:
                    pass

    print(f"\nInserted {inserted} aliases")

    # Final stats
    try:
        result = sb.table("appellation_aliases").select("source").execute()
        by_src: dict[str, int] = {}
        for row in result.data or []:
            by_src[row["source"]] = by_src.get(row["source"], 0) + 1
        print("\nFinal counts by source:")
        for src, count in sorted(by_src.items(), key=lambda x: -x[1]):
            print(f"  {src}: {count}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
