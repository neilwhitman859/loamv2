"""
TTB Appellation String → Region Resolver

Many TTB wine_appellation values are regions or countries, not appellations:
  CALIFORNIA, MENDOZA, VIRGINIA, TEXAS, SPAIN, ITALY, etc.

This script:
1. Builds a mapping of TTB appellation strings → region_id / country_id
2. Updates canonical wines linked to TTB records with the correct region
3. Also handles the "AMERICAN" → United States country mapping

Usage:
    python -m pipeline.promote.ttb_region_resolver [--dry-run]
"""

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize


def build_region_map(sb):
    """Build mapping of TTB appellation strings to region IDs."""
    print("Building region map...")

    # Load all regions
    regions = {}
    offset = 0
    while True:
        result = sb.table("regions").select("id,name,country_id,is_catch_all").range(offset, offset + 999).execute()
        for r in result.data:
            regions[r["name"].lower()] = r
        if len(result.data) < 1000:
            break
        offset += 1000

    # Load all appellation names (to exclude strings that ARE appellations)
    app_names = set()
    offset = 0
    while True:
        result = sb.table("appellations").select("name").range(offset, offset + 999).execute()
        for a in result.data:
            app_names.add(a["name"].lower())
        if len(result.data) < 1000:
            break
        offset += 1000

    # Load appellation aliases
    alias_names = set()
    offset = 0
    while True:
        result = sb.table("appellation_aliases").select("alias_normalized").range(offset, offset + 999).execute()
        for a in result.data:
            alias_names.add(a["alias_normalized"])
        if len(result.data) < 1000:
            break
        offset += 1000

    # Load countries
    countries = {}
    offset = 0
    while True:
        result = sb.table("countries").select("id,name").range(offset, offset + 999).execute()
        for c in result.data:
            countries[c["name"].lower()] = c["id"]
        if len(result.data) < 1000:
            break
        offset += 1000

    # Get distinct TTB appellation strings with counts
    # Use frequency threshold to focus on important ones
    print("Loading TTB appellation strings...")
    ttb_apps = defaultdict(int)
    offset = 0
    while True:
        result = (sb.table("source_ttb_colas")
                  .select("wine_appellation")
                  .not_.is_("wine_appellation", "null")
                  .range(offset, offset + 999)
                  .execute())
        for r in result.data:
            ttb_apps[r["wine_appellation"]] += 1
        if len(result.data) < 1000:
            break
        offset += 1000
        if offset % 100000 == 0:
            print(f"  ... loaded {offset} rows")

    print(f"  {len(ttb_apps)} distinct appellation strings")

    # Build the mapping
    region_map = {}  # ttb_string -> {region_id, country_id, type}
    us_country_id = countries.get("united states")

    for ttb_str, cnt in sorted(ttb_apps.items(), key=lambda x: -x[1]):
        lower = ttb_str.lower()

        # Skip if it's already a known appellation or alias
        if lower in app_names or lower in alias_names:
            continue

        # Check if it matches a region name
        region = regions.get(lower)
        if region:
            # Handle GEORGIA ambiguity — TTB context means US state
            if lower == "georgia" and region["country_id"] != us_country_id:
                # Look for the US Georgia region
                us_georgia = next((r for r in regions.values()
                                   if r["name"].lower() == "georgia"
                                   and r["country_id"] == us_country_id), None)
                if us_georgia:
                    region = us_georgia
                else:
                    continue

            region_map[ttb_str] = {
                "region_id": region["id"],
                "country_id": region["country_id"],
                "type": "region",
                "name": region["name"],
                "count": cnt,
            }
            continue

        # Check special mappings
        if lower == "american" and us_country_id:
            region_map[ttb_str] = {
                "region_id": None,
                "country_id": us_country_id,
                "type": "country",
                "name": "United States",
                "count": cnt,
            }
        elif lower in countries:
            region_map[ttb_str] = {
                "region_id": None,
                "country_id": countries[lower],
                "type": "country",
                "name": lower.title(),
                "count": cnt,
            }

    # Sort by count for reporting
    sorted_map = dict(sorted(region_map.items(), key=lambda x: -x[1]["count"]))

    print(f"\nRegion/country mappings found: {len(sorted_map)}")
    total_records = sum(v["count"] for v in sorted_map.values())
    print(f"Total TTB records covered: {total_records:,}")
    print("\nTop 20 mappings:")
    for ttb_str, info in list(sorted_map.items())[:20]:
        print(f"  {ttb_str:30s} → {info['name']:25s} ({info['count']:,} records)")

    return sorted_map


def update_canonical_wines(sb, region_map, dry_run=False):
    """Update canonical wines that are linked to TTB records with region-resolving strings."""
    print("\n" + "=" * 60)
    print("UPDATING CANONICAL WINES")
    print("=" * 60)

    # Get the TTB strings that map to regions
    region_strings = {k: v for k, v in region_map.items() if v["type"] == "region"}
    country_strings = {k: v for k, v in region_map.items() if v["type"] == "country"}

    total_updated = 0
    total_checked = 0

    # Process region-type mappings
    for ttb_str, info in region_strings.items():
        region_id = info["region_id"]
        country_id = info["country_id"]

        # Find canonical wines linked to TTB records with this appellation string
        # that currently have NULL region_id or wrong region
        # Use a chunked approach to avoid timeouts
        wine_ids = set()
        offset = 0
        while True:
            result = (sb.table("source_ttb_colas")
                      .select("canonical_wine_id")
                      .eq("wine_appellation", ttb_str)
                      .not_.is_("canonical_wine_id", "null")
                      .range(offset, offset + 999)
                      .execute())
            for r in result.data:
                wine_ids.add(r["canonical_wine_id"])
            if len(result.data) < 1000:
                break
            offset += 1000

        if not wine_ids:
            continue

        # Filter to wines that actually need updating (NULL region)
        wines_to_update = []
        wine_id_list = list(wine_ids)
        for i in range(0, len(wine_id_list), 200):
            batch = wine_id_list[i:i + 200]
            result = (sb.table("wines")
                      .select("id,region_id,country_id")
                      .in_("id", batch)
                      .is_("deleted_at", "null")
                      .execute())
            for w in result.data:
                needs_update = False
                update_data = {}
                if w.get("region_id") is None:
                    update_data["region_id"] = region_id
                    needs_update = True
                if w.get("country_id") is None:
                    update_data["country_id"] = country_id
                    needs_update = True
                if needs_update:
                    wines_to_update.append((w["id"], update_data))

        total_checked += len(wine_ids)

        if wines_to_update and not dry_run:
            for wine_id, update_data in wines_to_update:
                try:
                    sb.table("wines").update(update_data).eq("id", wine_id).execute()
                except Exception as e:
                    print(f"  Error updating {wine_id}: {e}")
            total_updated += len(wines_to_update)
            print(f"  {ttb_str:30s} → {info['name']:20s}: {len(wines_to_update):5d} wines updated")
        elif wines_to_update:
            total_updated += len(wines_to_update)
            print(f"  {ttb_str:30s} → {info['name']:20s}: {len(wines_to_update):5d} wines (dry-run)")

    # Process country-type mappings (AMERICAN, etc.)
    for ttb_str, info in country_strings.items():
        country_id = info["country_id"]

        wine_ids = set()
        offset = 0
        while True:
            result = (sb.table("source_ttb_colas")
                      .select("canonical_wine_id")
                      .eq("wine_appellation", ttb_str)
                      .not_.is_("canonical_wine_id", "null")
                      .range(offset, offset + 999)
                      .execute())
            for r in result.data:
                wine_ids.add(r["canonical_wine_id"])
            if len(result.data) < 1000:
                break
            offset += 1000

        if not wine_ids:
            continue

        wines_to_update = []
        wine_id_list = list(wine_ids)
        for i in range(0, len(wine_id_list), 200):
            batch = wine_id_list[i:i + 200]
            result = (sb.table("wines")
                      .select("id,country_id")
                      .in_("id", batch)
                      .is_("deleted_at", "null")
                      .execute())
            for w in result.data:
                if w.get("country_id") is None:
                    wines_to_update.append((w["id"], {"country_id": country_id}))

        total_checked += len(wine_ids)

        if wines_to_update and not dry_run:
            for wine_id, update_data in wines_to_update:
                try:
                    sb.table("wines").update(update_data).eq("id", wine_id).execute()
                except Exception as e:
                    print(f"  Error updating {wine_id}: {e}")
            total_updated += len(wines_to_update)
            print(f"  {ttb_str:30s} → {info['name']:20s}: {len(wines_to_update):5d} wines updated")
        elif wines_to_update:
            total_updated += len(wines_to_update)
            print(f"  {ttb_str:30s} → {info['name']:20s}: {len(wines_to_update):5d} wines (dry-run)")

    print(f"\nTotal wines checked: {total_checked:,}")
    print(f"Total wines updated with region: {total_updated:,}")
    if dry_run:
        print("** DRY RUN — no writes performed **")

    return total_updated


def add_unresolved_appellation_aliases(sb, region_map, dry_run=False):
    """
    For TTB appellation strings that DON'T resolve to regions but could be
    appellations with different capitalization/accents, add them as aliases.
    """
    print("\n" + "=" * 60)
    print("CHECKING UNRESOLVED TTB STRINGS FOR NEAR-MATCH APPELLATIONS")
    print("=" * 60)

    # Common TTB → proper name mappings for appellations with accents/special chars
    MANUAL_ALIASES = {
        "COTES DU RHONE": "Côtes du Rhône",
        "COTE ROTIE": "Côte-Rôtie",
        "COTES DU RHONE VILLAGES": "Côtes du Rhône Villages",
        "SAINT EMILION": "Saint-Émilion",
        "SAINT EMILION GRAND CRU": "Saint-Émilion Grand Cru",
        "SAINT ESTEPHE": "Saint-Estèphe",
        "SAINT JULIEN": "Saint-Julien",
        "CHATEAUNEUF DU PAPE": "Châteauneuf-du-Pape",
        "COTE DE BEAUNE": "Côte de Beaune",
        "COTE DE NUITS": "Côte de Nuits",
        "CREMANT D'ALSACE": "Crémant d'Alsace",
        "CREMANT DE BOURGOGNE": "Crémant de Bourgogne",
        "CREMANT DE LOIRE": "Crémant de Loire",
        "PESSAC LEOGNAN": "Pessac-Léognan",
        "HAUT MEDOC": "Haut-Médoc",
        "VIN DE FRANCE": "Vin de France",
        "PAYS D'OC": "Pays d'Oc",
        "COTES DE PROVENCE": "Côtes de Provence",
        "COTES DE GASCOGNE": "Côtes de Gascogne",
        "COTEAUX D'AIX EN PROVENCE": "Coteaux d'Aix-en-Provence",
        "COTES DU ROUSSILLON": "Côtes du Roussillon",
        "COTES DU VENTOUX": "Ventoux",
        "COTES DU LUBERON": "Luberon",
        "COTES CATALANES": "Côtes Catalanes",
        "GEVREY CHAMBERTIN": "Gevrey-Chambertin",
        "NUITS SAINT GEORGES": "Nuits-Saint-Georges",
        "VOSNE ROMANEE": "Vosne-Romanée",
        "PULIGNY MONTRACHET": "Puligny-Montrachet",
        "CHASSAGNE MONTRACHET": "Chassagne-Montrachet",
        "POUILLY FUISSE": "Pouilly-Fuissé",
        "POUILLY FUME": "Pouilly-Fumé",
        "COTES DU JURA": "Côtes du Jura",
        "CONDRIEU": "Condrieu",
        "COTE DE BROUILLY": "Côte de Brouilly",
        "BANDOL": "Bandol",
        "VOUVRAY": "Vouvray",
        "MUSCADET SEVRE ET MAINE": "Muscadet Sèvre et Maine",
        "VACQUEYRAS": "Vacqueyras",
        "GIGONDAS": "Gigondas",
        "CROZES HERMITAGE": "Crozes-Hermitage",
        "HERMITAGE": "Hermitage",
        "CORNAS": "Cornas",
        "MINERVOIS": "Minervois",
        "FAUGERES": "Faugères",
        "FITOU": "Fitou",
        "COSTIERES DE NIMES": "Costières de Nîmes",
        "ENTRE DEUX MERS": "Entre-deux-Mers",
        "COTES DE BOURG": "Côtes de Bourg",
        "COTES DE CASTILLON": "Castillon Côtes de Bordeaux",
        "PREMIERES COTES DE BORDEAUX": "Premières Côtes de Bordeaux",
        "FRONSAC": "Fronsac",
        "SAVENNIERES": "Savennières",
        "CHINON": "Chinon",
        "BOURGUEIL": "Bourgueil",
        "SAINT NICOLAS DE BOURGUEIL": "Saint-Nicolas-de-Bourgueil",
        # Italy
        "BRUNELLO DI MONTALCINO": "Brunello di Montalcino",
        "CHIANTI CLASSICO": "Chianti Classico",
        "AMARONE DELLA VALPOLICELLA": "Amarone della Valpolicella",
        "VALPOLICELLA RIPASSO": "Valpolicella Ripasso",
        "BAROLO": "Barolo",
        "BARBARESCO": "Barbaresco",
        "MONTEPULCIANO D'ABRUZZO": "Montepulciano d'Abruzzo",
        # Spain
        "PRIORAT": "Priorat",
        "RIAS BAIXAS": "Rías Baixas",
        "RUEDA": "Rueda",
        # Germany
        "RHEINGAU": "Rheingau",
        "RHEINHESSEN": "Rheinhessen",
        "PFALZ": "Pfalz",
        # Portugal
        "VINHO VERDE": "Vinho Verde",
        "DAO": "Dão",
        "DOURO": "Douro",
        "ALENTEJO": "Alentejo",
    }

    # Load appellation lookup
    apps = {}
    offset = 0
    while True:
        result = sb.table("appellations").select("id,name").range(offset, offset + 999).execute()
        for a in result.data:
            apps[a["name"].lower()] = a["id"]
        if len(result.data) < 1000:
            break
        offset += 1000

    # Check which manual aliases have matching appellations
    aliases_to_add = []
    for ttb_str, proper_name in MANUAL_ALIASES.items():
        if ttb_str.lower() in region_map:
            continue  # already handled as region
        app_id = apps.get(proper_name.lower())
        if app_id:
            aliases_to_add.append({
                "appellation_id": app_id,
                "alias_normalized": ttb_str.lower(),
            })

    print(f"  Manual appellation aliases to add: {len(aliases_to_add)}")
    if aliases_to_add and not dry_run:
        added = 0
        for alias in aliases_to_add:
            try:
                sb.table("appellation_aliases").insert(alias).execute()
                added += 1
                print(f"    + {alias['alias_normalized']}")
            except Exception as e:
                if "duplicate" in str(e).lower():
                    pass  # already exists
                else:
                    print(f"    Error: {e}")
        print(f"  Added: {added}")
    elif aliases_to_add:
        for alias in aliases_to_add[:10]:
            print(f"    + {alias['alias_normalized']} (dry-run)")

    return len(aliases_to_add)


def main():
    parser = argparse.ArgumentParser(description="Resolve TTB appellation strings to regions")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Step 1: Build the region mapping
    region_map = build_region_map(sb)

    # Step 2: Add appellation aliases for accent-stripped strings
    aliases_added = add_unresolved_appellation_aliases(sb, region_map, args.dry_run)

    # Step 3: Update canonical wines with region data
    updated = update_canonical_wines(sb, region_map, args.dry_run)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Region/country mappings: {len(region_map)}")
    print(f"  Appellation aliases added: {aliases_added}")
    print(f"  Canonical wines updated with region: {updated}")


if __name__ == "__main__":
    main()
