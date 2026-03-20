"""
Expand region_name_mappings to cover more wine_candidates region_names.

Strategy:
1. Exact match unmapped region_name to appellation.name
2. Normalized match (strip accents, lowercase) to appellation.name
3. French sub-appellation patterns (1er Cru, Grand Cru -> base appellation)
4. Italian sub-DOC patterns (Classico, Superiore -> base DOC)
5. Known manual mappings for common alternative names

Usage:
    python -m pipeline.vivino.expand_regions --dry-run
    python -m pipeline.vivino.expand_regions
"""

import sys
import re
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize

# ── Manual mappings for well-known alternative names ────────
# "region_name|country" -> {"region": "Region Name", "appellation"?: "Appellation Name"}
MANUAL_REGION_ALIASES = {
    # France - Bordeaux
    "Cotes de Bourg|France": {"region": "Bordeaux"},
    "Castillon-Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Puisseguin-Saint-Emilion|France": {"region": "Bordeaux"},
    "Blaye-Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Montagne-Saint-Emilion|France": {"region": "Bordeaux"},
    "Lussac-Saint-Emilion|France": {"region": "Bordeaux"},
    "Cotes de Bordeaux|France": {"region": "Bordeaux"},
    "Guyenne|France": {"region": "Bordeaux"},
    # France - Rhone
    "Ventoux|France": {"region": "Southern Rhone"},
    "Luberon|France": {"region": "Southern Rhone"},
    "Costieres-de-Nimes|France": {"region": "Southern Rhone"},
    "Cotes-du-Rhone-Villages|France": {"region": "Southern Rhone"},
    "Vaucluse|France": {"region": "Rhone Valley"},
    "Collines Rhodaniennes|France": {"region": "Rhone Valley"},
    # France - Burgundy
    "Bourgogne Hautes-Cotes de Beaune|France": {"region": "Burgundy"},
    "Bourgogne Hautes-Cotes de Nuits|France": {"region": "Burgundy"},
    "Saint-Aubin|France": {"region": "Burgundy"},
    "Vire-Clesse|France": {"region": "Burgundy"},
    "Cote de Nuits Villages|France": {"region": "Burgundy"},
    "Macon-Villages|France": {"region": "Burgundy"},
    "Bourgogne Aligote|France": {"region": "Burgundy"},
    "Mercurey|France": {"region": "Burgundy"},
    "Givry|France": {"region": "Burgundy"},
    "Rully|France": {"region": "Burgundy"},
    # France - Loire
    "Muscadet-Sevre et Maine|France": {"region": "Loire Valley"},
    "Touraine|France": {"region": "Loire Valley"},
    "Anjou|France": {"region": "Loire Valley"},
    "Savennieres|France": {"region": "Loire Valley"},
    # France - Languedoc
    "Pays d'Oc|France": {"region": "Languedoc-Roussillon"},
    "Pays d'Herault|France": {"region": "Languedoc-Roussillon"},
    "Picpoul de Pinet|France": {"region": "Languedoc"},
    "Cotes Catalanes|France": {"region": "Roussillon"},
    # France - Provence
    "Coteaux Varois en Provence|France": {"region": "Provence"},
    "Mediterranee|France": {"region": "Provence"},
    "Coteaux d'Aix-en-Provence|France": {"region": "Provence"},
    # France - Southwest
    "Cotes de Gascogne|France": {"region": "Southwest France"},
    "Gascogne|France": {"region": "Southwest France"},
    # France - Corsica
    "Ile de Beaute|France": {"region": "Corsica"},
    # Italy
    "Verona|Italy": {"region": "Veneto"},
    "Conegliano-Valdobbiadene Prosecco|Italy": {"region": "Veneto"},
    "Trevenezie|Italy": {"region": "Veneto"},
    "Colli Orientali del Friuli|Italy": {"region": "Friuli-Venezia Giulia"},
    "Collio|Italy": {"region": "Friuli-Venezia Giulia"},
    "Chianti Classico|Italy": {"region": "Tuscany"},
    "Maremma Toscana|Italy": {"region": "Tuscany"},
    "Morellino di Scansano|Italy": {"region": "Tuscany"},
    "Rosso di Montalcino|Italy": {"region": "Tuscany"},
    "Langhe|Italy": {"region": "Piedmont"},
    "Roero|Italy": {"region": "Piedmont"},
    "Gavi|Italy": {"region": "Piedmont"},
    "Monferrato|Italy": {"region": "Piedmont"},
    "Rubicone|Italy": {"region": "Emilia-Romagna"},
    "Terre Siciliane|Italy": {"region": "Sicily"},
    "Salento|Italy": {"region": "Puglia"},
    "Puglia|Italy": {"region": "Puglia"},
    "Umbria|Italy": {"region": "Umbria"},
    "Campania|Italy": {"region": "Campania"},
    # Spain
    "Rioja Alta|Spain": {"region": "Rioja"},
    "Rioja Alavesa|Spain": {"region": "Rioja"},
    "Oloroso Sherry|Spain": {"region": "Jerez"},
    "Manzanilla|Spain": {"region": "Jerez"},
    "Tierra de Castilla|Spain": {"region": "La Mancha"},
    "Ribeira Sacra|Spain": {"region": "Galicia"},
    # Portugal
    "Duriense|Portugal": {"region": "Douro"},
    "Evora|Portugal": {"region": "Alentejo"},
    # Germany
    "Brauneberg|Germany": {"region": "Mosel"},
    "Bernkastel|Germany": {"region": "Mosel"},
    "Rudesheim|Germany": {"region": "Rheingau"},
    "Nierstein|Germany": {"region": "Rheinhessen"},
    "Deidesheim|Germany": {"region": "Pfalz"},
    # Austria
    "Mittelburgenland|Austria": {"region": "Burgenland"},
    "Sudburgenland|Austria": {"region": "Burgenland"},
    "Neusiedlersee|Austria": {"region": "Burgenland"},
    # US - Napa
    "Mount Veeder|United States": {"region": "Napa Valley"},
    "Howell Mountain|United States": {"region": "Napa Valley"},
    "Stags Leap District|United States": {"region": "Napa Valley"},
    "Rutherford|United States": {"region": "Napa Valley"},
    "Oakville|United States": {"region": "Napa Valley"},
    "Calistoga|United States": {"region": "Napa Valley"},
    "St. Helena|United States": {"region": "Napa Valley"},
    # US - Sonoma
    "Alexander Valley|United States": {"region": "Sonoma County"},
    "Dry Creek Valley|United States": {"region": "Sonoma County"},
    "Russian River Valley|United States": {"region": "Sonoma County"},
    "Sonoma Coast|United States": {"region": "Sonoma County"},
    "Fort Ross-Seaview|United States": {"region": "Sonoma County"},
    # US - Central Coast / Santa Barbara
    "Santa Cruz Mountains|United States": {"region": "Central Coast"},
    "Edna Valley|United States": {"region": "Central Coast"},
    "Paso Robles|United States": {"region": "Central Coast"},
    "Santa Maria Valley|United States": {"region": "Santa Barbara County"},
    "Sta. Rita Hills|United States": {"region": "Santa Barbara County"},
    # Australia
    "Langhorne Creek|Australia": {"region": "South Australia"},
    "Limestone Coast|Australia": {"region": "South Australia"},
    "Great Southern|Australia": {"region": "Western Australia"},
    "King Valley|Australia": {"region": "Victoria"},
    "Geelong|Australia": {"region": "Victoria"},
    "Orange|Australia": {"region": "New South Wales"},
    # South Africa
    "Wellington|South Africa": {"region": "Coastal Region"},
    "Robertson|South Africa": {"region": "Western Cape"},
    "Hemel-en-Aarde Valley|South Africa": {"region": "Walker Bay"},
    # New Zealand
    "Waipara Valley|New Zealand": {"region": "Canterbury"},
    "Gimblett Gravels|New Zealand": {"region": "Hawke's Bay"},
    # Chile
    "San Antonio Valley|Chile": {"region": "Aconcagua Valley"},
    # Argentina
    "Paraje Altamira|Argentina": {"region": "Uco Valley"},
    "Tupungato|Argentina": {"region": "Uco Valley"},
    "Lujan de Cuyo|Argentina": {"region": "Mendoza"},
    "Cafayate|Argentina": {"region": "Salta"},
    # Canada
    "Ontario|Canada": {"region": "Niagara Peninsula"},
    "British Columbia|Canada": {"region": "Okanagan Valley"},
}


def fetch_all(table, columns="*", batch_size=1000):
    sb = get_supabase()
    rows, offset = [], 0
    while True:
        result = sb.table(table).select(columns).range(offset, offset + batch_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < batch_size: break
        offset += batch_size
    return rows


def main():
    parser = argparse.ArgumentParser(description="Expand region name mappings")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    print("Loading reference data...")
    regions = fetch_all("regions", "id,name,slug,country_id,is_catch_all")
    appellations = fetch_all("appellations", "id,name,region_id,country_id,designation_type")
    countries = fetch_all("countries", "id,name")
    existing_mappings = fetch_all("region_name_mappings", "region_name,country")

    country_map = {}
    for c in countries:
        country_map[c["name"]] = c["id"]
        country_map[c["id"]] = c["name"]

    region_by_name = {}
    region_by_norm = {}
    catch_all_by_country_id = {}
    for r in regions:
        region_by_name[f"{r['name']}|{r['country_id']}"] = r["id"]
        region_by_norm[f"{normalize(r['name'])}|{r['country_id']}"] = r["id"]
        if r.get("is_catch_all"):
            catch_all_by_country_id[r["country_id"]] = r["id"]

    appell_by_name = {}
    appell_by_norm = {}
    for a in appellations:
        appell_by_name[f"{a['name']}|{a['country_id']}"] = {"id": a["id"], "region_id": a.get("region_id")}
        appell_by_norm[f"{normalize(a['name'])}|{a['country_id']}"] = {"id": a["id"], "region_id": a.get("region_id")}

    existing_set = {f"{m['region_name']}|{m['country']}" for m in existing_mappings}

    print(f"  {len(regions)} regions, {len(appellations)} appellations, {len(existing_mappings)} existing mappings")

    # Get unmapped region_names from wine_candidates
    all_candidates = fetch_all("wine_candidates", "region_name,country")
    unmapped_counts: dict[str, int] = {}
    for wc in all_candidates:
        if not wc.get("region_name"):
            continue
        key = f"{wc['region_name']}|{wc['country']}"
        if key in existing_set:
            continue
        unmapped_counts[key] = unmapped_counts.get(key, 0) + 1

    total_wines = sum(unmapped_counts.values())
    print(f"  {len(unmapped_counts)} unmapped region_name|country combos ({total_wines} wines)\n")

    new_mappings = []
    matched_by_appellation = 0
    matched_by_appellation_norm = 0
    matched_by_region = 0
    matched_by_sub_appellation = 0
    matched_by_manual = 0
    unmatched = 0

    sorted_unmapped = sorted(unmapped_counts.items(), key=lambda x: -x[1])

    for key, wine_count in sorted_unmapped:
        sep = key.rfind("|")
        region_name = key[:sep]
        country = key[sep + 1:]
        country_id = country_map.get(country)
        if not country_id:
            unmatched += 1
            continue

        region_id = None
        appellation_id = None
        match_type = None

        # Strategy 1: Exact appellation match
        app_exact = appell_by_name.get(f"{region_name}|{country_id}")
        if app_exact:
            region_id = app_exact["region_id"]
            appellation_id = app_exact["id"]
            match_type = "appellation_exact"
            matched_by_appellation += 1

        # Strategy 2: Normalized appellation match
        if not match_type:
            app_norm = appell_by_norm.get(f"{normalize(region_name)}|{country_id}")
            if app_norm:
                region_id = app_norm["region_id"]
                appellation_id = app_norm["id"]
                match_type = "appellation_norm"
                matched_by_appellation_norm += 1

        # Strategy 3: French/Italian sub-appellation patterns
        if not match_type:
            base_name = region_name
            base_name = re.sub(r"\s+1er\s+Cru\s+['\"][^'\"]*['\"]$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Grand\s+Cru\s+['\"][^'\"]*['\"]$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+1er\s+Cru$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Grand\s+Cru$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Classico\s+Superiore$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Classico$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Superiore$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Riserva$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Ripasso\s+Classico$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Ripasso$", "", base_name, flags=re.IGNORECASE)
            base_name = re.sub(r"\s+Chiaretto$", "", base_name, flags=re.IGNORECASE)

            di_match = re.match(r"^(.+?)\s+di\s+\w+$", base_name, re.IGNORECASE)

            if base_name != region_name:
                stripped = appell_by_name.get(f"{base_name}|{country_id}") or appell_by_norm.get(f"{normalize(base_name)}|{country_id}")
                if stripped:
                    region_id = stripped["region_id"]
                    appellation_id = stripped["id"]
                    match_type = "sub_appellation"
                    matched_by_sub_appellation += 1

            if not match_type and di_match:
                di_base = di_match.group(1)
                di_stripped = appell_by_name.get(f"{di_base}|{country_id}") or appell_by_norm.get(f"{normalize(di_base)}|{country_id}")
                if di_stripped:
                    region_id = di_stripped["region_id"]
                    appellation_id = di_stripped["id"]
                    match_type = "sub_appellation"
                    matched_by_sub_appellation += 1

        # Strategy 4: Region match
        if not match_type:
            reg_exact = region_by_name.get(f"{region_name}|{country_id}")
            if reg_exact:
                region_id = reg_exact
                match_type = "region_exact"
                matched_by_region += 1

        if not match_type:
            reg_norm = region_by_norm.get(f"{normalize(region_name)}|{country_id}")
            if reg_norm:
                region_id = reg_norm
                match_type = "region_norm"
                matched_by_region += 1

        # Strategy 5: Manual mapping
        if not match_type:
            manual = MANUAL_REGION_ALIASES.get(f"{region_name}|{country}")
            if manual:
                region_id = region_by_name.get(f"{manual['region']}|{country_id}") or region_by_norm.get(f"{normalize(manual['region'])}|{country_id}")
                if manual.get("appellation"):
                    app = appell_by_name.get(f"{manual['appellation']}|{country_id}") or appell_by_norm.get(f"{normalize(manual['appellation'])}|{country_id}")
                    if app:
                        appellation_id = app["id"]
                if region_id:
                    match_type = "manual"
                    matched_by_manual += 1

        if match_type:
            new_mappings.append({
                "region_name": region_name, "country": country,
                "region_id": region_id, "appellation_id": appellation_id,
                "match_type": match_type,
            })
        else:
            if wine_count >= 20:
                print(f'  UNMATCHED ({wine_count}w): "{region_name}" [{country}]')
            unmatched += 1

    print(f"\n-- Match Results --")
    print(f"  Appellation exact:    {matched_by_appellation}")
    print(f"  Appellation norm:     {matched_by_appellation_norm}")
    print(f"  Sub-appellation:      {matched_by_sub_appellation}")
    print(f"  Region exact/norm:    {matched_by_region}")
    print(f"  Manual mapping:       {matched_by_manual}")
    print(f"  Total matched:        {len(new_mappings)}")
    print(f"  Unmatched:            {unmatched}")

    wines_matched = sum(unmapped_counts.get(f"{m['region_name']}|{m['country']}", 0) for m in new_mappings)
    print(f"  Wines covered by new mappings: {wines_matched}")

    if args.dry_run:
        print(f"\nDRY RUN -- no database changes made.")
        for match_type in ["appellation_exact", "appellation_norm", "sub_appellation", "region_exact", "region_norm", "manual"]:
            samples = [m for m in new_mappings if m["match_type"] == match_type][:5]
            if samples:
                print(f"\n  {match_type} samples:")
                for m in samples:
                    wc = unmapped_counts.get(f"{m['region_name']}|{m['country']}", 0)
                    reg_name = next((r["name"] for r in regions if r["id"] == m["region_id"]), "?")
                    app_name = next((a["name"] for a in appellations if a["id"] == m.get("appellation_id")), "-")
                    print(f'    "{m["region_name"]}" [{m["country"]}] -> region: {reg_name}, appellation: {app_name} ({wc}w)')
        return

    # Map internal match types to DB-allowed values
    DB_MATCH_TYPE = {
        "appellation_exact": "exact_appellation", "appellation_norm": "exact_appellation",
        "sub_appellation": "alias", "region_exact": "exact_region",
        "region_norm": "exact_region", "manual": "alias",
    }
    for m in new_mappings:
        m["match_type"] = DB_MATCH_TYPE.get(m["match_type"], "alias")

    # Insert new mappings
    print(f"\nInserting {len(new_mappings)} new region_name_mappings...")
    insert_errors = 0
    for i in range(0, len(new_mappings), 500):
        batch = new_mappings[i:i + 500]
        try:
            sb.table("region_name_mappings").insert(batch).execute()
        except Exception as e:
            print(f"  Batch error at {i}: {e}")
            insert_errors += 1

    print(f"  Done inserting {len(new_mappings)} mappings ({insert_errors} errors).")
    print(f"\nMappings inserted! Use SQL to update wines with new region_id / appellation_id.")


if __name__ == "__main__":
    main()
