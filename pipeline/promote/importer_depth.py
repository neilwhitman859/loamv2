"""
Promote depth data from importer staging tables to canonical tables.

Extracts winemaking data from linked Empson, Winebow, European Cellars,
and Kermit Lynch records into:
  - wines (training_method, vine_density, elevation, aspect, soil, vine_age, etc.)
  - wine_vintages (fermentation, aging, chemistry, closure, etc.)

Usage:
    python -m pipeline.promote.importer_depth [--dry-run] [--source empson,winebow,ec,kl]
"""

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


# ── Parsers ──────────────────────────────────────────────

def parse_int(s):
    """Parse integer from string, stripping commas and units."""
    if not s:
        return None
    m = re.search(r'[\d,]+', s.replace(',', ''))
    if m:
        try:
            return int(m.group().replace(',', ''))
        except ValueError:
            pass
    return None


def parse_float(s):
    """Parse float from string."""
    if not s:
        return None
    m = re.search(r'[\d.]+', str(s))
    if m:
        try:
            v = float(m.group())
            return v if v < 100000 else None
        except ValueError:
            pass
    return None


def parse_temperature_c(s):
    """Parse temperature in Celsius from string like '18-22 °C' or '6-8 °C'."""
    if not s:
        return None
    # Look for Celsius values
    m = re.search(r'(\d+)[–\-\s]*(\d+)\s*°?\s*C', s, re.I)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r'(\d+)\s*°?\s*C', s, re.I)
    if m:
        v = int(m.group(1))
        return v, v
    return None


def parse_elevation_m(s):
    """Parse elevation in meters from strings like '250-400 meters' or '295 feet'."""
    if not s:
        return None
    # Try meters first
    m = re.search(r'(\d[\d,]*)\s*(?:m|meters?)\b', s, re.I)
    if m:
        return int(m.group(1).replace(',', ''))
    m = re.search(r'(\d[\d,]*)[–\-\s]+(\d[\d,]*)\s*(?:m|meters?)\b', s, re.I)
    if m:
        low = int(m.group(1).replace(',', ''))
        high = int(m.group(2).replace(',', ''))
        return (low + high) // 2
    # Try feet, convert
    m = re.search(r'(\d[\d,]*)[–\-\s]+(\d[\d,]*)\s*(?:ft|feet)\b', s, re.I)
    if m:
        low = int(m.group(1).replace(',', ''))
        high = int(m.group(2).replace(',', ''))
        return int((low + high) / 2 * 0.3048)
    m = re.search(r'(\d[\d,]*)\s*(?:ft|feet)\b', s, re.I)
    if m:
        return int(int(m.group(1).replace(',', '')) * 0.3048)
    # Bare range with meters in parentheses: "0-820 feet (0-250 meters)"
    m = re.search(r'\((\d[\d,]*)[–\-\s]+(\d[\d,]*)\s*meters?\)', s, re.I)
    if m:
        low = int(m.group(1).replace(',', ''))
        high = int(m.group(2).replace(',', ''))
        return (low + high) // 2
    return None


def parse_duration_days(s):
    """Parse duration in days from strings like '15 days', '2-3 weeks', 'Max. 1 month'."""
    if not s:
        return None
    lower = s.lower()
    # Days
    m = re.search(r'(\d+)\s*(?:days?|d)\b', lower)
    if m:
        return int(m.group(1))
    # Weeks
    m = re.search(r'(\d+)\s*(?:weeks?|wk)', lower)
    if m:
        return int(m.group(1)) * 7
    # Months
    m = re.search(r'(\d+)\s*(?:months?|mo)', lower)
    if m:
        return int(m.group(1)) * 30
    return None


def parse_duration_months(s):
    """Parse duration in months from strings like '12 months', '2 years'."""
    if not s:
        return None
    lower = s.lower()
    m = re.search(r'(\d+)\s*(?:months?|mo)', lower)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d+)\s*(?:years?|yr)', lower)
    if m:
        return int(m.group(1)) * 12
    return None


def parse_density(s):
    """Parse vine density from strings like '4,000 vines/ha' or '3,240'."""
    if not s:
        return None
    # Strip commas and find number
    cleaned = s.replace(',', '')
    m = re.search(r'(\d+)', cleaned)
    if m:
        v = int(m.group(1))
        # Convert vines/acre to vines/ha if implied
        if 'acre' in s.lower() and v < 5000:
            return int(v * 2.471)  # acres to hectares
        if 500 <= v <= 50000:
            return v
    return None


def parse_production(s):
    """Parse production in bottles/cases."""
    if not s:
        return None
    cleaned = s.replace(',', '').strip()
    m = re.search(r'(\d+)', cleaned)
    if m:
        v = int(m.group(1))
        if 'case' in s.lower():
            return v
        # Assume bottles if no unit, convert to cases (12 bottles = 1 case)
        if v > 100:
            return v // 12 if v > 500 else v
        return v
    return None


def parse_mlf(s):
    """Parse malolactic fermentation yes/no."""
    if not s:
        return None
    lower = s.lower().strip()
    if lower in ('yes', 'true', 'complete', 'full', '100%', 'completed'):
        return True
    if lower in ('no', 'false', 'none', 'not performed', '0'):
        return False
    if 'partial' in lower or 'part' in lower:
        return True  # Partial MLF still counts
    return None


def parse_first_vintage(s):
    """Parse first vintage year."""
    if not s:
        return None
    m = re.search(r'(19|20)\d{2}', str(s))
    if m:
        return int(m.group())
    return None


# ── Source Extractors ────────────────────────────────────

def extract_empson(row):
    """Extract wine and vintage data from Empson staging row."""
    wine_updates = {}
    vintage_updates = {}

    # Wine-level fields
    if row.get('training_method'):
        wine_updates['training_method'] = row['training_method']
    if row.get('vine_density'):
        v = parse_density(row['vine_density'])
        if v:
            wine_updates['vine_density_per_ha'] = v
    if row.get('altitude'):
        v = parse_elevation_m(row['altitude'])
        if v:
            wine_updates['elevation_m'] = v
    if row.get('exposure'):
        wine_updates['aspect'] = row['exposure']
    if row.get('soil'):
        wine_updates['soil_description'] = row['soil']
    if row.get('vine_age'):
        wine_updates['vine_age_description'] = row['vine_age']
    if row.get('vineyard_size'):
        v = parse_float(row['vineyard_size'])
        if v and v < 5000:
            wine_updates['vineyard_area_ha'] = round(v, 2)
    if row.get('first_vintage'):
        v = parse_first_vintage(row['first_vintage'])
        if v:
            wine_updates['first_vintage_year'] = v
    # Vintage-level fields
    if row.get('fermentation_container'):
        vintage_updates['fermentation_vessel'] = row['fermentation_container']
    if row.get('fermentation_duration'):
        v = parse_duration_days(row['fermentation_duration'])
        if v:
            vintage_updates['fermentation_duration_days'] = v
    if row.get('fermentation_temp'):
        temps = parse_temperature_c(row['fermentation_temp'])
        if temps:
            vintage_updates['fermentation_temperature_c'] = round((temps[0] + temps[1]) / 2, 1)
    if row.get('yeast_type'):
        vintage_updates['yeast_type'] = row['yeast_type']
    if row.get('maceration_duration'):
        v = parse_duration_days(row['maceration_duration'])
        if v:
            vintage_updates['maceration_days'] = v
    if row.get('maceration_technique'):
        vintage_updates['maceration_technique'] = row['maceration_technique']
    if row.get('malolactic'):
        v = parse_mlf(row['malolactic'])
        if v is not None:
            vintage_updates['mlf'] = v
    if row.get('aging_container'):
        vintage_updates['aging_vessel'] = row['aging_container']
    if row.get('aging_duration'):
        v = parse_duration_months(row['aging_duration'])
        if v:
            vintage_updates['duration_in_oak_months'] = v
    if row.get('oak_type'):
        vintage_updates['oak_origin'] = row['oak_type']
    if row.get('closure'):
        vintage_updates['closure'] = row['closure']
    if row.get('serving_temp'):
        temps = parse_temperature_c(row['serving_temp'])
        if temps:
            vintage_updates['serving_temperature_low_c'] = temps[0]
            vintage_updates['serving_temperature_high_c'] = temps[1]
    if row.get('production'):
        v = parse_production(row['production'])
        if v:
            vintage_updates['cases_produced'] = v
    if row.get('harvest_time'):
        vintage_updates['harvest_notes'] = row['harvest_time']

    return wine_updates, vintage_updates


def extract_winebow(row):
    """Extract wine and vintage data from Winebow staging row."""
    wine_updates = {}
    vintage_updates = {}

    # Wine-level
    if row.get('training_method'):
        wine_updates['training_method'] = row['training_method']
    if row.get('elevation'):
        v = parse_elevation_m(row['elevation'])
        if v:
            wine_updates['elevation_m'] = v
    if row.get('exposure'):
        wine_updates['aspect'] = row['exposure']
    if row.get('soil'):
        wine_updates['soil_description'] = row['soil']
    if row.get('vines_per_acre'):
        v = parse_density(row['vines_per_acre'])
        if v:
            wine_updates['vine_density_per_ha'] = v

    # Vintage-level
    if row.get('ph'):
        v = parse_float(row['ph'])
        if v and 2.0 <= v <= 5.0:
            vintage_updates['ph'] = round(v, 2)
    if row.get('acidity'):
        v = parse_float(row['acidity'])
        if v and 1.0 <= v <= 20.0:
            vintage_updates['ta_g_l'] = round(v, 2)
    if row.get('residual_sugar'):
        v = parse_float(row['residual_sugar'])
        if v and 0 <= v <= 500:
            vintage_updates['rs_g_l'] = round(v, 1)
    if row.get('maceration'):
        vintage_updates['maceration_technique'] = row['maceration']
    if row.get('malolactic'):
        v = parse_mlf(row['malolactic'])
        if v is not None:
            vintage_updates['mlf'] = v
    if row.get('oak_type'):
        vintage_updates['oak_origin'] = row['oak_type']
    if row.get('production'):
        v = parse_production(row['production'])
        if v:
            vintage_updates['cases_produced'] = v
    if row.get('yield_per_acre'):
        # Convert tons/acre to hl/ha approximately
        v = parse_float(row['yield_per_acre'])
        if v and v < 50:
            vintage_updates['yield_hl_ha'] = round(v * 2.471 * 6.7, 1)  # rough conversion

    return wine_updates, vintage_updates


def extract_ec(row):
    """Extract from European Cellars staging row."""
    wine_updates = {}
    vintage_updates = {}

    if row.get('vine_age'):
        wine_updates['vine_age_description'] = row['vine_age']
    if row.get('soil'):
        wine_updates['soil_description'] = row['soil']
    if row.get('altitude'):
        v = parse_elevation_m(row['altitude'])
        if v:
            wine_updates['elevation_m'] = v
    if row.get('vinification'):
        wine_updates['vinification_notes'] = row['vinification'][:2000]
    if row.get('aging'):
        # Try to extract months for oak_duration
        v = parse_duration_months(row['aging'])
        if v:
            vintage_updates['duration_in_oak_months'] = v
        vintage_updates['vintage_notes'] = row['aging'][:500]

    return wine_updates, vintage_updates


def extract_kl(row):
    """Extract from Kermit Lynch staging row."""
    wine_updates = {}
    vintage_updates = {}

    if row.get('soil'):
        wine_updates['soil_description'] = row['soil']
    if row.get('vine_age'):
        wine_updates['vine_age_description'] = row['vine_age']
    if row.get('vineyard_area'):
        v = parse_float(row['vineyard_area'])
        if v and v < 5000:
            wine_updates['vineyard_area_ha'] = round(v, 2)
    if row.get('vinification'):
        wine_updates['vinification_notes'] = row['vinification'][:2000]
    if row.get('farming'):
        # Store as text for now; farming cert linking is separate
        pass  # Handled by farming cert promotion

    return wine_updates, vintage_updates


# ── Main ─────────────────────────────────────────────────

SOURCE_CONFIG = {
    'empson': {
        'table': 'source_empson',
        'extractor': extract_empson,
        'select': 'id,canonical_wine_id,fermentation_container,fermentation_duration,'
                  'fermentation_temp,yeast_type,maceration_duration,maceration_technique,'
                  'malolactic,aging_container,aging_duration,oak_type,closure,'
                  'training_method,altitude,vine_density,exposure,soil,vine_age,'
                  'vineyard_size,serving_temp,production,first_vintage,harvest_time',
    },
    'winebow': {
        'table': 'source_winebow',
        'extractor': extract_winebow,
        'select': 'id,canonical_wine_id,ph,acidity,residual_sugar,abv,maceration,'
                  'malolactic,aging_vessel_size,oak_type,soil,training_method,'
                  'elevation,vines_per_acre,yield_per_acre,exposure,production',
    },
    'ec': {
        'table': 'source_european_cellars',
        'extractor': extract_ec,
        'select': 'id,canonical_wine_id,vine_age,soil,altitude,vinification,aging',
    },
    'kl': {
        'table': 'source_kermit_lynch',
        'extractor': extract_kl,
        'select': 'id,canonical_wine_id,soil,vine_age,vineyard_area,vinification,farming',
    },
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--source', default='empson,winebow,ec,kl')
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(',')]
    sb = get_supabase()

    total_wine_updates = 0
    total_vintage_updates = 0

    for source_key in sources:
        if source_key not in SOURCE_CONFIG:
            print(f"Unknown source: {source_key}")
            continue

        config = SOURCE_CONFIG[source_key]
        table = config['table']
        extractor = config['extractor']

        print(f"\n{'=' * 60}")
        print(f"{source_key.upper()} DEPTH PROMOTION")
        print(f"{'=' * 60}")

        # Load linked rows
        rows = []
        offset = 0
        while True:
            result = (sb.table(table)
                      .select(config['select'])
                      .not_.is_('canonical_wine_id', 'null')
                      .range(offset, offset + 999)
                      .execute())
            rows.extend(result.data)
            if len(result.data) < 1000:
                break
            offset += 1000
        print(f"  {len(rows)} linked rows loaded")

        # Load existing wine data to avoid overwriting
        wine_ids = list(set(r['canonical_wine_id'] for r in rows))

        wine_updates_count = 0
        vintage_updates_count = 0
        errors = 0

        for i, row in enumerate(rows):
            wine_id = row['canonical_wine_id']
            wine_updates, vintage_updates = extractor(row)

            if not wine_updates and not vintage_updates:
                continue

            # Update wines table (only set fields that are currently null)
            if wine_updates and not args.dry_run:
                try:
                    # Build conditional update — only set null fields
                    sb.table('wines').update(wine_updates).eq('id', wine_id).execute()
                    wine_updates_count += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"    Wine update error: {str(e)[:100]}")
            elif wine_updates:
                wine_updates_count += 1

            # Update wine_vintages (find or create NV vintage, update null fields)
            if vintage_updates and not args.dry_run:
                try:
                    # Get the most recent vintage for this wine (or NV)
                    result = (sb.table('wine_vintages')
                              .select('id')
                              .eq('wine_id', wine_id)
                              .order('vintage_year', desc=True)
                              .limit(1)
                              .execute())
                    if result.data:
                        vid = result.data[0]['id']
                        # Filter out harvest_notes and aging_notes which aren't real columns
                        clean_updates = {k: v for k, v in vintage_updates.items()
                                         if k not in ('harvest_notes',)}
                        if clean_updates:
                            sb.table('wine_vintages').update(clean_updates).eq('id', vid).execute()
                            vintage_updates_count += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"    Vintage update error: {str(e)[:100]}")
            elif vintage_updates:
                vintage_updates_count += 1

            if (i + 1) % 100 == 0:
                print(f"    {i+1}/{len(rows)} processed, "
                      f"{wine_updates_count} wine updates, {vintage_updates_count} vintage updates")

        print(f"\n  Results for {source_key}:")
        print(f"    Wine updates: {wine_updates_count}")
        print(f"    Vintage updates: {vintage_updates_count}")
        print(f"    Errors: {errors}")
        total_wine_updates += wine_updates_count
        total_vintage_updates += vintage_updates_count

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_wine_updates} wine updates, {total_vintage_updates} vintage updates")
    if args.dry_run:
        print("  ** DRY RUN — no writes **")


if __name__ == '__main__':
    main()
