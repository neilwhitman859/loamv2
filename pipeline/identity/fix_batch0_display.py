"""
Fix Batch 0 display names and cuvee values.

Bug 1: 401 wines have display_name stored as cuvee (wines.name) -- should be NULL.
Bug 2: 904 wines have ALL CAPS grape names in display_name -- should be Title Case.

Usage:
    python -m pipeline.identity.fix_batch0_display --dry-run
    python -m pipeline.identity.fix_batch0_display --execute
"""
import argparse
import re
import sys

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify
from pipeline.identity.build_display_name import build_display_name
from pipeline.identity.clean_cuvee import extract_cuvee


# Common grape names that appear in LWIN wine names (ALL CAPS in display)
# Sorted longest-first to avoid partial matches
ALL_CAPS_GRAPES = sorted([
    "CABERNET SAUVIGNON", "SAUVIGNON BLANC", "PINOT NOIR", "CHENIN BLANC",
    "PINOT GRIGIO", "PINOT GRIS", "PINOT BLANC", "PINOT MEUNIER",
    "PETITE SIRAH", "PETIT VERDOT", "CABERNET FRANC", "NERO D'AVOLA",
    "MELON DE BOURGOGNE", "GRUNER VELTLINER",
    "TOURIGA NACIONAL", "TOURIGA FRANCA",
    "GEWURZTRAMINER",
    "CHARDONNAY", "RIESLING", "MERLOT", "SYRAH", "SHIRAZ",
    "GRENACHE", "TEMPRANILLO", "SANGIOVESE", "NEBBIOLO", "MALBEC",
    "MOURVEDRE", "CARIGNAN", "CINSAULT", "VIOGNIER",
    "SEMILLON", "MARSANNE", "ROUSSANNE", "MUSCAT", "MOSCATO",
    "GARGANEGA", "TREBBIANO", "VERDICCHIO", "VERDEJO",
    "ALBARINO", "GODELLO", "MENCIA",
    "BARBERA", "DOLCETTO", "PRIMITIVO", "ZINFANDEL", "AGLIANICO",
    "MONTEPULCIANO", "CORVINA", "GARNACHA",
    "PINOTAGE", "GAMAY", "ZWEIGELT",
    "FURMINT", "ASSYRTIKO",
    "SILVANER", "SCHEUREBE", "SPATBURGUNDER",
    "WEISSBURGUNDER", "GRAUBURGUNDER", "TROLLINGER", "LEMBERGER",
    "BOBAL", "MONASTRELL", "GRACIANO",
    "VERMENTINO", "PECORINO", "FALANGHINA", "FIANO", "GRECO",
    "GLERA", "PROSECCO", "LAMBRUSCO",
    "MATARO", "SIRAH",  # AU/US alternate names
    "THOMAS",  # Edge case seen in Penfolds
], key=len, reverse=True)

# Map for special Title Case handling (not simple .title())
GRAPE_TITLE_MAP = {
    "NERO D'AVOLA": "Nero d'Avola",
    "MELON DE BOURGOGNE": "Melon de Bourgogne",
    "GRUNER VELTLINER": "Gruner Veltliner",
    "PETITE SIRAH": "Petite Sirah",
}


def title_case_grape(name):
    """Title Case a grape name, handling special cases."""
    if name in GRAPE_TITLE_MAP:
        return GRAPE_TITLE_MAP[name]
    return name.title()


def fix_grape_casing(display_name):
    """Replace ALL CAPS grape names in display_name with Title Case."""
    result = display_name
    for grape in ALL_CAPS_GRAPES:
        # Word-boundary match to avoid partial replacements
        pattern = r'(?<!\w)' + re.escape(grape) + r'(?!\w)'
        match = re.search(pattern, result)
        if match:
            result = result[:match.start()] + title_case_grape(grape) + result[match.end():]
    return result


def load_grape_names_for_strip(cur):
    """Load grape names for cuvee stripping."""
    COMMON_GRAPES = {
        "CABERNET SAUVIGNON", "MERLOT", "PINOT NOIR", "SYRAH", "SHIRAZ",
        "GRENACHE", "TEMPRANILLO", "SANGIOVESE", "NEBBIOLO", "MALBEC",
        "CABERNET FRANC", "PETIT VERDOT", "MOURVEDRE",
        "CARIGNAN", "CINSAULT",
        "CHARDONNAY", "SAUVIGNON BLANC", "RIESLING", "PINOT GRIGIO",
        "PINOT GRIS", "VIOGNIER",
        "CHENIN BLANC", "SEMILLON", "MARSANNE", "ROUSSANNE",
        "MUSCAT", "MOSCATO", "GARGANEGA", "TREBBIANO", "VERDICCHIO",
        "GRUNER VELTLINER", "ALBARINO",
        "VERDEJO", "GODELLO", "MENCIA",
        "BARBERA", "DOLCETTO", "PRIMITIVO", "ZINFANDEL", "AGLIANICO",
        "NERO D'AVOLA", "MONTEPULCIANO", "CORVINA", "GARNACHA",
        "PETITE SIRAH", "PINOTAGE", "TOURIGA NACIONAL", "TOURIGA FRANCA",
        "GAMAY", "ZWEIGELT",
        "FURMINT", "ASSYRTIKO", "SILVANER", "SCHEUREBE",
        "SPATBURGUNDER", "WEISSBURGUNDER", "GRAUBURGUNDER",
        "VERMENTINO", "PECORINO", "FALANGHINA", "FIANO", "GRECO",
        "GLERA", "PROSECCO", "LAMBRUSCO",
        "MATARO", "SIRAH", "BOBAL", "MONASTRELL",
    }
    cur.execute("SELECT name FROM grapes")
    all_grapes = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT synonym FROM grape_synonyms")
    all_grapes.extend(r[0] for r in cur.fetchall())
    return sorted(
        [n for n in all_grapes if n.upper() in COMMON_GRAPES],
        key=len, reverse=True
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()
    execute = args.execute

    conn = get_conn()
    cur = conn.cursor()

    grape_names_for_strip = load_grape_names_for_strip(cur)

    # Load all wines with producer + identity for cuvee re-extraction
    cur.execute("""
        SELECT w.id, w.name, w.display_name,
               p.name as producer_name,
               a.name as appellation_name,
               c.iso_code as country_code
        FROM wines w
        JOIN producers p ON w.producer_id = p.id
        LEFT JOIN appellations a ON w.appellation_id = a.id
        LEFT JOIN countries c ON w.country_id = c.id
        ORDER BY p.name, w.display_name
    """)
    wines = cur.fetchall()
    print(f"Processing {len(wines)} wines...")

    # Load LWIN raw names for cuvee re-extraction
    cur.execute("""
        SELECT e.entity_id, sl.wine_name
        FROM external_ids e
        JOIN source_lwin sl ON sl.lwin_7::text = e.external_id
        WHERE e.entity_type = 'wine' AND e.system = 'lwin_7'
    """)
    lwin_names = {}
    for row in cur.fetchall():
        lwin_names[str(row[0])] = row[1]

    fixed_cuvee = 0
    fixed_display = 0
    fixed_both = 0

    for wine_id, old_name, old_display, producer_name, appellation_name, country_code in wines:
        wine_id_str = str(wine_id)

        # --- Fix 1: Re-extract cuvee ---
        raw_name = lwin_names.get(wine_id_str)
        if raw_name:
            new_cuvee = extract_cuvee(
                raw_name, producer_name, appellation_name,
                grape_names_for_strip, country_code
            )
        else:
            # No LWIN link -- if old name == display_name, it's the bug
            if old_name and old_name == old_display:
                new_cuvee = None
            else:
                new_cuvee = old_name

        # --- Fix 2: Title Case grapes in display_name ---
        new_display = fix_grape_casing(old_display)

        # If cuvee changed, we also need to update display_name to remove old cuvee
        # (display_name was built with cuvee=display_name for buggy rows)
        if old_name and old_name == old_display and new_cuvee is None:
            # The display name itself was built wrong -- it included the old
            # (incorrect) cuvee. But actually the display_name WAS the old cuvee,
            # so the display was just "Producer, Appellation" which is correct.
            # Just fix the grape casing.
            pass

        cuvee_changed = old_name != new_cuvee
        display_changed = old_display != new_display

        if not cuvee_changed and not display_changed:
            continue

        if cuvee_changed and display_changed:
            fixed_both += 1
        elif cuvee_changed:
            fixed_cuvee += 1
        elif display_changed:
            fixed_display += 1

        if not execute:
            if cuvee_changed:
                short_old = repr(old_name)[:50].ljust(50)
                print(f"  CUVEE: {short_old} -> {new_cuvee!r}")
            if display_changed:
                short_old = repr(old_display)[:60].ljust(60)
                print(f"  DISPL: {short_old} -> {new_display!r}")
        else:
            new_name_norm = normalize(new_cuvee) if new_cuvee else None
            new_slug = slugify(new_display)
            cur.execute("""
                UPDATE wines
                SET name = %s, name_normalized = %s, display_name = %s, slug = %s
                WHERE id = %s
            """, (new_cuvee, new_name_norm, new_display, new_slug, wine_id))

    if execute:
        conn.commit()

    total = fixed_cuvee + fixed_display + fixed_both
    print(f"\n{'[EXECUTE]' if execute else '[DRY RUN]'} Results:")
    print(f"  Cuvee only:   {fixed_cuvee}")
    print(f"  Display only: {fixed_display}")
    print(f"  Both:         {fixed_both}")
    print(f"  Total fixed:  {total}")
    print(f"  Unchanged:    {len(wines) - total}")

    conn.close()


if __name__ == "__main__":
    main()
