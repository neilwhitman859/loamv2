"""
Promote grape data from TTB records to wine_grapes table.

For each canonical wine that lacks grape data, finds TTB records via
canonical_wine_id index and resolves the grape_varietals string.

Uses direct Postgres (psycopg2) instead of Supabase REST API to avoid
HTTP/2 ConnectionTerminated errors on the 3.28M-row source_ttb_colas table.

Usage:
    python -m pipeline.promote.ttb_grape_promote [--limit 10000] [--dry-run]
"""

import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.resolve import ReferenceResolver


JUNK_STRINGS = {
    "the status is approved.", "the status is approved",
    "the status is expired.", "the status is expired",
    "the status is surrendered.", "the status is surrendered",
    "red wine", "white wine", "rose wine", "rosé wine", "table wine",
    "wine", "red", "white", "rose", "rosé", "blended wine", "blend",
    "na", "n/a", "none",
    # Vintage years / numeric noise erroneously in grape field
    "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012",
    "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020",
    "02", "03", "04", "05", "06", "07", "08", "09",  # 2-digit year remnants
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",  # more 2-digit noise
    # Generic Italian color/type terms
    "rosso", "bianco", "rosato",
}

# Style/appellation strings that appear in TTB grape field but are NOT grapes.
# Skip wines where grape_varietals matches these exactly.
BLEND_BLACKLIST = {
    "barbaresco", "barolo", "amarone", "valpolicella", "prosecco", "champagne",
    "blanc de blancs", "blanc de noirs", "marsala", "chianti", "rioja",
    "cava", "port", "sherry", "bordeaux", "burgundy",
    "pinot noir rose", "rose of pinot noir",
    "riesling spatlese", "riesling auslese", "riesling kabinett",
    "meritage", "red bordeaux", "red wine blend", "sancerre", "topaque",
    # Port classifications (not grape names)
    "ruby", "tawny", "vintage port", "late bottled vintage",
    # Italian/other wine styles (not grape names)
    "vin santo", "vino nobile", "amarone della valpolicella",
    # Appellation-prefixed names (grape + appellation)
    "traiser riesling",  # Riesling from Traisen — parse as single grape below
    # Run #5 additions — Haiku confirmed skip items
    "gavi", "crianza", "soave", "fendant", "bianchello del metauro",
    "white burgundy", "branco", "st vincent", "red bordeaux",
    "sparkling wine", "red table wine", "white zinfandel",
    "brut", "bianco", "rosso", "red blend", "white wine blend",
    "blanc de noir", "vendemmia", "cuvee", "the status is approved.",
    "the status is expired.", "the status is surrendered.",
    "red", "white", "rose wine", "rosé", "11/14/2005",
    "eec/eu (ttb use only)", "multiple countries",
    # Run #6 additions
    "white burgundy wine",     # style string (already have "white burgundy")
    "valdobbiadene",           # Prosecco appellation name, not a grape
    "palo cortado",            # Sherry style, not a grape
    "st. vincent",             # Patron saint of wine, not a grape (have "st vincent")
    "red burgundy",            # style/region name, not a grape
    "torrette",                # Valle d'Aosta DOC wine, not a grape
    "auslese",                 # German quality level, not a grape
    "chianti classico",        # Tuscan wine appellation, not a grape
    "ripasso",                 # Venetian wine style, not a grape
    "santenay",                # Burgundy appellation
    "brut rose",               # Sparkling wine style
    "brut rosé",
    "feinherb",                # German sweetness level
    "01",                      # numeric noise
    "montalcino",              # Brunello di Montalcino appellation, not a grape
    "ros wine",                # OCR/typo for "Rose Wine"
}

# TTB encodes accented chars as '?' — map common corrupted forms
TTB_GRAPE_FIXES = {
    "mourv?dre": "Mourvedre", "mourv dre": "Mourvedre",
    "albari?o": "Albarino", "albari o": "Albarino",
    "gew?rztraminer": "Gewurztraminer", "gew rztraminer": "Gewurztraminer",
    "gr?ner veltliner": "Gruner Veltliner", "gr ner veltliner": "Gruner Veltliner",
    "carmen?re": "Carmenere", "carmen re": "Carmenere",
    "aligot?": "Aligote", "aligot": "Aligote",
    "sp?tburgunder": "Spatburgunder", "sp tburgunder": "Spatburgunder",
    "valdigu?": "Valdiguie", "valdigu": "Valdiguie",
    "m?ller-thurgau": "Muller-Thurgau", "m ller-thurgau": "Muller-Thurgau",
    "m?ller thurgau": "Muller-Thurgau",
    "blaufr?nkisch": "Blaufrankisch", "blaufr?nkish": "Blaufrankisch",
    "torront?s": "Torrontes",
    "catarratto": "Catarratto Bianco Comune",
    "montepulciano d'abruzzo": "Montepulciano",
    "picpoul blanc": "Piquepoul Blanc", "picpoul": "Piquepoul Blanc",
    "pineau d'aunis": "Pineau d'Aunis",
    "gewurtztraminer": "Gewurztraminer",
    "reisling": "Riesling",
    "mar?chal foch": "Marechal Foch",
    "souz?o": "Souzao",
    "fum? blanc": "Fume Blanc",
    "nero d' avola": "Nero d'Avola",
    "nero d?avola": "Nero d'Avola",
    "gruner vetliner": "Gruner Veltliner",
    "pinot noir rose": "Pinot Noir",
    "rose of pinot noir": "Pinot Noir",
    "pinor noir": "Pinot Noir",
    "zinfindel": "Zinfandel",
    "gewruztraminer": "Gewurztraminer",
    "chardonnnay": "Chardonnay",
    "riseling": "Riesling",
    "cabernet france": "Cabernet Franc",
    "montepulciano d' abruzzo": "Montepulciano",
    # Run #3 additions
    # Blend strings with no space before percentage (handled as primary grape)
    "pinot noir20% chardonnay": "Pinot Noir",
    "cabernet sauvignon15% merlot": "Cabernet Sauvignon",
    # Other common fixes
    "gewuztraminer": "Gewurztraminer",
    "cataratto extra lucido": "Catarratto Bianco Comune",
    "weisserburgunder": "Pinot Blanc",  # German synonym
    "piot noir": "Pinot Noir",
    "garganaga": "Garganega",
    "musct de alejandria": "Muscat d'Alexandrie",
    "muscat de alejandria": "Muscat d'Alexandrie",
    "sauvingon blanc": "Sauvignon Blanc",
    "sauvingon": "Sauvignon Blanc",
    "pinot n0ir": "Pinot Noir",  # zero substituted for 'o'
    "gruner veltiner": "Gruner Veltliner",  # variant spelling (vetliner already covered)
    # Appellation-prefixed grape names (Haiku confirmed: extract the grape)
    "traiser riesling": "Riesling",  # Traisen = German appellation; grape is Riesling
    "nahe riesling": "Riesling",
    "mosel riesling": "Riesling",
    "rheingau riesling": "Riesling",
    "alsace riesling": "Riesling",
    "icewine riesling": "Riesling",
    # Typos
    "pinot auxerrios": "Pinot Auxerrois",
    "pinot auxerois": "Pinot Auxerrois",
    # Run #5 additions — truncated forms from TTB data
    "syr": "Syrah",         # truncated Syrah
    "carig": "Carignan",    # truncated Carignan/Cariñena
    "mourv": "Mourvèdre",   # truncated Mourvèdre (already have mourv?dre but not bare truncation)
    # Blend shorthand cleanup
    "viura malvoisia": "Viura",   # Viura is primary; Malvoisie/Malvasia is secondary
    "savagnin ouille": "Savagnin Blanc",  # Haiku confirmed Run #3: Savagnin Ouille = Savagnin Blanc
    # Run #5 — grape name resolution from DB inspection
    "baroques": "Baroque",              # plural form of Baroque (in DB as BAROQUE)
    "muscat petits grains": "Muscat a Petits Grains Blancs",  # common TTB form → VIVC canonical
    "argonez": "Tempranillo Tinto",     # typo for Aragonez, which is a synonym for Tempranillo
    "aragonez": "Tempranillo Tinto",    # Aragonez = Tempranillo synonym
    "zweiglt": "Zweigelt",              # common typo
    "zweigelt": "Zweigelt",
    "rkatsiteli rose": "Rkatsiteli",    # color suffix stripped
    "mouvedre": "Mourvèdre",            # common misspelling in TTB blends
    "alicante bouschet": "Alicante Bouschet",  # legitimate grape, ensure it resolves
    "tinto fino": "Tempranillo Tinto",  # Tinto Fino = Tempranillo in Spain
    # Run #5 — Haiku-confirmed fixes
    "riesling spatlese": "Riesling",        # quality level, extract grape
    "riesling auslese": "Riesling",
    "dry riesling": "Riesling",             # style prefix, extract grape
    "riesling spatlese": "Riesling",
    "gamay noir a jus blanc": "Gamay Noir", # French name (encoding strips accent)
    "gamay noir ? jus blanc": "Gamay Noir",
    "carinyena": "Carignan Noir",           # Catalan name for Carignan
    "mencia": "Mencia",                     # in DB as MENCIA
    "anglianico": "Aglianico",              # common typo
    "fruilano": "Friulano",                 # typo for Friulano
    "semillion": "Semillon",                # common misspelling
    "barbera asti": "Barbera Nera",         # Barbera d'Asti → extract grape
    "barbera d asti": "Barbera Nera",
    "barbera d'asti": "Barbera Nera",
    "barbera alba": "Barbera Nera",
    "barbera d'alba": "Barbera Nera",
    "verduzzo": "Verduzzo Friulano",        # matches VERDUZZO FRIULANO in DB
    "verduzzo friulano": "Verduzzo Friulano",
    "balufrankisch": "Blaufraenkisch",      # typo of Blaufrankisch (DB: BLAUFRAENKISCH via Lemberger synonym)
    "blaufrankisch": "Blaufraenkisch",
    "grenache rose": "Grenache",            # color suffix, extract grape
    "100% pineau d'aunis": "Pineau d'Aunis",  # percentage prefix
    "cabernet sauvingon": "Cabernet Sauvignon",  # misspelling
    # Run #5 second pass — more typos from 200K promotion run
    "pedro ximnez": "Pedro Ximenez",       # missing 'e'
    "pedro xim?nez": "Pedro Ximenez",      # encoding corruption
    "chadonnay": "Chardonnay",             # typo
    "pinot grigo": "Pinot Grigio",         # typo
    "rieling": "Riesling",                 # typo
    "smillon": "Semillon",                 # typo
    "cataratto": "Catarratto Bianco Comune",  # one 't' variant (cataratto vs catarratto)
    "rousanne": "Roussanne",               # typo
    "savignin": "Savagnin Blanc",          # 'Savignin' with i → Savagnin
    "muscato": "Muscat Blanc a Petits Grains",  # Italian vernacular for Muscat
    "savignon blanc": "Sauvignon Blanc",   # typo
    "grenache 25%": "Grenache",            # percentage embedded
    "grenache 80%": "Grenache",
    "grenache 50%": "Grenache",
    "malbec 85%": "Malbec",
    "syrah 85%": "Syrah",
    "chardonnay 85%": "Chardonnay",
    "pinot rose": "Pinot Gris",            # Pinot Rosé = Pinot Gris in most contexts
    # Run #6 additions — from top unresolved list
    "cabernet suavignon": "Cabernet Sauvignon",   # typo
    "pinot nior": "Pinot Noir",                   # typo
    "menca": "Mencia",                            # typo for Mencía (already have 'mencia')
    "friuliano": "Friulano",                      # typo with extra 'i'
    "gewurtzraminer": "Gewurztraminer",           # transposed letters (have 'gewurtztraminer')
    "hondarribi zuri": "Courbu Blanc",            # Basque synonym confirmed via VIVC
    "hondarrabi zuri": "Courbu Blanc",            # spelling variant
    "muscat d'alexandrie": "Muscat of Alexandria",  # French name → DB canonical
    "muscat d alexandrie": "Muscat of Alexandria",
    # Percentage-embedded single-grape strings
    "syrah 15%": "Syrah",
    "cabernet sauvignon 20%": "Cabernet Sauvignon",
    # Regional/obscure grape synonyms
    "jakot": "Friulano",                          # Slovenian backward spelling of Tocai Friulano
    "chiavennasca": "Nebbiolo",                   # Valtellina synonym for Nebbiolo
    "jacqure": "Jacquere",                        # typo for Jacquère (Savoie grape)
    "lon millot": "Leon Millot",                  # typo for Léon Millot (French-American hybrid)
    # More typos
    "rieslng": "Riesling",                        # missing 'i'
    "caberent sauvignon": "Cabernet Sauvignon",   # transposed 'e/r'
    "blaufrankish": "Blaufraenkisch",             # missing 'c'
    "1oo% chenin blanc": "Chenin Blanc",          # letter O instead of digit 0
    "100% chenin blanc": "Chenin Blanc",          # percentage prefix
    # Run #6, pass 4
    "san giovese": "Sangiovese",                  # typo (missing 'a')
    "riesling feinherb": "Riesling",              # Feinherb = off-dry style level
    "grenache noire": "Grenache Noir",            # French feminine form
    "garnatcha tinta": "Garnacha Tinta",          # variant spelling → DB canonical
    "garnache": "Garnacha Tinta",                 # short form → Garnacha
    "weiburgunder": "Pinot Blanc",               # typo for Weissburgunder
    "cabernet sauvigon": "Cabernet Sauvignon",   # missing 'n'
    "gerwurztraminer": "Gewurztraminer",          # transposed 'e/w'
    "gruner valtliner": "Gruner Veltliner",       # 'a' for 'e' typo
    "cabernet savignon": "Cabernet Sauvignon",   # transposed letters
    "malbec rose": "Malbec",                     # color suffix stripped
    "ros wine": "Rose Wine",                     # OCR error? → blacklisted below
    # More typos (Run #6, pass 3)
    "gewurtraminer": "Gewurztraminer",            # missing 'z'
    "catarrato": "Catarratto Bianco Comune",      # one 't' variant
    "shriaz": "Syrah",                            # transposed letters
    "weisburgunder": "Pinot Blanc",               # German: Weissburgunder = Pinot Blanc
    "weissburgunder": "Pinot Blanc",
    "sauvingnon blanc": "Sauvignon Blanc",        # transposed letters
    "trebbiano d'abruzzo": "Trebbiano Toscano",   # appellation-prefixed (extract grape)
    "trebbiano d abruzzo": "Trebbiano Toscano",
    "nebbiolo d'alba": "Nebbiolo",               # appellation-prefixed (extract grape)
    "nebbiolo d alba": "Nebbiolo",
}


def _fix_grape_name(name):
    """Fix TTB encoding corruption and known misspellings."""
    if not name:
        return None
    # TTB uses U+FFFD (replacement char) for accented chars, displayed as '?'
    # Normalize both \ufffd and ? to ? for lookup
    normalized = name.replace('\ufffd', '?')
    cleaned = name.replace('\ufffd', '').replace('?', '').strip()
    lower = normalized.lower()
    if lower in TTB_GRAPE_FIXES:
        return TTB_GRAPE_FIXES[lower]
    cleaned_lower = cleaned.lower()
    if cleaned_lower in TTB_GRAPE_FIXES:
        return TTB_GRAPE_FIXES[cleaned_lower]
    # Strip trailing percentage (e.g. "Syrah 15%" → "Syrah", "Chardonnay 100%" → "Chardonnay")
    pct_stripped = re.sub(r'\s+\d+\.?\d*\s*%\s*$', '', cleaned_lower).strip()
    if pct_stripped and pct_stripped != cleaned_lower:
        if pct_stripped in TTB_GRAPE_FIXES:
            return TTB_GRAPE_FIXES[pct_stripped]
        # Return the percentage-stripped version for resolver to handle
        return pct_stripped.title() if pct_stripped == pct_stripped.upper() else pct_stripped
    if cleaned_lower in JUNK_STRINGS:
        return None
    result = cleaned.title() if cleaned == cleaned.upper() else cleaned
    return result if len(result) >= 2 else None


def parse_grape_string(s):
    """Parse TTB grape_varietals string into [(name, percentage), ...].

    Handles encoding corruption (? replacing accented chars) and junk values.
    """
    if not s:
        return []
    s = s.strip()
    # Normalise: strip U+FFFD replacement chars used by TTB for accented chars
    s_lower = s.lower().replace('\ufffd', '?').strip()
    if s_lower in JUNK_STRINGS:
        return []
    if s_lower in BLEND_BLACKLIST:
        return []

    # Dash-separated blends: only split on dash if the result looks like grape names
    # (e.g. "Balufrankisch-Zweigelt-Cabernet Sauvignon-Merlot")
    # Heuristic: if no comma/slash and string has 2+ dash segments each 3+ chars → dash split
    if '-' in s and ',' not in s and '/' not in s:
        dash_parts = [p.strip() for p in s.split('-') if len(p.strip()) >= 3]
        if len(dash_parts) >= 2:
            parts = dash_parts
        else:
            parts = [s]
    else:
        parts = re.split(r'[,/|]', s)  # also split on pipe (|) separator
    grapes = []
    for part in parts:
        part = part.strip()
        if not part or len(part) < 2:
            continue
        part = re.sub(r'\s+100%$', '', part)
        pct_match = re.match(r'^(\d+)%?\s+(.+)$', part)
        if pct_match:
            pct = int(pct_match.group(1))
            name = pct_match.group(2).strip()
            if 0 < pct <= 100:
                name = _fix_grape_name(name)
                if name:
                    grapes.append((name, pct))
        else:
            name = re.sub(r'\s+', ' ', part).strip()
            name = _fix_grape_name(name)
            if name:
                grapes.append((name, None))
    return grapes


def main():
    import argparse
    from psycopg2.extras import execute_values

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_conn()
    cur = conn.cursor()

    # Load grape resolver (uses REST API internally for light reference reads -- that's fine)
    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Find ALL wines without grape data using a single SQL query with LEFT JOIN
    # No created_at filter -- processes the entire catalog
    print("Finding wines without grape data...")
    cur.execute("""
        SELECT w.id
        FROM wines w
        LEFT JOIN wine_grapes wg ON wg.wine_id = w.id
        WHERE w.deleted_at IS NULL
          AND wg.wine_id IS NULL
    """)
    wines_without_grapes = [row[0] for row in cur.fetchall()]
    print(f"  {len(wines_without_grapes)} wines without grapes")

    if args.limit and len(wines_without_grapes) > args.limit:
        wines_without_grapes = wines_without_grapes[:args.limit]
        print(f"  Limited to {args.limit}")

    if not wines_without_grapes:
        print("No wines need grape data.")
        cur.close()
        conn.close()
        return

    # Load TTB grape data for target wines using a single JOIN query
    # Uses a temp table to pass the wine ID list efficiently
    print("Loading grape data from TTB...")
    cur.execute("""
        CREATE TEMP TABLE _target_wines (wine_id uuid)
    """)
    execute_values(
        cur,
        "INSERT INTO _target_wines (wine_id) VALUES %s",
        [(wid,) for wid in wines_without_grapes],
        page_size=5000,
    )
    cur.execute("""
        SELECT DISTINCT ON (t.canonical_wine_id)
            t.canonical_wine_id, t.grape_varietals
        FROM source_ttb_colas t
        JOIN _target_wines tw ON tw.wine_id = t.canonical_wine_id
        WHERE t.grape_varietals IS NOT NULL
          AND t.canonical_wine_id IS NOT NULL
        ORDER BY t.canonical_wine_id, t.ttb_id
    """)
    wine_grape_map = {}
    for row in cur:
        wine_grape_map[row[0]] = row[1]

    cur.execute("DROP TABLE IF EXISTS _target_wines")
    conn.commit()

    print(f"  {len(wine_grape_map)} wines have TTB grape data")

    # Resolve grapes in memory
    print("Resolving grape names...")
    grape_inserts = []
    wines_resolved = 0
    grapes_resolved = 0
    grapes_unresolved = 0
    unresolved_names = {}

    for i, (wine_id, grape_string) in enumerate(wine_grape_map.items()):
        parsed = parse_grape_string(grape_string)

        if not parsed:
            continue

        wine_grapes = []
        for name, pct in parsed:
            grape = resolver.resolve_grape(name)
            if grape:
                wine_grapes.append((wine_id, grape["id"], pct))
                grapes_resolved += 1
            else:
                grapes_unresolved += 1
                unresolved_names[name] = unresolved_names.get(name, 0) + 1

        if wine_grapes:
            grape_inserts.extend(wine_grapes)
            wines_resolved += 1

        if (i + 1) % 5000 == 0:
            print(f"  {i + 1}/{len(wine_grape_map)} wines processed, "
                  f"{wines_resolved} resolved, {len(grape_inserts)} grape links")

    print(f"\nResults:")
    print(f"  {wines_resolved} wines with resolved grapes")
    print(f"  {len(grape_inserts)} grape links to insert")
    print(f"  {grapes_resolved} grapes resolved, {grapes_unresolved} unresolved")

    if unresolved_names:
        top_unresolved = sorted(unresolved_names.items(), key=lambda x: -x[1])[:15]
        print(f"\n  Top unresolved grape names:")
        for name, cnt in top_unresolved:
            safe_name = name.encode('ascii', 'replace').decode('ascii')
            print(f"    {safe_name:30s} {cnt}")

    if args.dry_run:
        print("\n  DRY RUN")
        cur.close()
        conn.close()
        return

    if grape_inserts:
        print(f"\nInserting {len(grape_inserts)} grape links...")
        execute_values(
            cur,
            """
            INSERT INTO wine_grapes (wine_id, grape_id, percentage)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            grape_inserts,
            page_size=1000,
        )
        conn.commit()
        inserted = cur.rowcount
        print(f"  Inserted: {inserted}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
