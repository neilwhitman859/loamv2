"""
Wine and producer name cleanup script.

Fixes 4 categories of data quality issues in canonical name fields:
1. HTML entities (&amp; &#8217; &quot; etc.) → decoded characters
2. Whitespace (double spaces, tabs, newlines, leading/trailing) → normalized
3. Wally's-style suffixes ("  2020 / 750 ml.") → stripped from wine names
4. U+FFFD encoding corruption (Latin-1/Win-1252 → UTF-8 mangling) → correct accents
5. Curly Unicode quotes → straight quotes

Usage:
    python -m pipeline.analyze.name_cleanup                  # dry-run (default)
    python -m pipeline.analyze.name_cleanup --execute        # apply changes
    python -m pipeline.analyze.name_cleanup --table wines     # wines only
    python -m pipeline.analyze.name_cleanup --table producers # producers only
"""

import argparse
import html
import re
import sys

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify

# ---------------------------------------------------------------------------
# U+FFFD repair dictionary
# Built programmatically from correct wine terms. For each word, any character
# in Latin-1 range 0x80-0xFF is replaced with U+FFFD to generate the corrupted
# key. This eliminates manual key-construction errors.
# ---------------------------------------------------------------------------

FFFD = "\ufffd"


def _corrupted_key(correct_word: str) -> str:
    """Generate the corrupted form of a word by replacing Latin-1 high bytes with FFFD."""
    result = []
    for c in correct_word:
        cp = ord(c)
        # Latin-1 high range (0x80-0xFF) — these are the bytes that get corrupted
        if 0x80 <= cp <= 0xFF:
            result.append(FFFD)
        else:
            result.append(c)
    return "".join(result).lower()


# Master list of correct wine terms with accented characters.
# The corrupted key is auto-generated, so no manual key construction needed.
_CORRECT_WORDS = [
    # === French wine terms ===
    "Cuvée", "Réserve", "Château", "Côte", "Côt", "Côté", "Côtes",
    "Rosé", "Rosée", "Fumé", "Classé", "Dosé", "Forté", "Naturé",
    "Doré", "Dorés", "Brûlées", "Brûlée", "Prieuré", "Densité",
    "Millésime", "Millésimé", "Pétillant", "Pét-Nat", "Pét",
    "Crémant", "Mélange", "Méthode", "Récolte", "Macération",
    "Sélection", "Héritage", "Impérial", "Prélude", "Découvrir",
    "Fût", "Saignée", "Montée", "Romanée", "Coulée", "Corvée",
    "Corvées", "Cépages", "Cépage", "Zéro", "Déduits", "Damnés",
    "Vallée", "Spéciale", "Pièce", "Ancêtres", "Sémillon",
    "Léon", "Maréchal", "N°",

    # French vineyard / lieu-dit names
    "Perrières", "Perrière", "Genevrières", "Genevrière",
    "Folatières", "Argillières", "Enseignères", "Barrières",
    "Bussière", "Chatenière", "Lavières", "Chaumées", "Chaumés",
    "Derrière", "Grèves", "Pézerolles", "Épenots", "Fuées",
    "Vaudésir", "Très", "Père", "Chênes", "Chêne", "Tête",
    "D'Eloïse", "D'Âge", "D'Initiés", "Bolorée",

    # === German wine terms ===
    "Spätlese", "Spätburgunder", "Grüner", "Gewürztraminer",
    "Blaufränkisch", "Müller-Thurgau", "Qualitätswein",
    "Fünf", "Ürziger", "Iphöfer", "Goldtröpfchen", "Würzgarten",
    "Prädikatswein", "Trockenbeerenauslese",

    # === Spanish wine terms ===
    "Albariño", "Viña", "Viñas", "Viñedo", "Viñedos",
    "Pequeñas", "Selección", "Generación", "Expresión",
    "Terruño", "Añón", "Años", "Alegría", "Sueño",
    "Mencía", "Río",

    # === Portuguese wine terms ===
    "Souzão", "Fernão",

    # === Italian ===
    "Masút",

    # === French — additional terms from missed words ===
    "Perpétuelle", "Crayères", "Élégance", "Liberté", "Musqué",
    "Résonance", "Alatré", "Brossé", "Pajé",
    "Olé", "Brézé", "Rosé-Pamplemousse",
    "Fräulein", "Métier", "Étoile", "Fête", "Forêt",
    "Soirée", "Pré", "Boutières", "Gryphées",
    "Lolières", "Monsnières",
    "Caché", "Aimé", "François", "Édition", "Mémoire",
    "Crème", "Försterlay", "Fûts", "Pères", "Señor",
    "Été", "Rhône",

    # === Spanish — additional ===
    "Colección", "Edición", "Castelão", "Montaña", "Doña",
    "Otoñal", "Bendición",

    # === Portuguese — additional ===
    "Sousão", "Gestação", "Chão",

    # === Grape varieties & general wine terms ===
    "Mourvèdre", "Carmenère", "Aligoté", "Valdiguié", "Vaccarèse",

    # === Producer names ===
    "Méo-Camuzet", "Frères", "Prüm", "Chanterêves", "Terruño",
]

# Also add words that have non-standard corruptions or alternate forms.
# These handle: truncated words, alternate spellings, partial accents
# (where TTB stored only some accents), and specific edge cases.
_EXTRA_REPAIRS = {
    # Truncated or alternate forms
    "cuv\ufffd": "Cuvé",
    "cuve\ufffd": "Cuvée",
    "p\ufffdt": "Pét",
    "f\ufffdt": "Fût",
    "c\ufffdt": "Côt",
    "n\ufffd": "N°",
    "blaufr\ufffdnkish": "Blaufränkisch",

    # Partial accents — TTB didn't store all accents, only some got corrupted
    # Brûlées: TTB stored BRULEES (no û), only é corrupted
    "brul\ufffdes": "Brulées",
    "brul\ufffde": "Brulée",
    # Élégance: TTB stored ELEGANCE, only the interior é corrupted
    "el\ufffdgance": "Elégance",
    # Brézé: one or both é's corrupted
    "brez\ufffd": "Brezé",
    "br\ufffdze": "Brézé",  # only first é

    # Additional French terms from missed words
    "caf\ufffd": "Café",
    "ann\ufffde": "Année",
    "particuli\ufffdre": "Particulière",
    "cumi\ufffdres": "Cumières",
    "gravi\ufffdres": "Gravières",
    "esp\ufffdrance": "Espérance",
    "\ufffdlev\ufffd": "Élevé",
    "ren\ufffd": "René",
    "perp\ufffdtuelle": "Perpétuelle",
    "rh\ufffdne": "Rhône",
    "m\ufffds": "Més",
    "pi\ufffd": "Pié",
    "mand\ufffd": "Mandé",

    # Müller standalone (without -Thurgau)
    "m\ufffdller": "Müller",

    # Portuguese
    "alvarelh\ufffdo": "Alvarelhão",
    "c\ufffdo": "ção",

    # Spanish
    "a\ufffdo": "año",
    "colecci\ufffdn": "Colección",
    "edici\ufffdn": "Edición",

    # ¡ (inverted exclamation, 0xA1 in Latin-1)
    "\ufffdsalud": "¡Salud",

    # Kuv?e — alternate spelling of Cuvée (German/Hungarian)
    "kuv\ufffde": "Kuvée",

    # Partial-accent variants (TTB stored some accents as plain ASCII)
    "bros\ufffd": "Brosé",
    "ser\ufffd": "Seré",
    "a\ufffdon": "Añon",  # Añón with only ñ corrupted (ó stored plain)
    "selecci\ufffd": "Selecció",  # truncated

    # Windows-1252 corrupted apostrophes (0x92 → FFFD)
    "maker\ufffds": "Maker's",
    "l\ufffdabbaye": "L'Abbaye",
    "l\ufffdam\ufffdrique": "L'Amérique",
    "d\ufffdor": "D'Or",
    "d\ufffdargent": "D'Argent",
}

# Build the lookup dictionary programmatically
_WORD_REPAIRS_LOWER = {}
for word in _CORRECT_WORDS:
    key = _corrupted_key(word)
    if key != word.lower():  # only add if it actually has a corruption
        _WORD_REPAIRS_LOWER[key] = word

# Add extra manual entries
for key, value in _EXTRA_REPAIRS.items():
    _WORD_REPAIRS_LOWER[key.lower()] = value


def _match_case(replacement: str, original: str) -> str:
    """Match the case pattern of the original word to the replacement."""
    stripped = original.rstrip(",;:.)!?")
    # Filter out FFFD chars for case detection (they're not letters)
    alpha_chars = [c for c in stripped if c != FFFD and c.isalpha()]
    if not alpha_chars:
        return replacement
    if all(c.isupper() for c in alpha_chars):
        return replacement.upper()
    elif all(c.islower() for c in alpha_chars):
        return replacement.lower()
    return replacement  # mixed case — use dictionary form (title-cased)


def repair_fffd_word(word: str) -> str:
    """Try to repair a word containing U+FFFD using the dictionary."""
    key = word.lower().rstrip(",;:.)!?")
    trailing = word[len(word.rstrip(",;:.)!?")):]
    if key in _WORD_REPAIRS_LOWER:
        repaired = _match_case(_WORD_REPAIRS_LOWER[key], word)
        return repaired + trailing
    return word  # return unchanged if not in dictionary


def repair_fffd(name: str) -> str:
    """Repair U+FFFD characters in a name using word-level dictionary lookup."""
    if FFFD not in name:
        return name

    words = name.split()
    result = []
    for word in words:
        if FFFD in word:
            repaired = repair_fffd_word(word)
            # If word-level lookup didn't fix it, try pattern-based repairs
            if FFFD in repaired:
                # N° + digits pattern (N°1, N°29, etc.)
                repaired = re.sub(
                    r"(?i)n" + FFFD + r"(\d+)",
                    lambda m: "N°" + m.group(1), repaired
                )
            if FFFD in repaired:
                # Degree sign after digits (65°, 45°, etc.)
                repaired = re.sub(
                    r"(\d+)" + FFFD + r"$",
                    lambda m: m.group(1) + "°", repaired
                )
            if FFFD in repaired:
                # Windows-1252 apostrophe: possessive 's pattern
                repaired = re.sub(
                    FFFD + r"([sS])(?=[,;:\s]|$)",
                    r"'\1", repaired
                )
            if FFFD in repaired:
                # Windows-1252 apostrophe: d' / l' + uppercase start
                repaired = re.sub(
                    r"(?i)([dl])" + FFFD + r"([A-Z])",
                    r"\1'\2", repaired
                )
            result.append(repaired)
        else:
            result.append(word)
    return " ".join(result)


# ---------------------------------------------------------------------------
# HTML entity decode
# ---------------------------------------------------------------------------

def decode_html_entities(name: str) -> str:
    """Decode HTML entities: &amp; &#8217; &quot; etc."""
    if "&" not in name:
        return name
    return html.unescape(name)


# ---------------------------------------------------------------------------
# Whitespace normalization
# ---------------------------------------------------------------------------

def normalize_whitespace(name: str) -> str:
    """Collapse double spaces, strip tabs/newlines, trim."""
    name = name.replace("\t", " ").replace("\n", " ").replace("\r", " ")
    name = re.sub(r"  +", " ", name)
    return name.strip()


# ---------------------------------------------------------------------------
# Wally's suffix stripping
# ---------------------------------------------------------------------------

# Matches patterns like "  2020 / 750 ml." or "  NV / 750 ml." or "  2021 /" at end
WALLY_SUFFIX_RE = re.compile(
    r"\s+(?:(?:19|20)\d{2}|NV)\s*/\s*(?:\d+(?:\.\d+)?\s*(?:ml|l|L)\s*\.?\s*)?$",
    re.IGNORECASE
)


def strip_wally_suffix(name: str) -> str:
    """Remove Wally's-style vintage/format suffixes from wine names."""
    return WALLY_SUFFIX_RE.sub("", name)


# ---------------------------------------------------------------------------
# Curly quote normalization
# ---------------------------------------------------------------------------

CURLY_QUOTES = {
    "\u2018": "'",  # left single
    "\u2019": "'",  # right single
    "\u201c": '"',  # left double
    "\u201d": '"',  # right double
}


def normalize_curly_quotes(name: str) -> str:
    """Replace curly/smart quotes with straight equivalents."""
    for curly, straight in CURLY_QUOTES.items():
        name = name.replace(curly, straight)
    return name


# ---------------------------------------------------------------------------
# Main cleanup pipeline
# ---------------------------------------------------------------------------

def clean_name(name: str, is_wine: bool = True) -> str:
    """Apply all cleanup passes to a name. Returns cleaned name."""
    if not name:
        return name

    result = name

    # Pass 1: HTML entities
    result = decode_html_entities(result)

    # Pass 2: Curly quotes
    result = normalize_curly_quotes(result)

    # Pass 3: Wally's suffix (wines only)
    if is_wine:
        result = strip_wally_suffix(result)

    # Pass 4: U+FFFD repair
    result = repair_fffd(result)

    # Pass 5: Whitespace (always last before return)
    result = normalize_whitespace(result)

    return result


def run_cleanup(table: str, dry_run: bool = True, limit: int = 0):
    """Run the cleanup on a table. Returns (changed, skipped, total) counts."""
    conn = get_conn()
    is_wine = table == "wines"

    try:
        with conn.cursor() as cur:
            # Fetch all rows that might need cleaning
            conditions = []
            conditions.append(f"name LIKE '%{FFFD}%'")
            conditions.append("name LIKE '%  %'")
            conditions.append("name LIKE '%&amp;%' OR name LIKE '%&quot;%' OR name LIKE '%&#%'")
            conditions.append("name != trim(name)")
            conditions.append(r"name ~ E'[\t\n\r]'")

            # Curly quotes
            for q in CURLY_QUOTES:
                conditions.append(f"name LIKE '%{q}%'")

            # Wally's suffixes (wines only)
            if is_wine:
                conditions.append(r"name ~ '\d{4}\s*/\s*(\d+\s*(ml|l|L))?\s*\.?\s*$'")
                conditions.append(r"name ~ 'NV\s*/\s*(\d+\s*(ml|l|L))?\s*\.?\s*$'")

            where = " OR ".join(f"({c})" for c in conditions)
            query = f"SELECT id, name FROM {table} WHERE {where}"
            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query)
            rows = cur.fetchall()

        total = len(rows)
        changes = []  # (id, old_name, new_name)
        unchanged = 0

        for row_id, old_name in rows:
            new_name = clean_name(old_name, is_wine=is_wine)
            if new_name != old_name:
                changes.append((row_id, old_name, new_name))
            else:
                unchanged += 1

        # Print summary
        print(f"\n{'=' * 70}")
        print(f"  {table.upper()} NAME CLEANUP {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
        print(f"{'=' * 70}")
        print(f"  Rows scanned:   {total}")
        print(f"  Will change:    {len(changes)}")
        print(f"  No change:      {unchanged}")
        print()

        # Categorize changes for reporting
        stats = {
            "html_decode": 0,
            "whitespace": 0,
            "wally_suffix": 0,
            "fffd_repair": 0,
            "fffd_remaining": 0,
            "curly_quotes": 0,
        }

        for row_id, old, new in changes:
            if "&" in old and ("&#" in old or "&amp;" in old or "&quot;" in old):
                stats["html_decode"] += 1
            if re.search(r"  |\t|\n|\r", old) or old != old.strip():
                stats["whitespace"] += 1
            if is_wine and WALLY_SUFFIX_RE.search(old):
                stats["wally_suffix"] += 1
            if FFFD in old:
                if FFFD in new:
                    stats["fffd_remaining"] += 1
                else:
                    stats["fffd_repair"] += 1
            if any(q in old for q in CURLY_QUOTES):
                stats["curly_quotes"] += 1

        print("  Breakdown (overlapping):")
        for cat, cnt in stats.items():
            if cnt:
                print(f"    {cat}: {cnt}")
        print()

        # Show samples
        def safe_print(s):
            """Print with fallback for Windows console encoding."""
            try:
                print(s)
            except UnicodeEncodeError:
                # Replace non-ASCII chars with ? for display
                print("".join(c if ord(c) < 128 else "?" for c in s))

        safe_print("  Sample changes (first 25):")
        for i, (row_id, old, new) in enumerate(changes[:25]):
            # Truncate for display
            old_disp = old[:60] + "..." if len(old) > 60 else old
            new_disp = new[:60] + "..." if len(new) > 60 else new
            safe_print(f"    {old_disp}")
            safe_print(f"      -> {new_disp}")
            print()

        # Show remaining U+FFFD that couldn't be fixed
        remaining_fffd = [(rid, old, new) for rid, old, new in changes if FFFD in new]
        unfixed_fffd = [(rid, old, old) for rid, old in rows
                        if FFFD in old and old == clean_name(old, is_wine)]
        remaining_fffd.extend(unfixed_fffd)

        if remaining_fffd:
            safe_print(f"\n  Remaining U+FFFD (not in dictionary): {len(remaining_fffd)}")
            # Extract unique corrupted words not in dictionary
            missed_words = {}
            for _, old, new in remaining_fffd:
                for word in (new if new != old else old).split():
                    if FFFD in word:
                        key = word.lower().rstrip(",;:.)!?")
                        missed_words[key] = missed_words.get(key, 0) + 1
            for word, cnt in sorted(missed_words.items(), key=lambda x: -x[1])[:30]:
                safe_print(f"      {word} ({cnt})")

        if not dry_run and changes:
            print(f"\n  Applying {len(changes)} changes...")
            updated = 0
            slug_conflicts = 0
            with conn.cursor() as cur:
                for row_id, old_name, new_name in changes:
                    new_norm = normalize(new_name)
                    new_slug = slugify(new_name)
                    try:
                        cur.execute(f"""
                            UPDATE {table}
                            SET name = %s, name_normalized = %s,
                                slug = %s, updated_at = now()
                            WHERE id = %s
                        """, (new_name, new_norm, new_slug, str(row_id)))
                        updated += 1
                    except Exception:
                        conn.rollback()
                        # Slug conflict — append disambiguator
                        try:
                            cur.execute(f"""
                                UPDATE {table}
                                SET name = %s, name_normalized = %s,
                                    slug = %s, updated_at = now()
                                WHERE id = %s
                            """, (new_name, new_norm, new_slug + "-2", str(row_id)))
                            updated += 1
                            slug_conflicts += 1
                        except Exception:
                            conn.rollback()
                            # Last resort: update name only, skip slug
                            cur.execute(f"""
                                UPDATE {table}
                                SET name = %s, name_normalized = %s,
                                    updated_at = now()
                                WHERE id = %s
                            """, (new_name, new_norm, str(row_id)))
                            updated += 1
                            slug_conflicts += 1

                    if updated % 1000 == 0:
                        conn.commit()
                        print(f"    Updated {updated}...")

                conn.commit()
            print(f"\n  Done. {updated} rows updated.")
            if slug_conflicts:
                print(f"  {slug_conflicts} slug conflicts resolved.")
            print("  updated_at bumped → search_vector triggers will re-fire.")
        elif not changes:
            print("  Nothing to change.")

        return len(changes), unchanged, total

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Clean up wine and producer names")
    parser.add_argument("--execute", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--table", choices=["wines", "producers", "both"],
                        default="both", help="Which table to clean")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit rows to process (for testing)")
    args = parser.parse_args()

    tables = ["wines", "producers"] if args.table == "both" else [args.table]
    grand_total = 0

    for table in tables:
        changed, _, _ = run_cleanup(table, dry_run=not args.execute, limit=args.limit)
        grand_total += changed

    if not args.execute and grand_total > 0:
        print(f"\n  Re-run with --execute to apply {grand_total} changes.")


if __name__ == "__main__":
    main()
