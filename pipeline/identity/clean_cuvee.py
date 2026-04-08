"""
Extract cuvée from raw wine names.

Implements the cuvée extraction algorithm from docs/IDENTITY_RULES.md Section 3.
Strips producer, appellation, grape, classification, color, vintage, and noise words
from raw wine names. What remains is the cuvée (or NULL).

Usage:
    python -m pipeline.identity.clean_cuvee --test  # run on sample names
"""
import re
import unicodedata
from typing import Optional


# Classification keywords to strip (see IDENTITY_RULES.md Section 3)
CLASSIFICATIONS_FR = [
    "Premier Cru", "1er Cru", "Grand Cru", "Cru Bourgeois",
    "Cru Classé", "Supérieur", "Villages",
]
CLASSIFICATIONS_IT = [
    "Riserva", "Gran Selezione", "Superiore", "Classico",
    "Passito", "Recioto", "Ripasso", "Sforzato", "Sfursat",
]
CLASSIFICATIONS_ES = [
    "Joven", "Roble", "Crianza", "Reserva", "Gran Reserva",
    "Vendimia Seleccionada",
]
CLASSIFICATIONS_DE = [
    "Kabinett", "Spätlese", "Auslese", "Beerenauslese",
    "Trockenbeerenauslese", "Eiswein", "Trocken", "Halbtrocken",
    "Feinherb", "GG", "Grosses Gewächs", "Erste Lage",
]
CLASSIFICATIONS_AT = ["Smaragd", "Federspiel", "Steinfeder"]
CLASSIFICATIONS_PT = [
    "Reserva", "Garrafeira", "Grande Reserva", "Colheita",
    "Late Bottled Vintage", "LBV", "Vintage", "Tawny", "Ruby",
]
CLASSIFICATIONS_GENERAL = [
    "Reserve", "Old Vine", "Old Vines", "Vieilles Vignes",
    "Barrel Select", "Single Vineyard", "Limited Edition",
    "Brut", "Extra Brut", "Brut Nature", "Extra Dry",
    "Sec", "Demi-Sec", "Doux",
    "Blanc de Blancs", "Blanc de Noirs",
    "Cuvée Prestige",
]

# All classifications merged, sorted longest-first for greedy matching
ALL_CLASSIFICATIONS = sorted(
    CLASSIFICATIONS_FR + CLASSIFICATIONS_IT + CLASSIFICATIONS_ES +
    CLASSIFICATIONS_DE + CLASSIFICATIONS_AT + CLASSIFICATIONS_PT +
    CLASSIFICATIONS_GENERAL,
    key=len, reverse=True,
)

COLOR_WORDS = [
    "Rouge", "Blanc", "Rosé", "Rose", "Rosato",
    "Tinto", "Blanco", "Bianco", "Rosso",
    "Red", "White",
]

NOISE_WORDS = [
    "Estate", "Vineyards", "Vineyard", "Winery", "Cellars",
    "Wine", "Wines",
]


def _strip_accents(s: str) -> str:
    """Strip accents for comparison while preserving other characters."""
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def _strip_word_boundary(text: str, word: str) -> str:
    """Remove word from text using word-boundary matching (case-insensitive)."""
    # Escape regex special chars in the word
    escaped = re.escape(word)
    # Match with word boundaries, case-insensitive
    pattern = r'(?<!\w)' + escaped + r'(?!\w)'
    return re.sub(pattern, '', text, flags=re.IGNORECASE).strip()


def _strip_phrase(text: str, phrase: str) -> str:
    """Remove phrase from text, trying both accented and unaccented forms."""
    result = _strip_word_boundary(text, phrase)
    if result != text:
        return result
    # Try unaccented comparison
    unaccented_phrase = _strip_accents(phrase)
    if unaccented_phrase != phrase:
        result = _strip_word_boundary(text, unaccented_phrase)
    return result


def extract_classifications(raw_name: str, country_code: Optional[str] = None) -> tuple[str, list[str]]:
    """
    Extract classification keywords from a wine name.

    Returns (remaining_name, list_of_classifications_found).
    """
    found = []
    result = raw_name

    # Use country-specific first, then general
    for cls in ALL_CLASSIFICATIONS:
        before = result
        result = _strip_phrase(result, cls)
        if result != before:
            found.append(cls)

    return result.strip(), found


def extract_cuvee(
    raw_name: str,
    producer_name: str,
    appellation_name: Optional[str] = None,
    grape_names: Optional[list[str]] = None,
    country_code: Optional[str] = None,
) -> Optional[str]:
    """
    Extract cuvée from a raw wine name by stripping known components.

    Algorithm (from IDENTITY_RULES.md Section 3):
        1. Normalize for matching (preserve original case for output)
        2. Strip producer name
        3. Strip appellation name
        4. Strip grape names
        5. Strip classification keywords (country-aware)
        6. Strip color words
        7. Strip vintage year patterns
        8. Strip noise words
        9. Clean whitespace and punctuation
        10. If empty → return None. Otherwise → cuvée.
    """
    if not raw_name:
        return None

    result = raw_name.strip()

    # 1. Work with the text directly (preserve case)

    # 2. Strip producer name (and common prefix/suffix variants)
    result = _strip_phrase(result, producer_name)
    # Also strip without common prefixes/suffixes
    stripped_prod = producer_name
    for prefix in ["Château ", "Chateau ", "Domaine ", "Bodegas ", "Weingut ",
                    "Tenuta ", "Casa ", "Cantina ", "Maison ", "Bodega "]:
        if stripped_prod.lower().startswith(prefix.lower()):
            stripped_prod = stripped_prod[len(prefix):]
            result = _strip_phrase(result, stripped_prod)
            break
    for suffix in [" Winery", " Vineyards", " Vineyard", " Estate",
                   " Cellars", " Wines", " Wine Company", " Wine Co"]:
        if stripped_prod.lower().endswith(suffix.lower()):
            stripped_prod = stripped_prod[:-len(suffix)]
            result = _strip_phrase(result, stripped_prod)
            break

    # 3. Strip appellation name
    if appellation_name:
        result = _strip_phrase(result, appellation_name)
        # Also strip "AOC/AOP/DOC/DOCG/DO/AVA" suffixes after appellation
        result = re.sub(r'\b(AOC|AOP|DOC|DOCG|DO|AVA|IGP|IGT|VDP|WO|DAC)\b', '', result, flags=re.IGNORECASE)

    # 4. Strip grape names (longest first to avoid partial matches)
    if grape_names:
        for grape in sorted(grape_names, key=len, reverse=True):
            result = _strip_phrase(result, grape)

    # 5. Strip classifications
    result, _ = extract_classifications(result, country_code)

    # 6. Strip color words — only at start or end to preserve names like "Pavillon Blanc"
    for color in COLOR_WORDS:
        # Strip at start
        pattern_start = r'^' + re.escape(color) + r'(?!\w)'
        result = re.sub(pattern_start, '', result, flags=re.IGNORECASE).strip()
        # Strip at end
        pattern_end = r'(?<!\w)' + re.escape(color) + r'$'
        result = re.sub(pattern_end, '', result, flags=re.IGNORECASE).strip()

    # 7. Strip vintage year patterns
    result = re.sub(r'\b(19|20)\d{2}\b', '', result)

    # 8. Strip noise words
    for noise in NOISE_WORDS:
        result = _strip_word_boundary(result, noise)

    # 9. Clean whitespace, dangling connectors, and punctuation
    result = re.sub(r'\s+', ' ', result).strip()
    # Remove dangling French/Italian/Spanish/Portuguese connectors at start or end
    result = re.sub(r'^(du|de|des|del|di|da|della|delle|dei|degli|das|dos|do)\s+', '', result, flags=re.IGNORECASE)
    result = re.sub(r'\s+(du|de|des|del|di|da|della|delle|dei|degli|das|dos|do)$', '', result, flags=re.IGNORECASE)
    # Strip leading/trailing punctuation (commas, dashes, dots, parens)
    result = re.sub(r'^[\s,\-.\(\)]+', '', result)
    result = re.sub(r'[\s,\-.\(\)]+$', '', result)
    result = result.strip()

    # 10. If empty or just a single article/connector → None
    if not result:
        return None
    # Single short connector/article words aren't valid cuvées
    single_word_junk = {
        "du", "de", "des", "del", "di", "da", "das", "dos", "do",
        "le", "la", "les", "il", "lo", "el", "the",
        "et", "and", "e", "y",
        "en", "au", "aux",
        "della", "delle", "dei", "degli", "dal", "dalla",
    }
    if result.lower() in single_word_junk:
        return None

    return validate_cuvee(result, producer_name)


def validate_cuvee(cuvee: Optional[str], producer_name: str) -> Optional[str]:
    """
    Sanity-check an extracted cuvée.

    Rejects cuvées that are:
        - Country names
        - Standalone grape names (handled by caller's grape stripping)
        - > 50 chars (probably incomplete stripping)
        - Same as producer name (fuzzy)
    """
    if not cuvee:
        return None

    # Too long — probably incomplete stripping
    if len(cuvee) > 60:
        return None

    # Same as producer name (case-insensitive, accent-insensitive)
    if _strip_accents(cuvee.lower()) == _strip_accents(producer_name.lower()):
        return None

    # Known country names that shouldn't be cuvées
    country_words = {
        "france", "italy", "spain", "germany", "australia", "united states",
        "portugal", "austria", "new zealand", "argentina", "chile",
        "south africa", "greece", "hungary", "usa",
    }
    if cuvee.lower() in country_words:
        return None

    return cuvee


def main():
    """Test cuvée extraction on sample names."""
    test_cases = [
        # (raw_name, producer_name, appellation_name, grape_names, country_code, expected)
        ("Monte Bello Cabernet Sauvignon", "Ridge Vineyards", "Santa Cruz Mountains",
         ["Cabernet Sauvignon"], "US", "Monte Bello"),
        ("Puligny-Montrachet Premier Cru Les Folatières", "Domaine Leflaive",
         "Puligny-Montrachet", [], "FR", "Les Folatières"),
        ("Cabernet Sauvignon", "Barefoot", None, ["Cabernet Sauvignon"], "US", None),
        ("Monfortino Barolo Riserva", "Giacomo Conterno", "Barolo", [], "IT", "Monfortino"),
        ("Viña Tondonia Reserva", "López de Heredia", "Rioja", [], "ES", "Viña Tondonia"),
        ("Wehlener Sonnenuhr Riesling Spätlese", "J.J. Prüm", "Mosel",
         ["Riesling"], "DE", "Wehlener Sonnenuhr"),
        ("Margaux", "Château Margaux", "Margaux", [], "FR", None),
        ("Pavillon Blanc du Chateau Margaux", "Château Margaux", "Margaux",
         [], "FR", "Pavillon Blanc"),
        ("Grange Shiraz", "Penfolds", None, ["Shiraz"], "AU", "Grange"),
        ("Tignanello", "Marchesi Antinori", "Bolgheri", [], "IT", "Tignanello"),
        ("Geyserville", "Ridge Vineyards", None, [], "US", "Geyserville"),
        ("Viña Tondonia Gran Reserva", "López de Heredia", "Rioja", [], "ES", "Viña Tondonia"),
    ]

    print("=== Cuvée Extraction Tests ===\n")
    pass_count = 0
    for raw, prod, app, grapes, cc, expected in test_cases:
        result = extract_cuvee(raw, prod, app, grapes, cc)
        ok = result == expected
        if ok:
            pass_count += 1
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {raw!r}")
        print(f"    => {result!r}  (expected: {expected!r})")

    print(f"\n{pass_count}/{len(test_cases)} passed")


if __name__ == "__main__":
    main()
