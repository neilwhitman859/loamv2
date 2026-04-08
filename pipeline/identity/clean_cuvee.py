"""
Extract cuvée from raw wine names.

Implements the cuvée extraction algorithm from docs/IDENTITY_RULES.md Section 3.
Strips producer, appellation, grape, classification, color, vintage, and noise words
from raw wine names. What remains is the cuvée (or NULL).

Usage:
    python -m pipeline.identity.clean_cuvee --test  # run on sample names
    python -m pipeline.identity.clean_cuvee --wine-id UUID  # single wine
"""
import argparse
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
]

COLOR_WORDS = [
    "Rouge", "Blanc", "Rosé", "Rose", "Rosato",
    "Tinto", "Blanco", "Bianco", "Rosso",
    "Red", "White",
]

NOISE_WORDS = [
    "Estate", "Vineyards", "Vineyard", "Winery", "Cellars",
    "Wine", "Wines", "Cuvée", "Cuvee",
]


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

    Args:
        raw_name: Wine name from staging data
        producer_name: Already-matched producer name
        appellation_name: Already-matched appellation (may be None)
        grape_names: Known grape variety names
        country_code: ISO 3166-1 alpha-2 country code

    Returns:
        Extracted cuvée string, or None if the wine has no cuvée.
    """
    raise NotImplementedError("Session 3")


def validate_cuvee(cuvee: Optional[str], producer_name: str) -> Optional[str]:
    """
    Sanity-check an extracted cuvée.

    Rejects cuvées that are:
        - Country names
        - Region names (unless genuinely part of wine name)
        - Standalone grape names
        - > 50 chars (probably incomplete stripping)
        - Same as producer name

    Returns cleaned cuvée or None.
    """
    raise NotImplementedError("Session 3")


def main():
    parser = argparse.ArgumentParser(description="Extract cuvée from wine names")
    parser.add_argument("--test", action="store_true", help="Run on test samples")
    parser.add_argument("--wine-id", help="Process single wine by UUID")
    args = parser.parse_args()

    if args.test:
        # Test cases from IDENTITY_RULES.md
        test_cases = [
            ("Monte Bello Cabernet Sauvignon", "Ridge Vineyards", None, ["Cabernet Sauvignon"], "US"),
            ("Puligny-Montrachet Premier Cru Les Folatières", "Domaine Leflaive", "Puligny-Montrachet", [], "FR"),
            ("Cabernet Sauvignon", "Barefoot", None, ["Cabernet Sauvignon"], "US"),
            ("Monfortino Barolo Riserva", "Giacomo Conterno", "Barolo", [], "IT"),
            ("Viña Tondonia Reserva", "López de Heredia", "Rioja", [], "ES"),
            ("Wehlener Sonnenuhr Riesling Spätlese", "J.J. Prüm", "Mosel", ["Riesling"], "DE"),
        ]
        for raw, prod, app, grapes, cc in test_cases:
            result = extract_cuvee(raw, prod, app, grapes, cc)
            print(f"  {raw!r} → {result!r}")


if __name__ == "__main__":
    main()
