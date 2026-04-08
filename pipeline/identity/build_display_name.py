"""
Build display names from wine identity components.

Implements country-aware display name patterns from docs/IDENTITY_RULES.md Section 2.
Deterministic: same inputs always produce same output.

Usage:
    python -m pipeline.identity.build_display_name --test  # run on examples
"""
from typing import Optional


# Country codes that use specific patterns
FRANCE = "FR"
USA = "US"
ITALY = "IT"
SPAIN = "ES"
GERMANY = "DE"
PORTUGAL = "PT"
AUSTRALIA = "AU"
NEW_ZEALAND = "NZ"
ARGENTINA = "AR"
CHILE = "CL"
SOUTH_AFRICA = "ZA"
AUSTRIA = "AT"
GREECE = "GR"

VARIETAL_REGION_COUNTRIES = {AUSTRALIA, NEW_ZEALAND, ARGENTINA, CHILE, SOUTH_AFRICA}

# Alsace appellation names (grape appears in display name for these)
ALSACE_NAMES = {"alsace", "alsace grand cru"}


def build_display_name(
    producer_name: str,
    cuvee: Optional[str],
    primary_grape: Optional[str],
    appellation_name: Optional[str],
    region_name: Optional[str],
    country_code: str,
    classification: Optional[str] = None,
    wine_type: Optional[str] = None,
    vineyard_name: Optional[str] = None,
) -> str:
    """
    Compute display name from identity components.

    Country patterns (from IDENTITY_RULES.md Section 2):
        FR: Producer, Appellation [Classification] [Cuvée]
        US: Producer [Cuvée] [Grape], Appellation
        IT: Producer [Cuvée], Appellation [Classification]
        ES: Producer [Cuvée] [Classification], Appellation
        DE: Producer [Vineyard] Grape [Prädikat], Region
        PT: Producer [Cuvée] [Classification], Appellation
        AU/NZ/AR/CL/ZA: Producer [Cuvée] Grape, Region
        AT: Producer Grape [Classification], DAC/Region
        GR: Producer [Cuvée] [Grape], Appellation
        Fallback: Producer [Cuvée], Appellation [Grape]
    """
    cc = (country_code or "").upper()

    if cc == FRANCE:
        return _build_france(producer_name, cuvee, primary_grape, appellation_name,
                             region_name, classification, wine_type, vineyard_name)
    elif cc == USA:
        return _build_usa(producer_name, cuvee, primary_grape, appellation_name,
                          region_name, classification, wine_type)
    elif cc == ITALY:
        return _build_italy(producer_name, cuvee, primary_grape, appellation_name,
                            region_name, classification, wine_type)
    elif cc == SPAIN:
        return _build_spain(producer_name, cuvee, primary_grape, appellation_name,
                            region_name, classification, wine_type)
    elif cc == GERMANY:
        return _build_germany(producer_name, cuvee, primary_grape, appellation_name,
                              region_name, classification, wine_type, vineyard_name)
    elif cc == PORTUGAL:
        return _build_portugal(producer_name, cuvee, primary_grape, appellation_name,
                               region_name, classification, wine_type)
    elif cc in VARIETAL_REGION_COUNTRIES:
        return _build_varietal_region(producer_name, cuvee, primary_grape,
                                      appellation_name, region_name, classification)
    elif cc == AUSTRIA:
        return _build_austria(producer_name, cuvee, primary_grape, appellation_name,
                              region_name, classification, wine_type)
    elif cc == GREECE:
        return _build_greece(producer_name, cuvee, primary_grape, appellation_name,
                             region_name, classification)
    else:
        return _build_fallback(producer_name, cuvee, primary_grape,
                               appellation_name, region_name)


def _join(*parts: Optional[str], sep: str = " ") -> str:
    """Join non-None, non-empty parts with separator."""
    return sep.join(p for p in parts if p)


def _geo(appellation_name: Optional[str], region_name: Optional[str]) -> Optional[str]:
    """Get the best geographic designation available."""
    return appellation_name or region_name


def _build_france(producer_name, cuvee, primary_grape, appellation_name,
                  region_name, classification, wine_type, vineyard_name) -> str:
    """French display name: appellation-driven."""
    geo = _geo(appellation_name, region_name)

    # Alsace: include grape
    is_alsace = appellation_name and appellation_name.lower().replace(" ", "").startswith("alsace")
    if is_alsace and primary_grape:
        left = _join(producer_name, primary_grape)
        right = _join(appellation_name, cuvee)
        return f"{left}, {right}" if right else left

    # Champagne: Producer [Cuvée], Champagne
    is_champagne = appellation_name and appellation_name.lower() == "champagne"
    if is_champagne:
        left = _join(producer_name, cuvee)
        return f"{left}, Champagne"

    # Standard French: Producer, Appellation [Classification] [Cuvée]
    right_parts = _join(geo, classification, cuvee)
    if right_parts:
        return f"{producer_name}, {right_parts}"
    return producer_name


def _build_usa(producer_name, cuvee, primary_grape, appellation_name,
               region_name, classification, wine_type) -> str:
    """US display name: varietal-driven."""
    geo = _geo(appellation_name, region_name)

    # Sparkling: Producer [Cuvée], Region
    if wine_type == "sparkling":
        left = _join(producer_name, cuvee)
        return f"{left}, {geo}" if geo else left

    # Standard: Producer [Cuvée] [Grape], Appellation
    left = _join(producer_name, cuvee, primary_grape)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_italy(producer_name, cuvee, primary_grape, appellation_name,
                 region_name, classification, wine_type) -> str:
    """Italian display name: appellation + cuvée."""
    geo = _geo(appellation_name, region_name)

    # Producer [Cuvée], Appellation [Classification]
    left = _join(producer_name, cuvee)
    right = _join(geo, classification)
    if right:
        return f"{left}, {right}"
    return left


def _build_spain(producer_name, cuvee, primary_grape, appellation_name,
                 region_name, classification, wine_type) -> str:
    """Spanish display name: classification-centric."""
    geo = _geo(appellation_name, region_name)

    # Sherry: Producer [Cuvée] Style, Jerez
    # (Sherry style is stored as classification)

    # Standard: Producer [Cuvée] [Classification], Appellation
    left = _join(producer_name, cuvee, classification)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_germany(producer_name, cuvee, primary_grape, appellation_name,
                   region_name, classification, wine_type, vineyard_name) -> str:
    """German display name: grape + vineyard + Prädikat."""
    geo = _geo(appellation_name, region_name)

    # Use cuvee as vineyard if vineyard_name not set but cuvee present
    vy = vineyard_name or cuvee

    # Producer [Vineyard] Grape [Prädikat], Region
    left = _join(producer_name, vy, primary_grape, classification)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_portugal(producer_name, cuvee, primary_grape, appellation_name,
                    region_name, classification, wine_type) -> str:
    """Portuguese display name."""
    geo = _geo(appellation_name, region_name)

    # Port/Madeira: Producer [Cuvée] Style, Porto/Madeira
    # (style is stored as classification)

    # Standard: Producer [Cuvée] [Classification], Appellation
    left = _join(producer_name, cuvee, classification)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_varietal_region(producer_name, cuvee, primary_grape,
                           appellation_name, region_name, classification) -> str:
    """AU/NZ/AR/CL/ZA pattern: Producer [Cuvée] Grape, Region."""
    geo = _geo(appellation_name, region_name)

    left = _join(producer_name, cuvee, primary_grape)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_austria(producer_name, cuvee, primary_grape, appellation_name,
                   region_name, classification, wine_type) -> str:
    """Austrian display name: grape + classification."""
    geo = _geo(appellation_name, region_name)

    # Producer Grape [Classification], DAC/Region
    left = _join(producer_name, primary_grape, classification)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_greece(producer_name, cuvee, primary_grape, appellation_name,
                  region_name, classification) -> str:
    """Greek display name."""
    geo = _geo(appellation_name, region_name)

    # Producer [Cuvée] [Grape], Appellation
    left = _join(producer_name, cuvee, primary_grape)
    if geo:
        return f"{left}, {geo}"
    return left


def _build_fallback(producer_name, cuvee, primary_grape,
                    appellation_name, region_name) -> str:
    """Fallback for all other countries."""
    geo = _geo(appellation_name, region_name)

    # Producer [Cuvée], Appellation [Grape]
    left = _join(producer_name, cuvee)
    right = _join(geo, primary_grape)
    if right:
        return f"{left}, {right}"
    return left


def main():
    """Test display name generation on sample wines."""
    examples = [
        # (producer, cuvee, grape, appellation, region, country, classification, wine_type, vineyard)
        ("Château Margaux", None, None, "Margaux", "Bordeaux", "FR", None, None, None),
        ("Ridge Vineyards", "Monte Bello", "Cabernet Sauvignon", "Santa Cruz Mountains", None, "US", None, None, None),
        ("Giacomo Conterno", "Monfortino", None, "Barolo", None, "IT", "Riserva", None, None),
        ("López de Heredia", "Viña Tondonia", None, "Rioja", None, "ES", "Reserva", None, None),
        ("J.J. Prüm", "Wehlener Sonnenuhr", "Riesling", None, "Mosel", "DE", "Spätlese", None, None),
        ("Barefoot", None, "Cabernet Sauvignon", None, "California", "US", None, None, None),
        ("Penfolds", "Grange", "Shiraz", None, "South Australia", "AU", None, None, None),
        ("Cloudy Bay", None, "Sauvignon Blanc", None, "Marlborough", "NZ", None, None, None),
        ("Catena Zapata", None, "Malbec", None, "Mendoza", "AR", None, None, None),
        ("Gaia", "Thalassitis", "Assyrtiko", "Santorini", None, "GR", None, None, None),
        ("Louis Jadot", None, None, "Gevrey-Chambertin", None, "FR", None, None, None),
        ("Domaine Leflaive", "Les Folatières", None, "Puligny-Montrachet", None, "FR", "Premier Cru", None, None),
        ("Krug", "Grande Cuvée", None, "Champagne", None, "FR", None, "sparkling", None),
        ("Opus One", None, None, None, "Napa Valley", "US", None, None, None),
        ("Niepoort", "Redoma", None, "Douro", None, "PT", None, None, None),
        ("F.X. Pichler", None, "Riesling", None, "Wachau", "AT", "Smaragd", None, None),
    ]

    print("=== Display Name Tests ===\n")
    for ex in examples:
        result = build_display_name(*ex)
        print(f"  {result}")


if __name__ == "__main__":
    main()
