"""
Build display names from wine identity components.

Implements country-aware display name patterns from docs/IDENTITY_RULES.md Section 2.
Deterministic: same inputs always produce same output.

Usage:
    python -m pipeline.identity.build_display_name --test  # run on examples
    python -m pipeline.identity.build_display_name --recompute [--dry-run]  # rebuild all
"""
import argparse
from typing import Optional


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

    Args:
        producer_name: Canonical producer name
        cuvee: wines.name (the cuvée, may be None)
        primary_grape: Name of dominant grape for display
        appellation_name: Appellation name
        region_name: Region name (fallback if no appellation)
        country_code: ISO 3166-1 alpha-2
        classification: Highest-rank label designation
        wine_type: 'table', 'sparkling', 'fortified'
        vineyard_name: For German/Burgundy vineyard sites

    Returns:
        Formatted display name string.
    """
    raise NotImplementedError("Session 3")


def _build_france(producer_name, cuvee, primary_grape, appellation_name,
                  region_name, classification, wine_type, vineyard_name) -> str:
    """French display name: appellation-driven."""
    raise NotImplementedError("Session 3")


def _build_usa(producer_name, cuvee, primary_grape, appellation_name,
               region_name, classification, wine_type) -> str:
    """US display name: varietal-driven."""
    raise NotImplementedError("Session 3")


def _build_italy(producer_name, cuvee, primary_grape, appellation_name,
                 region_name, classification, wine_type) -> str:
    """Italian display name: appellation + cuvée."""
    raise NotImplementedError("Session 3")


def _build_spain(producer_name, cuvee, primary_grape, appellation_name,
                 region_name, classification, wine_type) -> str:
    """Spanish display name: classification-centric."""
    raise NotImplementedError("Session 3")


def _build_germany(producer_name, cuvee, primary_grape, appellation_name,
                   region_name, classification, wine_type, vineyard_name) -> str:
    """German display name: grape + vineyard + Prädikat."""
    raise NotImplementedError("Session 3")


def _build_varietal_region(producer_name, cuvee, primary_grape,
                           appellation_name, region_name, classification) -> str:
    """AU/NZ/AR/CL/ZA pattern: Producer [Cuvée] Grape, Region."""
    raise NotImplementedError("Session 3")


def _build_fallback(producer_name, cuvee, primary_grape,
                    appellation_name, region_name) -> str:
    """Fallback for all other countries."""
    raise NotImplementedError("Session 3")


def main():
    parser = argparse.ArgumentParser(description="Build display names")
    parser.add_argument("--test", action="store_true", help="Run on examples")
    parser.add_argument("--recompute", action="store_true", help="Rebuild all")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()

    if args.test:
        examples = [
            ("Château Margaux", None, None, "Margaux", None, "FR", None, None, None),
            ("Ridge Vineyards", "Monte Bello", "Cabernet Sauvignon", "Santa Cruz Mountains", None, "US", None, None, None),
            ("Giacomo Conterno", "Monfortino", None, "Barolo", None, "IT", "Riserva", None, None),
            ("López de Heredia", "Viña Tondonia", None, "Rioja", None, "ES", "Reserva", None, None),
            ("J.J. Prüm", None, "Riesling", "Mosel", None, "DE", "Spätlese", None, "Wehlener Sonnenuhr"),
            ("Barefoot", None, "Cabernet Sauvignon", None, None, "US", None, None, None),
        ]
        for ex in examples:
            result = build_display_name(*ex)
            print(f"  {result}")


if __name__ == "__main__":
    main()
