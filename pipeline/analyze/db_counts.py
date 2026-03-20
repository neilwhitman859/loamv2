#!/usr/bin/env python3
"""
Quick row count utility for all major Loam tables.

Usage:
    python -m pipeline.analyze.db_counts
"""

import sys
from pathlib import Path

# Allow running as script or module
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


def main():
    sb = get_supabase()

    # Canonical tables
    canonical = [
        "producers", "wines", "wine_vintages", "wine_vintage_scores",
        "wine_vintage_prices", "wine_grapes", "external_ids",
        "entity_classifications", "winemakers", "wine_aliases",
        "producer_farming_certifications", "wine_label_designations",
        "wine_insights", "wine_vintage_insights",
    ]

    # Reference tables
    reference = [
        "countries", "regions", "appellations", "grapes", "grape_synonyms",
        "publications", "classifications", "classification_levels",
        "label_designations", "attribute_definitions", "tasting_descriptors",
        "farming_certifications", "soil_types", "source_types",
        "appellation_aliases", "region_aliases", "varietal_categories",
    ]

    # Insight tables
    insights = [
        "grape_insights", "region_insights", "appellation_insights",
        "country_insights",
    ]

    # Staging tables
    staging = [
        "source_lwin", "source_pro_platform", "source_tabc", "source_wv_abca",
        "source_kansas_brands", "source_polaner", "source_kermit_lynch",
        "source_kermit_lynch_growers", "source_skurnik", "source_winebow",
        "source_empson", "source_european_cellars", "source_last_bottle",
        "source_best_wine_store", "source_domestique", "source_openfoodfacts",
        "source_horizon", "source_winedeals", "source_lcbo", "source_pa",
        "source_systembolaget",
    ]

    def count_table(table_name: str) -> int | str:
        try:
            result = sb.table(table_name).select("*", count="exact", head=True).execute()
            return result.count if result.count is not None else "?"
        except Exception as e:
            return f"ERR: {e}"

    def print_section(title: str, tables: list[str]):
        print(f"\n{'=' * 50}")
        print(f"  {title}")
        print(f"{'=' * 50}")
        total = 0
        for t in tables:
            count = count_table(t)
            if isinstance(count, int):
                total += count
                print(f"  {t:<40} {count:>10,}")
            else:
                print(f"  {t:<40} {count}")
        print(f"  {'TOTAL':<40} {total:>10,}")

    print_section("CANONICAL", canonical)
    print_section("REFERENCE", reference)
    print_section("INSIGHTS", insights)
    print_section("STAGING", staging)


if __name__ == "__main__":
    main()
