"""
Cross-reference a producer across staging tables.

Given a producer name, search all staging sources and return matches with
confidence scores. Used to validate producer identity before canonical insertion.

Usage:
    python -m pipeline.identity.producer_crossref --name "Ridge Vineyards" [--dry-run]
"""
import argparse
from typing import Optional


def search_staging_for_producer(name: str) -> dict:
    """
    Search all staging tables for a producer name.

    Returns dict keyed by source name, each value is a list of matching rows
    with normalized names and row counts.

    Sources searched:
        - source_lwin (producer column)
        - source_ttb_colas (brand_name, applicant_name)
        - source_skurnik, source_empson, etc. (producer columns)
        - source_specs, source_flatiron, etc. (brand/producer columns)
        - source_texsom, source_berliner (producer columns)
    """
    raise NotImplementedError("Session 3")


def normalize_producer_name(name: str) -> str:
    """
    Normalize producer name for matching.

    Steps:
        1. Lowercase
        2. Strip accents (unidecode)
        3. Strip prefixes: Château, Domaine, Bodegas, Weingut, Tenuta, etc.
        4. Strip suffixes: Winery, Vineyards, Estate, Cellars, etc.
        5. Collapse whitespace
        6. Strip punctuation

    See docs/IDENTITY_RULES.md Section 5 for full list.
    """
    raise NotImplementedError("Session 3")


def is_junk_producer(name: str) -> bool:
    """
    Check if a producer name matches junk criteria.

    See docs/IDENTITY_RULES.md Section 7 for rules.
    Returns True if the name should be rejected.
    """
    raise NotImplementedError("Session 3")


def main():
    parser = argparse.ArgumentParser(description="Cross-reference producer in staging")
    parser.add_argument("--name", required=True, help="Producer name to search")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()

    results = search_staging_for_producer(args.name)
    # Print results summary
    for source, matches in results.items():
        print(f"  {source}: {len(matches)} matches")


if __name__ == "__main__":
    main()
