"""
Deterministic deduplication within wine clusters.

Identifies duplicate wines by matching identity tuples after normalization.
Does NOT use AI — purely rule-based matching.

Usage:
    python -m pipeline.identity.dedup_deterministic [--dry-run]
"""
import argparse


def find_duplicate_wines() -> list[tuple[str, str]]:
    """
    Scan canonical wines for potential duplicates.

    Two wines are duplicates if they share:
        - Same producer_id
        - Same normalized cuvée (or both NULL)
        - Same appellation_id (or both NULL)
        - Same color
        - Same wine_type

    Returns list of (wine_id_1, wine_id_2) pairs.
    """
    raise NotImplementedError("Session 3")


def merge_duplicate_wines(wine_id_keep: str, wine_id_remove: str,
                          dry_run: bool = True) -> None:
    """
    Merge a duplicate wine into the keeper.

    Steps:
        1. Re-point all child records (vintages, grapes, scores, prices, etc.)
        2. Re-point staging table links
        3. Merge external_ids
        4. Soft-delete the duplicate
        5. Log to data_provenance
    """
    raise NotImplementedError("Session 3")


def main():
    parser = argparse.ArgumentParser(description="Deterministic wine dedup")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()


if __name__ == "__main__":
    main()
