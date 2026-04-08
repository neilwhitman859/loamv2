"""
Select and promote wines for a validated producer.

Given a canonical producer, searches all staging tables for their wines,
clusters by identity tuple, deduplicates, and creates canonical wine records.

Usage:
    python -m pipeline.identity.select_wines --producer-id UUID [--dry-run]
    python -m pipeline.identity.select_wines --batch 0 [--dry-run]
"""
import argparse
from typing import Optional


def discover_wines_for_producer(producer_id: str, producer_name: str) -> list[dict]:
    """
    Search all staging tables for wines by this producer.

    Returns list of staging records, each with:
        - source: staging table name
        - source_id: row ID in staging table
        - raw_name: wine name as it appears in staging
        - appellation_text: raw appellation text (if any)
        - grape_text: raw grape text (if any)
        - color: color if available
        - vintage: vintage year if available
    """
    raise NotImplementedError("Session 3")


def cluster_wines(staging_records: list[dict], producer_name: str,
                  country_code: str) -> list[dict]:
    """
    Group staging records that represent the same wine.

    Uses identity tuple: (producer, cuvée, grapes, appellation, color, classification)
    After extracting cuvée and resolving appellation/grape FKs, groups records
    with matching identity tuples into clusters.

    Returns list of wine clusters, each with:
        - identity: the identity tuple
        - records: list of staging records in this cluster
        - best_source: the highest-quality record (LWIN > importer > TTB > retailer)
    """
    raise NotImplementedError("Session 3")


def create_canonical_wine(cluster: dict, producer_id: str,
                          dry_run: bool = True) -> Optional[str]:
    """
    Create a canonical wine record from a deduplicated cluster.

    Steps:
        1. Create wines row (name=cuvée, producer_id, appellation_id, color, etc.)
        2. Compute and store display_name
        3. Link staging records (set canonical_wine_id on staging rows)
        4. Log all fields to data_provenance
        5. Return the new wine UUID

    Args:
        cluster: Wine cluster from cluster_wines()
        producer_id: Canonical producer UUID
        dry_run: If True, only log what would happen

    Returns:
        New wine UUID, or None if dry_run.
    """
    raise NotImplementedError("Session 3")


def main():
    parser = argparse.ArgumentParser(description="Select and promote wines")
    parser.add_argument("--producer-id", help="Single producer UUID")
    parser.add_argument("--batch", type=int, help="Batch number (0, 1, 2, ...)")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()


if __name__ == "__main__":
    main()
