"""
Promote depth data from staging to canonical tables.

Handles: vintages, prices, scores, UPCs, label images, ABV,
and other wine_vintage fields from linked staging records.

Usage:
    python -m pipeline.quality.promote_depth --wine-id UUID [--dry-run]
    python -m pipeline.quality.promote_depth --batch 0 [--dry-run]
"""
import argparse


def promote_vintages(wine_id: str, dry_run: bool = True) -> int:
    """
    Create wine_vintages rows from linked staging records.
    Returns count of new vintages created.
    """
    raise NotImplementedError("Session 3+")


def promote_prices(wine_id: str, dry_run: bool = True) -> int:
    """
    Promote prices from staging to wine_vintage_prices.
    Returns count of new prices created.
    """
    raise NotImplementedError("Session 3+")


def promote_scores(wine_id: str, dry_run: bool = True) -> int:
    """
    Promote scores from staging to wine_vintage_scores.
    Returns count of new scores created.
    """
    raise NotImplementedError("Session 3+")


def promote_external_ids(wine_id: str, dry_run: bool = True) -> int:
    """
    Promote COLA, LWIN, UPC to external_ids.
    Returns count of new external IDs created.
    """
    raise NotImplementedError("Session 3+")


def promote_label_images(wine_id: str, dry_run: bool = True) -> int:
    """
    Link TTB label images to wine_vintages.
    Returns count of images linked.
    """
    raise NotImplementedError("Session 3+")


def cascade_geography(wine_id: str, dry_run: bool = True) -> dict:
    """
    Cascade geographic hierarchy: appellation → region → country.
    Returns dict of fields filled.
    """
    raise NotImplementedError("Session 3")


def main():
    parser = argparse.ArgumentParser(description="Promote depth data")
    parser.add_argument("--wine-id", help="Single wine UUID")
    parser.add_argument("--batch", type=int, help="Batch number")
    parser.add_argument("--dry-run", action="store_true", default=True)
    args = parser.parse_args()


if __name__ == "__main__":
    main()
