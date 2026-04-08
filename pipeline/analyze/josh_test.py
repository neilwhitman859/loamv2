"""
Josh Test: measure Loam's ability to find wines Americans actually encounter.

Loads the sample from data/josh_test_sample.json and tests each wine against
the canonical database. Measures find rate, data quality, and coverage by
price tier and country.

Usage:
    python -m pipeline.analyze.josh_test [--sample data/josh_test_sample.json]
    python -m pipeline.analyze.josh_test --tier "$30-100"  # single tier
    python -m pipeline.analyze.josh_test --staging  # check staging coverage
"""
import argparse
import json
from pathlib import Path


SAMPLE_PATH = Path("data/josh_test_sample.json")


def load_sample(path: Path = SAMPLE_PATH) -> list[dict]:
    """Load the Josh Test wine sample."""
    with open(path) as f:
        data = json.load(f)
    return data["wines"]


def test_findability(wines: list[dict]) -> dict:
    """
    Test if each wine can be found in the canonical database.

    For each wine, searches by:
        1. Producer name (fuzzy)
        2. Wine name / search term
        3. If found, checks data quality (confirmation, completeness)

    Returns:
        {
            "total": int,
            "found": int,
            "find_rate": float,
            "by_tier": {tier: {"total": N, "found": N, "rate": float}},
            "by_country": {cc: {"total": N, "found": N, "rate": float}},
            "missing": [list of unfound wine dicts],
            "avg_confirmation": str,
            "avg_completeness": float,
        }
    """
    raise NotImplementedError("Session 4+")


def test_staging_coverage(wines: list[dict]) -> dict:
    """
    Check how many Josh Test wines exist in staging tables (pre-promotion).

    Used to validate that our staging data can cover the test sample
    before we start promoting wines.

    Returns:
        {
            "total": int,
            "found_in_staging": int,
            "coverage_rate": float,
            "by_tier": {tier: {"total": N, "found": N}},
            "missing": [list of wines not in any staging table],
        }
    """
    raise NotImplementedError("Session 2 — implement for S2.3 check")


def print_report(results: dict) -> None:
    """Print formatted Josh Test results."""
    print(f"\n{'='*60}")
    print(f"  JOSH TEST RESULTS")
    print(f"{'='*60}")
    print(f"  Find rate: {results['found']}/{results['total']} ({results['find_rate']:.0%})")
    print()
    for tier, data in results.get("by_tier", {}).items():
        print(f"  {tier:>10}: {data['found']}/{data['total']} ({data['rate']:.0%})")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Josh Test")
    parser.add_argument("--sample", default=str(SAMPLE_PATH))
    parser.add_argument("--tier", help="Test single price tier only")
    parser.add_argument("--staging", action="store_true",
                        help="Check staging coverage instead of canonical")
    args = parser.parse_args()

    wines = load_sample(Path(args.sample))
    if args.tier:
        wines = [w for w in wines if w["price_tier"] == args.tier]

    if args.staging:
        results = test_staging_coverage(wines)
    else:
        results = test_findability(wines)

    print_report(results)


if __name__ == "__main__":
    main()
