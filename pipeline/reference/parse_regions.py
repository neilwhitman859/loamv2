"""
Simple utility: reads regions JSON file and prints a by-country summary.

Usage:
    python -m pipeline.reference.parse_regions
    python -m pipeline.reference.parse_regions --file data/regions_rebuild.json
"""

import sys
import json
import argparse
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    parser = argparse.ArgumentParser(description="Parse and summarize regions JSON")
    parser.add_argument("--file", default="data/regions_rebuild.json",
                        help="Path to regions JSON file")
    args = parser.parse_args()

    filepath = Path(args.file)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath

    if not filepath.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    data = json.loads(filepath.read_text(encoding="utf-8"))
    regions = data if isinstance(data, list) else data.get("regions", data.get("data", []))

    print(f"Total regions: {len(regions)}\n")

    # By-country summary
    by_country: Counter = Counter()
    by_level: Counter = Counter()
    catch_all_count = 0

    for r in regions:
        country = r.get("country") or r.get("country_slug") or "unknown"
        by_country[country] += 1
        level = r.get("level") or ("catch-all" if r.get("is_catch_all") else "named")
        by_level[level] += 1
        if r.get("is_catch_all"):
            catch_all_count += 1

    print("By country:")
    for country, count in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {country}: {count}")

    print(f"\nBy level:")
    for level, count in sorted(by_level.items()):
        print(f"  {level}: {count}")

    print(f"\nCatch-all regions: {catch_all_count}")
    print(f"Named regions: {len(regions) - catch_all_count}")


if __name__ == "__main__":
    main()
