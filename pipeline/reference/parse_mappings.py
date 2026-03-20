"""
Simple utility: reads a JSON data file and prints a formatted mapping table.

Usage:
    python -m pipeline.reference.parse_mappings
    python -m pipeline.reference.parse_mappings --file data/some_mappings.json
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    parser = argparse.ArgumentParser(description="Parse and display mapping data")
    parser.add_argument("--file", default="data/region_name_mappings.json",
                        help="Path to mappings JSON file")
    args = parser.parse_args()

    filepath = Path(args.file)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath

    if not filepath.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    data = json.loads(filepath.read_text(encoding="utf-8"))
    mappings = data if isinstance(data, list) else data.get("mappings", data.get("data", []))

    print(f"Total mappings: {len(mappings)}\n")

    # Print formatted table
    if not mappings:
        return

    # Auto-detect columns from first item
    keys = list(mappings[0].keys())
    widths = {k: max(len(k), max((len(str(m.get(k, ""))) for m in mappings), default=0))
              for k in keys}
    # Cap widths at 50
    widths = {k: min(v, 50) for k, v in widths.items()}

    # Header
    header = " | ".join(k.ljust(widths[k]) for k in keys)
    print(header)
    print("-+-".join("-" * widths[k] for k in keys))

    # Rows
    for m in mappings:
        row = " | ".join(str(m.get(k, "")).ljust(widths[k])[:widths[k]] for k in keys)
        print(row)


if __name__ == "__main__":
    main()
