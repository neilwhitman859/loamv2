"""
Filter PGI wine entries from eAmbrosia JSON data.

Reads the full eAmbrosia GI dataset, filters to PGI + WINE + registered,
extracts key fields, and writes to data/eambrosia_pgi_wines.json.

Usage:
    python -m pipeline.reference.extract_pgi_wines <input.json>
"""

import argparse
import json
import sys
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description="Extract PGI wines from eAmbrosia JSON")
    parser.add_argument("input_file", help="Input JSON file (full eAmbrosia dataset)")
    parser.add_argument("--output", default=str(PROJECT_ROOT / "data" / "eambrosia_pgi_wines.json"),
                        help="Output JSON file")
    args = parser.parse_args()

    raw = Path(args.input_file).read_text(encoding="utf-8")
    data = json.loads(raw)

    pgi_wines = [
        d for d in data
        if d.get("giType") == "PGI"
        and d.get("productType") == "WINE"
        and d.get("status") == "registered"
    ]

    extracted = sorted(
        [
            {
                "name": d["protectedNames"][0],
                "file_number": d.get("fileNumber"),
                "country": d["countries"][0],
                "eu_protection_date": d.get("euProtectionDate"),
                "gi_identifier": d.get("giIdentifier"),
                "transcriptions": d.get("transcriptions") or None,
            }
            for d in pgi_wines
        ],
        key=lambda x: (x["country"], x["name"]),
    )

    out_path = Path(args.output)
    out_path.write_text(json.dumps(extracted, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {len(extracted)} PGI wines to {out_path}")

    # Show by country
    show_countries = [
        "IT", "FR", "ES", "PT", "DE", "GR", "AT", "RO", "HU", "SI",
        "BG", "CZ", "CY", "GB", "NL", "DK", "BE", "MT", "SK", "CN", "US",
    ]
    for country in show_countries:
        entries = [e for e in extracted if e["country"] == country]
        if not entries:
            continue
        print(f"\n--- {country} ({len(entries)}) ---")
        for e in entries:
            print(f"  {e['name']}")


if __name__ == "__main__":
    main()
