#!/usr/bin/env python3
"""
Parse Pennsylvania PLCB Wholesale Catalog Excel -> JSON
Extracts wine products with UPC barcodes, prices, vintage, country, region.
Output: data/imports/pa_wines_parsed.json

Usage:
    python -m pipeline.load.parse_pa_catalog
"""

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import openpyxl

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"
INPUT = DATA_DIR / "pa_wine_catalog.xlsx"
OUTPUT = DATA_DIR / "pa_wines_parsed.json"


def main():
    wb = openpyxl.load_workbook(INPUT, read_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    # header=1 style: first row is headers, data starts at row 1
    # Filter: col[0] == 'Stock Wine'
    data = [r for r in all_rows[1:] if r[0] == "Stock Wine"]

    wine_groups = {"Table Wine", "Fortified Wine", "Other-Dessert Wines", "Sparkling Wine"}
    wines = [r for r in data if r[1] in wine_groups]

    output = []
    for r in wines:
        upcs = [str(r[i]).strip() for i in range(16, 21) if r[i]]
        vintage = r[28]
        vintage_year = int(vintage) if vintage and str(vintage).strip().isdigit() and len(str(vintage).strip()) == 4 else None

        output.append({
            "plcb_item": str(r[3]).strip() if r[3] else None,
            "name": r[4] or None,
            "plcb_scc": str(r[5]).strip() if r[5] else None,
            "manufacturer_scc": str(r[6]).strip() if r[6] else None,
            "volume": r[7] or None,
            "case_pack": int(r[8]) if r[8] else None,
            "price_usd": float(r[9]) if r[9] else None,
            "upcs": upcs,
            "upc_primary": upcs[0] if upcs else None,
            "proof": r[27] or None,
            "vintage": vintage or None,
            "vintage_year": vintage_year,
            "brand": r[29] or None,
            "import_domestic": r[30] or None,
            "country": r[31] or None,
            "region": r[32] or None,
            "group": r[1] or None,
            "class_name": r[2] or None,
        })

    all_upcs = set()
    for w in output:
        for u in w["upcs"]:
            all_upcs.add(u)

    stats = {
        "total": len(output),
        "has_upc": sum(1 for w in output if w["upc_primary"]),
        "unique_upcs": len(all_upcs),
        "has_vintage_year": sum(1 for w in output if w["vintage_year"]),
        "has_country": sum(1 for w in output if w["country"]),
        "has_region": sum(1 for w in output if w["region"]),
        "has_brand": sum(1 for w in output if w["brand"]),
        "has_proof": sum(1 for w in output if w["proof"] and w["proof"] != "N/A"),
        "has_price": sum(1 for w in output if w["price_usd"]),
    }

    print("=== PA PLCB Wine Catalog ===")
    print(f"Total wines: {stats['total']}")
    print(f"Has UPC: {stats['has_upc']} ({stats['has_upc'] / stats['total'] * 100:.1f}%)")
    print(f"Unique UPCs: {stats['unique_upcs']}")
    print(f"Has vintage year: {stats['has_vintage_year']}")
    print(f"Has country: {stats['has_country']}")
    print(f"Has region: {stats['has_region']}")
    print(f"Has brand: {stats['has_brand']}")
    print(f"Has proof: {stats['has_proof']}")
    print(f"Has price: {stats['has_price']}")

    countries = Counter(w["country"] for w in output if w["country"])
    print("\nTop countries:", dict(countries.most_common(15)))

    groups = Counter(w["group"] for w in output)
    print("Groups:", dict(groups))

    file_data = {
        "metadata": {
            "source": "Pennsylvania PLCB Wholesale Catalog",
            "file": "pa_wine_catalog.xlsx",
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "stats": stats,
        },
        "wines": output,
    }
    OUTPUT.write_text(json.dumps(file_data, indent=2), encoding="utf-8")
    print(f"\nSaved to {OUTPUT}")
    print(f"File size: {OUTPUT.stat().st_size / (1024 * 1024):.1f} MB")


if __name__ == "__main__":
    main()
