#!/usr/bin/env python3
"""
Load PA PLCB wine catalog into source_pa staging table.

Usage:
    python -m pipeline.load.pa
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import openpyxl

from pipeline.lib.db import get_supabase, batch_insert

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def read_xlsx(path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h) if h else f"col_{i}" for i, h in enumerate(next(rows_iter))]
    data = [dict(zip(headers, row)) for row in rows_iter]
    wb.close()
    return data


def main():
    print("Reading PA catalog...")
    data = read_xlsx(DATA_DIR / "pa_wine_catalog.xlsx")
    print(f"  {len(data)} rows")

    # Skip cocktails — keep everything else
    wines = [r for r in data if (r.get("Group Name") or "").lower() != "cocktails"]
    print(f"  {len(wines)} wine rows (excl cocktails)")

    rows = []
    for r in wines:
        # Collect all UPC values
        upcs = []
        for k, v in r.items():
            if (k == "UPC" or k.startswith("UPC_")) and v:
                upcs.append(str(v))

        rows.append({
            "plcb_item": r.get("PLCB Item") or None,
            "item_description": r.get("Item Description") or None,
            "manufacturer_scc": r.get("Manufacturer SCC") or None,
            "group_name": r.get("Group Name") or None,
            "class_name": r.get("Class Name") or None,
            "volume": r.get("Liquid Volume") or None,
            "case_pack": r.get("Case Pack") or None,
            "retail_price": float(r["Current Regular Retail"]) if r.get("Current Regular Retail") else None,
            "proof": r.get("Proof") or None,
            "vintage": r.get("Vintage") or None,
            "brand_name": r.get("Brand Name") or None,
            "import_domestic": r.get("Import/Domestic") or None,
            "country": r.get("Country") or None,
            "region": r.get("Region") or None,
            "upcs": upcs if upcs else None,
        })

    inserted = batch_insert("source_pa", rows, batch_size=500)
    print(f"\nInserted {inserted} rows into source_pa")


if __name__ == "__main__":
    main()
