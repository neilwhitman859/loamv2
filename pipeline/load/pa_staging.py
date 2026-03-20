#!/usr/bin/env python3
"""
Load PA PLCB wine catalog into source_pa staging table.

Usage:
    python -m pipeline.load.pa_staging
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import openpyxl
from pipeline.lib.db import get_supabase

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def main():
    sb = get_supabase()

    print("Reading PA catalog...")
    wb = openpyxl.load_workbook(DATA_DIR / "pa_wine_catalog.xlsx", read_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = next(rows_iter)
    header_idx = {h: i for i, h in enumerate(headers) if h}

    raw_rows = [dict(zip(headers, row)) for row in rows_iter]
    wb.close()
    print(f"  {len(raw_rows)} rows")

    # Skip cocktails — keep everything else
    wines = [r for r in raw_rows if (r.get("Group Name") or "").lower() != "cocktails"]
    print(f"  {len(wines)} wine rows (excl cocktails)")

    # Transform to staging format
    staging_rows = []
    for r in wines:
        # Collect all UPC values
        upcs = []
        for key in r:
            if key and (key == "UPC" or str(key).startswith("UPC_")):
                val = r[key]
                if val:
                    upcs.append(str(val))

        staging_rows.append({
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

    # Insert in batches
    BATCH = 500
    inserted = 0
    for i in range(0, len(staging_rows), BATCH):
        batch = staging_rows[i:i + BATCH]
        try:
            sb.table("source_pa").insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            print(f"  Batch {i // BATCH} error: {e}")

    print(f"\nInserted {inserted} rows into source_pa")


if __name__ == "__main__":
    main()
