"""
Fetch and load Utah DABS (Department of Alcoholic Beverage Services) product list.

Downloads the monthly XLSX from abs.utah.gov, filters to wine products,
parses product details, and upserts into source_utah_dabs.

Usage:
    python -m pipeline.fetch.utah_dabs [--dry-run] [--file path/to/local.xlsx]
"""

import argparse
import re
import sys
import tempfile
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn


# Utah fiscal year starts July. P1=July, P10=April, P12=June.
MONTH_TO_PERIOD = {
    1: 7, 2: 8, 3: 9, 4: 10, 5: 11, 6: 12,
    7: 1, 8: 2, 9: 3, 10: 4, 11: 5, 12: 6,
}

MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December',
}


def guess_xlsx_url(year=2026, month=4):
    """Try common URL patterns for the DABS product list XLSX."""
    fy = year if month >= 7 else year
    period = MONTH_TO_PERIOD[month]
    month_name = MONTH_NAMES[month]

    # Utah is inconsistent with separators — try both hyphen and underscore
    patterns = [
        f"https://abs.utah.gov/wp-content/uploads/{month_name}-{year}-Product-List-FY{fy % 100}_P{period}.xlsx",
        f"https://abs.utah.gov/wp-content/uploads/{month_name}-{year}-Product-List_FY{fy % 100}_P{period}.xlsx",
    ]
    return patterns


def download_xlsx(urls):
    """Try URLs in order, return path to downloaded file."""
    for url in urls:
        print(f"  Trying: {url}")
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 1000:
                tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
                tmp.write(resp.content)
                tmp.close()
                print(f"  Downloaded: {len(resp.content):,} bytes")
                return tmp.name
        except Exception as e:
            print(f"  Failed: {e}")
    return None


def parse_xlsx(filepath):
    """Parse the DABS XLSX and return wine rows."""
    import openpyxl

    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    # Find header row (look for 'CSC' or 'Description')
    header_idx = 0
    for i, row in enumerate(rows[:5]):
        row_str = [str(c or '').strip().upper() for c in row]
        if 'CSC' in row_str or 'DESCRIPTION' in row_str:
            header_idx = i
            break

    headers = [str(c or '').strip() for c in rows[header_idx]]
    print(f"  Headers: {headers}")

    # Map headers to column indexes
    col_map = {}
    for i, h in enumerate(headers):
        h_upper = h.upper().strip()
        if 'CSC' in h_upper:
            col_map['csc'] = i
        elif h_upper == 'DESCRIPTION':
            col_map['description'] = i
        elif h_upper == 'DIV' and 'NAME' not in h_upper:
            col_map['div'] = i
        elif h_upper == 'DEPT' and 'NAME' not in h_upper:
            col_map['dept'] = i
        elif 'CLASS' in h_upper and 'NAME' in h_upper:
            col_map['class_name'] = i
        elif h_upper == 'CLASS' and 'NAME' not in h_upper:
            col_map['class_code'] = i
        elif h_upper == 'SIZE':
            col_map['size'] = i
        elif 'RETAIL' in h_upper and 'PRICE' in h_upper:
            col_map['price'] = i
        elif h_upper in ('ITEM STATUS', 'STATUS'):
            col_map['status'] = i
        elif 'VENDOR' in h_upper and 'NAME' in h_upper:
            col_map['vendor_name'] = i
        elif 'VENDOR' in h_upper and ('CD' in h_upper or 'CODE' in h_upper):
            col_map['vendor_code'] = i
        elif h_upper == 'ON SPA':
            col_map['on_spa'] = i
        elif 'DIV' in h_upper and 'NAME' in h_upper:
            col_map['div_name'] = i
        elif 'DEPT' in h_upper and 'NAME' in h_upper:
            col_map['dept_name'] = i

    print(f"  Mapped columns: {list(col_map.keys())}")

    wine_rows = []
    for row in rows[header_idx + 1:]:
        if not row or not row[0]:
            continue

        def get(key):
            idx = col_map.get(key)
            if idx is None or idx >= len(row):
                return None
            return row[idx]

        # Filter to wine: check div_name or dept_name for 'WINE'
        div_name = str(get('div_name') or '').upper()
        dept_name = str(get('dept_name') or '').upper()
        class_name = str(get('class_name') or '').upper()

        is_wine = 'WINE' in div_name or 'WINE' in dept_name
        # Exclude fruit wine, cider, sake, mead
        if not is_wine:
            continue
        excludes = ['FRUIT', 'CIDER', 'SAKE', 'MEAD', 'HARD SELTZER', 'READY TO']
        if any(ex in class_name for ex in excludes) or any(ex in dept_name for ex in excludes):
            continue

        desc = str(get('description') or '').strip()
        if not desc:
            continue

        # Parse size from description or size column
        size_raw = get('size')
        size_ml = None
        if size_raw:
            size_str = str(size_raw).strip().upper()
            # Try to parse ML value
            m = re.search(r'(\d+)\s*ML', size_str)
            if m:
                size_ml = int(m.group(1))
            else:
                m = re.search(r'(\d+(?:\.\d+)?)\s*L', size_str)
                if m:
                    size_ml = int(float(m.group(1)) * 1000)

        price = get('price')
        price_usd = None
        if price is not None:
            try:
                price_usd = float(str(price).replace('$', '').replace(',', '').strip())
            except (ValueError, TypeError):
                pass

        status = str(get('status') or '').strip()
        on_spa = str(get('on_spa') or '').strip().upper() in ('Y', 'YES', 'TRUE', '1')

        wine_rows.append({
            'csc': str(get('csc')).strip(),
            'description': desc,
            'division': str(get('div_name') or get('div') or '').strip() or None,
            'department': str(get('dept_name') or get('dept') or '').strip() or None,
            'class_name': str(get('class_name') or '').strip() or None,
            'size_ml': size_ml,
            'price_usd': price_usd,
            'item_status': status or None,
            'vendor_name': str(get('vendor_name') or '').strip() or None,
            'vendor_code': str(get('vendor_code') or '').strip() or None,
            'on_spa': on_spa,
        })

    wb.close()
    return wine_rows


def load_to_db(rows, dry_run=False):
    """Upsert wine rows into source_utah_dabs."""
    if dry_run:
        print(f"  DRY RUN: would upsert {len(rows)} rows")
        return

    conn = get_conn()
    cur = conn.cursor()

    inserted = 0
    updated = 0
    errors = 0

    for row in rows:
        try:
            cur.execute("""
                INSERT INTO source_utah_dabs
                    (csc, description, division, department, class_name, size_ml,
                     price_usd, item_status, vendor_name, vendor_code, on_spa)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (csc) DO UPDATE SET
                    description = EXCLUDED.description,
                    price_usd = EXCLUDED.price_usd,
                    item_status = EXCLUDED.item_status,
                    vendor_name = EXCLUDED.vendor_name,
                    on_spa = EXCLUDED.on_spa
                RETURNING (xmax = 0) as is_insert
            """, (
                row['csc'], row['description'], row['division'], row['department'],
                row['class_name'], row['size_ml'], row['price_usd'],
                row['item_status'], row['vendor_name'], row['vendor_code'], row['on_spa'],
            ))
            result = cur.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Error on {row['csc']}: {str(e)[:100]}")
            conn.rollback()
            continue

    conn.commit()
    conn.close()
    print(f"  Inserted: {inserted}, Updated: {updated}, Errors: {errors}")


def main():
    parser = argparse.ArgumentParser(description="Fetch and load Utah DABS product list")
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--file', help="Local XLSX file instead of downloading")
    parser.add_argument('--year', type=int, default=2026)
    parser.add_argument('--month', type=int, default=4)
    args = parser.parse_args()

    t0 = time.time()

    if args.file:
        filepath = args.file
        print(f"Using local file: {filepath}")
    else:
        print("Downloading Utah DABS product list...")
        urls = guess_xlsx_url(args.year, args.month)
        filepath = download_xlsx(urls)
        if not filepath:
            print("ERROR: Could not download XLSX from any URL pattern")
            sys.exit(1)

    print("Parsing XLSX...")
    rows = parse_xlsx(filepath)
    print(f"  {len(rows)} wine rows parsed")

    if not rows:
        print("No wine rows found!")
        sys.exit(1)

    # Show stats
    active = [r for r in rows if r['item_status'] in ('1', 'L')]
    with_price = [r for r in rows if r['price_usd'] and r['price_usd'] > 0]
    classes = set(r['class_name'] for r in rows if r['class_name'])
    print(f"  Active (status 1/L): {len(active)}")
    print(f"  With price: {len(with_price)}")
    print(f"  Distinct classes: {len(classes)}")

    # Sample
    print("\n  Sample rows:")
    for r in rows[:5]:
        print(f"    {r['csc']} | {r['description'][:50]} | ${r['price_usd']} | {r['class_name']}")

    print(f"\nLoading to DB...")
    load_to_db(rows, args.dry_run)

    elapsed = time.time() - t0
    print(f"\nDone ({elapsed:.0f}s)")


if __name__ == '__main__':
    main()
