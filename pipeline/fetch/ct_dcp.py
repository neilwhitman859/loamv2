#!/usr/bin/env python3
"""
Connecticut DCP OpenAccess Wine Price List Scraper.

Source: biznet.ct.gov/DCPOpenAccess/LiquorControl
Value:  UPC <-> COLA(TTB ID#) bridge

Architecture:
  Phase 1: Playwright collects supplier GUIDs from the index page
  Phase 2: Downloads each supplier's PDF price list
  Phase 3: Parses PDFs with pymupdf/pdfplumber to extract tabular wine data

Output: data/imports/ct_dcp_wines.json

Usage:
    python -m pipeline.fetch.ct_dcp
    python -m pipeline.fetch.ct_dcp --month February --year 2026
    python -m pipeline.fetch.ct_dcp --guids-only
    python -m pipeline.fetch.ct_dcp --resume
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DATA_DIR = Path("data/imports")
PDF_DIR = DATA_DIR / "ct_dcp_pdfs"
GUIDS_FILE = DATA_DIR / "ct_dcp_guids.json"
OUTPUT_FILE = DATA_DIR / "ct_dcp_wines.json"
CHECKPOINT_FILE = DATA_DIR / "ct_dcp_checkpoint.json"

BASE_URL = "https://biznet.ct.gov/DCPOpenAccess/LiquorControl"
INDEX_URL = f"{BASE_URL}/ItemList.aspx"
DISPLAY_URL = f"{BASE_URL}/DisplayItem.aspx?ItemID="

DELAY_S = 2.0


# ============================================================
# Phase 1: Collect supplier GUIDs from the index page
# ============================================================
def collect_guids(page, month: str, year: str) -> list[dict]:
    """Use Playwright page to collect supplier GUIDs."""
    print(f"\n=== Phase 1: Collecting supplier GUIDs ({month} {year}) ===")

    page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    page.goto(INDEX_URL, wait_until="networkidle", timeout=30000)

    title = page.title()
    body_text = page.evaluate("() => document.body.innerText.substring(0, 200)")
    print(f"  Page title: \"{title}\"")
    if "rejected" in body_text.lower() or "blocked" in body_text.lower():
        raise RuntimeError("Blocked by BITS BOT WAF")

    # Select month and year via ASP.NET postback
    page.select_option("#ddlPostMonth", month)
    time.sleep(4)

    current_year = page.eval_on_selector("#ddlPostYear", "el => el.value")
    if current_year != year:
        page.select_option("#ddlPostYear", year)
        time.sleep(4)

    try:
        page.wait_for_selector('a[href*="DisplayItem"]', timeout=15000)
    except Exception:
        print("  Warning: No DisplayItem links found after month selection")

    suppliers = page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a[href*="DisplayItem"]'));
        return links.map(a => {
            const guid = a.href.match(/ItemID=([a-f0-9-]+)/i)?.[1];
            const tr = a.closest('tr');
            const tds = tr ? Array.from(tr.querySelectorAll('td')) : [];
            const company = tds.length > 1 ? tds[1].innerText.split('\\n')[0].trim() : '';
            const address = tds.length > 2 ? tds[2].innerText.replace(/\\n/g, ', ').trim() : '';
            let section = 'unknown';
            let el = tr;
            while (el) {
                const text = el.innerText || '';
                if (text.includes('Suppliers') && !text.includes('Wholesalers')) { section = 'supplier'; break; }
                if (text.includes('Wholesalers')) { section = 'wholesaler'; break; }
                el = el.previousElementSibling;
            }
            return { guid, company, address, section };
        }).filter(s => s.guid);
    }""")

    print(f"  Found {len(suppliers)} price list links")

    guid_data = {
        "month": month,
        "year": year,
        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total": len(suppliers),
        "suppliers": suppliers,
    }
    GUIDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    GUIDS_FILE.write_text(json.dumps(guid_data, indent=2))
    print(f"  Saved to {GUIDS_FILE}")

    return suppliers


# ============================================================
# Phase 2: Download PDFs
# ============================================================
def download_pdfs(page, suppliers: list[dict], resume: bool) -> list[str]:
    print(f"\n=== Phase 2: Downloading {len(suppliers)} PDFs ===")

    PDF_DIR.mkdir(parents=True, exist_ok=True)

    completed: set[str] = set()
    if resume and CHECKPOINT_FILE.exists():
        cp = json.loads(CHECKPOINT_FILE.read_text())
        completed = set(cp.get("completed", []))
        print(f"  Resuming: {len(completed)} already downloaded")

    downloaded = 0
    errors = 0

    for supplier in suppliers:
        guid = supplier["guid"]
        if guid in completed:
            continue

        try:
            pdf_path = PDF_DIR / f"{guid}.pdf"
            response = page.goto(
                f"{DISPLAY_URL}{guid}",
                wait_until="networkidle",
                timeout=30000,
            )

            if response is None:
                errors += 1
                continue

            content_type = response.headers.get("content-type", "")

            if "pdf" in content_type or "octet-stream" in content_type:
                body = response.body()
                header_str = body[:200].decode("utf-8", errors="replace")
                if "Request Rejected" in header_str:
                    print(f"  WAF BLOCKED: {supplier['company']}")
                    errors += 1
                    time.sleep(DELAY_S * 3)
                    continue
                pdf_path.write_bytes(body)
            else:
                # Try to find embedded PDF URL
                pdf_url = page.evaluate("""() => {
                    const embed = document.querySelector(
                        'embed[type="application/pdf"], object[type="application/pdf"], iframe[src*=".pdf"]'
                    );
                    return embed?.src || embed?.data || null;
                }""")

                if pdf_url:
                    pdf_resp = page.goto(pdf_url, wait_until="networkidle", timeout=30000)
                    if pdf_resp:
                        pdf_path.write_bytes(pdf_resp.body())
                else:
                    # Print page to PDF as fallback
                    page.pdf(path=str(pdf_path), format="Letter")

            completed.add(guid)
            downloaded += 1

            if downloaded % 20 == 0:
                print(f"  Downloaded {downloaded} ({supplier['company']})")
                CHECKPOINT_FILE.write_text(json.dumps({
                    "completed": list(completed),
                    "last_update": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }))

            time.sleep(DELAY_S)

        except Exception as err:
            print(f"  ERROR downloading {supplier['company']} ({guid}): {err}")
            errors += 1
            time.sleep(DELAY_S * 2)

    # Final checkpoint
    CHECKPOINT_FILE.write_text(json.dumps({
        "completed": list(completed),
        "last_update": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }))

    print(f"  Downloaded: {downloaded}, Errors: {errors}, Total complete: {len(completed)}")
    return list(completed)


# ============================================================
# Phase 3: Parse PDFs
# ============================================================
def parse_pdfs(completed_guids: list[str], suppliers: list[dict]) -> list[dict]:
    print(f"\n=== Phase 3: Parsing {len(completed_guids)} PDFs ===")

    try:
        import pdfplumber
    except ImportError:
        try:
            import fitz  # pymupdf
        except ImportError:
            print("Neither pdfplumber nor pymupdf available. Skipping Phase 3.")
            print(f"PDFs are saved in {PDF_DIR}")
            return []

    supplier_map = {s["guid"]: s for s in suppliers}
    all_wines: list[dict] = []
    parsed = 0
    errors = 0

    use_pdfplumber = "pdfplumber" in sys.modules

    for guid in completed_guids:
        pdf_path = PDF_DIR / f"{guid}.pdf"
        if not pdf_path.exists():
            continue

        try:
            text = ""
            if use_pdfplumber:
                import pdfplumber
                with pdfplumber.open(str(pdf_path)) as pdf:
                    for pg in pdf.pages:
                        text += (pg.extract_text() or "") + "\n"
            else:
                import fitz
                doc = fitz.open(str(pdf_path))
                for pg in doc:
                    text += pg.get_text() + "\n"
                doc.close()

            lines = [l.strip() for l in text.split("\n") if l.strip()]

            # Find header line
            header_idx = None
            for i, line in enumerate(lines):
                if "Item" in line and "UPC" in line and "COLA" in line:
                    header_idx = i
                    break

            if header_idx is None:
                continue

            supplier = supplier_map.get(guid)
            company_name = supplier["company"] if supplier else "Unknown"

            for line in lines[header_idx + 1:]:
                if not line or line.startswith("Page") or "Price List" in line:
                    continue

                upc_match = re.search(r"\b(\d[\d\s-]{10,15}\d)\b", line)
                cola_match = re.search(r"\b(\d{11,15})\b", line)

                if upc_match or cola_match:
                    wine: dict = {
                        "raw_line": line,
                        "supplier": company_name,
                        "supplier_guid": guid,
                        "upc": re.sub(r"[\s-]", "", upc_match.group(1)) if upc_match else None,
                        "cola_ttb_id": None,
                        "brand": None,
                        "description": None,
                        "vintage": None,
                        "abv": None,
                        "size": None,
                        "price": None,
                    }

                    vintage_match = re.search(r"\b(19\d{2}|20[0-2]\d)\b", line)
                    if vintage_match:
                        wine["vintage"] = int(vintage_match.group(1))

                    abv_match = re.search(r"\b(\d{1,2}\.\d{1,2})\b", line)
                    if abv_match:
                        val = float(abv_match.group(1))
                        if 5 <= val <= 25:
                            wine["abv"] = val

                    price_matches = re.findall(r"\b(\d{1,4}\.\d{2})\b", line)
                    if price_matches:
                        wine["price"] = float(price_matches[-1])

                    size_match = re.search(r"\b(\d+(?:\.\d+)?)\s*(ML|L)\b", line, re.IGNORECASE)
                    if size_match:
                        wine["size"] = size_match.group(0).upper()

                    all_nums = re.findall(r"\b\d{11,15}\b", line)
                    for num in all_nums:
                        clean = re.sub(r"[\s-]", "", num)
                        if clean != wine["upc"] and len(clean) >= 11:
                            wine["cola_ttb_id"] = clean
                            break

                    all_wines.append(wine)

            parsed += 1
            if parsed % 50 == 0:
                print(f"  Parsed {parsed} PDFs, {len(all_wines)} wine rows found")

        except Exception as err:
            errors += 1
            if errors <= 5:
                print(f"  Parse error for {guid}: {err}")

    print(f"  Parsed: {parsed}, Errors: {errors}, Wine rows: {len(all_wines)}")
    return all_wines


def main():
    parser = argparse.ArgumentParser(description="Connecticut DCP Wine Price List Scraper")
    parser.add_argument("--month", default="February")
    parser.add_argument("--year", default="2026")
    parser.add_argument("--guids-only", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    print("=== Connecticut DCP Wine Price List Scraper ===")
    print(f"Month: {args.month} {args.year}")
    print(f"Output: {OUTPUT_FILE}")

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            # Phase 1: Collect GUIDs
            if args.resume and GUIDS_FILE.exists():
                saved = json.loads(GUIDS_FILE.read_text())
                suppliers = saved["suppliers"]
                print(f"  Loaded {len(suppliers)} GUIDs from cache")
            else:
                suppliers = collect_guids(page, args.month, args.year)

            if args.guids_only:
                print("\n--guids-only: stopping after Phase 1")
                browser.close()
                return

            # Phase 2: Download PDFs
            completed_guids = download_pdfs(page, suppliers, args.resume)

        finally:
            browser.close()

    # Phase 3: Parse PDFs (no browser needed)
    wines = parse_pdfs(completed_guids, suppliers)

    # Stats
    stats = {
        "total_wines": len(wines),
        "has_upc": sum(1 for w in wines if w.get("upc")),
        "has_cola": sum(1 for w in wines if w.get("cola_ttb_id")),
        "has_both": sum(1 for w in wines if w.get("upc") and w.get("cola_ttb_id")),
        "has_vintage": sum(1 for w in wines if w.get("vintage")),
        "has_abv": sum(1 for w in wines if w.get("abv")),
        "has_price": sum(1 for w in wines if w.get("price")),
        "unique_suppliers": len({w["supplier"] for w in wines}),
    }

    print("\n=== RESULTS ===")
    print(f"Total wine rows: {stats['total_wines']}")
    print(f"Has UPC: {stats['has_upc']}")
    print(f"Has COLA/TTB ID: {stats['has_cola']}")
    print(f"Has BOTH UPC + COLA: {stats['has_both']}")
    print(f"Has vintage: {stats['has_vintage']}")
    print(f"Has ABV: {stats['has_abv']}")
    print(f"Unique suppliers: {stats['unique_suppliers']}")

    output = {
        "metadata": {
            "source": "Connecticut DCP Liquor Control Posted Prices",
            "url": INDEX_URL,
            "month": args.month,
            "year": args.year,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "stats": stats,
        },
        "wines": wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
