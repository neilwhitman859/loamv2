#!/usr/bin/env python3
"""B5.2 Task 2: Fetch producer wine pages and build ground truth.

Phase 1: Build manifest + fetch HTML
Phase 2: Extract ground truth (Opus inline)

Usage:
    python -m bakeoff.build_task2 --fetch      # Fetch HTML pages
    python -m bakeoff.build_task2 --manifest    # Just print manifest
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests

OUT_DIR = Path(__file__).parent / "data" / "task2"

# ── Producer wine page manifest ──────────────────────────────
# Each entry: producer name, wine name, URL, expected data richness
# Target: 50 pages across rich/moderate/sparse data availability

MANIFEST = [
    # ── US producers with tech sheets (rich data expected) ──
    {"producer": "Shafer Vineyards", "wine": "One Point Five Cabernet Sauvignon",
     "url": "https://www.shafervineyards.com/wines/one-point-five/", "tier": "rich"},
    {"producer": "Shafer Vineyards", "wine": "Hillside Select Cabernet Sauvignon",
     "url": "https://www.shafervineyards.com/wines/hillside-select/", "tier": "rich"},
    {"producer": "Shafer Vineyards", "wine": "Red Shoulder Ranch Chardonnay",
     "url": "https://www.shafervineyards.com/wines/red-shoulder-ranch/", "tier": "rich"},
    {"producer": "Cakebread Cellars", "wine": "Napa Valley Chardonnay",
     "url": "https://www.cakebread.com/wines/napa-valley-chardonnay", "tier": "rich"},
    {"producer": "Cakebread Cellars", "wine": "Napa Valley Cabernet Sauvignon",
     "url": "https://www.cakebread.com/wines/napa-valley-cabernet-sauvignon", "tier": "rich"},
    {"producer": "Joseph Phelps", "wine": "Insignia",
     "url": "https://www.josephphelps.com/wines/napa-valley/insignia", "tier": "rich"},
    {"producer": "Paul Hobbs", "wine": "Russian River Valley Pinot Noir",
     "url": "https://www.paulhobbswinery.com/wines/russian-river-valley-pinot-noir", "tier": "rich"},
    {"producer": "Bonterra", "wine": "Organic Cabernet Sauvignon",
     "url": "https://www.bonterra.com/wines/cabernet-sauvignon/", "tier": "rich"},
    {"producer": "Bonterra", "wine": "Organic Chardonnay",
     "url": "https://www.bonterra.com/wines/chardonnay/", "tier": "rich"},
    {"producer": "Tolosa", "wine": "1772 Pinot Noir",
     "url": "https://www.tolosawinery.com/wines/1772-pinot-noir/", "tier": "rich"},
    {"producer": "Tolosa", "wine": "Edna Ranch Chardonnay",
     "url": "https://www.tolosawinery.com/wines/edna-ranch-chardonnay/", "tier": "rich"},

    # ── European producers (moderate to rich data) ──
    {"producer": "Kir Yianni", "wine": "Ramnista",
     "url": "https://www.kiryianni.gr/en/wines/ramnista/", "tier": "moderate"},
    {"producer": "Kir Yianni", "wine": "Akakies Rosé",
     "url": "https://www.kiryianni.gr/en/wines/akakies/", "tier": "moderate"},
    {"producer": "Kir Yianni", "wine": "Paranga White",
     "url": "https://www.kiryianni.gr/en/wines/paranga-white/", "tier": "moderate"},
    {"producer": "Leo Hillinger", "wine": "Blaufrankisch",
     "url": "https://www.leo-hillinger.com/en/wines/blaufraenkisch/", "tier": "moderate"},
    {"producer": "Terroir Al Limit", "wine": "Les Tosses",
     "url": "https://www.terroirallimit.com/en/wines/les-tosses/", "tier": "moderate"},
    {"producer": "Lafage", "wine": "Tessellae Old Vines",
     "url": "https://www.domaine-lafage.com/en/wine/tessellae-old-vines/", "tier": "moderate"},

    # ── Australian producers (often detailed tech info) ──
    {"producer": "d'Arenberg", "wine": "The Dead Arm Shiraz",
     "url": "https://www.darenberg.com.au/the-dead-arm-shiraz", "tier": "rich"},
    {"producer": "d'Arenberg", "wine": "The Stump Jump Red",
     "url": "https://www.darenberg.com.au/the-stump-jump-red-blend", "tier": "rich"},
    {"producer": "d'Arenberg", "wine": "The High Trellis Cabernet Sauvignon",
     "url": "https://www.darenberg.com.au/the-high-trellis-cabernet-sauvignon", "tier": "rich"},
    {"producer": "Howard Park", "wine": "Leston Cabernet Sauvignon",
     "url": "https://www.howardparkwines.com.au/wines/leston-cabernet-sauvignon/", "tier": "rich"},
    {"producer": "Howard Park", "wine": "Miamup Chardonnay",
     "url": "https://www.howardparkwines.com.au/wines/miamup-chardonnay/", "tier": "rich"},
    {"producer": "Tyrrell's", "wine": "Vat 1 Hunter Semillon",
     "url": "https://www.tyrrells.com.au/wines/vat-1-hunter-semillon/", "tier": "rich"},
    {"producer": "Tyrrell's", "wine": "Vat 47 Hunter Chardonnay",
     "url": "https://www.tyrrells.com.au/wines/vat-47-hunter-chardonnay/", "tier": "rich"},
    {"producer": "Penfolds", "wine": "Bin 2 Shiraz Mourvedre",
     "url": "https://www.penfolds.com/en-us/wines/penfolds-bin-2-shiraz-mourvedre", "tier": "rich"},
    {"producer": "Penfolds", "wine": "Bin 389 Cabernet Shiraz",
     "url": "https://www.penfolds.com/en-us/wines/penfolds-bin-389-cabernet-shiraz", "tier": "rich"},
    {"producer": "Vasse Felix", "wine": "Filius Cabernet Sauvignon",
     "url": "https://www.vassefelix.com.au/wines/filius/filius-cabernet-sauvignon", "tier": "moderate"},
    {"producer": "Vasse Felix", "wine": "Filius Chardonnay",
     "url": "https://www.vassefelix.com.au/wines/filius/filius-chardonnay", "tier": "moderate"},
    {"producer": "Plantagenet", "wine": "Omrah Shiraz",
     "url": "https://www.plantagenetwines.com/wines/omrah/shiraz/", "tier": "sparse"},

    # ── South American ──
    {"producer": "Catena Zapata", "wine": "High Mountain Vines Malbec",
     "url": "https://www.catenawines.com/catena-zapata/high-mountain-vines-malbec", "tier": "moderate"},
    {"producer": "Montes", "wine": "Alpha M",
     "url": "https://www.monteswines.com/en/wines/icon/alpha-m/", "tier": "moderate"},
    {"producer": "Montes", "wine": "Alpha Cabernet Sauvignon",
     "url": "https://www.monteswines.com/en/wines/premium/alpha-cabernet-sauvignon/", "tier": "moderate"},

    # ── Budget producers (sparse data, tests null discipline) ──
    {"producer": "Barefoot", "wine": "Buttery Chardonnay",
     "url": "https://www.barefootwine.com/wines/chardonnay/buttery-chardonnay/", "tier": "sparse"},
    {"producer": "Barefoot", "wine": "Pinot Grigio",
     "url": "https://www.barefootwine.com/wines/pinot-grigio/pinot-grigio/", "tier": "sparse"},
    {"producer": "Yellow Tail", "wine": "Shiraz",
     "url": "https://www.yellowtailwine.com/wines/shiraz/", "tier": "sparse"},
    {"producer": "Yellow Tail", "wine": "Chardonnay",
     "url": "https://www.yellowtailwine.com/wines/chardonnay/", "tier": "sparse"},
    {"producer": "Black Box", "wine": "Cabernet Sauvignon",
     "url": "https://www.blackboxwines.com/cabernet-sauvignon", "tier": "sparse"},

    # ── NZ ──
    {"producer": "Oyster Bay", "wine": "Marlborough Sauvignon Blanc",
     "url": "https://www.oysterbaywines.com/our-wines/sauvignon-blanc/", "tier": "moderate"},
    {"producer": "Oyster Bay", "wine": "Marlborough Pinot Noir",
     "url": "https://www.oysterbaywines.com/our-wines/pinot-noir/", "tier": "moderate"},

    # ── More US for diversity ──
    {"producer": "Ridge Vineyards", "wine": "Monte Bello",
     "url": "https://www.ridgewine.com/wines/monte-bello/", "tier": "rich"},
    {"producer": "Ridge Vineyards", "wine": "Lytton Springs",
     "url": "https://www.ridgewine.com/wines/lytton-springs/", "tier": "rich"},
    {"producer": "Ridge Vineyards", "wine": "Three Valleys",
     "url": "https://www.ridgewine.com/wines/three-valleys/", "tier": "rich"},
    {"producer": "Stag's Leap Wine Cellars", "wine": "Cask 23",
     "url": "https://www.cask23.com/wines/cask-23/", "tier": "rich"},
    {"producer": "Bonny Doon", "wine": "Le Cigare Volant",
     "url": "https://www.bonnydoonvineyard.com/wines/le-cigare-volant/", "tier": "moderate"},

    # ── Italy ──
    {"producer": "Terre del Barolo", "wine": "Arnaldo Rivera Barolo",
     "url": "https://www.terredelbarolo.com/en/wines/arnaldo-rivera/", "tier": "moderate"},

    # ── More sparse for null testing ──
    {"producer": "Bota Box", "wine": "Nighthawk Black",
     "url": "https://www.botabox.com/wines/nighthawk-black/", "tier": "sparse"},
    {"producer": "Layer Cake", "wine": "Cabernet Sauvignon",
     "url": "https://www.layercakewines.com/wine/cabernet-sauvignon/", "tier": "sparse"},
    {"producer": "Cupcake Vineyards", "wine": "Pinot Noir",
     "url": "https://www.cupcakevineyards.com/wines/pinot-noir/", "tier": "sparse"},
    {"producer": "Cupcake Vineyards", "wine": "Sauvignon Blanc",
     "url": "https://www.cupcakevineyards.com/wines/sauvignon-blanc/", "tier": "sparse"},
]


def fetch_pages():
    """Fetch HTML for all manifest entries."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    results = []
    for i, entry in enumerate(MANIFEST):
        page_id = f"page_{i+1:03d}"
        html_file = OUT_DIR / f"{page_id}.html"
        meta_file = OUT_DIR / f"{page_id}_meta.json"

        if html_file.exists() and html_file.stat().st_size > 500:
            print(f"  [{page_id}] SKIP (exists): {entry['producer']} — {entry['wine']}")
            results.append({"page_id": page_id, "status": "cached", **entry})
            continue

        url = entry["url"]
        print(f"  [{page_id}] Fetching: {entry['producer']} — {entry['wine']}")
        print(f"           URL: {url}")

        try:
            resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            status = resp.status_code

            if status == 200 and len(resp.text) > 500:
                with open(html_file, "w", encoding="utf-8") as f:
                    f.write(resp.text)

                meta = {
                    "page_id": page_id,
                    "producer": entry["producer"],
                    "wine": entry["wine"],
                    "url": url,
                    "final_url": resp.url,
                    "status_code": status,
                    "content_length": len(resp.text),
                    "tier": entry["tier"],
                }
                with open(meta_file, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)

                print(f"           OK ({len(resp.text):,} bytes)")
                results.append({"page_id": page_id, "status": "ok", **entry,
                               "size": len(resp.text)})
            else:
                print(f"           FAIL (status={status}, size={len(resp.text)})")
                results.append({"page_id": page_id, "status": f"fail_{status}", **entry})

        except Exception as e:
            print(f"           ERROR: {e}")
            results.append({"page_id": page_id, "status": f"error_{type(e).__name__}", **entry})

        time.sleep(1)  # Polite delay

    # Summary
    ok = sum(1 for r in results if r["status"] in ("ok", "cached"))
    fail = len(results) - ok
    print(f"\n  Total: {len(results)} | OK: {ok} | Failed: {fail}")

    # Save manifest with results
    manifest_file = OUT_DIR / "manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"  Manifest saved to {manifest_file}")

    return results


def print_manifest():
    """Print the scrape manifest for review."""
    tier_counts = {}
    for entry in MANIFEST:
        tier_counts[entry["tier"]] = tier_counts.get(entry["tier"], 0) + 1

    print(f"\nTask 2 Scrape Manifest: {len(MANIFEST)} pages")
    print(f"  Tiers: {tier_counts}")
    print()

    producers = {}
    for entry in MANIFEST:
        producers.setdefault(entry["producer"], []).append(entry)

    for producer, entries in sorted(producers.items()):
        print(f"  {producer} ({len(entries)} pages):")
        for e in entries:
            print(f"    [{e['tier']}] {e['wine']}")
            print(f"         {e['url']}")


def main():
    parser = argparse.ArgumentParser(description="Build Task 2 test data")
    parser.add_argument("--fetch", action="store_true", help="Fetch HTML pages")
    parser.add_argument("--manifest", action="store_true", help="Print manifest")
    args = parser.parse_args()

    if args.manifest or not args.fetch:
        print_manifest()

    if args.fetch:
        fetch_pages()


if __name__ == "__main__":
    main()
