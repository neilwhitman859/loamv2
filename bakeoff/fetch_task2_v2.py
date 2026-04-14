#!/usr/bin/env python3
"""Fetch Task 2 v2 pages from corrected, diverse URLs."""

import json
import os
import sys
import time
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"

import requests
from bs4 import BeautifulSoup

OUT_DIR = Path(__file__).parent / "data" / "task2" / "v2"
OUT_DIR.mkdir(parents=True, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Curated manifest: max 4 per producer, diverse and data-rich
MANIFEST = [
    # Tablas Creek: rich tech sheets (6)
    ("Tablas Creek", "Esprit de Tablas", "https://tablascreek.com/wines/2023_esprit_de_tablas"),
    ("Tablas Creek", "Mourvedre", "https://tablascreek.com/wines/2023_mourvedre"),
    ("Tablas Creek", "Roussanne", "https://tablascreek.com/wines/2024_roussanne"),
    ("Tablas Creek", "Patelin de Tablas", "https://tablascreek.com/wines/2023_patelin_de_tablas"),
    ("Tablas Creek", "Panoplie", "https://tablascreek.com/wines/2023_panoplie"),
    ("Tablas Creek", "Dianthus Rose", "https://tablascreek.com/wines/2025_dianthus"),
    # J. Lohr: rich tech sheets (5)
    ("J. Lohr", "Hilltop Cabernet Sauvignon", "https://www.jlohr.com/wines/2023-hilltop-cabernet-sauvignon"),
    ("J. Lohr", "Arroyo Vista Chardonnay", "https://www.jlohr.com/wines/2024-arroyo-vista-chardonnay"),
    ("J. Lohr", "Fogs Reach Pinot Noir", "https://www.jlohr.com/wines/2023-fogs-reach-pinot-noir"),
    ("J. Lohr", "Seven Oaks Cabernet Sauvignon", "https://www.jlohr.com/wines/2023-seven-oaks-cabernet-sauvignon"),
    ("J. Lohr", "Gesture Viognier", "https://www.jlohr.com/wines/2024-gesture-viognier"),
    # Cakebread: corrected URLs (3)
    ("Cakebread Cellars", "Chardonnay", "https://www.cakebread.com/2023-chardonnay-napa-valley-1CH0723.html"),
    ("Cakebread Cellars", "Cabernet Sauvignon", "https://www.cakebread.com/2022-cabernet-sauvignon-napa-valley-1CS0722.html"),
    ("Cakebread Cellars", "Two Creeks Pinot Noir", "https://www.cakebread.com/2023-two-creeks-pinot-noir%2C-anderson-valley-1PNT0723.html"),
    # Duckhorn: shop domain (3)
    ("Duckhorn", "Stags Leap Cabernet", "https://www.duckhornwineshop.com/product/2023-duckhorn-vineyards-napa-valley-stags-leap-district-cabernet-sauvignon"),
    ("Duckhorn", "50th Anniversary Merlot", "https://www.duckhornwineshop.com/product/2023-duckhorn-vineyards-50th-anniversary-napa-valley-merlot"),
    ("Duckhorn", "Huichica Hills Chardonnay", "https://www.duckhornwineshop.com/product/2024-duckhorn-vineyards-napa-valley-carneros-chardonnay-huichica-hills-vineyard"),
    # Fetzer (3)
    ("Fetzer", "Cabernet Sauvignon", "https://fetzer.com/wines/cabernet-sauvignon/"),
    ("Fetzer", "Gewurztraminer", "https://fetzer.com/wines/gewurztraminer/"),
    ("Fetzer", "Riesling", "https://fetzer.com/wines/riesling/"),
    # Shafer: corrected (1)
    ("Shafer Vineyards", "One Point Five", "https://shafervineyards.com/wine/one-point-five.php"),
    # Corrected budget producers
    ("Barefoot", "Buttery Chardonnay", "https://www.barefootwine.com/buttery-chardonnay.html"),
    ("Yellow Tail", "Shiraz", "https://www.yellowtailwine.com/en-us/product/shiraz/"),
    ("Black Box", "Cabernet Sauvignon", "https://www.blackboxwines.com/cabernet-sauvignon.html"),
    # Corrected international
    ("d'Arenberg", "The Dead Arm Shiraz", "https://www.darenberg.com.au/2019-the-dead-arm-shiraz"),
    ("Vasse Felix", "Filius Cabernet Sauvignon", "https://www.vassefelix.com.au/product/2022-Filius-Cabernet-Sauvignon"),
    ("Oyster Bay", "Sauvignon Blanc", "https://www.oysterbaywines.com/us/our-wines/marlborough-sauvignon-blanc/"),
    ("Kir-Yianni", "Ramnista", "https://kiryianni.gr/wines/ramnista/"),
    ("Bonny Doon", "Le Cigare Volant", "https://www.bonnydoonvineyard.com/product/2023-bonny-doon-le-cigare-volant/"),
    ("Penfolds", "Bin 389", "https://www.penfolds.com/en-us/our-wines/featured-wines/bin-389"),
    # Best from v1
    ("Ridge Vineyards", "Monte Bello", "https://www.ridgewine.com/wines/monte-bello/"),
    ("Ridge Vineyards", "Lytton Springs", "https://www.ridgewine.com/wines/lytton-springs/"),
    ("Bonterra", "Cabernet Sauvignon", "https://www.bonterra.com/wines/cabernet-sauvignon/"),
    ("Bonterra", "Pinot Noir", "https://www.bonterra.com/wines/pinot-noir/"),
    ("Bonterra", "Zinfandel", "https://www.bonterra.com/wines/zinfandel/"),
    ("Bogle", "Old Vine Zinfandel", "https://www.boglewinery.com/wines/old-vine-zinfandel/"),
    ("Bogle", "Petite Sirah", "https://www.boglewinery.com/wines/petite-sirah/"),
    ("La Crema", "Monterey Chardonnay", "https://www.lacrema.com/wines/monterey-chardonnay/"),
    ("La Crema", "Sonoma Coast Pinot Noir", "https://www.lacrema.com/wines/sonoma-coast-pinot-noir/"),
    ("Meiomi", "Pinot Noir", "https://www.meiomi.com/wines/pinot-noir/"),
    ("Cupcake Vineyards", "Pinot Noir", "https://www.cupcakevineyards.com/wines/pinot-noir/"),
    ("Cupcake Vineyards", "Prosecco", "https://www.cupcakevineyards.com/wines/prosecco/"),
    # More diversity: Kendall-Jackson, Dark Horse, Sutter Home
    ("Kendall-Jackson", "Grand Reserve Cab", "https://www.kj.com/wines/grand-reserve-cabernet-sauvignon"),
    ("Dark Horse", "Cabernet Sauvignon", "https://www.darkhorsewine.com/wines/cabernet-sauvignon"),
    ("Sutter Home", "White Zinfandel", "https://www.sutterhome.com/wines/white-zinfandel/"),
    ("Leo Hillinger", "Blaufrankisch", "https://www.leo-hillinger.com/en/wines/blaufraenkisch/"),
    ("Bedrock Wine", "Old Vine Zinfandel", "https://www.bedrockwineco.com/wines/old-vine-zinfandel/"),
    ("Jordan Winery", "Chardonnay", "https://www.jordanwinery.com/wines/chardonnay/"),
]

print(f"Manifest: {len(MANIFEST)} pages from {len(set(m[0] for m in MANIFEST))} producers\n")

results = []
for i, (producer, wine, url) in enumerate(MANIFEST):
    page_id = f"page_{i+1:03d}"
    html_file = OUT_DIR / f"{page_id}.html"
    meta_file = OUT_DIR / f"{page_id}_meta.json"

    if html_file.exists() and html_file.stat().st_size > 500:
        results.append({"page_id": page_id, "status": "cached",
                       "producer": producer, "wine": wine, "url": url})
        print(f"  [{page_id}] CACHE {producer} -- {wine}")
        continue

    try:
        resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        if resp.status_code == 200 and len(resp.text) > 500:
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator=" ", strip=True)
            if len(text) > 100:
                with open(html_file, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                meta = {
                    "page_id": page_id, "producer": producer, "wine": wine,
                    "url": url, "final_url": resp.url,
                    "status_code": 200, "content_length": len(resp.text),
                }
                with open(meta_file, "w") as f:
                    json.dump(meta, f, indent=2)
                has_data = any(kw in text.lower() for kw in
                              ["abv", "alcohol", "blend", "oak", "barrel", "ph "])
                print(f"  [{page_id}] OK {producer} -- {wine} "
                      f"({len(resp.text):,}b, data={has_data})")
                results.append({"page_id": page_id, "status": "ok",
                               "producer": producer, "wine": wine, "url": url,
                               "size": len(resp.text)})
            else:
                print(f"  [{page_id}] JS {producer} -- {wine}")
                results.append({"page_id": page_id, "status": "js",
                               "producer": producer, "wine": wine, "url": url})
        else:
            print(f"  [{page_id}] FAIL {producer} -- {wine} ({resp.status_code})")
            results.append({"page_id": page_id, "status": f"fail_{resp.status_code}",
                           "producer": producer, "wine": wine, "url": url})
    except Exception as e:
        print(f"  [{page_id}] ERR {producer} -- {wine} ({type(e).__name__})")
        results.append({"page_id": page_id, "status": "error",
                       "producer": producer, "wine": wine, "url": url})

    time.sleep(0.8)

ok_count = sum(1 for r in results if r["status"] in ("ok", "cached"))
total = len(results)
print(f"\n  OK: {ok_count}/{total}")
print(f"  Producers: {len(set(r['producer'] for r in results if r['status'] in ('ok', 'cached')))}")

with open(OUT_DIR / "manifest.json", "w") as f:
    json.dump(results, f, indent=2)
print(f"  Manifest saved to {OUT_DIR / 'manifest.json'}")
