#!/usr/bin/env python3
"""
Scrape Ridge Vineyards (ridgewine.com) wine catalog and detail pages.

Phase 1: Crawl catalog pages -> collect all wine URLs
Phase 2: Fetch each detail page -> parse structured data
Phase 3: Write to JSONL
Phase 4: Insert JSONL data into DB (--insert)

Usage:
    python -m pipeline.fetch.ridge
    python -m pipeline.fetch.ridge --resume
    python -m pipeline.fetch.ridge --detail-only
    python -m pipeline.fetch.ridge --insert
"""

import argparse
import html
import json
import re
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BASE_URL = "https://www.ridgewine.com"
CATALOG_URL = f"{BASE_URL}/wines/"
OUTPUT_FILE = Path("ridge_wines.jsonl")
URLS_FILE = Path("ridge_urls.json")
PROGRESS_FILE = Path("ridge_progress.json")
DELAY_S = 4.0
CATALOG_DELAY_S = 2.0

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)

GRAPE_ALIASES: dict[str, str] = {
    "carignane": "Carignan", "carignan": "Carignan",
    "mataro": "Mourvèdre", "mataró": "Mourvèdre",
    "mourvèdre": "Mourvèdre", "mourvedre": "Mourvèdre",
    "mataro (mourvedre)": "Mourvèdre", "mataro (mourvèdre)": "Mourvèdre",
    "mataró (mourvèdre)": "Mourvèdre",
    "petit verdot": "Petit Verdot", "petit verdo": "Petit Verdot", "petite verdot": "Petit Verdot",
    "petire sirah": "Petite Sirah", "petite sirah": "Petite Sirah",
    "alicante bouschet": "Alicante Bouschet", "alicante bouchet": "Alicante Bouschet", "alicante": "Alicante Bouschet",
    "cabernet sauvignon": "Cabernet Sauvignon", "cabernet franc": "Cabernet Franc", "franc": "Cabernet Franc",
    "grenache blanc": "Grenache Blanc", "grenache": "Grenache",
    "chenin blanc": "Chenin Blanc", "chardonnay": "Chardonnay",
    "zinfandel": "Zinfandel", "syrah": "Syrah", "merlot": "Merlot",
    "pinot noir": "Pinot Noir", "primitivo": "Primitivo",
    "gamay noir": "Gamay", "gamay": "Gamay", "cinsaut": "Cinsaut", "counoise": "Counoise",
    "falanghina": "Falanghina", "valdiguié": "Valdiguie", "valdiguie": "Valdiguie",
    "teroldego": "Teroldego", "viognier": "Viognier", "malbec": "Malbec",
    "barbera": "Barbera", "sangiovese": "Sangiovese", "roussanne": "Roussanne",
    "picpoul": "Picpoul", "vermentino": "Vermentino",
    "semillon": "Sémillon", "sémillon": "Sémillon", "muscadelle": "Muscadelle",
    "ruby cabernet": "Ruby Cabernet", "charbono": "Charbono", "peloursin": "Peloursin",
    "grand noir": "Grand Noir", "lenoir": "Lenoir", "palomino": "Palomino",
    "black malvoisie": "Cinsaut", "burger": "Burger",
}

APPELLATION_ALIASES: dict[str, str] = {
    "moon mountain": "moon mountain district",
    "santa cruz county": "santa cruz mountains",
    "adelaida district": "paso robles",
    "san louis obispo county": "paso robles", "san luis obispo county": "paso robles",
    "napa county": "napa valley", "nap county": "napa valley",
    "calistoga, napa valley": "calistoga",
    "spring mountain": "spring mountain district",
    "spring mountain, napa county": "spring mountain district",
    "spring mountain, napa valley": "spring mountain district",
    "howell mountain, napa county": "howell mountain",
    "oakville, napa valley": "oakville", "rutherford, napa valley": "rutherford",
    "york creek, napa county": "napa valley",
    "dry creek, sonoma county": "dry creek valley",
    "dry creek valley, sonoma": "dry creek valley",
    "dry creek, alexander, and russian river valleys": "sonoma county",
    "the hills and bench land separating dry creek and alexander valleys": "sonoma county",
    "sonoma": "sonoma county",
    "foothills amador county": "amador county", "amador foothills": "amador county",
    "san francisco bay region": "livermore valley",
}

PUB_ALIASES: dict[str, str] = {
    "jamessuckling.com": "james suckling",
    "robert parker wine advocate": "wine advocate",
    "the wine advocate": "wine advocate",
    "rober parker wine advocate": "wine advocate",
    "vinous media": "vinous", "vinous": "vinous",
    "wine spectator": "wine spectator", "wine specator": "wine spectator",
    "winespectator": "wine spectator",
    "wine enthusiast": "wine enthusiast", "wineenthusiast.com": "wine enthusiast",
    "decanter": "decanter", "decanter.com": "decanter", "decanter magazine": "decanter",
    "owen bargreen": "owenbargreen.com", "owen bargreen.com": "owenbargreen.com",
    "owen bargeen": "owenbargreen.com",
}


def normalize(s: str) -> str:
    import unicodedata
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ",
        unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower())).strip()


def slugify(s: str) -> str:
    import unicodedata
    return re.sub(r"^-|-$", "", re.sub(r"[^a-z0-9]+", "-",
        unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower()))


def normalize_grape_name(name: str) -> str:
    n = html.unescape(name)
    n = re.sub(r"\s*\u2013\s*Organically Grown", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s*\u2013\s*Picchetti.*$", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s*\u2013\s*Jimsomare.*$", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s*\(Mourv[eè]dre\)", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s*\(Primitivo\)", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+\d+%.*$", "", n, flags=re.IGNORECASE)
    return n.strip()


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"lastDetailIndex": -1, "catalogDone": False}


def save_progress(data: dict):
    data["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    PROGRESS_FILE.write_text(json.dumps(data))


def fetch_page(client: httpx.Client, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            resp = client.get(url)
            if resp.status_code == 429:
                time.sleep(30)
                continue
            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                print(f"  HTTP {resp.status_code} for {url} (attempt {attempt + 1})")
                time.sleep(10)
                continue
            return resp.text
        except Exception as err:
            print(f"  Fetch error for {url}: {err} (attempt {attempt + 1})")
            time.sleep(10)
    print(f"  Failed after {retries} attempts: {url}")
    return None


# -- Parsing --

def parse_grape_composition(text: str) -> list[dict]:
    grapes = []
    for m in re.finditer(r"(\d+)%\s+([^,]+)", text):
        grapes.append({"percentage": int(m.group(1)), "grape": m.group(2).strip()})
    return grapes


def parse_scores(html_text: str) -> list[dict]:
    normalized = html_text.replace("&#8211;", "\u2013").replace("&ndash;", "\u2013")
    scores = []

    for m in re.finditer(
        r"<b>(\d+)\s*Points?\s*(?:\+\s*([^<]+))?</b>\s*\u2013\s*(?:([^,<]+),\s*)?<em>\s*([^<]+)</em>",
        normalized, re.IGNORECASE,
    ):
        scores.append({
            "score": int(m.group(1)),
            "designation": m.group(2).strip() if m.group(2) else None,
            "critic": m.group(3).strip() if m.group(3) else None,
            "publication": m.group(4).strip(),
        })

    for m in re.finditer(
        r"<b>(\d+)\s*Points?\s*(?:\+\s*([^<]+))?</b>\s*\u2013\s*([^<\n]+?)(?:<br|</p|$)",
        normalized, re.IGNORECASE,
    ):
        critic = m.group(3).strip().rstrip(",")
        if any(s["score"] == int(m.group(1)) and (s.get("critic") == critic or s.get("publication") == critic) for s in scores):
            continue
        scores.append({
            "score": int(m.group(1)),
            "designation": m.group(2).strip() if m.group(2) else None,
            "critic": critic,
            "publication": None,
        })

    return scores


def parse_winemaking(text: str) -> dict:
    data: dict = {}
    for field, pattern in [
        ("harvestDates", r"Harvest Dates?:\s*(.+?)(?:\n|$)"),
        ("brix", r"(?:Average\s+)?Brix[:\s]*(\d+\.?\d*)"),
        ("ta", r"TA:\s*(\d+\.?\d*)\s*g/L"),
        ("ph", r"pH:\s*(\d+\.?\d*)"),
        ("barrels", r"Barrels?:\s*(.+?)(?:\n|$)"),
        ("aging", r"Aging:\s*(.+?)(?:\n|$)"),
        ("newOakPct", r"(\d+)%\s*new\s*oak"),
        ("fermentation", r"Fermentation:\s*(.+?)(?:\n|$)"),
        ("selection", r"Selection:\s*(.+?)(?:\n|$)"),
        ("casesProduced", r"(\d[\d,]*)\s*cases?\s*(?:produced|made|bottled)"),
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if field in ("brix", "ta", "ph"):
                data[field] = float(val)
            elif field in ("newOakPct", "casesProduced"):
                data[field] = int(val.replace(",", ""))
            else:
                data[field] = val
    data["fullText"] = text.strip()
    return data


def parse_growing_season(text: str) -> dict:
    data: dict = {}
    for field, pattern in [
        ("rainfall", r"Rainfall:\s*(.+?)(?:\n|$)"),
        ("bloom", r"Bloom:\s*(.+?)(?:\n|$)"),
        ("weather", r"Weather:\s*(.+)"),
    ]:
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if m:
            data[field] = m.group(1).strip()
    data["fullText"] = text.strip()
    return data


def parse_price(html_text: str) -> float | None:
    for pattern in [
        r"Item\s*Price\s*\$(\d+\.?\d*)",
        r"\$(\d+\.?\d*)\s*/??\s*750\s*ml",
        r'class="[^"]*price[^"]*"[^>]*>\s*\$(\d+\.?\d*)',
    ]:
        m = re.search(pattern, html_text, re.IGNORECASE)
        if m:
            return float(m.group(1))
    return None


def parse_detail_page(html_text: str, url: str) -> dict:
    data: dict = {
        "url": url, "title": None, "vintage": None, "wineName": None,
        "grapes": [], "scores": [], "vineyard": None, "appellation": None,
        "abv": None, "price": None, "winemakerNotes": None, "vintageNotes": None,
        "history": None, "growingSeason": None, "winemaking": None, "membersOnly": False,
    }

    # Title
    title_match = re.search(r'<h1[^>]*class="membersOverlay-title"[^>]*>([^<]+)</h1>', html_text, re.IGNORECASE)
    if not title_match:
        title_match = re.search(r"<h1[^>]*>(\d{4}\s+[^<]+)</h1>", html_text, re.IGNORECASE)
    if not title_match:
        page_title = re.search(r"<title>([^<]+)\s*-\s*Ridge Vineyards</title>", html_text, re.IGNORECASE)
        if page_title:
            title_match = page_title
    if title_match:
        data["title"] = title_match.group(1).strip()
        year_match = re.match(r"^(\d{4})\s+(.+)$", data["title"])
        if year_match:
            data["vintage"] = int(year_match.group(1))
            data["wineName"] = year_match.group(2).strip()
        else:
            data["wineName"] = data["title"]

    if "membersOverlay-left" in html_text and "MEMBERS" in html_text:
        data["membersOnly"] = True

    # Wine info
    wine_info_match = re.search(r'<div class="wineInfo">([\s\S]*?)</div>', html_text, re.IGNORECASE)
    if wine_info_match:
        info_html = wine_info_match.group(1)
        first_p = re.search(r"<p>([^<]*?%[^<]*?)</p>", info_html)
        if first_p:
            data["grapes"] = parse_grape_composition(first_p.group(1))
        data["scores"] = parse_scores(info_html)

    # Labeled fields
    for m in re.finditer(
        r"<h3[^>]*>\s*(Vintage|Vineyard|Appellation|Alcohol By Volume|Price|Drinking Window)\s*</h3>\s*(?:<p>)?\s*([^<]+)",
        html_text, re.IGNORECASE,
    ):
        field, value = m.group(1).strip(), m.group(2).strip()
        if field == "Vintage":
            data["vintage"] = data["vintage"] or int(value)
        elif field == "Vineyard":
            data["vineyard"] = value
        elif field == "Appellation":
            data["appellation"] = value
        elif field == "Alcohol By Volume":
            data["abv"] = float(value.replace("%", ""))
        elif field == "Price":
            data["price"] = float(re.sub(r"[^0-9.]", "", value))
        elif field == "Drinking Window":
            data["drinkingWindow"] = value

    # Accordion sections
    for m in re.finditer(
        r'<div class="accordion"[^>]*>\s*<h3[^>]*>([\s\S]*?)</h3>\s*<div class="accordion-content"[^>]*>([\s\S]*?)</div>\s*</div>',
        html_text, re.IGNORECASE,
    ):
        section = re.sub(r"<[^>]+>", "", m.group(1)).strip()
        content = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(2))).strip()
        if section == "Winemaker Tasting Notes":
            data["winemakerNotes"] = re.sub(r"\s*[A-Z]{1,3}\s*\(\d+/\d+\)\s*$", "", content).strip()
        elif section == "Vintage Notes":
            data["vintageNotes"] = re.sub(r"\s*[A-Z]{1,3}\s*\(\d+/\d+\)\s*$", "", content).strip()
        elif section == "History":
            data["history"] = content[:2000]
        elif section == "Growing Season":
            data["growingSeason"] = parse_growing_season(content)
        elif section == "Winemaking":
            data["winemaking"] = parse_winemaking(content)
        elif section == "Food Pairings":
            if content and "See all food pairing" not in content:
                data["foodPairings"] = content

    if not data["price"]:
        data["price"] = parse_price(html_text)

    return data


# -- Catalog Discovery --

def discover_wine_urls(client: httpx.Client) -> list[str]:
    print("Phase 1: Discovering wine URLs from catalog...")
    all_urls: list[str] = []
    page_no = 1

    while True:
        url = CATALOG_URL if page_no == 1 else f"{CATALOG_URL}?pageNo={page_no}"
        print(f"  Page {page_no}...", end="")
        page_html = fetch_page(client, url)
        if not page_html:
            print(" empty response, stopping.")
            break

        page_urls: list[str] = []
        for pattern in [
            r'class="wineItem[^"]*"[^>]*data="([^"]+)"',
            r'data="(https://www\.ridgewine\.com/wines/[^"]+)"[^>]*class="wineItem',
        ]:
            for m in re.finditer(pattern, page_html):
                if m.group(1) not in page_urls:
                    page_urls.append(m.group(1))

        if not page_urls:
            print(" no wines found, stopping.")
            break

        print(f" {len(page_urls)} wines")
        all_urls.extend(page_urls)

        if "Next Page" not in page_html:
            print("  No 'Next Page' link -- last page reached.")
            break

        page_no += 1
        time.sleep(CATALOG_DELAY_S)

    print(f"\nDiscovered {len(all_urls)} wine URLs across {page_no} pages")
    URLS_FILE.write_text(json.dumps(all_urls, indent=2))
    return all_urls


# -- Varietal Classification --

def classify_varietal(grapes: list[dict], wine_name: str) -> str:
    if not grapes:
        name = wine_name.lower()
        varietal_map = [
            ("zinfandel", "Zinfandel"), ("cabernet sauvignon", "Cabernet Sauvignon"),
            ("cabernet franc", "Cabernet Franc"), ("chardonnay", "Chardonnay"),
            ("petite sirah", "Petite Sirah"), ("syrah", "Syrah"), ("pinot noir", "Pinot Noir"),
            ("grenache blanc", "Grenache Blanc"), ("grenache", "Grenache"), ("merlot", "Merlot"),
            ("primitivo", "Primitivo"), ("gamay", "Gamay"), ("falanghina", "Falanghina"),
            ("valdiguié", "Valdiguie"), ("valdiguie", "Valdiguie"),
            ("chenin blanc", "Chenin Blanc"), ("teroldego", "Teroldego"),
            ("rosé", "Rosé Blend"), ("rose", "Rosé Blend"), ("blanc", "White Blend"),
        ]
        for kw, cat in varietal_map:
            if kw in name:
                return cat
        return "Red Blend"

    primary = grapes[0]
    primary_name = primary["grape"].lower()
    pct = primary["percentage"]

    if pct >= 75:
        for kw, cat in [("zinfandel", "Zinfandel"), ("cabernet sauvignon", "Cabernet Sauvignon"),
                         ("cabernet franc", "Cabernet Franc"), ("chardonnay", "Chardonnay"),
                         ("petite sirah", "Petite Sirah"), ("syrah", "Syrah"), ("merlot", "Merlot"),
                         ("pinot noir", "Pinot Noir"), ("grenache blanc", "Grenache Blanc"),
                         ("grenache", "Grenache")]:
            if kw in primary_name:
                return cat

    bordeaux = ["cabernet sauvignon", "merlot", "cabernet franc", "petit verdot", "malbec"]
    if all(any(bg in g["grape"].lower() for bg in bordeaux) for g in grapes):
        if any(any(bg in g["grape"].lower() for bg in bordeaux) for g in grapes):
            return "Bordeaux Blend"

    rhone = ["syrah", "grenache", "mourvèdre", "mourvedre", "mataro", "viognier"]
    if any(any(rg in g["grape"].lower() for rg in rhone) for g in grapes):
        if "syrah" in primary_name and pct >= 50:
            return "Syrah"
        if "grenache" in primary_name and pct >= 50:
            return "Grenache"
        return "Rhône Blend"

    if "zinfandel" in primary_name and pct >= 50:
        return "Zinfandel"

    whites = ["chardonnay", "chenin blanc", "grenache blanc", "falanghina", "viognier"]
    if any(wg in primary_name for wg in whites):
        if pct >= 75:
            if "chardonnay" in primary_name:
                return "Chardonnay"
            if "grenache blanc" in primary_name:
                return "Grenache Blanc"
        return "White Blend"

    if "rosé" in wine_name.lower() or "rose" in wine_name.lower():
        return "Rosé Blend"

    return "Red Blend"


# -- DB Insertion --

def insert_data():
    """Insert scraped JSONL data into Supabase."""
    from pipeline.lib.db import get_supabase, fetch_all

    print("\n=== Phase 3: DB Insertion ===\n")

    if not OUTPUT_FILE.exists():
        print(f"No {OUTPUT_FILE} found. Run scraper first.")
        sys.exit(1)

    lines = OUTPUT_FILE.read_text().strip().split("\n")
    wines = [json.loads(l) for l in lines if l.strip()]
    wines = [w for w in wines if w.get("wineName")]
    print(f"Loaded {len(wines)} wines from {OUTPUT_FILE}")

    sb = get_supabase()

    # Reference data
    us_country = sb.from_("countries").select("id").ilike("name", "%United States%").limit(1).execute().data[0]
    country_id = us_country["id"]

    ca_region = sb.from_("regions").select("id").ilike("name", "%California%").limit(1).execute().data[0]
    region_id = ca_region["id"]

    appellations = fetch_all(sb, "appellations", "id,name")
    appellation_map = {a["name"].lower(): a["id"] for a in appellations}

    grapes_ref = fetch_all(sb, "grapes", "id,name")
    grape_map = {g["name"].lower(): g["id"] for g in grapes_ref}

    varietals = fetch_all(sb, "varietal_categories", "id,name,slug")
    varietal_map = {v["name"].lower(): v["id"] for v in varietals}
    for v in varietals:
        varietal_map[v["slug"]] = v["id"]

    source_types = sb.from_("source_types").select("id,slug").execute().data
    winery_source_id = next((s["id"] for s in source_types if s["slug"] == "winery-website"), None)

    publications = fetch_all(sb, "publications", "id,name,slug")
    pub_map = {p["name"].lower(): p["id"] for p in publications}
    for p in publications:
        pub_map[p["slug"]] = p["id"]
    for alias, canonical in PUB_ALIASES.items():
        if canonical in pub_map:
            pub_map[alias] = pub_map[canonical]

    # Create producer
    print("\nCreating Ridge Vineyards producer...")
    existing = sb.from_("producers").select("id").eq("slug", "ridge-vineyards").execute().data
    if existing:
        producer_id = existing[0]["id"]
        print(f"  Using existing producer: {producer_id}")
    else:
        producer_id = str(uuid.uuid4())
        sb.from_("producers").insert({
            "id": producer_id, "slug": "ridge-vineyards", "name": "Ridge Vineyards",
            "name_normalized": normalize("Ridge Vineyards"),
            "country_id": country_id, "website_url": "https://www.ridgewine.com",
            "year_established": 1962,
            "metadata": {
                "winemaking_philosophy": "Pre-industrial winemaking: native yeasts, natural malolactic, air-dried American oak, minimum effective sulfur",
                "regions": ["Santa Cruz Mountains", "Sonoma County", "Paso Robles"],
            },
        }).execute()
        print(f"  Producer created: {producer_id}")

    # Group by wine name
    wines_by_name: dict[str, list[dict]] = {}
    for w in wines:
        key = w["wineName"]
        wines_by_name.setdefault(key, []).append(w)
    print(f"\n{len(wines_by_name)} unique wine names, {len(wines)} total vintages")

    # Create wines
    print("\nCreating wine records...")
    wine_id_map: dict[str, str] = {}
    wine_count = 0

    for wine_name, vintages in wines_by_name.items():
        latest = sorted(vintages, key=lambda v: v.get("vintage") or 0, reverse=True)[0]
        app_name = (latest.get("appellation") or "").lower()
        app_lookup = APPELLATION_ALIASES.get(app_name, app_name)
        appellation_id = appellation_map.get(app_lookup)

        varietal_name = classify_varietal(latest.get("grapes", []), wine_name)
        varietal_id = varietal_map.get(varietal_name.lower()) or varietal_map.get(slugify(varietal_name))
        if not varietal_id:
            varietal_id = varietal_map.get("red-blend") or varietal_map.get("red blend")

        wine_id = str(uuid.uuid4())
        try:
            sb.from_("wines").insert({
                "id": wine_id, "slug": slugify(f"ridge {wine_name}"),
                "name": wine_name, "name_normalized": normalize(wine_name),
                "producer_id": producer_id, "country_id": country_id,
                "region_id": region_id, "appellation_id": appellation_id,
                "varietal_category_id": varietal_id,
                "varietal_category_source": winery_source_id,
                "food_pairings": latest.get("foodPairings"),
            }).execute()
            wine_id_map[wine_name] = wine_id
            wine_count += 1
        except Exception as err:
            print(f'  Wine "{wine_name}" error: {err}')

    print(f"  Created {wine_count} wines")

    # Create grape compositions
    print("\nCreating grape compositions...")
    grape_count = 0
    for wine_name, vintages in wines_by_name.items():
        wine_id = wine_id_map.get(wine_name)
        if not wine_id:
            continue
        latest = sorted(vintages, key=lambda v: v.get("vintage") or 0, reverse=True)[0]
        for g in latest.get("grapes", []):
            cleaned = normalize_grape_name(g["grape"])
            grape_name = GRAPE_ALIASES.get(cleaned.lower(), cleaned)
            grape_id = grape_map.get(grape_name.lower())
            if not grape_id:
                print(f'    Unknown grape: "{g["grape"]}" (normalized: "{grape_name}")')
                continue
            try:
                sb.from_("wine_grapes").insert({
                    "wine_id": wine_id, "grape_id": grape_id,
                    "percentage": g["percentage"], "percentage_source": winery_source_id,
                }).execute()
                grape_count += 1
            except Exception as err:
                print(f"    Grape insert error: {err}")
    print(f"  Created {grape_count} grape entries")

    # Create vintages
    print("\nCreating vintage records...")
    vintage_count = 0
    MONTH_WORDS = {
        "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
        "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
        "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
        "eighteen": 18, "nineteen": 19, "twenty": 20, "twenty-one": 21, "twenty-two": 22,
        "twenty-three": 23, "twenty-four": 24,
    }

    for w in wines:
        wine_id = wine_id_map.get(w.get("wineName"))
        if not wine_id:
            continue
        wm = w.get("winemaking") or {}
        gs = w.get("growingSeason") or {}

        metadata: dict = {}
        for key, src in [("rainfall", gs), ("bloom", gs), ("growing_season_weather", gs),
                         ("barrels", wm), ("aging", wm), ("fermentation", wm),
                         ("selection", wm), ("winemaking_full", wm)]:
            src_key = "weather" if key == "growing_season_weather" else ("fullText" if key == "winemaking_full" else key)
            if src.get(src_key):
                metadata[key] = src[src_key]
        if w.get("history"):
            metadata["history"] = w["history"]
        if w.get("membersOnly"):
            metadata["members_only"] = True

        oak_months = None
        if wm.get("aging"):
            aging_lower = wm["aging"].lower()
            for word, num in MONTH_WORDS.items():
                if f"{word} month" in aging_lower:
                    oak_months = num
                    break
            num_match = re.search(r"(\d+)\s*months?", wm["aging"], re.IGNORECASE)
            if num_match:
                oak_months = int(num_match.group(1))

        try:
            sb.from_("wine_vintages").insert({
                "wine_id": wine_id, "vintage_year": w.get("vintage"),
                "abv": w.get("abv"), "ph": wm.get("ph"),
                "ta_g_l": wm.get("ta"), "brix_at_harvest": wm.get("brix"),
                "duration_in_oak_months": oak_months, "new_oak_pct": wm.get("newOakPct"),
                "mlf": "Natural",
                "winemaker_notes": w.get("winemakerNotes"),
                "vintage_notes": w.get("vintageNotes"),
                "cases_produced": wm.get("casesProduced"),
                "release_price_usd": w.get("price"),
                "release_price_currency": "USD" if w.get("price") else None,
                "release_price_source": winery_source_id if w.get("price") else None,
                "metadata": metadata or {},
            }).execute()
            vintage_count += 1
        except Exception as err:
            print(f"  Vintage {w.get('wineName')} {w.get('vintage')} error: {err}")

    print(f"  Created {vintage_count} vintages")

    # Create scores
    print("\nCreating score records...")
    score_count = 0
    for w in wines:
        wine_id = wine_id_map.get(w.get("wineName"))
        if not wine_id or not w.get("scores"):
            continue
        for s in w["scores"]:
            pub_id = None
            if s.get("publication"):
                decoded = html.unescape(s["publication"])
                pub_key = decoded.lower().strip()
                pub_id = pub_map.get(pub_key)
                if not pub_id:
                    pub_slug = slugify(decoded)
                    try:
                        result = sb.from_("publications").insert({
                            "slug": pub_slug, "name": decoded, "type": "critic_publication",
                        }).execute()
                        if result.data:
                            pub_id = result.data[0]["id"]
                            pub_map[pub_key] = pub_id
                    except Exception:
                        existing = sb.from_("publications").select("id").eq("slug", pub_slug).execute().data
                        if existing:
                            pub_id = existing[0]["id"]

            try:
                sb.from_("wine_vintage_scores").insert({
                    "wine_id": wine_id, "vintage_year": w.get("vintage"),
                    "score": s["score"], "score_scale": "100",
                    "publication_id": pub_id, "critic": s.get("critic"),
                    "source_id": winery_source_id, "url": w.get("url"),
                    "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }).execute()
                score_count += 1
            except Exception:
                pass  # likely duplicate

    print(f"  Created {score_count} scores")

    print("\n========================================")
    print("   RIDGE VINEYARDS IMPORT COMPLETE")
    print("========================================")
    print(f"  Producer: Ridge Vineyards ({producer_id})")
    print(f"  Wines: {wine_count}")
    print(f"  Vintages: {vintage_count}")
    print(f"  Scores: {score_count}")
    print(f"  Grape entries: {grape_count}")


def main():
    parser = argparse.ArgumentParser(description="Scrape Ridge Vineyards catalog")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--detail-only", action="store_true")
    parser.add_argument("--insert", action="store_true")
    args = parser.parse_args()

    if args.insert:
        insert_data()
        return

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    with httpx.Client(timeout=30.0, headers=headers, follow_redirects=True) as client:
        # Phase 1: Discover URLs
        if args.detail_only and URLS_FILE.exists():
            urls = json.loads(URLS_FILE.read_text())
            print(f"Loaded {len(urls)} URLs from {URLS_FILE}")
        else:
            urls = discover_wine_urls(client)

        # Phase 2: Scrape detail pages
        progress = load_progress() if args.resume else {"lastDetailIndex": -1, "catalogDone": True}
        start_idx = progress["lastDetailIndex"] + 1

        if start_idx > 0:
            print(f"\nResuming from index {start_idx} ({len(urls) - start_idx} remaining)")

        print(f"\nPhase 2: Scraping {len(urls) - start_idx} detail pages...")
        scraped = 0
        failed = 0

        for i in range(start_idx, len(urls)):
            url = urls[i]
            slug = url.split("/wines/")[1] if "/wines/" in url else url
            print(f"  [{i + 1}/{len(urls)}] {slug}...", end="")

            page_html = fetch_page(client, url)
            if not page_html:
                print(" FAILED")
                failed += 1
                save_progress({"lastDetailIndex": i, "catalogDone": True})
                time.sleep(DELAY_S)
                continue

            data = parse_detail_page(page_html, url)
            if not data["wineName"]:
                print(" NO TITLE")
                failed += 1
            else:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
                scraped += 1
                grape_count = len(data.get("grapes", []))
                score_count = len(data.get("scores", []))
                print(f" {data['vintage']} {data['wineName']} ({grape_count} grapes, {score_count} scores)")

            save_progress({"lastDetailIndex": i, "catalogDone": True})

            if i < len(urls) - 1:
                time.sleep(DELAY_S)

    print(f"\n========================================")
    print(f"  RIDGE SCRAPE COMPLETE")
    print(f"========================================")
    print(f"  Total URLs: {len(urls)}")
    print(f"  Scraped: {scraped}")
    print(f"  Failed: {failed}")
    print(f"  Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
