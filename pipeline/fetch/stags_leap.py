#!/usr/bin/env python3
"""
Scrape Stag's Leap Wine Cellars (stagsleapwinecellars.com) wine catalog.

Three data sources:
  1. Product pages (current wines) -- technical data, tasting notes
  2. Past-vintages pages (historical) -- blend, ABV, pH, TA, aging, tasting notes
  3. Wine-acclaim page -- critic scores across all wines/vintages

Usage:
    python -m pipeline.fetch.stags_leap
    python -m pipeline.fetch.stags_leap --resume
    python -m pipeline.fetch.stags_leap --insert
"""

import argparse
import html as html_mod
import json
import re
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BASE_URL = "https://www.stagsleapwinecellars.com"
SITEMAP_URL = f"{BASE_URL}/product-sitemap.xml"
ACCLAIM_URL = f"{BASE_URL}/wine-acclaim/"
PAST_VINTAGE_SLUGS = [
    "cask-23-cabernet-sauvignon",
    "s-l-v-cabernet-sauvignon",
    "fay-cabernet-sauvignon",
    "artemis-cabernet-sauvignon",
]
OUTPUT_FILE = Path("stags_leap_wines.jsonl")
SCORES_FILE = Path("stags_leap_scores.jsonl")
URLS_FILE = Path("stags_leap_urls.json")
PROGRESS_FILE = Path("stags_leap_progress.json")
DELAY_S = 10.0  # robots.txt crawl-delay

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)


def normalize(s: str) -> str:
    import unicodedata
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ",
        unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower())).strip()


def slugify(s: str) -> str:
    import unicodedata
    return re.sub(r"^-|-$", "", re.sub(r"[^a-z0-9]+", "-",
        unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower()))


def decode_entities(s: str) -> str:
    return html_mod.unescape(s)


def strip_html(s: str) -> str:
    return decode_entities(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s)).strip())


def normalize_wine_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip().lower()


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"lastProductIndex": -1, "productsDone": False, "pastVintagesDone": False, "acclaimDone": False}


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


def parse_grape_composition(text: str) -> list[dict]:
    grapes = []
    for m in re.finditer(r"([\d.]+)%\s+([^,]+)", text):
        grapes.append({"percentage": float(m.group(1)), "grape": m.group(2).strip()})
    return grapes


# -- Phase 1: Discover product URLs --
def discover_product_urls(client: httpx.Client) -> list[str]:
    print("Phase 1: Fetching product sitemap...")
    xml = fetch_page(client, SITEMAP_URL)
    if not xml:
        print("Failed to fetch sitemap")
        sys.exit(1)

    all_urls = [m.group(1) for m in re.finditer(r"<loc>([^<]+)</loc>", xml)]
    print(f"  Found {len(all_urls)} total sitemap URLs")

    skip_patterns = [
        r"1-5l", r"magnum", r"12pk", r"boa-", r"gift", r"tasting",
        r"experience", r"event", r"membership", r"ice-pack", r"club",
        r"holiday", r"shipping", r"estate-visit", r"virtual",
        r"set$", r"insert$", r"pack$",
    ]

    wine_urls = []
    for url in all_urls:
        slug_match = url.split("/product/")
        slug = slug_match[1].rstrip("/") if len(slug_match) > 1 else ""
        if not slug or re.match(r"^\d+$", slug):
            continue
        if any(re.search(p, slug, re.IGNORECASE) for p in skip_patterns):
            continue
        wine_urls.append(url)

    print(f"  Filtered to {len(wine_urls)} wine product URLs")
    URLS_FILE.write_text(json.dumps(wine_urls, indent=2))
    return wine_urls


# -- Phase 2: Parse product detail pages --
def parse_product_page(html_text: str, url: str) -> dict:
    data: dict = {
        "url": url, "source": "product", "title": None, "vintage": None,
        "wineName": None, "grapes": [], "vineyard": None, "appellation": None,
        "abv": None, "ph": None, "ta": None, "aging": None,
        "tastingNotes": None, "vintageNotes": None, "winemakingNotes": None, "aboutNotes": None,
    }

    title_match = re.search(r"<h1[^>]*>([\s\S]*?)</h1>", html_text, re.IGNORECASE)
    if title_match:
        data["title"] = strip_html(title_match.group(1))
        year_match = re.match(r"^(\d{4})\s+(.+)$", data["title"])
        if year_match:
            data["vintage"] = int(year_match.group(1))
            data["wineName"] = year_match.group(2).strip()
        else:
            data["wineName"] = data["title"]

    # Analysis section
    analysis_match = (
        re.search(r'<div class="c7-product__analysis">([\s\S]*?)</div>\s*<!--analysis-->', html_text, re.IGNORECASE)
        or re.search(r'<div class="c7-product__analysis">([\s\S]*?)</div>\s*<!--info-->', html_text, re.IGNORECASE)
    )
    if analysis_match:
        analysis_html = analysis_match.group(1)
        for m in re.finditer(r"<div>\s*<div>([^<]+)</div>\s*<div>([^<]+)</div>\s*</div>", analysis_html, re.IGNORECASE):
            label, value = m.group(1).strip(), strip_html(m.group(2))
            if label == "Blend":
                data["grapes"] = parse_grape_composition(value)
            elif label == "Aging":
                data["aging"] = value
            elif label == "Alcohol":
                try:
                    data["abv"] = float(value.replace("%", ""))
                except ValueError:
                    pass
            elif label == "TA":
                v = re.search(r"([\d.]+)", value)
                if v:
                    data["ta"] = float(v.group(1))
            elif label == "pH":
                v = re.search(r"([\d.]+)", value)
                if v:
                    data["ph"] = float(v.group(1))
            elif label == "Appellation":
                data["appellation"] = value
            elif label in ("Vineyard", "Vineyards"):
                data["vineyard"] = value

    # Content sections
    section_regex = re.compile(
        r"<h2[^>]*>\s*(?:<strong>)?\s*(ABOUT[^<]*|VINEYARDS?\s*(?:&amp;|&)\s*WINEMAKING|VINTAGE|TASTING\s+NOTES?)\s*(?:</strong>)?\s*</h2>\s*([\s\S]*?)(?=<h2[^>]*>|<div class=\"c7-|<footer|<section class=\"(?!product))",
        re.IGNORECASE,
    )
    for m in section_regex.finditer(html_text):
        section = strip_html(m.group(1)).upper()
        content = strip_html(m.group(2))[:2000]
        if len(content) < 20:
            continue
        if section.startswith("ABOUT"):
            data["aboutNotes"] = content
        elif "WINEMAKING" in section:
            data["winemakingNotes"] = content
        elif section == "VINTAGE":
            data["vintageNotes"] = content
        elif "TASTING" in section:
            data["tastingNotes"] = content

    if not data["appellation"]:
        app_match = re.search(r"(?:Stags? Leap District|Napa Valley|Oak Knoll District|Atlas Peak|Coombsville)", html_text, re.IGNORECASE)
        if app_match:
            data["appellation"] = app_match.group(0)

    return data


# -- Phase 3: Past-vintages pages --
def parse_past_vintages_page(html_text: str, wine_name: str) -> list[dict]:
    vintages: list[dict] = []
    seen: set[int] = set()

    sections = re.split(r"<h2[^>]*>\s*(\d{4})\s*</h2>", html_text, flags=re.IGNORECASE)

    for i in range(1, len(sections), 2):
        vintage = int(sections[i])
        content = sections[i + 1] if i + 1 < len(sections) else ""
        if vintage in seen:
            continue
        seen.add(vintage)

        data: dict = {
            "source": "past_vintages", "vintage": vintage, "wineName": wine_name,
            "grapes": [], "abv": None, "ph": None, "ta": None,
            "aging": None, "tastingNotes": None, "appellation": None,
        }

        analysis_match = re.search(r'<div class="vintage-analysis">([\s\S]*?)</div>\s*<!--analysis-->', content, re.IGNORECASE)
        if analysis_match:
            for m in re.finditer(r"<div>\s*<div>([^<]+)</div>\s*<div>([^<]+)</div>\s*</div>", analysis_match.group(1), re.IGNORECASE):
                label, value = m.group(1).strip(), strip_html(m.group(2))
                if label == "Blend":
                    data["grapes"] = parse_grape_composition(value)
                elif label == "Aging":
                    data["aging"] = value
                elif label == "Alcohol":
                    try:
                        data["abv"] = float(value.replace("%", ""))
                    except ValueError:
                        pass
                elif label == "TA":
                    v = re.search(r"([\d.]+)", value)
                    if v:
                        data["ta"] = float(v.group(1))
                elif label == "pH":
                    v = re.search(r"([\d.]+)", value)
                    if v:
                        data["ph"] = float(v.group(1))

        app_match = re.search(r"<h[23][^>]*>\s*(?:<strong>)?\s*(Napa Valley|Stags? Leap District)\s*(?:</strong>)?\s*</h[23]>", content, re.IGNORECASE)
        if app_match:
            data["appellation"] = strip_html(app_match.group(1))

        for p in re.finditer(r"<p[^>]*>([\s\S]*?)</p>", content, re.IGNORECASE):
            text = strip_html(p.group(1))
            if len(text) < 40:
                continue
            if re.match(r"^(?:Not for Purchase|View Tech Sheet|The story of)", text, re.IGNORECASE):
                continue
            data["tastingNotes"] = text[:2000]
            break

        if data["grapes"] or data["tastingNotes"] or data["abv"]:
            vintages.append(data)

    return vintages


# -- Phase 4: Wine-acclaim scores --
def parse_acclaim_page(html_text: str) -> list[dict]:
    scores: list[dict] = []
    full_text = strip_html(html_text)

    wine_names = [
        "CASK 23", "S.L.V.", "SLV", "FAY", "ARTEMIS", "AVETA", "KARIA", "ARCADIA",
        "ARMILLARY", "Heart of FAY", "DANIKA", "CELLARIUS", "BATTUELLO", "SODA CANYON", "CHASE CREEK",
    ]
    publications = [
        "Wine Enthusiast", "Wine Spectator", "Wine Advocate", "The Wine Advocate",
        "JamesSuckling.com", "James Suckling", "Jeb Dunnuck", "JebDunnuck.com",
        "Vinous", "Vinous Media", "Wine & Spirits", "Decanter", "Robert Parker",
    ]

    for m in re.finditer(r"(\d{2,3})\s*(?:Points?|pts?|/100)", html_text, re.IGNORECASE):
        score = int(m.group(1))
        if score < 80 or score > 100:
            continue

        start = max(0, m.start() - 500)
        end = min(len(html_text), m.end() + 500)
        context = strip_html(html_text[start:end])

        wine = None
        for wn in wine_names:
            if wn in context:
                wine = wn
                break

        vintage_match = re.search(r"\b(19[7-9]\d|20[0-2]\d)\b", context)
        vintage = int(vintage_match.group(1)) if vintage_match else None

        pub = None
        for p in publications:
            if p.lower() in context.lower():
                pub = p
                break

        if wine and vintage and pub:
            is_dup = any(
                s["wine"] == wine and s["vintage"] == vintage and s["score"] == score and s["publication"] == pub
                for s in scores
            )
            if not is_dup:
                scores.append({"wine": wine, "vintage": vintage, "score": score, "publication": pub})

    return scores


# -- Varietal Classification --

GRAPE_ALIASES: dict[str, str] = {
    "cabernet sauvignon": "Cabernet Sauvignon", "cabernet franc": "Cabernet Franc",
    "petit verdot": "Petit Verdot", "merlot": "Merlot", "malbec": "Malbec",
    "chardonnay": "Chardonnay", "sauvignon blanc": "Sauvignon Blanc",
    "petite sirah": "Petite Sirah", "syrah": "Syrah",
}

PUB_ALIASES: dict[str, str] = {
    "wine enthusiast": "wine enthusiast", "wine spectator": "wine spectator",
    "the wine advocate": "wine advocate", "wine advocate": "wine advocate",
    "robert parker": "wine advocate",
    "jamessuckling.com": "james suckling", "james suckling": "james suckling",
    "jeb dunnuck": "jeb dunnuck", "jebdunnuck.com": "jeb dunnuck",
    "vinous": "vinous", "vinous media": "vinous",
    "wine & spirits": "wine & spirits", "decanter": "decanter",
}


def classify_varietal(grapes: list[dict], wine_name: str) -> str:
    name = wine_name.lower()

    if grapes:
        primary = grapes[0]
        pct = primary["percentage"]
        grape = primary["grape"].lower()

        if pct >= 75:
            for kw, cat in [("cabernet sauvignon", "Cabernet Sauvignon"), ("cabernet franc", "Cabernet Franc"),
                             ("chardonnay", "Chardonnay"), ("sauvignon blanc", "Sauvignon Blanc"), ("merlot", "Merlot")]:
                if kw in grape:
                    return cat

        bordeaux = ["cabernet sauvignon", "merlot", "cabernet franc", "petit verdot", "malbec"]
        if all(any(bg in g["grape"].lower() for bg in bordeaux) for g in grapes):
            return "Bordeaux Blend"

    for kw, cat in [
        ("cabernet sauvignon", "Cabernet Sauvignon"), ("cask 23", "Cabernet Sauvignon"),
        ("s.l.v", "Cabernet Sauvignon"), ("slv", "Cabernet Sauvignon"),
        ("fay", "Cabernet Sauvignon"), ("artemis", "Cabernet Sauvignon"),
        ("armillary", "Cabernet Sauvignon"), ("cabernet franc", "Cabernet Franc"),
        ("chardonnay", "Chardonnay"), ("sauvignon blanc", "Sauvignon Blanc"),
        ("merlot", "Merlot"), ("petit verdot", "Petit Verdot"), ("red blend", "Red Blend"),
    ]:
        if kw in name:
            return cat

    return "Red Blend"


APPELLATION_ALIASES: dict[str, str] = {
    "stags leap district": "stags leap district",
    "stag's leap district": "stags leap district",
    "napa valley": "napa valley",
    "oak knoll district": "oak knoll district of napa valley",
    "oak knoll district of napa valley": "oak knoll district of napa valley",
    "atlas peak": "atlas peak",
    "coombsville": "coombsville",
}


def parse_oak_months(aging: str | None) -> int | None:
    if not aging:
        return None
    m = re.search(r"(\d+)\s*months?", aging, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_new_oak_pct(aging: str | None) -> int | None:
    if not aging:
        return None
    m = re.search(r"(\d+)%\s*new", aging, re.IGNORECASE)
    return int(m.group(1)) if m else None


# -- DB Insertion --

def insert_data():
    from pipeline.lib.db import get_supabase, fetch_all

    print("\n=== STAG'S LEAP DB INSERTION ===\n")

    if not OUTPUT_FILE.exists():
        print(f"No {OUTPUT_FILE} found. Run scraper first.")
        sys.exit(1)

    lines = OUTPUT_FILE.read_text().strip().split("\n")
    entries = [json.loads(l) for l in lines if l.strip()]
    entries = [e for e in entries if e.get("wineName") and e.get("vintage")]

    skip_names = re.compile(r"event|vertical|exploration|test product|boxed set", re.IGNORECASE)
    entries = [e for e in entries if not skip_names.search(e["wineName"])]

    for e in entries:
        e["wineName"] = re.sub(r",\s*\d+\s*Points?\s*$", "", e["wineName"], flags=re.IGNORECASE).strip()
        if e.get("ph") and e["ph"] < 2.0 and e.get("ta") and e["ta"] > 2.0:
            e["ph"], e["ta"] = e["ta"], e["ph"]
        elif e.get("ph") and e["ph"] < 2.0:
            e["ta"] = e.get("ta") or e["ph"]
            e["ph"] = None

    print(f"Loaded {len(entries)} wine entries (cleaned)")

    scores: list[dict] = []
    if SCORES_FILE.exists():
        scores = [json.loads(l) for l in SCORES_FILE.read_text().strip().split("\n") if l.strip()]
        print(f"Loaded {len(scores)} score entries")

    sb = get_supabase()

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
    existing = sb.from_("producers").select("id").eq("slug", "stags-leap-wine-cellars").execute().data
    if existing:
        producer_id = existing[0]["id"]
        print(f"  Using existing producer: {producer_id}")
    else:
        producer_id = str(uuid.uuid4())
        sb.from_("producers").insert({
            "id": producer_id, "slug": "stags-leap-wine-cellars",
            "name": "Stag's Leap Wine Cellars", "name_normalized": normalize("Stag's Leap Wine Cellars"),
            "country_id": country_id, "website_url": BASE_URL, "year_established": 1970,
            "metadata": {"famous_for": "1976 Judgment of Paris winner (S.L.V. 1973)",
                         "appellations": ["Stags Leap District", "Napa Valley"], "winemaker": "Marcus Notaro"},
        }).execute()
        print(f"  Created producer: {producer_id}")

    # Dedup entries
    entry_map: dict[str, dict] = {}
    for e in entries:
        key = f"{normalize_wine_name(e['wineName'])}|{e['vintage']}"
        existing_e = entry_map.get(key)
        if not existing_e or (e.get("source") == "product" and existing_e.get("source") != "product"):
            entry_map[key] = e
        elif not existing_e.get("abv") and e.get("abv"):
            merged = {**existing_e}
            for k, v in e.items():
                if v is not None and not merged.get(k):
                    merged[k] = v
            entry_map[key] = merged

    unique_entries = list(entry_map.values())
    print(f"\n{len(unique_entries)} unique wine-vintage entries (after dedup)")

    wines_by_name: dict[str, list[dict]] = {}
    for e in unique_entries:
        key = normalize_wine_name(e["wineName"])
        wines_by_name.setdefault(key, []).append(e)
    print(f"{len(wines_by_name)} unique wine names")

    # Create wines
    print("\nCreating wine records...")
    wine_id_map: dict[str, str] = {}
    wine_count = 0
    for norm_name, vintages_list in wines_by_name.items():
        latest = sorted(vintages_list, key=lambda v: v.get("vintage") or 0, reverse=True)[0]
        app_name = (latest.get("appellation") or "").lower().strip()
        app_lookup = APPELLATION_ALIASES.get(app_name, app_name)
        appellation_id = appellation_map.get(app_lookup)
        varietal_name = classify_varietal(latest.get("grapes", []), latest["wineName"])
        varietal_id = varietal_map.get(varietal_name.lower()) or varietal_map.get(slugify(varietal_name))
        if not varietal_id:
            varietal_id = varietal_map.get("red-blend") or varietal_map.get("red blend")

        wine_id = str(uuid.uuid4())
        try:
            sb.from_("wines").insert({
                "id": wine_id, "slug": slugify(f"stags-leap-wc-{latest['wineName']}"),
                "name": latest["wineName"], "name_normalized": normalize(latest["wineName"]),
                "producer_id": producer_id, "country_id": country_id,
                "region_id": region_id, "appellation_id": appellation_id,
                "varietal_category_id": varietal_id, "varietal_category_source": winery_source_id,
            }).execute()
            wine_id_map[norm_name] = wine_id
            wine_count += 1
        except Exception as err:
            print(f'  Wine "{latest["wineName"]}" error: {err}')

    print(f"  Created {wine_count} wines")

    # Grapes
    print("\nCreating grape compositions...")
    grape_count = 0
    for norm_name, vintages_list in wines_by_name.items():
        wine_id = wine_id_map.get(norm_name)
        if not wine_id:
            continue
        latest = sorted(vintages_list, key=lambda v: v.get("vintage") or 0, reverse=True)[0]
        for g in latest.get("grapes", []):
            grape_name = GRAPE_ALIASES.get(g["grape"].lower(), g["grape"])
            grape_id = grape_map.get(grape_name.lower())
            if not grape_id:
                continue
            try:
                sb.from_("wine_grapes").insert({
                    "wine_id": wine_id, "grape_id": grape_id,
                    "percentage": g["percentage"], "percentage_source": winery_source_id,
                }).execute()
                grape_count += 1
            except Exception:
                pass
    print(f"  Created {grape_count} grape entries")

    # Vintages
    print("\nCreating vintage records...")
    vintage_count = 0
    for e in unique_entries:
        wine_id = wine_id_map.get(normalize_wine_name(e["wineName"]))
        if not wine_id or not e.get("vintage"):
            continue
        metadata: dict = {}
        if e.get("winemakingNotes"):
            metadata["winemaking"] = e["winemakingNotes"]
        try:
            sb.from_("wine_vintages").insert({
                "wine_id": wine_id, "vintage_year": e["vintage"],
                "abv": e.get("abv"), "ph": e.get("ph"),
                "ta_g_l": e["ta"] * 10 if e.get("ta") else None,
                "duration_in_oak_months": parse_oak_months(e.get("aging")),
                "new_oak_pct": parse_new_oak_pct(e.get("aging")),
                "winemaker_notes": e.get("tastingNotes"),
                "vintage_notes": e.get("vintageNotes"),
                "metadata": metadata or {},
            }).execute()
            vintage_count += 1
        except Exception as err:
            if "duplicate" not in str(err).lower():
                print(f"  Vintage {e['wineName']} {e['vintage']}: {err}")
    print(f"  Created {vintage_count} vintages")

    # Scores
    print("\nCreating score records...")
    score_count = 0
    acclaim_name_map = {
        "CASK 23": "CASK 23 Cabernet Sauvignon", "S.L.V.": "S.L.V. Cabernet Sauvignon",
        "SLV": "S.L.V. Cabernet Sauvignon", "FAY": "FAY Cabernet Sauvignon",
        "ARTEMIS": "ARTEMIS Cabernet Sauvignon", "AVETA": "AVETA Sauvignon Blanc",
        "KARIA": "KARIA Chardonnay", "ARCADIA": "ARCADIA Chardonnay",
        "ARMILLARY": "ARMILLARY Cabernet Sauvignon", "DANIKA": "DANIKA RANCH Sauvignon Blanc",
        "CELLARIUS": "CELLARIUS Cabernet Sauvignon", "BATTUELLO": "BATTUELLO Cabernet Sauvignon",
        "SODA CANYON": "Soda Canyon Cabernet Sauvignon", "CHASE CREEK": "Chase Creek Cabernet Sauvignon",
        "Heart of FAY": "Heart of FAY Cabernet Sauvignon",
    }

    for s in scores:
        full_name = acclaim_name_map.get(s["wine"], s["wine"])
        wine_id = wine_id_map.get(normalize_wine_name(full_name))
        if not wine_id:
            for norm_name, wid in wine_id_map.items():
                if normalize_wine_name(s["wine"]) in norm_name:
                    wine_id = wid
                    break
        if not wine_id:
            continue

        pub_key = s["publication"].lower().strip()
        pub_id = pub_map.get(pub_key) or pub_map.get(PUB_ALIASES.get(pub_key, ""))
        if not pub_id:
            continue

        try:
            sb.from_("wine_vintage_scores").insert({
                "wine_id": wine_id, "vintage_year": s["vintage"],
                "score": s["score"], "score_scale": "100",
                "publication_id": pub_id, "source_id": winery_source_id,
                "url": ACCLAIM_URL,
                "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }).execute()
            score_count += 1
        except Exception:
            pass

    print(f"  Created {score_count} scores")

    print("\n========================================")
    print("   STAG'S LEAP WINE CELLARS IMPORT COMPLETE")
    print("========================================")
    print(f"  Producer: Stag's Leap Wine Cellars ({producer_id})")
    print(f"  Wines: {wine_count}")
    print(f"  Vintages: {vintage_count}")
    print(f"  Scores: {score_count}")
    print(f"  Grape entries: {grape_count}")


# -- Main scrape flow --

def scrape(client: httpx.Client, resume: bool):
    # Phase 1: Discover URLs
    if URLS_FILE.exists():
        urls = json.loads(URLS_FILE.read_text())
        print(f"Loaded {len(urls)} URLs from {URLS_FILE}")
    else:
        urls = discover_product_urls(client)

    progress = load_progress() if resume else {
        "lastProductIndex": -1, "productsDone": False, "pastVintagesDone": False, "acclaimDone": False,
    }

    # Phase 2: Product pages
    if not progress["productsDone"]:
        start_idx = progress["lastProductIndex"] + 1
        print(f"\nPhase 2: Scraping {len(urls) - start_idx} product pages (10s delay per robots.txt)...")

        for i in range(start_idx, len(urls)):
            url = urls[i]
            slug_parts = url.split("/product/")
            slug = slug_parts[1].rstrip("/") if len(slug_parts) > 1 else url
            print(f"  [{i + 1}/{len(urls)}] {slug}...", end="")

            page_html = fetch_page(client, url)
            if not page_html:
                print(" FAILED")
                save_progress({**progress, "lastProductIndex": i})
                time.sleep(DELAY_S)
                continue

            data = parse_product_page(page_html, url)
            if not data["wineName"]:
                print(" NO TITLE")
            else:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
                print(f" {data.get('vintage') or 'NV'} {data['wineName']} ({len(data['grapes'])} grapes, ABV:{data.get('abv') or '-'})")

            save_progress({**progress, "lastProductIndex": i})
            if i < len(urls) - 1:
                time.sleep(DELAY_S)

        progress["productsDone"] = True
        save_progress(progress)
        print("  Product pages done.")

    # Phase 3: Past-vintages pages
    if not progress["pastVintagesDone"]:
        print("\nPhase 3: Scraping past-vintages pages...")
        wine_name_map = {
            "cask-23-cabernet-sauvignon": "CASK 23 Cabernet Sauvignon",
            "s-l-v-cabernet-sauvignon": "S.L.V. Cabernet Sauvignon",
            "fay-cabernet-sauvignon": "FAY Cabernet Sauvignon",
            "artemis-cabernet-sauvignon": "ARTEMIS Cabernet Sauvignon",
        }

        for slug in PAST_VINTAGE_SLUGS:
            url = f"{BASE_URL}/past-vintages/{slug}/"
            wine_name = wine_name_map.get(slug, slug)
            print(f"  {wine_name}...", end="")

            page_html = fetch_page(client, url)
            if not page_html:
                print(" FAILED")
                time.sleep(DELAY_S)
                continue

            past_vintages = parse_past_vintages_page(page_html, wine_name)
            print(f" {len(past_vintages)} vintages")

            for v in past_vintages:
                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(v, ensure_ascii=False) + "\n")

            time.sleep(DELAY_S)

        progress["pastVintagesDone"] = True
        save_progress(progress)

    # Phase 4: Wine-acclaim scores
    if not progress["acclaimDone"]:
        print("\nPhase 4: Scraping wine-acclaim page for scores...")
        page_html = fetch_page(client, ACCLAIM_URL)
        if page_html:
            acclaim_scores = parse_acclaim_page(page_html)
            print(f"  Found {len(acclaim_scores)} scores")
            for s in acclaim_scores:
                with open(SCORES_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(s, ensure_ascii=False) + "\n")
        else:
            print("  FAILED to fetch acclaim page")

        progress["acclaimDone"] = True
        save_progress(progress)

    # Summary
    output_lines = len(OUTPUT_FILE.read_text().strip().split("\n")) if OUTPUT_FILE.exists() else 0
    score_lines = len(SCORES_FILE.read_text().strip().split("\n")) if SCORES_FILE.exists() else 0

    print("\n========================================")
    print("  STAG'S LEAP SCRAPE COMPLETE")
    print("========================================")
    print(f"  Wine entries: {output_lines}")
    print(f"  Score entries: {score_lines}")
    print(f"  Output: {OUTPUT_FILE}, {SCORES_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Scrape Stag's Leap Wine Cellars catalog")
    parser.add_argument("--resume", action="store_true")
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
        scrape(client, args.resume)


if __name__ == "__main__":
    main()
