#!/usr/bin/env python3
"""
Scrape Tablas Creek Vineyard (tablascreek.com) wine catalog.

Data source: individual wine vintage pages with rich structured data.

Usage:
    python -m pipeline.fetch.tablas_creek
    python -m pipeline.fetch.tablas_creek --resume
    python -m pipeline.fetch.tablas_creek --insert
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

OUTPUT_FILE = Path("tablas_creek_wines.jsonl")
URLS_FILE = Path("tablas_creek_urls.json")
PROGRESS_FILE = Path("tablas_creek_progress.json")
DELAY_S = 3.0

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


def strip_html(s: str) -> str:
    decoded = html_mod.unescape(s)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", decoded)).strip()


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"lastIndex": -1}


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
                time.sleep(5)
                continue
            return resp.text
        except Exception as err:
            print(f"  Error: {err}")
            time.sleep(5)
    return None


# -- Parse wine page --

def parse_wine_page(html_text: str, url: str) -> dict:
    data: dict = {
        "url": url, "title": None, "vintage": None, "wineName": None,
        "grapes": [], "appellation": None, "abv": None,
        "casesProduced": None, "bottlingDate": None, "blendingDate": None,
        "aging": None, "tastingNotes": None, "productionNotes": None,
        "certifications": [], "foodPairings": [], "scores": [],
    }

    # Title
    h1_match = re.search(r"<h1[^>]*>([\s\S]*?)</h1>", html_text, re.IGNORECASE)
    if h1_match:
        data["title"] = strip_html(h1_match.group(1))
        year_match = re.match(r"^(\d{4})\s+(.+)$", data["title"])
        if year_match:
            data["vintage"] = int(year_match.group(1))
            data["wineName"] = year_match.group(2).strip()
        else:
            data["wineName"] = data["title"]

    # Appellation
    app_section = re.search(r"<h4>Appellation</h4>\s*<ul>\s*([\s\S]*?)</ul>", html_text, re.IGNORECASE)
    if app_section:
        li_match = re.search(r"<li>([\s\S]*?)</li>", app_section.group(1), re.IGNORECASE)
        if li_match:
            data["appellation"] = strip_html(li_match.group(1))

    # Technical Notes
    tech_section = re.search(r"<h4>Technical Notes</h4>\s*<ul>([\s\S]*?)</ul>", html_text, re.IGNORECASE)
    if tech_section:
        for li in re.finditer(r"<li>([\s\S]*?)</li>", tech_section.group(1), re.IGNORECASE):
            text = strip_html(li.group(1))
            abv_match = re.search(r"([\d.]+)%\s*Alcohol", text, re.IGNORECASE)
            if abv_match:
                data["abv"] = float(abv_match.group(1))
            cases_match = re.search(r"([\d,]+)\s*Cases", text, re.IGNORECASE)
            if cases_match:
                data["casesProduced"] = int(cases_match.group(1).replace(",", ""))

    # Blend
    blend_section = re.search(r"<h4>Blend</h4>\s*<ul>([\s\S]*?)</ul>", html_text, re.IGNORECASE)
    if blend_section:
        for li in re.finditer(r"<li>([\s\S]*?)</li>", blend_section.group(1), re.IGNORECASE):
            text = strip_html(li.group(1))
            grape_match = re.search(r"([\d.]+)%\s+(.+)", text)
            if grape_match:
                data["grapes"].append({
                    "percentage": float(grape_match.group(1)),
                    "grape": grape_match.group(2).strip(),
                })

    # Certifications
    cert_section = re.search(r"<h4>Certifications</h4>\s*<ul>([\s\S]*?)</ul>", html_text, re.IGNORECASE)
    if cert_section:
        data["certifications"] = [
            strip_html(li.group(1))
            for li in re.finditer(r"<li>([\s\S]*?)</li>", cert_section.group(1), re.IGNORECASE)
            if strip_html(li.group(1))
        ]

    # Food Pairings
    fp_section = re.search(r"<h4>Food Pairings</h4>\s*[\s\S]*?<ul>([\s\S]*?)</ul>", html_text, re.IGNORECASE)
    if fp_section:
        data["foodPairings"] = [
            strip_html(li.group(1))
            for li in re.finditer(r"<li>([\s\S]*?)</li>", fp_section.group(1), re.IGNORECASE)
            if strip_html(li.group(1))
        ]

    # Tasting Notes
    tn_section = re.search(r"wine_page__tasting_notes[\s\S]*?<p>([\s\S]*?)</p>", html_text, re.IGNORECASE)
    if tn_section:
        data["tastingNotes"] = strip_html(tn_section.group(1))[:2000]

    # Production Notes
    pn_section = re.search(r"wine_page__production_notes[\s\S]*?<p>([\s\S]*?)</p>", html_text, re.IGNORECASE)
    if pn_section:
        pn = strip_html(pn_section.group(1))[:2000]
        data["productionNotes"] = pn

        bottling_match = re.search(r"bottl(?:ing|ed)\s+in\s+(\w+\s+\d{4})", pn, re.IGNORECASE)
        if bottling_match:
            data["bottlingDate"] = bottling_match.group(1)

        blending_match = re.search(r"blended?\s+in\s+(\w+\s+\d{4})", pn, re.IGNORECASE)
        if blending_match:
            data["blendingDate"] = blending_match.group(1)

        aging_match = re.search(r"aged?\s+in\s+([^.]+?)(?:\s+before|\s+prior|\.|$)", pn, re.IGNORECASE)
        if aging_match:
            data["aging"] = aging_match.group(1).strip()

    # Scores
    for m in re.finditer(
        r'(\d{2,3})\s*points?[;:]\s*(?:"([^"]*)"[;:]\s*)?([^(]+?)\s*\(([^)]+)\)',
        html_text, re.IGNORECASE,
    ):
        score = int(m.group(1))
        if 80 <= score <= 100:
            data["scores"].append({
                "score": score,
                "quote": m.group(2) or None,
                "publication": m.group(3).strip().rstrip(","),
                "date": m.group(4).strip(),
            })

    return data


# -- Varietal Classification --

GRAPE_ALIASES: dict[str, str] = {
    "mourvedre": "Mourvèdre", "mourvèdre": "Mourvèdre",
    "grenache": "Grenache", "grenache noir": "Grenache", "grenache blanc": "Grenache Blanc",
    "syrah": "Syrah", "counoise": "Counoise", "cinsaut": "Cinsaut",
    "roussanne": "Roussanne", "marsanne": "Marsanne", "viognier": "Viognier",
    "vermentino": "Vermentino", "picpoul blanc": "Picpoul", "picpoul": "Picpoul",
    "clairette blanche": "Clairette", "clairette": "Clairette",
    "bourboulenc": "Bourboulenc", "picardan": "Picardan",
    "tannat": "Tannat", "petit manseng": "Petit Manseng",
    "cabernet sauvignon": "Cabernet Sauvignon", "pinot noir": "Pinot Noir",
    "chardonnay": "Chardonnay", "vaccarese": "Vaccarèse", "vaccarèse": "Vaccarèse",
    "muscardin": "Muscardin", "terret noir": "Terret Noir",
}

PUB_ALIASES: dict[str, str] = {
    "wine advocate": "wine advocate", "the wine advocate": "wine advocate",
    "robert parker wine advocate": "wine advocate",
    "vinous": "vinous", "vinous media": "vinous",
    "jamessuckling.com": "james suckling", "james suckling": "james suckling",
    "wine enthusiast": "wine enthusiast", "wine spectator": "wine spectator",
    "jeb dunnuck": "jeb dunnuck", "jebdunnuck.com": "jeb dunnuck",
    "decanter": "decanter", "wine & spirits": "wine & spirits",
    "owen bargreen": "owenbargreen.com",
}


def classify_varietal(grapes: list[dict], wine_name: str) -> str:
    name = wine_name.lower()

    # Check common Tablas Creek names
    if "esprit de tablas" in name and "blanc" not in name:
        return "Rhône Blend"
    if "esprit de tablas" in name and "blanc" in name:
        return "White Blend"
    if "esprit de beaucastel" in name and "blanc" not in name:
        return "Rhône Blend"
    if "esprit de beaucastel" in name and "blanc" in name:
        return "White Blend"
    if "côtes de tablas" in name or "cotes de tablas" in name:
        return "White Blend" if "blanc" in name else "Rhône Blend"
    if "patelin" in name:
        if "blanc" in name:
            return "White Blend"
        if "rosé" in name or "rose" in name:
            return "Rosé Blend"
        return "Rhône Blend"
    if "panoplie" in name:
        return "Rhône Blend"
    if "en gobelet" in name:
        return "Rhône Blend"
    if "dianthus" in name:
        return "Rosé Blend"
    if "rosé" in name or "rose" in name:
        return "Rosé Blend"

    if grapes:
        primary = grapes[0]
        if primary["percentage"] >= 75:
            g = primary["grape"].lower()
            for kw, cat in [
                ("mourv", "Mourvèdre"), ("grenache blanc", "Grenache Blanc"),
                ("grenache", "Grenache"), ("syrah", "Syrah"),
                ("roussanne", "Roussanne"), ("viognier", "Viognier"),
                ("vermentino", "Vermentino"), ("tannat", "Tannat"),
                ("picpoul", "Picpoul"), ("marsanne", "Marsanne"),
                ("counoise", "Counoise"), ("pinot noir", "Pinot Noir"),
                ("cabernet", "Cabernet Sauvignon"), ("chardonnay", "Chardonnay"),
                ("petit manseng", "Petit Manseng"),
            ]:
                if kw in g:
                    return cat

    whites = ["roussanne", "marsanne", "viognier", "grenache blanc", "picpoul", "vermentino", "clairette", "bourboulenc"]
    if grapes and all(any(wg in g["grape"].lower() for wg in whites) for g in grapes):
        return "White Blend"
    if "blanc" in name:
        return "White Blend"
    return "Rhône Blend"


def parse_bottling_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    }
    m = re.match(r"(\w+)\s+(\d{4})", date_str)
    if m:
        mo = months.get(m.group(1).lower())
        if mo:
            return f"{m.group(2)}-{mo:02d}-01"
    return None


# -- DB Insertion --

def insert_data():
    from pipeline.lib.db import get_supabase, fetch_all

    print("\n=== TABLAS CREEK DB INSERTION ===\n")

    lines = OUTPUT_FILE.read_text().strip().split("\n")
    entries = [json.loads(l) for l in lines if l.strip()]
    entries = [e for e in entries if e.get("wineName") and e.get("vintage")]
    print(f"Loaded {len(entries)} wine entries")

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
    existing = sb.from_("producers").select("id").eq("slug", "tablas-creek-vineyard").execute().data
    if existing:
        producer_id = existing[0]["id"]
        print(f"Using existing producer: {producer_id}")
    else:
        producer_id = str(uuid.uuid4())
        sb.from_("producers").insert({
            "id": producer_id, "slug": "tablas-creek-vineyard",
            "name": "Tablas Creek Vineyard", "name_normalized": normalize("Tablas Creek Vineyard"),
            "country_id": country_id, "website_url": "https://tablascreek.com",
            "year_established": 1989,
            "metadata": {
                "partnership": "Château de Beaucastel (Perrin family) and Robert Haas",
                "appellations": ["Adelaida District", "Paso Robles"],
                "certifications": ["Regenerative Organic Certified", "CCOF Organic"],
            },
        }).execute()
        print(f"Created producer: {producer_id}")

    # Dedup
    entry_map: dict[str, dict] = {}
    for e in entries:
        key = f"{e['wineName'].lower()}|{e['vintage']}"
        if key not in entry_map:
            entry_map[key] = e
    unique_entries = list(entry_map.values())
    print(f"{len(unique_entries)} unique wine-vintage entries")

    wines_by_name: dict[str, list[dict]] = {}
    for e in unique_entries:
        key = e["wineName"].lower()
        wines_by_name.setdefault(key, []).append(e)
    print(f"{len(wines_by_name)} unique wine names")

    APP_ALIASES: dict[str, str] = {
        "adelaida district paso robles": "adelaida district",
        "adelaida district, paso robles": "adelaida district",
        "paso robles": "paso robles",
        "paso robles estrella district": "paso robles",
    }

    # Create wines
    print("\nCreating wine records...")
    wine_id_map: dict[str, str] = {}
    wine_count = 0
    for norm_name, vintages_list in wines_by_name.items():
        latest = sorted(vintages_list, key=lambda v: v.get("vintage") or 0, reverse=True)[0]
        app_name = (latest.get("appellation") or "").lower().strip()
        app_lookup = APP_ALIASES.get(app_name, app_name)
        appellation_id = appellation_map.get(app_lookup)
        varietal_name = classify_varietal(latest.get("grapes", []), latest["wineName"])
        varietal_id = varietal_map.get(varietal_name.lower()) or varietal_map.get(slugify(varietal_name))
        if not varietal_id:
            varietal_id = varietal_map.get("red-blend")

        wine_id = str(uuid.uuid4())
        try:
            sb.from_("wines").insert({
                "id": wine_id, "slug": slugify(f"tablas-creek-{latest['wineName']}"),
                "name": latest["wineName"], "name_normalized": normalize(latest["wineName"]),
                "producer_id": producer_id, "country_id": country_id,
                "region_id": region_id, "appellation_id": appellation_id,
                "varietal_category_id": varietal_id, "varietal_category_source": winery_source_id,
                "food_pairings": ", ".join(latest.get("foodPairings", [])) or None,
                "metadata": {
                    "certifications": latest.get("certifications", []),
                    "tablas_creek_url": latest.get("url"),
                },
            }).execute()
            wine_id_map[norm_name] = wine_id
            wine_count += 1
        except Exception as err:
            print(f'  Wine "{latest["wineName"]}": {err}')

    print(f"  Created {wine_count} wines")

    # Create grapes
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
                try:
                    result = sb.from_("grapes").insert({
                        "name": grape_name, "slug": slugify(grape_name),
                        "name_normalized": normalize(grape_name),
                    }).execute()
                    if result.data:
                        grape_id = result.data[0]["id"]
                        grape_map[grape_name.lower()] = grape_id
                        print(f"    Created grape: {grape_name}")
                except Exception as err:
                    print(f'    Grape "{g["grape"]}": {err}')
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

    # Create vintages
    print("\nCreating vintage records...")
    vintage_count = 0
    for e in unique_entries:
        wine_id = wine_id_map.get(e["wineName"].lower())
        if not wine_id:
            continue
        metadata: dict = {}
        if e.get("productionNotes"):
            metadata["production_notes"] = e["productionNotes"]
        if e.get("blendingDate"):
            metadata["blending_date"] = e["blendingDate"]
        if e.get("certifications"):
            metadata["certifications"] = e["certifications"]

        try:
            sb.from_("wine_vintages").insert({
                "wine_id": wine_id, "vintage_year": e["vintage"],
                "abv": e.get("abv"), "cases_produced": e.get("casesProduced"),
                "bottling_date": parse_bottling_date(e.get("bottlingDate")),
                "winemaker_notes": e.get("tastingNotes"),
                "vintage_notes": e.get("productionNotes"),
                "metadata": metadata or {},
            }).execute()
            vintage_count += 1
        except Exception as err:
            if "duplicate" not in str(err).lower():
                print(f"  {e['vintage']} {e['wineName']}: {err}")
    print(f"  Created {vintage_count} vintages")

    # Create scores
    print("\nCreating score records...")
    score_count = 0
    for e in unique_entries:
        wine_id = wine_id_map.get(e["wineName"].lower())
        if not wine_id or not e.get("scores"):
            continue
        for s in e["scores"]:
            pub_key = s["publication"].lower().strip()
            pub_id = pub_map.get(pub_key) or pub_map.get(PUB_ALIASES.get(pub_key, ""))
            if not pub_id:
                pub_slug = slugify(s["publication"])
                try:
                    result = sb.from_("publications").insert({
                        "slug": pub_slug, "name": s["publication"], "type": "critic_publication",
                    }).execute()
                    if result.data:
                        pub_id = result.data[0]["id"]
                        pub_map[pub_key] = pub_id
                except Exception:
                    existing_pub = sb.from_("publications").select("id").eq("slug", pub_slug).execute().data
                    if existing_pub:
                        pub_id = existing_pub[0]["id"]
                        pub_map[pub_key] = pub_id
            if not pub_id:
                continue
            try:
                sb.from_("wine_vintage_scores").insert({
                    "wine_id": wine_id, "vintage_year": e["vintage"],
                    "score": s["score"], "score_scale": "100",
                    "publication_id": pub_id, "source_id": winery_source_id,
                    "url": e.get("url"),
                    "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }).execute()
                score_count += 1
            except Exception:
                pass
    print(f"  Created {score_count} scores")

    print("\n========================================")
    print("   TABLAS CREEK IMPORT COMPLETE")
    print("========================================")
    print(f"  Producer: Tablas Creek Vineyard ({producer_id})")
    print(f"  Wines: {wine_count}")
    print(f"  Vintages: {vintage_count}")
    print(f"  Scores: {score_count}")
    print(f"  Grape entries: {grape_count}")


# -- Scrape --

def scrape(client: httpx.Client, resume: bool):
    if not URLS_FILE.exists():
        print("Run URL discovery first: create tablas_creek_urls.json")
        sys.exit(1)

    urls = json.loads(URLS_FILE.read_text())
    print(f"Loaded {len(urls)} URLs from {URLS_FILE}")

    # Filter duplicates/boxes
    urls = [u for u in urls if not any(
        u.split("/wines/")[1].endswith(suffix) if "/wines/" in u else False
        for suffix in ("_", "_2", "_1")
    ) and "_box" not in u and "en_primeur" not in u]
    print(f"Filtered to {len(urls)} URLs")

    progress = load_progress() if resume else {"lastIndex": -1}
    start_idx = progress["lastIndex"] + 1
    print(f"\nScraping {len(urls) - start_idx} pages ({DELAY_S}s delay)...")

    scraped = 0
    failed = 0

    for i in range(start_idx, len(urls)):
        slug = urls[i].split("/wines/")[1] if "/wines/" in urls[i] else urls[i]
        print(f"  [{i + 1}/{len(urls)}] {slug}...", end="")

        page_html = fetch_page(client, urls[i])
        if not page_html:
            print(" FAILED")
            failed += 1
            save_progress({"lastIndex": i})
            time.sleep(DELAY_S)
            continue

        data = parse_wine_page(page_html, urls[i])
        if not data["wineName"]:
            print(" NO TITLE")
            failed += 1
        else:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
            scraped += 1
            cases = f"{data['casesProduced']}cs" if data.get("casesProduced") else "-"
            bottled = data.get("bottlingDate") or "-"
            print(f" {data['vintage']} {data['wineName']} ({len(data['grapes'])}gr, ABV:{data.get('abv') or '-'}, {cases}, btl:{bottled}, {len(data['scores'])}sc)")

        save_progress({"lastIndex": i})
        if i < len(urls) - 1:
            time.sleep(DELAY_S)

    print("\n========================================")
    print("  TABLAS CREEK SCRAPE COMPLETE")
    print("========================================")
    print(f"  Scraped: {scraped}")
    print(f"  Failed: {failed}")
    print(f"  Output: {OUTPUT_FILE}")


def main():
    parser = argparse.ArgumentParser(description="Scrape Tablas Creek Vineyard catalog")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--insert", action="store_true")
    args = parser.parse_args()

    if args.insert:
        insert_data()
        return

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    with httpx.Client(timeout=30.0, headers=headers, follow_redirects=True) as client:
        scrape(client, args.resume)


if __name__ == "__main__":
    main()
