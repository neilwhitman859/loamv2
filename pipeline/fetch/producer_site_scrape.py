#!/usr/bin/env python3
"""
Scrape top producer websites for structured wine and producer data.

Generic scraper: uses Haiku to extract structured data from any winery website.
Fetches homepage + wines page + individual wine detail pages.
Falls back to Playwright for JS-heavy sites that return empty via requests.
Uses Haiku fuzzy matching to avoid creating duplicate wines.
Writes to canonical tables: producers, wines, wine_vintages, wine_grapes, winemakers.

Resume-safe: tracks completed producers in data/stats/producer_scrape_progress.json.

Usage:
    python -m pipeline.fetch.producer_site_scrape                      # dry-run
    python -m pipeline.fetch.producer_site_scrape --execute             # full run
    python -m pipeline.fetch.producer_site_scrape --execute --limit 5   # pilot
    python -m pipeline.fetch.producer_site_scrape --execute --budget 25
    python -m pipeline.fetch.producer_site_scrape --execute --skip-to "Opus One"
    python -m pipeline.fetch.producer_site_scrape --execute --no-resume  # fresh run
    python -m pipeline.fetch.producer_site_scrape --execute --retry-failed  # retry previously failed
"""

import argparse
import json
import re
import sys
import time
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL
from pipeline.lib.normalize import normalize, slugify, normalize_producer
from pipeline.lib.resolve import ReferenceResolver

# Suppress noisy warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── Constants ─────────────────────────────────────────────────────────────────
MAX_TOKENS = 8192
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00
HTTP_TIMEOUT = 20
FETCH_DELAY = 1.0  # seconds between requests to same host
MAX_WINE_PAGES = 50  # max wine detail pages per producer
MAX_PAGE_CHARS = 25000

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

PROGRESS_FILE = Path(__file__).resolve().parents[2] / "data" / "stats" / "producer_scrape_progress.json"
JOURNAL_FILE = Path(__file__).resolve().parents[2] / "data" / "stats" / "producer_scrape_journal.md"


# ── Producer Manifest (100 producers: ~50 US, ~50 international) ──────────────
PRODUCERS = [
    # === US — Napa Valley / Santa Cruz Mountains ===
    {"name": "Ridge Vineyards", "url": "https://www.ridgewine.com", "country": "US"},
    {"name": "Opus One", "url": "https://www.opusonewinery.com", "country": "US"},
    {"name": "Caymus Vineyards", "url": "https://www.caymus.com", "country": "US"},
    {"name": "Silver Oak Cellars", "url": "https://www.silveroak.com", "country": "US"},
    {"name": "Stag's Leap Wine Cellars", "url": "https://www.cask23.com", "country": "US"},
    {"name": "Robert Mondavi Winery", "url": "https://www.robertmondaviwinery.com", "country": "US"},
    {"name": "Joseph Phelps Vineyards", "url": "https://www.josephphelps.com", "country": "US"},
    {"name": "Shafer Vineyards", "url": "https://www.shafervineyards.com", "country": "US"},
    {"name": "Duckhorn Vineyards", "url": "https://www.duckhorn.com", "country": "US"},
    {"name": "Far Niente Winery", "url": "https://www.farniente.com", "country": "US"},
    {"name": "Inglenook", "url": "https://www.inglenook.com", "country": "US"},
    {"name": "Cakebread Cellars", "url": "https://www.cakebread.com", "country": "US"},
    {"name": "Spottswoode Estate", "url": "https://www.spottswoode.com", "country": "US"},
    {"name": "Heitz Cellar", "url": "https://www.heitzcellar.com", "country": "US"},
    {"name": "Chateau Montelena", "url": "https://www.montelena.com", "country": "US"},
    {"name": "Dominus Estate", "url": "https://www.dominusestate.com", "country": "US"},
    {"name": "Harlan Estate", "url": "https://www.harlanestate.com", "country": "US"},
    {"name": "Colgin Cellars", "url": "https://www.colgincellars.com", "country": "US"},
    {"name": "Mayacamas Vineyards", "url": "https://www.mayacamas.com", "country": "US"},
    {"name": "Dunn Vineyards", "url": "https://www.dunnvineyards.com", "country": "US"},
    # === US — Sonoma ===
    {"name": "Williams Selyem", "url": "https://www.williamsselyem.com", "country": "US"},
    {"name": "Kistler Vineyards", "url": "https://www.kistlervineyards.com", "country": "US"},
    {"name": "Peter Michael Winery", "url": "https://www.petermichaelwinery.com", "country": "US"},
    {"name": "Littorai", "url": "https://www.littorai.com", "country": "US"},
    {"name": "Flowers Vineyards & Winery", "url": "https://www.flowerswinery.com", "country": "US"},
    {"name": "Hirsch Vineyards", "url": "https://www.hirschvineyards.com", "country": "US"},
    {"name": "Bedrock Wine Co", "url": "https://www.bedrockwineco.com", "country": "US"},
    {"name": "Jordan Vineyard & Winery", "url": "https://www.jordanwinery.com", "country": "US"},
    # === US — Central Coast / Santa Barbara ===
    {"name": "Tablas Creek Vineyard", "url": "https://www.tablascreek.com", "country": "US"},
    {"name": "Au Bon Climat", "url": "https://www.aubonclimat.com", "country": "US"},
    {"name": "DAOU Family Estates", "url": "https://www.daouvineyards.com", "country": "US"},
    {"name": "Turley Wine Cellars", "url": "https://www.turleywinecellars.com", "country": "US"},
    {"name": "Calera Wine Company", "url": "https://www.calerawine.com", "country": "US"},
    # === US — Oregon ===
    {"name": "Domaine Drouhin Oregon", "url": "https://www.domainedrouhin.com", "country": "US"},
    {"name": "The Eyrie Vineyards", "url": "https://www.eyrievineyards.com", "country": "US"},
    {"name": "Domaine Serene", "url": "https://www.domaineserene.com", "country": "US"},
    {"name": "Beaux Frères", "url": "https://www.beauxfreres.com", "country": "US"},
    {"name": "Bergström Wines", "url": "https://www.bergstromwines.com", "country": "US"},
    # === US — Washington ===
    {"name": "Quilceda Creek", "url": "https://www.quilcedacreek.com", "country": "US"},
    {"name": "Leonetti Cellar", "url": "https://www.leonetticellar.com", "country": "US"},
    {"name": "Cayuse Vineyards", "url": "https://www.cayusevineyards.com", "country": "US"},
    {"name": "Charles Smith Wines", "url": "https://www.charlessmithwines.com", "country": "US"},
    # === US — Other Notable ===
    {"name": "Paul Hobbs Winery", "url": "https://www.paulhobbswinery.com", "country": "US"},
    {"name": "Ramey Wine Cellars", "url": "https://www.rameywine.com", "country": "US"},
    {"name": "Realm Cellars", "url": "https://www.realmcellars.com", "country": "US"},
    {"name": "Continuum Estate", "url": "https://www.continuumestate.com", "country": "US"},
    {"name": "Pine Ridge Vineyards", "url": "https://www.pineridgevineyards.com", "country": "US"},
    {"name": "Staglin Family Vineyard", "url": "https://www.staglinvineyards.com", "country": "US"},
    {"name": "BOND Estates", "url": "https://www.bondestates.com", "country": "US"},
    {"name": "Screaming Eagle", "url": "https://www.screamingeagle.com", "country": "US"},
    # === France — Bordeaux ===
    {"name": "Château Lafite Rothschild", "url": "https://www.lafite.com", "country": "FR"},
    {"name": "Château Latour", "url": "https://www.chateau-latour.com", "country": "FR"},
    {"name": "Château Margaux", "url": "https://www.chateau-margaux.com", "country": "FR"},
    {"name": "Château Mouton Rothschild", "url": "https://www.chateau-mouton-rothschild.com", "country": "FR"},
    {"name": "Château Haut-Brion", "url": "https://www.haut-brion.com", "country": "FR"},
    {"name": "Château Cheval Blanc", "url": "https://www.chateau-cheval-blanc.com", "country": "FR"},
    {"name": "Château d'Yquem", "url": "https://www.chateau-yquem.fr", "country": "FR"},
    {"name": "Château Palmer", "url": "https://www.chateau-palmer.com", "country": "FR"},
    {"name": "Château Lynch-Bages", "url": "https://www.lynchbages.com", "country": "FR"},
    {"name": "Château Ducru-Beaucaillou", "url": "https://www.chateau-ducru-beaucaillou.com", "country": "FR"},
    # === France — Burgundy ===
    {"name": "Maison Louis Jadot", "url": "https://www.louisjadot.com", "country": "FR"},
    {"name": "Maison Joseph Drouhin", "url": "https://www.drouhin.com", "country": "FR"},
    {"name": "Bouchard Père & Fils", "url": "https://www.bouchard-pereetfils.com", "country": "FR"},
    {"name": "Domaine Leflaive", "url": "https://www.leflaive.fr", "country": "FR"},
    {"name": "Maison Louis Latour", "url": "https://www.louislatour.com", "country": "FR"},
    {"name": "Domaine Faiveley", "url": "https://www.domaine-faiveley.com", "country": "FR"},
    {"name": "Albert Bichot", "url": "https://www.albertbichot.com", "country": "FR"},
    {"name": "Domaine William Fèvre", "url": "https://www.williamfevre.fr", "country": "FR"},
    # === France — Champagne ===
    {"name": "Krug", "url": "https://www.krug.com", "country": "FR"},
    {"name": "Louis Roederer", "url": "https://www.louis-roederer.com", "country": "FR"},
    {"name": "Bollinger", "url": "https://www.champagne-bollinger.com", "country": "FR"},
    {"name": "Pol Roger", "url": "https://www.polroger.com", "country": "FR"},
    {"name": "Dom Pérignon", "url": "https://www.domperignon.com", "country": "FR"},
    # === France — Rhône ===
    {"name": "M. Chapoutier", "url": "https://www.chapoutier.com", "country": "FR"},
    {"name": "E. Guigal", "url": "https://www.guigal.com", "country": "FR"},
    {"name": "Château de Beaucastel", "url": "https://www.beaucastel.com", "country": "FR"},
    # === Italy ===
    {"name": "Gaja", "url": "https://www.gaja.com", "country": "IT"},
    {"name": "Antinori", "url": "https://www.antinori.it", "country": "IT"},
    {"name": "Tenuta San Guido", "url": "https://www.tenutasanguido.com", "country": "IT"},
    {"name": "Ornellaia", "url": "https://www.ornellaia.com", "country": "IT"},
    {"name": "Bruno Giacosa", "url": "https://www.brunogiacosa.it", "country": "IT"},
    {"name": "Vietti", "url": "https://www.vietti.com", "country": "IT"},
    {"name": "Fontodi", "url": "https://www.fontodi.com", "country": "IT"},
    {"name": "Masseto", "url": "https://www.masseto.com", "country": "IT"},
    {"name": "Allegrini", "url": "https://www.allegrini.it", "country": "IT"},
    {"name": "Masi", "url": "https://www.masi.it", "country": "IT"},
    # === Spain ===
    {"name": "Vega Sicilia", "url": "https://www.vega-sicilia.com", "country": "ES"},
    {"name": "López de Heredia", "url": "https://www.lopezdeheredia.com", "country": "ES"},
    {"name": "Marqués de Riscal", "url": "https://www.marquesderiscal.com", "country": "ES"},
    {"name": "Álvaro Palacios", "url": "https://www.alvaropalacios.com", "country": "ES"},
    {"name": "CVNE", "url": "https://www.cvne.com", "country": "ES"},
    # === Germany ===
    {"name": "Dr. Loosen", "url": "https://www.drloosen.com", "country": "DE"},
    {"name": "Weingut Robert Weil", "url": "https://www.weingut-robert-weil.com", "country": "DE"},
    {"name": "Joh. Jos. Prüm", "url": "https://www.jjpruem.com", "country": "DE"},
    # === Australia / New Zealand ===
    {"name": "Penfolds", "url": "https://www.penfolds.com", "country": "AU"},
    {"name": "Henschke", "url": "https://www.henschke.com.au", "country": "AU"},
    {"name": "Torbreck", "url": "https://www.torbreck.com", "country": "AU"},
    {"name": "Cloudy Bay", "url": "https://www.cloudybay.com", "country": "NZ"},
    # === South America / Africa ===
    {"name": "Catena Zapata", "url": "https://www.catenawines.com", "country": "AR"},
    {"name": "Kanonkop", "url": "https://www.kanonkop.co.za", "country": "ZA"},
]


# ── Haiku Prompts ─────────────────────────────────────────────────────────────

PRODUCER_SYSTEM = """You extract structured data from a wine producer's website. You receive text from their homepage and wines/portfolio page.

Return ONLY valid JSON:
{
  "producer": {
    "year_established": <integer or null>,
    "description": <2-3 sentence summary of philosophy/history/style, or null>,
    "winemaker": <head winemaker full name, or null>,
    "farming": ["organic", "biodynamic", "sustainable"],
    "vineyard_acres": <number or null>,
    "production": <string like "60000 cases" or null>
  },
  "wines": [
    {
      "name": <wine name WITHOUT producer prefix>,
      "color": <"red"|"white"|"rose" or null>,
      "wine_type": <"table"|"sparkling"|"fortified">,
      "appellation": <specific AVA/AOC/DOC or null>,
      "grapes": [{"name": "Cabernet Sauvignon", "percentage": 77}],
      "detail_url": <URL from the LINKS section, or null — NEVER fabricate>
    }
  ]
}

Rules:
- Extract ALL wines, not just featured ones
- Wine name must NOT include the producer name prefix
- Use standard grape names; percentage only if stated
- detail_url MUST come from provided LINKS — never invent URLs
- null for anything not explicitly on the page"""

WINE_DETAIL_SYSTEM = """You extract structured wine data from wine detail/tech sheet pages. Pages are separated by === markers.

Return ONLY a JSON array:
[
  {
    "name": <wine name WITHOUT producer prefix>,
    "vintage_year": <integer or 0 for NV>,
    "color": <"red"|"white"|"rose">,
    "wine_type": <"table"|"sparkling"|"fortified">,
    "grapes": [{"name": "Cabernet Sauvignon", "percentage": 77}],
    "appellation": <AVA/AOC/DOC>,
    "abv": <decimal like 13.5 or null>,
    "cases_produced": <integer number of cases — if given in bottles divide by 12 — or null>,
    "winemaker_notes": <winemaker tasting/description text, max 500 chars, or null>,
    "vineyard": <specific vineyard name or null>,
    "duration_in_oak_months": <integer or null>,
    "oak_origin": <"French"|"American"|"Hungarian" or null>,
    "ph": <decimal or null>,
    "ta_g_l": <total acidity in g/L or null>,
    "rs_g_l": <residual sugar in g/L or null>,
    "vintage_notes": <brief vintage/wine description, or null>
  }
]

Rules:
- One entry per vintage shown; if multiple vintages on one page, create multiple entries
- For production: give cases (if given in bottles, divide by 12)
- Use standard grape names; percentage only if stated
- null for anything not on the page — never guess
- If page has no wine data, return []"""


# ── Playwright (lazy-init) ───────────────────────────────────────────────────

_playwright = None
_browser = None

def _get_browser():
    """Lazy-init Playwright Chromium browser. Returns browser instance."""
    global _playwright, _browser
    if _browser is None:
        from playwright.sync_api import sync_playwright
        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(headless=True)
        print("  [Playwright] Browser launched")
    return _browser

def _shutdown_playwright():
    """Clean shutdown of Playwright."""
    global _playwright, _browser
    if _browser:
        _browser.close()
        _browser = None
    if _playwright:
        _playwright.stop()
        _playwright = None


def playwright_fetch(url, wait_ms=3000, click_age_gate=True):
    """Fetch a URL using Playwright (renders JS). Returns HTML or None."""
    try:
        browser = _get_browser()
        page = browser.new_page(
            user_agent=UA,
            viewport={"width": 1280, "height": 900}
        )
        page.goto(url, timeout=20000, wait_until="domcontentloaded")
        page.wait_for_timeout(wait_ms)

        # Handle common age gates (try multiple patterns)
        if click_age_gate:
            for selector in [
                "button:has-text('Yes')",
                "button:has-text('Enter')",
                "button:has-text('I am')",
                "button:has-text('Confirm')",
                "button:has-text('agree')",
                "button:has-text('legal')",
                "a:has-text('Yes')",
                "a:has-text('Enter')",
                "text=Enter",  # bare text link
                "[class*='age'] button",
                "[class*='verify'] button",
                "[id*='age'] button",
                "[class*='gate'] button",
            ]:
                try:
                    el = page.locator(selector).last  # .last catches buttons at end
                    if el.is_visible(timeout=500):
                        el.click()
                        page.wait_for_timeout(2000)
                        break
                except Exception:
                    continue
            # Wait for any post-gate content to load
            page.wait_for_timeout(wait_ms)

        html = page.content()
        page.close()
        if len(html) > 200:
            return html
    except Exception as e:
        print(f"    [Playwright] ERROR: {url} -> {type(e).__name__}: {e}")
    return None


# ── HTTP / HTML Utilities ─────────────────────────────────────────────────────

MIN_PAGE_CHARS = 200  # below this we consider the page JS-only / empty

def fetch_page(url, use_playwright_fallback=True):
    """Fetch URL with requests, fall back to Playwright for JS-heavy sites."""
    html = _fetch_requests(url)
    if html:
        text = page_text(html)
        if len(text) >= MIN_PAGE_CHARS:
            return html
        # Got HTML but text is too short -- likely JS-rendered
        if use_playwright_fallback:
            print(f"    [Playwright fallback] Text too short ({len(text)} chars), trying JS render...")
            pw_html = playwright_fetch(url)
            if pw_html:
                return pw_html
        return html  # return the thin HTML anyway
    # requests failed entirely -- try Playwright
    if use_playwright_fallback:
        print(f"    [Playwright fallback] requests failed, trying JS render...")
        return playwright_fetch(url)
    return None


def _fetch_requests(url):
    """Fetch URL via requests only. Returns HTML string or None."""
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT,
                         allow_redirects=True)
        if r.status_code == 200 and len(r.text) > 200:
            return r.text
        if r.status_code != 200:
            print(f"    HTTP {r.status_code}: {url}")
    except requests.exceptions.SSLError:
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT,
                             allow_redirects=True, verify=False)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
    except requests.exceptions.TooManyRedirects:
        print(f"    TOO MANY REDIRECTS: {url}")
    except requests.exceptions.Timeout:
        print(f"    TIMEOUT: {url}")
    except Exception as e:
        print(f"    FETCH ERROR: {url} -> {type(e).__name__}")
    return None


def page_text(html, max_chars=MAX_PAGE_CHARS):
    """Strip HTML to readable text."""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "path"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:max_chars]


def page_links(html, base_url):
    """Extract same-domain links as [(text, full_url), ...]."""
    soup = BeautifulSoup(html, "lxml")
    base_host = urlparse(base_url).netloc
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        full = urljoin(base_url, href)
        if urlparse(full).netloc != base_host:
            continue
        if full in seen:
            continue
        seen.add(full)
        text = a.get_text(strip=True)[:80]
        if text:
            links.append((text, full))
    return links


def find_wines_url(links, base_url):
    """Identify the wines/portfolio page from nav links."""
    nav_texts = {"wines", "our wines", "the wines", "wine portfolio", "portfolio",
                 "current releases", "all wines", "our wine", "the wine"}
    for text, url in links:
        if text.lower().strip() in nav_texts:
            return url
    for _, url in links:
        path = urlparse(url).path.lower().rstrip("/")
        if path in ("/wines", "/our-wines", "/wine", "/portfolio",
                     "/current-releases", "/collections/all"):
            return url
    return None


def guess_wines_url(base_url):
    """Try common wine page URL patterns."""
    for suffix in ["/wines", "/our-wines", "/wine", "/portfolio",
                   "/current-releases", "/collections/all"]:
        url = base_url.rstrip("/") + suffix
        html = fetch_page(url, use_playwright_fallback=True)
        if html and len(page_text(html)) > 500:
            return url, html
        time.sleep(0.5)
    return None, None


# ── Haiku API ─────────────────────────────────────────────────────────────────

def compute_cost(inp, out):
    return (inp * INPUT_COST_PER_M + out * OUTPUT_COST_PER_M) / 1_000_000


def parse_json_response(text):
    """Robustly extract JSON from Haiku output."""
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    for sc, ec in [("{", "}"), ("[", "]")]:
        start = text.find(sc)
        if start >= 0:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == sc:
                    depth += 1
                elif text[i] == ec:
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[start:i + 1])
                        except json.JSONDecodeError:
                            break
    return None


def call_haiku(client, system, user_msg):
    """Call Haiku, return (parsed_json, cost)."""
    try:
        r = client.messages.create(
            model=HAIKU_MODEL, max_tokens=MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        cost = compute_cost(r.usage.input_tokens, r.usage.output_tokens)
        parsed = parse_json_response(r.content[0].text)
        return parsed, cost
    except anthropic.RateLimitError:
        print("    Rate limited — waiting 30s")
        time.sleep(30)
        return call_haiku(client, system, user_msg)
    except Exception as e:
        print(f"    HAIKU ERROR: {e}")
        return None, 0.0


# ── DB Operations ─────────────────────────────────────────────────────────────

def match_producer(conn, name):
    """Find producer by name. Returns (id, db_name) or None."""
    norm = normalize(name)
    norm_prod = normalize_producer(name)
    with conn.cursor() as cur:
        # Exact normalized match
        cur.execute("SELECT id, name FROM producers WHERE name_normalized = %s LIMIT 1", (norm,))
        row = cur.fetchone()
        if row:
            return row
        # Stripped-suffix exact match
        if norm_prod != norm:
            cur.execute("SELECT id, name FROM producers WHERE name_normalized = %s LIMIT 1",
                        (norm_prod,))
            row = cur.fetchone()
            if row:
                return row
        # ILIKE prefix match — only use the full name to avoid false positives
        cur.execute("""SELECT id, name FROM producers
                       WHERE name ILIKE %s ORDER BY LENGTH(name) ASC LIMIT 1""",
                    (f"{name}%",))
        row = cur.fetchone()
        if row:
            return row
    return None


def create_producer(conn, name, country_id, website):
    """Create a new producer. Returns producer_id."""
    pid = str(uuid.uuid4())
    norm = normalize(name)
    slg = slugify(name)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO producers (id, name, name_normalized, slug, country_id, website_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (slug) DO UPDATE SET website_url = COALESCE(producers.website_url, EXCLUDED.website_url)
            RETURNING id
        """, (pid, name, norm, slg, country_id, website))
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else pid


def update_producer(conn, pid, updates):
    """NULL-fill producer fields."""
    sets, vals = [], []
    for col, val in updates.items():
        if val is not None:
            sets.append(f"{col} = COALESCE({col}, %s)")
            vals.append(val)
    if not sets:
        return
    vals.append(pid)
    with conn.cursor() as cur:
        cur.execute(f"UPDATE producers SET {', '.join(sets)} WHERE id = %s", vals)
        conn.commit()


def normalize_wine_name(name):
    """Aggressively normalize wine name for matching: strip vintage, format, producer echoes."""
    s = name
    # Strip leading/trailing vintage years
    s = re.sub(r"^(19|20)\d{2}\s+", "", s)
    s = re.sub(r"\s+(19|20)\d{2}$", "", s)
    # Strip bottle format suffixes
    s = re.sub(r"\s+\d+(\.\d+)?\s*(ml|ML|mL|L|l)\b.*$", "", s)
    s = re.sub(r"\s+(Magnum|Double Magnum|Jeroboam|Imperial|Methuselah|Salmanazar|Nebuchadnezzar)\b.*$", "", s, flags=re.I)
    # Strip trailing " - " descriptions
    s = re.sub(r"\s*[-]\s*\d+\s*(ml|L).*$", "", s, flags=re.I)
    return s.strip()


def match_wine(conn, producer_id, wine_name):
    """Find wine by producer + normalized name. Returns (id, name) or None."""
    norm = normalize(wine_name)
    with conn.cursor() as cur:
        cur.execute("""SELECT id, name FROM wines
                       WHERE producer_id = %s AND name_normalized = %s LIMIT 1""",
                    (producer_id, norm))
        row = cur.fetchone()
        if row:
            return row
        # Try with aggressive normalization (strip year, format)
        clean = normalize_wine_name(wine_name)
        if clean != wine_name:
            norm2 = normalize(clean)
            cur.execute("""SELECT id, name FROM wines
                           WHERE producer_id = %s AND name_normalized = %s LIMIT 1""",
                        (producer_id, norm2))
            row = cur.fetchone()
            if row:
                return row
        return None


def get_existing_wines_for_producer(conn, producer_id):
    """Get all wine names for a producer (for fuzzy matching)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM wines WHERE producer_id = %s", (producer_id,))
        return cur.fetchall()


FUZZY_WINE_SYSTEM = """You are a wine name matching expert. Given a NEW wine name from a producer's website and a list of EXISTING wine names in our database (same producer), determine if the new wine matches any existing one.

Wine names can differ in:
- Vintage years: "Opus One 2022" = "Opus One"
- Bottle formats: "Cabernet Sauvignon 1.5L" = "Cabernet Sauvignon"
- Minor wording: "Estate Cabernet" vs "Cabernet Sauvignon, Estate"
- Punctuation/accents: "Cote Rotie" = "Côte-Rôtie"

But these are DIFFERENT wines:
- Different vineyard: "Hillside Select" vs "Napa Valley"
- Different grape: "Chardonnay" vs "Pinot Noir"
- Different tier: "Grand Vin" vs "Second Vin"

Return ONLY valid JSON:
{"match_index": <0-based index into existing list, or null if no match>, "confidence": <0.0-1.0>, "reason": "<brief explanation>"}"""


def fuzzy_match_wine(client, new_name, existing_wines, stats):
    """Use Haiku to fuzzy-match a wine name against existing wines from the same producer.
    Returns (wine_id, db_name) or None. Updates stats["cost"]."""
    if not existing_wines or len(existing_wines) > 200:
        return None  # too many to send to Haiku

    existing_list = "\n".join(f"{i}. {name}" for i, (wid, name) in enumerate(existing_wines))
    user_msg = f"NEW wine: {new_name}\n\nEXISTING wines:\n{existing_list}"

    result, cost = call_haiku(client, FUZZY_WINE_SYSTEM, user_msg)
    stats["cost"] += cost

    if not result:
        return None
    idx = result.get("match_index")
    conf = result.get("confidence", 0)
    if idx is not None and conf >= 0.8 and 0 <= idx < len(existing_wines):
        wid, db_name = existing_wines[idx]
        print(f"    Fuzzy matched: '{new_name}' -> '{db_name}' ({conf:.0%})")
        return (wid, db_name)
    return None


def create_wine(conn, producer_id, producer_name, wine_data, resolver, country_code):
    """Create a wine from extracted data. Returns wine_id or None."""
    name = wine_data["name"]
    norm = normalize(name)
    slg = slugify(f"{producer_name} {name}")

    appellation_id = region_id = country_id = None
    if wine_data.get("appellation"):
        app = resolver.resolve_appellation(wine_data["appellation"])
        if app:
            appellation_id = app["id"]
            region_id = app.get("region_id")
            country_id = app.get("country_id")
    if not country_id:
        country_id = resolver.resolve_country(country_code)

    color = wine_data.get("color")
    if color not in ("red", "white", "rose"):
        color = None
    wine_type = wine_data.get("wine_type", "table")
    if wine_type not in ("table", "sparkling", "fortified"):
        wine_type = "table"

    wid = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wines (id, name, name_normalized, slug, producer_id,
                             color, wine_type, appellation_id, region_id, country_id, data_grade)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'F')
            ON CONFLICT (slug) DO NOTHING RETURNING id
        """, (wid, name, norm, slg, producer_id, color, wine_type,
              appellation_id, region_id, country_id))
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None


def ensure_vintage(conn, wine_id, year, data=None):
    """Create or update a wine_vintage. Returns vintage_id."""
    if year is None:
        year = 0
    # Map extracted field names to actual DB column names
    DB_COLS = ["abv", "cases_produced", "winemaker_notes",
               "duration_in_oak_months", "oak_origin", "ph",
               "ta_g_l", "rs_g_l", "vintage_notes"]
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM wine_vintages WHERE wine_id = %s AND vintage_year = %s",
                    (wine_id, year))
        row = cur.fetchone()
        if row:
            vid = row[0]
            if data:
                sets, vals = [], []
                for c in DB_COLS:
                    if data.get(c) is not None:
                        sets.append(f"{c} = COALESCE({c}, %s)")
                        vals.append(data[c])
                if sets:
                    vals.append(vid)
                    cur.execute(f"UPDATE wine_vintages SET {', '.join(sets)} WHERE id = %s", vals)
                    conn.commit()
            return vid
        vid = str(uuid.uuid4())
        col_names = ", ".join(["id", "wine_id", "vintage_year"] + DB_COLS)
        placeholders = ", ".join(["%s"] * (3 + len(DB_COLS)))
        vals = [vid, wine_id, year] + [data.get(c) if data else None for c in DB_COLS]
        cur.execute(f"""
            INSERT INTO wine_vintages ({col_names})
            VALUES ({placeholders})
            ON CONFLICT (wine_id, vintage_year) DO NOTHING RETURNING id
        """, vals)
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None


def link_grapes(conn, wine_id, grapes, resolver):
    """Link grapes to a wine. Returns count of new links."""
    linked = 0
    for g in (grapes or []):
        grape = resolver.resolve_grape(g.get("name", ""))
        if not grape:
            continue
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO wine_grapes (wine_id, grape_id, percentage)
                           VALUES (%s, %s, %s) ON CONFLICT (wine_id, grape_id) DO NOTHING""",
                        (wine_id, grape["id"], g.get("percentage")))
            if cur.rowcount > 0:
                linked += 1
            conn.commit()
    return linked


def ensure_winemaker(conn, name, producer_id):
    """Create winemaker if needed, link to producer. Returns winemaker_id."""
    slg = slugify(name)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM winemakers WHERE slug = %s", (slg,))
        row = cur.fetchone()
        if row:
            wm_id = row[0]
        else:
            wm_id = str(uuid.uuid4())
            cur.execute("""INSERT INTO winemakers (id, name, slug)
                           VALUES (%s, %s, %s) ON CONFLICT (slug) DO NOTHING RETURNING id""",
                        (wm_id, name, slg))
            r = cur.fetchone()
            wm_id = r[0] if r else wm_id
            conn.commit()
        cur.execute("""INSERT INTO producer_winemakers (producer_id, winemaker_id)
                       VALUES (%s, %s) ON CONFLICT DO NOTHING""",
                    (producer_id, wm_id))
        conn.commit()
        return wm_id


def parse_production(s):
    """Parse production string to integer bottles."""
    if not s:
        return None
    s = str(s).lower().strip().replace(",", "")
    m = re.match(r"(\d+)\s*cases?", s)
    if m:
        return int(m.group(1)) * 12
    m = re.match(r"(\d+)\s*bottles?", s)
    if m:
        return int(m.group(1))
    return None


# ── Progress Tracking ─────────────────────────────────────────────────────────

def load_progress():
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"completed": {}, "failed": {}}


def save_progress(progress):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def log_observation(category, producer, detail):
    """Append a data improvement observation to the journal."""
    with open(JOURNAL_FILE, "a", encoding="utf-8") as f:
        f.write(f"- **{category}**: {producer} -- {detail}\n")


# ── Main Processing ───────────────────────────────────────────────────────────

def process_producer(entry, conn, resolver, client, execute=False):
    """Scrape one producer. Returns stats dict."""
    name = entry["name"]
    base_url = entry["url"]
    country_code = entry.get("country", "US")

    stats = {"wines_found": 0, "wines_created": 0, "vintages_created": 0,
             "grapes_linked": 0, "producer_updated": False, "cost": 0.0,
             "skipped": False, "reason": None}

    print(f"\n{'=' * 65}")
    print(f"  PRODUCER: {name}")
    print(f"  URL: {base_url}")

    # ── 1. Fetch homepage (requests first, Playwright fallback) ────────
    home_html = fetch_page(base_url, use_playwright_fallback=True)
    if not home_html:
        stats["skipped"], stats["reason"] = True, "homepage fetch failed"
        print(f"  SKIP: Cannot fetch homepage (requests + Playwright both failed)")
        return stats

    home_text = page_text(home_html)
    if len(home_text) < MIN_PAGE_CHARS:
        stats["skipped"], stats["reason"] = True, "page too short even with Playwright"
        print(f"  SKIP: Page too short ({len(home_text)} chars)")
        return stats

    home_links = page_links(home_html, base_url)
    print(f"  Homepage: {len(home_text)} chars, {len(home_links)} links")

    # ── 2a. Try to fetch about/story page for winemaker info ─────────────
    about_text = ""
    about_keywords = {"about", "about us", "our story", "the story", "story",
                      "our team", "team", "history", "winemaker", "winemaking"}
    about_url = None
    for text, url in home_links:
        if text.lower().strip() in about_keywords:
            about_url = url
            break
    if not about_url:
        for _, url in home_links:
            path = urlparse(url).path.lower().rstrip("/")
            if path in ("/about", "/about-us", "/our-story", "/story", "/team", "/winemaker"):
                about_url = url
                break
    if about_url:
        about_html = fetch_page(about_url)
        if about_html:
            about_text = page_text(about_html, max_chars=8000)
            print(f"  About page: {about_url} ({len(about_text)} chars)")
        time.sleep(FETCH_DELAY)

    # ── 2b. Find and fetch wines page ─────────────────────────────────────
    wines_url = find_wines_url(home_links, base_url)
    wines_text = ""
    wines_links = []
    wines_html = None

    if wines_url:
        wines_html = fetch_page(wines_url)
        if wines_html:
            wines_text = page_text(wines_html)
            wines_links = page_links(wines_html, wines_url)
            print(f"  Wines page: {wines_url} ({len(wines_text)} chars)")
        time.sleep(FETCH_DELAY)
    else:
        wines_url, wines_html = guess_wines_url(base_url)
        if wines_html:
            wines_text = page_text(wines_html)
            wines_links = page_links(wines_html, wines_url)
            print(f"  Wines page (guessed): {wines_url} ({len(wines_text)} chars)")
        else:
            print(f"  No wines page found — using homepage only")

    # ── 3. Haiku: extract producer info + wine list ───────────────────────
    combined = f"=== HOMEPAGE ===\n{home_text[:12000]}\n\n"
    if about_text:
        combined += f"=== ABOUT PAGE ===\n{about_text[:8000]}\n\n"
    if wines_text:
        combined += f"=== WINES PAGE ===\n{wines_text[:12000]}\n\n"
    all_links = wines_links or home_links
    if all_links:
        link_block = "\n".join(f"- [{t}]({u})" for t, u in all_links[:100])
        combined += f"=== LINKS ===\n{link_block}\n"

    result, cost = call_haiku(client, PRODUCER_SYSTEM,
                              f"Producer: {name}\nCountry: {country_code}\n\n{combined}")
    stats["cost"] += cost

    if not result:
        stats["skipped"], stats["reason"] = True, "Haiku extraction failed"
        print(f"  SKIP: Haiku extraction failed")
        return stats

    producer_info = result.get("producer", {})
    wine_list = result.get("wines", [])
    stats["wines_found"] = len(wine_list)

    yr = producer_info.get("year_established")
    wm = producer_info.get("winemaker")
    print(f"  Extracted: year={yr}, winemaker={wm}, farming={producer_info.get('farming', [])}")

    # Post-process: detect vintage-as-name pattern
    # Handles both "2022" and "Opus One 2022" style names
    year_rx = re.compile(r"^(.*?)\s*((?:19|20)\d{2})\s*$")
    vintage_groups = {}  # base_name -> [(year, wine_entry), ...]
    non_vintage = []
    for w in wine_list:
        wn = str(w.get("name", ""))
        m = year_rx.match(wn)
        if m:
            base = m.group(1).strip()
            yr_val = int(m.group(2))
            if not base:
                base = re.sub(r"\s+(Winery|Vineyards|Cellars|Estate|Wines)$", "", name, flags=re.I)
            vintage_groups.setdefault(normalize(base), {"base": base, "entries": []})
            vintage_groups[normalize(base)]["entries"].append((yr_val, w))
        else:
            non_vintage.append(w)

    # Collapse groups where multiple vintages share a base name
    collapsed_any = False
    for norm_base, group in vintage_groups.items():
        entries = group["entries"]
        if len(entries) >= 2:
            # Multiple vintages of same wine — collapse
            base_wine = group["base"]
            first = entries[0][1]
            collapsed = {
                "name": base_wine,
                "color": first.get("color"),
                "wine_type": first.get("wine_type", "table"),
                "appellation": first.get("appellation"),
                "grapes": first.get("grapes", []),
                "detail_url": first.get("detail_url"),
                "_vintages": [yr for yr, _ in entries],
            }
            non_vintage.insert(0, collapsed)
            collapsed_any = True
            print(f"  Collapsed {len(entries)} vintages -> '{base_wine}'")
        else:
            # Single vintage entry — keep as-is but strip year from name
            yr_val, w = entries[0]
            w["name"] = group["base"]
            w.setdefault("_vintages", [yr_val])
            non_vintage.append(w)

    if collapsed_any or any(w.get("_vintages") for w in non_vintage):
        wine_list = non_vintage

    print(f"  Wines found: {len(wine_list)}")
    if wine_list:
        for w in wine_list[:5]:
            print(f"    - {w.get('name', '?')} ({w.get('color', '?')}) [{w.get('appellation', '?')}]")
        if len(wine_list) > 5:
            print(f"    ... and {len(wine_list) - 5} more")

    # ── 4. Fetch wine detail pages ────────────────────────────────────────
    detail_urls = {}
    for w in wine_list:
        durl = w.get("detail_url")
        if durl and durl.startswith("http"):
            detail_urls[w["name"]] = durl

    detail_pages = {}
    if detail_urls:
        # Deduplicate URLs (some sites have all wines on one page)
        unique_urls = list(dict.fromkeys(detail_urls.values()))[:MAX_WINE_PAGES]
        print(f"  Fetching {len(unique_urls)} wine detail pages...")
        # Use requests only for detail pages (Playwright can't run in threads)
        # Then fall back to sequential Playwright for failures
        pw_retry = []
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_fetch_requests, u): u for u in unique_urls}
            for f in as_completed(futures):
                url = futures[f]
                try:
                    html = f.result()
                except Exception as exc:
                    print(f"    Detail fetch error: {url} -> {type(exc).__name__}")
                    html = None
                if html:
                    detail_pages[url] = page_text(html, max_chars=15000)
                else:
                    pw_retry.append(url)
        # Sequential Playwright fallback for failed detail pages
        if pw_retry:
            print(f"  Playwright retry for {len(pw_retry)} failed detail pages...")
            for url in pw_retry[:10]:  # cap at 10 to avoid slow runs
                html = playwright_fetch(url, wait_ms=2000)
                if html:
                    detail_pages[url] = page_text(html, max_chars=15000)
                time.sleep(0.5)
        print(f"  Fetched: {len(detail_pages)}/{len(unique_urls)} pages")

    # ── 5. Haiku: extract wine details from detail pages ──────────────────
    wine_details = []
    if detail_pages:
        pages_list = list(detail_pages.items())
        batch_size = 5
        for i in range(0, len(pages_list), batch_size):
            batch = pages_list[i:i + batch_size]
            batch_text = ""
            for url, text in batch:
                batch_text += f"\n=== WINE PAGE: {url} ===\n{text}\n"
            detail_result, cost = call_haiku(client, WINE_DETAIL_SYSTEM,
                                             f"Producer: {name}\n{batch_text}")
            stats["cost"] += cost
            if detail_result and isinstance(detail_result, list):
                wine_details.extend(detail_result)
            time.sleep(FETCH_DELAY)

    if wine_details:
        print(f"  Wine details extracted: {len(wine_details)} entries")

    # ── 6. DB writes ──────────────────────────────────────────────────────
    if not execute:
        print(f"  [DRY RUN] Would write producer info + {len(wine_list)} wines")
        print(f"  Cost so far: ${stats['cost']:.4f}")
        return stats

    # Match or create producer
    match = match_producer(conn, name)
    if match:
        producer_id, db_name = match
        print(f"  Matched producer: {db_name} (id={producer_id[:8]}...)")
        if db_name != name:
            log_observation("NAMING", name, f"DB has '{db_name}', producer site uses '{name}'")
    else:
        country_id = resolver.resolve_country(country_code)
        producer_id = create_producer(conn, name, country_id, base_url)
        print(f"  Created producer: {name} (id={producer_id[:8]}...)")
        log_observation("MISSING_PRODUCER", name, f"Not in DB, created from scrape ({base_url})")

    # Update producer metadata
    updates = {"website_url": base_url}
    if yr and isinstance(yr, int) and 1600 < yr < 2030:
        updates["year_established"] = yr
    desc = producer_info.get("description")
    if desc and len(desc) > 20:
        updates["description"] = desc[:1000]
    update_producer(conn, producer_id, updates)
    stats["producer_updated"] = True

    # Create winemaker
    if wm and len(wm) > 3:
        ensure_winemaker(conn, wm, producer_id)
        print(f"  Winemaker: {wm}")

    # Build a lookup: wine name → detail data (from detail page extraction)
    detail_by_name = {}
    for d in wine_details:
        dname = d.get("name", "")
        if dname:
            detail_by_name.setdefault(normalize(dname), []).append(d)

    # Pre-fetch existing wines for this producer (for Haiku fuzzy matching)
    existing_wines = get_existing_wines_for_producer(conn, producer_id)

    # Process each wine
    for w in wine_list:
        wname = w.get("name")
        if not wname:
            continue

        # Match or create wine (3-tier: exact -> aggressive normalize -> Haiku fuzzy)
        wine_match = match_wine(conn, producer_id, wname)
        if wine_match:
            wine_id = wine_match[0]
        else:
            # Haiku fuzzy match against all existing wines for this producer
            fuzzy = fuzzy_match_wine(client, wname, existing_wines, stats)
            if fuzzy:
                wine_id = fuzzy[0]
            else:
                wine_id = create_wine(conn, producer_id, name, w, resolver, country_code)
                if wine_id:
                    stats["wines_created"] += 1
                    # Add to existing wines list for subsequent fuzzy matches
                    existing_wines.append((wine_id, wname))
                else:
                    continue  # slug conflict

        # Link grapes from listing
        grapes_to_link = w.get("grapes", [])

        # Check for detail data
        norm_wname = normalize(wname)
        details_for_wine = detail_by_name.get(norm_wname, [])

        # Create vintages from collapsed year-named entries
        if w.get("_vintages"):
            for vy in w["_vintages"]:
                vid = ensure_vintage(conn, wine_id, vy)
                if vid:
                    stats["vintages_created"] += 1

        if details_for_wine:
            for d in details_for_wine:
                vy = d.get("vintage_year")
                if vy is None:
                    continue
                vintage_data = {
                    "abv": d.get("abv"),
                    "cases_produced": d.get("cases_produced"),
                    "winemaker_notes": d.get("winemaker_notes"),
                    "duration_in_oak_months": d.get("duration_in_oak_months"),
                    "oak_origin": d.get("oak_origin"),
                    "ph": d.get("ph"),
                    "ta_g_l": d.get("ta_g_l"),
                    "rs_g_l": d.get("rs_g_l"),
                    "vintage_notes": d.get("vintage_notes"),
                }
                vid = ensure_vintage(conn, wine_id, vy, vintage_data)
                if vid:
                    stats["vintages_created"] += 1

                # Use detail grapes if available (more specific per-vintage)
                if d.get("grapes"):
                    grapes_to_link = d["grapes"]

        # Link grapes
        stats["grapes_linked"] += link_grapes(conn, wine_id, grapes_to_link, resolver)

    print(f"  DB: +{stats['wines_created']} wines, +{stats['vintages_created']} vintages, "
          f"+{stats['grapes_linked']} grapes | ${stats['cost']:.4f}")
    return stats


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape top producer websites")
    parser.add_argument("--execute", action="store_true", help="Write to DB (default: dry-run)")
    parser.add_argument("--limit", type=int, help="Max producers to process")
    parser.add_argument("--budget", type=float, default=25.0, help="Max Haiku spend in USD")
    parser.add_argument("--skip-to", type=str, help="Skip to this producer name")
    parser.add_argument("--no-resume", action="store_true", help="Ignore progress file")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Re-attempt previously failed producers (clears failed list)")
    args = parser.parse_args()

    print(f"Producer Site Scraper — {len(PRODUCERS)} producers in manifest")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print(f"Budget: ${args.budget:.2f}")
    if args.limit:
        print(f"Limit: {args.limit} producers")
    print()

    # Initialize
    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)

    conn = None
    resolver = None
    if args.execute:
        conn = get_conn()
        resolver = ReferenceResolver()
        resolver.init_sync()
        print("DB connected, resolver loaded")
    else:
        # Still need resolver for dry-run display
        resolver = ReferenceResolver()
        resolver.init_sync()

    progress = {} if args.no_resume else load_progress()
    completed = progress.get("completed", {})
    failed = progress.get("failed", {})

    if args.retry_failed:
        retry_names = set(failed.keys())
        print(f"Retry mode: clearing {len(retry_names)} failed producers for re-attempt")
        failed = {}
        if args.execute:
            save_progress({"completed": completed, "failed": failed})

    # Filter producers
    producers = PRODUCERS[:]
    if args.skip_to:
        idx = next((i for i, p in enumerate(producers) if p["name"] == args.skip_to), None)
        if idx is not None:
            producers = producers[idx:]
            print(f"Skipping to: {args.skip_to} (index {idx})")
    if args.limit:
        producers = producers[:args.limit]

    # Process
    total_cost = 0.0
    total_wines = 0
    total_created = 0
    total_vintages = 0
    total_grapes = 0
    processed = 0
    skipped = 0

    for i, entry in enumerate(producers):
        name = entry["name"]

        if name in completed and not args.no_resume:
            print(f"\n[{i + 1}/{len(producers)}] {name} — already done, skipping")
            continue

        if total_cost >= args.budget:
            print(f"\nBUDGET REACHED (${total_cost:.2f} >= ${args.budget:.2f}). Stopping.")
            break

        print(f"\n[{i + 1}/{len(producers)}]", end="")
        stats = process_producer(entry, conn, resolver, client, execute=args.execute)

        total_cost += stats["cost"]
        total_wines += stats["wines_found"]
        processed += 1

        if stats["skipped"]:
            skipped += 1
            failed[name] = {"reason": stats["reason"],
                            "timestamp": datetime.now(timezone.utc).isoformat()}
        else:
            total_created += stats["wines_created"]
            total_vintages += stats["vintages_created"]
            total_grapes += stats["grapes_linked"]
            completed[name] = {
                "wines_found": stats["wines_found"],
                "wines_created": stats["wines_created"],
                "vintages_created": stats["vintages_created"],
                "grapes_linked": stats["grapes_linked"],
                "cost": round(stats["cost"], 4),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        # Save progress after each producer (execute mode only)
        if args.execute:
            save_progress({"completed": completed, "failed": failed})
        time.sleep(FETCH_DELAY)

    # Summary
    print(f"\n{'=' * 65}")
    print(f"SUMMARY")
    print(f"  Processed: {processed} producers ({skipped} skipped)")
    print(f"  Wines found: {total_wines}")
    print(f"  Wines created: {total_created}")
    print(f"  Vintages created: {total_vintages}")
    print(f"  Grapes linked: {total_grapes}")
    print(f"  Total cost: ${total_cost:.4f}")
    print(f"  Progress saved to: {PROGRESS_FILE}")

    if conn:
        conn.close()
    _shutdown_playwright()


if __name__ == "__main__":
    main()
