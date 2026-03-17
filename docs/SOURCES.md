# Loam Data Sources

Master reference for all external data sources — evaluated, integrated, planned, or rejected. Nothing gets lost.

**Last updated**: 2026-03-17
**Coverage**: All 50 US states + DC surveyed. 22+ importers researched. 12 competitions, 17 associations, 16 international retailers, 19 auction/trading platforms, 10+ wine APIs evaluated.

---

## Status Legend

| Status | Meaning |
|--------|---------|
| **INTEGRATED** | Data imported and live in Loam DB |
| **IN HAND** | Data downloaded, analysis complete, import planned |
| **PRIORITY** | Approved for near-term integration |
| **EVALUATED** | Researched, decision pending |
| **DEFERRED** | Evaluated, useful but not needed yet |
| **SKIPPED** | Evaluated and rejected (with reason) |

---

## 1. Wine Identity Sources (Catalog Building)

These sources provide wine-level identity data (producer, wine name, geography, grape, vintage) for building the catalog backbone.

### LWIN Database — IN HAND
- **Status**: Excel file downloaded, deep analysis complete
- **Data**: 186,586 wine records + 3,084 fortified wines (211K total with spirits)
- **Fields**: LWIN-7 code, producer (title + name), wine name, country, region, sub_region, site, parcel, color, type/sub_type, designation (AOP/AVA/DOC/etc.), classification (Premier Cru, Grand Cru, etc.), vintage_config, first/final vintage
- **Unique producers**: 37,369
- **Geography**: 60 countries, 349 regions, 1,516 sub-regions, 1,658 sites
- **Coverage bias**: Strong $30+ (fine wine/auction). Weak $10-30 (mass market). 64K France, 35K US, 24K Italy.
- **What it lacks**: No grapes, no ABV, no barcode, no prices, no scores. Pure identity.
- **License**: Creative Commons (free forever)
- **File**: `data/LWINdatabase.xlsx`
- **Import plan**: Phase A of merge pipeline. Identity backbone.

### TTB COLA via COLA Cloud API — PRIORITY
- **Status**: Free tier available, Kaggle demo analyzed
- **Data**: ~1.2M wine COLAs (2.6M total all beverages). 2,300 new/week.
- **Fields**: TTB ID, brand name, fanciful name, class/type, origin, appellation (96%), grape varietals (54%), vintage year (65%), OCR ABV (87%), barcode (35%), label images, LLM-extracted designations/tasting notes/categories
- **API**: REST at `app.colacloud.us/api/v1/colas`. Filter by `product_type=wine`.
- **Pricing**: Free (500 req/mo = 50K records), Starter $39/mo (10K req = 1M records), Pro $199/mo
- **Coverage**: Every wine label approved for US sale. Best mass-market coverage of any source.
- **What it lacks**: No scores, no prices (these are label approvals, not retail data)
- **Kaggle demo**: `data/imports/cola_demo.zip` (112,904 wine records from 2017-2018)
- **Import plan**: Phase C of merge pipeline. Sign up for $39/mo Starter, pull all wine COLAs.
- **URL**: https://colacloud.us/

### TTB COLA Public Registry — EVALUATED
- **Status**: FOIA request filed 2026-03-16 (backup strategy)
- **Data**: Same 2.6M COLAs as COLA Cloud, but raw fields only (~15 fields)
- **What it lacks vs COLA Cloud**: No grapes, no barcode, no ABV (those require OCR of label images)
- **Access**: Web search at ttbonline.gov. No bulk API. TTB IDs are enumerable (YYDDD + sequence).
- **License**: CC0 (public domain)
- **Verdict**: COLA Cloud is the better path. FOIA is backup. DIY scraping not worth the effort.
- **URL**: https://www.ttbonline.gov/colasonline/publicSearchColasBasic.do

### Wine.com Sitemaps — IN HAND
- **Status**: All 262,474 product URLs downloaded and analyzed
- **Data**: 262,474 wine product URLs across 6 sitemaps + 32,811 vineyard/producer pages
- **Fields from URL slugs**: Wine name + vintage (97% have vintage year), Wine.com product ID. Producer/wine name separation requires dictionary matching.
- **Grape in slug**: ~56%. Appellation in slug: ~16%.
- **Product pages**: DataDome captcha-blocked. No structured data extractable.
- **Coverage**: Broadest US online wine catalog. Good $10-150 coverage.
- **File**: `data/imports/wine_com_all_urls.txt` (20MB)
- **Import plan**: Phase D. Parse URLs, match against canonical wines for identity confirmation + Wine.com IDs.
- **URL**: https://www.wine.com/

### Total Wine Sitemaps — EVALUATED
- **Status**: Sitemaps confirmed accessible (17 product sitemaps, ~9,500 products)
- **URL pattern**: `/wine/red-wine/cabernet-sauvignon/[wine-slug]/p/[id]` — includes grape variety in path
- **Anti-bot**: Product pages return 403
- **Verdict**: Worth extracting — URL pattern is richer than Wine.com (includes wine type + grape in path). ~9,500 wines.
- **URL**: https://www.totalwine.com/

### FirstLeaf Wine Directory — FETCHED ✅
- **Status**: 1,770 products fetched via Shopify JSON API
- **Data**: Schema.org structured data on pages. Strong $10-30 coverage.
- **Script**: `scripts/fetch_firstleaf.mjs` → `data/imports/firstleaf_raw.json`
- **Import method**: Existing `import_shopify_wines.mjs` works
- **URL**: https://www.firstleaf.com/

---

## 2. State Brand Registration Databases (All 50 States + DC)

Government sources containing wines approved for sale in that state. Public data, no licensing concerns.

### Tier 1 — High Value (downloaded or tested)

| State | Status | Fields | Export | Wine Records | Key Value |
|-------|--------|--------|--------|-------------|-----------|
| **Kansas** | ⭐ IN HAND | COLA (92%), appellation (84%), vintage (70%), ABV (100%) | JSON API | 31,216 | COLA IDs bridge to federal TTB |
| **Pennsylvania** | ⭐ IN HAND | UPC (100%), grape, region hierarchy, vintage (60%), price | Excel | 4,812 | Best barcode source |
| **New Jersey** | EVALUATED | UPC, grape variety, registrant, distributor | Web (acct req) | Unknown | UPC + grape (rare combo) |

**Kansas Active Brands**
- **Format**: JSON at `kdor.ks.gov/apps/liquorlicensee/Data/liquorlicenseefull.json`
- **File**: `data/imports/kansas_active_brands.json` (24.6MB)
- **Coverage**: Strong $10-150 US market. 6,625 unique brands including mass-market (Barefoot, Josh, 19 Crimes) that LWIN misses.
- **Import plan**: Phase B of merge pipeline.

**Pennsylvania PLCB Wine Catalog**
- **Format**: Excel download from PA PLCB website
- **File**: `data/imports/pa_wine_catalog.xlsx`
- **Price range**: $2.33 - $2,429.99, median $18.99
- **Import plan**: Phase B of merge pipeline.

**New Jersey POSSE**
- **URL**: https://abc.lps.nj.gov/ABCPublic/
- **Caveat**: Requires free account registration to execute searches

### Tier 2 — PRO Platform (12 states, one scraper)

| State | Status | Notes |
|-------|--------|-------|
| **AR, CO, IL, KY, LA, MN, NM, NY, OH, OK, SC, SD** | PRIORITY | Identical Sovos ShipCompliant system |

- **Fields**: COLA number, brand/label description, vintage, appellation, ABV, supplier, distributor, container type, unit size
- **Export**: "Export Results" to Excel (no auth — `GET /Export/DownloadActiveBrandsSummary`)
- **Wine coverage**: Low (~0.9% in NY). SC has Product Type filter for Wine specifically.
- **Recommended states**: IL (largest market), CO, MN, SC (has wine filter)
- **URL pattern**: `{state}.productregistrationonline.com/brands`

### Tier 3 — Individually Researched (worth pursuing)

| State | Status | URL | Fields | Export | Value |
|-------|--------|-----|--------|--------|-------|
| **North Carolina** | EVALUATED | abc2.nc.gov/Search/Product | Brand, fanciful name, ABV%, wine class, size, status, brand origin | Web scrape only | **HIGH** — ABV + wine classification |
| **Missouri** | EVALUATED | data.mo.gov Socrata | Brand name, type, wholesaler | CSV/JSON API (Socrata) | **MEDIUM** — ~354K rows, great API, no wine metadata |
| **Texas** | EVALUATED | tabcaims.elicense365.com | Label ID, product name, ABV, TTB COLA, date registered | Excel | **MEDIUM** — 2nd largest market, COLA IDs |
| **West Virginia** | EVALUATED | wvabca.com/winesearch.aspx | TTB COLA, brand, fanciful name, class, vintage, ABV | Web scrape only | **MEDIUM** — wine-specific DB |
| **Virginia** | EVALUATED | abc.virginia.gov BWC Reports | Code, brand, supplier, wholesaler | Excel (spirits); web (wine) | **MEDIUM** — 403 on fetch, needs browser |
| **Tennessee** | EVALUATED | tn.gov TNTAP | Brand, COLA required for registration | Unknown (portal timeout) | **MEDIUM** — COLA captured, access unclear |

**North Carolina** — `abc2.nc.gov/Search/Product` and `/Search/Brand`. ASP.NET app with wine class filters (Red, White, Rose, Sparkling, Dessert). Has ABV and brand origin. No COLA/vintage/appellation. Scrape-friendly.

**Missouri** — Socrata open data at `data.mo.gov/api/views/gfq7-aa86/rows.csv`. Filter by `Type='Wine'`. Also has "Conditional Label Approvals" dataset (2,107 rows with Fanciful Name, Class, Proof). No wine-specific metadata but excellent bulk access.

**Tennessee** — COLA is required for brand registration (promising), but TNTAP portal timed out on access. Worth investigating with a browser.

### Tier 4 — Low Value or Login Required

| State | Status | Notes |
|-------|--------|-------|
| **Utah** | IN HAND (LOW) | ~2,970 wines, no vintage/appellation/UPC/COLA/ABV. File: `data/imports/utah_product_list.xlsx` |
| **Alabama** | Spirits only | QPL Excel download for spirits. Wine sold through private retailers, no public DB |
| **Mississippi** | Not public | Control state, ~4,100 products in internal Price Book. Contact abcpurchasing@dor.ms.gov |
| **Maine** | Login required | BELLS portal (launched Oct 2024). COLA captured at registration. Industry-facing. |
| **Rhode Island** | Login required | Tyler Technologies portal. COLA captured. Requires email for account setup. |
| **Vermont** | Spirits only | 802spirits.com covers spirits + fortified only. Table wine via private retailers. |

### Tier 5 — No Database

| State | Reason |
|-------|--------|
| **California** | No brand registration or product database |
| **Washington** | No product DB. Licensee lists + aggregate wine sales only. Privatized spirits 2012. |
| **Arizona** | No brand registration or product database |
| **Maryland** | No brand registration or product database |
| **Delaware** | No brand registration or product database |
| **Hawaii** | No brand registration or product database |
| **North Dakota** | No brand registration or product database |
| **Alaska** | No label registration required. Wholesalers handle brand registration. |
| **Indiana** | ATC has authority but no public product database. Licensee lookup only. |
| **Nevada** | Brand registration (supplier-wholesaler designation) but no product/label registration. |
| **Wisconsin** | Brand registration exists but not publicly searchable. |
| **DC** | Licensee list only, no product database. |
| **Florida** | Limited public access |
| **Georgia** | Limited public access |
| **Massachusetts** | Limited public access |
| **Connecticut** | Limited public access |
| **Michigan** | Login required |
| **Montana** | Login required |
| **Wyoming** | Login required |
| **New Hampshire** | Login required |
| **Oregon** | Spirits-only database |
| **Iowa** | Spirits-only database |
| **Nebraska** | Beer-only brand registration |
| **Idaho** | Small retail catalog, no structured wine data |

---

## 3. Wine Importer Catalogs

Professional importers with public-facing wine catalogs. High-quality metadata from producer tech sheets.

### Tier 1 — High Priority (rich data, easy to scrape)

| Importer | Wines | Countries | Platform | Difficulty | Specialty |
|----------|-------|-----------|----------|------------|-----------|
| **Kermit Lynch** | 1,467 | France, Italy | — | — | ✅ INTEGRATED |
| **Skurnik** | 5,394 | 20+ | FacetWP REST API | EASY | ✅ FETCHED |
| **Polaner Selections** ⭐ | 1,680 | 11 | WordPress REST API | EASY | ✅ FETCHED |
| **Winebow** | 536 | 15 | Drupal | EASY | ✅ FETCHED |
| **Empson** ⭐ | 279 | Italy | WordPress | EASY | ✅ FETCHED |
| **European Cellars** | 443 | Spain, France | WordPress | EASY (slow) | ✅ FETCHED |
| **Kysela** | ~1,000 | 13 | Joomla | MEDIUM | Grape %, vinification detail |
| **Louis/Dressner** | 1,163 | 6 | Custom AJAX | MEDIUM | Natural wine, sulfur data |

**Kermit Lynch Wine Merchant — INTEGRATED**
- 193 producers, 1,467 wines imported
- Script: `scripts/import_kl.mjs` + `scripts/fetch_kl_catalog.mjs`
- File: `data/imports/kermit_lynch_catalog.json`

**Skurnik Wines — FETCHED** ✅
- 5,394 wines fetched via FacetWP REST API (`POST /wp-json/facetwp/v1/refresh`, template `our_wines_22`)
- Phase 1 (listing): 98% producer, 100% grape, 97% appellation/region/country, 77% farming, 98% vintage
- Phase 2 (detail pages, available but not yet run): ABV, blend %, soil, vineyard, aging, scores with drinking windows, tech sheet PDFs
- Country distribution: France 1,057, US 1,039, Italy 812, Germany 731, Spain 382, Austria 324
- Script: `scripts/fetch_skurnik.mjs` → `data/imports/skurnik_catalog.json`
- URL: https://skurnik.com/

**Polaner Selections — FETCHED** ✅
- 1,680 wines fetched via WordPress REST API (`/wp-json/wp/v2/wine`)
- Fields: Wine title, country (99.6%), region (99.6%), appellation (98.2%), certifications (35.1%)
- Taxonomy data only — detailed fields (grapes, soil, vinification) are in ACF, not exposed via API
- Certifications: biodynamic (377), natural (241), HVE (36), organic (11), regenerative (85)
- Country distribution: France 797, Italy 417, USA 249, Spain 107, Portugal 79
- Script: `scripts/fetch_polaner.mjs` → `data/imports/polaner_catalog.json`
- URL: https://www.polanerselections.com/

**Winebow — FETCHED** ✅
- 536 wines from 153 brand pages. Drupal site with excellent per-wine data.
- Coverage: 94% grape, 98% ABV, 93% acidity, 89% RS, 86% pH, 79% soil, 79% production, 51% vineyard, 48% scores, 99% description
- 19 Drupal Views fields per wine: appellation, vineyard name/size, soil composition, elevation, exposure, training method, vines/acre, yield/acre, bottles produced, varietal composition, maceration, MLF, aging vessel size, oak type, pH, acidity, ABV, residual sugar
- Scores section with publication names and tasting notes
- Script: `scripts/fetch_winebow.mjs` → `data/imports/winebow_catalog.json`
- URL: https://winebow.com/

**Empson & Co. — FETCHED** ✅
- 279 wines from WordPress sitemap. Italian specialist with richest per-wine data.
- Coverage: 99% grape, 92% soil, 87% altitude, 57% vine_age, 88% ABV, 84% production, 93% tasting_notes/winemaker/fermentation, 44% oak_type
- 27+ fields per wine: grape (100% breakdown), fermentation (container, duration, temperature, yeast), maceration (technique, duration), aging (container, size, oak type, duration), closure, vineyard (location, size, soil, training, altitude, density, yield, exposure, vine age), harvest timing, production, tasting notes, food pairings, aging potential, ABV, winemaker, serving temp, first vintage
- Script: `scripts/fetch_empson.mjs` → `data/imports/empson_catalog.json`
- URL: https://www.empson.com/wines/

**European Cellars (Eric Solomon) — FETCHED** ✅
- 443 wines from WordPress sitemap. 10-second crawl delay respected.
- Coverage: 100% grape/soil/farming/vinification/aging, 99% altitude/vine_age, 89% producer, 80% scores, 88% certifications
- Fields: Producer (h3.producer-header), wine name (h1), grape, vine age, farming, soil, altitude, vinification, aging, wine type (from CSS class), certifications (organic, biodynamic, vegan), vintage-level scores
- Script: `scripts/fetch_european_cellars.mjs` → `data/imports/european_cellars_catalog.json`
- URL: https://europeancellars.com/

**Kysela Pere et Fils — PRIORITY**
- Aggressive crawl delays (40s for Google/Bing). Several bots blocked.
- Fields: Producer, wine name, grape (with percentages!), region, appellation, tasting notes, soil, vinification (detailed), vine age, production volume, farming practice, tech sheet PDFs
- URL: https://kysela.com/

**Louis/Dressner Selections — PRIORITY**
- Custom AJAX carousel pagination, needs API reverse-engineering.
- Fields: Producer, wine name, filtering metadata (soil, grape, region, farming practice, sulfur use). 78% have detailed tech info.
- URL: https://louisdressner.com/

### Tier 2 — Medium Priority (good catalogs, moderate data)

| Importer | Wines | Countries | Platform | Difficulty | Specialty |
|----------|-------|-----------|----------|------------|-----------|
| **Broadbent** ⭐ | 200-300 | 16 | WordPress | EASY | Portuguese/SA/Lebanese |
| **Vintus** ⭐ | ~1,000+ | 12+ | WordPress/AJAX | MEDIUM | Luxury (Margaux, Guigal, Bollinger) |
| **Rosenthal** ⭐ | 844 (retail) | 5 | Shopify (retail) | EASY | France/Italy artisan |
| **Wilson Daniels** | ~550 | Various | React/Next.js | MEDIUM | DRC, Gaja, Biondi-Santi |
| **MMD USA** ⭐ | ~100-200 | 7 | Custom CDN | MEDIUM | Roederer, Dominus, Pichon Comtesse |
| **Palm Bay** | 100-150 | 10 | React/Next.js | EASY | ABV, acidity, RS, pH data |
| **Olé & Obrigado** | 200-400 | 2 | jQuery DataTable | EASY | Iberian, Port/Sherry |
| **Dreyfus Ashby** | ~150 | France | WordPress/WC | EASY | Drouhin family |

**Broadbent Selections — EVALUATED** ⭐ NEW
- Portuguese wines (Madeira, Port, Vinho Verde), South Africa (Kanonkop, Vilafonte, Warwick), Château Musar
- Fields: Wine name, varietal, region, country, tasting notes, production details, ratings, organic/sustainable/vegan certifications
- Filtering by producer, country, varietal, style, distinction (90+ ratings, organic, vegan, women in wine)
- URL: https://broadbent.com/

**Vintus — EVALUATED** ⭐ NEW
- ~500K cases annually. Ultra-premium (Chateau Margaux, Bollinger, Ornellaia, Masseto, E. Guigal, Sandrone)
- WordPress with Ajax Search Pro. Nonce tokens in AJAX requests.
- Wine-level data depth uncertain — need to assess producer sub-pages
- URL: https://vintus.com/

**Rosenthal Wine Merchant — EVALUATED** ⭐ NEW
- 143 growers (88 France, 41 Italy, 5 Switzerland, 5 Spain, 4 Austria). Artisan producers (Barthod, Fourrier, Montevertine, Brovia)
- Importer site (rosenthalwinemerchant.com): Umbraco CMS, narrative-heavy
- Retail site (rwmselections.com): Shopify, 844 items — easier for structured scraping
- URL: https://www.rosenthalwinemerchant.com/ | https://rwmselections.com/

**Wilson Daniels — EVALUATED**
- ~60 premium producers. React/Next.js, sitemap with 1,084 URLs.
- Winery-level data with fact sheet PDFs. JS rendering likely needed.
- URL: https://wilsondaniels.com/

**Maisons Marques & Domaines (MMD USA) — EVALUATED** ⭐ NEW
- 16-17 brands, ~300K cases. Louis Roederer, Dominus, Pichon Comtesse, Pio Cesare, Domaines Ott, Ramos Pinto, Meerlust
- Custom site with CDN (b-cdn.net). "Portfolio by Wine" page.
- URL: https://www.mmdusa.net/

**Palm Bay International — EVALUATED**
- Fields: Producer, wine name, grape (100%), region, appellation, country, vintage, winemaker, ABV, acidity, RS, pH, closure, tasting notes, production method, vineyard info, tech sheet PDFs
- React/Next.js, 2-second crawl delay. Small but deep.
- URL: https://palmbay.com/

**Olé & Obrigado — EVALUATED**
- jQuery DataTable with all wines loaded on one page! Easiest scrape possible.
- Fields: Producer, wine name, grape, region, country, ABV, soil, tasting notes, production notes
- URL: https://oleobrigado.com/

**Dreyfus, Ashby & Co. — EVALUATED**
- 168 products in sitemap. WordPress + WooCommerce.
- Fields: Producer, wine name, grape, region, appellation, country, soil, production method. Missing: vintage, tasting notes, ABV.
- URL: https://dreyfusashby.com/

### Tier 3 — Lower Priority (limited data, harder access)

| Importer | Wines | Platform | Difficulty | Notes |
|----------|-------|----------|------------|-------|
| **T. Edward** | 500+ | Shopify B2B | MEDIUM | Very sparse data fields |
| **Vineyard Brands** ⭐ | 80+ wineries | ASP.NET legacy | HARD | Ponsot, Dauvissat, Salon, Petrus. Data in PDFs/Salsify |
| **Banfi** ⭐ | 40+ lines | WordPress | MEDIUM-HARD | Wordfence protection. Mostly own-brand. |
| **Ethica Wines** ⭐ | 60+ brands | WordPress/WC | MEDIUM | Italian specialist, thin web data, PDFs have more |
| **Terlato Wine Group** ⭐ | 80+ brands | Salesforce | HARD | Not scraper-friendly |

### Niche Importers (small but interesting)

| Importer | Wines | Notes |
|----------|-------|-------|
| **Jenny & François** | 279 wines, 71 producers | WordPress. Natural wine. Small but data-rich |
| **Rare Wine Co.** | ~36 producers | Umbraco. Library/retail (Conterno, Pingus, Selosse) |

### Importers — INACCESSIBLE

| Importer | Issue | Notable Producers |
|----------|-------|-------------------|
| **Frederick Wildman & Sons** | ECONNREFUSED (site down) | — |
| **Kobrand Corporation** | Expired SSL, ECONNREFUSED | Taittinger, Ruffino, Vega Sicilia |
| **Robert Kacher Selections** | Redirects to `/lander` → 403 | — |
| **Jorge Ordoñez Selections** | Same `/lander` → 403 | — |
| **Martin Scott Wines** | Domain compromised (spam) | — |
| **Dalla Terra** | Cloudflare, blocks ClaudeBot/GPTBot | Vietti, Selvapiana, Lageder, Inama |

### Mass-Market Producers/Distributors — NOT USEFUL FOR SCRAPING

| Company | Cases | Why Not Useful |
|---------|-------|---------------|
| E&J Gallo | 90M | Mass-market domestic, no browsable import catalog |
| The Wine Group | 43M | Bulk/value domestic |
| Trinchero Family | 17M | Domestic (Sutter Home) |
| Delicato Family | 16M | Domestic |
| Deutsch Family | 12M | Mass-market (Yellow Tail, Josh Cellars) |
| Constellation Brands | — | Mass-market (Mondavi, Kim Crawford) |
| Treasury Wine Estates | 5.5M | Corporate (Penfolds, 19 Crimes) |
| Pernod Ricard USA | — | Spirits-focused |

---

## 4. Barcode/UPC Sources

### COLA Cloud Barcodes — PRIORITY
- **Coverage**: ~200-250K wine barcodes extracted from TTB label images
- **Types**: UPC-A (57%), EAN-13 (42%)
- **Access**: Via COLA Cloud API (same as wine identity source above)
- **Note**: Barcodes are wine-level identifiers, NOT vintage-specific (GS1 allows reuse across vintages)

### Pennsylvania PLCB UPCs — IN HAND
- **Coverage**: 4,812 wines with 100% UPC coverage
- **Best free barcode source available right now**

### New Jersey POSSE UPCs — EVALUATED
- **Coverage**: Unknown count, but UPC field confirmed in search results
- **Requires free account registration**

### Open Food Facts — EVALUATED
- **Coverage**: ~16K wines globally, ~2K US. Every product has a barcode (primary key).
- **Access**: Free API + full dump (Parquet/CSV on Hugging Face)
- **License**: ODbL
- **Vintage data**: Inconsistent (sometimes in product name, not structured)
- **URL**: https://world.openfoodfacts.org/

### UPC Data 4 Beverage Alcohol — EVALUATED
- **Coverage**: 150K+ beverage alcohol records, ~30-50K wine subset
- **Fields**: UPC-E/A, EAN-8/13, GS1 manufacturer, brand, item name, container, size, country, region, appellation, ABV
- **Access**: Licensed database (contact for pricing). ASCII/Excel/Access delivery.
- **Quality**: High — sourced from distributor systems (SGWS, RNDC)
- **URL**: https://upcdata4spirits.com/

### CellarTracker Barcodes — SKIPPED (no access)
- **Coverage**: 858,686 UPC/EAN codes mapped to 2M+ wines
- **Access**: No API, community-contributed, locked behind CellarTracker walls
- **Long-term**: Partnership conversation with Eric LeVine

### Validation Tools
- **UPCitemdb**: Free 100 lookups/day. Spot-check tool. https://devs.upcitemdb.com/
- **Go-UPC**: $19.95/mo for 5K calls. Claims wine coverage. https://go-upc.com/
- **GS1 Verified**: Free 30 lookups/day. Authoritative but expensive at scale. https://www.gs1.org/services/verified-by-gs1

---

## 5. Geographic & Boundary Sources

### Integrated

| Source | Data | License | Script |
|--------|------|---------|--------|
| UC Davis AVA Project | 284 US AVA boundaries | CC0 | `import_us_avas.mjs` |
| Natural Earth | 62 country boundaries | Public domain | `import_accurate_boundaries.mjs` |
| Eurac Research EU Wine PDO | 1,174 EU PDO boundaries | CC BY 4.0 | `import_eu_pdo.mjs` |
| Wine Australia GI | 106 Australian GI boundaries | CC BY 4.0 AU | `import_australia_gi.mjs` |
| Germany RLP Vineyard Register | 1,192 Einzellagen boundaries | Datenlizenz DE 2.0 | `import_rlp_einzellagen.mjs` |
| New Zealand IPONZ GI | 21 NZ GI boundaries | — | `import_nz_gi.mjs` |
| Nominatim/OSM | 375 appellation + 64 region boundaries | ODbL | Various geocoding scripts |

### Deferred

| Source | Data | License | URL |
|--------|------|---------|-----|
| INAO AOC Parcel Boundaries (France) | Parcel-level, 289 French AOCs. Gold standard. 253MB shapefile | Licence Ouverte | data.gouv.fr |
| MAPA Spanish Wine DO | Official Spanish DOP/IGP boundaries. 7.33MB shapefile | Free w/ attribution | mapa.gob.es |

---

## 6. Reference Data Sources (Already Integrated)

| Source | Data | Tables Populated |
|--------|------|-----------------|
| VIVC | 9,690 grapes, 34,833 synonyms, parentage | grapes, grape_synonyms |
| INAO OpenDataSoft API | 2,557 French AOC product variants | appellation_aliases |
| Anderson & Aryal Dataset | Region/country grape plantings | region_grapes, country_grapes |
| EU Reg 1308/2013, 2019/33 | Wine regulations | label_designations (116) |
| German Weingesetz, Italian disciplinari, etc. | National wine laws | label_designation_rules, classification_levels (32) |
| WSET L3/L4 + UC Davis Wine Aroma Wheel | Tasting framework | attribute_definitions (73), tasting_descriptors (304) |

---

## 7. Enrichment & Context Sources (Post-Launch)

| Source | Data | License | Status | URL |
|--------|------|---------|--------|-----|
| Open-Meteo Climate API | Historical weather, GDD | Free non-commercial | DEFERRED | open-meteo.com |
| SSURGO (USDA) | Complete US soil maps | Public domain | DEFERRED | websoilsurvey.nrcs.usda.gov |
| SoilGrids 250m | Global soil properties, 250m res | CC BY 4.0 | DEFERRED | soilgrids.org |
| Wikidata Wine Graph | Grape varieties, synonyms, Q-IDs | CC0 | DEFERRED | query.wikidata.org |
| Global Wine Score | Aggregated critic scores | Free API | EVALUATED | globalwinescore.com/api |
| db.wine (Wine Folly) | Producer-verified wine data | Commercial API | EVALUATED | db.wine |
| FooDB Wine Chemistry | ~1,000 wine compounds | CC BY-NC 4.0 | DEFERRED | foodb.ca |
| FlavorGraph (Sony AI) | Food-chemical graph for AI pairing | Apache 2.0 | DEFERRED | github.com/lamypark/FlavorGraph |
| WineSensed Dataset | 350K+ vintages with label images | CC BY-NC-ND 4.0 | DEFERRED | data.dtu.dk |

---

## 8. Score/Review Sources (Deferred)

| Source | Coverage | Access | Status |
|--------|----------|--------|--------|
| Wine Enthusiast | 400K+ reviews | WordPress, scrapeable | DEFERRED |
| Jeb Dunnuck | Free drinking windows | WordPress | DEFERRED |
| Vinous | ~100K+ reviews | Subscription, no API | DEFERRED |
| Wine Advocate / Robert Parker | ~400K+ reviews | Paid subscription | DEFERRED |
| Decanter | ~50K+ reviews | Subscription, UK-centric | DEFERRED |
| JancisRobinson.com | ~200K+ tasting notes | Subscription | DEFERRED |

All score content is copyrighted. Numerical scores are facts (displayable); tasting note text is not reproducible verbatim. See DECISIONS.md.

---

## 9. Wine Competition Databases

Structured medal/score data from professional wine competitions. Public results, no licensing concerns for factual data (medals, scores).

### Tier 1 — High Priority

| Competition | Annual Wines | Data Access | Historical | Key Fields | US Coverage |
|-------------|-------------|-------------|------------|------------|-------------|
| **IWSC** ⭐ | ~4-5K | **CSV export** (email) | 2013+ | Producer, wine, vintage, country, medal, score | Moderate |
| **Berliner Wine Trophy** ⭐ | ~13-15K | Web (200/page, scraper-friendly) | 2009+ (74K total) | Producer, wine, vintage, grape, country, **ABV, RS, acidity**, organic status | Limited (EU-heavy) |
| **TEXSOM** ⭐ | ~3,200 | Web (sortable HTML tables) | **1985+** (40 years!) | Brand, description, appellation, country, vintage, medal, 400+ varieties | Very good |
| **SF Chronicle** | ~5,500 | Web (single printable page) | 2014+ | Winery, wine, vintage, varietal, appellation, medal, price range | **Excellent** (US-focused) |
| **DWWA** | ~18,000 | Web (React SPA, needs API reverse-eng) | 2004+ | Producer, wine, vintage, country, region, grape, medal, score, price | Good |

**IWSC** — iwsc.net/results/search — CSV export via email makes this the easiest competition to integrate. Founded 1969, ~2% Gold rate. No scraping needed.

**Berliner Wine Trophy** — results.wine-trophy.com — Richest structured data: includes alcohol %, residual sugar, acidity alongside medals. 74K awarded wines. OIV-patronized. Also covers Asia Wine Trophy, Portugal Wine Trophy.

**TEXSOM** — texsom.com/results — 40 years of structured, sortable data. 400+ variety filters, 300+ appellation filters. Easy to scrape.

**SF Chronicle** — winejudging.com — Largest North American competition. Categories organized by varietal + price point ($10-150 sweet spot). WordPress, no anti-bot.

### Tier 2 — Worth Pursuing

| Competition | Annual Wines | Notes |
|-------------|-------------|-------|
| **Concours Mondial de Bruxelles** | ~7-10K | results.concoursmondial.com. OIV-recognized. Strong European/South American. |
| **IWC** | ~12-15K | internationalwinechallenge.com. Results also on Wine-Searcher (2008+). |
| **London Wine Competition** | Growing | londonwinecompetition.com. Unique triple score: Quality 50%, Value 25%, Packaging 25%. |
| **Mundus Vini** | ~12K | meininger.de. **403 blocks automated access.** Sensory spider web diagrams. |

### Aggregators

| Source | Coverage | Notes |
|--------|----------|-------|
| **EnofileOnline** | 215K bottles, 15K wineries | enofileonline.com. US regional competition aggregator. Searchable by brand, varietal, price, award, appellation. |
| **Wine-Searcher Awards** | Multiple competitions | wine-searcher.com/awards. Aggregates IWC, IWSC, DWWA, CMB. Anti-bot blocks access. |

### Matching Strategy
Primary match key: producer name + wine name + vintage year + country. Competitions provide structured grape/region/appellation for validation. IWSC CSV is the fastest path to integration.

---

## 10. International Retailers & State Monopolies

Government-run wine monopolies have the best structured data of any commercial source — every field curated centrally.

### Tier 1 — Official APIs (Exceptional Data)

**Vinmonopolet (Norway)** ⭐⭐ — PRIORITY
- **URL**: api.vinmonopolet.no (official API portal)
- **Catalog**: ~35,000 products
- **API**: Free "Open" tier. Register for API key, no approval needed.
- **Fields**: Product ID, name, vintage, **grape varieties with percentages**, ABV, producer, country, district, subdistrict, **EAN-13/GTIN barcode**, price, **sugar g/L, acid g/L**, tasting notes (Norwegian), **flavor scales (tannins, fullness, sweetness, freshness, bitterness — 0-100 numeric)**, food pairings, certifications (organic, biodynamic, fair trade, kosher), cork type, images
- **Language**: Norwegian tasting notes, English field names
- **Quality**: **BEST structured wine data source found in all research.** Government-mandated accuracy. Grape percentages measured. Sugar/acid measured. Flavor scales standardized.
- **Matching**: Producer + wine + vintage + country/district. GTIN barcode enables cross-source dedup.

**Systembolaget (Sweden)** — EVALUATED
- **URL**: api-extern.systembolaget.se (internal API, unofficial access)
- **Catalog**: ~16,400 wines
- **API**: Official API removed. Community data mirror at github.com/AlexGustafsson/systembolaget-api-data (~73MB JSON dumps, regularly updated).
- **Fields**: Product ID, name, vintage, grapes (array), producer, country, origin L1/L2, ABV, price, **taste clock (bitterness, body, sweetness, fruitacid, roughness, smokiness — 1-12 numeric)**, color, food pairings, organic/ethical/kosher labels
- **Language**: Swedish
- **Quality**: Very high but fewer fields than Vinmonopolet (no barcode, no sugar/acid g/L).
- **Legal risk**: Community mirrors may violate ToS. Official API was deliberately removed.

**SAQ — Société des alcools du Québec (Canada)** — EVALUATED
- **URL**: saq.com
- **Catalog**: ~8-12K wines
- **API**: No official API. B2B assortment Excel download exists.
- **Fields**: SAQ code, **UPC/barcode**, producer, **grape variety with % composition**, ABV, region, appellation, **sugar g/L**, tasting notes (aromas, acidity, sweetness, body, mouthfeel, wood), food pairings, serving temp, aging potential, certifications (organic, natural, biodynamic), price
- **Language**: Bilingual English/French
- **Quality**: Very high. UPC barcodes + grape percentages + sugar g/L. Scraping required but clean structure.

### Tier 2 — Good Data, Moderate Access

| Source | Country | Wines | API | Key Value | Notes |
|--------|---------|-------|-----|-----------|-------|
| **Alko** | Finland | ~8,700 | Excel price list + scrape | Monthly Excel download, acidity/tannin levels | alko.fi |
| **LCBO** | Canada/ON | Large | Third-party GraphQL (lcbo.dev) | English, critic scores on some | lcbo.com |
| **BC Liquor** | Canada/BC | Unknown | CSV on Open Canada portal | Open Government Licence | bcliquorstores.com |
| **Berry Bros & Rudd** | UK | ~12,200 | JSON-LD schema.org on pages | **Maturity status** (unique), 327 years of trading | bbr.com |
| **Tannico** | Italy | ~13,000 | Shopify (may support /products.json) | Largest Italian online retailer, bilingual | tannico.com |

**Berry Bros & Rudd** is notable for maturity assessments (ready/youthful/mature/at best/not ready) — a unique data point not available elsewhere.

### Tier 3 — Limited Value

| Source | Country | Notes |
|--------|---------|-------|
| Majestic Wine | UK | ~1,500 wines, no API |
| Wine Society | UK | Members-only access |
| Waitrose Cellar | UK | 403 on fetch |
| Laithwaites | UK | No structured data access |
| Dan Murphy's | Australia | 403, JS-rendered, consumer-grade data |
| Lavinia | France/Spain | 6K wines, no API |
| Nicolas | France | No data access |

---

## 11. Producer & Appellation Associations

Member directories with structured producer data. Best for the California + Oregon vertical slice.

### US Associations (Priority)

| Association | Members | Key Unique Data | Anti-bot | Priority |
|-------------|---------|----------------|----------|----------|
| **Sonoma County Vintners** ⭐ | 250+ | **AVA + grape varieties per producer** | Medium (reCAPTCHA) | HIGHEST |
| **Napa Valley Vintners** | 500+ | Name, address, website | Low | HIGH |
| **Willamette Valley WA** | ~200-300 | **12 sub-AVA attributions**, sustainability cert | Medium (JS) | HIGH |
| **Oregon Wine Board** | 540+ | Region + certifications | HIGH (Cloudflare) | HIGH (hard access) |
| **Paso Robles WCA** | 200+ | AVA districts + certification filters + price ranges | Medium | MEDIUM |
| **Walla Walla Valley WA** | 150+ | 60+ variety filter options | Low | MEDIUM |
| **Washington State Wine** | 1,000+ | Broad coverage | Unknown | MEDIUM |
| **Lodi Winegrape Commission** | ~75 | LODI RULES certification | Medium | LOW |

**Sonoma County Vintners** — sonomawine.com — Best US association directory. Filterable by AVA and grape variety per producer. Directly populates producer→appellation and producer→grape associations.

**Willamette Valley WA** — willamettewines.com — 12 sub-AVA filters (Chehalem Mountains, Dundee Hills, Eola-Amity Hills, etc.). Women-led and sustainability tags.

### International Associations

| Association | Members | Key Data | Priority |
|-------------|---------|----------|----------|
| **UGCB** (Bordeaux) | 134 châteaux | Classification + appellation (authoritative) | HIGH |
| **Consorzio Barolo** | 592 | Village/commune addresses | HIGH |
| **Consorzio Chianti Classico** | 500+ | Municipality + bio cert filter | HIGH |
| **Consorzio Brunello** | 200+ | Production type (Brunello/Rosso/Sant'Antimo) | MEDIUM |
| **Comité Champagne** | 16,000+ | Authoritative if accessible (404 on current URLs) | MEDIUM |
| **UGCB** | 134 | ugcb.net — Classification data is authoritative for Bordeaux | HIGH |

---

## 12. Auction Houses & Trading Platforms

Fine wine price data and drinking windows. Most block programmatic access.

### Accessible

| Source | Type | Public Data | Key Value |
|--------|------|------------|-----------|
| **Liv-ex indices** | Trading | Free (Fine Wine 50/100/1000 + 8 regional sub-indices) | Market context. They create LWIN. |
| **Sotheby's** | Auction | Browsable results (7,617+ searchable) | Hammer prices, provenance |
| **Bonhams** | Auction | Publicly accessible results with prices | Hammer prices with buyer's premium |
| **iDealwine** | Auction (French) | Active listings with prices, 25 years of data | Best French wine auction source |
| **WineBid** | Online auction | Current lots browsable | Consumer auction, broader price range |

### Blocked/Paywalled

| Source | Issue | Notes |
|--------|-------|-------|
| **Acker** | 403 on fetch | Largest US wine auction house, 30+ years of data |
| **Christie's** | 403 on fetch | Premier international, wine since 1766 |
| **Zachys** | 403 on fetch | Major US auction, 20+ years |
| **Hart Davis Hart** | ECONNREFUSED | US fine wine specialist |
| **K&L Auctions** | DataDome | Same protection as K&L retail |
| **Wine-Searcher** | $50K+/year API | Best price aggregator but prohibitively expensive |
| **WineDecider** | 403 on fetch | Unique maturity curves, drinking window data |
| **CellarTracker** | 405 Method Not Allowed | 13M reviews, 5M wines, best community drinking windows |

### Drinking Window / Maturity Data
Best sources: CellarTracker (no access), WineDecider (no access), Berry Bros & Rudd (maturity status on product pages), Jeb Dunnuck (free drinking windows, WordPress). This remains a **significant gap** — no programmatic access to any major maturity data source.

---

## 13. Wine APIs & Databases

### Commercial APIs Worth Investigating

| API | Coverage | Key Value | Pricing | Priority |
|-----|----------|-----------|---------|----------|
| **VineRadar** ⭐ | 40K+ wineries globally | **Vineyard GPS coordinates + terroir data** | Commercial (pricing TBD) | HIGH |
| **db.wine (Wine Folly)** | Producer-verified data | API access, producer-submitted accuracy | Free + Pro tiers | MEDIUM |
| **Wine Labs** | LWIN matching | Automate LWIN assignment to existing wines | Commercial | MEDIUM |
| **Winevybe** | General wine DB | Regions, grapes, awards, packaging | RapidAPI | LOW |

**VineRadar** — vineradar.com — 40K+ wineries with vineyard GPS, terroir data, tasting notes, food pairings, 500+ grape profiles. Sub-100ms API. SOC 2. Distribution: 8,500 Bordeaux/Burgundy/Champagne, 12K Italy, 4.7K California, 4.3K Spain, 2.8K Australia. **Directly serves Loam's terroir mission.** Contact for pricing.

**db.wine** — Producer-verified wine data with API. Quality > quantity. Worth evaluating free tier.

### Community / Academic

| Source | Coverage | Notes |
|--------|----------|-------|
| **wein.plus** | 248K wines, 25K EU producers, 26K-keyword wine lexicon | Membership-gated |
| **WineSensed** | 897K label images, 350K+ vintages | CC BY-NC-ND 4.0 (non-commercial) |
| **X-Wines** | 530K wines, 2.2M vintages | Already in Loam as xwines_* tables |

---

## 14. Certification & Agricultural Databases

### Sustainability Certifications

| Source | Coverage | Access | Priority |
|--------|----------|--------|----------|
| **USDA Organic Integrity DB** ⭐ | All USDA-certified organic producers incl. wineries | Free, searchable (ams.usda.gov/integrity) | **HIGH** — quick win |
| **Demeter International** | 7,000+ biodynamic farms globally (wine subset) | Browsable map at demeter.net | MEDIUM |
| **SIP Certified** | 43K vineyard acres (CA, OR, MI) | Searchable on sipcertified.org | LOW |
| **LIVE Certified** | Oregon sustainable wineries | Searchable | LOW |

**USDA Organic Integrity DB** — Free, structured, covers all certified organic wineries in the US. Maps directly to `producer_farming_certifications`. Quick win for sustainability data.

### Agricultural Data

| Source | Data | Access | Value |
|--------|------|--------|-------|
| **USDA NASS** | CA grape acreage by variety + county | Free (Quick Stats API) | MEDIUM — enriches region_grapes |
| **OIV Statistics** | Global production, consumption, trade | Free reports | LOW — macro level |
| **Eurostat Vineyard Stats** | EU vineyard area, varieties, vine age | Free open data | LOW — macro level |
| **AAWE Annual Database** | Global wine markets 1835-2018 | Free Excel | LOW — academic economics |

### Emerging: EU E-Labels (u-label.com)
Since Dec 2023, all EU wines must have digital labels (QR codes) with ingredients, nutrition, and allergen data. u-label.com (operated by CEEV/EFOW) has ~500K+ wines registered. **This is a massive new structured data source that didn't exist 2 years ago.** API access unclear — needs research. Post-launch but HIGH value for ingredient/nutrition data on EU wines.

---

## 15. Skipped Sources (with reasons)

| Source | Reason |
|--------|--------|
| Wine-Searcher API | $250-2,000/mo. Aggressive bot blocking. |
| CellarTracker bulk | No API, ToS prohibits commercial use |
| Vivino scraping | Already have X-Wines (530K wines). ToS prohibits scraping. |
| Amazon wine data | Restricted category, spotty coverage |
| Instacart | Location-dependent, aggressive anti-bot |
| Kroger/Fred Meyer | Aggressive anti-bot (Cloudflare) |
| Costco online | Limited online catalog (~500 SKUs), aggressive protection |
| Trader Joe's | No online product catalog at all |
| Google Shopping | Merchant-only API, scraping against ToS |
| GS1 Data Hub | $500-6,500/yr, uncertain wine coverage |
| Southern Glazer's / RNDC / Breakthru | B2B only, no public access |
| SevenFifty | 47K+ products but Imperva-blocked. Partnership play only. |
| Commerce7 / WineDirect DTC stores | No central catalog, labor-intensive per winery |
| Somm.ai restaurant lists | Expensive B2B platform |
| Wine subscription clubs | Most sell private-label wines with no real producer identity (Winc, Bright Cellars, WSJ Wine, Martha Stewart Wine) |
| Naked Wines | Cloudflare + reCAPTCHA. Independent winemaker profiles have some value but hard to access |
| Dan Murphy's (Australia) | 403, JS-rendered, consumer-grade data |
| WIN Data (Wine Industry DB) | Expensive B2B subscription. US producer operational data |
| Winevizer | Restaurant wine list tool, not a data source |
| Open Wine Data / OpenWines.eu | Nascent/inactive projects |

---

## 16. Coverage Gap Analysis

### Geographic Gaps

| Region | Gap Severity | Current Coverage | Suggested Sources |
|--------|-------------|-----------------|-------------------|
| **Australia/NZ** | HIGH | No AU/NZ importer. LWIN + COLA cover identity. | Old Bridge Cellars, Negociants USA, Wine Australia trade portal |
| **South America** | HIGH | Only Catena Zapata trial import | Vine Connections, Click Wine Group, Wines of Chile, Wines of Argentina |
| **South Africa** | HIGH | Only Broadbent + Kanonkop | Cape Classics (~40 producers), Indigo Wine |
| **Portugal** | HIGH | Only Broadbent (fortified) + Olé & Obrigado | ViniPortugal, Sogrape, IVDP (Port registry) |
| **Germany/Austria everyday** | MEDIUM | Skurnik covers enthusiast. Mass-market thin. | DWI, Austrian Wine Marketing Board |
| **Greece/Croatia/Hungary** | MEDIUM | Trial imports only | Diamond Wine Importers, Athenee Importers |
| **UK/Canada emerging** | LOW | No sources | Wines of Great Britain, Wine Growers Canada |

### Data Field Gaps

| Field | Sources | Gap Severity |
|-------|---------|-------------|
| **Soil composition** | Only importers (~12K wines) | HIGH |
| **Elevation/altitude** | Empson, VineRadar (if API accessible) | HIGH |
| **Vine age** | Empson, Kysela, European Cellars | HIGH |
| **Drinking windows** | No programmatic source | HIGH |
| **Sustainability certifications** | USDA Organic DB (quick win) + importers | MEDIUM |
| **Production volume** | Empson, Winebow, Kysela | MEDIUM |
| **Sugar/acid g/L** | Vinmonopolet API, Winebow, Palm Bay | MEDIUM |
| **Vineyard GPS** | VineRadar API (if accessible) | MEDIUM |

### Critical Technical Gap
**Identity matching engine** — Without reliable fuzzy matching (producer + wine + vintage + region), scaling beyond ~12K importer wines into the 200K+ LWIN/COLA universe will create deduplication crisis. This is the #1 technical priority. See merge architecture in CLAUDE.md.

---

## 17. Coverage Projections

### By Price Segment

| Segment | Primary Sources | Est. Unique Wines |
|---------|----------------|-------------------|
| $50-150 (enthusiast) | LWIN + COLA Cloud + Importers | ~80K |
| $20-50 (wine shop) | LWIN + COLA + Wine.com + State DBs | ~100K |
| $10-20 (grocery/mass) | COLA Cloud + Kansas + PA + Wine.com | ~70K |
| **Total** | | **~200-250K unique wines** |

### Barcode Coverage
~200K+ barcode mappings (COLA Cloud + PA + NJ + OFF). Covers the majority of wines a US consumer would encounter.

---

## 11. Import Priority Order

| Phase | Source | Est. Wines | Key Value |
|-------|--------|-----------|-----------|
| A | **LWIN** | 186K | Identity backbone |
| B | **Kansas + Pennsylvania** | 36K | COLA IDs + UPCs |
| C | **COLA Cloud API** | ~1.2M COLAs | Barcodes + appellation + grapes at scale |
| D | **Importer catalogs** | ~12K | Deep winemaking metadata |
| | — Skurnik | 5,394 ✅ | German/Austrian |
| | — Polaner ⭐ | 1,680 ✅ | Best new discovery |
| | — Winebow | 536 ✅ | Chemistry data |
| | — European Cellars | 443 ✅ | Spanish/French terroir |
| | — Empson ⭐ | 279 ✅ | Italian tech sheets |
| | — Kysela | 1K | Grape percentages |
| | — Louis/Dressner | 1.2K | Natural wine |
| E | **Wine.com sitemaps** | 262K URLs | Identity confirmation |
| F | **Total Wine sitemaps** | 9.5K | Secondary retailer |
| G | **PRO Platform states** | 12 states | Consistent COLA data |
| H | **NC + MO + TX state DBs** | Unknown | Additional state coverage |
| I | **Open Food Facts** | ~16K | Supplemental barcodes |
| J | **FirstLeaf** | 5.1K | Value segment |

See `docs/ENRICHMENT.md` for the merge engine architecture and `CLAUDE.md` for schema details.
