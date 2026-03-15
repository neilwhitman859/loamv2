# Loam Data Sources

Master reference for all external data sources — evaluated, integrated, or planned.

**Last updated**: 2026-03-14

---

## Status Legend

| Status | Meaning |
|--------|---------|
| **INTEGRATED** | Data imported and live in Loam DB |
| **PRIORITY** | Approved for near-term integration |
| **PLANNED** | Approved but deferred to a later phase |
| **EVALUATED** | Researched, decision pending |
| **SKIPPED** | Evaluated and rejected (with reason) |

---

## Integrated Sources

### Vivino (via xwines staging tables)
- **Data**: 529,973 wines, 32,349 producers, 2,175,664 vintages, 306,024 scores, 49,512 prices
- **Format**: API scraping → JSONL → Supabase staging tables (xwines_*)
- **License**: Proprietary (scraped)
- **Scripts**: `fetch_vivino_mega.mjs`, `match_vivino_to_loam.mjs`, `create_wines_from_vivino.mjs`, `fetch_producer_wines.mjs`
- **Notes**: Data in staging tables, not yet fully promoted to production

### UC Davis AVA Project
- **Data**: 284/284 US appellations with PostGIS boundary polygons
- **Format**: GeoJSON (CC0)
- **License**: CC0 (public domain)
- **URL**: https://github.com/UCDavisLibrary/ava
- **Script**: `import_us_avas.mjs`
- **Coverage**: 100% of US AVAs
- **Notes**: Gold standard for US appellations. All imported.

### Natural Earth Country Boundaries
- **Data**: 62/62 wine-producing countries with PostGIS boundary polygons
- **Format**: Shapefile (public domain)
- **License**: Public domain
- **Script**: `import_accurate_boundaries.mjs`
- **Coverage**: 100% of countries in DB

### Winery Scrapers (Production Tables)
- **Ridge Vineyards**: 182 wines, 1,119 vintages, 867 scores | Script: `scrape_ridge.mjs`
- **Stag's Leap Wine Cellars**: 29 wines, 190 vintages, 173 scores
- **Tablas Creek Vineyard**: 56 wines, 448 vintages, 1,174 scores, 116 grape entries | Script: `scrape_tablas_creek.mjs`
- **Format**: Deterministic CSS-selector scraping → JSONL → Supabase
- **License**: N/A (public website data)
- **Notes**: 100% accuracy via deterministic parsing. Each winery publishes different data fields.

### Nominatim / OSM Boundaries (partial)
- **Data**: 375/662 non-US appellations, 64/352 regions
- **Format**: Nominatim API → PostGIS
- **License**: ODbL
- **Scripts**: `geocode_appellations.mjs`, `fetch_global_boundaries.mjs`
- **Notes**: Matches municipality/city boundaries, NOT actual wine appellation boundaries. Being superseded by authoritative sources below.

### EU Wine PDO Geospatial Inventory (Eurac Research)
- **Data**: 1,174 wine PDOs across 21 European countries — boundaries + authorized grape varieties + max yields
- **Format**: GeoPackage (boundaries) + CSV (regulatory data)
- **License**: CC BY 4.0
- **URL**: https://doi.org/10.6084/m9.figshare.c.5877659.v1
- **Interactive map**: https://winemap.eurac.edu/
- **Script**: `import_eu_pdo.mjs`
- **Coverage**: 1,174 EU appellations imported with municipality-level boundary polygons
- **Accuracy**: Municipality-level approximation (better than Nominatim city-center, but not parcel-level like INAO)

### Wine Australia GI Boundaries
- **Data**: 106 Australian GIs (zones, regions, subregions) with official boundary polygons
- **Format**: GeoJSON
- **License**: CC BY 4.0 Australia
- **URL**: https://wineaustralia-opendata-wineaustralia.hub.arcgis.com/
- **Script**: `import_australia_gi.mjs`
- **Coverage**: 100% of Australian GIs

### Germany Rhineland-Palatinate Vineyard Register
- **Data**: 1,192 German Einzellagen (individual vineyard sites) for 6 RLP wine regions
- **Format**: JSON via OGC API
- **License**: Datenlizenz Deutschland 2.0
- **URL**: https://demo.ldproxy.net/vineyards/collections
- **Script**: `import_rlp_einzellagen.mjs`
- **Coverage**: 1,192 RLP Einzellagen with official vineyard boundaries

### New Zealand IPONZ GI Boundaries
- **Data**: 21 NZ GIs (18 with official boundary polygons, 3 enduring GIs)
- **Format**: Esri JSON (converted to GeoJSON)
- **License**: IPONZ (New Zealand IP Office)
- **Script**: `import_nz_gi.mjs`
- **Coverage**: 18/21 NZ GIs with boundaries

### Argentina INV Appellations
- **Data**: 36 Argentine IGs/DOCs from official INV list
- **Format**: Manual data entry from official INV list + Nominatim geocoding
- **Script**: `import_argentina_ig.mjs`
- **Coverage**: All Argentine IGs/DOCs, centroids only (no boundary data available)

### VIVC (Vitis International Variety Catalogue)
- **Data**: 9,690 grapes imported (9,400 wine grapes from Phase 1 crawl + enrichment)
- **Format**: Web scraping (structured HTML tables)
- **License**: JKI (German Federal Research Centre)
- **URL**: https://www.vivc.de/
- **Script**: `import_vivc_grapes.mjs`
- **Cache**: `data/vivc_grapes_cache.json`
- **Loam use**: Authoritative grape reference. Grape names, colors, countries of origin. Synonym and parentage data available for future phases.

---

## Regulatory & Reference Sources

These are the authoritative regulatory and reference sources used to populate Loam's reference data tables (label designations, classifications, varietal categories, blend compositions). These are not imported as datasets — they were consulted by a wine expert (Claude) to seed accurate reference data.

### Label Designations (98 designations, 200 rules, 14 categories)

| Source | Jurisdiction | What it governs | How used in Loam |
|--------|-------------|-----------------|------------------|
| **EU Reg 1308/2013** (CMO Regulation) | EU-wide | Wine labeling framework, quality tiers (PDO/PGI/varietal) | EU-wide label designation categories, quality tier definitions |
| **EU Reg 2019/33** (Delegated Act) | EU-wide | Sweetness terms and residual sugar thresholds for still and sparkling wines | Trocken ≤9 g/L, Halbtrocken ≤18 g/L, sparkling sweetness scale (Brut Nature through Doux) |
| **German Weingesetz + Weinverordnung** | Germany | Prädikat wine system, Oechsle minimums by climate zone | 78 rules: 13 Anbaugebiete × 6 Prädikat levels with Zone A/B Oechsle minimums |
| **Italian DOCG/DOC disciplinari di produzione** | Italy | Riserva aging, Superiore ABV/yield thresholds per denomination | 31 Superiore rules + 23 Riserva rules across specific DOCGs/DOCs |
| **Spanish Ley del Vino 24/2003 + Consejo Regulador rules** | Spain | Crianza/Reserva/Gran Reserva aging requirements | 7 rules for Rioja, Ribera del Duero, Navarra deviations from national defaults |
| **Portuguese IVV regulations** | Portugal | Reserva/Grande Reserva ABV thresholds, Garrafeira aging | 14 rules: ABV thresholds by DOC + Garrafeira national rule |
| **Austrian Weingesetz 2009 §14** | Austria | KMW minimums for Prädikat wines | Spätlese through TBA threshold rules |
| **Hungarian Tokaj Wine Region law** | Hungary | Aszú, Eszencia requirements | Puttonyos system, Eszencia RS minimums |
| **INAO decrees** (various) | France | VT, SGN, Nouveau, Crémant specifications | French-specific label designations and rules |
| **WSET Level 3 Award in Wines** | International | Standard wine terminology reference | Cross-reference for consistency across jurisdictions |

### Classifications (13 systems, 32 levels)

| Source | System | How used in Loam |
|--------|--------|------------------|
| **1855 Exposition Universelle records** | Bordeaux 1855 Médoc + Sauternes | 5 Médoc crus + 3 Sauternes tiers (Premier Cru Supérieur, Premier, Deuxième) |
| **INAO decree (Legifrance JORFTEXT000046772378)** | Saint-Émilion Grand Cru Classé | 2022 revision: 3 tiers (Premier Grand Cru Classé A/B, Grand Cru Classé) |
| **French ministerial decree 1953** | Graves Classification | Single tier (Cru Classé) |
| **French ministerial decree 1955** | Provence Cru Classé | Single tier (Cru Classé), 18 estates |
| **INAO regulations** | Burgundy Vineyard Classification | 4 tiers: Grand Cru, Premier Cru, Village, Regional |
| **INAO regulations** | Alsace Grand Cru | Single tier (Grand Cru), 51 vineyard sites |
| **INAO / CIVC regulations** | Champagne Premier & Grand Cru | 2 tiers: Grand Cru (17 villages, 100% échelle), Premier Cru (44 villages, 90-99%) |
| **EU regulation 2006 + French ministerial arrêté** | Cru Artisan | Single tier, 8 Haut-Médoc appellations |
| **Alliance des Crus Bourgeois (2020 re-establishment)** | Cru Bourgeois | 3 tiers: Cru Bourgeois Exceptionnel, Supérieur, Cru Bourgeois |
| **VDP (Verband Deutscher Prädikatsweingüter)** | VDP Germany | 4 tiers: Grosse Lage, Erste Lage, Ortswein, Gutswein |
| **ÖTW (Österreichische Traditionsweingüter)** | ÖTW Austria | 2 tiers: Erste Lage, Klassifizierte Lage |
| **Langton's Fine Wine Auctions** | Langton's Classification of Australian Wine | 4 tiers: Exceptional, Outstanding, Excellent, Distinguished (7th edition, 2018) |

### Varietal Categories (159 categories) & Blend Compositions (94 rows)

| Source | Scope | How used in Loam |
|--------|-------|------------------|
| **DOCG/DOC disciplinari di produzione** | Italian blends | Chianti Classico (Sangiovese 80-100%), Amarone/Valpolicella (Corvina 45-95% + Corvinone + Rondinella), Barolo/Barbaresco (100% Nebbiolo), Brunello (100% Sangiovese), Prosecco (Glera 85-100%), Soave (Garganega 70-100%) |
| **INAO cahiers des charges** | French blends | Champagne Blend (Chardonnay + Pinot Noir + Meunier), Châteauneuf-du-Pape (13 varieties), Côtes du Rhône (Grenache + Syrah + Mourvèdre), Bordeaux Blend (6 red, 5 white varieties) |
| **Consejo Regulador rules** | Spanish blends | Rioja Blend (Tempranillo + Garnacha + Graciano + Mazuelo), Priorat (Garnacha + Cariñena), Cava Blend (Macabeo + Parellada + Xarel·lo) |
| **Cape Winemakers Guild** | South Africa | Cape Blend: Pinotage required (30-70%) + Bordeaux varieties |
| **Meritage Alliance** | USA | Meritage: Bordeaux varieties only, no single variety >90% |
| **WSET Level 3 Award in Wines** | International | General varietal category definitions, standard blend names, grape variety reference |
| **Oxford Companion to Wine (4th ed.)** | International | Grape name standardization (e.g., Negroamaro not Negramaro), synonym resolution |
| **Wine Spectator** | International | Cross-reference for grape naming conventions |

### Key Regulatory Principles Applied

1. **Appellation-specific rules override national defaults**: e.g., Rioja Reserva aging differs from Spanish national standard
2. **NULL appellation_id = national-level rule**: When a label designation rule applies country-wide (e.g., Portuguese Garrafeira), `appellation_id` is NULL
3. **NULL color = not always the same**: If a varietal category can be made in multiple colors (e.g., Provence Blend, Field Blend), `color` is NULL rather than defaulting to the most common color
4. **Synonym handling**: Where grapes have multiple names (Macabeo=Viura, Tinta Roriz=Tempranillo, Mazuelo=Carignan), the canonical grape ID is used regardless of regional naming

---

## Priority Sources (Near-term Integration)

### INAO AOC Parcel Boundaries (France)
- **Data**: Parcel-level boundaries for 289 French wine AOCs (~3.8M parcels)
- **Format**: Shapefile (EPSG:2154 Lambert93), 253.8 MB
- **License**: Licence Ouverte (equivalent to CC BY)
- **URL**: https://www.data.gouv.fr/datasets/delimitation-parcellaire-des-aoc-viticoles-de-linao
- **Current gap**: French appellations use Nominatim city boundaries. INAO gives actual vineyard parcels.
- **Loam use**: Replace French appellation boundaries with gold-standard parcel data
- **Accuracy**: Parcel-level precision. THE authoritative source.

### MAPA Spanish Wine DO Shapefiles
- **Data**: Official boundaries for all Spanish DOP and IGP wine regions
- **Format**: Shapefile (ETRS89), 7.33 MB
- **License**: Free with MAPA attribution
- **URL**: https://www.mapa.gob.es/es/cartografia-y-sig/ide/descargas/alimentacion/vinos
- **Download**: https://www.mapa.gob.es/app/descargas/descargafichero.aspx?f=calidaddiferenciada_vinos.zip
- **Current gap**: Spanish appellations use Nominatim approximations
- **Loam use**: Official Spanish DO boundaries. ~69+ denominations.
- **Accuracy**: Official government cartography at 1:25,000 scale

### Liv-ex LWIN Database
- **Data**: 200,000+ wines and spirits with standardized LWIN identifiers
- **Format**: CSV download, APIs with membership
- **License**: Creative Commons (free forever)
- **URL**: https://www.liv-ex.com/lwin/
- **Loam use**: Add `lwin_id` to wines table. Universal wine identifier used by auction houses and trading platforms. Cross-reference backbone.

### Importer Site Scrapers
- **Kermit Lynch Wine Merchant**: French/Italian specialty. ~300+ wines. Structured producer pages with tech sheet data.
- **Skurnik Wines**: Major US importer. Structured wine detail pages.
- **Louis/Dressner Selections**: ~78% have detailed tech info. Natural wine focus.
- **Format**: Deterministic CSS-selector scraping (100% accuracy approach)
- **License**: N/A (public website data)
- **Loam use**: High-quality wine data for European producers. One template per importer = hundreds of wines per scraper.
- **Notes**: Priority for wine table population. Professional data entry means high reliability.

### K&L Wines (Retailer)
- **Data**: Multiple critic scores per wine, prices, availability
- **Format**: Server-rendered HTML, sitemaps available, no bot protection
- **License**: N/A (public website data)
- **URL**: https://www.klwines.com/
- **Loam use**: Best aggregator target. Multi-critic scores in one place. Price data.
- **Notes**: Most accessible major retailer for scraping

### Wikidata Wine Graph (SPARQL)
- **Data**: Grape varieties with synonyms, parentage, origins, VIVC IDs, Q-IDs
- **Format**: SPARQL endpoint → JSON/CSV
- **License**: CC0 (public domain)
- **URL**: https://query.wikidata.org/
- **Loam use**: Grape synonym resolution (Syrah=Shiraz), parentage data, cross-reference IDs. Build a synonym lookup table.
- **Accuracy**: Community-maintained but well-curated for grape varieties

### USDA NASS QuickStats API
- **Data**: US grape production, acreage, prices, yields by state/county/variety
- **Format**: REST API (JSON/CSV/XML), free key, max 50K records/query
- **License**: Public domain
- **URL**: https://quickstats.nass.usda.gov/api
- **Current gap**: No production/market data in Loam
- **Loam use**: Production context for US wines. Need to design schema for production/market data tables.

### FAOSTAT Global Production/Trade
- **Data**: Global grape/wine production, trade, consumption — 245 countries, 1961–present
- **Format**: CSV bulk download, R/Python packages
- **License**: CC BY-NC-SA 3.0 IGO
- **URL**: https://www.fao.org/faostat/ | Bulk: https://fenixservices.fao.org/faostat/static/bulkdownloads
- **Loam use**: Country-level production and trade data. Connects land with trade.

### TTB COLA Registry
- **Data**: 2.6M+ wine label approvals — brand, appellation, class, vintage, alcohol, label images
- **Format**: Web search + limited API. ~2,300 new records/week
- **License**: Public domain
- **URL**: https://www.ttbonline.gov/colasonline/publicSearchColasBasic.do
- **Loam use**: Validation tool for wine data accuracy. Authoritative US wine census. Label images.
- **Notes**: Bulk extraction is the challenge. Consider as validation layer initially, not primary source.
- **See also**: COLA Cloud (https://colacloud.us/) — commercial AI-enriched wrapper, enterprise pricing

---

## Planned Sources (Deferred)

### Portugal IVV DO Shapefiles
- **Data**: Portuguese wine region boundaries
- **Format**: Shapefiles (reported)
- **License**: Portuguese government data
- **URL**: https://www.ivv.gov.pt/
- **Loam use**: Portuguese appellation boundaries

### Wine Enthusiast Reviews
- **Data**: 400K+ professional reviews with scores, tasting notes
- **Format**: WordPress site, scrapable
- **License**: Copyrighted content
- **URL**: https://www.winemag.com/
- **Loam use**: Critic scores and tasting note text. Circle back later.

### Jeb Dunnuck
- **Data**: Free drinking windows, professional scores, tasting notes
- **Format**: WordPress site
- **License**: Copyrighted content
- **URL**: https://jebdunnuck.com/
- **Loam use**: Drinking windows (unique free source), critic scores. Circle back later.

### SSURGO Soil Data (USDA)
- **Data**: Complete US soil maps — type, texture, drainage, pH, organic matter, depth
- **Format**: Shapefile + SQLite (gSSURGO), Soil Data Access REST API
- **License**: Public domain
- **URL**: https://websoilsurvey.nrcs.usda.gov/ | API: https://sdmdataaccess.nrcs.usda.gov/
- **Loam use**: Overlay soil data on AVA boundaries = true terroir profiles. HUGE for differentiation.
- **Notes**: High effort but massive payoff. Phase 3+.

### Open-Meteo Climate API
- **Data**: Historical weather (ERA5 reanalysis 1940–present), growing degree days built-in
- **Format**: REST API (JSON), no key needed
- **License**: Free non-commercial; commercial plans available
- **URL**: https://open-meteo.com/
- **Loam use**: Compute Winkler GDD indices for wine regions. Vintage weather analysis.
- **Notes**: Better than NASA POWER for our use case (higher resolution, built-in GDD, cleaner API)

### Computational Wine Wheel 2.0 (CWW)
- **Data**: 985 categorized wine descriptor attributes from 100K+ professional reviews
- **Format**: CSV on IEEE DataPort (open access, login required)
- **License**: Open Access
- **URL**: https://ieee-dataport.org/open-access/wineinformatics-21st-century-bordeaux-wines-dataset
- **Loam use**: Machine-readable sensory vocabulary. Canonical descriptor taxonomy for parsing tasting notes.

### eAmbrosia EU GI Register
- **Data**: Official EU register of all PDO/PGI wines — status, authorized varieties, product specs
- **Format**: Web + API at data.europa.eu
- **License**: EU open data
- **URL**: https://ec.europa.eu/agriculture/eambrosia/geographical-indications-register/
- **Loam use**: Authorized grape varieties and winemaking rules per appellation

### OIV Statistics Database
- **Data**: Global wine production, consumption, trade — 50+ countries
- **Format**: Web visualization, PDF publications
- **License**: Free and unrestricted
- **URL**: https://www.oiv.int/what-we-do/statistics
- **Loam use**: Authoritative global production/trade comparisons

### Eurostat Vineyard Survey
- **Data**: EU vineyard area, vine varieties, age class by country and NUTS-2 region
- **Format**: TSV, JSON via SDMX API
- **License**: Free with attribution
- **Loam use**: European vineyard planting data

### TTB Wine Production Statistics
- **Data**: Monthly/annual US wine production by type and state
- **Format**: CSV, JSON
- **License**: Public domain
- **URL**: https://www.ttb.gov/regulated-commodities/beverage-alcohol/wine/wine-statistics
- **Loam use**: US production trends

### OpenStreetMap Vineyard Polygons
- **Data**: Community-mapped vineyard boundaries globally (`landuse=vineyard`)
- **Format**: Overpass API (GeoJSON)
- **License**: ODbL
- **URL**: https://overpass-turbo.eu/
- **Loam use**: Show where vineyards physically exist within appellations. Great France/Germany/Italy coverage.

### SoilGrids 250m (Global)
- **Data**: Global soil properties at 250m resolution — pH, carbon, texture, etc.
- **Format**: WCS, WebDAV (GeoTIFF)
- **License**: CC BY 4.0
- **URL**: https://soilgrids.org/
- **Loam use**: Global soil data for terroir characterization (complements US-only SSURGO)

### PRISM Climate Data
- **Data**: High-resolution (800m) US climate data 1895–present
- **Format**: BIL raster, R package
- **License**: Free for most uses
- **URL**: https://prism.oregonstate.edu/
- **Loam use**: Terrain-aware US climate data. Better than Open-Meteo for US-specific analysis.

### FooDB Wine Chemistry
- **Data**: ~800–1,000 chemical compounds found in wine with concentrations
- **Format**: CSV (952 MB), JSON (87 MB), MySQL dump
- **License**: CC BY-NC 4.0
- **URL**: https://foodb.ca/downloads
- **Loam use**: Wine chemistry profiles by type. Powers "what compounds are in this wine" features.

### Phenol-Explorer
- **Data**: 500+ polyphenols in wine with concentration data
- **Format**: Excel/CSV export
- **License**: Free academic
- **URL**: http://phenol-explorer.eu/
- **Loam use**: Polyphenol/health compound data by wine type

### FlavorGraph (Sony AI)
- **Data**: 8K node food-chemical graph, 147K edges, 300-dim food embeddings
- **Format**: Pickle files, PyTorch
- **License**: Apache 2.0 (commercial OK)
- **URL**: https://github.com/lamypark/FlavorGraph
- **Loam use**: AI-driven food-wine pairing engine

### WineSensed Dataset (DTU)
- **Data**: 897K label images, 824K reviews, 350K vintages, pairwise flavor distances
- **Format**: CSV + JPG images
- **License**: CC BY-NC-ND 4.0
- **URL**: https://data.dtu.dk/articles/dataset/23376560
- **Loam use**: Label recognition, flavor similarity features

### Argentina INV Open Data
- **Data**: Vineyard area by province, wine production by variety, export data
- **Format**: CSV (CC BY 4.0)
- **URL**: https://datos.magyp.gob.ar/dataset/inv-actividad-vitivinicola
- **Loam use**: Argentine production data

---

## Skipped Sources (with reasons)

### X-Wines Dataset
- **Data**: 100,646 wines, 21M ratings (CC0)
- **Reason**: Already explored; data too scattered and overlaps with Vivino staging data
- **URL**: https://github.com/rogerioxavier/X-Wines

### USDA FoodData Central
- **Data**: Wine nutritional data (calories, sugar, vitamins)
- **Reason**: Not needed for current roadmap
- **URL**: https://fdc.nal.usda.gov/

### TTB Permittees List
- **Data**: All US wine producers with federal permits
- **Reason**: Deprioritized for now
- **URL**: https://www.ttb.gov/public-information/foia/list-of-permittees

### Adelaide Wine Economics Databases
- **Data**: 11 databases, 200 years of global wine data
- **Reason**: Deprioritized for now. Revisit for market intelligence features.
- **URL**: https://economics.adelaide.edu.au/wine-economics/databases

### FlavorDB2
- **Data**: 25,595 flavor molecules, 936 ingredients
- **Reason**: Deprioritized for now
- **URL**: https://cosylab.iiitd.edu.in/flavordb2/

### USDA Organic Integrity Database
- **Data**: Certified organic operations
- **Reason**: Deprioritized for now
- **URL**: https://organic.ams.usda.gov/integrity/

### Wine-Searcher API
- **Data**: Wine pricing and availability
- **Reason**: Commercial ($250–2,000/mo). Aggressive bot blocking.
- **URL**: https://www.wine-searcher.com/trade/api

### CellarTracker
- **Data**: 5M wines, 13M reviews, drinkability curves
- **Reason**: No public API, ToS prohibits commercial use, SiteBlackBox anti-bot. Considered competition.
- **URL**: https://www.cellartracker.com/

### NASA POWER
- **Data**: Solar radiation, temperature, precipitation (global)
- **Reason**: Open-Meteo preferred (better resolution, built-in GDD, cleaner API)
- **URL**: https://power.larc.nasa.gov/

### Wine Folly GWDB
- **Data**: Producer-verified wine data, API available
- **Reason**: Semi-open / contract required
- **URL**: https://winefolly.com/

### RAW Wine
- **Data**: 18K natural wines
- **Reason**: Semi-open / likely requires partnership
- **URL**: https://rawwine.com/

---

## Notes

### Accuracy Principle
Loam prioritizes 100% accuracy over volume. We would rather have less data than more data with errors. All scraping uses deterministic CSS-selector parsing, never LLM interpretation. Data is validated against known types and ranges before insertion.

### LLM Scraping
Evaluated but currently rejected. ~98% accuracy is insufficient. Will revisit if we find a way to achieve 100% accuracy (e.g., LLM extraction + deterministic validation + human review queue).

### COLA Cloud
Commercial wrapper around TTB COLA with AI enrichment (4.6M label images, 470K barcodes). Enterprise pricing. Evaluate cost vs. building our own TTB extraction pipeline.
