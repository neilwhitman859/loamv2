# LWIN Strategy & Wine Identity Research

*Session date: 2026-03-13*

---

## Decision: LWIN as Identity Backbone

LWIN (Liv-ex Wine Identification Number) will serve as the canonical external wine identifier for Loam. It is the wine industry's closest equivalent to an ISBN.

- **License:** CC BY 4.0 — free, open, commercial use, attribution only
- **Coverage:** 211K records (187K wines after filtering spirits/beer), 37K producers
- **Structure:** LWIN-7 (wine) → LWIN-11 (+vintage) → LWIN-16 (+bottle size) → LWIN-18 (+pack)
- **Adoption:** Liv-ex (600+ merchants), Wine-Searcher (integrated 2022)
- **Download:** Form submission at liv-ex.com/lwin/, delivers Excel file

The LWIN-7 maps to `wines`, LWIN-11 maps to `wine_vintages`. Clean hierarchy, no schema contortion needed.

---

## LWIN Database Contents

**File:** `C:\Users\neilw\Downloads\LWINdatabase.xlsx` (211,168 rows)

### Columns
| Column | Description | Example |
|--------|------------|---------|
| LWIN | 7-digit identifier | 1123164 |
| STATUS | Live / Combined / Deleted | Live |
| DISPLAY_NAME | Full formatted name | Ridge, Monte Bello, Santa Cruz Mountains |
| PRODUCER_TITLE | Usually "NA" | NA |
| PRODUCER_NAME | Producer only | Ridge |
| WINE | Wine/cuvée name | Monte Bello |
| COUNTRY | Country | United States |
| REGION | Wine region | California |
| SUB_REGION | Sub-region/appellation | Santa Cruz Mountains |
| SITE | Vineyard site | (mostly NA) |
| PARCEL | Parcel within site | (mostly NA) |
| COLOUR | Red/White/Rose/NA/Mixed | Red |
| TYPE | Wine/Spirit/Fortified Wine/etc. | Wine |
| SUB_TYPE | Still/Sparkling/Port/etc. | Still |
| DESIGNATION | Quality tier on wine | AVA |
| CLASSIFICATION | Grand Cru/Premier Cru/etc. | (mostly NA) |
| VINTAGE_CONFIG | sequential/nonSequential/singleVintageOnly | sequential |
| FIRST_VINTAGE / FINAL_VINTAGE | Vintage range bounds | (often NA) |
| DATE_ADDED / DATE_UPDATED | Excel date serial | |
| REFERENCE | (mostly empty) | |

### Key Distributions
- **STATUS:** 204K Live, 6K Combined, 500 Deleted
- **TYPE:** 187K Wine, 20K Spirit, 3K Fortified
- **COLOUR:** 102K Red, 75K White, 9K Rosé
- **DESIGNATION:** AOP 62K, AVA 29K, DOC 14K, GI 12K, DO 10K, DOCG 7K
- **Producers:** 37,369 unique
- **Regions:** 370 unique values

### Existing Loam Producer Coverage
| Producer | LWIN wines | Loam wines |
|----------|-----------|------------|
| Ridge | 72 | 182 |
| Stag's Leap Wine Cellars | 35 | 29 |
| Tablas Creek | 41 | 56 |

---

## What LWIN Does NOT Have
- **No grape/varietal data** — zero. This is the biggest gap.
- **No vintage-level records** in this download — LWIN-7 only
- **No scores, prices, or tasting notes**
- **German sub-regions:** 98.3% are "NA" (Loam has 1,192 Einzellagen with official boundaries)
- **No winemaking details** (oak, fermentation, etc.)

---

## Region/Sub-Region Mapping: LWIN vs Loam

### Structural Difference

| Level | LWIN | Loam |
|-------|------|------|
| 1 | COUNTRY | countries |
| 2 | REGION | regions (L1) |
| 3 | SUB_REGION | regions (L2) OR appellations |
| Wine-level | DESIGNATION (AOP/AVA/DOC) | — |
| Appellation-level | — | designation_type |

**Key insight:** LWIN SUB_REGION usually maps to a Loam **appellation**, not a Loam L2 region. LWIN DESIGNATION is wine-level; Loam designation_type is appellation-level.

### Name Mismatches (LWIN REGION → Loam Region)

| LWIN REGION | Loam Region | Issue | Wines |
|---|---|---|---|
| Rhone | Rhône Valley | accent + "Valley" | 5,239 |
| Loire | Loire Valley | "Valley" | 3,506 |
| Sud Ouest | Southwest France | different name | 976 |
| Piedmont | Piemonte | English vs Italian | 5,514 |
| Emilia Romagna | Emilia-Romagna | hyphen | 352 |
| Friuli Venezia Giulia | Friuli-Venezia Giulia | hyphens | 1,079 |
| Trentino Alto Adige | Trentino-Alto Adige | hyphen | 1,180 |
| Niederosterreich | Niederösterreich | umlaut | 1,928 |
| Wurttemberg | Württemberg | umlauts | 194 |
| Hessische Bergstrasse | Hessische Bergstraße | ß | 34 |
| Castilla La Mancha | Castilla-La Mancha | hyphen | 502 |
| Castilla y Leon | Castilla y León | accent | 1,849 |
| Andalucia | Andalucía | accent | 725 |
| Aconcagua | Aconcagua Region | suffix | 572 |
| Central Valley | Central Valley Region | suffix | 2,507 |
| Coquimbo | Coquimbo Region | suffix | 210 |
| Sur | Southern Region | different name | 248 |

### Structural Mismatches

| LWIN REGION | Loam Structure | Issue | Wines |
|---|---|---|---|
| Languedoc | Southern France → Languedoc-Roussillon (L2) | LWIN=L1, Loam=L2 | 1,853 |
| Roussillon | Southern France → Languedoc-Roussillon (L2) | same L2 | 599 |
| Provence | Southern France → Provence (L2) | LWIN=L1, Loam=L2 | 1,153 |
| Pays d'Oc | Southern France | LWIN=region, Loam=IGP zone | 883 |
| Cava | Spain catch-all | LWIN=region, Loam=appellation | 352 |
| Prosecco | (varies) | LWIN=region | 602 |
| Walla Walla Valley | Washington | LWIN=region, should be appellation | 815 |
| Marlborough | South Island → Marlborough (L2) | NZ island structure | 1,440 |
| Central Otago | South Island → Central Otago (L2) | NZ island structure | 572 |
| Hawke's Bay | North Island → Hawke's Bay (L2) | NZ island structure | 799 |

### IGP-level Zones (no Loam region equivalent)
Pays d'Oc, Mediterranee, Vaucluse, Gard, Cevennes, Cotes Catalanes, Comtes Rhodaniens, Delle Venezie, Vigneti delle Dolomiti, Trevenezie — map to parent region or country catch-all.

### Germany — Skip Sub-Region Mapping
98.3% of German wines in LWIN have SUB_REGION = "NA". Loam's German data (1,192 Einzellagen with official boundaries) is far richer. LWIN adds region-level identity only for German wines.

### Recommended Approach
Build a static mapping file `data/lwin_region_mapping.json` (~100 entries). For SUB_REGION → appellation: fuzzy name match within resolved country. Expected 85%+ auto-match for major countries. Unmapped wines get region only (appellation_id NULL).

---

## Three-Layer Data Strategy

**User decision: No Vivino, Wine-Searcher, or CellarTracker data.** All sources must be first-hand or regulatory.

### Layer 1: LWIN — Identity Backbone
- Industry-standard wine ID (who/what/where)
- 187K wines, 37K producers
- CC BY 4.0, free forever
- Fine wine bias (Bordeaux, Burgundy, Napa heavy)

### Layer 2: Government Registries — Catalog Completeness
Fills the everyday wine gap that LWIN's fine wine bias leaves:

- **TTB COLA (US)** — every wine label approved for US sale. Hundreds of thousands of labels. Public, bulk downloadable. Has producer, brand, appellation, grape variety, vintage, alcohol %. Most actionable source right now.
- **EU e-label registries** — mandatory since Dec 2023 for 2024+ harvest wines. Structured data (ingredients, nutrition, allergens). Emerging ecosystem, will become the most complete European wine database.
- **INAO (France)** — official AOC producer/production registry. Parcel-level.
- **Wine Australia** — export approval database
- **INV (Argentina)** — producer and production tracking (already used for appellation data)
- **SAG (Chile)** — DO registry

### Layer 3: Producer Direct — Enrichment Depth
The Loam value-add that no database provides:
- Winemaking details (oak, fermentation, aging)
- Tasting notes and vintage commentary
- Vineyard specifics (soil, elevation, aspect)
- Terroir narrative and AI synthesis
- Current scraper pipeline (Ridge, Tablas Creek, Stag's Leap) is the template

**Summary:** LWIN handles identity. Government sources handle breadth. Producer websites handle depth. All authoritative, no crowdsourced platforms.

---

## Import Approach (High-Level)

User wants wines imported directly into canonical `wines` table (not a staging table), with enrichment tracking to distinguish sparse LWIN skeletons from fully enriched wines.

### Schema implications
- Add `lwin` column to `wines` (unique, nullable)
- Add `enrichment_level` column to track data completeness
- Need "Unclassified" varietal_category placeholder (LWIN has no grape data; varietal_category_id is NOT NULL)
- Need `lwin_producer_name` on producers for matching
- Bulk-create ~37K producer records
- Build region mapping (static JSON file)
- Appellation matching (fuzzy name match, ~85% expected)

### What we don't import
- No wine_vintages (LWIN-7 download has no vintage-level data)
- No wine_grapes (no grape data in LWIN)
- No scores or prices

### Existing wine merge
The 267 existing wines (Ridge, Tablas Creek, Stag's Leap) need LWIN codes assigned without duplication. Import must be additive.

### Open questions for schema session
1. Enrichment tracking design — column enum vs separate table?
2. Producer table readiness for 37K records — need aliases? parent company? producer_type?
3. Varietal classification pipeline — Haiku inference from wine names? Manual? Batch or on-demand?
4. Frontend visibility — show all 187K wines or only enriched ones?
5. Sequencing — schema changes first, then LWIN import, or interleaved?

---

## Key Files
- `C:\Users\neilw\Downloads\LWINdatabase.xlsx` — the LWIN database download
- `C:\Users\neilw\.claude\plans\synchronous-wiggling-bonbon.md` — detailed import plan (pre-strategic discussion)
