# Loam v2 — Historical Detail

Moved from CLAUDE.md on 2026-04-03 to keep the main context file lean. This is reference material — consult when you need to understand how something was built, not for every session.

---

## Schema Hardening (2026-03-14) — Phase 1a

24 new tables created + ~45 columns added to 10 existing tables. All scrape data cleared (wines, vintages, scores, producers) — starting fresh.

New tables (Phase 1a): entity_attributes, external_ids, wine_appellations, grape_synonyms, classifications, classification_levels, entity_classifications, appellation_grapes, varietal_category_grapes, producer_farming_certifications, producer_biodiversity_certifications, vineyards, vineyard_producers, vineyard_soils, wine_vintage_tasting_insights, wine_vintage_nv_components, tasting_descriptors, wine_vintage_descriptors, importers, producer_importers. (attribute_definitions pre-existed.)
New tables (Phase 1b): label_designations, label_designation_rules, wine_label_designations, region_grapes, country_grapes.

Key deviations from original spec: vineyards got region_id + country_id + CHECK constraint; wine_vintage_components renamed to wine_vintage_nv_components. appellation_grapes `is_required` boolean replaced with `association_type` text ('required'/'typical') — same column added to region_grapes and country_grapes.

**Post-KL-import refinements (2026-03-15):** 5 schema changes from bulk import stress test:
- `wines.varietal_category_id` made nullable (no external source provides varietal categories natively)
- `producer_farming_certifications.certification_status` added (certified/practicing/transitioning)
- `producers.latitude/longitude` added (GPS coords from grower profiles)
- `wines.vinification_notes` added (free text winemaking approach)
- `appellation_aliases` table created and seeded with 18,631 aliases from 4+ sources (originally 17,558, expanded during LWIN import):
  - INAO OpenDataSoft API: 2,557 official French AOC product variants (color, style, cru)
  - Mechanical color suffixes: 9,866 (FR/IT/ES/PT/DE/US/AU/NZ/ZA/CL/AR)
  - Mechanical designation suffixes: 3,193 (appellation + AOC/DOC/DOCG/etc.)
  - Slash-form variants + informal/industry aliases + translations: 1,942
  - Script: `scripts/seed_appellation_aliases.mjs`
  - KL appellation resolution improved: 10.8% → 67.0% (983/1,468 wines)

**Schema sharpening (2026-03-15):** 8 data integrity and normalization fixes:
- CHECK constraints added: wines.color (red/white/rose/orange), wines.wine_type, wines.effervescence, producers.producer_type
- Color standardized: 'rosé' → 'rose' (ASCII, matches varietal_categories)
- Dropped from wine_vintages: vivino_id, wine_searcher_id, cellartracker_id (use external_ids table), alcohol_pct, alcohol_level (redundant with abv)
- Dropped from wines: oak_origin, yeast_type, fining, filtration, closure, fermentation_vessel + _source columns (winemaking lives on wine_vintages only; wines.vinification_notes for defaults), vineyard_id, vineyard_name (use wine_vineyards table), latitude, longitude (wines get geography from appellation/vineyard)
- Scores dedup index: UNIQUE on (wine_id, vintage_year, publication_id, critic, review_date) with COALESCE for nulls

---

## Reference Data Progress (Phase 1b, 2026-03-14)

**Classifications:** 13 systems, 32 levels. Audited by two independent wine expert passes. France: Bordeaux 1855 Médoc (5), Sauternes (3), Saint-Émilion (3), Graves (1), Burgundy Vineyard (2), Alsace Grand Cru (1), Champagne Cru (2), Cru Bourgeois (3), Cru Artisan (1), Provence Cru Classé (1). Germany: VDP (4). Austria: ÖTW Erste Lagen (2). Australia: Langton's (4). Systems: 11 government, 2 industry.

**Label designations:** 98 designations across 14 categories, 200 rules. Audited by two independent passes. Categories: aging_tier (15), sweetness_style (17), sparkling_type (14), pradikat_tier (12), production_method (10), estate_bottling (7), vineyard_age (6), late_harvest (4), botrytis_sweet (3), ice_wine (3), vineyard_designation (3), early_release (2), quality_tier (1), geographic_qualifier (1). Key rule sets:
- Italian Superiore: 31 rules (DOC/DOCG ABV/yield thresholds)
- Italian Riserva: 23 rules (22 DOCGs + 1 DOC aging requirements)
- German Prädikats: 78 rules (13 Anbaugebiete × 6 levels, Zone A/B Oechsle minimums)
- Portuguese Reserva/Grande Reserva: 14 rules (ABV thresholds by DOC)
- Spanish aging tiers: national defaults + Rioja/Ribera del Duero/Navarra deviations
- Austrian Prädikats: 8 rules (KMW minimums from Weingesetz 2009)
- EU sparkling sweetness: 7 rules (g/L RS from EU Reg 2019/33)

**Grapes:** VIVC import complete — 9,690 grapes imported from VIVC cache, 34,833 synonyms, parentage resolved (~3,000+ grapes with parent links). Three-tier display name strategy: 26 Tier 1 overrides (Merlot, Malbec, Grenache, etc.), 154 Tier 2 family-preserved (Pinot Noir, Cabernet Sauvignon), 9,510 Tier 3 auto. Country-specific synonyms added (Zinfandel/US, Primitivo/IT, Garnacha/ES, Monastrell/ES, Alvarinho/PT, Gouveio/PT). `display_name` column added to grapes table.

**Publications:** 78 publications rebuilt from authoritative sources. Scoring systems, scale ranges, active status. Types: critic_publication, community, auction_house, competition, aggregator. Two-pass audit applied.

**Attribute definitions:** 73 definitions across 6 categories (chemistry 8, winemaking 23, viticulture 13, production 15, service 6, business 8).

**Tasting descriptors:** 304 descriptors in 3-tier hierarchy. Sources: WSET SAT (primary), UC Davis Wine Aroma Wheel, CMS Deductive Tasting Grid. 12 top-level categories → ~35 subcategories → ~257 leaf descriptors.

**Appellation grapes:** 9,278 rows across all 3,206 appellations (100% coverage). Regulated varieties for EU appellations, key planted varieties for non-EU. Notable gaps: Blaufränkisch not in grapes table, Hondarrabi Zuri missing, Tintilia missing.

**Region grapes:** 1,673 rows across all 324 named regions (100% coverage). Seeded from Anderson & Aryal dataset + authoritative sources. Two-pass expert audit completed with cross-table validation.

**Country grapes:** 541 rows across all 68 countries (100% coverage). Seeded from Anderson & Aryal dataset. 18 country-level audit fixes applied.

**Soil types:** 39 soil types with drainage_rate, heat_retention, water_holding_capacity, geological_origin.

---

## Trial Imports (Phase 1c, 2026-03-15) — CLEARED during LWIN promotion

6 producers across 4 countries:
- Fort Ross Vineyard (US/Sonoma, estate): 15 wines, 112 vintages, 84 scores
- Sea Slopes (US/Sonoma, child of Fort Ross): 2 wines, 24 vintages, 15 scores
- Moone Tsai (US/Napa, negociant): 10 wines, 83 vintages, 48 scores
- Lopez de Heredia (Spain/Rioja, estate): 9 wines, 115 vintages, 67 scores
- Marchesi Antinori (Italy/Tuscany, estate): 23 wines, 76 vintages, 98 scores
- Louis Jadot (France/Burgundy, negociant): 44 wines, 149 vintages, 209 scores, 40 classifications

Wine-type stress test (10 producers, 2026-03-16): Louis Roederer, Donnhoff, Chateau d'Yquem, Taylor's Port, Penfolds, Royal Tokaji, Felton Road, Catena Zapata, Chateau Miraval, Vega Sicilia.

Global coverage stress test (5 producers, 2026-03-16): Kanonkop, Chateau Musar, Pheasant's Tears, Blandy's, Billecart-Salmon.

Kermit Lynch bulk import: 193 producers, 1,467 wines. Drove creation of appellation_aliases table. Shopify retailer imports: Last Bottle (234 wines), Best Wine Store (752), Domestique (245).

---

## Import Architecture Details

Import library: `pipeline/lib/importer.py` (converted from `lib/import.mjs`) + `data/imports/{slug}.json` per-producer data. Key features:
- `--replace` mode, `parseDate()`, classification linkage, vineyard sourcing
- Pradikat auto-detection, grape aliases, publication aliases
- NV convention (vintage_year=0), wine_type normalization ('still' → 'table')
- Score validation (100-point, 20-point, 5-point scales)
- Pre-import validation (`--validate` flag)
- Accent-tolerant resolution via `normalize()`
- Region aliases (in-code): ~75 entries mapping English/alternative names

---

## Metadata Promotion (2026-03-16)

Phase 1: 849 fields moved from JSONB metadata to proper columns (vinification notes, release dates, first vintage years, production cases, VDP classifications). 616 HTML entities cleaned.

Phase 2: 4,611 fields promoted (soil_description, vine_age_description, vineyard_area_ha, commune, altitude, aspect, slope_pct, monopole, producers.address). Remaining in metadata: classification (28 unmapped Italian DOC/DOCG), cooperage (~80), vineyard_sources (~79).

---

## Migrations Applied (2026-03-16)

- 3 alias tables: region_aliases, producer_aliases, label_designation_aliases
- 3 Lebanon regions: Bekaa Valley, Mount Lebanon, Batroun
- 16 new label designations (Nykteri, Colheita, En Rama, etc.) Total: 115.
- 9 new wine columns: soil_description, vine_age_description, vineyard_area_ha, commune, altitude_m_low/high, aspect, slope_pct, monopole
- 1 new producer column: address

Drinking Window Schema Fix: critic_drink_window → critic_drinking_window, typical_drinking_window split to min/max, peak_drinking_window added.

Pre-Import Schema Expansion (2026-03-17): 11 new columns including wines.barcode, wine_vintage_scores.medal, EU e-label columns (ingredients, allergens, energy_kcal, nutrition_data), maceration_technique, aging_vessel_size_l, maturity_status.

---

## Schema Post-Import Hardening (2026-03-15)

- Metadata → columns: release_date, first_vintage_year, style, philosophy promoted
- Enrichment log rebuilt with model/cost tracking, prompt versioning, field-level changes
- Appellation rules table with flexible JSONB rules column
- updated_at triggers on all 36 tables
- Orphan validation function for polymorphic FKs
- Soft delete consistency audited (15 entity tables)

## Schema Scan & Hardening Round 2 (2026-03-15)

29 issues identified and triaged. Key changes:
- retailers table created, wines.country_id nullable, effervescence DEFAULT 'still'
- wine_vintage_scores/prices.wine_vintage_id FK added (backfilled)
- score_provenance, compare_at_price_usd, grapes.name_normalized added
- enrichment_log status CHECK expanded
- grape_plantings documented, wine_regions/producer_regions noted for future

---

## Data Acquisition Research (2026-03-16) — COMPLETE

Comprehensive research across 17 source categories. See `docs/SOURCES.md` for unified reference.

Key findings: LWIN (186K wines, fine wine bias), COLA Cloud ($39/mo, ~1.2M COLAs), 50-state survey (PRO Platform #1 discovery — 12 states, 1.56M brand registrations), 22 importers researched, Vinmonopolet (richest structured data globally), competition databases (IWSC, Berliner, DWWA, TEXSOM), Wine APIs (VineRadar, db.wine), EU e-labels (500K+ wines), certification databases, international retailers, auction/trading.

Import priority: PRO Platform → TTB COLA direct → State DBs → Importers → UPC sources → COLA Cloud → Retailer sitemaps.

---

## TTB COLA Direct Scraping (2026-03-17 to 2026-03-27) — SCRAPE COMPLETE

Discovered TTB has **structured grape varietal data as a native field**. Chrome inject architecture (Python HTTP server + JS in browser Console) bypasses Shape Security WAF.

**Phase 1 (CSV harvest):** 3,283,319 records loaded. Basic metadata.

**Phase 2a (detail + printable):** Detail scraper (18.25h, ~30 rec/s): 2M records. Printable scraper (24.3h, ~20 rec/s): 1.55M records. Two form versions (old pre-2013, new post-2013). Zero WAF blocks.

**Phase 2b (gap fill + re-scrape):** Detail: 3,178,691 / 3,283,319 (96.8%). Printable: 1,824,749 / 1,827,384 001-format (99.86%). Image URL separation: `application_scan_urls` (detail) vs `label_image_urls` (printable).

**Overnight lessons:** Parallel scrapers cause silent rate limiting. Chrome background tab throttling collapses setTimeout. Use MessageChannel delay, sequential runs, concurrency 20.

**TTB ID format:** 001 = standard COLA (1.83M, full data), 000/002/003 = different forms (1.35M combined, detail only, no printable). Old short IDs: pre-2003, ~670K.

Scripts: `pipeline/fetch/ttb_chrome_scraper.py` + `ttb_chrome_inject.js`, `pipeline/fetch/ttb_printable_scraper.py` + `ttb_printable_inject.js`

---

## 50-State UPC/COLA Survey (2026-03-18) — COMPLETE

**PRO Platform:** 12 US states, identical API, XLSX export (no auth needed). 1.56M brand registrations, 99% with COLA. States: AR, CO, IL, KY, LA, MN, NM, NY, OH, OK, SC, SD. ~270MB total.

**Texas TABC:** 201K wines via Socrata. 100% TTB numbers, 99.8% ABV. Pre-Sept 2021.

**West Virginia ABCA:** 55K wines via REST API (now dead). Public API key in history.

**UPC sources fetched:** Open Food Facts (5,176), Horizon Beverage (6,441, now dead), PA PLCB (5,905), LCBO (3,513), WineDeals (3,200).

**Dead ends (28 states):** Spirits-only, no wine control, fortified only, licensee-restricted, or no COLA/UPC in output.

**CT DCP:** No UPC/COLA but valuable for wholesale pricing. **NJ POSSE:** OPRA request needed.

---

## Additional Source Fetchers (2026-03-21)

| Source | Wines | UPC | Script |
|--------|-------|-----|--------|
| Spec's Wine | 21,913 | 21,912 (100%) | `pipeline/fetch/specs.py` |
| Berliner Wine Trophy | 73,899 | — | `pipeline/fetch/berliner_wine_trophy.py` |
| TEXSOM | 46,896 | — | `pipeline/fetch/texsom.py` |
| Wally's Wine | 19,446 | — | `pipeline/fetch/wallys.py` |
| EnofileOnline | 9,166 | — | `pipeline/fetch/enofileonline.py` |
| Flatiron Wines | 4,130 | — | `pipeline/fetch/shopify.py` |
| BC Liquor | 3,300 | 3,270 (99.5%) | `pipeline/fetch/bc_liquor.py` |

Key: Spec's is the best UPC source. Berliner is massive (73.9K, 42 competitions). TEXSOM has 40 years.

---

## Source Audit (2026-03-23/24)

- **Dead APIs:** WV ABCA, Horizon Beverage
- **Access changed:** Systembolaget (auth required), Spec's WooCommerce (404), Skurnik (TLS issues)
- **Stale:** TABC +18K, Open Food Facts +11K
- **Healthy:** PRO Platform, Kansas, LCBO, BC Liquor, all 5 importers

---

## TTB Label Image to Barcode Pipeline (2026-03-23/24)

**Image download:** 490,373 images (~21GB) from 350,939 records with label_image_urls. Two URL patterns: publicViewImage (231K), publicViewAttachment (120K). Pre-2007 images mostly tiny thumbnails.

**Barcode detection:** zxing-cpp. Test scan of 3,407 images: 619 labels with UPC/EAN (18.2%), 516 unique barcodes. Full 490K scan running separately.

**Printable page images:** 1.82M label_image_urls from printable (different from 1.34M application_scan_urls from detail). Plan: Supabase Storage for canonical wines ($4-8/mo).

---

## Importer Promotion Results (2026-03-18)

- KL: 1,468 wines → 830 new, 638 matched
- Skurnik: 5,541 → 2,605 new, 2,912 matched
- Winebow: 536 → 340 new, 59 matched, 137 skipped
- Empson: 279 → 178 new, 96 matched
- EC: 443 → 324 new, 71 matched

Polaner deprioritized (metadata-thin, 1,680 titles parsed via Haiku).

---

## Content Table History (2026-04-02 to 2026-04-03)

**Enrichment promotion:** 3,575 wine_grapes, 1,005 scores, 1,238 wines updated with soil/vinification/vine_age from 5 importers. 866 grape names unresolved.

**Sparkling wine fix:** 8,977 wines reclassified via keyword detection + sparkling-only appellations.

**Sonnet accuracy audit:** 300 random wines, 96% accuracy, $0.05. Non-sparkling data 100% clean.

**COLA bridge merge:** Reverse-bridged 2,701 producer IDs PRO→TTB. +9,708 vintages, +4,357 ABVs. 2 COLA collisions found (IDs can be reused).

**Importer producer re-linking:** 3-pass matching across 5 importers. Normalized matching added 1,187 links.

**Importer AI wine matching:** Sonnet-assisted, 3,837/4,157 matched (93%), $1.93. 850 new wines created from unmatched.

**Tier B+C promotion:**
- Tier B: 233,548 TTB records linked to 59,546 canonical wines (4 strategies)
- Tier C1: 80K new wines from TTB + resolved appellation + grapes
- Tier C2: +104,628 new wines, +43 appellation aliases
- TTB producer re-linking: 839 brands, +64,067 TTB records

**Phase B wine creation:** 30,493 new wines from 4,430 producers. TTB wine linking v2: 284K wines linked to 666K TTB records. COLA depth: +123K vintages, +169K COLA IDs. Grape promotion: +3,656 links.

**Competition linking:** Berliner 3.6K scores, TEXSOM 13.3K scores.

**Retailer linking:** Spec's 4,807 wines (3,095 UPC + 2,796 prices), Wally's 6,822 (6,217 prices), Flatiron 761, LCBO 1,360 (464 UPC), Systembolaget 1,222, BC Liquor 276.

**Region refinement:** 75,774 wines updated L1→L2. 3,224 cross-boundary mismatches remain.

**Search fix:** search_catalog v2 with unaccent + producer name matching. Findability 12%→83%.

---

## Closed: architecture changed (2026-04-11, Session 14 Phase A)

### Grade C enrichment quality fix (three-layer redesign) — deprecated
Originally a P0 backlog item (the Session 10 audit found Grade C averaging 2.48/5 with 111+ factual_error tags). Deprecated on 2026-04-11 when the project pivoted to Reference-First enrichment. Under the new architecture, wine pages become thin synthesis over an enriched reference layer (grape + region + appellation + producer insights). Wine-level voice regressions no longer apply the same way, so the old three-layer L1+L3 redesign (which Session 12 actually built and validated for Grade B — see `data/sprints/30k/journal.md` Session 12 entry) is paused for Grade C and will be revisited (or replaced) inside the Sprint 2 vertical slice.

### 270 thin Grade C wines with <3 of 5 canonical facts — deprecated
Originally a P1 backlog item. Same reason: wine-level Grade C is being replaced by Reference-First synthesis. A thin wine (no grape, no appellation, no vintage, no score, no price) can still render a useful page if the reference layer is rich — because the page pulls context from grape/appellation insights rather than trying to generate original wine-level copy. Grade-level re-classification will happen inside the RF sprint.

### Session 10 S11.6 misclassification (false positive) — closed
Session 10's S11.6 check reported 2,272 duplicate wines via `GROUP BY name_normalized`. That grouping inflated the count because mass-market wines with NULL `name` were being lumped together. Session 14 Phase A fix #4 added `validate_post_dedup()` to `pipeline/analyze/thirty_k_validate.py` with the corrected grouping `(producer_id, display_name, appellation_id)`. New count: **0 real duplicate groups**. The old broken grouping remains as an informational-only warn line for reference.

---

## Pre-30K rebuild history (moved from CLAUDE.md "Content Tables" on 2026-04-11, Session 14 Phase A W4)

These bullets describe the pre-rebuild dataset (~477K wines, ~42K producers) before Phase 0 of the 30K plan archived everything and rebuilt a clean corpus. They were living in CLAUDE.md and going stale every week. They're preserved here for auditing the old pipelines but **do not reflect current DB state** — always query the DB for live numbers.

The full session-by-session narrative for the 30K rebuild itself lives in `data/sprints/30k/journal.md` (and, post-closure, `data/sprints/_archive/30k/journal.md`).

### Content Tables as of 2026-04-04 (post-recovery + 10 follow-ups, pre-rebuild)

- **~42K producers**, **~518K wines**, **~354K vintages**, **~27K scores**, **~140K prices**, **~293K wine_grapes**, **~614K external_ids** (294K COLA + 106K UPC + 189K LWIN + 13K QR URL + 1.4K QR), **~16K entity_classifications**, **14 retailers**, **194 winemakers**
- **~432K wines with color (83.5%)** via free fills + Haiku color classify. Scripts: `pipeline/enrich/haiku_color_classify.py`, `pipeline/promote/ttb_color_fill.py`
- **~262K wines with appellation_id** via TTB wine_appellation backfill
- **~413K wines with region_id** via TTB direct resolve + cascade from appellation
- **~468K wines with country_id**
- **~167K wine_vintages with label image URLs** (restored from TTB after column was wiped)
- **~169K wine_vintages with ABV**
- **293K wines linked to TTB** (689K TTB records linked)
- **Wine type:** table 473,232, sparkling 18,757, fortified 4,937
- **COLA-keyed state merge:** 170K state DB records linked (PRO 84K, TABC 52K, WV 22K, Kansas 13K)
- **Price coverage:** 8.39% (41,187 distinct wines with prices out of 490,933 wines)
- **Data grade:** F=467,355, D=29,568, C=0, B=3
- **Score coverage:** ~2%
- **UPCs:** 117,250 across 80,618 wines (TTB label/scan barcode scanning)

### Promotion rounds 1-17 (2026-04-04 to 2026-04-05, all pre-rebuild)

Each round applied strictly definitional / direct-source fills only (no inference). Highlights:

- **Round 1 — Recovery (2026-04-04):** +7,707 prices (Wally's title parser), +29,249 wine_grapes (TTB grape promotion re-run), +86,015 colors (LWIN `colour` column backfill + 1,681 from importers). See DECISIONS.md "Recovery of lost data via authoritative sources only."
- **Round 2 — TTB label images + ABV (2026-04-04):** +163,635 label images from TTB (0 to 167,164), +2,670 ABV values, +32,135 appellation_id values from TTB wine_appellation, +4,166 sparkling and +223 fortified reclassifications from TTB class_type_desc.
- **Round 3 — Region/country cascade (2026-04-04):** +125,844 region_id values (26,441 cascaded from appellation + 99,403 resolved directly from TTB wine_appellation + origin_desc), +323 fortified reclassifications from name-based Port/Sherry/Madeira/Marsala/Banyuls/Maury match, +1,966 wine country_id + 188 producer country_id cascaded.
- **Round 4 — TTB grape parsing (2026-04-05):** +3,542 wine_grapes via greedy longest-match parser on TTB space-separated blends. +6 TA from Winebow chemistry.
- **Round 5 — Fortified age statements (2026-04-05):** +95 age_statement_years on fortified wines parsed from name ("10 Year Tawny Port" to 10).
- **Round 6 — LWIN backfill + search vector rebuild (2026-04-05):** +898 LWIN external_ids backfilled. Search vector rebuild: +2,574 wines, +6,859 producers, +1 appellation.
- **Round 7 — COLA + varietal_category cascade (2026-04-05):** +2,160 COLA external_ids backfilled. +62,519 wines.varietal_category_id via strictly definitional single-grape cascade (0 to 62,519).
- **Round 8 — identity_confidence cascade (2026-04-05):** +435,050 identity_confidence values cascaded from external_ids (LWIN > COLA > UPC precedence). +3,184 fortified varietal_category_id via name match.
- **Round 9 — Single-grape percentage fill (2026-04-05):** +79,189 wine_grapes.percentage=100 on single-grape wines. +9,002 wine_vintage_prices.price_original copied on USD-currency rows.
- **Round 10 — State DB ABV fill (2026-04-05):** +536 from PRO Platform, +147 from WV ABCA (both vintage-matched definitionally). TABC skipped (no vintage column).
- **Round 11 — Label designations by name match (2026-04-05):** +53,128 wine_label_designations (0 to 53,128). Word-boundary match of 93 canonical designation names against wines.name.
- **Round 12 — Farming cert gap fills (2026-04-05):** +123 wine_farming_certifications from Skurnik + Kermit Lynch. +78 producer_farming_certifications from KL growers.
- **Round 13 — Empson first_vintage + appellation to varietal (2026-04-05):** +76 first_vintage_year from Empson's explicit column. +1,803 varietal_category_id from strict appellation-name == category-name match (Franciacorta 467, Sauternes 389, Prosecco 359, etc.).
- **Round 14 — Wally's bottle formats + appellation data (2026-04-05):** +15,940 wine_vintage_formats from Wally's title size parsing. appellation_vintages populated (134,877 rows from weather data). appellation_soils: 930 links.
- **Round 15 — TEXSOM review dates (2026-04-05):** +5,452 review_date on TEXSOM scores via vintage-matched backfill from source_texsom.year.
- **Round 16 — Producer external_ids (2026-04-05):** +538 producer external_ids (Skurnik 379 slugs, Kermit Lynch 120 grower IDs, Empson 39 slugs). First producer entity_type entries in external_ids.
- **Round 17 — NV score sync (2026-04-05):** +1,025 wine_vintage_scores.vintage_year set to 0 (NV) on rows where `wine_vintage_id` FK already pointed to a wine_vintages(vintage_year=0) row. Orphan score count went to 0.

### Path A appellation_rules seeding (2026-04-05 to 2026-04-07)

Seeded **1,165 appellation_rules** and **10,413 appellation_grapes** with full legal-document provenance. Sources: INAO CDCs (French AOCs), MAPA pliegos (Spanish DOPs), MASAF catalogoviti sweep (398 Italian DOC/DOCG PDFs), IVV Portugal, BML Austria via Bundeskellereiinspektion, BLE Germany, EU eAmbrosia/OJ C, Hungarian Wine Act, Greek PDO register, Georgian National Wine Agency, Slovenian MoA, Romanian ONVPV, Swiss federal + cantonal. Countries covered: US, Italy, France, Austria, Portugal, Spain, Germany, Greece, Australia, South Africa, Hungary, Switzerland, Georgia, Slovenia, Romania, North Macedonia, Moldova, Czech Republic.

Rollback SQL per batch lives in `docs/PATH_A_ROLLBACK.md`. Pre-existing illegal wine colors (~895 wines — 800 Champagne red, 50 Chablis red, 9 Barolo rosé, etc.) catalogued during this work and left in place per no-overwrite rule — queued for Session 14 Phase B bug fix #5.

### Producer website scrape (2026-04-07)

`pipeline/fetch/producer_site_scrape.py` — generic Haiku-based extraction from top 100 wine producer websites. 77/100 completed initially, +7 via Playwright fallback in v2. **+522 wines, +356 vintages (80 ABV, 36 pH, 36 TA, 135 oak, 252 winemaker notes), +560 wine_grapes, +28 winemakers, +55 year_established, +71 producer descriptions, +113 website_urls on producers.** Cost: ~$1.84 Haiku total (v1 + v2).

### Accent cleanup (2026-04-07)

`pipeline/analyze/accent_cleanup.py` — deterministic accent restoration, 28 rules covering French/Spanish/Portuguese/German patterns. **~23,118 names fixed** across producers (4,772) and wines (18,346). Zero AI cost. 9,630 slug collisions revealed accent-variant duplicate entries, logged for dedup.

### Knowledge seed (2026-04-08)

`pipeline/promote/knowledge_seed.py` — 6-stage pipeline (generate → dedup → ttb-match → validate → promote → report). 920 generated across 32 categories, 656 already existed (71% overlap), 204 promoted after dual Haiku+Sonnet validation. **+200 wines, +31 producers, +321 wine_grapes.** Backbone IDs: 9/200 (COLA only). Total cost: ~$2.50. Staging table: `source_claude_knowledge`.

### Haiku data gap fills loop (2026-04-07)

- **Free fills:** +15,653 white-grape to white cascade, +11,171 name-keyword colors, +134,877 appellation_vintages from weather
- **Track A color classify:** ~$22 (4 parallel runs, coverage 62.4% to 83.5%, +109K wines with color)
- **Track B appellation soils:** $0.34 (0 to 930 links across 304 appellations)
- **Track C dupe reclassify:** $0.68 (2,017/2,682 unclear reclassified → 1,982 true_duplicate + 35 distinct)
- **Track D grape extract:** $0.35 — killed at 2% hit rate, not cost-effective
- Scripts: `pipeline/enrich/haiku_color_classify.py`, `pipeline/enrich/haiku_appellation_soils.py`, `pipeline/enrich/haiku_dupe_reclassify.py`, `pipeline/enrich/haiku_grape_extract.py`

### Inference reverts (2026-04-04)

See `docs/DECISIONS.md` entry "No probabilistic inference on canonical columns" and `memory/feedback_no_probabilistic_inference.md`. This session applied 18 inference operations across the canonical tables; 14 were reverted after user caught errors. Reverts had collateral damage: ~44K legit wine_grapes links cleared along with the 81K pattern-inferred ones, ~85K pre-session colors cleared before TTB restoration, 28.6K NV price rows removed.

**Kept (strictly definitional/direct):** wine_vintage_id composite-key backfill, region_id from appellations.region_id, country_id from region/appellation, wine_type regulatory reclassification (Champagne to sparkling, Tawny Port to fortified — legal category names), data grade F to D from raw data presence, and all direct staging promotions (not inference).

### Depth data populated via recovery + follow-up rounds

- **Wine depth** (was 0): 211K label images, 6.4K farming certs, 4.7K bottle formats, 809 food pairings, 696 descriptions, 449 sweetness, 166 winemakers, 233 pH, 251 TA, 192 RS, 100 fermentation vessels, 106 yeast types, 224 MLF, 166 oak duration, 158 oak origin, 101 closures, 321 production, 88 serving temps, 343 critic_score_avg
- **Producer depth** (was 0): 155 year_established, 185 websites, 117 GPS, 183 descriptions, 110 production, 194 winemaker links
- Alias tables seeded: 96 region, 75 label designation, 18,631 appellation

### Other pre-rebuild milestones

- Sonnet accuracy audit: 96% on 300-sample ($0.05). Non-sparkling data 100% clean.
- Sparkling wine fix applied: 8,977 reclassified. Distribution: table 93.6%, sparkling 4.7%, fortified 1.6%.
- Search fix: `search_catalog` v2 with unaccent + producer name matching. Findability 12% to 83%.
