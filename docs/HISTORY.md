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
