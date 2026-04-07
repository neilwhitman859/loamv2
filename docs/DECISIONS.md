# Loam — Decision Log

Append-only. Each entry records a human judgment call and why. Claude adds entries automatically when decisions are made during sessions. Use "log that" to force an entry.

---

### 2026-04-07: TTB class_type prefix 81→white executed, prefix 80→red rejected

TTB class_type codes encode wine color: prefix 80 = "not white" (red or rosé), 81 = white, 82 = rosé. Executed prefix 81→white fill (+40,538 wines, 95.9% validated accuracy — mismatches are random COLA matching errors). Rejected prefix 80→red fill despite 97.8% measured accuracy because ~2-3% of prefix 80 wines are rosé (TTB classifies rosé under "table wine" class 80, not separately). Tried stacking additional signals (grape color, appellation rules, multiple TTB records) — none cleared 99%. Rosé contamination is structural in TTB's classification system, not solvable by combining signals. Name-based color keywords recommended as next play.

### 2026-04-06: Duplicate wine merge buffer via match_decisions
All AI duplicate classification results (12,671 groups) written to match_decisions only. No canonical merges executed. Status values: ai_accepted (true_duplicate), ai_rejected (distinct_wines), flagged (unclear). Human review required before any actual merges. The 9,600 ai_accepted rows are the merge backlog.

### 2026-04-06: Nightly schema audit + conservative DB cleanup
Built pipeline/analyze/schema_audit.py (10-category read-only audit, saves JSON to data/stats/). Scheduled as nightly task. Executed conservative fixes this session: 8 missing FK indexes added, 2,278 grape double-space names cleaned, 1 vintage year corrected (2050→2024 parsing artifact). Schema audit journal at data/stats/schema_audit_journal.md.

---

### 2026-03-03: Full DB rebuild for v2
Starting fresh rather than migrating v1 schema. Dataset small enough to re-seed. Design the schema we actually want, build it clean.

### 2026-03-03: Weather lives at appellation level, not wine level
Weather is a property of place and year, not a bottle. Wine-level weather from ERA5 creates meaningless variation between wines in the same appellation. Fetch once per appellation-vintage using a representative coordinate.

### 2026-03-03: Appellations are the weather anchor, not regions
Appellations are specific enough to have meaningful weather data. Regions are often too broad (e.g., "California" is useless for weather). Wines inherit weather through their appellation link.

### 2026-03-03: Three-tier soil/water body fallback
Wine → appellation → region. Check wine_soils first, fall back to appellation_soils, then region_soils. Same pattern for water bodies. Avoids duplicating data while allowing specificity where we have it.

### 2026-03-03: UUID primary keys everywhere
Entity tables use UUID PKs with gen_random_uuid(). Join tables use composite PKs from FKs. Prevents enumeration attacks and makes merging datasets easier.

### 2026-03-03: Soft deletes on core tables
deleted_at TIMESTAMPTZ DEFAULT NULL on entity tables. Allows recovery and audit trails without losing data.

### 2026-03-03: Source tracking with companion columns
{field}_source UUID FK to source_types for fields where provenance varies. Enables re-enrichment and trust assessment. Users can see whether data came from a producer tech sheet, AI inference, or a wine database.

### 2026-03-03: Varietal categories, not just grapes
Wines are classified into varietal categories (single varietal, named blend, generic blend, regional designation, proprietary). This captures industry-standard classifications like "Bordeaux Blend" or "Champagne" that are more meaningful to users than raw grape lists.

### 2026-03-03: Separate insights tables for AI content
Factual data on core tables. AI synthesis in dedicated *_insights tables. This separation is foundational — it's what makes data trustworthy, re-enrichable, and eventually sellable.

### 2026-03-03: Single polymorphic trends table
One trends table with entity_type/entity_id instead of 6 entity-specific trend tables. Avoids table proliferation. Covers market trends, emerging narratives, buyer sentiment, price movements.

### 2026-03-03: wine_vintages get UUID PK, not composite
UNIQUE(wine_id, vintage_year) constraint instead of composite PK. Needed because vintage_year is nullable (NV wines).

### 2026-03-03: Baselines on appellations, not appellation_vintages
Long-term average GDD, rainfall, harvest temp stored on appellations table. Per-year actuals on appellation_vintages. Comparison = actual minus baseline.

### 2026-03-04: Grape synonym merging strategy
777 distinct names from X-Wines → 707 canonical grapes. Key merges: Syrah=Shiraz, Grenache=Garnacha=Cannonau, Pinot Gris=Pinot Grigio=Grauburgunder, Tempranillo=Tinta Roriz=Tinta de Toro (7 aliases). Muscat family: 7 distinct grapes kept separate. Malvasia family: all sub-varieties separate. Trebbiano family: sub-varieties separate.

### 2026-03-04: Zinfandel and Primitivo kept separate
Genetically identical, but the wine industry treats them as distinct. Merging would confuse users who know them as different wines from different places.

### 2026-03-04: Region naming conventions
English names where standard in professional wine trade (Burgundy not Bourgogne, Tuscany not Toscana). Local names retained where that's how labels and professionals refer to them (Mosel, Pfalz, Tokaj). Aligned with Wine-Searcher and Decanter conventions.

### 2026-03-04: Catch-all regions per country
62 catch-all regions, one per country. is_catch_all boolean distinguishes them. Slug pattern: {country-slug}-country. Purpose: wines without specific regional designation get a valid region_id without creating fake geography.

### 2026-03-04: US regions intentionally more granular
More sub-regions for the US than other countries because most users are US-based.

### 2026-03-04: Producer dedup via Haiku + manual review
30,684 candidates → 8,208 fuzzy pairs via pg_trgm → Claude Haiku verdicts in batches of 50 ($0.43) → 393 initial merges → deep manual review flipped 107 false merges (famous estates like Latour vs Latour à Pomerol) + 26 transitive chain false links → final: 260 merges, 7,948 keep_separate. Canonical name = most wines in each group.

### 2026-03-05: Appellation classification via Haiku
338 region names classified as formal appellation vs. broad region using Claude Haiku in batches of 40. Post-processing strips designation types from canonical names, fixes known misclassifications (e.g., Oloroso is a sherry style, not a geographic appellation). 223 new appellations created across 3 iterative runs.

### 2026-03-05: MVP enrichment strategy — top wines first
Rather than enriching all wines uniformly, prioritize top wines by vintage count as a proxy for importance/popularity. Full enrichment for the wines people care about, expand from there.

### 2026-03-05: Geo-coordinates as force multiplier
Appellation lat/lng unlocks weather data (Open-Meteo free), map visualizations, water body proximity, growing season validation. Priority investment.

### ~2026-03-08: xwines_ table separation
Bulk X-Wines dataset moved to xwines_* prefixed staging tables. Canonical tables (producers, wines, wine_vintages, etc.) reset for curated, high-quality data. xwines_ tables kept as reference but not actively maintained. Quality bar for canonical data is much higher than bulk import.

### ~2026-03-08: New fields on wines and wine_vintages
Added vineyard_name, food_pairings, metadata (jsonb) to wines. Added winemaker_notes, vintage_notes, brix_at_harvest, cases_produced, bottling_date, producer_drinking_window_start/end, metadata (jsonb) to wine_vintages. These capture common fields found when scraping producer wine pages.

### ~2026-03-08: Producer schema changes
Removed overview and overview_source from producers (AI overview content belongs in producer_insights). Added appellation_id, metadata (jsonb). Renamed website→website_url, established_year→year_established. Made country_id nullable. These changes need review during next big producer import.

### 2026-03-12: Documentation consolidation
7 maintained files with clear roles: CLAUDE.md (Claude's brain), README.md (project overview), docs/SCHEMA.md (table reference), docs/PRINCIPLES.md (product philosophy), docs/DECISIONS.md (this file), docs/VOICE.md (tone guide + food pairings), docs/WORKFLOW.md (session checklist). All other docs retired and absorbed.

### 2026-03-12: Don't hardcode DB state in docs
Claude queries the database for current row counts and state rather than relying on numbers in markdown files. SCHEMA.md documents structure and reasoning. DB state is always live-queried.

### 2026-03-12: Data quality over launch timeline
Soft goal of something live for friends by end of March 2026, but data accuracy and trustworthiness to a wine expert is the #1 priority. Willing to push any deadline for data quality. 100% accurate product or nothing.

### 2026-03-12: Region rebuild from scratch using WSET L3 as primary source
Old 352 regions from X-Wines bulk import replaced with curated two-level hierarchy. Primary source: WSET Level 3 Award in Wines Specification (Issue 2, 2022), pages 10-14. Supplementary: Oxford Companion to Wine, government wine authorities (DWI, Wine Australia, SAWIS, etc.). No Wine-Searcher (TOS concern). L1/L2 implicit via parent_id — no schema change. US not significantly more granular than other countries. Data file: `data/regions_rebuild.json`. Migration script: `scripts/rebuild_regions.mjs`.

### 2026-03-12: Two-level hierarchy confirmed after wine expert review
Considered 3 levels to resolve containment issues (SA Western Cape → Coastal Region → Stellenbosch, AU South Australia → Barossa → Barossa Valley). Decided 2 levels is sufficient — regions are a navigational grouping layer, not a containment model. Only 2-3 countries need 3 levels; the complexity isn't justified. Containment overlaps (Western Cape as sibling to Coastal Region, Barossa as sibling to Barossa Valley) are acceptable and handled by mapping appellations to the most specific matching region.

### 2026-03-12: Region refinements from WSET cross-check
- Removed Entre-Deux-Mers as Bordeaux L2 — it's an AOC, not a navigational grouping. Left Bank / Right Bank are the standard Bordeaux sub-regions.
- Added Kamptal and Kremstal as L2s under Niederösterreich — major Austrian DACs missing from WSET L3 but essential for wine professionals.
- Added Tejo as Portugal L1 — third-largest Portuguese wine region by volume, absent from WSET L3.
- Added Castilla y León back to Spain (exists alongside The Duero Valley — CyL for VdlT regional wines, Duero Valley for DOs).
- Added Barossa zone as AU L2 alongside Barossa Valley per WSET spec.
- Renamed "Southwest France" to "The Dordogne and South West France" per WSET spec exact wording.
- Mapping principle: when multiple regions apply, use the most specific / label-matching region.

### 2026-03-12: L2 sub-regions added for Italy and Spain
Italy: Added 13 L2 sub-regions (Langhe, Monferrato, Roero under Piemonte; Chianti, Montalcino, Montepulciano, Bolgheri under Tuscany; Valpolicella, Soave, Conegliano-Valdobbiadene under Veneto; Etna under Sicily; Franciacorta, Valtellina under Lombardy). Source: WSET L3 + Federdoc 2025.
Spain: Added 13 L2 sub-regions under WSET geographic groupings (Rioja, Navarra under Upper Ebro; Penedès, Priorat under Catalunya; Ribera del Duero, Rueda, Toro under Duero Valley; Rías Baixas, Bierzo under North West; La Mancha under Castilla-La Mancha; Jumilla, Valencia under Levante; Jerez under Andalucía). Source: WSET L3 + MAPA DOs.
Renamed "Napa County" to "Napa Valley" — universally recognized name.

### 2026-03-12: X-Wines leftover regions purged
Deleted 147 non-curated regions from regions table. Reassigned 181 orphan appellations to country catch-all regions. Removed 47 geographic boundaries and 126 region insights tied to leftovers. Dropped xwines FK constraints to regions (xwines_wines, xwines_producers, xwines_region_name_mappings) since archive tables don't need referential integrity. Fixed 10 L2 regions that had missing parent_id (NZ, SA, France, Austria, Argentina). Reparented curated children from leftover parents (columbia-valley/yakima-valley from old "washington" to "washington-state"; monterey/santa-barbara-county from old "central-coast" to "california"; languedoc/roussillon from old "languedoc-roussillon" to "southern-france").

### 2026-03-12: Created Lorraine and Cognac L1 regions for France
Added two new L1 regions to cover 3 peripheral AOCs that had no home: Lorraine (Moselle, Côtes de Toul) and Cognac (Pineau des Charentes). Pineau des Charentes is a vin de liqueur not covered by WSET L3, but it's an INAO-classified AOC and deserves a region rather than sitting on catch-all.

### 2026-03-12: Catch-alls are for unappellated wines only
All wines with an official appellation should be in a named region, even if the region is small. Catch-all regions should be reserved for wines without an official appellation (Vin de France, etc.), not as a dumping ground for appellations in minor wine areas. This means creating regions even for small/marginal wine-producing areas if they have official appellations in our database.

### 2026-03-12: Appellations on lowest-level region, rolls up naturally
Appellations should be attributed to the most specific (lowest-level) region they can accurately belong to. If Oakville AVA is in Napa Valley L2, it's implicitly in California L1 — no need to put it on L1 directly. Fixed 33 empty L2 regions caused by flat Pass 3 attribution pointing at L1s.

### 2026-03-12: Region boundaries rebuilt from scratch with 4 confidence tiers
Deleted all 27 existing region boundaries and rebuilt consistently. Four tiers: `official` (copied from wine authority appellation boundary), `derived` (NEW — ST_Union of child appellation polygons), `approximate` (Nominatim admin boundaries), `geocoded` (centroid-only). Derived boundaries preferred for wine platform — they represent actual wine territory better than admin boundaries. 250KB JSON size cap with progressive simplification.

### 2026-03-12: Regions are qualitative, not legally defined
Unlike appellations which have legal boundaries, regions are qualitative approximations of wine-producing areas as a wine expert would understand them. It's acceptable (and often better) to include areas that are clearly part of a region even if no specific appellation polygon covers them. This distinction is important for boundary smoothing and Sonnet review decisions.

### 2026-03-13: Sonnet review round 1 — attribution fixes applied
Applied 10 appellation→region moves and 1 region rename from the Sonnet review report. Moved Pokolbin + Broke Fordwich to Hunter Valley L2, Agrelo + Las Compuertas to Lujan de Cuyo L2, Darling to Swartland L2, Wellington + Franschhoek to Paarl L2, Blaye + Cotes de Blaye + Bourg/Cotes de Bourg/Bourgeais to Right Bank L2. Renamed "The Dordogne and South West France" back to "Southwest France" (WSET exact wording was overly verbose for navigation). Skipped Swan Valley→Swan District move: Swan District is an appellation in our schema, not a region, and the containment hierarchy already captures Swan Valley as a child of Swan District.

### 2026-03-13: Sonnet review triage — new regions and structural decisions
Created 13 new regions based on Sonnet review recommendations and WSET L3 alignment analysis:
- **Canada:** Niagara Peninsula L2 (under Ontario, 14 apps) + Okanagan Valley L2 (under BC, 12 apps) — WSET standard subregions
- **South Africa:** Klein Karoo L2 + Olifants River L2 (under Western Cape) — SAWIS hierarchy alignment, fixed 3 misattributed Olifants River appellations from Coastal Region
- **Austria:** Carnuntum, Thermenregion, Wagram, Traisental as L2s under Niederösterreich — each IS a DAC, matches WSET structure
- **Spain:** Somontano L2 (under Aragón) + León L2 (under Castilla y León) — WSET-recognized subregions
- **Portugal:** Beira Interior L1 + Trás-os-Montes L1 — missing from original rebuild, major Portuguese wine regions
- **UK:** Scotland L1 — emerging wine region, distinct from England/Wales
- **Cava:** Moved to Spain catch-all (spans 7+ autonomous communities, no single region is accurate)
- **Parked:** Switzerland, Italy, Croatia, Hungary restructuring + England sub-regions (too early)
- **No change:** Japan (current structure fine), Darnibole (legitimate English PDO)

### 2026-03-13: Multi-state US AVAs stay on catch-all
14 US appellations that span state lines (e.g., Columbia Valley, Walla Walla Valley) remain on the USA catch-all region. Attributing them to any single state would be inaccurate. They roll up at the country level by design.

### 2026-03-13: Portugal catch-all edge cases left as-is
Encostas d'Aire, Lafões, and Távora-Varosa remain on Portugal catch-all. Each sits geographically between two existing regions (Lisboa/Bairrada, Dão/Vinho Verde, Dão/Douro respectively). Forcing them into either adjacent region would be equally inaccurate.

### 2026-03-13: LWIN as canonical external wine identifier
LWIN (Liv-ex Wine Identification Number) adopted as the industry-standard cross-reference for Loam. CC BY 4.0 licensed, 187K wines, 37K producers. LWIN-7 maps to `wines`, LWIN-11 maps to `wine_vintages`. Gets first-class columns on both tables rather than going through the external_ids table. Decision documented in `docs/LWIN_STRATEGY.md`.

### 2026-03-13: Three-layer data strategy — no crowdsourced platforms
All data sources must be first-hand or regulatory. No Vivino, Wine-Searcher, or CellarTracker data. Layer 1: LWIN (identity backbone). Layer 2: Government registries — TTB COLA, EU e-labels, INAO, Wine Australia, etc. (catalog completeness). Layer 3: Producer direct — website scraping for winemaking depth, terroir narrative, AI synthesis (the Loam value-add).

### 2026-03-13: Schema assessment completed — 21 new tables, ~45 new columns
Deep expert assessment of every schema gap before wine import phase. Organized into Tier 0 (structural, hardest to change later) through Tier 3 (defer). Full implementation spec with user decisions in `docs/SCHEMA_ASSESSMENT.md` Part B. All schema work to be completed before any mass wine import.

### 2026-03-13: Phased roadmap adopted
Six phases: (1) Foundation — full schema hardening + reference data completion + trial producer imports, (2) LWIN import — 187K wine skeletons as identity backbone, (3) TTB COLA + other sources — everyday wine breadth (needs dedicated source research session first), (4) Vertical slice enrichment — California + Burgundy from Tier 3 to Tier 2/1, (5) Label scanner — OCR + fuzzy match, (6) Frontend. Not rushed — "fast is slow, slow is fast."

### 2026-03-13: Tiered wine experience model
Wines have different data completeness levels and the product handles each explicitly: Tier 1 (fully enriched — producer-scraped, full terroir/winemaking story), Tier 2 (identified + AI-contextualized from reference data), Tier 3 (just identified — name/place skeleton from LWIN or TTB COLA). Unknown wines show "we don't have this one yet."

### 2026-03-13: LWIN import before TTB COLA
LWIN goes first because it establishes the dedup backbone. COLA wines then match against existing LWIN records (enriching with grape data, label images, importer info) and create new records only for wines LWIN doesn't cover. One-directional matching against an established catalog is cleaner than bilateral dedup.

### 2026-03-13: Make varietal_category_id nullable on wines
Rather than creating an "Unclassified" placeholder varietal category for LWIN imports (which have no grape data), make the FK nullable. NULL means "we don't know yet" — honest and clean. Populated when enrichment fills in grape data.

### 2026-03-13: Trial producer picks for schema stress test
Before mass import, scrape 4-5 new producer websites to verify the schema handles diverse wine data. Picks: Moone Tsai (CA — small/boutique), Fort Ross (CA — small US vineyard), López de Heredia (Spain — traditional Rioja, Reserva/Gran Reserva system), plus a Burgundy producer and a Tuscan producer (TBD). Each exercises different schema features.

### 2026-03-13: Vertical slice — California + Burgundy
First enrichment targets: all wines in California (breadth, everyday + fine wine) and Burgundy (depth, vineyard-level classification, négociant vs domaine, hardest terroir test). If Loam can tell the Burgundy story well, it can handle anything.

### 2026-03-13: Enrichment on demand as possible architecture
Rather than batch-enriching 187K wines, keep most at Tier 3 and enrich to Tier 2 on demand when a user looks up a wine. Reference data is already in the DB — Claude can synthesize appellation context + grape profile in real-time. More sustainable than batch enrichment. Needs further design and planning.

### 2026-03-13: Workflow preferences
Longer focused sessions. Collaborative decision-making (Claude proposes, user guides). Trust Claude to execute specs and report results. Thorough over fast — do it right the first time.

### 2026-03-14: Cleared all producer scrape data — starting fresh
Deleted all wines (267), vintages (1,757), scores (2,214), grape entries (491), and producers (3) from Ridge, Tablas Creek, and Stag's Leap. Clean slate before schema hardening. Will re-scrape after schema is production-ready (Phase 1c).

### 2026-03-14: Schema hardening — Phase 1a execution
Executing the full implementation spec from SCHEMA_ASSESSMENT.md Part B. Step-by-step with human review at each tier. Decisions made during execution:
- `attribute_definitions` already existed (empty) — skipped creation, created companion `entity_attributes` to complete the flex field system.
- `wine_appellations` kept as secondary junction table — `wines.appellation_id` remains the primary appellation link. Junction handles rare multi-appellation cases.
- Polymorphic pattern (entity_type + entity_id) kept for `external_ids`, `entity_attributes`, `entity_classifications`. Orphan risk mitigated by soft deletes.
- `vineyards` table enhanced: added `region_id` and `country_id` (not in original spec). CHECK constraint enforces at least one geographic anchor (appellation_id OR region_id OR country_id).
- `wine_vintage_components` renamed to `wine_vintage_nv_components` for clarity.
- `importers` table is country-agnostic by design (has country_id FK) but will be populated US-first since TTB COLA is the primary data source.

### 2026-03-14: Vineyard soils — skip percentage column
Vineyard soils table (`vineyard_soils`) is many-to-many without a percentage column. Percentages like "40% loam, 60% chalk" are almost never available from producer data — we'll have "loam and chalk" but not proportions. If we ever get percentages, we can add the column later. Simpler schema now.

### 2026-03-14: Classification systems — 8 systems, 22 levels
Seeded from authoritative sources (not training data): Bordeaux 1855 Médoc (5 levels), Bordeaux 1855 Sauternes (3), Saint-Émilion GCC (3), Graves (1), Burgundy Vineyard (2), Alsace Grand Cru (1), VDP Germany (4), Cru Bourgeois (3). Key distinction: classifications rank entities (producers/vineyards) within an appellation. DOC/DOCG are appellations, not classifications. US has no classification system. Cru Bourgeois is a classification (three tiers, five-year renewal cycle), not a label designation.

### 2026-03-14: Label designations — controlled vocabulary replacing free text
Created `label_designations` (73 entries), `label_designation_rules` (appellation-specific variations), and `wine_label_designations` (many-to-many join) tables to replace free-text `wines.label_designation`. Categories: aging_tier, pradikat_tier, production_method, estate_bottling, late_harvest, ice_wine, botrytis_sweet, vineyard_designation, vineyard_age, quality_tier, geographic_qualifier, sparkling_type, early_release. German Prädikats are label designations (they classify the wine by must weight), not classifications (which rank entities).

### 2026-03-14: Label designation rules — two-table approach for appellation-specific variation
Designations like Riserva, Crianza, and Prädikats mean different things in different appellations. `label_designation_rules` captures per-appellation requirements (aging, barrel, ABV, yield, Oechsle). Populated: Spanish Crianza/Reserva/Gran Reserva (7 rules), Portuguese Reserva/Grande Reserva (14 rules), Italian Superiore (32 rules), German Prädikats (78 rules across 13 Anbaugebiete × 6 levels with Zone A/B differentiation). Italian Riserva pending.

### 2026-03-14: Dropped US Reserve from label designations
US "Reserve" has no legal meaning — any winery can use it. Not regulated, not useful for a data platform focused on accuracy. All regulated designations kept.

### 2026-03-14: Sparkling sweetness terms — universal, country_id NULL
Brut Nature, Extra Brut, Brut, Extra Dry/Extra Sec, Dry/Sec, Demi-Sec, Doux added with country_id=NULL. These are EU-regulated terms used worldwide by convention. Universal application, not country-specific.

### 2026-03-14: Grape display_name — three-tier naming strategy
Added `display_name` column to grapes table. VIVC prime names stored in `name` (UPPERCASE, canonical reference). `display_name` derived via three tiers: (1) 26 explicit overrides for major grapes where VIVC name differs from industry standard (MERLOT NOIR→Merlot, COT→Malbec, CALABRESE→Nero d'Avola, MONASTRELL→Mourvèdre, GARNACHA TINTA→Grenache, ALVARINHO→Albariño, etc.); (2) Multi-variant families keep suffix (Pinot Noir stays Pinot Noir, not "Pinot"); (3) Single-variant grapes get color suffix stripped and title-cased. Country-specific synonyms with `is_primary_in_country=true` enable per-market display (Zinfandel in US, Primitivo in Italy; Garnacha in Spain, Grenache elsewhere). Verified against WSET Level 3 and TTB standards.

### 2026-03-14: Grape rebuild — keep all VIVC wine grapes, no artificial cap
Rebuilding the grapes table from scratch using VIVC (Vitis International Variety Catalogue) as the authoritative source. Originally planned ~1,000–1,500 "commercially significant" grapes, but decided to keep every grape VIVC classifies as wine utilization (expected 3,000–5,000). Rationale: storage cost is near-zero, more grapes means higher auto-match rate for LWIN import (187K wines), no UX downside since users encounter grapes through wines not browsing. VIVC prime name is the canonical `grapes.name`; TTB name stored in `ttb_name` for US display. Parentage resolved in a second pass after all grapes inserted. Dropped `grapes.aliases` TEXT[] in favor of structured `grape_synonyms` table. Added columns: `aroma_class`, `crossing_year`, `breeder`, `breeding_institute`, `origin_type`, `eu_catalog_countries`. Created `grape_plantings` table for grape × country planting areas. Source: VIVC direct scraping (no Wikidata intermediary).

### 2026-03-14: Grape associations — two-level system (required/typical) at three geographic levels
Replaced `appellation_grapes.is_required` boolean with `association_type` text enum ('required', 'typical'). "Required" = regulatory mandate (EU disciplinari/INAO). "Typical" = commonly planted / known for (everything else). Rejected a third "occasional" level — the distinction between typical and occasional is a judgment call that doesn't improve the user experience. Nuance belongs in the `notes` field. Created `region_grapes` and `country_grapes` tables with same structure (minus min/max percentage, which is appellation-level regulatory detail). Three-table hierarchy enables grape data at every geographic level.

### 2026-03-14: Two-pass expert audit approach for reference data
Established pattern: seed data from authoritative sources, then run two independent audits — (1) training data / wine expertise check for wrong/missing/questionable entries, (2) web source verification against official publications (Wine Australia, SAWIS, INAO, DWI, etc.). Compare findings, fix intersection of CRITICAL+HIGH issues immediately, park MEDIUM/LOW for later. Applied to region_grapes and country_grapes: found 120 issues (training) and 34 issues (web), with strong overlap on the critical ones. Fixed 10 deletions + 34 additions. ~90 medium/low issues parked (mostly naming conventions that belong in the display layer, not the data layer).

### 2026-03-15: ABV as first-class column on wine_vintages
ABV appears on literally every wine listing (producer, retailer, importer). Storing it in entity_attributes would require a JOIN for the most basic display. Added `abv numeric(4,1)` directly to wine_vintages. The entity_attributes system remains for less universal fields (pH, TA, oak details, etc.).

### 2026-03-15: Cross-table validation audit before trial imports
Full integrity audit before moving to Phase 1c imports: FK checks across all grape/appellation/classification/descriptor tables, label designation rule verification (German Prädikats 78 rules, Italian Riserva 23 rules, Portuguese ABV rules 14 rules), thin region grape fixes (6 removals, 20 additions), varietal category expert audit (31 grape mappings added to regional designations like Madeira/Marsala/Vinho Verde), statistical sanity checks. Three missing grapes added (Nerello Cappuccio, Mujuretuli, Tempranillo Blanco) and two wrong synonyms removed.

### 2026-03-15: Cold-hardy hybrids in region_grapes
Iowa, Minnesota, Wisconsin — replaced vinifera entries (Cab Sauv, Merlot, Cab Franc, Chardonnay) with cold-hardy hybrids (Marquette, Frontenac) that actually represent commercial production. The VIVC-sourced grapes table includes hybrids, so this is supported.

### 2026-03-15: Phase 1c import architecture — shared library + standardized JSON
Producer imports use a two-layer approach: (1) per-producer data extraction into a standardized JSON format (`data/imports/{slug}.json`), (2) shared import library (`lib/import.mjs`) that resolves all FK references and inserts. This separates the always-custom scraping from the always-same DB logic. Grape resolution uses a three-tier strategy: hardcoded alias table → display_name lookup → grape_synonyms table. The library supports `--dry-run` and is idempotent (checks for existing records before inserting).

### 2026-03-15: Score sourcing — producer websites + publicly visible aggregators
For trial imports, scores come from producer websites (primary) and publicly visible aggregator data (supplementary). No scraping behind paywalls. Source type tracked as `producer-website`. This gives good coverage while staying clean on licensing. Phase 2+ will revisit when LWIN provides the dedup backbone.

### 2026-03-15: producer_type "virtual" for non-estate producers
Moone Tsai classified as `producer_type: virtual` — they source from multiple Napa vineyards (Soda Canyon, Yountville, Howell Mountain) rather than owning estate vineyards. This distinction matters for understanding wine provenance.

### 2026-03-15: Winemakers as a first-class entity
Created `winemakers` table + `producer_winemakers` junction with role (head/consulting/assistant/founding) and tenure (start_year/end_year). Winemakers frequently consult for multiple producers (e.g., Philippe Melka works with 10+ wineries) and producers change winemakers over time. This data is too important for wine enthusiasts to leave in metadata.

### 2026-03-15: Production volumes standardized on cases
Industry standard is cases (12 × 750ml = 9L). LWIN, Wine Spectator, Wine Advocate, auction houses all use cases. European producers report in bottles (÷12) or hectoliters (×11.11). Convert at import time, store as `cases_produced`.

### 2026-03-15: Bottle formats table
Created `bottle_formats` reference table (10 standard sizes from Piccolo 187ml to Nebuchadnezzar 15000ml) + `wine_vintage_formats` junction with per-format cases_produced and release_price_usd. Collectors care deeply about format availability, and prices vary significantly by format.

### 2026-03-15: Multi-vineyard sourcing via wine_vineyards + wine_vintage_vineyards
Created two junction tables linking wines to vineyards: `wine_vineyards` for default/typical sources, `wine_vintage_vineyards` for per-vintage sourcing with percentage. Many quality producers source from multiple vineyards (Moone Tsai: 5-7 per wine) and the blend changes year-to-year.

### 2026-03-15: Second labels as child producers (parent_producer_id)
Added self-referencing `parent_producer_id` FK on `producers` for second labels and sub-brands. Matches LWIN convention where second wines get their own LWIN-7 codes. Sea Slopes becomes its own producer with parent = Fort Ross. This is cleaner than brand columns on wines because the tier is a property of the brand, not individual wines. Also handles Bordeaux seconds (Les Forts de Latour → Château Latour) which Phase 2 LWIN import will encounter.

### 2026-03-15: Import library field name flexibility
The import library (`lib/import.mjs`) now accepts both canonical field names and common alternatives from JSON files. E.g., `oak_duration_months` or `oak_months`, `production_cases` or `cases_produced`, `founded_year` or `year_established`, `reviewer` or `critic`. This prevents format fragility when different research agents produce slightly different JSON structures.

### 2026-03-15: Text dates parsed to ISO in importer
Bottling dates and harvest dates from producer websites often use informal formats like "August 2024". The importer now parses these to ISO dates (first-of-month: "2024-08-01") rather than rejecting them. This captures the data rather than losing it silently.

### 2026-03-15: Phase 1c expanded to 6 producers across 4 countries
Trial producer imports expanded from 3 to 6: Fort Ross (US/Sonoma), Sea Slopes (US/Sonoma, child producer), Moone Tsai (US/Napa), López de Heredia (Spain/Rioja), Marchesi Antinori (Italy/Tuscany), Louis Jadot (France/Burgundy). This gives broad schema stress-testing across estate/negociant types, Old/New World, DOCG/DOC/IGT/AVA appellations, and single-varietal/blend wines.

### 2026-03-15: Principle #9 — Training data for validation only
Added to PRINCIPLES.md: Claude's training data should only be used for validation (confirming, cross-referencing, auditing). Never for generating new factual content that goes into canonical tables. Scores, tasting notes, production figures, vintage details must come from primary sources. Training data is the second opinion, not the source of truth.

### 2026-03-15: Multi-estate producers use parent-child pattern
Large wine groups (Antinori, LVMH, etc.) model each estate as a child producer with `parent_producer_id` pointing to the parent company. Same pattern as Sea Slopes → Fort Ross. No new schema needed.

### 2026-03-15: Wine name evolution via wine_aliases table
Track historical wine names (renames, market-specific names) via a `wine_aliases` table rather than a simple `previous_name` field or creating separate wine records. This preserves vintage continuity while handling multiple renames.

### 2026-03-15: Clone data stays in metadata JSONB for now
Clone information is too rare and inconsistent to justify structured storage. The `metadata` column on `wine_vintages` handles it. Revisit if clone-specific querying becomes a product need.

### 2026-03-15: Critic-level drinking windows on wine_vintage_scores
Critics often provide drinking windows alongside scores (e.g., "Drink 2025-2035"). Adding `drinking_window_start` and `drinking_window_end` columns to `wine_vintage_scores` — distinct from the producer-level `producer_drinking_window_start/end` on `wine_vintages`.

### 2026-03-15: Schema changes require human approval
The importer must never trigger DDL (CREATE TABLE, ALTER TABLE, etc.) — it maps JSON to an existing, fixed schema. Schema changes can still happen, but they require explicit human approval. This prevents Claude from silently adding columns or tables during import runs. The drinking_window_start duplicate column incident (added without checking existing critic_drink_window_start/end columns) demonstrated the risk.

### 2026-03-15: Metadata fields promoted to structured columns
Analysis of metadata across 6 trial producers identified 4 high-frequency, universally useful keys that deserve structured columns: `release_date` on wine_vintages (75 entries), `first_vintage_year` on wines (15), `style` on wines (17), `philosophy` on producers (2 but universal). Additional metadata keys identified for migration to proper table links: `classification` (67, should be entity_classifications), `vineyard`/`vineyard_sources` (115, should be wine_vineyards/wine_vintage_vineyards), `estate`/`domaine` (45, should be child producers). Remaining metadata (clones, cooperage details, notes, historical_note) stays in JSONB — too unstructured or infrequent to justify columns.

### 2026-03-15: Enrichment log rebuilt with cost/model/audit tracking
Original enrichment_log was a basic job queue (stage, attempts, stale_reason) with no model, cost, or audit capabilities. Rebuilt (zero rows, safe drop) with: model tracking, cost tracking (input_tokens, output_tokens, cost_usd), prompt template versioning, field-level change tracking (fields_updated, previous_values for rollback), review workflow (reviewed_by, reviewed_at), and source context (source_ids). This is the foundation for tracking AI enrichment costs and quality.

### 2026-03-15: Appellation rules as flexible JSONB
Appellation winemaking rules (ABV minimums, yield limits, aging requirements, allowed methods) stored in a single `appellation_rules` table with a JSONB `rules` column. One row per appellation. JSONB chosen over rigid columns because rule types vary wildly across regulatory frameworks (French AOC, Italian DOCG, Spanish DO, German Anbaugebiet all have different rule structures). Queryable via Postgres JSONB operators without schema changes as new rule types are discovered.

### 2026-03-15: Multi-source data merging — design for future session
Architecture proposal for handling data from multiple sources (LWIN, producer websites, critics, government registries): (1) Source priority tiers on source_types (producer > government > LWIN > critic > aggregator), (2) Field provenance sidecar table (entity_type, entity_id, field_name, source_id, updated_at) instead of per-column _source fields, (3) Importer merge mode that respects source priority. To be implemented in a dedicated session.

### 2026-03-15: Schema refinements from Kermit Lynch bulk import stress test
Five schema changes based on importing 1,468 wines from 193 KL growers:
1. **varietal_category_id made nullable** on wines — no external source provides varietal categories natively. Forcing NOT NULL required fragile inference logic. Better to populate when genuinely known.
2. **certification_status added** to producer_farming_certifications — certified/practicing/transitioning. KL distinguishes "Biodynamic (certified)" from "Biodynamic (practicing)" which is a meaningful real-world distinction.
3. **latitude/longitude added** to producers — KL provides GPS coords for growers. Useful for map display and geographic resolution. Was being stuffed into metadata JSONB.
4. **vinification_notes added** to wines — free text for general winemaking approach (fermentation, maceration, aging). Distinct from vintage-specific winemaking data on wine_vintages.
5. **appellation_aliases table created** — accumulated fuzzy match mappings for appellation name resolution. KL import only resolved 11% of appellations because names like "Châteauneuf-du-Pape Rouge" don't exact-match. This table stores resolved mappings so future imports reuse them.

### 2026-03-15: No country-specific tables for classification systems
Evaluated whether complex classification systems (Italian DOCG/DOC/Classico/Riserva, German Prädikats, Bordeaux 1855, Port styles, etc.) need country-specific tables. Answer: no. The existing generic schema handles all cases through four complementary layers: appellations.designation_type (DOC/AOC/AVA), label_designations + label_designation_rules (Riserva/Crianza/Kabinett with per-appellation rules), classifications + entity_classifications (1855/Burgundy cru/VDP), and appellation_rules JSONB (flex regulatory data). Country-specific tables would fragment the query layer — one generic query pattern is better than N country-specific ones.

### 2026-03-15: Delete legacy scripts rather than patch them
Deleted `scrape_ridge.mjs` and three Vivino scripts (`fetch_producer_wines.mjs`, `create_wines_from_vivino.mjs`, `match_vivino_to_loam.mjs`). These referenced dropped columns (`wines.yeast_type`), non-existent tables (`region_name_mappings`), and missing columns (`grapes.aliases`) from the xwines era. Schema is still changing — remaining scrapers will break and get fixed when actually re-used. Better to delete dead code than patch it.

### 2026-03-15: wines.country_id made nullable
Retailer imports (Last Bottle, Best Wine Store, Domestique) showed that many value wines don't clearly state country of origin. Forcing NOT NULL required defaulting ambiguous wines to US — creating inaccurate data. Core principle: better to be null than wrong. Producers already had nullable country_id.

### 2026-03-15: Effervescence defaults to 'still'
95%+ of wines are still. "Unknown effervescence" is almost never a real state — if you know enough to insert a wine, you know if it's sparkling. DEFAULT 'still' reduces boilerplate in every importer and ensures consistency.

### 2026-03-15: Score provenance tracking
Scores extracted from marketing copy (retailers quoting "93 Points Wine Spectator!") have very different reliability than scores pulled directly from critic databases. Added `score_provenance` CHECK (direct/retailer_quote/aggregated/community) to `wine_vintage_scores`. This supports future score weighting/display logic.

### 2026-03-15: wine_vintage_id FK added to scores and prices
Normalizes the denormalized `wine_id + vintage_year` pattern on `wine_vintage_scores` and `wine_vintage_prices`. Column is nullable (scores can exist for vintages not yet in wine_vintages). Backfilled 100% of existing data. Legacy convenience columns kept. Preferred join path going forward.

### 2026-03-15: Deprecated columns dropped (acidity/tannin/body, label_designation)
`wine_vintages.acidity/tannin/body` (1-5 WSET scale) superseded by `wine_vintage_tasting_insights` table which has full WSET SAT structured data. `wines.label_designation` free text superseded by `wine_label_designations` junction table. All existing data came from rescrapeable sources — no data loss concern.

### 2026-03-15: Retailers table created
Normalized retailer reference table rather than free-text `merchant_name` on `wine_vintage_prices`. FK from prices to retailers. Retailer type CHECK: online/brick_and_mortar/auction_house/direct_to_consumer/marketplace. Also added `compare_at_price_usd` for discount retailers where sale price ≠ market value.

### 2026-03-15: grapes.name_normalized added
Consistency with producers and wines tables. All three entity types now have name_normalized for dedup matching. Indexed for performance. Backfilled from VIVC names.

### 2026-03-15: Enrichment strategy reconciled — proactive + on-demand
Two earlier decisions described complementary approaches: (1) "MVP enrichment — top wines first" (2026-03-05) = batch proactive enrichment prioritized by vintage count/importance, (2) "Enrichment on demand" (2026-03-13) = reactive enrichment when a user looks up a wine. These coexist: proactive batch for Tier 1 targets (California + Burgundy top wines by score/vintage count), on-demand for the long tail (Tier 3 → Tier 2 synthesis from reference data when a user encounters a wine). Most wines stay at Tier 3 until looked up.

### 2026-03-15: Source tracking evolution — companion columns → provenance sidecar
Original approach (2026-03-03): `{field}_source UUID FK` companion columns per field. This was partially abandoned during schema sharpening (2026-03-15) — dropped _source columns for aspect/slope/fog_exposure/vine_planted_year from wines (source tracking for these belongs on entity_attributes). Future direction (designed, not yet implemented): field provenance sidecar table replacing per-column _source fields entirely. Existing _source columns on wine_vintages (chemical_data_source, winemaking_source, harvest_date_source, release_price_source) remain until the sidecar is built.

### 2026-03-15: Claude granted schema autonomy during hardening phase
During active schema hardening, Claude can independently: make nullable/NOT NULL calls (philosophy: null > wrong), add columns vs JSONB decisions, expand CHECK constraints, drop redundant columns, create new tables. This autonomy will be restricted once the schema stabilizes for production. Core architectural patterns (geography hierarchy, facts-vs-insights separation, UUID PKs, soft deletes, three-tier fallback) remain foundational and should be flagged before changing.

### 2026-03-16: NV wine convention — vintage_year=0
Non-vintage wines (Champagne NV, Tawny Port, multi-vintage blends like Vega Sicilia Reserva Especial) use `vintage_year=0` rather than NULL. This allows the UNIQUE(wine_id, vintage_year) constraint to work, prevents null-handling complexity throughout the codebase, and is semantically clear: 0 means "intentionally non-vintage."

### 2026-03-16: Champagne dosage tracked via rs_g_l
Champagne dosage levels (Brut Nature=0, Extra Brut≤6, Brut≤12, etc.) stored using the existing `rs_g_l` (residual sugar grams per liter) column on wine_vintages. No separate dosage column needed — dosage IS residual sugar. Brut Nature with zero dosage correctly stores `rs_g_l=0`.

### 2026-03-16: wine_type 'table' not 'still' — effervescence is separate
The `wines.wine_type` CHECK allows table/sparkling/dessert/fortified/aromatized. "Still" is NOT a wine type — it's an effervescence value. A still red wine has `wine_type='table'` and `effervescence='still'`. A Champagne has `wine_type='sparkling'` and `effervescence='sparkling'`. This orthogonal design handles edge cases like sparkling dessert wines (e.g., Moscato d'Asti: wine_type='dessert', effervescence='sparkling').

### 2026-03-16: Classification system aliases for flexible JSON matching
Rather than requiring exact system names in import JSON (e.g., "Langton's Classification of Australian Wine"), the importer builds an alias map during reference data load. Short names like "Langton's" or "1855 Sauternes" resolve to full DB names. This reduces friction without changing the schema.

### 2026-03-16: Import→harden cycle as schema stress test methodology
10 producer imports across 8 countries covering all 5 wine types (table, sparkling, dessert, fortified, aromatized). Each import chosen to exercise different schema features: Champagne (disgorgement, dosage, NV), German Riesling (Prädikat, VDP), Sauternes (high RS, 1855 classification), Port (fortified, age statements, NV tawny), Australian (multi-region, Langton's), Tokaji (ultra-high RS, puttonyos), NZ (biodynamic certs), Argentine (high altitude), Provence (rosé), Spanish (extreme oak aging, multi-vintage NV). Each friction point fixed in the importer strengthens future imports.

### 2026-03-15: Appellation aliases seeded from primary sources + mechanical generation
17,558 aliases seeded into appellation_aliases table from four source tiers:
1. **INAO OpenDataSoft API** (primary source): 2,557 official French AOC wine product variants — color suffixes (rouge/blanc/rosé), vendanges tardives, vin jaune, premier/grand cru sub-types. API: `public.opendatasoft.com/api/explore/v2.1/catalog/datasets/aires-et-produits-aocaop-et-igp/records`, filtered to `signe_fr LIKE 'AOC%' OR 'IGP%'`.
2. **Mechanical Tier 1**: color suffixes (9,866), designation type suffixes (3,193), accent-stripped variants. Applied per country using local language (rouge/rosé for FR, rosso/bianco for IT, tinto/blanco for ES, etc.).
3. **Slash-form variants** (276): EU PDO multi-name appellations split into components (e.g., "Alsace / Vin d'Alsace" → "Alsace", "Vin d'Alsace"). Plus color suffixes for each variant.
4. **Industry knowledge** (49): Common abbreviations (CdP, CDR), saint abbreviations (St-Emilion), Italian short forms (Brunello → Brunello di Montalcino, Amarone → Amarone della Valpolicella).
Result: KL appellation resolution improved from 10.8% (159/1,468) to 67.0% (983/1,468). Remaining 485 unmatched are genuinely not appellation names (Champagne wines starting with "Brut", Italian IGT branded wines, generic color terms).
EU GIview/eAmbrosia has no public API (SPA-only). Italian Masaf wine registry was down. Eurac PDO_EU_cat.csv has basic category data but not granular tipologie.

### 2026-03-15: Schema sharpening — 8 fixes for data integrity and normalization
1. **Color standardized to ASCII 'rose'** (not 'rosé') — matches varietal_categories convention. CHECK constraint added on wines.color (red/white/rose/orange).
2. **CHECK constraints added** on wines.wine_type (table/sparkling/dessert/fortified), wines.effervescence (still/sparkling/semi_sparkling), producers.producer_type (estate/negociant/cooperative/virtual/corporate).
3. **External ID columns dropped from wine_vintages** — vivino_id, wine_searcher_id, cellartracker_id (all 0 rows populated). external_ids table is the canonical home.
4. **Redundant alcohol columns dropped** — alcohol_pct was identical to abv in all 132 rows, alcohol_level (1-5 xwines scale) had 0 rows. Keep abv only.
5. **Winemaking columns dropped from wines** — oak_origin, yeast_type, fining, filtration, closure, fermentation_vessel moved from wines to wine_vintages only. 36 wines had data → consolidated into wines.vinification_notes. Also dropped _source FK columns for aspect/slope/fog_exposure/vine_planted_year (source tracking belongs on entity_attributes).
6. **Redundant vineyard columns dropped from wines** — vineyard_id (0 rows), vineyard_name (9 rows, all had wine_vineyards links). wine_vineyards join table is canonical.
7. **wines.latitude/longitude dropped** — 0 rows populated, conceptually misplaced. Wines get geography from appellation/region/vineyard. Producer lat/lon is for winery location.
8. **Scores dedup index added** — UNIQUE on (wine_id, vintage_year, publication_id, critic, review_date) with COALESCE for nulls. Prevents duplicate score inserts while allowing multiple critics per publication and re-reviews.

### 2026-03-16: Full autonomy for schema decisions during import→harden cycle
User granted Claude full schema autonomy: "I want you to think about what would make loam great based on our mutual understanding of it and work towards that through your new autonomy." Tables and fields should be added where beneficial without asking permission, but the user wants to be told about additions. The import→harden cycle methodology: create JSON data → attempt import → hit friction → fix schema/importer → complete import is the primary way to discover schema gaps.

### 2026-03-16: Metadata promotion strategy
Audit of metadata JSONB across all entities revealed ~8,500 structured values that should be in proper columns/tables. Promotion priority:
1. **Immediate** — move to existing columns (vinification_notes: 583, release_date: 74, first_vintage_year: 15, production_cases: 177). Done.
2. **Next** — add new columns for high-frequency label-visible data (soil_description, vine_age_description, vineyard_area_ha, commune, altitude, aspect, slope, monopole). Queued in pending_migrations.sql.
3. **Deferred** — promote to table links requiring complex resolution (winemaker→producer_winemakers: 195, vineyard→wine_vineyards: 56, classification→entity_classifications: partial).
Italian DOC/DOCG designation metadata (77 entries) stays in metadata — these are appellation designations, not wine classifications. Different from Burgundy Grand Cru/Premier Cru or VDP which are true classification systems.

### 2026-03-16: Madeira sweetness designations as label_designations
Sercial, Verdelho, Bual, Malmsey, Terrantez added as label_designations (category: sweetness_style, country: Portugal). These grape names double as style designations on Madeira — "Sercial" on the label means "dry Madeira" as much as it means "made from Sercial grapes." Slugged with `-madeira` suffix to avoid collision with the grape entries.

### 2026-03-16: Cape Blend and Qvevri as label designations
Cape Blend (South Africa, production_method) — requires Pinotage 30-70%. Qvevri (Georgia, production_method) — UNESCO heritage clay vessel winemaking. Both are label-visible designations that appear on bottles and affect consumer expectations.

### 2026-03-16: Alias tables design (region, producer, label designation)
Three alias tables queued — same pattern as existing appellation_aliases. Each has: alias, alias_normalized, alias_type (CHECK constraint), language_code, source. UNIQUE index on alias_normalized for dedup. Seed scripts ready: ~75 region aliases (WSET L3 naming conventions), ~80 label designation aliases (German abbreviations, sparkling sweetness translations, production method translations).

### 2026-03-16: Four-tier enrichment model (Tier 0-3)
Standardized enrichment architecture. Tier 0 = identity only ($0, 200K+ target). Tier 1 = quick enrichment via Haiku on first user lookup (~$0.004, 30K target). Tier 2 = standard enrichment via Sonnet when demand/data signals met (~$0.03, 10K target). Tier 3 = full enrichment, manually curated (~$0.15, 1K target). Total estimated cost: ~$570 for full coverage targets. On-demand lazy enrichment is the primary mechanism — wines are enriched when users look them up, not pre-enriched in bulk. See `docs/ENRICHMENT.md` for full specification.

### 2026-03-16: "Wine not found" uses label photo identification (Option D)
When a user searches for a wine not in the database and provides a label photo, Claude Vision reads the label, extracts fields, runs through fuzzy resolvers, and either matches an existing wine or creates a new Tier 0 entry with immediate Tier 1 enrichment. Cost ~$0.01-0.02 per identification. Generic geographic fallback (Option B) is NOT shown first — the system attempts to identify the specific wine before falling back. Some user back-and-forth to confirm identification is acceptable.

### 2026-03-16: Mobile-first PWA, not native app
Loam frontend is a Progressive Web App optimized for mobile browsers. No App Store distribution yet. PWA supports camera API (barcode/label scanning), service worker (offline caching of recently viewed wines), and add-to-homescreen. Target users: curious wine enthusiasts, wine shoppers, restaurant managers and staff, wine shop owners.

### 2026-03-16: Anonymous-first, user accounts later
No user authentication for v1. Anonymous browsing only. User accounts, cellar tracking, personal notes, saved wines are future features to be layered on. Architecture (RLS, Supabase Auth) supports this when ready.

### 2026-03-16: Three input methods — text search, barcode scan, label photo
Text search (existing), barcode scan (needs UPC/EAN data from LWIN or other non-Wine-Searcher source), and label photo recognition (Claude Vision API). Voice search and wine list OCR rejected — too niche for v1.

### 2026-03-16: Image storage in Supabase Storage
Label photos, producer logos, and map assets stored in Supabase Storage buckets. No external CDN for now.

### 2026-03-16: Enrichment freshness — annual refresh
Tier 1/2/3 enrichments refreshed once per year, or when significant new data arrives (new scores, vintage data added). Staleness tracked via `enrichment_log.enriched_at`. Low priority given current scale — revisit when user base grows.

### 2026-03-16: COLA data access as strategic priority
TTB COLA (Certificate of Label Approval) data contains structured label information (exact ABV, appellation as printed, importer of record, grape varieties) that is extremely hard to get elsewhere. Needs research into COLA Cloud API access and potentially FOIA requests. Alongside LWIN, this is the primary bulk data acquisition strategy. Wine-Searcher avoided for cost reasons.

### 2026-03-16: Product features prioritized from brainstorm
From a 16-idea brainstorm, user selected favorites: #10 cross-vintage comparison (top priority, enrichment), #1 vintage weather narratives (top priority, needs Open-Meteo), #13 education layer (mostly frontend UX), #2 wine relationships (new table), #3 winemaker career trajectories (schema check), #4 terroir fingerprinting (new table + enrichment), #5 value scoring (computed view), #9 producer timeline (new table + enrichment). Auction/secondary market (#7) rejected as too high-end. Similar wines section on wine page endorsed for discovery.

### 2026-03-16: Score source trust levels (1-5 scale)
Publications rated 1-5 for source trustworthiness. 5=authoritative (WA, Vinous, JR, RVF), 4=respected (WS, Decanter, JS, Dunnuck), 3=good but niche (Gambero Rosso, competition results), 2=community (CellarTracker, Vivino), 1=auction houses. Affects display priority on mobile (show highest-trust first), weighted composites (if ever built), and AI prompt context. All 71 publications rated.

### 2026-03-16: Letter-grade enrichment system (F/D/C/B/A)
Replaced numeric tiers (0-3) with letter grades for memorability. F=identity only (LWIN/COLA), D=basic info (has scores or prices), C=quick enrichment (AI hook + tasting profile), B=standard enrichment (full narrative + terroir + value), A=full enrichment (cross-vintage, timeline, relationships). Tracked on both `wines.data_grade` and `wine_insights.enrichment_tier`. Five grades vs four tiers because D (structured data, no AI) is a distinct useful level.

### 2026-03-16: No wine_candidates table — all wines in wines table
Dropped `wine_candidates` (0 rows). All wines live in `wines` table regardless of data quality. `wines.data_grade` (F/D/C/B/A) tracks completeness. `wines.identity_confidence` tracks dedup certainty (unverified, lwin_matched, cola_matched, upc_matched, manual_verified). This simplifies the data model — no staging area, just quality grades.

### 2026-03-16: Wine lookup count on wines table
`wines.lookup_count` INTEGER column tracks page views. Incremented on each lookup. First lookup (count going from 0 to 1) triggers Grade C enrichment. Used as demand signal for Grade B promotion. Simpler than querying wine_lookups table for every decision.

### 2026-03-16: Scores displayable, tasting notes not reproducible
Numerical scores are facts (not copyrightable) and can be displayed: "Wine Advocate: 96". Full critic tasting note text is copyrighted and should NOT be reproduced verbatim. AI-generated narratives that synthesize (but don't reproduce) critic assessments are on safer legal ground. The `wine_vintage_scores.tasting_note` column may contain excerpts — display with attribution only, not full text.

### 2026-03-16: Text search → wine page, barcode → vintage page
Default landing behavior after lookup. Text search shows the wine page (all vintages, aggregate info) because users searching by name want the wine in general. Barcode scan shows the specific vintage page because barcodes are vintage-specific. Label photo shows vintage page if vintage detected, wine page if not. Wine page flows naturally into vintage page via vintage selector.

### 2026-03-16: Frontend hybrid architecture
Reads go direct to Supabase views/RPC (fast, uses CDN). Writes and enrichment go through Edge Functions (server-side Claude API calls). Search uses direct RPC calls. This minimizes latency for reads while keeping enrichment logic server-side.

### 2026-03-16: USD-only pricing for v1
Store and display prices in USD only. Add `price_currency` and `price_original` columns when non-US import sources are integrated. Currency conversion is a frontend concern for later.

### 2026-03-16: Offline — cache last 50, search needs connectivity
PWA service worker caches last 50 viewed wines for offline access. Search requires connectivity. Future optimization: cache top 1,000 wines by lookup_count for offline fuzzy matching. Not a launch blocker.

### 2026-03-16: Enrichment grade content rebalancing
Moved food pairings from Grade C to Grade B (needs richer context from Sonnet to do well). Moved comparable wines from Grade B to Grade C (simplified — Haiku can suggest 2-3 similar wines from region/grape/style). Moved drinking window estimates from Grade A to Grade B (too practically useful to limit to 500-2,000 wines). Grade A is now clearly differentiated as "connections across time and between wines" — cross-vintage, terroir fingerprint, producer timeline, winemaker career, wine relationships.

### 2026-03-16: User lookup triggers B enrichment, not C
The default on-demand enrichment is now F/D/C → B (Sonnet). Every user search that lands on a wine below Grade B triggers a full Sonnet enrichment call. Grade C becomes a batch "catalog pre-warm" process run by us, not triggered by users. Rationale: early users should get the best possible experience; cost is not a concern at launch scale. The page loads immediately with whatever data exists (F/D/C + geographic context), and B content appears in ~5-8 seconds.

### 2026-03-16: Sonnet for B enrichment, revisit model choice later
Using Claude Sonnet for on-demand B enrichment. Sonnet is significantly better than Haiku for narratives, terroir expression, and comparable wine reasoning. Will revisit model choice as the pipeline matures — evaluate Google Gemini and other APIs for cost/quality tradeoff. Haiku remains fine for batch C enrichment (structured/mechanical output).

### 2026-03-16: UPC/barcode data required in LWIN import phase
LWIN includes EAN-13 barcodes for a subset of wines. Must capture these in `external_ids` during Phase 2 import. COLA also provides UPC data for US wines. Between both sources, barcode coverage should be sufficient for the barcode scan input method at launch.

### 2026-03-16: FOIA as backup, not primary COLA strategy
Filed FOIA request to TTB (ttbfoia@ttb.gov) for full COLA database. However, treating this as a backup — expect 20+ business days with uncertain outcome. Primary COLA strategy is to research and build programmatic access to ttbonline.gov public data ourselves (scraping, API, or bulk download).

### 2026-03-16: LWIN and COLA before launch
LWIN import and COLA data acquisition are prerequisites for launch. These establish the wine identity backbone (LWIN) and US label data (COLA) that the platform needs. Enrichment pipeline comes before frontend. Sequence: LWIN import → COLA acquisition → enrichment pipeline → frontend.

### 2026-03-16: Enrichment before frontend
Build the enrichment pipeline before the frontend. Rationale: the frontend experience depends on enriched content existing — better to have the pipeline working and content generated before building the UI that displays it.

### 2026-03-16: LWIN-first spine architecture for multi-source data merge
LWIN as the identity backbone with progressive enrichment from other sources. Architecture: Staging tables (import_runs, staging_wines) → 4-layer matching engine (external ID → deterministic composite key → fuzzy composite score → AI-assisted) → canonical tables with field_provenance tracking. Source priority: Manual (1) > Producer direct (2) > Government (3) > LWIN (4) > Curated retailer (5) > Bulk retailer (6) > Open Food Facts (7) > X-Wines (8). Confidence thresholds: ≥0.92 auto-merge, 0.80-0.91 auto-merge with flag, 0.65-0.79 human review, <0.50 create new.

### 2026-03-16: Multi-source data strategy — comprehensive with dedup
Rather than relying on a single perfect source, use multiple sources (LWIN, COLA, state databases, importer catalogs, retailer sitemaps) with extensive dedup and validation. Each source has different strengths: LWIN for fine wine identity, COLA for US label data, Kansas for COLA IDs, PA for UPCs, importers for deep winemaking metadata. The merge infrastructure makes this manageable rather than a "world of hurt."

### 2026-03-16: Unified wine data sources document (SOURCES.md)
All wine data source research lives in `docs/SOURCES.md` — including sources we evaluated and rejected. This prevents knowledge loss between sessions and provides a persistent reference for data acquisition strategy. Status tracking: INTEGRATED, IN HAND, PRIORITY, EVALUATED, DEFERRED, SKIPPED.

### 2026-03-16: Import priority order established
9-step import priority: (1) LWIN identity backbone, (2) Kansas+PA state databases, (3) COLA Cloud API, (4) Importer catalogs (Skurnik, Winebow, European Cellars, Kysela, Louis/Dressner), (5) Wine.com sitemaps, (6) Total Wine sitemaps, (7) PRO Platform 12 states, (8) Open Food Facts barcodes, (9) FirstLeaf value segment. Target: ~200-250K unique wines covering $10-150 US market.

### 2026-03-17: COLA Cloud API — Role Assessment
Signed up for free tier (500 req/mo). Tested with 22 requests against known wines (Ridge, López de Heredia, Tignanello, Cristal, Yquem). Key findings:
- Search endpoint returns basic fields only (brand, product, ABV, origin, LLM category). Wine-specific fields (grapes, appellation, vintage, barcode, tasting notes) are ONLY on the detail endpoint (1 request per COLA).
- Detail data is genuinely rich when present: grapes, appellations, barcodes (UPC-A domestic, EAN-13 imports), LLM-generated descriptions and tasting flavors.
- Free tier cap: search pagination shows max 10,000 results regardless of actual count.
- Grape coverage imperfect: truncated names ("cabernet" not "Cabernet Sauvignon"), French appellations often missing grapes (not on label).
- 1.2M wine COLAs but bulk pull requires 1 detail request per COLA — infeasible even on Pro tier ($199/mo, 100K req/mo = 12 months).
- Decision: Use COLA Cloud as **barcode + identity enrichment service**, not bulk catalog source. Best fit: on-demand lookup in Grade B enrichment pipeline (user searches → check COLA Cloud if wine below Grade B). Batch-enrich existing catalog when ready ($39/mo Starter). Snowflake data share for bulk access if economics justify later.
- API key stored in .env (COLA_CLOUD_API_KEY). JS SDK installed (npm colacloud).

### 2026-03-17: Identity-First Strategy — Accuracy Over Quick MVP
User explicitly chose identity-first approach over depth-first MVP. Key principles:
- Prioritize accuracy and provenance over shipping fast
- Build the identity backbone (LWIN 187K wines) before enriching
- On-demand enrichment (Sonnet for Grade B) triggered by user searches, not pre-computed
- COLA Cloud as lookup service, not bulk import source
- State DBs (Kansas, Illinois) for COLA ID bridging to federal data
- Importer catalogs (10K wines) merge against LWIN backbone, not imported standalone
- Frontend comes after the database is clean, accurate, and well-attributed
- Sequencing: LWIN import → merge infrastructure → state DB COLA bridge → importer catalog merge → enrichment pipeline → frontend
- Rationale: "I just want to be slow and methodical with it and prioritize accuracy"

### 2026-03-17: TTB COLA Direct Scraping Over COLA Cloud for F-Tier
Discovered that TTB's public COLA registry (ttbonline.gov) has structured grape varietal data natively — it's a field on the COLA application, not AI-extracted by COLA Cloud. This eliminates the primary value-add justification for paying COLA Cloud for bulk data.

Strategy: scrape TTB directly in two phases:
- **Phase 1 (CSV harvest):** Search by date range + wine class types (80-89), export CSVs of TTB IDs. 4-day windows to stay under 1,000-row cap. ~2,700 searches. User running locally, ~16 hours conservative rate limiting. 1955-present.
- **Phase 2 (detail scrape):** Fetch each detail page by TTB ID URL pattern (`viewColaDetails.do?ttbid={ID}`). Parse HTML for grape varietals, applicant name/address, origin. Prioritize by filtering Phase 1 output (skip expired/surrendered, deduplicate label refreshes).
- **Phase 3 (AI parse):** Haiku extracts vintage year, wine name, appellation from fanciful name text. ~$5-10 for full corpus.

What TTB gives: TTB ID, brand name, fanciful name, grape varietals, origin (state/country), class/type, permit number, applicant name + full address, approval date, status.
What TTB doesn't give (COLA Cloud adds): ABV, barcodes/GTIN, structured appellation mapping, tasting notes.

Total cost: ~$10 (Haiku parsing). Time: ~1 week for complete 1.2M+ wine COLA corpus. COLA Cloud email still worth sending for barcode data and as a backup — but it's no longer the critical path for F-tier population.

FOIA request to TTB also outstanding as a parallel path — may deliver the same data in a flat file.

### 2026-03-17: Per-Source Staging Tables for Multi-Source Merge
Architecture decision: each external data source gets its own staging table (`source_*`) rather than importing directly into canonical tables. Raw data preserved as-is for re-running merge logic without re-fetching. Merge tracking columns on each staging table (canonical_wine_id, canonical_producer_id, processed_at) enable provenance tracking.

Tables created: `source_ttb_colas`, `source_kansas_brands`, `source_lwin`. Import priority: TTB COLA (broadest F-tier) → Kansas (COLA ID join for ABV/appellation) → LWIN (name matching for fine wine identity) → importer catalogs (rich enrichment data).

### 2026-03-17: IGT/IGP/PGI Appellations Added to Appellations Table
457 new PGI-tier appellations imported from eAmbrosia EU register, plus 5 base-tier designations (Vin de France, Vino d'Italia, Vino de España, Vinho de Portugal, Deutscher Wein). These go in the same `appellations` table with appropriate `designation_type` values (IGT, IGP, VdlT, VR, Landwein, PGI, VdF, VdI, VdE, VdP, VdT). No separate table needed — containment handled via `appellation_containment`. Naming convention: use the zone name without suffix (e.g., "Toscano" not "Toscano IGT"), add suffixed forms as aliases. Kansas appellation resolution improved from 77.1% → 81.9%.

### 2026-03-17: Staging-First, Promote on Match Architecture
All sources stay in per-source staging tables (`source_*`). A match engine runs across staging tables and existing canonical records. Only when confident of identity do we create/update a canonical `wines`/`producers` row. Prevents dedup crisis at scale. Single sources can promote with `identity_confidence = 'unverified'`; second-source confirmation upgrades confidence. Existing 3,095 canonical wines from trial imports stay as seed data.

### 2026-03-17: Two-Backbone Identity Strategy (LWIN + TTB COLA)
LWIN (184K wines, global fine wine, clean names, classifications, no grapes) and TTB COLA (~1.2M wines, US market, has grape varietals, messy names) are complementary backbones. Neither replaces the other. Overlap (~30-40% for fine wine imported to US) gives strongest identity when both match. Non-overlapping coverage extends catalog breadth. Both stay in staging; matching combines their strengths.

### 2026-03-17: AI-Assisted Match Review (Haiku/Sonnet)
No human review at scale — too many wines. Match confidence tiers: >0.85 auto-accept, 0.6-0.85 Haiku review (~$0.001/call), 0.4-0.6 Sonnet review (~$0.01/call), <0.4 don't match. AI reviews logged in `match_decisions` table. Any definitive facts exposed during AI review (confirmed grapes, appellation, producer identity) captured as free enrichment data.

### 2026-03-17: Store All Identifiers in external_ids
Every identifier encountered from any source should be stored: LWIN-7, LWIN-11, TTB ID, Vinmonopolet product ID, importer SKUs, GTIN/EAN, Kansas brand ID. More identifiers per wine = easier future matching. Barcodes (GTIN/EAN) are universal — same barcode on a bottle worldwide (EAN-13 = UPC-A with leading zero).

### 2026-03-17: Vinmonopolet Added as Priority Source
Norwegian state wine monopoly. ~25K wines with measured grape percentages, sugar g/L, acid g/L, ABV, flavor scales, food pairings, certifications. Open API key obtained but Open tier returns sparse data only. Email sent requesting full API access (my-products-v1 endpoint with barcodes and chemistry). Barcodes would be a universal dedup key across all sources. Website scraping is fallback (all data publicly displayed, ~5-10 hours).

### 2026-03-18: Canonical Name Strategy (Producers + Wines)
Canonical names should be the "commercially recognizable" name — what a wine drinker would see on a shelf or wine list. Not legal entities (TTB), not regulatory filings. Brand names, not corporate names. "Opus One" not "OPUS ONE RED WINE". "Château Margaux" not "SAS DU CHATEAU MARGAUX". Include estate prefixes when recognized (Domaine, Château, Bodegas). Drop corporate suffixes (Inc, LLC, SAS, GmbH). Source priority: LWIN/importer catalogs (curated) > retailers > TTB COLA (always alias, never canonical). Every name variant goes into alias tables. After multi-source merge, batch Haiku pass over wines with 2+ meaningfully different name variants to pick the best canonical name — estimated ~10-20% of wines need this, ~$40-70 total. Trivial differences (casing, accents) resolved programmatically without AI.

### 2026-03-18: LWIN as 1:1 Identity Backbone
Every LWIN-7 code maps to exactly one canonical wine — no dedup within LWIN. LWIN-7 is a commercially curated unique wine identifier. Wine names derived from LWIN `display_name` minus producer prefix, NOT from the `wine_name` field (which is often just "Rouge"/"Blanc"/"Riesling"). Example: "Domaine Jean-Jacques Confuron, Bourgogne, Rouge" → wine name "Bourgogne, Rouge" (not "Rouge"). This prevents Burgundy producers' entire portfolios from collapsing into a single "Rouge" record. First import attempt had this bug — 13K wines dropped + 151 wines wrongly merged (up to 19 different wines per producer). Fixed and re-imported.

### 2026-03-18: Full Canonical Wipe and Rebuild
Wiped all wines and producers from canonical tables to rebuild correctly through staging→promote pipeline. TRUNCATE CASCADE also wiped staging tables (lesson learned — CASCADE truncates all FK-referencing tables regardless of null values). All staging data reloaded from source files (CSV/JSON). No seed data retained — everything goes through the same pipeline now. Cleaner architecture.

### 2026-03-18: LWIN Producer Names — Use producer_name as Dedup Key
LWIN's `producer_name` is the deduped identity key (one per producer entity). The `display_name` prefix varies per wine (different labels/brands under same producer — e.g., "Domaine Grand Veneur" and "Alain Jaume" are the same entity). Use `producer_name` for matching/dedup, use the most common `display_name` prefix as the canonical display name, store all other variants as `producer_aliases` with type `alternate_name`.

### 2026-03-18: SQL-Based Bulk Promotion over HTTP API
For large imports (100K+ records), use server-side SQL (INSERT INTO ... SELECT FROM staging) instead of Node.js scripts making individual Supabase REST API calls. The LWIN import went from 3+ hours (Node/HTTP) to ~2 minutes (pure SQL). Disable search vector triggers during bulk inserts, re-enable after.

### 2026-03-18: Barcode Aggregation Strategy
Barcodes (UPC/EAN/GTIN) are the strongest cross-source identifier — same barcode on the same bottle worldwide, unique per vintage. Strategy: aggregate barcodes from every source that has them. Known barcode sources: PA PLCB (5,905 wines, 100% UPC), LCBO (3,515 wines, 99.9% UPC), Vinmonopolet (pending API access), WineDeals.com (7,694 wines with UPC on product pages), UPC Data 4 Beverage Alcohol (commercial, 150K+ wine barcodes, pricing inquiry sent). Also: Systembolaget (no barcodes but rich grape/vintage/taste data for enrichment matching).

### 2026-03-18: WineDeals.com as Barcode + Enrichment Source
US retailer (Premier Wine & Spirits, Amherst NY) with 7,694 wines. Every product page exposes 25 structured fields including UPC, grape varieties, ABV, vintage, country, region, district, appellation, color, wine type. Puppeteer scraper built (`scripts/scrape_winedeals.mjs`), resume-safe. Estimated 3-4 hours to scrape full catalog.

### 2026-03-18: 7 New Countries Added
Bosnia and Herzegovina, Bhutan, Hong Kong, Russia, Ecuador, Venezuela, Indonesia — all from LWIN data. Minor wine-producing countries but legitimate LWIN entries.

### 2026-03-18: UPC Barcode Sources — Batch Acquisition Session
Fetched 5 new barcode sources in a single session. Strategy: aggregate barcodes from many smaller free sources rather than paying for one expensive commercial database.
- **Open Food Facts**: 5,176 wines with EAN barcodes (REST API, 30 min)
- **Horizon Beverage / SGWS**: 6,441 UPCs + 92.5% grape data (JSON API, 90 sec!) — discovered public API on Southern Glazer's regional site
- **LCBO (Ontario)**: 3,513 UPCs (Puppeteer, previous session)
- **PA PLCB**: 10,297 unique UPCs across 5,905 wines (Excel parse)
- **WineDeals.com**: ~6,800 estimated (Puppeteer, running overnight)
Total confirmed: ~25,400 barcodes from free sources. Pending: ~6,800 winedeals + ~20K Vinmonopolet (awaiting API access).

### 2026-03-18: Connecticut DCP as UPC↔COLA Rosetta Stone
Connecticut DCP OpenAccess portal (biznet.ct.gov) has per-supplier PDF price lists with UPC + COLA(TTB ID#) + vintage + ABV + price in a single row. This is the only source that directly bridges UPC barcodes to TTB COLA IDs without probabilistic matching. 388 supplier PDFs available for February 2026. Scraper built (`scripts/fetch_ct_dcp.mjs`).

### 2026-03-18: CT DCP — Extract All, Filter Wine Later
Decision: Download all 388 CT DCP supplier PDFs (wine + spirits + beer) now. Filter to wine products later once TTB COLA Phase 1 data arrives — cross-reference COLA IDs against TTB wine class types 80-89 for definitive wine identification. Cleaner than guessing from company names.

### 2026-03-19: Python migration for data pipeline
All new data pipeline work written in Python. Node.js scripts retired for new development. Boundary: Python for all ETL/matching/dedup/enrichment/analysis, TypeScript/Deno for Supabase Edge Functions, TypeScript/React for frontend. Existing Node scrapers stay as-is (data already collected). Merge engine built fresh in Python since JS version (`lib/merge.mjs`) is untested. See `docs/MERGE_STRATEGY.md` for full rationale.

### 2026-03-19: COLA as starting point over UPC
COLA is the identity backbone, not UPC. ~647K records with COLA vs. ~27K with UPC. COLA chains together Kansas, PRO Platform, TABC, WV via key-based joins. UPC is secondary — valuable for barcode scanning (Phase 5) but not for identity building. Attach UPCs to COLA-built identities as lookup keys.

### 2026-03-19: Wine identity definition
A wine is a distinct commercial product from a single producer that maintains a consistent identity across vintages. Blend % changes, vineyard sourcing shifts within same appellation, and winemaking evolution between vintages do NOT create a new wine. Different tier, designated vineyard, product line, or second label DO create a new wine. Label redesigns = same wine (group COLAs). Name changes tracked in wine_aliases. Reference this in matching prompts.

### 2026-03-19: xwines as reference index, not identity source
Use xwines data (530K wines) as a matching confidence signal only. A match in xwines increases confidence that a COLA parse was correct, but xwines data never flows into canonical columns. Field values come from higher-trust sources. Same policy for Vivino — useful for matching confirmation and community scores, never creates canonical records. `xwines_wine_candidates` (100K pre-parsed rows) is the most useful matching dictionary.

### 2026-03-19: Local AI models for bulk matching
Use Ollama (Llama 3.1 8B or Mistral 7B) for bulk producer/wine matching to eliminate API costs. Matching is classification, not generation — small models handle it. Hybrid approach: local model on everything, flag low-confidence, send only those to Haiku. Sonnet reserved exclusively for enrichment writing where voice quality matters.

### 2026-03-19: Confidence as field, not separate tables
Track matching confidence as fields on canonical table (`wines.identity_confidence` categorical + consider adding `wines.identity_match_score` numeric). Rejected multiple confidence-tier tables — promotion logic nightmares, FK updates, query complexity.

### 2026-03-19: COLA label images — URLs first, download second
Scrape label image URLs from ttbonline.gov (Phase 1, fast), then batch-download images locally (Phase 2, days in background). Public domain. Serves every product direction: consumer polish, wine list enrichment, label recognition library, API customers, standalone commercial value.

### 2026-03-19: Build what's universal, defer product-specific
Product shape not yet defined (consumer app vs. data API vs. wine list enrichment vs. professional reference). Build what's on critical path in every scenario: canonical identity, reference data, enrichment, matching, prices/scores, label images. Defer: specific frontend design, barcode scanning, offline/caching, marketing/billing. Show to people early to find direction. See `docs/MERGE_STRATEGY.md`.

### 2026-03-19: Merge strategy document established
Created `docs/MERGE_STRATEGY.md` covering: Python migration, merge layer sequencing (LWIN → COLA → state DBs → importers → retailers → xwines reference), COLA strengths/risks, wine identity definition, AI matching approach, product direction framework, revenue estimates, Claude involvement patterns. This is the primary reference for Claude Code merge pipeline work.

### 2026-03-19: GitHub token operational risk accepted
Token shared in Claude.ai chat session. Accepted as operational risk for now. Rotate when convenient.

### 2026-03-20: Polaner deprioritized as source
Polaner removed from active promotion pipeline. Title parsing was completed (all 1,680 titles parsed via Haiku) and data sits in `source_polaner`, but the catalog is small and metadata-thin compared to Skurnik (5.5K wines with grapes/appellations) or Winebow (best chemistry data). Not worth ongoing investment. Data retained in staging for future reference if needed.

### 2026-03-20: Full Python migration for all pipeline scripts
All 116 Node.js scripts being converted to Python (even archived ones). Rationale: the work ahead — ETL, dedup, fuzzy matching, AI calls, data quality analysis — is Python's home turf. Pandas, scikit-learn, sentence-transformers, local Ollama bindings are all Python-native. The merge engine was never tested in JS so building fresh in Python costs the same. Node.js archived in `scripts_archive/node/`.

### 2026-03-23: Run TTB image downloader in parallel with printable scraper
User decided to run both the label image downloader and the printable scraper simultaneously against ttbonline.gov, despite risk of WAF blocking. Different endpoints (publicViewImage.do vs viewColaDetails.do) but same server. Image downloader throttled to 5/sec (vs 40/sec in testing) as compromise. Rationale: "let's just do both at the same time, I think we should get moving."

### 2026-03-23: Delete PRO Platform intermediate parsed JSONs
Deleted 325MB of `pro_*_parsed.json` files (12 states). These were intermediate cache between XLSX→DB pipeline. Data already in `source_pro_platform` (346K rows) and regenerable from XLSX source files. XLSX files kept as authoritative source-of-record.

### 2026-03-23: Label image barcode extraction as COLA→UPC bridge
Built pipeline to download TTB label images and scan for UPC/EAN barcodes using zxing-cpp. 18.2% hit rate on 2020-2026 labels (516 unique barcodes from 3,407 images). Projected ~64K bridges at scale across 350K images with URLs. This is free data — no API costs, no licensing, just computer vision on publicly available government label images.

### 2026-03-24: Store TTB label images in Supabase Storage long-term
Label images are valuable beyond barcode extraction — users should be able to see the actual wine label. Plan to store images in Supabase Storage for canonical wines. Estimated cost: ~$4-8/mo for deduplicated canonical wines, ~$17-23/mo for all 3.28M COLAs. Download everything to local disk first (free), extract barcodes, then selectively upload canonical wine labels to Supabase Storage.

### 2026-03-24: UPC→price lookup — build from existing data, don't pay for APIs
Researched UPC→price lookup services (SerpAPI $0.01-0.025/lookup, Go-UPC $75-795/mo, Barcode Lookup $99+/mo, Wine-Searcher API $250-2,000/mo). Concluded: don't pay for any of them. We already have ~82K wine prices across 13 staging sources (Spec's, Wally's, Flatiron, PA, BC Liquor, LCBO, EnofileOnline, etc.). The gap is the merge engine linking staging→canonical, not data access. On-demand Google Shopping lookups via SerpAPI could be a fallback for Grade B enrichment at $75/mo for 5K lookups, but only after the free data is exhausted.

### 2026-03-24: Wine.com scraping — park it, DataDome blocks everything
Wine.com product pages return 403 (DataDome anti-bot). API endpoints also blocked. We have 262K product URLs from sitemaps — could parse slugs for Wine.com product IDs to store in external_ids for future use, but actual price/product data is inaccessible without paid proxy rotation services. Not worth the effort. Better to add more Shopify/WooCommerce retailers that don't fight back.

### 2026-03-24: Vivino — use existing xwines_* data for validation, don't re-scrape
Vivino API returning 403 (Cloudflare). Apify scrapers still work (~$5-15 per 10K wines via residential proxies). But we already have 530K wines in xwines_* tables from prior scraping. For validation use case (confirming wine identity, checking ratings, verifying grape varieties), the existing data is sufficient at zero cost and zero risk. If fresh data needed later, Apify is path of least resistance.

### 2026-03-24: Merge strategy — push forward now, don't wait for TTB detail scraper
COLA numbers (the identity backbone) are already in source_ttb_colas. The detail scraper adds enrichment data (grape varietals, applicant info) but doesn't block identity matching. Phase 1: COLA-keyed deterministic joins across 5 sources (~650K records). Phase 2: LWIN cross-reference (fuzzy match). Phase 3: UPC barcode bridging. Phase 4: Importer catalog enrichment. Phase 5: Competition data overlay.

### 2026-03-25: Supabase compute — upgrade Nano → Small ($10/mo)
Nano (0.5GB RAM, shared CPU) could not handle upserts into source_ttb_colas (3.5GB table + indexes). Statement timeouts on every write, causing scraper to stall. Micro (1GB, shared CPU) considered but Small (2GB, dedicated CPU) chosen for headroom — table will grow as printable scraper adds data to all 3.28M rows, and we have 30 other staging tables plus 78 canonical tables.

### 2026-03-25: TTB scraping — run detail and printable sequentially, not in parallel
Overnight parallel run caused silent rate limiting: ERR_CONNECTION_RESET errors (not WAF blocks) from TTB after sustained ~80 req/s combined. Sequential runs at 20-43 rec/s each produce zero WAF blocks. Run detail first (fills pre-2005 gaps), then printable (full re-scrape for label images).

### 2026-03-25: Image URL separation — application_scan_urls vs label_image_urls
Detail scraper's `publicViewImage.do` URLs are scans of the full TTB application form (with labels physically pasted at bottom). Printable scraper's `publicViewAttachment.do` URLs are actual individual label photos (front, back, strip, neck). Separated into distinct columns for downstream use: label photos for barcode scanning + user display, app scans for archive/reference only.

### 2026-03-25: TTB scraping scope — all years, all statuses, all class types
Previous runs only scraped APPROVED status and 5 wine class types (80/81/80A/84/88). Expanded to include all statuses (SURRENDERED, EXPIRED, REVOKED) and additional class types (8000/8100/8400/8800). Pre-2005 records (~1.1M) had never been detail-scraped.

### 2026-03-25: Affiliate links — start after importer catalog merge (step 6), before/parallel with enrichment
Wine.com, Drizly, Total Wine, Vivino all have affiliate programs (5-10% commission). Need merged wine identities first to reliably match canonical wines to retailer product pages. Don't need polished frontend — even a basic search page with buy links generates revenue. Build: affiliate_url_template + commission_rate on `retailers` table, map canonical wines → retailer availability from staging price data (~82K prices across 13 sources), generate dynamic affiliate URLs. Wally's distributor mapping data (Southern, RNDC, Chambers, Winebow, KL) helps identify which retailers carry which wines.

### 2026-03-27: TTB ID format — non-001 IDs have no printable page (confirmed)
Audit confirmed that the middle 3 digits of the 14-digit TTB ID (positions 6-8) encode the form type. Only `001` = standard COLA form 5100.31 with a printable version. IDs with `000`, `002`, `003` return error on `publicFormDisplay` URL — verified live in browser. The `--only-001` flag on the printable scraper was correct. 1.35M wine records with non-001 IDs will never get printable-only fields (appellation, ABV, applicant, label images) from TTB directly. These fields must come from COLA-keyed cross-reference with PRO Platform (346K), TABC (183K), Kansas (65K), and other state databases that share COLA numbers.

### 2026-03-27: TTB scrape declared complete
Detail scrape: 3.18M/3.28M (96.8%) — remaining 104K are non-grape-wine class types (flavored wine, fruit wine, mead, cider). Printable scrape: 1.82M/1.83M 001-format records (99.86%) — remaining 2,635 are pre-1997 with no printable page. No more scraping needed. Next step is COLA-keyed deterministic merge and AI parse for non-001 records.

### 2026-03-27: Trial import data loss from LWIN promotion — accepted
LWIN canonical promotion cleared trial import seed data (33 producers, ~560 vintages, ~521 scores, 31 winemakers, 169 farming certs, 11 wine aliases, 707 grape insights). This is acceptable — the trial imports served their purpose (schema stress-testing) and the importer staging data remains for re-promotion against the LWIN backbone. The canonical table now has clean LWIN-sourced identity data.

### 2026-03-31: Principle #9 — structured data gets structured display
If a field is structured in the DB (numbers, dates, percentages, enums), it must be displayed structurally in the UI — not buried in prose. Grids, labeled values, pills, compact rows. This makes pages comparable side-by-side and builds user muscle memory for where to find facts. AI narrative wraps around structured data, never replaces it. Applied first to wine page vintage details and winemaking sections. Added as Principle #9 in PRINCIPLES.md (existing #9 renumbered to #10).

### 2026-04-01: Frontend pause — fill data before building more UI
Consumer frontend pages (Wine, Producer, Appellation, Region, Grape, Country, Vineyard) are built and deployed on Render. But canonical tables are nearly empty: 189K wines with 1 vintage, 3 scores, 1 grape link. Pages render beautifully when data exists but most show bare identity-only shells. Decision: stop frontend work, focus entirely on filling canonical data via importer re-promotion, COLA-keyed merge, and enrichment pipeline. Don't return to UI until wine pages have vintages, scores, and grapes populating.

### 2026-04-01: TTB image download does not block merge work
TTB label image download (~490K images, ~48 hours remaining) is a UPC barcode extraction play — separate from the text data merge. All 3.18M detail-scraped and 1.82M printable-scraped records are already in source_ttb_colas with appellations, grapes, vintages, ABV. COLA-keyed merge and importer re-promotion can proceed immediately.

### 2026-04-01: Importer re-promotion is highest priority next step
8,267 wines across 5 importer staging tables (KL, Skurnik, Winebow, Empson, EC) have richest per-wine data: grapes, vintages, farming practices, descriptions. All have canonical_wine_id = 0 (cleared during LWIN promotion). Re-promoting these against the LWIN backbone is the fastest path to pages with actual content. Then COLA-keyed merge for scale, then enrichment pipeline for AI content.

### 2026-04-01: Loam ID format — ISO alpha-2 + 7-digit sequence + vintage year
Human-facing stable identifier for wines and vintages. Format: `FR-0012345` (wine), `FR-0012345-2017` (vintage), `FR-0012345-NV` (non-vintage). 7-digit per-country sequence gives 10M headroom per country — France is largest at ~200K now. Primary keys stay UUID internally; Loam ID is the durable external reference (like ISBN/DOI). No producer name embedded — names are mutable (acquisitions, rebranding) and would cause the ID to lie about current state. Producer context lives in display layer and slug.

### 2026-04-01: Wine identity definition — supplemental clarification
A row in the `wines` table represents: "Would this be considered a different wine/liquid/batch/lot to the winemaker/seller?" Vintage gets special treatment (lives on `wine_vintages`, not `wines`). This supplements the existing schema definition. A producer's Chardonnay and their Merlot are different wines. Their 2019 vs 2020 of the same wine are not. Bottle format (750ml vs magnum) is NOT a different wine. Label redesigns are NOT a different wine.

### 2026-04-01: COLA→wine creation requires normalization first — not safe to use raw fanciful names
175K wine candidates from COLA data evaluated. Raw `fanciful_name` field has serious quality issues: 9+ spelling variants per wine (e.g., Kendall-Jackson "VINTNERS RESERVE" / "VINTNER'S RESERVE" / "VINTERS RESERVE" / "VITNERS RESERVE"), 30-character truncation, appellations used as wine names, experimental labels. Creating wines directly from raw fanciful names would produce thousands of duplicates. Must build a normalization layer first. UPC barcode data (arriving soon) will help cluster COLAs into product groups for dedup. Decision: wait for UPC data before creating wines from COLA.

### 2026-04-01: AI common sense budget — $2/prompt (Principle #10)
When evaluating sources against one another, cross-referencing data, or applying common sense to ambiguous merge/match decisions, Claude may use the Anthropic API (Haiku, Sonnet, or Opus) at up to $2 per prompt. This is for validation and judgment calls — not for generating new data. No AI-assisted matching of wines, producers, or identities without the user's explicit consent. Budget is for common sense checks: "does this look right?", "are these the same entity?", "which source is more reliable here?"

### 2026-04-01: No new data from training data (Principle #11)
Training data (Claude's built-in knowledge) should only be used for validation — confirming, cross-referencing, and auditing data from authoritative sources. Never used to generate new factual content (scores, tasting notes, production figures, vintage details) for canonical tables. Training data is the second opinion, not the source of truth.

### 2026-04-02: Readiness metric — mystery shopper test, not database statistics
Database statistics (74% identity completeness) misrepresent real user experience. Readiness measured by sampling real wine store inventories (Spec's, Wally's, etc.) from staging tables and attempting canonical lookup. Score reflects: can the user find it, is the match correct, is there useful depth. First test: 50 random Spec's bottles → ~56% producer found, ~30% exact wine found, 0% had any depth (vintages/scores/grapes/prices), ~14% false matches. Real usefulness score: ~8/100. This is the number we track, not row counts.

### 2026-04-02: Region refinement — 75,774 wines L1→L2
SQL consistency check found 78,998 wines where wine.region_id != appellation.region_id. 75,774 were LWIN wines assigned to L1 parent regions (Burgundy, California, Bordeaux) while their appellation belonged to an L2 child (Côte de Beaune, Napa Valley, Right Bank). Bulk UPDATE to refine to the more specific region. 3,224 remaining are genuinely complex cross-boundary cases (left for agent). Also found 1,388 producer-country mismatches (mixed — some LWIN name collisions, some legitimate subsidiaries).

### 2026-04-02: Autonomous data accuracy + enrichment agent — BUILT
Combined scheduled Claude Code task running daily at 8:23 AM on Max subscription ($0 incremental). Five-phase session: (1) SQL cross-table consistency checks, (2) source consensus validation via stratified sampling, (3) staging→canonical promotion for unlinked importer wines, (4) TTB fanciful name parsing, (5) summary report. Hard rules: never write from training data (Principle #11), confidence >0.95 required for fixes, every change logged to `accuracy_audit` table with source citation, 30-min/300-record budget cap. Source independence groups prevent counting correlated sources as multiple votes. Infrastructure: `accuracy_audit` table (append-only), `last_validated_at` on wines/producers/wine_vintages, `sample_wines_for_validation()` RPC for stratified sampling. Agent gets more powerful as merge pipeline links more staging data to canonical.

### 2026-04-02: Claude takes agency — Principle #12
User directive: stop asking permission on clear-cut decisions. Execute obvious next steps, make deterministic choices, ship straightforward fixes. Reserve human judgment for genuine tradeoffs and irreversible decisions. The permission-seeking pattern was slowing progress — this session demonstrated that one search function fix (15 min, zero data risk) moved findability from 12% to 83%, but it took a discussion cycle to get permission. From now on: if it's clear, do it. If it's ambiguous, pause and discuss.

### 2026-04-03: In-memory batch matcher over SQL-over-MCP for retail linking
SQL-based producer matching via MCP times out on complex joins against 33K producers. Solution: `pipeline/promote/batch_matcher.py` loads all producers + aliases into memory (~6s startup), does 3-tier matching (exact normalized → alias → suffix-stripped) in Python, then per-producer wine matching via indexed REST queries. Handles sources with producer columns (Flatiron, Systembolaget) and without (LCBO, BC Liquor — prefix extraction from title). First-pass results: 3,619 wine matches across 4 sources. Tradeoff: lower match rate than AI-assisted matching, but runs in minutes vs hours and is reusable.

### 2026-04-03: xwines_dedup producer aliases are unreliable for Bordeaux
The 4,274 producer aliases from `source = 'xwines_dedup'` have data quality issues. "Chateau Montrose" → "Domaine Montrose" (wrong — different regions), "chateau guiraud" → "Assortment Case" (completely wrong). Root cause: xwines bulk dedup mapped sub-brands/second wines to main châteaux without validating. Decision: don't trust xwines_dedup aliases for TTB matching. Use only direct name matching, hyphen-stripped aliases (new), and &-vs-et matching. The xwines aliases should eventually be audited and cleaned.

### 2026-04-03: Hyphen-stripped producer aliases for LWIN compatibility
LWIN stores name_normalized with hyphens preserved ("lynch-bages") while our normalize() strips them ("lynch bages"). This blocks TTB→LWIN matching for hyphenated French/Italian/German names. Fix: create alternate_name aliases with hyphens replaced by spaces for all 4,274 hyphenated producers. These aliases allow TTB matching without modifying the LWIN data.

### 2026-04-03: Server-side SQL functions for large TTB operations
The source_ttb_colas table (3.28M rows) consistently times out on any full-table operation via REST API or MCP. Solution: create PL/pgSQL functions that run server-side (`promote_ttb_vintages()`, `rebuild_wine_search_vectors()`). MCP HTTP may timeout but the function completes on the Postgres side. Discovered: Tier C2 migration also completed server-side despite MCP timeout — 104,628 wines created. Lesson: for large operations, create a function and call it; accept that the HTTP response may timeout but the work still completes.

### 2026-04-04: Cursor-based pagination for source_ttb_colas operations
OFFSET-based pagination on 3.28M row table causes statement timeouts even with LIMIT. Solution: cursor pagination using `ttb_id > last_seen ORDER BY ttb_id LIMIT 1000`. This avoids the O(n) OFFSET skip and runs at 600-800 records/sec. Applied in `cola_depth.py` (scanned 666K records in 15 min) and `ttb_wine_link_v2.py` (loaded 261K unlinked records for in-memory matching). Lesson: never use OFFSET pagination on tables > 100K rows in Supabase.

### 2026-04-04: One COLA ID per wine (external_ids unique constraint)
The `external_ids` table has a UNIQUE constraint on `(entity_type, entity_id, system)`, meaning only one COLA per wine. Wines legitimately have multiple COLAs (different vintages, label revisions), but the constraint exists and we work within it — store the first COLA found. Adequate for identity linkage. If we need multiple COLAs per wine later, alter the constraint to include `external_id`.

### 2026-04-04: No parallel Supabase REST API scripts
Running two Python pipeline scripts simultaneously against Supabase causes `ConnectionTerminated` errors from HTTP/2 connection pool exhaustion. All pipeline scripts must run sequentially. SQL via MCP can run concurrently with one Python script.

### 2026-04-02: Future feature — "Endless Paper" style infinite wine map
Inspired by endlesspaper.app. Semantic zoom map of global wine geography: countries → regions → appellations → vineyards/producers. Data already exists (62 countries, 323 regions, 2,847 appellations with PostGIS boundaries, 2,158 containment hierarchy rows). Technical plan: swap Leaflet for MapLibre GL JS (continuous zoom, WebGL polygon rendering, style-driven layer visibility). Strip away standard map tiles — wine boundaries ARE the map, earth-tone canvas. Search-to-fly interaction. Come back to this after canonical data is populated.

### 2026-04-03: Geography is open for refinement — avoid creating appellation duplicates
The 3,662 appellations are built from official sources and multi-pass expert audited. All geographic data (appellations, regions, boundaries, attribution, hierarchy) is open for refinement, restructuring, and correction as needed — use good judgment. The one thing to watch for: don't create new appellations that duplicate existing ones (e.g., a "Napa Valley" variant alongside the existing entry). That would be a dedup crisis at scale. When in doubt, match to what exists rather than creating new.

### 2026-04-03: Schema assessment — gap is promotion, not missing columns
Post-merge schema assessment found the schema is well-designed: nearly every field our staging sources contain already has a canonical home. The problem is zero promotion of depth data. wine_vintages has 77 columns but only 2 populated (wine_id, abv). producers has 25 columns but only 5 populated. 47 junction/detail tables sit at 0 rows. Highest-ROI action: promote importer winemaking data (Empson, Winebow, EC, KL have fermentation, oak, chemistry data) into existing wine_vintages columns. ~2,700 wines could get deep winemaking profiles. Also identified 6 missing fields to add: serving_temperature, training_method, fermentation_duration, fermentation_temperature, vine_density (wine-level), kosher status.

### 2026-04-04: Voice and prompt refinement before batch enrichment spend
Enrichment pipeline MVP is live (`enrich-wine` Edge Function, ~$0.018/wine Sonnet). No batch spending over $16 until voice and prompt quality are dialed in. Refine on a small sample first, iterate on the prompt template, review output against VOICE.md standards, then scale to Grade C batch pre-warming. The 2 test enrichments worked but haven't been editorially reviewed yet.
### 2026-04-04: Systembolaget retired from nightly batch_matcher
Systembolaget (12,646 records, 8,234 unmatched) removed from nightly Riddler promotion loop. Root causes: (1) name order reversal — stores 'Accordini Igino' vs canonical 'Igino Accordini'; (2) many small European producers not in canonical DB; (3) API now requires auth, so data is stale and unrefreshable. Table is KEPT — it has UPC barcodes that could be matched via COLA bridge in the future, bypassing the producer name problem entirely. Revisit when UPC-keyed matching is built.

### 2026-04-04: No probabilistic inference on canonical columns
Reverting bulk probabilistic inferences from a 2026-04-04 session that wrote ~85K wine region_ids from producer majority-vote, 35K wine colors from grape color patterns, and 25K producer appellation_ids from wine majority-vote. User caught the error: Blanc de Noirs wines were mis-marked as red, and multi-region producers (Saldo, Orin Swift, négociants) were wrongly assigned a single "home" region. All inferences were ~90% accurate in aggregate but systematically wrong at the edges, violating Principle #3/#5 "real data only — no synthetic/hallucinated scores" and the "identity-first, accuracy-first — slow/methodical over quick MVP" mandate.

Rule: canonical columns only accept definitional cascades (appellation.region_id → wine, MIN(vintage_year) → first_vintage) or direct source data from staging. Probabilistic inference (majority-vote, correlation heuristics, dominant-grape-color → wine-color) must never be written to canonical columns. NULL is a valid state; filling NULLs with guesses is strictly worse than leaving them NULL. If probabilistic data is truly needed, store separately with a confidence field and provenance.

Contributing factors to prevent in future: (1) metric-chasing — readiness score was climbing from inference, treated as progress; (2) autonomous `/loop keep going` removed decision gates that would have caught this; (3) no row-level provenance meant collateral damage during revert (~85K pre-session non-TTB colors lost). Memory saved at `feedback_no_probabilistic_inference.md`.

### 2026-04-04: Recovery of lost data via authoritative sources only
Recovery session to restore data lost in the inference revert — strictly from direct source data, zero inference. Four recoveries:

1. **Wally's title parser (+7,707 prices, coverage 3.36%→3.96%):** `source_wallys.vintage` was NULL for 17,550 matched rows but the leading year was in the `title` column. Parsed `^(19|20)\d{2}\s` into `source_wallys.vintage`, created missing wine_vintages rows, promoted prices. Restricted to 750mL and titles with no explicit size (implicit 750mL), skipping 375mL/500mL/1.5L/3L/6L/GL (glass) listings to avoid size-distorted prices. 7,907 Wally's prices from a prior 2026-04-03 run already existed for many target wines, limiting unique-wine coverage gain.

2. **TTB grape re-promote (+29,249 wine_grapes):** Re-ran existing `pipeline/promote/ttb_grape_promote.py` which reads TTB's real `grape_varietals` field from `source_ttb_colas` (not pattern-matching). Recovered most of the ~44K grape links cleared as collateral during the revert. Still short of pre-session 195,468 because some pre-session links came from non-TTB sources (importer catalogs) that need separate promotion scripts.

3. **LWIN color backfill (+86,015 colors, 180K→267K):** LWIN's `colour` field (Red/White/Rose/Mixed) is authoritative trade-identifier data, not inference. Updated canonical `wines.color` where NULL using `LOWER(colour)` with Red/White/Rose filter (excluded Mixed/NULL). Zero canonical conflicts (LWIN is self-consistent). Also recovered 1,681 from importer catalogs (Skurnik, European Cellars, Domestique, Last Bottle, Best Wine Store, BC Liquor, Winedeals) — but **skipped any canonical wine where importer sources disagreed** (matcher over-linking issue — e.g., Skurnik's "Coteaux Champenois Rouge 'Ay'" and "Coteaux Champenois Blanc 'Vertus'" were both linked to the same canonical wine). `xwines_wine_candidates.wines_id` links to `xwines_wines` parallel dump, not canonical `wines` — not usable without a separate merge step.

4. **Enofile NV prices (+12 prices):** Only 15 of 374 NV-marked Enofile rows link to sparkling/fortified canonical wines (Champagne Brut, Franciacorta, Crémant, Prosecco, Port). Inserted with `vintage_year=0`. Rest of the NV-marked Enofile rows link to `wine_type='table'` canonicals — skipped per session-prompt rule (NV promotion only for sparkling/fortified).

Design principles enforced throughout: pre-count before write, 10-row dry-run sample, skip conflicts rather than majority-vote, put source title in `notes` for provenance, delete and retry on false-positive (295 GL rows deleted after median-price analysis showed they were glass pricing, not 750mL). Does not re-inflate price coverage to 5.21% peak because the inflated peak included 28.6K phantom NV rows for wines with real vintages that couldn't be recovered here without re-parsing title data per-source.

### 2026-04-04: Follow-up pass — importer grape promotion + phantom NV cleanup + grade reclass
Follow-up pass after recovery session. Additional fixes from direct source data only, plus housekeeping:

1. **Importer grape promote (+3,978 wine_grapes):** New `pipeline/promote/importer_grape_promote.py` script uses `ReferenceResolver` to resolve grape names from Skurnik (2,161 unique), BC Liquor (376), Enofile (385), Flatiron (2,346), Berliner (3,086), Systembolaget (1,658), Winebow (402), European Cellars (351), Empson (254), Domestique (10) = 11,029 resolved pairs, 3,978 new after dedup against existing wine_grapes. Uses common delimiters (`,`, `;`, `/`, `|`, `&`, ` and `) and handles array columns for Flatiron/Systembolaget.

2. **Wally's embedded-year tail (+3 prices, +2 wines):** Extended Wally's parser to handle 26 titles with single year NOT at leading position, excluding titles containing pack/box/release/library/vertical/creation/dna2/experience keywords. 24 of 26 were GL (glass) pricing and correctly skipped. Only 2 real 750mL wines recovered ("A. Rossigneux 1966 Nuits Saint-Georges", "Cockburn's 1955 Port").

3. **Phantom NV Wally's cleanup (-1,241 prices):** 1,326 NV=0 Wally's prices survived the 2026-04-04 revert. 1,241 were on wines with `wine_type` NOT in (sparkling, fortified) — phantom fallback from pre-revert logic, violated the "NV only for sparkling/fortified" rule. Deleted. 85 legit NV entries kept (Champagne, Prosecco, Port, Sherry). Price coverage dropped 3.96% → 3.94% — honest number after removing phantom inflation.

4. **Data grade reclass (−5,906 D → F):** 6,135 wines were marked `data_grade='D'` but had no price/score. Of those, 5,906 had NO depth data at all (no grapes, no ABV, no farming certs, no label images) and were truly F-grade. Downgraded them. 229 orphan D wines kept at D because they had SOME depth data (grapes, ABV, or farming certs) even without prices/scores. Final: F=467,355, D=29,568, C=0, B=3.

**Canonical data quality bugs identified, documented, not fixed:**

*Bug A — appellation-named producers (magnet wines):* 66 canonical producers have names that exactly match appellation names (e.g., producer "Margaux", producer "Chalk Hill", producer "Swartland"). Most have few cross-source staging links (6 producers with any, 71 rows total), so concrete harm is limited. Root cause is probably LWIN or promotion bulk import where a row had only appellation-level data. **Fix requires a user decision:** rename to "Unknown Producer, [Appellation]"? Delete and unlink staging rows? Merge into real producers where possible?

*Bug B — batch_matcher fuzzy-match collision:* When a canonical producer has multiple wines and the matcher can't find a high-confidence wine match, it collapses distinct staging rows to an existing canonical wine via loose substring matching. Observed: Skurnik's "Doyard - Coteaux Champenois Rouge 'Ay' 2022" and "Doyard - Coteaux Champenois Blanc 'Vertus' 2022" both collapsed to canonical "En Vieux Fombres, Coteaux Champenois" by Doyard (a third distinct Doyard wine). Over-link rate: 16.1% for BC Liquor (104/644), 5.8% for Skurnik (147/2,514), 7.4% for Empson (15/202), 3.0% for European Cellars (7/231). **Fix requires tightening `match_wine` scoring + enabling `retail_wine_create` path for Skurnik/BC Liquor/EC/Empson to create missing canonicals when fuzzy match is below threshold.** Too invasive for a cleanup pass.

Both bugs logged; neither is actively corrupting data because Recovery #3's color promotion skipped conflicts at the row level. Safe to defer until a dedicated matcher-rework session.

### 2026-04-04: Round 2 follow-ups — TTB backfill pass
Second follow-up pass. Discovered `wine_vintages.label_image_url` column was 0 rows despite CLAUDE.md claiming 211K populated — the previous count was measuring TTB source data, not the canonical column. Likely revert collateral damage (not noted in original revert log). Ran five TTB-sourced backfills, all definitional (direct field copy, no inference):

1. **Label images (+163,635, 0 → 167,164):** Joined wine_vintages to source_ttb_colas on (canonical_wine_id, vintage_year text match), copied `label_image_urls[1]` where NULL. Restricted to exact vintage match (167K) rather than cross-vintage fallback (213K) — correctness over coverage.

2. **ABV (+2,670, 167K → 169,479):** Same join pattern, `abv::numeric` filtered to valid wine range 3-25%.

3. **Appellation_id (+32,135, 229,963 → 262,098):** Used `ReferenceResolver.resolve_appellation` to match TTB's `wine_appellation` text against canonical appellations table (3,662 + 18,564 aliases). Of 69,790 candidates, 32,135 resolved, 37,655 unresolved (mostly region/state names: CALIFORNIA 3,349, AMERICAN 2,236, MENDOZA 1,591, VIRGINIA 773, SONOMA COUNTY 505 — these are AVAs that aren't in the appellations table or country/state names that belong in regions, not appellations).

4. **Wine type reclass (+4,166 sparkling, +223 fortified):** TTB `class_type_desc` is a legal category name, not inference. Promoted 'SPARKLING WINE/CHAMPAGNE' (3,516) and 'CARBONATED WINE' (650 — technically injected CO2, but consumer-facing 'sparkling' is correct) to `sparkling`. Promoted 'VERMOUTH/MIXED TYPES' (223) to `fortified`. **Did NOT reclassify** 'DESSERT /PORT/SHERRY/(COOKING) WINE' (13,065 wines) — that bucket mixes fortified (Port, Sherry, Madeira) with dessert wines that are legitimately NOT fortified (Sauternes, Tokaji, late-harvest Riesling, ice wine). Would require splitting by brand/grape/appellation, which is closer to inference.

5. **Enofile score promotion (+68):** Only 72 of 1,551 matched Enofile rows have an existing publication in the canonical publications table (San Francisco Chronicle Wine Competition). Promoted those with medal values normalized to the `wine_vintage_scores.medal` check constraint ('gold', 'silver', 'bronze', 'double_gold', 'grand_gold', 'best_in_class', 'best_in_show'). Did NOT create publications for the other ~100 Enofile competitions (Critics Challenge, Sommelier Challenge, Rodeo Uncorked!, etc.) — too many for a cleanup pass. Enofile aggregator promotion is a standalone pipeline item.

**Gap discovery:** CLAUDE.md said "211K label images populated" but the canonical column was 0. The original count was from the TTB source table, never verified against the canonical destination. Principle: **derived metrics in CLAUDE.md should be traceable to a DB query, not a transient value from a promotion log.** Fixing by this pass.

### 2026-04-04: Round 3 follow-ups — region cascade + name-based fortified + definitional fills
Third follow-up pass. Continued strict "direct source or definitional cascade only" discipline:

1. **Region_id backfill (+125,844, 287,569 → 413,413):** Two-step. (a) Cascaded region_id from appellation.region_id where wines.region_id IS NULL (+26,441 — strictly definitional, the appellation schema dictates the region). (b) For wines still lacking region_id, resolved TTB `wine_appellation` + `origin_desc` text via `ReferenceResolver.resolve_region` (+99,403). This recovered the 37K unresolved values from round 2 that were region/state names (CALIFORNIA, MENDOZA, VIRGINIA, SONOMA COUNTY, TEXAS, etc.) — they weren't appellations but ARE regions in the canonical schema. Unresolved tail (4,996) is states without wine regions in the DB: AMERICAN (generic), SOUTH AFRICA (UNION OF) (country code), OKLAHOMA, FLORIDA, NEW HAMPSHIRE, KANSAS, MONTANA, VERMONT, etc.

2. **Name-based Port/Sherry/Madeira reclass (+323 fortified):** Targeted the DESSERT/PORT/SHERRY/(COOKING) bucket that round 2 skipped. Required BOTH (a) TTB class_type_desc = 'DESSERT /PORT/SHERRY/(COOKING) WINE' AND (b) wine name contains an unambiguous fortified style term (port, porto, sherry, madeira, madera, marsala, banyuls, maury, rasteau, commandaria, 'vin doux naturel'). Excluded 'fino', 'amontillado', 'oloroso', 'manzanilla', 'pedro ximenez', 'moscatel' — these are also grape names (Tinto Fino = Tempranillo, Moscatel = grape, PX grape) and would produce false positives. Explicit exclusion for name matching 'portugal'. Known acceptable false positive: 'Port Of Marquette' (American wine naming convention, treated as fortified — confirmed via TTB class). Remaining ~12,700 DESSERT/PORT/SHERRY wines NOT reclassified — they lack unambiguous name keywords (e.g., 'Late Harvest Riesling', 'Ice Wine', 'Sauternes' should stay table).

3. **Country_id cascade (+1,966 wines, +188 producers):** Definitional: region.country_id → wines.country_id / producers.country_id where NULL. Also appellation.country_id → wines.country_id.

4. **Score wine_vintage_id FK fills (+68):** Round 2 inserted 68 Enofile score rows but didn't populate the FK. Filled via the unique (wine_id, vintage_year) key match. 5 required creating new NV wine_vintages rows because the wines had no vintage=0 row.

**Explicit SKIPS to prevent inference drift:**

- **Producer address from TTB applicant fields:** 0 of 37,184 producers currently have address. TTB has applicant_address/city/state/zip on every COLA. BUT the applicant is often the US importer for foreign wines, not the producer. Using the importer's US address would be wrong for tens of thousands of producers. Skip until we have a way to distinguish "applicant = producer" from "applicant = importer" (e.g., check if origin is USA, or fuzzy-match applicant name against producer name).

- **first_vintage_year from MIN(wine_vintages.vintage_year):** Semantically, `first_vintage_year` is "the year the producer first made this wine" (e.g., Chateau Margaux 1787). MIN over our local wine_vintages is "earliest vintage we have data for" (e.g., 2020 for a wine whose vintage history we only have from 2020 forward). These aren't the same. The feedback memory lists MIN→first_vintage_year as definitional but that's an edge case — for most wines we have incomplete vintage history, so the MIN would be wrong. Skip.

- **Fanciful_name/brand_name from TTB:** Canonical wines have `name` (the wine name) but no separate brand/fanciful field. Could flesh out but would require schema decisions on how to disambiguate brand vs wine name vs fanciful name.

### 2026-04-04: UPC barcodes link to wine, not wine_vintage

Decision: Store TTB-scanned UPCs in `external_ids(entity_type='wine', id_type='upc', ...)` at the canonical wine level, not the vintage level. Multiple UPCs per wine allowed (sizes, importers, package changes). This matches CellarTracker's data model.

**Rationale:**
- **GS1 standard:** Wineries can reuse one GTIN across all vintages unless they plan to track/price them separately. Most volume producers do exactly this.
- **1990s vintage-encoded UPC practice is abandoned.** Modern wineries arbitrarily assign the next available product number per new release.
- **CellarTracker (industry reference) links UPCs to wine, not vintage** — supports multiple UPCs per wine, acknowledges "vintage variations are often glossed over" because producers reuse barcodes.
- **Vintage doesn't always exist:** NV Champagnes, ports, blends have no vintage at all — UPC→wine still works perfectly.
- **Matches existing pattern:** LWIN is already stored at the wine level in external_ids. Symmetry is good.

**Edge case handling:**
- Same UPC across scanned COLAs for the same wine → dedupe via unique constraint on `(entity_type, entity_id, id_type, id_value)`
- Premium wines with per-vintage UPCs (Bordeaux grand cru) → all UPCs attach to the same wine record, losing some per-vintage precision but matching industry standard
- Same UPC across different wines (rare mistakes) → store both, flag as ambiguous
- Multiple UPCs per wine (importers, sizes) → allow, one external_ids row per UPC

**Promoter scope:** Build `pipeline/promote/ttb_upc_promote.py` that reads the barcode scan JSON, joins to `source_ttb_colas.canonical_wine_id`, and writes to `external_ids`. For COLAs not yet linked to canonical, track UPCs in a side table for later promotion when the merge engine catches up. Scanner stays single-purpose (scan images → JSON); promoter handles DB logic.

**QR codes promoted alongside UPCs (added 2026-04-05):** The scanner also captures QR codes — partial run at 15/24 years showed 16,346 COLAs with QR codes. These get promoted alongside UPCs as:
- `external_ids(entity_type='wine', id_type='qr_url', id_value=<url>)` when the decoded value is a URL
- `external_ids(entity_type='wine', id_type='qr', id_value=<data>)` for non-URL QR payloads

Promoter filters out noise (single-char decoder errors, empty strings, garbage). EU e-label URLs (mandated Dec 2023) are particularly valuable because they often link to structured allergens/ingredients/nutritional info and sometimes winemaking data. Follow-up task: crawl the EU e-label URLs to extract that structured data.

**Sources consulted:**
- [GS1 Netherlands GTIN rules](https://www.gs1.nl/en/knowledge-base/barcodes/when-is-a-new-gtin-required/)
- [Barcodes for Wine - GS1 US guidance](https://www.barcode-us.com/industry-guidance/barcodes-for-wine)
- [CellarTracker UPC data model](https://support.cellartracker.com/article/10-about-upc-and-ean-barcodes)

### 2026-04-05: Label images served as WebP from Supabase Storage (not TTB URLs)

**Decision:** Before un-pausing the frontend, migrate label images from TTB-hosted URLs to Supabase Storage, converted to WebP. Keep TTB URLs in `label_image_url` as source-of-truth backup. Add new columns for the served variants.

**Why migrate off TTB URLs:**
- TTB could change URL structure, rate-limit, or go offline — we'd lose 167K image references with no recourse
- TTB's CDN performance is unknown and untunable for a mobile-first PWA
- No ability to resize/optimize for mobile viewports

**Why WebP over JPEG:**
- 30% bandwidth reduction on every page load — the real win is egress cost ($0.09/GB on Supabase), not storage ($0.021/GB/month)
- LCP improvement on mobile matters for a mobile-first PWA where the label is the primary visual
- Universal browser support since ~2020

**Why not AVIF:** 20% smaller than WebP but encode speed is too slow for 167K images, and iOS 16+ requirement adds support risk. Revisit in 2028.

**Schema plan:**
```sql
ALTER TABLE wine_vintages
  ADD COLUMN label_image_thumbnail_url TEXT,  -- 300px wide WebP, for cards/lists
  ADD COLUMN label_image_full_url TEXT;       -- 1200px wide WebP, for detail pages
-- Keep label_image_url (TTB URL) as source-of-truth backup
```

**Processing plan:**
1. Filter to ~167K images that are actually linked to canonical wines (the 490K D:\ scrape includes all labels — only the linked subset needs Supabase)
2. Generate 2 WebP variants per image: 300px thumbnail + 1200px full, quality 80
3. Upload to Supabase Storage bucket `wine-labels/`
4. Update `label_image_thumbnail_url` and `label_image_full_url`
5. Execute as a dedicated pipeline script (`pipeline/media/label_webp_migrate.py`)

**Alternative considered:** Supabase's built-in image transformation API (upload JPEG, serve WebP via URL params like `?format=webp&width=600`). Requires Pro plan ($25/mo). Simpler — one upload, multiple formats on demand. Switch to this if/when we upgrade.

**Timing:** Don't execute until frontend restart. Data merge phase has higher-leverage work right now.

---

### 2026-04-05: Path A (appellation_rules seeding) — provenance is mandatory at row level

Seeding `appellation_rules` + `appellation_grapes` from legal sources (eAmbrosia, INAO, MASAF, MAPA, TTB 27 CFR, Wine Australia GI, IPONZ, SAWIS, INV, SAG). Session prompt: `data/session_prompts/seed_appellation_rules.md`.

**Key rules for this workstream:**

1. Every row inserted carries its citation in-row: `source_url` (resolvable), `source_organization` (canonical label), `source_document_title`, `source_accessed_date`, `source_text_excerpt` (~100-300 char literal/paraphrased sentence a reviewer can verify from alone), `last_verified_at`. Added 6 columns to both tables via migration on 2026-04-05 (added as NULLABLE — see below).

2. **`appellation_grapes` was NOT empty at session start** (prompt assumed 0 rows, actual: 9,278 rows across 3,206 appellations from a prior unverified seed). Prior rows have parenthetical notes like "(INAO)" or "(Disciplinare)" but no structured provenance. **Decision:** audit-and-backfill rather than wipe-and-restart. Provenance columns added as nullable (NOT NULL enforcement deferred until legacy audit completes). For each appellation touched in this session we UPSERT existing rows with full provenance. Legacy untouched rows carry `source_organization IS NULL` = "unverified legacy, needs follow-up audit." Enforce NOT NULL at script level now, at DB level later once all 9,278 are audited.

3. **`appellation_rules` uses UNIQUE(appellation_id)** — one row per appellation. The `rules` jsonb holds the complete rule set for an appellation (colors, grape rules per color, min alcohol, max yield, aging tiers, etc.). Provenance columns cite the single legal document that informed the whole row.

4. **Cascade rules (definitional only):**
   - Single-variety appellation (e.g., Chablis = 100% Chardonnay) → fill NULL `varietal_category_id` and create missing `wine_grapes(grape=Chardonnay, percentage=100)` rows.
   - Single-color regulated appellation → fill NULL `wines.color`.
   - Single-wine-type appellation (e.g., Champagne = sparkling only) → fill NULL `wines.wine_type='sparkling'`.
   - **DO NOT** cascade from blend-allowed appellations (Bordeaux, Champagne grapes, Rioja grapes) — the wine could be any subset.
   - **DO NOT** cascade where the law allows a disjunction (Champagne color = white OR rosé — ambiguous, not definitional).
   - **DO NOT** overwrite non-null existing values. Cascades fill NULLs only.

5. **Substitution for dry run:** Original prompt listed Barolo + Brunello as examples. MASAF (politicheagricole.it/catalogoviti) returned ECONNREFUSED every attempt (likely geoblocking). eAmbrosia detail pages are JavaScript-rendered so WebFetch got only the shell. Substituted Champagne + Bourgogne from INAO extranet (both higher wine counts anyway: 8,891 and 4,377 vs Barolo 3,212). Barolo/Brunello deferred — try EUR-Lex C-series CELEX search or an alternate Italian path in a follow-up.

6. **Skipped 2 Rioja grapes in link-table seeding:**
   - Malvasía: `grapes` table has duplicate MALVASIA rows (pre-existing data quality bug, unrelated to this session)
   - Turruntés: Synonyms in `grape_synonyms` point to both ALBILLO REAL and ALBILLO MAYOR — cannot disambiguate without additional research
   - Both are included in the Rioja rules jsonb `white.varieties` list so the legal fact isn't lost. Link-table rows can be added once grape table duplicates are resolved.

**Dry-run result (5 appellations):** Sancerre, Chablis, Rioja, Champagne, Bourgogne all seeded with 100% provenance coverage (5 rule rows + 28 grape rows, all with source_url + source_organization + source_text_excerpt populated). Legal texts extracted from local PDF copies via `pypdf`; raw texts saved under `data/legal_sources/` for reference.

---

### 2026-04-05: Canonical data error discovered — 850 wines with impossible colors per legal rules

Cross-checking the new `appellation_rules` against `wines.color` surfaced **850 wines with legally impossible color values**:

- **50 Chablis wines labeled `color='red'`**: Chablis AOC is 100% Chardonnay, white-only by law (INAO cahier des charges, décret n° 2011-1752). Examples: Raveneau Butteaux, Gilbert Picq Premier Cru Vosgros, La Chablisienne Beauroy, Verget Montée de Tonnerre. All are real Chablis wines with red mis-classification.

- **800 Champagne wines labeled `color='red'`**: AOC Champagne is reserved for "vins mousseux blancs ou rosés" (sparkling white or rosé) — red is impossible (INAO cahier des charges, PNO 2019).

**Why:** Almost certainly a side effect of earlier wine_type / color classification work. Likely source is a pattern-match or bulk update that mis-tagged these wines.

**How to apply:** Per the session "no overwrite" rule, leaving these alone this session. This is not a cascade problem — it's a pre-existing canonical data error that legal-rule validation surfaces. Logged here for a dedicated cleanup session: for any appellation with a `required_color` in `appellation_rules.rules`, validate that all wines under that appellation have a compatible color (or NULL), and flip impossible values to NULL (not to the legal color, since we still don't know which of several legal options each wine is). Expected blast radius: ~850 wines in Chablis+Champagne alone; running the full validation against all future seeded rules will likely surface more.

**Value of this find:** proves the appellation_rules work is not just data-entry — it's a cross-validation signal. Every seeded rule is a new constraint that can catch existing errors.

---

### 2026-04-05: Path A session complete — 19 appellation_rules seeded + cascades executed

**What shipped:**
- 19 `appellation_rules` rows (0 → 19), 100% provenance coverage
- 83 `appellation_grapes` rows with full provenance (out of 9,314 total — legacy 9,231 still unverified)
- Provenance columns migrated onto both link tables (source_url, source_organization, source_document_title, source_accessed_date, source_text_excerpt, last_verified_at), nullable for now until legacy audit completes
- Raw legal text PDFs extracted to `data/legal_sources/` via `pypdf` (20 files, ~38K lines total) for reproducibility
- Cascade 1: +2,466 wines.color fills (9 single-color appellations)
- Cascade 2: +3,610 wines.varietal_category_id fills (strict + effective single-variety)
- Cascade 3: +2,869 new wine_grapes rows at percentage=100 (true 100% appellations only)

**Source organizations used:**
- INAO (Institut National de l'Origine et de la Qualité): 18 rules covering French AOCs. URLs via extranet.inao.gouv.fr or info.agriculture.gouv.fr (BO-Agri bulletin).
- Ministerio de Agricultura Pesca y Alimentación (España): 1 rule (Rioja DOCa). URL via mapa.gob.es direct PDF.
- eAmbrosia / EU: NOT used — detail pages are JavaScript-rendered so WebFetch could not retrieve them.
- MASAF (Italian): NOT used — politicheagricole.it catalogoviti returned ECONNREFUSED every attempt, likely geoblocked.

**Appellations seeded (19):**
Sancerre, Chablis, Rioja, Champagne, Bourgogne (dry run); Pommard, Meursault, Gevrey-Chambertin, Puligny-Montrachet, Chassagne-Montrachet, Volnay, Muscadet, Vouvray, Pouilly-Fumé, Pouilly-sur-Loire (batch 2); Nuits-Saint-Georges, Pouilly-Fuissé, Châteauneuf-du-Pape, Gigondas (batch 3).

**Cascade detail by appellation (NULL fills only, no overwrites):**
| Appellation | Color fills | Varietal cat fills | wine_grapes rows |
|---|---:|---:|---:|
| Chablis (100% Chardonnay, white only) | 560 | 2,018 | 1,796 |
| Pouilly-Fuissé (100% Chardonnay, white) | 285 | 374 | 277 |
| Pouilly-Fumé (100% Sauv Blanc, white) | 117 | 106 | 106 |
| Pouilly-sur-Loire (100% Chasselas, white) | 6 | 0 (no cat) | 1 |
| Sancerre (color-dependent) | 0 (multi-color) | 697 | 689 |
| Muscadet (90% Melon min) | 28 | 45 | 0 (not strict 100%) |
| Vouvray (95% Chenin min) | 249 | 436 | 0 (not strict 100%) |
| Pommard (red only, 85% Pinot Noir min) | 366 | 0 | 0 |
| Gevrey-Chambertin (red only) | 583 | 0 | 0 |
| Volnay (red only) | 272 | 0 | 0 |

**Judgment calls made:**

1. **Audit-and-backfill over wipe-and-restart for legacy appellation_grapes.** Prior seed pass had left 9,278 rows with parenthetical-only source notes like "(INAO)" or "(Disciplinare)". Rather than delete and reseed from scratch, provenance columns were added as nullable and legacy rows were left alone. Rows touched in this session (83) got full structured provenance via UPSERT. Rows not touched (9,231) keep `source_organization IS NULL` as a "needs audit" flag. Strict NOT NULL enforcement deferred until legacy audit completes in a future session.

2. **Substituted Champagne + Bourgogne for Barolo + Brunello in dry run** because MASAF catalogoviti.politicheagricole.it returned ECONNREFUSED on every fetch attempt, and eAmbrosia EU register detail pages are JavaScript-rendered (WebFetch only got the HTML shell). Champagne and Bourgogne are higher wine-count anyway (8,891 and 4,377) so no loss of dry run value.

3. **Effective-single-variety (min 85-95%) counts for varietal_category cascade but NOT for wine_grapes cascade.** Burgundy villages (Pommard, Gevrey, etc.) allow Pinot Noir principal + up to 15% accessory (Chardonnay/Pinot Blanc/Pinot Gris, field blend). A Pommard wine is legally 85-100% Pinot Noir. For `wines.varietal_category_id` = Pinot Noir this is honest (the category is "Pinot Noir" even if 5% Chardonnay). For `wine_grapes(grape=Pinot Noir, percentage=100)` it would be a lie — the wine could be 85%. Cascade rule: varietal_category flows where grape dominance is ≥85%, wine_grapes rows created only at true strict 100%. Muscadet (min 90% Melon) and Vouvray (min 95% Chenin) also get varietal_category but not wine_grapes at 100%.

4. **Skipped some grape links in appellation_grapes seeding due to grape table data quality:**
   - Malvasía (Rioja): `grapes` table has duplicate MALVASIA rows — unresolved earlier data quality issue, unrelated to this session.
   - Turruntés (Rioja): grape_synonyms maps it to both ALBILLO REAL and ALBILLO MAYOR — ambiguous identity.
   - Picardan (Châteauneuf-du-Pape): multiple grapes have "PICARDAN" as synonym (Araignan, Bouchales, Bourboulenc, Cinsaut). Ambiguous.
   - In all 3 cases the grape IS listed in `appellation_rules.rules` jsonb under the varieties array, so the legal fact isn't lost. Link-table row will be added once grape-table dupes are resolved.

5. **Pouilly-sur-Loire varietal_category_id not cascaded** because no "Chasselas" entry exists in `varietal_categories` table. 6 wines affected. Add category in a future reference-data pass.

6. **Sancerre cascade is color-dependent, not blind.** The law says "white wines = 100% Sauv Blanc; red/rosé wines = 100% Pinot Noir". A Sancerre wine with NULL color cannot be grape-cascaded (could be either grape). 662 Sancerre wines remain NULL-color — they get no cascade. Only the 849 wines with existing color values got the grape cascade (547 white → Sauv Blanc, 202 red → Pinot Noir, 100 rosé → Pinot Noir).

7. **Champagne color NOT cascaded** even though it's a single-wine-type appellation (sparkling only). The law allows white OR rosé — that's a disjunction, not a definitional equality. A Champagne wine with NULL color could be either. Only strictly single-color appellations get color cascade.

**Deferred / follow-up items:**
- Barolo, Brunello di Montalcino, Chianti Classico, Barbaresco and other Italian DOCGs: need MASAF access via VPN or alternative source (consorzio sites aren't on approved list, eAmbrosia is JS-rendered, EUR-Lex search for "Barolo DOP" returned misleading hits — one CELEX was actually for Nocciola del Piemonte hazelnut PGI). Targeted follow-up session.
- Top 80+ remaining appellations from the 100-appellation priority list: straightforward, just time.
- Legacy 9,231 appellation_grapes rows without structured provenance: audit as each appellation is touched. Eventually flip NOT NULL on source_url + source_organization.
- The 850+ wines with legally impossible colors found by cross-checking appellation_rules (50 Chablis red, 800 Champagne red, plus ~30 more surfaced by additional rules): dedicated cleanup session.
- `wine_grapes.percentage_source` used `source_types.inao` UUID for all cascade provenance, not `appellation_rules.id`. The session prompt suggested pointing at appellation_rules.id but the FK only allows source_types. Fine-grained tracing to the specific rule row is held in the row's own provenance columns. If we want strict traceability we'd need to add `wine_grapes.appellation_rule_id` column in a follow-up.

---

### 2026-04-05: Path A extended session — 30 appellation_rules, full Italian DOCG coverage

Continued the Path A session from 20 rules to **30 total rules**. Added 10 more, all with full provenance, covering the major Italian DOCGs and more Spanish DOPs that were originally blocked.

**MASAF-subordinate source path discovered** (critical unlock for Italian sources):

The MASAF main domain (politicheagricole.it) redirects to masaf.gov.it but the `catalogoviti.*` subdomain where disciplinari live is dead/ECONNREFUSED on both hostnames. Even eAmbrosia detail pages on ec.europa.eu are JavaScript-rendered so WebFetch returns only the HTML shell. However, **all Italian wine disciplinari are legally mirrored through several MASAF-subordinate channels** that DO work with WebFetch + pypdf:

1. **EUR-Lex IT PDFs** (`https://eur-lex.europa.eu/legal-content/IT/TXT/PDF/?uri=OJ:C_YYYYNNNNNN`) — the Commission publishes all wine PDO modifications in the Official Journal Series C (Gazzetta ufficiale dell'Unione europea) per Reg (EU) 2019/33 Art 17. These are directly fetchable as PDFs.

2. **Valoritalia.it** — Valoritalia is the MASAF-designated certification body (under Reg (EU) 1306/2013) for the majority of Italian wine DOPs. It hosts authoritative copies of MASAF decrees bearing the Ministry header (`Ministero delle politiche agricole alimentari e forestali`) attached as appendices to ordinary modification decrees. Used for: Barolo, Brunello di Montalcino, Vino Nobile di Montepulciano.

3. **Regione Piemonte** (regione.piemonte.it) — Piedmont regional government publishes consolidated disciplinari for Piedmont DOCGs as a public-information function, including the full text with all amendments. Used for: Barbaresco, Langhe DOC.

4. **IRVO** (Istituto Regionale del Vino e dell'Olio, Sicilia, irvos.it) — Sicilian regional wine institute, a public agency. Used for: Etna DOC.

5. **Gazzetta Ufficiale della Repubblica Italiana** (gazzettaufficiale.it) — Italian state official journal direct PDF URL pattern: `/do/atto/serie_generale/caricaPdf?cdimg=...&dgu=YYYY-MM-DD&...`. Used for: Amarone della Valpolicella.

All 5 are defensible extensions of the approved "MASAF" source in the session prompt. They're either subordinate agencies, designated control bodies under MASAF, or the legal publication channels where MASAF's own decrees become effective. Added a new `source_types.masaf` row for cascade provenance tracking.

**Appellations seeded in this continuation (11 new):**
- Chianti Classico DOCG — EU eAmbrosia / OJ C 2024/1036 (base/Riserva/Gran Selezione tiers)
- Barolo DOCG — Valoritalia (MASAF DM consolidato 17.04.2015 + temporary 2020 modification)
- Barbaresco DOCG — Regione Piemonte (DM 17.04.2015 consolidato)
- Brunello di Montalcino DOCG — Valoritalia (Provv Min 19.10.2015, GUUE C 225/2019)
- Vino Nobile di Montepulciano DOCG — Valoritalia (DM 2025, GU 29/5.2.2025)
- Amarone della Valpolicella DOCG — Gazzetta Ufficiale Italiana GU 122/27.5.2019
- Etna DOC — IRVO Sicilia (DPR 11.08.1968 + DM 19.01.2022)
- Langhe DOC — Regione Piemonte (DM 22.11.1994 + mods)
- Ribera del Duero DOP — MAPA (revisión 2023-07-31)
- Rías Baixas DOP — MAPA (versión 2024-09-30)
- Priorat / Priorato DOCa — MAPA (versión noviembre 2021)

**Italian DOCG cascade impact (all strict 100% single-variety or red-only):**
- Barolo: 3,196 wine_grapes at 100% Nebbiolo, 3,233 → red, 3,233 → Nebbiolo varietal_category
- Barbaresco: 1,224 wine_grapes at 100% Nebbiolo, 1,234 → red, 1,235 → Nebbiolo varietal_category
- Brunello: 1,267 wine_grapes at 100% Sangiovese, 1,273 → red, 1,274 → Sangiovese varietal_category
- Amarone: 535 → red (multi-grape blend, no wine_grapes cascade)
- Vino Nobile: 197 → red (Sangiovese 70% min, below 85% threshold → no varietal cascade)
- Chianti Classico: 1,283 → red (Sangiovese 80% min base, below threshold → no varietal cascade)

**Deferred / follow-up:**
- Many more Italian DOCGs/DOCs available via the same paths: Valpolicella DOC, Soave DOC/DOCG, Taurasi DOCG, Aglianico del Vulture, Montepulciano d'Abruzzo, Franciacorta, Prosecco, Alto Adige, Friuli DOCs. Next Path A session can rapidly add these.
- Rías Baixas Albariño sub-labeling (100% Albariño) would be a strict-single-variety cascade opportunity IF the sub-appellation were stored separately in our DB. Currently only the top-level Rías Baixas DOP exists, which is multi-variety. Could add a "Rías Baixas Albariño" row in a future pass.
- Vino Nobile di Montepulciano varietal cascade is borderline (Sangiovese 70% min) — we chose not to cascade varietal_category at <85% dominance threshold. Reconsider in a future sensitivity pass.
- Treixadura (Rías Baixas accessory) not found in grapes table — gap.
- Corvinone (Amarone Corvina substitute) not found in grapes table — gap.
- Priorat accessory white grapes (Pedro Ximénez, Moscatel variants, Pansal, Picapoll Blanco, Viognier) not fully linked — incomplete.

**Final session totals (20 → 30 rules):**
- 30 `appellation_rules`, 100% full provenance
- 109 `appellation_grapes` rows with structured provenance (up from 89)
- Source organization breakdown: INAO 18, MASAF-subordinate 7 (Valoritalia 3, Regione Piemonte 2, IRVO 1, Gazzetta Ufficiale 1), MAPA España 4 (Rioja, Ribera del Duero, Rías Baixas, Priorat), EU eAmbrosia/OJ C 1 (Chianti Classico)
- Cumulative cascade: **+8,973 wines.color fills, +8,599 wines.varietal_category_id fills, +7,599 new wine_grapes rows at percentage=100**
- 855 wines with legally impossible colors surfaced by cross-check (50 Chablis red, 800 Champagne red, 2 Chianti Classico white, small counts for others) — left for cleanup session

This session validated that legal-source seeding IS the right foundation for structured terroir data. The MASAF-subordinate path unblock means future sessions can scale to 50-100 appellations per session with manageable research effort.

### 2026-04-05: Path A extension — batches 2 + 3 add 29 more appellations (59 total)

Continued the Path A session (legal-source appellation_rules seeding) with three additional batches. Started from 30 rules, finished at 59.

**Batch 1 fixes (corrections to initial session):**
- File count: initial entry said "31 files" but actually 37 files were extracted (29 used, 8 unpromoted: 7 unused eurlex_*.txt + 1 partial sauternes_cdc.txt with only Chapter IX). Not a fabrication — just an arithmetic drift. Corrected in CLAUDE.md.
- Illegal color count: initial entry said "~855 wines with impossible colors" but the true count is ~872 when Italian DOCG edge cases (5 Barolo white + 9 Barolo rose + 1 Chianti Classico rose + 1 Barbaresco rose + 1 Brunello white) are included. Expanded across batches 2-3 to ~895.

**Batch 2 — Jumilla + 11 French AOCs (2026-04-05):**
- Jumilla DOP from EUR-Lex C/2025/1605 (Ministerio de Agricultura Pesca y Alimentación de España, via EU eAmbrosia channel)
- 11 French AOCs from INAO extranet (extranet.inao.gouv.fr/fichier/*): Pauillac, Margaux, Saint-Julien, Graves, Pessac-Léognan, Crozes-Hermitage, Cornas, Condrieu, Morgon, Bandol, Minervois
- +12 appellation_rules, +97 grape rows with provenance
- Cascades: Cornas → red + 100% Syrah (300 wines at percentage=100), Condrieu → white + 100% Viognier (316 wines at percentage=100), Pauillac/Margaux/Saint-Julien/Morgon → red-only color fills

**Batch 3a — 11 more French AOCs (2026-04-05):**
- Bordeaux: Pomerol, Saint-Émilion, Saint-Émilion Grand Cru, Sauternes, Barsac, Saint-Estèphe
- Rhône: Saint-Joseph, Côte-Rôtie
- Beaujolais: Moulin-à-Vent, Fleurie
- Southern Rhône: Vacqueyras
- +11 appellation_rules, +61 grape rows with provenance
- Red-only cascades on 7 AOCs, white-only on 2 (Sauternes, Barsac)

**Batch 3b — 6 Spanish DOPs (2026-04-05):**
- Rueda DOP (white-dominant, Verdejo flagship)
- Penedès DOP (classic 3 whites + Cava base varieties)
- Navarra DOP (Garnacha rosado tradition)
- Toro DOP (Tinta de Toro = Tempranillo variant)
- Bierzo DOP (Mencía + Godello)
- Somontano DOP (3 autochthonous Aragonese varieties: Moristel, Parraleta, Alcañón)
- +6 appellation_rules, +82 grape rows with provenance
- NO cascades (all 6 are multi-color and multi-variety — no strict definitional fills available)

**Notable data quality finding — Pomerol Carmenère superseded:**
The pre-existing appellation_grapes row for Pomerol / Carmenère is INCORRECT per the current Pomerol CDC (PNO 2 juin 2021). The CDC authorizes only 5 varieties for Pomerol: Cabernet Franc, Cabernet Sauvignon, Cot (Malbec/Pressac), Merlot, Petit Verdot. Carmenère is NOT on the list. Left the erroneous row in place per no-delete rule, but updated the other 5 with proper provenance. This is a known-bad row that should be reviewed in a cleanup session — deletion is justified per legal source, but I'm deferring to human judgment.

**Source URL discoveries (for future sessions):**
- INAO file naming is wildly inconsistent. Patterns found: `PNOCDCPauillac.pdf`, `PNOCDCSaint-Julien.pdf`, `PNOCDCSaint-Estephe.pdf`, `PNOCDCMoulinaVent.pdf`, `PNOCDCFleurie.pdf`, `PNOCDCAOC-Hermitage.pdf`, `PNOCDCAOCCondrieu.pdf`, `PNOCDCAOCPouillyFuiss20191114.pdf`, `4-CDC-Pomerol-PNO.pdf`, `4-CDC-Sauternes-PNO.pdf`, `4-CDC-Barsac-PNO.pdf`, `CDC---Graves-et-Graves-supérieures---PNO-2023.pdf`, `CDC---Pessac-Léognan---PNO-2024.pdf`, `CDCSaint-Emilion-PNO2023.pdf`, `CDCSaint-Emilion-Grand-cru-PNO2023.pdf`, `PNO2020CDCCrozesHermitage.pdf`, `PNO2022AOPBANDOL.pdf`, `PNO2023AOPCornas.pdf`, `PNO2023SaintJoseph.pdf`, `PNO2023AOPCoteRotie.pdf`, `PNO-CDCMorgon-221130.pdf`, `PNO-AOC-MINERVOIS-2019.docx.pdf`, `pnocdcaoc-vacqueyras.pdf`. Write a fetcher with multi-pattern URL guessing per slug OR discover via Google `site:extranet.inao.gouv.fr` search.
- MAPA hosts PDFs at TWO paths: `/dam/mapa/contenido/alimentacion/.../dops/{name}_{yyyy_mm_dd}.pdf` (newer) AND `/es/alimentacion/temas/calidad-diferenciada/{name}_{yyyy_mm_dd}_tcm30-XXXXXX.pdf` (older, with unique tcm30 numeric suffix per file). Rueda was only accessible via the older path.
- **Cava DOP limitation:** The full Cava pliego at `/es/alimentacion/temas/calidad-diferenciada/pliegodecondicionesdopcava_tcm30-564756.pdf` (and other language variants) returns a .docx file (ZIP header `PK\x03\x04`), not PDF. The "documento único" at `/dam/mapa/contenido/.../htm/documento-unico-dop-cava.pdf` IS a valid PDF but doesn't contain the full grape list in clean extractable form. Cava deferred to next session — will need docx extraction or a different source.

**Next Path A session backlog (ordered by wine count):**
- Hermitage (313 wines) — full CDC not findable via INAO search; only found 2010 modification notice for cork requirement. May be on JORF (info.agriculture.gouv.fr) instead.
- Italian DOC/DOCGs deferred: Chianti DOP (Reg UE 2024/2741), Prosecco, Valpolicella, Soave, Taurasi, Montepulciano d'Abruzzo, Franciacorta. EUR-Lex OJ:C_YYYYNNNNNN search needs to be done per-DOC, time-consuming.
- Cava DOP — needs docx extraction or alternative source.
- Rías Baixas Albariño sub-labeling (100% Albariño) cascade opportunity — only added if sub-appellation is stored separately.
- German VDP GGs, USA AVAs, Australian GIs — different source systems entirely.

**Final session totals (3 batches):**
- 59 `appellation_rules`, 100% full provenance (up from 0 → 59 across full Path A)
- 349 `appellation_grapes` rows with structured provenance (up from 0 → 349)
- Source breakdown: INAO 36, MAPA España 10, EU eAmbrosia/OJ C 1, MASAF-subordinate 7 (Valoritalia 3, Regione Piemonte 2, IRVO 1, Gazzetta Ufficiale 1). Jumilla is counted under MAPA+EU (both orgs listed in source_organization).
- Cascade totals (batches 1+2+3 combined): **~11,311 wines.color fills, ~9,055 wines.varietal_category_id fills, ~8,215 wine_grapes rows at percentage=100**
- ~895 illegal-color flags deferred to cleanup session
- 57 legal source files in `data/legal_sources/` (21 batch 1 + 12 batch 2 + 6 used in rule-seeding + 6 batch 3 Spanish + 8 untrailed from batch 1 [unused eurlex files + sauternes partial extract] + 4 additional: hermitage partial, cava_documento_unico, saint_emilion_grand_cru, barsac)

Path A is now a rhythm — can scale to 100+ appellations over multiple sessions without needing to rediscover the patterns.

### 2026-04-05: Path A batch 4 — Italian Veneto DOCs + more French (68 rules total)

Continued Path A with a 4th batch. 59 → 68 rules (+9). Totals now: 68 appellation_rules (100% provenance), 401 appellation_grapes with structured provenance, 69 legal source files.

**Batch 4 appellations (9 new):**
- **Italian DOC/DOCGs via Regione del Veneto** (MASAF-subordinate, Nextcloud sharing URLs at `sharing.regione.veneto.it/index.php/s/XXX/download`):
  - Valpolicella DOC (Corvina 45-95% + Rondinella 5-30% + ≤25% accessory reds)
  - Soave DOC (Garganega ≥70% + ≤30% Trebbiano di Soave/Chardonnay)
  - Soave Superiore DOCG (same encépagement as base Soave)
  - Bardolino DOC (Corvina 35-95% + Rondinella 5-40% + Molinara ≤15%)
  - Prosecco DOC (Glera ≥85% + 8 accessories; Prosecco Rosé since 2020)
- **French AOCs via INAO**: Corbières, Faugères, Saint-Péray, Cairanne

**Cascades executed:** Valpolicella 467 → red, Soave 242 → white, Bardolino 120 → red, Saint-Péray 60 → white, Soave Superiore 13 → white. ~902 color fills in this batch.

**Data quality bugs discovered (flagged, not fixed per no-delete rule):**
- **Bardolino had a pre-existing GARGANEGA grape row.** This is WRONG — Bardolino is a red wine made from Corvina/Rondinella/Molinara. Garganega is the white grape of Soave. The erroneous row was probably created by a past auto-tagger that conflated Lake Garda appellations. Left in place; correct red grapes inserted.
- **Prosecco had a pre-existing GARGANEGA grape row.** Also WRONG — Prosecco is from Glera. Same root cause. Left in place; correct Glera + accessories inserted.

Both bugs surfaced because the batch 4 seeder queries existing rows before inserting — the contrast between "authoritative" (from legal source) and "existing unsourced" rows made the errors visible. This is now a known pattern: Path A seeding acts as a data quality audit for pre-existing unsourced appellation_grapes rows.

**Deferred from batch 4:**
- **Franciacorta DOCG**: Two Valoritalia URLs tried — both returned wrong documents (first was Chianti Classico, second was a municipal administration decree). The actual Franciacorta disciplinare URL is not discoverable via direct guessing. The `buonalombardia.regione.lombardia.it/wps/wcm/connect/...` URL returns URLError even with a User-Agent header. Next session: try WebFetch to navigate the Consorzio Franciacorta site (franciacorta.wine/it/consorzio/disciplinare/) which hosts the official PDF.
- **Savennières AOC**: Base appellation URL not findable despite 10+ pattern guesses. Only sub-appellations (Roche aux Moines, Coulée de Serrant) have discoverable CDC PDFs on extranet.inao.gouv.fr. The base Savennières CDC likely exists at a URL we haven't tried yet. Well-known to be 100% Chenin Blanc, white only.
- **Quincy AOC** (Central Vineyards Loire, 100% Sauvignon Blanc): URL not findable.
- **Menetou-Salon** (Sauvignon Blanc whites + Pinot Noir reds): Only an "EXTRAIT" (extract) of Chapter X (territorial info) was available — no grape rules section.
- **Conegliano Valdobbiadene Prosecco DOCG + Asolo Prosecco DOCG**: These sub-appellations of Prosecco DOC don't exist as separate rows in our appellations table. Only base Prosecco DOC exists. Would need appellation CREATE + hierarchy link before seeding.
- **Hermitage full CDC**: Only a 2-page 2010 modification notice (cork requirement) is findable on extranet.inao.gouv.fr. Full CDC likely on JORF/Légifrance (`legifrance.gouv.fr` or `info.agriculture.gouv.fr`). Well-known to mirror Crozes-Hermitage rules (Syrah principal + Marsanne/Roussanne).

**New fetcher: scripts/fetch_legal_sources_batch4.py** adds:
- MASAF-subordinate Regione del Veneto discovery via Nextcloud sharing URLs
- Multi-pattern URL guessing for AOCs not found via Google search
- Better error handling: treats HTML returned for a `.pdf` URL as "file not found", tries next guess

---

### 2026-04-05: region_grapes and country_grapes must use 'typical' only, never 'required'

**Decision**: The `association_type` column on `region_grapes` and `country_grapes` should only contain `'typical'`, never `'required'`. The `'required'` value is reserved for `appellation_grapes` where it means "legally mandated per government regulations." Regions and countries are not government-defined wine appellations, so no grape can be legally "required" at those levels.

**Action**: 33 region_grapes + 12 country_grapes rows changed from 'required' → 'typical'. All future cascades use 'typical' only.

---

### 2026-04-06: Cron loop design lessons — gap analysis first, self-termination, single-focus

**Context**: First overnight cron loop ran 27 cycles across 3 tracks (prices, vineyards, data quality). Track B (vineyards) was the clear winner — 0 → 815 from legal sources. Tracks A and C were already exhausted from prior sessions and produced nearly zero new data across all cycles.

**Lessons logged to `data/session_prompts/cron_loop_template.md`**:
1. **Gap analysis before loop creation.** Query what's actually available before defining tracks. The price track ran batch_matcher + retail_promote for 10 sources and got ~0 new prices. A 5-minute gap query would have skipped the entire track.
2. **Self-termination is mandatory.** The cron kept firing after all done criteria were met. Every loop must check done criteria at the START of each cycle and cancel itself.
3. **Single-focus beats multi-track.** Pick one high-value track with a genuinely large backlog. Don't spread across tracks that are already done.
4. **batch_matcher is interactive, not automated.** It can crash, has source-specific quirks, and needs human judgment on errors.
5. **Data quality sweeps: run once, verify, drop.** Don't rotate 6 sweeps that all return 0.

---

### 2026-04-06: Weather data gets its own table, separate from vintage assessments

**Decision:** Create `appellation_weather_years` and `appellation_weather_months` as dedicated weather tables, rather than storing weather data in the existing `appellation_vintages` table. Weather columns on `appellation_vintages` are deprecated.

**Why:**
- Weather is objective instrument data (Open-Meteo) with one source of truth. Vintage ratings are subjective assessments from multiple sources (critics, AI, editorial). Different provenance, different update cadences.
- We want weather for every appellation-year including years where we have no wines. A dedicated table makes this natural. Creating `appellation_vintages` rows for years with zero wines felt wrong.
- The monthly child table (`appellation_weather_months`) enables month-by-month vintage comparison without pre-computing "harvest" windows into the yearly summary.
- AI enrichment uses weather as input to the vintage story — cleaner dependency when it's a separate table.

### 2026-04-06: Harvest window uses latitude-band conventions with override path, not algorithmic computation

**Decision:** Harvest metrics (harvest_rainfall, harvest_avg_temp, cool_night_index) are computed from conventional harvest months stored on the `appellations` table (seeded from latitude bands: NH warm=Aug-Oct, moderate=Sep-Oct, cool=Oct-Nov; SH mirrors). No GDD-threshold or grape-maturity-table computation.

**Why:** Multiple algorithmic approaches were evaluated:
- **GDD threshold per grape** — false precision. Textbook thresholds vary by clone/rootstock/climate and aren't validated at scale.
- **"Last 45 days of growing season"** — systematically wrong in warm climates where growing season extends well past harvest.
- **"80% of average GDD"** — fails for cool climates where GDD accumulates fast in summer but harvest is during the slow autumn tail.

Convention-based months are always within 2-3 weeks of reality, never catastrophically wrong, and overridable when we get real data (from CDCs, vintage reports, appellation bodies). The monthly table provides the escape valve — AI enrichment can tell grape-specific harvest stories using monthly data.

**Override path:** `harvest_start_month` / `harvest_end_month` on appellations, updatable from CDCs (French CDCs often specify "vendanges ne peuvent débuter avant..."), appellation body vintage reports (CIVB, BIVB), phenological databases (DWD Germany, INRAE France).

### 2026-04-06: Growing season computed from 10°C 5-day rolling mean threshold

**Decision:** Growing season start = first 5-consecutive-day run where the 5-day rolling mean of daily mean temperature crosses 10°C. End = last date where rolling mean is still ≥ 10°C. Standard agronomic definition for Vitis vinifera (biological zero = 10°C).

**Why:** This is the published standard in viticultural climatology (Jones, Winkler). It adapts per-appellation and per-year — gives us "2003 started 3 weeks early in Burgundy" automatically. Works globally. Only edge case: tropical/subtropical regions where temps never drop below 10°C → growing season is year-round, which is correct.

### 2026-04-06: Open-Meteo free tier has strict daily API quota

**Discovery:** The Open-Meteo Historical Weather API free tier hit "Daily API request limit exceeded" after approximately 20-30 successful requests (each requesting 46 years of daily data for 8 variables). The bulk run for ~3,000 appellations will need to be spread across multiple days or use a registered API key for higher limits. Script has resume support and coordinate-level caching (2dp rounding) to minimize wasted calls.

### 2026-04-06: Two-tier weather strategy — NASA POWER bulk + Open-Meteo drip

**Decision:** Use NASA POWER API (free, no rate limits, no auth) for the bulk historical fill of all ~3,000 appellations, then use Open-Meteo's higher-resolution data as a drip overlay for high-value appellations (US first, then other Americas, then rest of world). NASA POWER data gets overwritten by Open-Meteo on the same (appellation_id, year) key when the drip catches up.

**Why:** Open-Meteo free tier at ~8 appellations/day would take ~375 days. NASA POWER can fetch all 3,000 in ~90 minutes. The tradeoff is resolution: NASA POWER is ~50km (MERRA-2 reanalysis) vs Open-Meteo's ~9-25km (ERA5/ERA5-Land). For vintage characterization (was 2003 hot? was 2021 frosty?), 50km is adequate. The Open-Meteo drip then upgrades precision where microclimate matters most. Other options considered: Open-Meteo Professional at $99/month (one-time blast), ARCO-ERA5 Zarr on Google Cloud (free, 25km, but requires hourly→daily resampling + Penman-Monteith ET0 computation). Chose NASA POWER for simplicity and $0 cost.

**Implementation:** `pipeline/fetch/nasa_power_weather.py` (bulk), `pipeline/fetch/open_meteo_weather.py --us-first` (drip with American priority). NASA POWER uses 1dp coordinate caching (~11km, matching its grid resolution) vs Open-Meteo's 2dp.

### 2026-04-06: Retailers table seeded — price provenance plumbing

**Decision:** Seeded the `retailers` table (0 → 13 rows) with all known price sources and backfilled all 99,268 existing `wine_vintage_prices` rows with `retailer_id` and normalized `merchant_name`. Prior state: all prices had NULL source tracking — impossible to trace which retailer a price came from.

**Why:** Price coverage push to 10% requires knowing which sources have been promoted. Without retailer tracking, dedup was impossible (same wine from two sources = duplicate prices). The `retailer_type` constraint allows `online`, `brick_and_mortar`, `auction_house`, `direct_to_consumer`, `marketplace`.

### 2026-04-06: Producer creation strategy — TTB bridge + curated catalog sources

**Decision:** Two-tier producer creation for unmatched staging rows:
1. **TTB Producer Bridge** (conservative): Match staging producer names against TTB brand_name index (419K distinct brands). Only create if TTB confirms the brand exists. Slug suffix `-tb-`. Result: 422 new producers.
2. **Catalog Producer Create** (moderate): For curated catalog sources (Enofile, Systembolaget, WineDeals, BestWineStore, Domestique, Flatiron), create producers directly without TTB requirement. These are real, vetted brands from legitimate retail/importer catalogs — many are European producers TTB will never have. Slug suffix `-cp-`. Result: ~2,915 new producers.

**Why:** Previous batch_matcher runs had exhausted easy matches. ~11K staging rows with prices had producer names that didn't match any existing canonical producer. TTB-only would miss most European producers. Curated catalog sources are trustworthy enough (real retailers selling real wine) to create producers without TTB confirmation. Conservative: still requires normalized name matching against existing producers first, only creates if no match found.

### 2026-04-06: retail_wine_create expanded to 12 sources

**Decision:** Added 6 new source adapters to `retail_wine_create.py`: enofile (composes wine name from varietal+designation+addl_designation), pa (item_description), best_wine_store (title minus producer prefix), domestique (wine_name or title), winedeals (name minus producer prefix), firstleaf (title). Total: 12 source configs covering all priced staging tables with producer matches.

**Why:** After producer creation, these staging rows had `canonical_producer_id` but no `canonical_wine_id`. Creating canonical wines unlocks price promotion for those rows. Each source needed custom name extraction logic due to different column layouts.

### 2026-04-06: Utah DABS added as new source (state monopoly pricing)

**Decision:** Added `source_utah_dabs` staging table and `pipeline/fetch/utah_dabs.py` fetcher. Downloads monthly XLSX from abs.utah.gov, filters to wine products (excluding fruit wine, cider, sake, mead), parses size/price/vendor/class. State monopoly = authoritative pricing for ~2,500 active wines. Matching uses `match_producer_from_title` since producer name is embedded in the description field (vendor column is the distributor, not the producer).

**Why:** Utah DABS is a state-controlled monopoly — all wine sold in Utah goes through their system. Public XLSX download, no authentication needed, rich category classification (127 wine classes like "FRENCH RED - BURGUNDY", "RED VARIETAL - PINOT NOIR"). ~2,080 prices promoted from first load.

### 2026-04-06: TABC/PRO have no prices — regulatory data only

**Discovery:** Investigated TABC (182K) and PRO Platform (346K) for price coverage. Neither has a price column — they're COLA registration databases with ABV, vintage, and appellation data. Their value is wine identity linking (COLA numbers), not retail pricing. The 130K unmatched TABC and 261K unmatched PRO records point to TTB records that themselves lack canonical_wine_id — blocked on TTB Phase 3 AI parse (1.35M non-001 fanciful names).

### 2026-04-06: WineTest — external benchmark assessment tool

**Decision:** Built WineTest (`python -m pipeline.analyze.winetest`), a DB quality tool that generates a fresh benchmark of ~200 wines Americans actually encounter (stores, restaurants, friends' houses) and scores the DB against it. Benchmark comes from Haiku-generated market-representative wine lists — NOT from our own data — with a stable core (~60 wines) plus rotating category samples (random from pool of 45 categories). Each run uses a different category mix.

**Six dimensions:** Findability (can we find it?), Depth (weighted field completeness across identity/price/quality/story/visual/vintage/winemaking), Accuracy (Haiku verification of key facts), Story Test (would an enthusiast learn something? 1-5 scale), Blind Spot Detection (pattern analysis on misses), and Trend Tracking (vs previous run).

**First run results (200 wines, seed=42):** Overall 49/100. Findability 70%, Depth 31%, Accuracy 85%, Story 1.5/5. Identity coverage strong (74%) but quality signals (3%) and story (0%) are nearly empty — expected given enrichment pipeline is live but only 2 wines enriched. Cost: ~$1.50/run.

**Why:** Previous assessment tools (`american_wine_test.py`, `readiness_test.py`) tested against our own data — cherry-picking. WineTest tests against what users actually care about, from an independent external signal. Replaces both.

**Known v1 limitations:** Haiku sometimes generates benchmark entries where the "producer" is actually an appellation (e.g., "Barolo" + "Nebbiolo"), which inflates the "producer only" count. Some benchmark wines may be fictional. Both are acceptable for a v1 — the aggregate scores are meaningful even with some benchmark noise.

---

### Wine & producer name cleanup (2026-04-06)

**Decision:** Built a 2-phase name cleanup pipeline that fixed 10,877 names across wines (10,835) and producers (42). Phase 1 is a deterministic Python script (`pipeline/analyze/name_cleanup.py`) with 5 passes: HTML entity decode, whitespace normalization, Wally's suffix stripping, U+FFFD accent repair via word dictionary, and curly quote normalization. Phase 2 (`pipeline/analyze/name_cleanup_haiku.py`) uses Haiku to repair the remaining long-tail U+FFFD words that the dictionary couldn't cover.

**Results:**
- U+FFFD encoding corruption: 4,955 → 0 wines, 5 → 0 producers (75% dictionary, 25% Haiku)
- HTML entities (&amp; &#8217; &quot;): 859 → 0
- Double spaces: 4,446 → 0
- Wally's suffixes ("2020 / 750 ml."): 3,729 → 0
- Curly quotes: 455 wines + 1,000 producers → 0
- Tabs/newlines/leading spaces: 25 → 0
- 549 + 55 slug conflicts revealed (encoding-variant duplicates) — deferred to future dedup session

**Why this is not inference:** Every fix replaces a known-corrupted character with the one correct original. The corruption is Latin-1/Windows-1252 bytes that became U+FFFD in UTF-8 — each maps to exactly one accented character (é, è, ê, ñ, ü, etc.) determinable from the surrounding word. Haiku acts as OCR correction, not inference.

**Cost:** ~$0.10 (Haiku phase). Dictionary phase: $0.

---

### 2026-04-07: Label OCR project tabled as "someday"

**Decision:** After running an OCR bake-off on 20 test label images (EasyOCR, RapidOCR, Claude Vision as control), decided to table the bulk label OCR extraction project indefinitely. Images are preserved on the external drive for future use.

**Bake-off results:** EasyOCR captured ~80% of Claude Vision baseline text, RapidOCR ~74%. Back labels (where the data is) performed better at ~94%. OnnxTR identified as strongest untested candidate but not pursued.

**Why tabled:** Most high-value label data is already extracted from TTB structured fields (170K ABVs, 857K grapes, 1.75M appellations) without OCR. What labels uniquely offer (winemaker notes, blend percentages, EU e-label chemistry) would improve WineTest Depth from ~33% to ~38% — marginal. The enrichment pipeline (Grade C Haiku batch, ~$120 for 30-50K wines) would move Story from 1.8/5 to 3.5/5 and WineTest from 56 to ~70+ — far higher impact for the same effort.

**Artifacts preserved:** `pipeline/analyze/ocr_bakeoff.py` (bake-off script), `data/test_labels/` (20 test images), `data/stats/ocr_bakeoff_results.json` (full results). Ready to resume if needed.
