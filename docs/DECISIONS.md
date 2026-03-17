# Loam — Decision Log

Append-only. Each entry records a human judgment call and why. Claude adds entries automatically when decisions are made during sessions. Use "log that" to force an entry.

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
