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
