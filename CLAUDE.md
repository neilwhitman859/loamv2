# Loam v2 — Claude Context

Loam is a wine intelligence platform. Users look up a wine and get the full story — place, vintage weather, soil, grapes, producer choices. All the scattered information brought together and connected by AI synthesis. The name is a soil type. Terroir is central.

**Supabase project:** `vgbppjhmvbggfjztzobl` (us-east-1)
**GitHub:** github.com/neilwhitman859/loamv2
**Stack:** Supabase (Postgres), Python pipeline, Anthropic Claude, Open-Meteo, Vite/React frontend

---

## Docs — When to Consult Each

- `docs/SCHEMA.md` — Table-by-table field reference. Read when working with DB structure or writing queries.
- `docs/PRINCIPLES.md` — Product philosophy. Read when making judgment calls about what to build or how.
- `docs/DECISIONS.md` — Append-only log of human decisions with reasoning. Read when you need to understand why something was done a certain way. Never re-litigate settled decisions without the user raising it.
- `docs/VOICE.md` — Voice, tone, and food pairing guidance for all AI-generated content. Read before writing any enrichment prompts or insight content.
- `docs/ENRICHMENT.md` — Letter-grade enrichment architecture (F/D/C/B/A), cost model, on-demand pipeline, wine-not-found flow. Read before building or modifying the enrichment pipeline.
- `docs/SOURCES.md` — Master reference for all external data sources (evaluated, integrated, planned, rejected). Read when working on data acquisition or import pipelines.
- `docs/ROADMAP.md` — Phased development plan. Read at session start to know what phase we're in and what's next.
- `docs/MERGE_STRATEGY.md` — Merge pipeline decisions: Python migration, merge layer sequencing, COLA risks, wine identity definition, AI matching approach, product direction. Read before building merge/matching infrastructure.
- `docs/WORKFLOW.md` — Human-facing session checklist. You don't need to read this, but follow the behavioral instructions below.
- `docs/reference/` — Retired docs kept for historical reference, not actively updated. Includes LWIN_STRATEGY.md (superseded by SOURCES.md + ROADMAP.md), SCHEMA_ASSESSMENT.md (Phase 1a spec, fully executed).

---

## Behavioral Instructions

### Session Briefings
When starting a session or recovering from compaction, give a medium briefing:
```
SESSION BRIEFING
- Last session: [what was accomplished]
- Current DB state: [query the DB for row counts — never rely on hardcoded numbers]
- Open items: [anything left mid-stream]
- Suggested next step: [what makes sense to pick up]
```
Query the database for current state. Do not guess or use stale numbers from this file.

### Auto-Update CLAUDE.md
Update this file at natural breakpoints — after a pipeline run, a schema change, a significant decision, or when wrapping up a session. Tell the user what changed: "Updated CLAUDE.md with [summary]."

### Auto-Log Decisions
When the user makes a judgment call (choosing between options, setting a direction, defining how something should work), append it to `docs/DECISIONS.md` automatically. Notify briefly: "Logged to DECISIONS.md: [one-line summary]."

If the user says **"log that"**, force an entry even if you didn't think it was significant.

### Auto-Update SCHEMA.md
When you modify the database schema (CREATE TABLE, ALTER TABLE, DROP, etc.), update `docs/SCHEMA.md` to reflect the change, including the reasoning.

### Commit at Milestones
When something is important enough to update CLAUDE.md, it's important enough to commit. Commit with a clear message after meaningful milestones.

### Always Recommend
When asking the user a clarifying question, **always give a recommendation**. If the answer is unclear, explain the case for each option. Don't just ask — propose a direction.

### Nudge the User
If the user is going a long stretch without wrapping up, if decisions are being made but not logged, or if a session is ending without updating files — say something. Be direct: "We've made some decisions this session that aren't logged yet. Want me to update DECISIONS.md and CLAUDE.md before we stop?"

---

## Current State

### Supabase Compute
**Small** ($10/mo, 2GB RAM, dedicated CPU) — upgraded from Nano 2026-03-25. Required for `source_ttb_colas` table (4.7GB with indexes). Nano could not complete upserts without statement timeouts. DB total size: 6.6 GB.

### Pipeline Language
**Python** for all data pipeline work (2026-03-20). Node.js retired. All 116 Node.js scripts archived to `scripts_archive/node/` and being converted to Python in `pipeline/`.

Pipeline structure:
- `pipeline/lib/` — shared libraries (db.py, normalize.py, resolve.py, importer.py, merge.py)
- `pipeline/fetch/` — data fetchers and web scrapers
- `pipeline/load/` — staging table loaders
- `pipeline/promote/` — staging → canonical promotion
- `pipeline/enrich/` — AI enrichment scripts
- `pipeline/reference/` — reference data seeding
- `pipeline/geo/` — geographic boundary scripts
- `pipeline/vivino/` — Vivino-specific pipeline (archive/reference)
- `pipeline/analyze/` — analysis and utility scripts

See `docs/MERGE_STRATEGY.md` for rationale.

### Architecture
The database has two layers:
- **Canonical tables** (`producers`, `wines`, `wine_vintages`, etc.) — curated, high-quality data. 78 canonical tables. LWIN promoted as backbone (189K wines, 33K producers). Quality bar is high.
- **source_* staging tables** — per-source raw data for multi-source merge. `source_ttb_colas` (TTB COLA registry, 3.28M records, scrape complete), `source_pro_platform` (346K), `source_lwin` (189K, all promoted), `source_kansas_brands` (65K), `source_tabc` (183K), `source_wv_abca` (55K). Each has merge tracking columns (canonical_wine_id, canonical_producer_id, processed_at).
- **xwines_* tables** — bulk X-Wines dataset dump (~530K wines, ~2.2M vintages, ~32K producers). Kept as reference but not actively maintained. Data quality is lower.

### Reference Tables (complete)
Countries (68), regions (389), appellations (3,662), grapes (9,693 + 34,820 synonyms), varietal categories (161), publications (78), attribute definitions (73), tasting descriptors (304), farming certifications (21), biodiversity certifications (7), soil types (39). All seeded, audited, and cross-validated. See `docs/HISTORY.md` for detail.

### Geographic Data (open for refinement — avoid appellation duplicates)
Geographic boundaries with PostGIS geometry. All geographic data open for refinement. **One rule: don't create appellations that duplicate existing ones** (DECISIONS.md 2026-04-03). Match to what exists.
- Countries: 68/68 (100%). Regions: 323/324 (99.7%). Appellations: 2,847/3,205 (88.8%).
- Appellation containment hierarchy: 2,158 relationships across 19 countries.
- Appellation→region attribution: 96.4% (115 remain on catch-all by design).
- Known gaps: 358 appellations without boundaries, parked restructuring (CH/IT/HR-HU/England L2).
- Sources: UC Davis AVA, Eurac EU PDO, Wine Australia, IPONZ, ldproxy RLP, Nominatim.

### Insights (mostly empty)
Region insights (202), appellation insights (82), country insights (62). All other insight tables empty.

### Schema (Phase 1a/1b complete)
78 canonical tables, 30 staging tables. Schema hardened across 3 rounds (Phase 1a, post-import, scan round 2). All reference data seeded and audited. See `docs/SCHEMA.md` for field reference, `docs/HISTORY.md` for schema change history.

### Content Tables (updated 2026-04-04, post recovery + 10 follow-ups)
Query DB for current counts — these are snapshots. See `docs/HISTORY.md` for promotion/merge event history.
- **~37K producers**, **~497K wines**, **~328K vintages**, **~27K scores**, **~33K prices**, **~188K wine_grapes**, **~492K external_ids** (292K COLA + 18K UPC + 189K LWIN), **~16K entity_classifications**
- **~267K wines with color** (up from 180K post-revert via LWIN backfill)
- **~262K wines with appellation_id** (up from 230K via TTB wine_appellation backfill)
- **~413K wines with region_id** (up from 287K via TTB direct resolve + cascade from appellation)
- **~468K wines with country_id** (up from 466K via cascade)
- **~167K wine_vintages with label image URLs** (restored from TTB after column was wiped)
- **~169K wine_vintages with ABV** (up from 167K via TTB backfill)
- **292K wines linked to TTB** (686K TTB records linked)
- **Wine type:** table 473,232, sparkling 18,757, fortified 4,937 (+4,712 reclassified this session: 4,166 sparkling from TTB class_type_desc, 223 vermouth→fortified, 323 Port/Sherry/Madeira via name+TTB context match)
- **COLA-keyed state merge:** 170K state DB records linked (PRO 84K, TABC 52K, WV 22K, Kansas 13K). +10,136 appellation assignments, +543 vintages.
- **Price coverage:** 3.94% (19,574 distinct wines with prices). Was 3.36% post-revert, peaked at 5.25% pre-revert, ~1% at start of Phase 2. Honest post-phantom-cleanup number — 1,241 phantom NV Wally's rows deleted.
- **Data grade:** F=467,355, D=29,568, C=0, B=3 (5,906 phantom D reclassified to F after revert).
- **Score coverage:** ~2% (distinct wines with scores from TEXSOM +8.5K, Berliner +1.7K, BC Liquor community +592).
- **UPCs:** 17,701 (+5,836 this session from Horizon/PA/OpenFoodFacts + Spec's + LCBO + BC Liquor)
- **Farming certs:** 9,324 (+2,937 from Skurnik/KL/EC/Polaner pattern text matching — legitimate, values are explicit farming terms)
- **Wine depth (2026-04-04):** +1,921 sweetness (Flatiron), +1,158 vine_age (KL), +254 description (Skurnik) — direct source promotions
- **Recovery session (2026-04-04 evening):** +7,707 prices (Wally's title parser extracting leading-year vintage from title), +29,249 wine_grapes (TTB grape promotion re-run, real `grape_varietals` field), +86,015 colors (LWIN `colour` column backfill + 1,681 from importers), +12 prices (Enofile NV for sparkling/fortified). All recoveries from direct source data, zero inference. See DECISIONS.md "Recovery of lost data via authoritative sources only."
- **Follow-up pass (2026-04-04 late):** +3,978 wine_grapes via new `importer_grape_promote.py` (Skurnik, BC Liquor, Flatiron, Berliner, Systembolaget, Winebow, EC, Empson, Domestique, Enofile), +3 prices (Wally's embedded-year tail), -1,241 phantom NV Wally's rows (non-sparkling/fortified wines), -5,906 orphan D wines reclassified to F. Identified 2 canonical data quality bugs to log but not fix autonomously: (A) 66 producers named as appellations create magnet wines, concrete harm limited to ~71 staging rows; (B) batch_matcher `match_wine` collapses distinct wines from same producer via loose substring matching (~170 known collisions across Skurnik/Empson/EC, needs `retail_wine_create` to create missing canonicals). Both documented in DECISIONS.md.
- **Round 2 follow-ups (2026-04-04 very late):** +163,635 label images restored from TTB (0 → 167,164 — CLAUDE.md's 211K figure was actually a TTB count, not a canonical count — the column was wiped at some point, possibly revert collateral). +2,670 ABV values from TTB. +32,135 appellation_id values from TTB wine_appellation (resolver-matched, 229,963 → 262,098). +4,166 sparkling and +223 fortified reclassifications from TTB class_type_desc ('SPARKLING WINE/CHAMPAGNE', 'CARBONATED WINE', 'VERMOUTH/MIXED TYPES'). +68 SF Chronicle Wine Competition medal scores from Enofile (rest of Enofile's ~100 competitions have no matching publication record — skipped).
- **Round 3 follow-ups (2026-04-04 late-late):** +125,844 region_id values (26,441 cascaded from appellation, 99,403 resolved directly from TTB wine_appellation + origin_desc via resolve_region). +323 fortified reclassifications from name-based Port/Sherry/Madeira/Marsala/Banyuls/Maury/Rasteau/Commandaria/VDN match (only when TTB class_type_desc confirmed fortified context). +1,966 wine country_id + +188 producer country_id cascaded from region/appellation. 68 score wine_vintage_id FKs filled (5 needed new NV wine_vintage rows). Explicit SKIP: producer address backfill from TTB applicant fields (applicant is US importer for foreign wines, not producer — risky); first_vintage_year from MIN(wine_vintages.year) (semantically "first vintage producer ever made" ≠ "earliest we have data for").
- **Round 4 follow-ups (2026-04-05 early):** +3,542 wine_grapes via greedy longest-match parser on TTB space-separated blends ("Merlot Cabernet Sauvignon" → Merlot + Cabernet Sauvignon). +6 TA values from Winebow chemistry on matching vintages. TTB vintage creation (0 new — previous rounds already covered). Flatiron sweetness (0 new — already promoted).
- **Round 5 follow-ups (2026-04-05):** +95 age_statement_years on fortified wines parsed from name ("10 Year Tawny Port" → 10). Restricted to wine_type='fortified' AND name matching `N Years (Old|Tawny|Porto|Port|Madeira|Malmsey|Sercial|Verdelho|Bual)` to avoid false positives. KL growers producer depth confirmed fully promoted (107 year, 78 web, 120 desc, 117 GPS, 110 cases — matches source exactly). No remaining gaps in existing importer sources.
- **Round 6 follow-ups (2026-04-05):** +898 LWIN external_ids backfilled (source_lwin had 189,359 matched but only 188,465 had lwin_7 external_id — 898 gap). Search vector rebuild on NULL entries: +2,574 wines, +6,859 producers, +1 appellation. Triggered via `UPDATE ... SET updated_at = updated_at` which re-fires the search_vector trigger. Producer region_id cascade from appellation: 0 fillable (already covered in round 3). Vintage-level LWIN backfill (lwin_11/lwin_18): not possible — source_lwin only has lwin_7, no vintage-level IDs were imported.
- **Round 7 follow-ups (2026-04-05):** +2,160 COLA external_ids backfilled (TTB-linked wines without COLA in external_ids; set-difference revealed more than the simple count suggested — 2,145 orphan COLA entries exist in external_ids for wines no longer in source_ttb_colas). **+62,519 wines.varietal_category_id** via strictly definitional cascade: wines with exactly 1 wine_grapes link, where that grape name matches a varietal_categories name (e.g., Chardonnay grape → Chardonnay category). 0 → 62,519. TEXSOM/Berliner medal coverage verified complete (all competition scores have medals set).
- **Round 8 follow-ups (2026-04-05):** **+435,050 identity_confidence values** cascaded from existing external_ids (unverified 496,471 → lwin_matched 188,908 + cola_matched 234,308 + upc_matched 12,289 + unverified 61,421). Strictly definitional: if external_ids has lwin_7 entry → lwin_matched (precedence: LWIN > COLA > UPC). **+3,184 fortified varietal_category_id** via name match (Port/Sherry/Madeira/Marsala — wines with wine_type=fortified AND name contains unambiguous fortified style keyword, category name matches verbatim). Total varietal_category: 65,703.
- **Round 9 follow-ups (2026-04-05):** **+79,189 wine_grapes.percentage=100** on single-grape wines (strictly definitional — one grape link = 100%). **+9,002 wine_vintage_prices.price_original** copied from price_usd on USD-currency rows (same-currency identity). Infrastructure audit: name_normalized, slug NULLs all 0 across wines+producers (clean). wines.style is a free-text AI enrichment field (0 populated, not a backfill target).
- **Round 10 follow-ups (2026-04-05):** ABV fill from state distribution databases. **+536 from PRO Platform** (vintage-matched, abv in 3-25% range). **+147 from WV ABCA** (has vintage column, matched definitionally). TABC skipped — lacks vintage column, applying ABV across all vintages of a wine would be aggregation. Wally's pairings/body/sweetness columns confirmed empty (columns exist but no data loaded). Total vintages with ABV: 170,162.
- **Round 11 follow-ups (2026-04-05):** **+53,128 wine_label_designations** (0 → 53,128). Name-based word-boundary match of canonical label designation names (93 distinct, length ≥4 chars to exclude ambiguous "Dry"/"VOS"/"Sec") against wines.name via Python regex pipeline. Strictly definitional: if wine name literally contains "Brut Nature" as a word-bounded token, it has that designation. Top designations: Brut 10,429, Reserva 4,981, Riserva 3,391, Vieilles Vignes 3,020, Classico 2,830, Auslese 2,310, Blanc de Blancs 2,129, Kabinett 2,026. Sample of 15 random matches validated 100% clean.
- **Round 12 follow-ups (2026-04-05):** Farming cert gap fills. **+123 wine_farming_certifications** (56 from Skurnik "Certified X" values, 67 from Kermit Lynch certified tags — excluded "practicing" tags which aren't actual certifications). **+78 producer_farming_certifications** (0 → 78) from KL growers farming arrays at producer level. `wine_vintages.chemical_data_source` skipped — UUID FK, provenance wasn't tracked when chemistry was promoted in earlier rounds, can't retroactively attribute.
- **Round 13 follow-ups (2026-04-05):** **+76 first_vintage_year** from Empson's explicit `first_vintage` column (direct source data, not MIN inference — Empson records "first year the producer made this wine" per producer interviews). **+1,803 varietal_category_id** from strict appellation-name == category-name match (Franciacorta 467, Sauternes 389, Prosecco 359, Beaujolais 253, Vinho Verde 213, Madeira 104, Marsala 18). Both strictly definitional. Total varietal_category: 65,703 → 67,506.
- **Round 14 follow-ups (2026-04-05):** **+15,940 wine_vintage_formats** (4,697 → 20,637) from Wally's title size parsing. Definitional: format tokens (750mL, 1.5L, 3L, Magnum, etc.) are literally in Wally's title strings, mapped to bottle_formats reference table by volume. **AUDIT: appellation_vintages table is empty** (0 rows despite schema being ready with gdd/rainfall/harvest_avg_temp/heat_spike_days/diurnal_range/growing_season fields). Open-Meteo integration not yet populated — deferred as external-source integration task, not a conservative direct fill. Other empty tables confirmed: appellation_grapes (0), appellation_rules (0), wine_vintage_vineyards (0), wine_vintage_documents (0), wine_vintage_descriptors (0), wine_vintage_nv_components (0). All require either external data integration or expert knowledge seeding — not conservative direct-source work.
- **Round 15 follow-ups (2026-04-05):** **+5,452 review_date on TEXSOM scores** (17,163 → 22,615) via vintage-matched backfill from source_texsom.year (restricted to pure 4-digit year values, MAX per wine-vintage pair, stored as YYYY-01-01). Berliner backfill blocked by unique constraint `idx_scores_dedup(wine_id, vintage_year, publication_id, critic, review_date)` — 1,656 null-date rows would collide with existing 2,817 dated rows that already have the same (wine, vintage, critic) tuple. Flatiron body column (Medium/Light/Full Bodied, ~2,139 rows) skipped: no canonical text body column exists on wines/wine_vintages — would need wine_vintage_tasting_insights which is the AI enrichment target, not a direct-fill target.
- **Round 16 follow-ups (2026-04-05):** **+538 producer external_ids** (Skurnik 379 slugs, Kermit Lynch 120 grower IDs, Empson 39 slugs). First producer entity_type entries in external_ids — gives canonical producers back-pointers to importer websites. wine_vintage_grapes (per-vintage grape composition) confirmed empty with no source candidates — no source tracks grape percentage variation per vintage year, wine_grapes at wine-level is the correct home for this data.
- **Round 17 follow-ups (2026-04-05):** Reference integrity audit clean (regions/appellations all have country_id, appellations have region_id, 268 grapes without color are edge VIVC gaps). **+1,025 wine_vintage_scores.vintage_year** set to 0 (NV) on rows where `wine_vintage_id` FK already pointed to a wine_vintages(vintage_year=0) row — NULL/0 mismatch between score.vintage_year column and the linked wine_vintages row. Orphan score count went to 0. Strictly definitional: the FK target already said NV, the column just wasn't synchronized.
- **Path A session (2026-04-05): Seed appellation_rules from legal sources.** Provenance columns added (source_url, source_organization, source_document_title, source_accessed_date, source_text_excerpt, last_verified_at) to `appellation_rules` and `appellation_grapes`. **68 appellation_rules seeded** (0 → 68), all with 100% provenance coverage traceable to legal documents. Source breakdown: **40 INAO cahiers des charges** (French AOCs) + **10 MAPA pliegos de condiciones** (Spanish DOPs) + **1 EU eAmbrosia/OJ C** (Chianti Classico via EUR-Lex C/2024/1036) + **12 MASAF-subordinate Italian sources** (Barolo/Brunello/Vino Nobile via Valoritalia; Barbaresco/Langhe via Regione Piemonte; Etna via IRVO; Amarone via Gazzetta Ufficiale; Valpolicella/Soave/Soave Superiore/Bardolino/Prosecco via Regione del Veneto). **401 appellation_grapes rows** carry full structured provenance (up from 109 in initial batch). Legal texts extracted from local PDFs via `pypdf`, saved under `data/legal_sources/` (69 files across 4 batches).
  - **Batch 1 (2026-04-05 morning):** 19 appellations seeded with ~109 grape rows. 19 French AOCs, 4 MAPA + 1 EU eAmbrosia + 7 MASAF-subordinate Italian DOCGs.
  - **Batch 2 (2026-04-05 later):** +12 rules (1 Jumilla from Spain + 11 French: Pauillac, Margaux, Saint-Julien, Graves, Pessac-Léognan, Crozes-Hermitage, Cornas, Condrieu, Morgon, Bandol, Minervois). +97 grape rows.
  - **Batch 3 (2026-04-05 evening):** +11 French AOCs (Pomerol, Saint-Émilion, Saint-Émilion Grand Cru, Sauternes, Barsac, Saint-Estèphe, Saint-Joseph, Côte-Rôtie, Moulin-à-Vent, Fleurie, Vacqueyras) +6 Spanish DOPs (Rueda, Penedès, Navarra, Toro, Bierzo, Somontano). +143 grape rows.
  - **Batch 4 (2026-04-05 night):** +5 Veneto Italian DOC/DOCGs (Valpolicella DOC, Soave DOC, Soave Superiore DOCG, Bardolino DOC, Prosecco DOC — via MASAF-subordinate Regione del Veneto) +4 French AOCs (Corbières, Faugères, Saint-Péray, Cairanne). +52 grape rows. Cascades: Valpolicella 467 red, Soave 242 white, Bardolino 120 red, Saint-Péray 60 white, Soave Superiore 13 white. Discovered 2 pre-existing data quality bugs: Bardolino had wrong GARGANEGA grape row (is red, not white), Prosecco had wrong GARGANEGA grape row (is Glera-based). Both left in place per no-delete rule, correct rows added.
  - **MASAF-subordinate path (breakthrough, allows Italian DOCGs):** catalogoviti.politicheagricole.it/masaf.gov.it subdomain is ECONNREFUSED, but MASAF decrees are mirrored on (a) **EUR-Lex IT PDFs** via the `OJ:C_YYYYNNNNNN` URL pattern for Reg (EU) 2019/33 Art 17 wine PDO modifications, (b) **Valoritalia** (MASAF-designated control body), (c) **Regione Piemonte** (Piedmont regional government), (d) **IRVO** (Sicilian regional wine institute), (e) **Gazzetta Ufficiale della Repubblica Italiana** (gazzettaufficiale.it direct PDFs). All 5 are defensible extensions of the MASAF source.
  - **Notable finding (Pomerol):** The Pomerol CDC authorizes only 5 varieties (Cabernet Franc, Cabernet Sauvignon, Cot/Malbec, Merlot, Petit Verdot) — Carmenère is NOT permitted (unlike Médoc and Saint-Émilion). Pre-existing unsourced Carmenère row in appellation_grapes for Pomerol left in place per no-delete rule, but flagged as superseded per current CDC.
  - **Cascades executed across all 3 batches** (strictly definitional, NULL-fills only): **~2,338 additional wines.color** fills beyond batch 1 (batch 1: 8,973; batch 2: 769 across Pauillac/Margaux/Saint-Julien/Cornas/Morgon/Condrieu; batch 3: ~1,569 across Pomerol/Saint-Émilion/Saint-Émilion GC/Saint-Estèphe/Côte-Rôtie/Moulin-à-Vent/Fleurie/Sauternes/Barsac) **~11,311 total color fills**. **~9,055 wines.varietal_category_id** fills (batch 1 + Cornas→Syrah + Condrieu→Viognier). **~8,215 wine_grapes rows** at percentage=100 for true single-variety appellations (batch 1 + Cornas 300 + Condrieu 316). Cross-check found **~895 wines with legally impossible colors** (batch 1: 855 — 50 Chablis red, 800 Champagne red, 2 Chianti Classico white, 2 Pommard non-red, 1 Gevrey non-red; plus Italian DOCG edge cases: 9 Barolo rose, 5 Barolo white, 1 Chianti Classico rose, 1 Barbaresco rose, 1 Brunello white; batch 2-3 flags: ~7 Sauternes red, 2 Barsac red, 1 each Pomerol/Saint-Émilion/Saint-Émilion GC/Saint-Estèphe/Moulin-à-Vent white) — all left alone per no-overwrite rule, flagged in DECISIONS.md for cleanup session.
- **⚠️ Inference reverts (2026-04-04):** See `docs/DECISIONS.md` entry "No probabilistic inference on canonical columns" and `memory/feedback_no_probabilistic_inference.md`. This session applied 18 inference operations across the canonical tables; 14 were reverted after user caught errors (Blanc de Noirs mis-marked red, Saldo-type multi-region producers wrongly single-region'd, etc.). Reverts had collateral damage (no row-level provenance in the schema): ~44K legit wine_grapes links cleared along with the 81K pattern-inferred ones, ~85K pre-session colors cleared before TTB restoration, 28.6K NV price rows removed (most Wally's vintage data was in titles, never parsed).
- **Kept (strictly definitional/direct):** wine_vintage_id composite-key backfill, region_id from appellations.region_id, country_id from region/appellation, wine_type regulatory reclassification (Champagne→sparkling, Tawny Port→fortified — legal category names), data grade F→D from raw data presence, and all direct staging promotions (not inference).
- Alias tables seeded: 96 region, 75 label designation, 18,631 appellation
- **Depth data now populated** (was all 0): 211K label images, 6.4K farming certs, 4.7K bottle formats, 809 food pairings, 696 descriptions, 449 sweetness, 166 winemakers, 233 pH, 251 TA, 192 RS, 100 fermentation vessels, 106 yeast types, 224 MLF, 166 oak duration, 158 oak origin, 101 closures, 321 production, 88 serving temps, 343 critic_score_avg, +141 importer scores, +681 colors
- **Producer depth** (was all 0): 107 year_established, 78 websites, 117 GPS, 120 descriptions, 110 production, 173 winemaker links
- Sonnet accuracy audit: 96% on 300-sample ($0.05). Non-sparkling data 100% clean.
- Sparkling wine fix applied: 8,977 reclassified. Distribution: table 93.6%, sparkling 4.7%, fortified 1.6%.
- Search: `search_catalog` v2 with unaccent + producer name matching. Findability 12%→83%.

### Multi-Source Merge Infrastructure (2026-03-18)
Staging-first architecture: all external data goes through per-source staging tables, then a match engine promotes to canonical tables. Prevents dedup crisis at scale.

**30 staging tables (~4.35M total rows, audited 2026-03-27):**
- `match_decisions` — audit trail for cross-source matching decisions (AI review, confidence, extracted data)
- **Regulatory/ID sources:**
  - `source_ttb_colas` (3,283,319) — TTB COLA registry. **Scrape complete.** 3.18M detail-scraped (96.8%), 1.82M printable-scraped (99.86% of 001-format). 1.82M label image URLs, 1.75M appellations, 857K grapes, 1.50M vintages, 856K ABV. Non-001 IDs (1.35M) confirmed no printable page on TTB.
  - `source_pro_platform` (346,080) — 12 US states via PRO Platform XLSX. COLA + vintage + appellation + ABV.
  - `source_lwin` (189,359) — LWIN trade identifiers. Fine wine backbone.
  - `source_tabc` (182,933) — Texas TABC via Socrata. 100% TTB numbers, 99.8% ABV. **Refreshed 2026-04-03** (201K API records → 183K unique TTB after dedup, no net new).
  - `source_kansas_brands` (65,476) — KS KDOR. All beverage types; wine subset ~31K. URL moved to new app.
  - `source_wv_abca` (55,093) — West Virginia ABCA. 96.7% TTB IDs. **⚠️ API is dead** (returns empty). Data is archival. Detail scraper cannot run.
- **Competition sources:**
  - `source_berliner` (73,896) — Berliner Wine Trophy. 42 competitions (2009-2026). 100% grapes/country/medal.
  - `source_texsom` (46,896) — TEXSOM. 40 years (1985-2025). Producer, appellation, vintage, medal.
  - `source_enofile` (9,166) — EnofileOnline. Appellation/varietal/price, competition medals.
- **UPC barcode sources:**
  - `source_specs` (21,913) — Spec's Wine. **100% UPC barcodes** (best barcode source). Prices. WooCommerce API may have changed.
  - `source_systembolaget` (12,646) — Sweden monopoly. Barcodes, structured data. API now requires auth.
  - `source_lcbo` (7,030) — Ontario LCBO. UPC barcodes.
  - `source_horizon` (6,441) — Horizon Beverage (SGWS MA/RI). UPC barcodes. **⚠️ API is dead** (404). Data archival.
  - `source_pa` (5,905) — Pennsylvania PLCB. 10,297 UPCs.
  - `source_openfoodfacts` (5,176) — Crowdsourced UPCs, 62% French. **⚠️ Stale** — source now has 16K (3x growth).
  - `source_bc_liquor` (3,200) — BC Liquor. 99.5% UPC, grapes, ABV, tasting notes.
  - `source_winedeals` (3,200) — Retailer. 2,760 with UPC.
- **Importer catalogs:**
  - `source_skurnik` (5,541) — Grapes 100%, appellation 97%.
  - `source_polaner` (1,680) — Deprioritized (metadata-thin).
  - `source_kermit_lynch` (1,468) + `source_kermit_lynch_growers` (193) — Rich metadata.
  - `source_winebow` (536) — Best chemistry data (ABV/pH/acidity/RS).
  - `source_european_cellars` (443) — 100% soil/farming/vinification.
  - `source_empson` (279) — Richest per-wine data (27+ fields).
- **Retailer catalogs:**
  - `source_wallys` (19,446) — Prices, distributor mapping.
  - `source_flatiron` (4,130) — Structured Shopify tags.
  - `source_firstleaf` (1,770) — DTC wine club.
  - `source_best_wine_store` (1,658) — Value retailer.
  - `source_domestique` (247) — Natural wine.
  - `source_last_bottle` (160) — Flash sale prices.

**RPC functions:** `match_producer_fuzzy()`, `match_wine_fuzzy()` — pg_trgm similarity search for the match engine.

**Active Python scripts (all under `pipeline/`):**
- `python -m pipeline.load.staging --source kl,skurnik,...` — loads raw JSON catalogs into staging tables
- `python -m pipeline.promote.staging --source skurnik [--dry-run]` — matches staging → canonical, creates/links records
- `python -m pipeline.promote.lwin [--analyze|--dry-run|--promote]` — LWIN staging → canonical promotion
- `python -m pipeline.load.pro_staging --state ar,co,...` — loads PRO Platform XLSX into staging
- `python -m pipeline.load.tabc_staging` — loads TX TABC into staging
- `python -m pipeline.load.wv_staging` — loads WV ABCA into staging
- `python -m pipeline.load.upc_staging` — loads Open Food Facts, Horizon, WineDeals into staging
- `python -m pipeline.fetch.wv_details` — WV ABCA detail fetcher with resume support (**⚠️ API dead, cannot run**)
- `python -m pipeline.fetch.ttb_image_downloader` — downloads label images from TTB by year range
- `python -m pipeline.analyze.barcode_scanner` — scans downloaded label images for UPC/EAN/QR barcodes
- `python -m pipeline.analyze.db_counts` — row counts across all tables

**Key promotion scripts:**
- `pipeline/promote/batch_matcher.py` — reusable in-memory producer matching with suffix stripping
- `pipeline/promote/retail_promote.py` — UPCs, prices, vintages from matched retailers
- `pipeline/promote/ttb_wine_link_v2.py` — TTB→canonical wine linking
- `pipeline/promote/cola_depth.py` — COLA IDs, vintages, grapes from linked TTB records
- `pipeline/promote/grape_from_helper.py` — TTB grape promotion (handles encoding corruption)
- `pipeline/promote/ttb_producer_relink.py` — normalized producer matching for TTB brands

**Data quality infrastructure:**
- `accuracy_audit` table + `accuracy_audit_daily` view
- `last_validated_at` column + `sample_wines_for_validation(batch_size)` RPC
- Scheduled `data-accuracy-agent` task (currently paused)

See `docs/HISTORY.md` for promotion results, Tier B+C details, competition/retailer linking results.

**Status (2026-04-03):** Data merge paused (progress made, more to do — resume when ready). TTB barcode scan running in separate session.

**Completed (2026-04-03 planning session):**
- Wally's batch_matcher: 743 wines, 6,907 producers matched (no longer crashes solo)
- Spec's/LCBO/Systembolaget re-run: +110 wines (LCBO), +104 wines (Systembolaget), +7,708 producers (Spec's)
- Grape promotion: +3,527 grape links. Fixed `ttb_grape_promote.py` for batched queries (was crashing on per-wine TTB lookups). Only 5,258/50K wines had TTB grape data — most Phase B wines on non-001 records lack grape field.
- Lesson: batch_matcher must run each source in its own process (combined runs hit ConnectionTerminated)

**Completed (2026-04-03 data gaps session):**
- TABC refresh: fetched 201K from Socrata, but 183K unique TTB after dedup — no net new records. Stale flag removed.
- Grape promotion (full catalog): +2,511 grape links (179K → 184K). Fixed U+FFFD encoding corruption in `ttb_grape_promote.py` and `grape_from_helper.py`. Only ~9K wines in TTB have grape data for wines lacking it — 335K wines without grapes simply have no TTB grape_varietals field.
- Phase B wine creation: +6,767 wines from 4,430 producers (471K → 478K wines). Script resume-safe, 0 errors.
- TTB wine linking: refreshed _tmp_wine_match (478K entries), linked 7,964 new TTB records to Phase B wines.
- COLA depth (round 2): +8,089 vintages, +7,256 COLA IDs from newly linked wines.
- Spec's promotion: +139 UPCs, +3,665 prices from linked Spec's records.
- Retail promotion (Flatiron/LCBO/Systembolaget/BC Liquor): +45 UPCs, +93 prices, +1,280 vintages ensured.
- Readiness re-measured: **39/100 avg** (3-run, up from 8/100 on 2026-04-02). Producer findability ~73%, wine findability ~39%, depth ~0.74/4.
- DB password regenerated for psycopg2 connection (was stale).
- **Schema assessment:** The schema is well-designed — 47 empty canonical tables all have matching data in staging sources. The gap is **zero promotion of depth data**, not missing columns. `wine_vintages` has 77 columns, only 2 populated. See `data/stats/2026-04-03.json`.

**Completed (2026-04-03 depth promotion session):**
- ✅ Importer depth: 1,586 wine + 488 vintage updates (Empson/Winebow/EC/KL → fermentation, oak, chemistry, closure, serving temp, production)
- ✅ KL Growers producer metadata: 120 producers with year_established, website, GPS, production, description
- ✅ Label images: 211,266 wines with TTB label_image_url
- ✅ Farming certifications: 6,387 total (845 importer + 5,542 TTB organic)
- ✅ Score rollups: 343 wines with critic_score_avg
- ✅ Winemakers: 166 created, 173 producer-winemaker links
- ✅ Food pairings: 809 structured links + 203 text descriptions from Empson
- ✅ Sweetness: 449 wines from BC Liquor + Systembolaget
- ✅ 6 new schema fields: serving_temperature_low/high_c, fermentation_duration_days, fermentation_temperature_c, training_method, vine_density_per_ha
- New script: `pipeline/promote/importer_depth.py`

**Completed (2026-04-04 price coverage session — TARGET HIT):**
- ✅ wine_vintage_id backfill: 23,208 orphaned prices → 0. Created 9,658 missing vintages, rescued 2,424 NV orphans.
- ✅ Wally's prices: +17,550 (biggest single addition, all NV/USD)
- ✅ Spec's/LCBO/BC Liquor/Systembolaget/PA/FirstLeaf prices via bulk SQL
- ✅ Grape promotion: +7,252 links from Berliner/Flatiron/Systembolaget. Matched via grapes + grape_synonyms.
- ✅ Added 6 new batch_matcher adapters: enofile, domestique, last_bottle, pa, berliner, texsom
- ✅ Enofile: +1,551 new wine matches + prices promoted
- ✅ PA PLCB: +1,615 new wine matches + prices promoted
- ✅ Berliner: +880 new wine matches, +3,717 competition scores promoted
- ✅ TEXSOM: +7,399 new wine matches, +7,726 competition scores promoted
- ✅ WineDeals: +1,769 distinct wines promoted (was matched but never promoted — found during audit)
- ✅ Virginia ABC researched — spirits-only, not useful. Utah DABS backup.
- retail_promote.py REST approach killed in favor of bulk SQL (10x faster, no UPC dedup errors)
- **Session totals:** +24,118 prices, +8,466 scores, +7,252 grapes, +27,673 vintages, +1,533 UPCs
- **Price coverage ~1% → 5.21%** (25,898 distinct wines). Score coverage 1.24% → 2.24%.

**Next steps (resume here):**
1. ✅ **Enrichment pipeline MVP LIVE** — `enrich-wine` Edge Function deployed. Sonnet enrichment on-demand. Tested: ~$0.018/wine, 25s latency. Writes to wine_insights, wine_vintage_tasting_insights, enrichment_log. Updates data_grade to B.
2. ✅ **Price coverage 5%+ hit** (5.21% = 25,898 distinct wines)
3. **Push price coverage to 10%** — Add Utah DABS (~2.9K wines), Kermit Lynch, Skurnik, Winebow importer prices, NH NHSLC, Systembolaget better Swedish producer matching.
4. **Grade C batch pre-warming** — Haiku batch for 30-50K wines (~$120). Build the batch script.
5. **Frontend integration** — Wire up enrichment trigger on wine page load for sub-B wines.
6. **Tier D (fuzzy tail)** — AI-assisted matching for remaining 20K+ unmatched Berliner/TEXSOM records (needs Haiku fuzzy matching).
7. **TTB COLA Phase 3 AI parse** — Haiku on 1.35M non-001 fanciful names (~$10).
8. **Frontend resume** — canonical tables now have real depth + enrichment pipeline live.

### Major Gaps
- **Score coverage 2.24%** — competition sources now matched (Berliner 4.9K/73K, TEXSOM 21.7K/46.9K). Rest require fuzzy matching.
- **Enrichment pipeline live but 2 wines enriched** — need batch pre-warming (Grade C) and frontend integration (Grade B on-demand).
- UPC barcodes: ~13K (barcode scan running should add ~64K)
- ~40 canonical tables still at 0 rows (vineyards, descriptors, wine_relationships, etc.)
- Weather data, soil/water body links, producer_timeline — all empty.

---

## Consumer Frontend (PAUSED 2026-04-01)

**Deployed:** loam.onrender.com (Render static site, auto-deploys from GitHub push)
**Stack:** Vite + React + Tailwind, mobile-first PWA
**Design tokens:** Playfair Display (headings), Inter (body), wine/earth/stone color palettes

**Pages built (all data-dense, structured fact grids, minimal prose):**
- `WinePage` — Full vintage chemistry (ABV, pH, TA, RS, VA, SO2, brix), winemaking details, aging, EU e-label, production, appellation structured fields, producer details, label designations, farming certifications, score table, grape pills, dual maps, identifiers (LWIN, barcode), drink window, other vintages comparison table
- `ProducerPage` — Details grid, farming/biodiversity certifications, aliases, parent/child producer links, region map, wine list with appellations
- `AppellationPage` — Structured fields (established, area, yield, min ABV, aging, elevation, GDD, rainfall, growing season), production rules, terroir (soil/climate/style), map, grape varieties (required/typical), sub-appellations from containment hierarchy, producer list
- `RegionPage` — Region grapes, sub-regions, appellations list, producer list, map, AI terroir
- `GrapePage` — VIVC identity, synonyms with country, parent/child grape links, country/region/appellation associations, wine count
- `CountryPage` — Map, country grapes, region grid, stats
- `VineyardPage` — Site details (elevation, aspect, slope, density), soil types with properties, producer/wine links, map
- `HomePage` — Search bar, `SearchPage` — results

**Routes:** `/wine/:id`, `/producer/:id`, `/appellation/:id`, `/region/:id`, `/grape/:id`, `/country/:id`, `/vineyard/:id`, `/search`, `/`
**Dev tools preserved:** `/data/*` (data explorer), `/dev/*` (schema browser)

**Why paused:** Canonical tables nearly empty — 189K wines but ~1 vintage, ~3 scores, ~1 grape link. Pages render beautifully with data but most show identity-only shells. Need importer re-promotion + COLA merge + enrichment pipeline before more UI work.

**Design principle (Principle #9):** Structured data in DB → structured display in UI. Numbers, dates, percentages, enums displayed as labeled fact grids — never buried in prose.

---

## Current Focus

**Phase 2: Multi-Source Data Population** — Fill canonical tables. See `docs/ROADMAP.md` for full phased plan.

### Strategic Context (updated 2026-03-19)
- **Backbone IDs:** Three identifier systems anchor every wine: **COLA** (US regulatory, ~1.2M labels), **LWIN** (fine wine trade, 189K wines — already in canonical), **UPC** (retail barcode, fragmented sources). All stored in `external_ids`. Cross-referencing Backbone IDs is the primary dedup mechanism. See `docs/SOURCES.md` for the formal definition.
- **Multi-source data strategy:** LWIN (canonical backbone, already loaded) → TTB COLA direct (Phase 1 running) → State DBs (COLA + UPC bridge) → Importer catalogs (enrichment) → COLA Cloud (barcode enrichment) → Retailer sitemaps.
- **Letter-grade enrichment:** F (identity) → D (has scores/prices) → C (batch Haiku) → B (on-demand Sonnet) → A (curated). See `docs/ENRICHMENT.md`.
- **Identity-first, accuracy-first:** User explicitly chose slow/methodical over quick MVP. On-demand enrichment for user searches. Barcodes considered from the start to avoid re-matching later.
- **Vertical slice:** California + Burgundy as first enrichment targets.
- **User lookup triggers B enrichment:** On-demand Sonnet for every search landing on a wine below Grade B. C is batch pre-warming. See ENRICHMENT.md.

### Next Steps (cleaned 2026-04-03)

**All Phase 1 foundation work is complete** (schema, reference data, data acquisition, LWIN promotion, TTB scraping, staging table loads, initial merge passes). See git history for details.

**Active:**
- TTB barcode scan (490K images) — running in separate session
- Data merge paused — see "Next steps (resume here)" in merge infrastructure section

**Upcoming:**
- Link Phase B wines back to TTB (6,767 new wines need canonical_wine_id backlinks)
- Enrichment pipeline MVP (Edge Function + prompts) — next major phase
- COLA-keyed deterministic merge (PRO/TABC/WV/Kansas/barcode → shared COLA numbers, pure SQL)
- Importer catalog merge (10K wines against TTB+LWIN backbone)
- TTB COLA Phase 3 AI parse (Haiku on 1.35M non-001 fanciful names, ~$10) — lower priority
- Remaining importer scrapers (Kysela, Louis/Dressner, Broadbent)
- Frontend resume — after canonical tables have real depth

**Dropped:**
- ~~NJ OPRA request~~ — deprioritized 2026-04-03
- ~~Vinmonopolet follow-up~~ — deprioritized 2026-04-03

### Schema Hardening (complete — see `docs/HISTORY.md` for detail)
3 rounds of hardening applied. Key infrastructure: `set_updated_at()` triggers on 36 tables, `validate_polymorphic_fks()` orphan checker, enrichment_log with cost/model tracking, `appellation_rules` table. `wine_vintage_scores` and `wine_vintage_prices` have `wine_vintage_id` FK (preferred join path). `retailers` table created but 0 rows.

### Technical Debt (pre-frontend)
- **RLS policies:** ✅ COMPLETE. 94/94 canonical tables have RLS enabled (91 original + 3 new tables this session). Policy pattern: `public_read_*` (anon+authenticated SELECT), `service_write_*` (service_role ALL). wine_lookups also has `anon_insert` for anonymous page views.
- **Search infrastructure:** ✅ COMPLETE. `search_vector` tsvector columns + GIN indexes on wines, producers, appellations, regions, grapes. Trigram indexes on all searchable name columns. Auto-update triggers on INSERT/UPDATE. Two RPC functions: `search_catalog(query, limit, entity_types[])` for unified cross-entity search bar, `search_wines(query, filter_*, sort_by, limit, offset)` for filtered wine browse. Both granted to anon+authenticated.
- **API views:** 4 views created: `wine_detail_view`, `producer_detail_view`, `wine_vintage_detail_view`, `wine_search_view`.
- **Alias tables:** ✅ SEEDED. region_aliases (96), label_designation_aliases (75), appellation_aliases (17,558).
- **JSONB metadata:** ✅ CLEAN. All promotable fields moved to proper columns. Remaining metadata is appropriate for JSONB (import provenance, cooperage, clones, narrative notes).
- **Direct Postgres connection:** ✅ `get_conn()` in `pipeline/lib/db.py` via session pooler (psycopg2). Eliminates HTTP/2 ConnectionTerminated crashes. `batch_matcher.py` and `ttb_grape_promote.py` migrated. `get_supabase()` still works for light reads.
- **Nightly agent (Riddler):** Scheduled task at midnight. Validates data, runs promotion scripts if needed, measures readiness, tracks trends in `data/stats/`. Self-improving via journal (`data/stats/agent_journal.md`). ~$2/night Haiku budget for fuzzy matching.
- **Session prompts:** `data/session_prompts/` for passing focused work instructions to new sessions.
- **Migrations in git:** All DDL via Supabase MCP. Need `supabase/migrations/` before multi-developer.
- **FK normalization (partially addressed):** `wine_vintage_scores` and `wine_vintage_prices` now have `wine_vintage_id` FK (backfilled). `wine_vintage_grapes` already had optional `wine_vintage_id`. Legacy `wine_id + vintage_year` columns kept as convenience but `wine_vintage_id` is now the preferred join path.

### Completed Research & Pipelines (see `docs/HISTORY.md` + `docs/SOURCES.md` for detail)
- **Data acquisition:** 17 source categories researched. See `docs/SOURCES.md`.
- **TTB COLA scraping COMPLETE:** 3.28M records. Detail: 3.18M (96.8%). Printable: 1.82M (99.86% of 001-format). Chrome inject architecture (Python + JS). See `docs/HISTORY.md`.
- **50-state survey COMPLETE:** PRO Platform (12 states, 346K COLAs loaded), TABC (183K), WV (55K, API now dead), Kansas (65K). 28 states confirmed dead ends.
- **490K label images downloaded** (~21GB). Barcode scan running in separate session. Test: 18.2% hit rate → projected ~64K COLA→UPC bridges.
- **7 additional fetchers built and loaded:** Spec's, Berliner, TEXSOM, Wally's, Enofile, Flatiron, BC Liquor.
- **Source audit (2026-03-24):** Dead: WV ABCA, Horizon. Stale: TABC +18K, OFF +11K. Healthy: PRO Platform, Kansas, LCBO, BC Liquor, all importers.

### Open Questions (deferred)
- Data freshness strategy (how/when to re-import)
- Data licensing for scores (Wine Spectator, Parker, CellarTracker)
- UPC Data 4 Beverage Alcohol pricing inquiry
- VineRadar API pricing inquiry (vineyard GPS + terroir data)
- Vinmonopolet API key — email sent 2026-03-18, awaiting response
- Southern Hemisphere importer gap (no dedicated importers researched for AU/NZ/AR/CL/ZA)
- COLA Cloud Snowflake data share pricing (for barcode bulk access if email negotiation fails)
- COLA Cloud one-time export email (drafted, not yet sent)
- CT DCP bulk export — call Richard Mindek (860) 713-6229
- NJ POSSE account registration — UPC+COLA data since Jan 2023
- ~~WV ABCA detail scraper~~ — **CANCELLED**, API is dead (2026-03-23 audit confirmed)
- PRO Platform wine-only re-exports — current XLSX files include all beverages, need wine filtering
- Systembolaget/Alko barcode sources — still need investigation
- UPC→price lookup tool — **RESEARCHED**: SerpAPI ($0.01-0.025/lookup), Go-UPC ($75-795/mo), Wine-Searcher API ($250-2K/mo). Decision: don't pay. We have ~82K prices in 13 staging sources already. On-demand SerpAPI at $75/mo is fallback for Grade B enrichment after merge engine runs.
- Wine.com scraping — **BLOCKED**: DataDome 403 on all product pages and API endpoints. 262K sitemap URLs in hand for future slug parsing (Wine.com product IDs). Park until API partnership or DataDome bypass becomes viable.
- Vivino re-scraping — **UNNECESSARY**: API returning 403 (Cloudflare). Apify scrapers work at ~$5-15/10K wines. But xwines_* tables already have 530K wines for validation. Use existing data, don't re-scrape.
- TTB AVA shapefiles at https://www.ttb.gov/ava — research for boundary data
- Tech sheet extraction tool for winery PDFs — design and build

---

## Key Phrases

- **"wrap up"** — End-of-session routine: **consider every doc file** for updates, then commit and push. Go through this checklist — skip only if genuinely nothing changed for that doc:
  - `CLAUDE.md` — always update (current state, what was accomplished)
  - `docs/DECISIONS.md` — append if any decisions were made
  - `docs/ROADMAP.md` — update if phase status or priorities changed
  - `docs/SCHEMA.md` — update if schema changed (CREATE/ALTER/DROP)
  - `docs/SOURCES.md` — update if source status changed (new source, fetcher built, data loaded)
  - `docs/ENRICHMENT.md` — update if enrichment architecture changed
  - `docs/PRINCIPLES.md` — update if product philosophy changed
  - `docs/VOICE.md` — update if tone/content guidance changed
  - `docs/WORKFLOW.md` — update if session workflow changed
- **"log that"** — Force a DECISIONS.md entry.
- **"briefing"** — Give current state summary anytime mid-session.
