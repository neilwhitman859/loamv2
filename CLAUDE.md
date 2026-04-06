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

### Cron Loops — Explicit Request Only
Never create a cron loop or automated recurring task unless the user explicitly says
"create a loop" (or similar: "set up a cron", "run this overnight"). When the user
does request a loop, remind them of this workflow before starting:

```
CRON LOOP PRE-FLIGHT CHECKLIST
1. Read the journal    → data/stats/cron_loop_journal.md (what worked/failed before)
2. Gap analysis        → Query the DB to prove the work actually exists
3. Single focus        → Recommend ONE track (multi-track often wastes cycles)
4. Skip if small       → If there are only a few items, just do them now — no loop needed
5. Build the manifest  → Explicit numbered list: Cycle 1 = X, Cycle 2 = Y, ...
6. Self-termination    → Loop checks done criteria at the START of every cycle
7. User approval       → Show the manifest and get a thumbs-up before creating the cron
```

See `data/session_prompts/cron_loop_template.md` for the full structural template
and `data/stats/cron_loop_journal.md` for past loop outcomes and remaining backlog.

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
78 canonical tables, 31 staging tables. Schema hardened across 3 rounds (Phase 1a, post-import, scan round 2). All reference data seeded and audited. See `docs/SCHEMA.md` for field reference, `docs/HISTORY.md` for schema change history.

### Content Tables (updated 2026-04-04, post recovery + 10 follow-ups)
Query DB for current counts — these are snapshots. See `docs/HISTORY.md` for promotion/merge event history.
- **~42K producers**, **~491K wines**, **~348K vintages**, **~27K scores**, **~140K prices**, **~278K wine_grapes**, **~604K external_ids** (294K COLA + 106K UPC + 189K LWIN + 13K QR URL + 1.4K QR), **~16K entity_classifications**, **14 retailers**
- **~267K wines with color** (up from 180K post-revert via LWIN backfill)
- **~262K wines with appellation_id** (up from 230K via TTB wine_appellation backfill)
- **~413K wines with region_id** (up from 287K via TTB direct resolve + cascade from appellation)
- **~468K wines with country_id** (up from 466K via cascade)
- **~167K wine_vintages with label image URLs** (restored from TTB after column was wiped)
- **~169K wine_vintages with ABV** (up from 167K via TTB backfill)
- **293K wines linked to TTB** (689K TTB records linked)
- **Wine type:** table 473,232, sparkling 18,757, fortified 4,937 (+4,712 reclassified this session: 4,166 sparkling from TTB class_type_desc, 223 vermouth→fortified, 323 Port/Sherry/Madeira via name+TTB context match)
- **COLA-keyed state merge:** 170K state DB records linked (PRO 84K, TABC 52K, WV 22K, Kansas 13K). +10,136 appellation assignments, +543 vintages.
- **Price coverage:** 8.39% (41,187 distinct wines with prices out of 490,933 wines). Was 3.94% pre-session. Gained via: retailers table seeded (13 retailers, 99K prices backfilled with retailer_id + merchant_name), TTB producer bridge (+422 new producers from TTB brand matching), catalog producer creation (+2,915 producers from curated sources: Enofile/Systembolaget/WineDeals/BestWineStore/Domestique/Flatiron), retail_wine_create expanded (6 new source adapters), bulk price promotion from all matched sources. Utah DABS fetched (2,834 wines, state monopoly pricing) and FirstLeaf matched (458 producers). PA PLCB producer creation (+474 producers via TTB bridge + catalog). Re-run sweeps on Spec's/Wally's/LCBO/BC Liquor with expanded producer index. Was 3.36% post-revert, peaked at 5.25% pre-revert, ~1% at start of Phase 2.
- **Data grade:** F=467,355, D=29,568, C=0, B=3 (5,906 phantom D reclassified to F after revert).
- **Score coverage:** ~2% (distinct wines with scores from TEXSOM +8.5K, Berliner +1.7K, BC Liquor community +592).
- **UPCs:** 117,250 across 80,618 wines (TTB label/scan barcode scanning + staging source backfill, 2026-04-06). Also 12,529 QR URLs + 1,390 QR codes. 404 fake UPCs cleaned. 466 high-confidence duplicate wine pairs identified via shared UPCs (same producer, >0.6 name similarity) — logged for merge session.
- **Name cleanup (2026-04-06):** 10,877 wine+producer names fixed. U+FFFD encoding corruption: 4,960 → 0 (dictionary + Haiku ~$0.10). HTML entities: 859 → 0. Double spaces: 4,446 → 0. Wally's suffixes: 3,729 → 0. Curly quotes: 1,455 → 0. Scripts: `pipeline/analyze/name_cleanup.py` (deterministic, 5-pass) + `pipeline/analyze/name_cleanup_haiku.py` (Haiku long-tail). 604 slug conflicts revealed encoding-variant duplicates — deferred to dedup session.
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
- **Path A session (2026-04-05): Seed appellation_rules from legal sources.** Provenance columns added (source_url, source_organization, source_document_title, source_accessed_date, source_text_excerpt, last_verified_at) to `appellation_rules` and `appellation_grapes`. **100 appellation_rules seeded** (0 → 100 — reached milestone in batch 8), all with 100% provenance coverage traceable to legal documents. Source breakdown: **40 INAO cahiers des charges** (French AOCs) + **10 MAPA pliegos de condiciones** (Spanish DOPs) + **1 EU eAmbrosia/OJ C** (Chianti Classico) + **19 MASAF-subordinate Italian sources** (Barolo/Brunello/Vino Nobile/Bolgheri via Valoritalia; Barbaresco/Langhe via Regione Piemonte; Etna via IRVO; Amarone via Gazzetta Ufficiale; Valpolicella/Soave/Soave Superiore/Bardolino/Prosecco via Regione del Veneto; Taurasi/Fiano di Avellino/Greco di Tufo via Regione Campania; Vernaccia di San Gimignano/Morellino di Scansano/San Gimignano via Valoritalia) + **11 IVV Portugal** (Bucelas, Colares, Carcavelos, Bairrada, Vinho Verde, Dão, Alentejo, Tejo, Madeira, Douro, Porto) + **14 BML Austria via Bundeskellereiinspektion** (Wachau, Kamptal, Kremstal, Traisental, Wagram, Weinviertel, Carnuntum, Thermenregion, Neusiedlersee, Leithaberg, Mittelburgenland, Eisenberg, Südsteiermark, Weststeiermark). **606 appellation_grapes rows** carry full structured provenance (up from 109 in initial batch). Legal texts extracted from local PDFs via `pypdf`, saved under `data/legal_sources/` (90+ files across 8 batches).
  - **Batch 1 (2026-04-05 morning):** 19 appellations seeded with ~109 grape rows. 19 French AOCs, 4 MAPA + 1 EU eAmbrosia + 7 MASAF-subordinate Italian DOCGs.
  - **Batch 2 (2026-04-05 later):** +12 rules (1 Jumilla from Spain + 11 French: Pauillac, Margaux, Saint-Julien, Graves, Pessac-Léognan, Crozes-Hermitage, Cornas, Condrieu, Morgon, Bandol, Minervois). +97 grape rows.
  - **Batch 3 (2026-04-05 evening):** +11 French AOCs (Pomerol, Saint-Émilion, Saint-Émilion Grand Cru, Sauternes, Barsac, Saint-Estèphe, Saint-Joseph, Côte-Rôtie, Moulin-à-Vent, Fleurie, Vacqueyras) +6 Spanish DOPs (Rueda, Penedès, Navarra, Toro, Bierzo, Somontano). +143 grape rows.
  - **Batch 4 (2026-04-05 night):** +5 Veneto Italian DOC/DOCGs (Valpolicella DOC, Soave DOC, Soave Superiore DOCG, Bardolino DOC, Prosecco DOC — via MASAF-subordinate Regione del Veneto) +4 French AOCs (Corbières, Faugères, Saint-Péray, Cairanne). +52 grape rows. Cascades: Valpolicella 467 red, Soave 242 white, Bardolino 120 red, Saint-Péray 60 white, Soave Superiore 13 white. Discovered 2 pre-existing data quality bugs: Bardolino had wrong GARGANEGA grape row (is red, not white), Prosecco had wrong GARGANEGA grape row (is Glera-based). Both left in place per no-delete rule, correct rows added.
  - **Batch 5 (2026-04-05 loop-cycle 1):** +3 Italian Campania DOCGs via Regione Campania (Taurasi DOCG Aglianico 85%+, Fiano di Avellino DOCG Fiano 85%+, Greco di Tufo DOCG Greco+Coda di Volpe). +7 grape rows. Cascades: Taurasi 147 red, Fiano di Avellino 115 white, Greco di Tufo 111 white. Audit round: 0 duplicates, 100% provenance; 21 "color mismatch" flags verified as legitimate accessory field-blend grapes per CDC texts (Viognier in Côte-Rôtie, Aligoté/Chardonnay/Melon in Beaujolais crus, etc.); 2 "red grape in white-only rule" = Sauvignon Gris grapes.color='red' data quality bug (Sauvignon Gris is actually a pink-berried white mutation). ~975 pre-existing illegal wine colors catalogued (Champagne red 800, Chablis red 50, etc.) — NOT session-caused. New doc: `docs/PATH_A_ROLLBACK.md` with per-migration rollback SQL.
  - **Batch 6 (2026-04-05 loop-cycle 2):** +5 Portuguese DOPs via IVV Portugal (Bucelas, Colares, Carcavelos, Bairrada, Vinho Verde). +59 grape rows. Cascade: Bucelas 5 white only (rest deferred — multi-color). Discovered 3 more pre-existing data quality bugs: Bucelas, Colares, Carcavelos all had phantom TOURIGA NACIONAL grape rows (wrong — none of them authorize Touriga Nacional per caderno). Left in place per no-delete rule, correct rows added. Also added Ramisco for Colares (signature grape, mentioned in caderno section 7 Dados sobre o produto but not in section 6 OIV list — handled conservatively by citing section 7 text).
  - **Batch 7 (2026-04-05 loop-cycle 3):** +6 more Portuguese DOPs (Dão, Alentejo, Tejo, Madeira, Douro, Porto). +71 grape rows. Dão includes the 3-tier casta system (category 1 principals for post-1993 plantings, categories 2-3 with restrictions). Madeira seeded with the 4 classic "noble" grapes (Sercial/Verdelho/Boal/Malvasia) + Terrantez. Douro and Porto both seeded with the 5 "castas nobres" for red (Touriga Nacional, Touriga Franca, Tinta Roriz, Tinto Cão, Tinta Barroca) + key whites (Viosinho, Rabigato, Gouveio, Malvasia Fina, Arinto). Totals: 82 rules (+6), 538 grape rows with provenance (+71). Tinta Negra (workhorse Madeira grape) skipped — our grapes table has NEGRA MOLE variants but not "TINTA NEGRA" specifically. Deferred: DO Setúbal (caderno in hand but our appellation is "Península de Setúbal" which is the broader IGP, not DO Setúbal — different scope).
  - **Batch 8 (2026-04-05 loop-cycle 4):** **Milestone: 100 appellation_rules reached.** +14 Austrian DACs (Wachau, Kamptal, Kremstal, Traisental, Wagram, Weinviertel, Carnuntum, Thermenregion, Neusiedlersee, Leithaberg, Mittelburgenland, Eisenberg, Südsteiermark, Weststeiermark — via Bundeskellereiinspektion, BML-subordinate) + 4 Italian Tuscan DOCs (Bolgheri, Vernaccia di San Gimignano DOCG, Morellino di Scansano DOCG, San Gimignano DOC — via Valoritalia). +68 grape rows. Cascades: Kamptal/Kremstal/Traisental/Weinviertel/Südsteiermark → white; Neusiedlersee/Mittelburgenland/Morellino di Scansano → red; Vernaccia di San Gimignano → white. Weinviertel 100% Grüner Veltliner + Mittelburgenland 100% Blaufränkisch wine_grapes cascades executed (137 new wine_grapes rows at percentage=100). Wien DAC and Vulkanland Steiermark DAC URLs returned HTML 404 — deferred. Rosalia DAC and Ruster Ausbruch DAC don't exist as appellations in our DB — deferred. Chianti (base, non-Classico) — not yet seeded.
  - **Batch 9-10 (2026-04-05 loop-cycle 5-6):** +18 rules across remaining French AOCs, additional MAPA sources. Rules: 118.
  - **MASAF catalogoviti BREAKTHROUGH (batch 11, 2026-04-05):** Discovered that `catalogoviti.politicheagricole.it` subdomain is back online and returns disciplinari PDFs directly via URL pattern `/scheda_denom.php?t=dsc&q={numeric_id}`. Built `scripts/sweep_masaf_catalogoviti.py` and swept q=1001-1100 (70 DOCG hits) + q=2000-2500 (328 DOC hits) = **398 Italian DOC/DOCG PDFs fetched in one sweep**. Index saved to `data/legal_sources/_masaf_catalogoviti_index.json`.
  - **Batch 11 (2026-04-05 loop-cycle 7): 22 Italian DOCs/DOCGs seeded from MASAF sweep.** Rules: 118 → 140. Grape rows: 638 → 673. Seeded: Aglianico del Vulture, Barbera d'Alba, Gattinara, Primitivo di Manduria, Montepulciano d'Abruzzo, Verdicchio dei Castelli di Jesi, Verdicchio di Matelica, Lugana, Cesanese del Piglio, Trebbiano d'Abruzzo, Cerasuolo di Vittoria, Frascati Superiore, Offida, Conegliano Valdobbiadene Prosecco, Montefalco, Trento, Alta Langa + 5 umbrella DOCs (Alto Adige, Trentino, Collio Goriziano, Friuli Isonzo, Colli Tortonesi — rule text only, no grape seeding due to multi-subtype complexity). **Cascades**: 2,190 wines.color fills (7 red cascades + 5 white cascades), 10 varietal_category_id cascades (Aglianico/Barbera/Nebbiolo/Primitivo/Montepulciano/Verdicchio/Trebbiano/Prosecco categories), 79 new wine_grapes rows at percentage=100 for Aglianico del Vulture (only true 100% single-variety in the batch). Pre-existing illegal colors (2 Barbera d'Alba white, 5 Verdicchio Jesi red, 7 Trebbiano d'Abruzzo red, etc.) left intact per no-overwrite rule. Rollback SQL added to `docs/PATH_A_ROLLBACK.md`.
  - **Batch 12 (2026-04-05 loop-cycle 8): 17 more Italian DOCs/DOCGs from MASAF sweep.** Rules: 140 → 157. Grape rows: 673 → 688. Seeded: Rosso di Montalcino, Nebbiolo d'Alba, Dolcetto d'Alba, Cannonau di Sardegna, Romagna Albana DOCG, Valpolicella Ripasso, Salice Salentino, Faro, Vin Santo del Chianti Classico, Gioia del Colle, Monferrato DOC, Sannio, Valle d'Aosta, Abruzzo DOC, Cortona, Colline Novaresi, Marsala. Cascades: 1,432 wine color fills (6 red + 1 white), 2 varietal_category_id cascades (Rosso di Montalcino → Sangiovese, Nebbiolo d'Alba → Nebbiolo), wine_grapes 100% cascades for Rosso di Montalcino / Nebbiolo d'Alba / Dolcetto d'Alba (true 100% single-variety appellations).
  - **Batch 13 (2026-04-05 loop-cycle 9): 24 more Italian DOCs/DOCGs from MASAF sweep.** Rules: 157 → 181. Grape rows: 688 → 717. Seeded: Sicilia DOC, Romagna DOC, Recioto della Valpolicella DOCG, Venezia DOC, Cerasuolo d'Abruzzo, Colli Piacentini, Bianco di Custoza, Rosso di Montepulciano, Garda DOC, Carso, Vesuvio, Castel del Monte, Erbaluce di Caluso DOCG, Orvieto, Ischia, Ghemme DOCG, Montecucco, Sforzato di Valtellina DOCG, Vittoria DOC, Lacrima di Morro d'Alba, Valtellina Rosso, Colli di Luni, Lambrusco di Sorbara DOC, Lambrusco Grasparossa di Castelvetro. Cascades: 6 red + 3 white color cascades, 5 varietal_category_id cascades (Sangiovese/Nebbiolo/Montepulciano), 1 wine_grapes 100% cascade (Erbaluce di Caluso). Cerasuolo d'Abruzzo skipped for color cascade (rosato).
  - **Batch 14 (2026-04-05 loop-cycle 10): 21 more Italian DOCs/DOCGs from MASAF sweep.** Rules: 181 → 202. Grape rows: 717 → 739. Seeded: Brachetto d'Acqui DOCG, Carmignano DOCG, Ruchè di Castagnole Monferrato DOCG, Ramandolo DOCG, Vernaccia di Serrapetrona DOCG, Dolcetto di Diano d'Alba, Cinque Terre / Sciacchetrà, Colli Bolognesi, Rosso Piceno, Sant'Antimo, Breganze, Riviera Ligure di Ponente, Cirò, Grignolino d'Asti, Cortese dell'Alto Monferrato, Copertino, Malvasia delle Lipari, Monica di Sardegna, Nuragus di Cagliari, Freisa d'Asti, Aleatico di Puglia. Asti + Barbera d'Asti skipped (already seeded). Brunello di Montalcino already seeded via Valoritalia. Cascades: 8 red + 5 white color cascades, 1 varietal_category_id (Carmignano → Sangiovese), 3 wine_grapes 100% cascades (Dolcetto di Diano d'Alba, Ramandolo, Freisa d'Asti).
  - **Batch 15 (2026-04-05 loop-cycle 11): 17 Burgundy AOCs from INAO CDCs.** Rules: 202 → 219. Grape rows: 739 → 770. Seeded: Beaune, Chambolle-Musigny, Corton, Santenay, Saint-Aubin, Morey-Saint-Denis, Mercurey, Marsannay, Vosne-Romanée, Rully, Pernand-Vergelesses, Clos de Vougeot, Aloxe-Corton, Corton-Charlemagne (grand cru, white only), Savigny-lès-Beaune, Auxey-Duresses, Echezeaux (grand cru, red only). 17 new CDC PDFs fetched from INAO extranet (extranet.inao.gouv.fr/fichier/PNOCDC*.pdf with varying naming patterns — PNOCDC{Name}.pdf, PNOCDC-{Name}.pdf, PNOCDC{NameCompressed}.pdf). Cascades: 2 red (Chambolle-Musigny, Vosne-Romanée, Echezeaux) + 1 white (Corton-Charlemagne) color fills, 4 Pinot Noir/Chardonnay varietal_category_id cascades.
  - **Batch 16 (2026-04-05 loop-cycle 12): 11 major French AOCs from INAO CDCs.** Rules: 219 → 230. Grape rows: 770 → 797. Seeded: Alsace, Bordeaux, Côtes du Rhône, Arbois (Jura), Chinon, Touraine, Saumur, Languedoc, Beaujolais, Côtes du Rhône Villages, Coteaux Champenois. Fetched 11 new CDC PDFs across INAO URL patterns (PNOCDC{Name}.pdf, PNOCDC-{Name}.pdf, 3-CDC-{Name}.pdf, PNO-CdcArbois-cn220210.pdf, CPAOV-2017-224-Chinon.pdf). No color cascades (all multi-color umbrellas). Grape rows seeded for primary varieties only.
  - **Batch 32 (2026-04-05 loop-cycle 28): 16 small distinctive appellations.** Rules: 533 → 549. Seeded: France (Romanée-Saint-Vivant GC, Pouilly-Loché Chardonnay, Clairette de Die méthode ancestrale, Muscadet Côtes de Grandlieu 100% Melon, Moulis-en-Médoc, Coteaux de l'Aubance sweet Chenin, Palette micro-AOC), Spain (La Palma volcanic, Gran Canaria, Pla de Bages Sumoll+Picapoll indigenous), Italy (Colli Perugini, Colli Maceratesi Maceratino/Ribona 70%+, Valli Ossolane alpine Nebbiolo, Montescudaio), Moldova (Codru), Portugal (DOC Setúbal Moscatel fortified 67%+). **Running totals: 549 appellation_rules, 17 countries.**
  - **Batch 31 (2026-04-05 loop-cycle 27): 16 more across 7 countries — 533 rules, 17 countries (added Moldova).** Seeded: Austria (Weinland zone, Vulkanland Steiermark DAC), Spain (Valle de la Orotava volcanic, Arribes Juan García, Cebreros old-vine Garnacha), Portugal (Minho IG, Pico Azores volcanic UNESCO), Moldova (Valul lui Traian Fetească Neagră), Italy (Colli Asolani Prosecco DOCG, Cilento, Modena Lambrusco, Terre Alfieri Arneis+Nebbiolo, Val d'Arno di Sopra, Lago di Corbara, Valdadige, Monreale). **Running totals: 533 appellation_rules, 17 countries.**
  - **Batch 30 (2026-04-05 loop-cycle 26): 17 more across 6 countries — 517 rules.** Seeded: Spain (Vinos de Madrid, Mallorca IGP, Alella, Málaga, Binissalem, Condado de Huelva), Portugal (Alentejano, Lisboa, Beira Interior), Greece (Paros), Slovenia (Kras Teran), France (Bellet, Blaye), Italy (Alghero, Colli Pesaresi, Todi, Colli Martani). **Running totals: 517 appellation_rules, 16 countries.**
  - **Batch 29 (2026-04-05 loop-cycle 25): 23 more Italian/French/Spanish — 500 RULES MILESTONE.** Rules: 477 → 500. Seeded 8 Italian DOCs (Colli Euganei umbrella, Colli Berici Tai Rosso, Bardolino Superiore DOCG, Montecucco Sangiovese DOCG 90%+, Vin Santo di Montepulciano, Gutturnio Barbera+Croatina, Lessini Durello Durella 85%+ sparkling, Valtènesi Groppello, Cesanese di Olevano Romano, Canavese) + 10 French AOCs (Anjou Villages, Rosé d'Anjou, Crémant de Bordeaux, Côte de Beaune, Corbières-Boutenac, Marcillac Fer Servadou 90%+, Saint-Mont Tannat, Côtes du Marmandais, Saint-Pourçain, Bugey) + 3 Spanish DOPs (Tarragona, Ribera del Guadiana, Pla i Llevant Mallorcan indigenous). **MILESTONE: 500 appellation_rules. 0 duplicates. 100% provenance. 16 countries.**
  - **Batch 28 (2026-04-05 loop-cycle 24): 23 more Italian DOCs — 477 rules.** Seeded: Nizza DOCG (100% Barbera), Campi Flegrei (Falanghina+Piedirosso), Frascati base DOC, Recioto di Soave DOCG (Garganega passito), Rosso Cònero + Cònero DOCG (Montepulciano 85%+), Falerio (Marche white), Falerno del Massico (Aglianico+Falanghina), Grignolino del Monferrato Casalese (90%+), Aglianico del Vulture Superiore DOCG (100%), Tintilia del Molise (95%+ indigenous), Gambellara (Garganega 80%+), Torgiano (Sangiovese), Bianchello del Metauro (95%+ Biancame), Colline Lucchesi (Tuscan Sangiovese), Costa d'Amalfi (Falanghina+Piedirosso), Eloro (Nero d'Avola 80%+), Castelli di Jesi Verdicchio Riserva DOCG, Pomino (Chard+Sangiovese), Matera (multi-subtype), Spoleto (Trebbiano Spoletino 85%+), Friuli Aquileia (umbrella 85%+), Menfi (Sicily umbrella). Cascades: 5 red + 7 white + 6 varietal_category. **Running totals: 477 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance.**
  - **Batch 27 (2026-04-05 loop-cycle 23): 27 more across 8 countries — 454 rules, 16 countries.** Seeded: Austria (Wien DAC Gemischter Satz), Switzerland (La Côte Chasselas, Ticino Merlot, Graubünden Pinot Noir/Completer), Spain (Conca de Barberà Trepat, Méntrida Garnacha, Monterrei Godello+Mencía, Bizkaiko+Arabako Txakolina, Valdepeñas Tempranillo, Bullas Monastrell, Lanzarote Malvasía Volcánica), France (Bonnezeaux + Jasnières 100% Chenin, Sainte-Croix-du-Mont sweet, Ajaccio Sciacarello+Vermentino, Saint-Nicolas-de-Bourgueil Cab Franc, Chambertin-Clos de Bèze + Bonnes-Mares + Bâtard-Montrachet GCs, Blagny, Listrac-Médoc, Canon Fronsac, Irouléguy Tannat), Georgia (Manavi 100% Mtsvane white), Slovenia (Slovenska Istra Malvazija+Refošk), Hungary (Somlói volcanic Furmint+Juhfark). Cascades: 4 red + 6 white + 3 varietal_category. **Running totals: 454 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance, 16 countries.**
  - **Batch 26 (2026-04-05 loop-cycle 22): 19 more across Spain/Austria/Switzerland/Portugal/Czechia/France/Italy — 427 rules, 16 countries.** Seeded: Cava (EU sparkling), Manzanilla (Sherry sub-type), Montilla-Moriles (PX fortified), Terra Alta (Garnacha Blanca), Yecla (Monastrell), Empordà, Cigales, Manchuela (Bobal), Getariako Txakolina (Basque), Alicante (Monastrell+Moscatel), Cariñena (Aragón), Burgenland + Niederösterreich (Austrian generic zones), Lavaux (Swiss Chasselas UNESCO), Península de Setúbal (Portuguese Moscatel), Morava (Czech), Quarts de Chaume (Loire 100% Chenin sweet), Vin Santo del Chianti, Piave DOC (Veneto). **Running totals: 427 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance, 16 countries (added Czech Republic).**
  - **Batch 25 (2026-04-05 loop-cycle 21): 20 more French AOCs + 3 Italian DOCs — 400+ milestone.** Rules: 389 → 408. Seeded: 3 Loire (Coteaux du Giennois, Cour-Cheverny 100% Romorantin — only AOC in world, Coteaux du Loir), 5 Bordeaux (Graves de Vayres, Saint-Georges/Puisseguin/Lussac-Saint-Emilion, Loupiac sweet), Chorey-lès-Beaune, Ruchottes-Chambertin GC, Les Baux de Provence, Muscat de Rivesaltes VDN 100% Muscat, Pineau des Charentes (fortified), Pacherenc du Vic-Bilh (Gros/Petit Manseng), Macvin du Jura, Crémant de Limoux, Fitou red, La Clape, Noto DOC (Sicily Nero d''Avola), Reggiano DOC (Lambrusco umbrella), Orcia DOC (Tuscany Sangiovese). Cascades: 4 red + 4 white + 1 varietal_category. **Running totals: 408 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance, 15 countries.**
  - **Batch 24 (2026-04-05 loop-cycle 20): 39 more French AOCs + Swiss Valais.** Rules: 350 → 389. Seeded: Swiss Valais AOC (Fendant/Chasselas + Pinot Noir + indigenous Alpine varieties) + 4 Burgundy GCs (Clos Saint-Denis, Richebourg, Chapelle-Chambertin, Griotte-Chambertin) + 7 Burgundy village/regional (Côte de Nuits-Villages, Irancy PN+César, Passe-tout-grains Gamay+PN, Bouzeron 100% Aligoté, Saint-Bris 100% Sauvignon, Pouilly-Vinzelles Chardonnay, Mâcon) + 4 Beaujolais crus (Juliénas, Chénas, Côte de Brouilly, Régnié) + 3 Loire (Menetou-Salon, Quincy 100% SB, Reuilly) + 7 Languedoc-Roussillon (CDR Villages red, Picpoul de Pinet 100% Picpoul, Saint-Chinian, Limoux, Fitou red, La Clape, Rivesaltes VDN, Maury VDN) + 2 Rhône (Vinsobres red, Beaumes de Venise red+Muscat VDN) + 4 Bordeaux (Côtes de Bordeaux red, Cadillac sweet, Lalande-de-Pomerol red, Sainte-Foy-Bordeaux) + 3 SW France (Jurançon Gros/Petit Manseng white, Monbazillac sweet, Fronton Négrette 50%+) + Patrimonio Corsica (Nielluccio/Vermentino 90%+) + Roussette de Savoie (100% Altesse) + L'Étoile Jura white. Cascades: 16 red + 11 white + 6 varietal_category. **Running totals: 389 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance, 15 countries.**
  - **Batch 23 (2026-04-05 loop-cycle 19): 12 Eastern European + Mediterranean PDOs.** Rules: 338 → 350. Seeded: Hungary (Villány, Eger Bikavér, Sopron), Greece (Mantinia 100% Moschofilero white, Rapsani Xinomavro+Krassato+Stavroto red), Georgia (Kakheti umbrella Saperavi+Rkatsiteli, Khvanchkara Alexandrouli+Mujuretuli semi-sweet red), Slovenia (Goriška Brda umbrella Rebula/Chardonnay orange, Vipavska dolina Zelen+Pinela indigenous), Romania (Dealu Mare Fetească Neagră, Cotnari sweet white), North Macedonia (Tikveš Vranec). Sources: EU eAmbrosia PDO register, national wine laws (Hungarian Wine Act, Greek ΥΠΑΑΤ, Georgian National Wine Agency, Slovenian MoA, Romanian ONVPV). Cascades: 2 red + 2 white single-color. **Running totals: 350 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance. Now covering 14 countries.**
  - **Batch 22 (2026-04-05 loop-cycle 18): 12 more Italian DOCs/DOCGs from MASAF.** Rules: 326 → 338. Seeded: Carema (Nebbiolo 85%+), Curtefranca (Franciacorta still), Pantelleria (Zibibbo/Moscato), Teroldego Rotaliano (100% Teroldego), Boca (Nebbiolo 70-90%), Aglianico del Taburno DOCG (Aglianico 85%+), Verduno Pelaverga (Pelaverga piccolo 85%+), Lessona (Nebbiolo 85%+), Bramaterra (Nebbiolo 50-80% + Croatina + Uva Rara), Carignano del Sulcis (Carignano 85%+), Rossese di Dolceacqua (Rossese 95%+), Falanghina del Sannio (Falanghina 85%+ white). Cascades: 7 red + 1 white color; 2 Nebbiolo varietal_category; Teroldego Rotaliano 100% wine_grapes. **Running totals: 338 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance.**
  - **Batch 21 (2026-04-05 loop-cycle 17): 36 more French AOCs — mass seeding across all regions.** Rules: 290 → 326. Seeded 5 Burgundy GCs (Mazis-Chambertin, Chablis Grand Cru, Montrachet, Chevalier-Montrachet, Musigny) + 4 Burgundy villages (Monthélie, Vougeot, Saint-Véran, Viré-Clessé) + 2 Beaujolais crus (Saint-Amour, Chiroubles) + 5 Bordeaux (Côtes de Blaye, Haut-Médoc, Fronsac, Entre-deux-Mers white, Bourg) + 5 Loire (Muscadet Sèvre et Maine 100% Melon, Coteaux du Layon 100% Chenin sweet, Savennières 100% Chenin, Crémant de Loire, Cheverny) + 4 Rhône (Ventoux, Lirac, Luberon, Tavel rosé-only) + 4 Languedoc-Roussillon (Côtes du Roussillon, Collioure, Banyuls VDN, Terrasses du Larzac) + 3 SW France (Cahors 70%+ Malbec, Madiran 60%+ Tannat, Gaillac) + Vin de Savoie umbrella + Crémant du Jura + Coteaux Varois en Provence + Vin de Corse (Nielluccio+Sciacarello+Vermentino). Cascades: 7 red + 5 white + 1 rosé (Tavel) single-color; 7 varietal_category_id. **Running totals: 326 appellation_rules, 829 appellation_grapes, 0 duplicates, 100% provenance.**
  - **Batch 20 (2026-04-05 loop-cycle 16): 29 more French AOCs.** Rules: 261 → 290. Seeded 10 Burgundy villages/grand crus (Givry, Saint-Romain, Fixin, Ladoix, Montagny white-only, Maranges, Charmes-Chambertin GC, Chambertin GC, Petit Chablis white-only, Clos de la Roche GC) + Bourgogne Aligoté (100% Aligoté white-only) + Coteaux Bourguignons (Gamay/Pinot/Chard/Aligoté) + Crémant de Bourgogne (sparkling) + Brouilly (Beaujolais cru, Gamay red) + Saumur-Champigny (Cab Franc red) + Bourgueil (Cab Franc red/rosé) + Montlouis-sur-Loire (100% Chenin Blanc white) + Anjou (umbrella) + Bordeaux supérieur (red) + Médoc (red) + Montagne-Saint-Emilion (red) + Hermitage (Syrah red, Marsanne+Roussanne white) + Rasteau (Grenache+VDN) + Côtes du Jura (Jura varieties) + Côtes de Provence (rosé dominant) + Bergerac (Bordeaux-adjacent) + Crémant d'Alsace (sparkling) + Costières de Nîmes (southern Rhône) + Coteaux d'Aix-en-Provence. Fetched 22 new CDC PDFs from INAO extranet. Cascades: 7 red + 3 white strictly single-color; 5 varietal_category_id (Pinot Noir / Chardonnay). **Running totals: 290 appellation_rules, 829 appellation_grapes with full provenance, 0 duplicates, 100% provenance coverage.**
  - **Batch 19 (2026-04-05 loop-cycle 15): Hungarian Tokaj + 3 Greek PDOs.** Rules: 257 → 261. Grape rows: 821 → 829. Seeded: Tokaj/Tokaji (6 authorized varieties: Furmint ~60%, Hárslevelű ~30%, Sárgamuskotály, Kabar, Kövérszőlő, Zéta — white only), Nemea (100% Agiorgitiko, red only — rosé prohibited per PDO), Naoussa (100% Xinomavro/Xynomavro, red only), Santorini (Assyrtiko 75%+, white dominant, includes Nykteri + Vinsanto styles). Sources: Hungarian Wine Act 2004 CXXVIII via EU eAmbrosia; Greek Wine Law via EU PDO register. Cascades: Tokaj → white, Nemea → red, Naoussa → red, Santorini → white. Wine_grapes 100%: Nemea (Agiorgitiko) + Naoussa (Xynomavro). **Running totals: 261 appellation_rules, 829 appellation_grapes with full provenance, 0 duplicates, 100% provenance coverage.**
  - **Batch 18 (2026-04-05 loop-cycle 14): All 13 German Anbaugebiete (wine PDOs) via BLE Produktspezifikationen.** Rules: 244 → 257. Seeded: Mosel, Pfalz, Rheinhessen, Rheingau, Nahe, Baden, Franken, Württemberg, Mittelrhein, Ahr, Hessische Bergstraße, Sachsen, Saale-Unstrut. Source: BLE Produktspezifikation per Anbaugebiet (filed with EU). Note: German wine law is very permissive (~190 authorized grape varieties per Anbaugebiet per Weinverordnung) — rules note regional signatures (e.g. Mosel = Riesling ~60%, Baden = Spätburgunder ~35%, Ahr = Spätburgunder ~60%, Franken = Silvaner, Württemberg = Trollinger). Mosel specification verified via EUR-Lex C_202401037. No color cascades (all multi-color). No grape rows seeded (>100 varieties per region makes individual rows impractical). **Running totals: 257 appellation_rules, 821 appellation_grapes with full provenance, 0 duplicates, 100% provenance coverage.**
  - **Batch 17 (2026-04-05 loop-cycle 13): 14 Spanish DOPs via MAPA pliegos.** Rules: 230 → 244. Grape rows: 797 → 821. Seeded: Jerez-Xérès-Sherry (fortified), La Mancha, Utiel-Requena, Valencia, Calatayud, Montsant, Cataluña, Almansa, Granada, Campo de Borja, Valdeorras, Ribeiro, Costers del Segre, Ribeira Sacra. Discovered MAPA pliego URL pattern `https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/.../pliego-condiciones-vinos/dops/{slug}_{YYYY_MM_DD}.pdf`; URLs obtained via WebFetch on individual DOP landing pages (e.g., `/vcprd/DOP_jerez.aspx`) + WebSearch for remaining. Primary grapes seeded per DOP: Palomino Fino/PX/Moscatel (Jerez), Airén + Tempranillo (La Mancha), Bobal (Utiel-Requena), Garnacha Tinta (Calatayud + Campo de Borja), Garnacha Tintorera (Almansa), Godello + Mencía (Valdeorras), Mencía 70%+ (Ribeira Sacra), Treixadura + Godello (Ribeiro). No color cascades (all multi-color umbrellas; Jerez fortified skipped due to wines.color enum ambiguity for fortified). User granted permission to add missing appellations; Rías Baixas confirmed already seeded. **Running totals: 244 appellation_rules, 821 appellation_grapes with full provenance, 0 duplicates, 100% provenance coverage.**
  - **MASAF-subordinate path (breakthrough, allows Italian DOCGs):** catalogoviti.politicheagricole.it/masaf.gov.it subdomain is ECONNREFUSED, but MASAF decrees are mirrored on (a) **EUR-Lex IT PDFs** via the `OJ:C_YYYYNNNNNN` URL pattern for Reg (EU) 2019/33 Art 17 wine PDO modifications, (b) **Valoritalia** (MASAF-designated control body), (c) **Regione Piemonte** (Piedmont regional government), (d) **IRVO** (Sicilian regional wine institute), (e) **Gazzetta Ufficiale della Repubblica Italiana** (gazzettaufficiale.it direct PDFs). All 5 are defensible extensions of the MASAF source.
  - **Notable finding (Pomerol):** The Pomerol CDC authorizes only 5 varieties (Cabernet Franc, Cabernet Sauvignon, Cot/Malbec, Merlot, Petit Verdot) — Carmenère is NOT permitted (unlike Médoc and Saint-Émilion). Pre-existing unsourced Carmenère row in appellation_grapes for Pomerol left in place per no-delete rule, but flagged as superseded per current CDC.
  - **Cascades executed across all 3 batches** (strictly definitional, NULL-fills only): **~2,338 additional wines.color** fills beyond batch 1 (batch 1: 8,973; batch 2: 769 across Pauillac/Margaux/Saint-Julien/Cornas/Morgon/Condrieu; batch 3: ~1,569 across Pomerol/Saint-Émilion/Saint-Émilion GC/Saint-Estèphe/Côte-Rôtie/Moulin-à-Vent/Fleurie/Sauternes/Barsac) **~11,311 total color fills**. **~9,055 wines.varietal_category_id** fills (batch 1 + Cornas→Syrah + Condrieu→Viognier). **~8,215 wine_grapes rows** at percentage=100 for true single-variety appellations (batch 1 + Cornas 300 + Condrieu 316). Cross-check found **~895 wines with legally impossible colors** (batch 1: 855 — 50 Chablis red, 800 Champagne red, 2 Chianti Classico white, 2 Pommard non-red, 1 Gevrey non-red; plus Italian DOCG edge cases: 9 Barolo rose, 5 Barolo white, 1 Chianti Classico rose, 1 Barbaresco rose, 1 Brunello white; batch 2-3 flags: ~7 Sauternes red, 2 Barsac red, 1 each Pomerol/Saint-Émilion/Saint-Émilion GC/Saint-Estèphe/Moulin-à-Vent white) — all left alone per no-overwrite rule, flagged in DECISIONS.md for cleanup session.
- **Cron loop overnight (2026-04-06): Vineyard seeding from legal sources — 0 → 815 vineyards.** 3-track loop (*/10 cron, 27 cycles). Track A (price promotion): 0 new — all sources already promoted. Track B (vineyards): **+806 vineyards** — 585 Burgundy Premier Cru climats from INAO CDCs (26 villages), 170 Barolo MGAs from MASAF disciplinare, 51 Alsace Grand Crus linked to existing appellations. Track C (data quality sweeps): 0 rows — all already clean. Track B was the only productive track; Tracks A and C were wasted cycles (would have been caught by gap analysis). Infrastructure created: `data/session_prompts/cron_loop_template.md` (structural template for future loops), `data/stats/cron_loop_journal.md` (append-only journal of loop outcomes). 9 slug conflicts resolved (Les Fourneaux Chablis vs Mercurey, etc.). Barolo PDF artifacts manually corrected (OCR split words, missing commas).
- **WineTest session (2026-04-06):** Built WineTest DB quality assessment tool (`pipeline/analyze/winetest/`). First score: 49/100. Improved to **56/100** via: (a) matching improvements in scorer.py (grape-as-name fallback, eponymous wine detection, keyword matching, swap-and-retry, producer fuzzy verification — findability 70%→83%), (b) **+80,428 wine_grapes** via `grape_from_name.py` (greedy longest-match on curated 95-grape set against `name_normalized`, strictly definitional — grape name literally appears in wine name as word-bounded token). Total wine_grapes: 198K→278K. WineTest dimensions: Findability 83%, Depth 33%, Accuracy 88%, Story 1.8/5.0. Biggest remaining levers: enrichment pipeline (Story stuck at 0%), price coverage (8.4%), appellation backfill.
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

**31 staging tables (~4.35M total rows, audited 2026-03-27):**
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
  - `source_utah_dabs` (2,834) — Utah DABS state monopoly. Monthly XLSX, 127 wine classes, authoritative pricing.

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
- `python -m pipeline.analyze.barcode_scanner --image-dir "D:\TTB Label Images\labels" --workers 12` — scans label/scan images for UPC/EAN/QR barcodes (year-by-year streaming, incremental save, resume support)
- `python -m pipeline.promote.ttb_upc_promote --execute --qr` — promotes barcode scan UPCs + QR codes to external_ids via source_ttb_colas.canonical_wine_id join
- `python -m pipeline.analyze.name_cleanup [--execute] [--table wines|producers|both]` — deterministic 5-pass name cleanup (HTML decode, whitespace, Wally's suffix strip, U+FFFD dictionary repair, curly quotes). Dry-run by default.
- `python -m pipeline.analyze.name_cleanup_haiku [--execute] [--table wines]` — Haiku-powered repair of remaining U+FFFD long-tail words. ~$0.10 for 1,237 names.
- `python -m pipeline.analyze.db_counts` — row counts across all tables
- `python -m pipeline.analyze.winetest [--size 200] [--categories 4] [--seed N] [--no-accuracy] [--accuracy-sample 30]` — WineTest DB quality assessment. Haiku-generated benchmark of wines Americans actually encounter, measures findability/depth/accuracy/story. ~$0.60/run with accuracy+story checks.
- `python -m pipeline.promote.grape_from_name [--dry-run|--execute] [--limit N]` — grape backfill from wine names via greedy longest-match on curated 95-grape set
- `python -m pipeline.fetch.nasa_power_weather [--test|--limit N|--id UUID|--no-resume|--delay N]` — NASA POWER bulk weather fetch (~50km resolution, 1981-2025). 1dp coordinate caching. **COMPLETE: all 2,997 appellations fetched.**
- `python -m pipeline.fetch.open_meteo_weather [--test|--limit N|--id UUID|--no-resume|--delay N|--by-wines]` — Open-Meteo high-resolution weather drip (~9-25km, 1980-2025). 2dp coordinate caching, resume support (skips open-meteo sourced), daily limit detection. `--by-wines` orders by wine count (Napa first). Nightly scheduled task runs this.

**Key promotion scripts:**
- `pipeline/promote/batch_matcher.py` — reusable in-memory producer matching with suffix stripping
- `pipeline/promote/retail_promote.py` — UPCs, prices, vintages from matched retailers
- `pipeline/promote/ttb_wine_link_v2.py` — TTB→canonical wine linking
- `pipeline/promote/cola_depth.py` — COLA IDs, vintages, grapes from linked TTB records
- `pipeline/promote/grape_from_helper.py` — TTB grape promotion (handles encoding corruption)
- `pipeline/promote/ttb_producer_relink.py` — normalized producer matching for TTB brands
- `pipeline/promote/ttb_producer_bridge.py` — creates producers from TTB brand_name matches for unlinked staging rows
- `pipeline/promote/catalog_producer_create.py` — creates producers from curated catalog sources (Enofile, Systembolaget, WineDeals, etc.)
- `pipeline/promote/retail_wine_create.py` — creates canonical wines from producer-matched staging records (12 sources)

**Data quality infrastructure:**
- `accuracy_audit` table + `accuracy_audit_daily` view
- `last_validated_at` column + `sample_wines_for_validation(batch_size)` RPC
- Scheduled `data-accuracy-agent` task (currently paused)
- **WineTest** (`pipeline/analyze/winetest/`) — DB quality assessment tool. Generates Haiku-powered benchmark of ~200 wines Americans actually encounter (restaurants, stores, friends' houses). Measures 4 dimensions: Findability (can we find the wine?), Depth (how complete is our data?), Accuracy (are facts correct? — Haiku-verified), Story (would an enthusiast learn something? — Haiku-rated 1-5). Includes blind spot detection and trend tracking. ~$0.60/run. **Latest score: 56/100** (Findability 83%, Depth 33%, Accuracy 88%, Story 1.8/5). Results saved to `data/stats/winetest/`. Main levers to improve: enrichment pipeline (Story 0→target), price coverage (8.4% currently), appellation backfill.

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
- ~~UPC barcodes~~ **DONE:** 106K UPCs across 80K wines (TTB label+scan barcode scanning complete, promoted to external_ids 2026-04-06)
- ~38 canonical tables still at 0 rows (descriptors, wine_relationships, etc.) — vineyards has 815 rows, weather tables now fully populated
- **Weather data: BULK COMPLETE, DRIP UPGRADING.** 2,997 appellations × 45 years = 134,867 yearly rows + 1,618,404 monthly rows. Bulk fill via NASA POWER API (~50km resolution, 1981-2025). Nightly scheduled task (`open-meteo-weather-drip`, 3am) upgrades ~8 appellations/night to Open-Meteo's higher resolution (~9-25km, 1980-2025) in wine-count priority order (Napa first, then Champagne, Paso Robles, etc.). Pipelines: `pipeline/fetch/nasa_power_weather.py` (bulk, complete), `pipeline/fetch/open_meteo_weather.py --by-wines` (drip, ongoing).
- Soil/water body links, producer_timeline — still empty.

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
- ~~TTB barcode scan~~ **COMPLETE** (2026-04-06): 3M labels + 332K scans scanned, 106K UPCs promoted
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
3 rounds of hardening applied. Key infrastructure: `set_updated_at()` triggers on 36 tables, `validate_polymorphic_fks()` orphan checker, enrichment_log with cost/model tracking, `appellation_rules` table. `wine_vintage_scores` and `wine_vintage_prices` have `wine_vintage_id` FK (preferred join path). `retailers` table seeded with 13 retailers (all price sources).

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
- **3M+ label images + 332K scan images downloaded** to external drive (D:\TTB Label Images, ~770GB). Barcode scan complete: 142K unique UPCs detected, 106K promoted to external_ids across 80K wines. 45K QR codes also captured and promoted (12.5K URLs + 1.4K data).
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
