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
- `data/stats/loam_roadmap.json` + `python -m pipeline.analyze.loam_roadmap` — Phased development plan. The JSON holds phase structure; the script renders it with live DB metrics. Replaced `docs/ROADMAP.md` (deleted in Session 14 Phase A — it duplicated the JSON and went stale). Read at session start to know what phase we're in.
- `data/sprints/current.json` + `python -m pipeline.analyze.sprint_dashboard` — Active sprint state. Each sprint lives under `data/sprints/<name>/` (sessions.json, budget.json, journal.md, status.md, meta.json, prompts/); archived sprints live under `data/sprints/_archive/<name>/`.
- `docs/MERGE_STRATEGY.md` — Merge pipeline decisions: Python migration, merge layer sequencing, COLA risks, wine identity definition, AI matching approach, product direction. Read before building merge/matching infrastructure.
- `docs/WORKFLOW.md` — Human-facing session checklist. You don't need to read this, but follow the behavioral instructions below.
- `docs/reference/` — Retired docs kept for historical reference, not actively updated. Includes LWIN_STRATEGY.md (superseded by SOURCES.md + `data/stats/loam_roadmap.json`), SCHEMA_ASSESSMENT.md (Phase 1a spec, fully executed).

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
When something is important enough to update CLAUDE.md, it's important enough to commit. Commit with a clear message after meaningful milestones. Also update your entry in `data/sessions.md` with current progress.

### Always Recommend
When asking the user a clarifying question, **always give a recommendation**. If the answer is unclear, explain the case for each option. Don't just ask — propose a direction.

### Nudge the User
If the user is going a long stretch without wrapping up, if decisions are being made but not logged, or if a session is ending without updating files — say something. Be direct: "We've made some decisions this session that aren't logged yet. Want me to update DECISIONS.md and CLAUDE.md before we stop?"

### Session Whiteboard
Read `data/sessions.md` at session start. Log what you're working on under **Active** (include tables you're writing to). At wrap-up, move your entry to **Done** with a summary. If another session is active, don't edit CLAUDE.md or docs/ — the next solo session merges it in.

### Prefer Opus Inline for Audit & Reasoning
For audit-class and reasoning-class work — data audits, primary-source fact-checking,
cross-record pattern recognition, findings synthesis, quality/severity judgments —
default to doing the work inline in the current Opus 4.6 / 1M-context conversation
instead of pre-authorizing a Haiku/Sonnet script budget. Same rigor, ~$0 marginal
project cost, and Opus can cross-reference across the whole batch in one pass in
a way a per-item scripted call cannot.

Still use scripted Haiku/Sonnet for: mass-batch promotion (tens of thousands of
rows), per-item transformations that must be reproducible, workloads exceeding
the 1M context window, unattended scheduled runs, anything that writes to the DB
via automation.

When a session spec proposes a Haiku/Sonnet budget for "fact-checking" or "auditing"
or "reasoning across records," pause and ask whether Opus inline is the right tool
first — default yes. Note the pivot in the session journal so budget tracking stays
honest. See `memory/feedback_opus_inline_reasoning.md` for full rationale and the
S2.3 evidence.

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

**Sprint 2 (Audit) is active as of 2026-04-11.** Multi-expert read-only audit
producing a Sprint 3 fix backlog. S2.1 (DB canonical) ran with 34 findings
(6 P0, 12 P1, 11 P2, 5 P3) → `data/sprints/audit/findings/findings_db_canonical.md`.
**S2.2 (DB staging)** ran with 31 findings (6 P0, 11 P1, 9 P2, 5 P3) →
`findings_db_staging.md`. **S2.3 (wine canonical, 99 wines + 50 producers sommelier
audit)** ran with 22 findings (9 P0, 8 P1, 4 P2, 1 P3) → `findings_wine_canonical.md`.
**S2.4 (wine reference content)** ran with 30 findings (8 P0, 14 P1, 7 P2, 1 P3) →
`findings_wine_reference.md`. **S2.5 (code expert — 265 pipeline/*.py files + 2
edge functions + shared libs + scheduled tasks)** ran with 32 findings (9 P0, 14 P1,
7 P2, 2 P3) → `findings_code.md`. **S2.6 (voice expert — 5,454-row enriched
corpus + 5 prompt files + live enrich-wine edge function)** ran with 32 findings
(9 P0, 14 P1, 7 P2, 2 P3) → `findings_voice.md`. All six sessions at $0 actual
spend. Sprint 2 budget $0 / $25 ceiling. **181 findings** total across
S2.1+S2.2+S2.3+S2.4+S2.5+S2.6 so far.

**Six-session headline:** (S2.2 F1) 286,918 wine_id pointers across 29 of 31
wine-bearing staging tables are dangling archive references — Sprint 3's highest-ROI
task is a staging relink (reuse S13 pattern) to unlock ~52K prices + ~48K scores +
~200K vintage-grade fields + ~40K UPCs. **S2.5 F3 pinpoints the code cause:**
`relink_staging_to_current.py` STAGING_TABLES_WINE lists only 1 of 30 tables with an
unresolved TODO comment. (S2.3 F2 → S2.4 F2 → S2.5 F2) The Chardonnay/Pinot Blanc
grape-linkage bug is systemic (2,743 of 2,809 display_name Chardonnay wines have
Pinot Blanc linked) — S2.4 found the data root cause (PINOT BLANC has VIVC synonyms
`PINOT CHARDONNAY`, `CHARDONNET PINOT BLANC`, `PINOT BLANC CHARDONNET`, `PINOT GRIGIO`)
AND S2.5 found the CODE root cause — `batch_pipeline._match_ttb_to_wine` collapses
4 grape-specific TTB COLAs (Chardonnay/Cab/Shiraz/Shiraz) onto 1 canonical wine for
~2,700 wines (verified via live DB query on De Bortoli 17 Trees wine). The resolver
in `pipeline/lib/resolve.py` is correct — the bug is upstream wine-identity dedup.
**(S2.6 F4) Contamination has reached the user-facing content layer:** 487 Chardonnay
wines with wine_insights carry Claude-invented rationales for the impossible grape
pair (Waterbrook Icon hook: "blends 100% Chardonnay with 75% Pinot Blanc — an unusual
high-proportion white blend"). **(S2.6 F5) Contamination feedback loop traced
end-to-end:** Knights Valley appellation_insight (2026-03-06) confabulates "volcanic
soils from ancient Mayacamas eruptions" → edge function `assembleContext` injects
it as Grade B wine prompt context → Beringer Alluvium Grade B inherits and extends
with a DIFFERENT wrong volcano ("Mount St. Helena eruptions"). Same pattern across
RRV (false volcanic ash), Sonoma Coast (false volcanic intrusions), Howell Mountain
(wrong "tufa and obsidian"). **(S2.6 F1/F2) Prompt drift is the voice problem:**
`enrich_prompts.py` is the only tightened voice source in the codebase; four
reference-layer enrichment scripts and the live `enrich-wine` edge function all
ship a weak 8-word banlist with no hedging/sommelier-theater/performative-enthusiasm
rules — Grade B LIKE scan: 59% "likely", 52% "showcases", 41% "premium", 39%
"elegant"; Grade C under the tightened prompt: 2%, 1%, 3.9%, 16% (10-50x deltas).
**(S2.6 F3) Voice rules cannot prevent factual confabulation** — 5/5 random Grade
C hook samples contained invented facts (Schramsberg J Schram falsely described
as "still rosé", DRC Corton-Charlemagne falsely claims "avoids MLF", Pax Obsidian
"likely 14.5%+" fabricated ABV) validating the Session-10 feature-flag decision
to keep `enrich-wine` `ENRICHMENT_ENABLED=false`. **(S2.6 F9) Reference corpus is
100% US-biased at the appellation level** — 82/82 `appellation_insights` are US
AVAs; zero Chambertin, Barolo, Champagne, Rioja, Chablis — which is WHY DRC
Corton-Charlemagne's Grade B prompt got no appellation context and fell back to
Claude training knowledge. (S2.5 F1) `describe-chemical` edge function is DEPLOYED,
ACTIVE, `verify_jwt=false`, shares `ANTHROPIC_API_KEY`, has ZERO wine logic —
leftover from another project, unauthenticated credit-burn risk, delete immediately.
(S2.5 F4) `enrich-wine` edge function reads `grapes.name` (VIVC "CHARDONNAY BLANC")
not `grapes.display_name` ("Chardonnay") — every Grade B prompt inherits wrong
grape labels. (S2.5 F5) Three Anthropic model IDs coexist with no central config
(haiku-4-5, sonnet-4 stale, sonnet-4-6). (S2.5 F18) `lwin_long_tail.py` inserts
wines without populating `display_name` — 50,908 long-tail wines affected, biasing
S2.3 F2 sample toward BATCH_0. (S2.4 F1) varietal_categories has 5+ P0 wrong-grape
links — Merlot → Grolleau Noir, Riesling → Crouchen, Verdejo → Trousseau Noir,
Greco → Albana Bianca, St. Laurent → Muscat St. Laurent. (S2.4 F3) 121 famous
appellations stored with slash-concatenated alias names. (S2.4 F9) 240 FR AOCs +
105 IT DOCs have fake 1973 `established_year` default. (S2.3 F3) All 15 hand-picked
famous producers (DRC, Lafite, Latour, Margaux, etc.) have zero metadata.
**(S2.6 F8) `wine_food_pairings` structured table is empty; CLAUDE.md claims
"809 structured links + 203 text descriptions from Empson" is stale — 30K rebuild
wiped the table, archive still has 809 rows. (S2.6 F6) `grape_insights` table has
zero rows despite `grape_insights.py` containing the best food-pairing prompt in
Loam (VOICE.md-compliant classics-first structured guidance, never run). (S2.6 F7)
99% of enriched wines (5,003 of 5,062 Grade C) have no food-pairing prose at all
because `GRADE_C_FIELDS` schema drops the field entirely.**

**Sprint 3 sequence (refined by S2.6):** Pre-reqs: (i) voice module consolidation
— create `pipeline/lib/voice.py` with shared VOICE_RULES_BLOCK + NEVER INVENT
block, rewrite 4 reference enrichment scripts + vendored edge function to import
it, ~4-6 hrs (S2.6 F1/F2, closes 14 of 32 S2.6 findings). (ii) Delete `describe-chemical`
edge function, centralize Anthropic model IDs via `pipeline/lib/models.py`, vendor
`enrich-wine` source into `supabase/functions/` — ~30 min combined (S2.5 F1/F5/F31).
(iii) Restore `wine_food_pairings` from `archive.wine_food_pairings` (S2.6 F8).
Then: (a) S2.2 F1 staging relink **— code owner identified: extend
`relink_staging_to_current.py` STAGING_TABLES_WINE from 1 to 30 tables (S2.5 F3),
~2hr fix** → (b) S2.3 F3 producer seed file → (c) refined grape-repair workstream —
**3a** `grapes.name` cleanup + `display_name` (S2.4 F6, F15) → **3b** 921 synonym
collision resolution + delete PINOT BLANC's 4 polluting synonyms (S2.4 F2, F7) →
**3c** fix varietal_categories wrong links (S2.4 F1) → **3c.5** fix
`batch_pipeline._match_ttb_to_wine` multi-COLA collapse (S2.5 F2) → **3c.6** fix
`ttb_grape_promote` DISTINCT ON arbitrary pick (S2.5 F17) → **3c.7** consolidate
grape resolvers on `ReferenceResolver` (S2.5 F11) → **3d** re-run grape resolver
against wine_grapes (S2.3 F2) → **3e** JSONB content backfill + appellation_grapes
language fixes + appellation_soils provenance schema (S2.4 F11-F17) → (d) F6
color+country repair → (e) L3 fact-check gate scaffolding (S2.6 F3; the $18 S2.3
pre-auth re-scoped from "re-fact-check existing prose" to "build L3 gate that
blocks writes without fact-check") → (f) Sprint 5: **reference regen first, wine
regen second** (S2.6 F5; contamination direction is one-way). Skipping any of
(a)-(e) re-contaminates (f). **The `ENRICHMENT_ENABLED=false` feature flag on the
`enrich-wine` edge function must stay OFF through Sprint 3 and into Sprint 5; do
not flip until F1+F2+F3+F4 all land.** **S2.4 + S2.5 + S2.6 findings blocking
Sprint 3: 26 P0 + 42 P1 = 68 items total (net ~65 after S2.5↔S2.6 overlap
dedup).**

Live sprint state: `data/sprints/current.json`. 30K Plan (Sprint 1) closed 2026-04-11
and is archived at `data/sprints/_archive/30k/`. Pre-30K history (the ~477K-wine
pre-rebuild dataset) lives in `docs/HISTORY.md` under "Pre-30K rebuild history".

**Sprint sequence (locked 2026-04-11):** 1 (30K, done) → 2 (Audit, active) → 3
(Execute fixes) → 4 (Reference Design) → 5 (Reference Enrichment). Enrichment at
scale does NOT start until Sprint 5. See `memory/project_quality_before_enrichment.md`.

**Numbers in this section are periodic snapshots.** Always query the DB (or run
`python -m pipeline.analyze.sprint_dashboard` / `powershell -File scripts/dash.ps1`)
before relying on any count. The S2.1 audit found ≥6 places where hardcoded counts
in this file had drifted from live DB state (finding F28). Trust live queries, not
the numbers below.

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
78 canonical tables, 32 staging tables. Schema hardened across 3 rounds (Phase 1a, post-import, scan round 2). All reference data seeded and audited. See `docs/SCHEMA.md` for field reference, `docs/HISTORY.md` for schema change history.

### Content Tables (snapshot 2026-04-11, Session 14 Phase A)
Run `python -m pipeline.analyze.sprint_dashboard` or query the DB for live numbers. These are point-in-time snapshots. Pre-30K rebuild history lives in `docs/HISTORY.md` under "Pre-30K rebuild history"; 30K sprint session-by-session narrative lives in `data/sprints/30k/journal.md`.

**Corpus scale (post-S13 LWIN long-tail):**
- **Active wines:** 155,623   **Producers:** 10,676   **Wine_vintages:** 83,531   **Wine_grapes:** 47,035
- **Soft-deleted dupes:** 947 (Session 13 merged 718 via strict + Haiku fuzzy passes)
- **Data grade distribution:** B = 105  ·  C = 4,973  ·  D = 33  ·  F = 150,512  (the long-tail wines are identity-only F)

**Coverage (active wines):**
- Color: 153,311 (98.5%)   Country: 155,623 (100%)   Region: 148,009 (95.1%)   Appellation: 104,788 (67.3%)
- Vintages with ABV: 48,700   Label designations: 11,589

**External IDs:**
- LWIN: 170,797 (119,889 `lwin` + 50,908 `lwin_7` from S13 long-tail)
- COLA: 253,301   UPC: 13,162   QR URL: 1,400   QR: 163
- `source_ttb_colas` linked to current canonical: 83,183 rows → 20,500 distinct wines

**Enrichment:**
- `wine_insights`: 5,108 rows (3.3% of corpus — ~4,900 from pre-30K Grade B/C batches in Sessions 9/12, ~200 new canon during S12 validation)
- `data_provenance`: 223,947 entries
- Grade B Edge Function deployed behind `ENRICHMENT_ENABLED` feature flag (disabled pending Reference-First sprint)

**Reference layer (seeded, ready for Sprint 2 enrichment):**
- `appellation_rules`: 1,165   `appellation_grapes`: 10,414 (full provenance, ~100% coverage for seeded appellations)
- `appellation_vintages`: 134,877 (weather data, all appellations × 45 years)
- `appellation_soils`: 930 links across 304 appellations
- Insight tables still thin: `region_insights` 202  ·  `appellation_insights` 82  ·  `country_insights` 62  ·  all others 0

**Aspirational / pending (0 rows live, preserved schema):**
- `vineyards`, `water_bodies`, `wine_vintage_descriptors`, `wine_vintage_nv_components`, `producer_timeline`, etc. — canonical scaffolding preserved. The pre-30K "815 vineyards" lives in `archive_vineyards`; current public.vineyards is empty post-rebuild.

**Pre-30K history was pruned here in S14 Phase A.** Session-by-session recovery/follow-up rounds (Rounds 1-17, Path A batches, producer scrapes, accent cleanup, knowledge seed, etc.) all moved to `docs/HISTORY.md` under "Pre-30K rebuild history". Do not re-litigate the pre-rebuild numbers without querying first.

### Multi-Source Merge Infrastructure (2026-03-18)
Staging-first architecture: all external data goes through per-source staging tables, then a match engine promotes to canonical tables. Prevents dedup crisis at scale.

**32 staging tables (~4.35M total rows, audited 2026-03-27):**
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
  - `source_claude_knowledge` (920) — Claude training data wine seed. 32 categories, dual Haiku+Sonnet validation. 656 matched existing, 204 promoted, 60 rejected.

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
- `python -m pipeline.analyze.ocr_bakeoff [--image-dir DIR] [--limit N] [--skip-claude]` — OCR bake-off comparing EasyOCR, RapidOCR, Claude Vision on wine labels. Results in `data/stats/ocr_bakeoff_results.json`.
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
- `pipeline/fetch/producer_site_scrape.py` — generic Haiku-based producer website scraper. Manifest of 100 top producers, resume-safe, extracts wines + vintages + grapes + winemakers from HTML. `--execute --budget N --skip-to NAME --no-resume`
- `pipeline/promote/knowledge_seed.py` — multi-stage pipeline to seed notable wines from Claude training data. 6 stages: generate → dedup → ttb-match → validate → promote → report. Haiku generation + Sonnet quality gate. `python -m pipeline.promote.knowledge_seed <stage> [--budget N] [--model sonnet] [--recheck] [--dry-run]`
- `pipeline/promote/lwin_sonnet_match.py` — Sonnet-powered fuzzy LWIN matching for wines without backbone IDs. `python -m pipeline.promote.lwin_sonnet_match [--budget 5.0] [--dry-run]`

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

**Session 14 housekeeping interregnum** — 30K Plan (Sprint 1) closes at end of Session 14 Phase B. Sprint 2 = Reference-First Enrichment (planning session 15, execution sessions 16+). See `data/stats/loam_roadmap.json` + `python -m pipeline.analyze.loam_roadmap` for the full phased plan.

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
- ~~Bulk label OCR extraction~~ — tabled 2026-04-07. Bake-off done (EasyOCR 80%, RapidOCR 74% vs Claude baseline). ROI too low vs enrichment pipeline. Images preserved on D: drive.

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
  - `data/stats/loam_roadmap.json` — update if phase status or priorities changed (no more `docs/ROADMAP.md` — deleted in S14 Phase A)
  - `data/sprints/current.json` + `data/sprints/<name>/` — update sprint state (sessions.json, budget.json, journal.md, status.md)
  - `docs/SCHEMA.md` — update if schema changed (CREATE/ALTER/DROP)
  - `docs/SOURCES.md` — update if source status changed (new source, fetcher built, data loaded)
  - `docs/ENRICHMENT.md` — update if enrichment architecture changed
  - `docs/PRINCIPLES.md` — update if product philosophy changed
  - `docs/VOICE.md` — update if tone/content guidance changed
  - `docs/WORKFLOW.md` — update if session workflow changed
- **"log that"** — Force a DECISIONS.md entry.
- **"briefing"** — Give current state summary anytime mid-session.
