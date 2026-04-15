# Loam v2 — Claude Context

Loam is a wine data platform. Users look up a wine and get the full story — place, vintage weather, soil, grapes, producer choices — as structured, connected data. The product is organized facts rendered clearly, not AI prose. The name is a soil type. Terroir is central.

**Supabase project:** `vgbppjhmvbggfjztzobl` (us-east-1)
**GitHub:** github.com/neilwhitman859/loamv2
**Stack:** Supabase (Postgres), Python pipeline, Anthropic Claude, Open-Meteo, Vite/React frontend

---

## Docs — When to Consult Each

- `docs/SCHEMA.md` — Table-by-table field reference. Read when working with DB structure or writing queries.
- `docs/PRINCIPLES.md` — Product philosophy. Read when making judgment calls about what to build or how.
- `docs/DECISIONS.md` — Append-only log of human decisions with reasoning. Read when you need to understand why something was done a certain way. Never re-litigate settled decisions without the user raising it.
- `docs/VOICE.md` — Voice, tone, and food pairing guidance for all AI-generated content. Read before writing any enrichment prompts or insight content.
- `docs/SOURCES.md` — Master reference for all external data sources (evaluated, integrated, planned, rejected). Read when working on data acquisition or import pipelines.
- `docs/IDENTITY_RULES.md` — Wine identity definition rules for dedup and matching.
- `data/dashboard.html` — **Single source of truth for sprint progress.** Track checklist, session log, budget, snapshot metrics. Read at session start. Update at milestones and before every commit. User keeps it open in Notepad++ with auto-reload.
- `data/sprints/current.json` — Sprint state pointer. Each sprint lives under `data/sprints/<name>/` (sessions.json, budget.json, journal.md); archived sprints under `data/sprints/_archive/<name>/`.
- `data/sprints/fix/plan.md` — Sprint 3 (Fix) full plan with 6 tracks and execution order.
- `docs/reference/` — Retired docs kept for historical reference, not actively updated. Includes LWIN_STRATEGY.md, SCHEMA_ASSESSMENT.md, 30K_PLAN.md, PATH_A_ROLLBACK.md, AUDIT_2026-04-01.md, MERGE_STRATEGY.md, BACKLOG.md.

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

### Dashboard
Update `data/dashboard.html` at session start (refresh metrics), at milestones (check
off completed tracks), and before every commit. This is the single source of truth
for sprint progress. User keeps it open in Notepad++ with auto-reload.

### Session Routines

**Session open (3 steps):**
1. Read `data/dashboard.html` + `CLAUDE.md`
2. Query DB for current metrics — update dashboard snapshot
3. Log session start in `data/sessions.md` under Active

**Session close (4 steps):**
1. Update `data/dashboard.html` — check off tracks, refresh metrics
2. Update `CLAUDE.md` if anything meaningful changed
3. Move session entry to Done in `data/sessions.md`
4. Commit and push

**Sprint open:** Create sprint dir (`data/sprints/<name>/`), sessions.json,
budget.json, journal.md. Update `data/sprints/current.json`.

**Sprint close:** Re-run all dashboard metrics. Archive sprint. Update current.json
pointer. Decide: re-audit or move to next phase.

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

**Sprint 3 (Fix) COMPLETE.** All 6 tracks + re-audit + quick-fix done, $0 spent. 158 fixed, 10 partial, 101 deferred.
Sprint 2 (Audit) deliverable: [`data/sprints/audit/findings/synthesis.md`](data/sprints/audit/findings/synthesis.md).
Sprint 3 plan: [`data/sprints/fix/plan.md`](data/sprints/fix/plan.md) — 6 tracks, ~179 findings in scope.
Sprint 3 re-audit: [`data/sprints/fix/reaudit_findings.md`](data/sprints/fix/reaudit_findings.md) — S3.7 (9-expert, 275 findings audited) + S3.7b (quick-fix pass).

| Session | Expert | Findings (P0/P1/P2/P3) | Findings file |
|---|---|---|---|
| S2.1 | db_canonical | 34 (6/12/11/5) | `findings_db_canonical.md` |
| S2.2 | db_staging | 31 (6/11/9/5) | `findings_db_staging.md` |
| S2.3 | wine_canonical (sommelier) | 22 (9/8/4/1) | `findings_wine_canonical.md` |
| S2.4 | wine_reference | 30 (8/14/7/1) | `findings_wine_reference.md` |
| S2.5 | code | 32 (9/14/7/2) | `findings_code.md` |
| S2.6 | voice | 32 (9/14/7/2) | `findings_voice.md` |
| S2.7 | ux | 32 (9/14/7/2) | `findings_ux.md` |
| S2.8 | meta | 32 (9/14/7/2) | `findings_meta.md` |
| S2.9 | business | 30 (8/14/6/2) | `findings_business.md` |

**Sprint 2 headline** (the one-paragraph version; synthesis.md is authoritative):
Sprint 3 unblocks Sprint 5 by (1) eliminating contamination vectors that would
re-infect regenerated content, (2) unlocking the staging-locked data depth waiting
in archive tables, and (3) repairing first-impression credibility on the ~500 wines
a sommelier demo is likeliest to hit. Biggest concrete opens: the Chardonnay/Pinot
Blanc compound bug (12 findings across 5 experts → Track 3), staging archive relink
(S2.2 F1 + S2.5 F3 → Track 2, unlocks ~116K archive prices + ~22K scores from the
pre-30K world currently dangling), code/voice hygiene bundle (21 findings across
S2.5+S2.6+S2.9 → Track 1, closes in 1-2 sessions), `describe-chemical` rogue edge
function still deployed at Sprint 2 close (S2.5 F1 / S2.8 / S2.9 all re-verified
deployed — delete in Sprint 3 Session 1 Minute 1), corpus-wide producer metadata
vacuum (0 of 10,676 producers have metadata, not just 15 marquee), doc hygiene
bundle (23 S2.8 findings), UI hygiene bundle (15 S2.7 findings including CountryPage
1-char typo breaking 100% of country pages), and the new business-layer findings
from S2.9: **no monetization model, 0 user wine lookups ever, moat inverted vs
direct LLM queries today, ICP undefined, terroir positioning 10x more polished than
data supports**. Cost is not a constraint — Sprint 5 full coverage projects at
$620-700 total; correctness + voice consolidation + fact-check gate are the
constraints. **`ENRICHMENT_ENABLED=false` feature flag on `enrich-wine` stays OFF
through Sprint 3 into Sprint 5** until voice module + L3 fact-check gate + grape
repair compound + AI-disclaimer UI all land.

**Sprint sequence:** 1 (Build, done) → 2 (Audit, done) → 3 (Fix, done) → 4 (Demo, done) → **5 (AI Bakeoff, done)** → 6 (Execute with winners: dedup, data fill, re-enrichment, share).
Sprint 4 plan: [`data/sprints/demo/plan.md`](data/sprints/demo/plan.md). Sprint 5 plan: [`data/sprints/ai-bakeoff/plan.md`](data/sprints/ai-bakeoff/plan.md). Sprint 5 outcome: [`bakeoff/scores/tournament_results.md`](bakeoff/scores/tournament_results.md). See `data/dashboard.html` for live progress.

**Always query the DB for live numbers.** Never rely on hardcoded counts in this file.
Live sprint state: `data/sprints/current.json`.

---

Sprint 2 per-session findings narrative is captured in `data/sprints/audit/findings/` (9 files) and collapsed into tracks in `data/sprints/audit/findings/synthesis.md`. Sprint 3 execution plan is `data/sprints/fix/plan.md`.

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
- `pipeline/analyze/` — analysis and utility scripts

### Architecture
The database has two layers:
- **Canonical tables** (`producers`, `wines`, `wine_vintages`, etc.) — curated, high-quality data. LWIN promoted as backbone. Quality bar is high.
- **source_* staging tables** — per-source raw data for multi-source merge. Each has merge tracking columns (canonical_wine_id, canonical_producer_id, processed_at). See staging table list below for sources.

### Reference Tables (audited S2, corrections applied S3.3)
Countries, regions, appellations, grapes + synonyms, varietal categories, publications, attribute definitions, tasting descriptors, farming/biodiversity certifications, soil types. All seeded, audited, and cross-validated. Query the DB for current counts. See `docs/SCHEMA.md` for field reference, `docs/HISTORY.md` for detail.

### Geographic Data (open for refinement — avoid appellation duplicates)
Geographic boundaries with PostGIS geometry. **One rule: don't create appellations that duplicate existing ones** (DECISIONS.md 2026-04-03). Match to what exists. Sources: UC Davis AVA, Eurac EU PDO, Wine Australia, IPONZ, ldproxy RLP, Nominatim. Query the DB for current coverage.

### Schema
Schema hardened across 3 rounds. All reference data seeded and audited. See `docs/SCHEMA.md` for field reference, `docs/HISTORY.md` for schema change history. Query the DB for table counts.

### Content Tables
**Always query the DB for live numbers.** `data/dashboard.html` has the latest snapshot metrics. Pre-30K rebuild history lives in `docs/HISTORY.md`; 30K sprint journal lives in `data/sprints/_archive/30k/journal.md`.

Key tables: `wines`, `producers`, `wine_vintages`, `wine_grapes`, `wine_vintage_prices`, `wine_vintage_scores`, `wine_insights`, `external_ids`, `data_provenance`. Grade B Edge Function deployed behind `ENRICHMENT_ENABLED` feature flag (disabled). Aspirational tables (`vineyards`, `water_bodies`, `wine_vintage_descriptors`, etc.) preserved as canonical scaffolding, mostly empty.

### Multi-Source Merge Infrastructure (2026-03-18)
Staging-first architecture: all external data goes through per-source staging tables, then a match engine promotes to canonical tables. Prevents dedup crisis at scale.

**Staging tables** (query DB for current row counts, see `docs/SOURCES.md` for full source details):
- `match_decisions` — audit trail for cross-source matching decisions
- **Regulatory/ID:** `source_ttb_colas` (TTB COLA, scrape complete), `source_pro_platform` (12 US states), `source_lwin` (LWIN backbone, all promoted), `source_tabc` (Texas TABC), `source_kansas_brands` (KS KDOR), `source_wv_abca` (WV ABCA, **API dead**)
- **Competition:** `source_berliner` (Berliner Wine Trophy), `source_texsom` (TEXSOM), `source_enofile` (EnofileOnline)
- **UPC barcode:** `source_specs` (Spec's, best barcode source), `source_systembolaget` (Sweden), `source_lcbo` (Ontario), `source_horizon` (**API dead**), `source_pa` (PA PLCB), `source_openfoodfacts`, `source_bc_liquor`, `source_winedeals`
- **Importer:** `source_skurnik`, `source_polaner`, `source_kermit_lynch` + `source_kermit_lynch_growers`, `source_winebow` (best chemistry), `source_european_cellars`, `source_empson` (richest per-wine)
- **Retailer:** `source_wallys`, `source_flatiron`, `source_firstleaf`, `source_best_wine_store`, `source_domestique`, `source_last_bottle`, `source_utah_dabs`, `source_claude_knowledge`

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
- **WineTest** (`pipeline/analyze/winetest/`) — DB quality assessment tool. Haiku-powered benchmark of wines Americans actually encounter. Measures Findability, Depth, Accuracy, Story. ~$0.60/run. Results saved to `data/stats/winetest/`.

See `docs/HISTORY.md` for promotion results, session-by-session build history, and competition/retailer linking details. Sprint 1 session logs archived to `data/sprints/_archive/30k/journal.md`.

### Major Gaps (query DB for current numbers)
- **Price/score coverage improved but still low** — S3.2 relink tripled coverage (price ~5.5%, score ~4.9%). Further gains require retail_wine_create or new source imports.
- **Enrichment pipeline live but paused** — `ENRICHMENT_ENABLED=false`. Needs voice module + L3 gate before re-enabling (grape repair done in S3.3).
- **Weather data: BULK COMPLETE, DRIP UPGRADING.** Nightly scheduled task (`open-meteo-weather-drip`, 3am) upgrades appellations to high-resolution data in wine-count priority order. Pipelines: `pipeline/fetch/nasa_power_weather.py` (bulk, complete), `pipeline/fetch/open_meteo_weather.py --by-wines` (drip, ongoing).
- **Producer metadata:** 0 producers have metadata. Deferred to Sprint 4 ($50-100).
- Many canonical tables still empty (descriptors, wine_relationships, vineyards, producer_timeline, etc.).

---

## Consumer Frontend (S3.4 UI hygiene applied)

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

**Why paused:** Pages render with data but most show identity-only shells until enrichment runs. S3.4 fixed rendering bugs (error boundary, 404, AI disclaimers, heading hierarchy, accessibility). S3.2 tripled price/score depth. S3.3 fixed grape data + display names. Resumes with Sprint 5 vertical slice.

**Design principle (Principle #9):** Structured data in DB → structured display in UI. Numbers, dates, percentages, enums displayed as labeled fact grids — never buried in prose.

---

## Current Focus

**Sprint 5 (AI Bakeoff) — COMPLETE 2026-04-15.** Current-prompt leader: **openai/gpt-5.4-mini**.
**Production model selection is NOT locked.** The bake-off ranked 29 models under the *current*
enrichment prompt across 6 rounds. gpt-5.4-mini topped the leaderboard (composite 3.960, zero
banned-word violations, flat tier performance, $452/170K). But cheap models came close enough
that a better prompt + L3 fact-check gate could flip the ranking — DeepSeek v3.2 at $93/170K
and gemini-3-flash-preview at $159/170K are both viable under better prompting. Full tournament
report: [`bakeoff/scores/tournament_results.md`](bakeoff/scores/tournament_results.md).
Sprint 5 total spend: ~$55 (B5.1-B5.4 ~$3 + B5.5 prose $11.86 + B5.6 tournament R1-R6 ~$40 + B5.7 caching test $0.82).

**Headline takeaway:** cheaper models can do the work; the prompt is the bigger lever.
Garbage-in/garbage-out applies regardless of which model sits at the top of the composite
score. Search grounding and field-split multi-model generation were both provisionally ruled
out — both worth retesting once prompt v2 + L3 fact-check gate land.

**B5.7 additional findings:**
- R4 repechage validated gemini-3-flash-preview as cheap-tier value winner (3.67 / $159)
- R5 field-specialization test: gpt-5.4-mini wins 7/11 correctness fields outright, ties on 4; split-generation not viable
- R6 search-grounded + Chinese: best :online at 3.89 still below gpt-5.4-mini base at 3.96; Sonar disappoints at 2.85
- Prompt caching via OpenRouter: tested, NOT viable currently (OR upstream bug + Opus 4,096-token minimum). Revisit when OR fixes it, when we switch to direct Anthropic API, or when the L3 gate pushes the prompt over 4K static tokens.

**Sprint 6 inputs (pending B5.8 strategy):**
- Sprint 6 scope = **producer dedup** (~4,079 suspected duplicates per dashboard). Dedup is a separate model decision — don't auto-apply gpt-5.4-mini; revisit cheap tier for classification.
- Re-enrichment of 515 demo wines + full-corpus enrichment is **deferred** to a later sprint that lands prompt v2 + L3 fact-check gate first. Using current-prompt rankings as-is would bake in the current-prompt ceiling.
- Preliminary cost projections (current-prompt basis, not a commitment): gpt-5.4-mini ~$452/170K, gemini-3-flash ~$159/170K, DeepSeek base ~$93/170K, Sonnet baseline ~$5,953/170K.
- `ENRICHMENT_ENABLED=false` feature flag stays OFF indefinitely — re-enrichment runs via Python pipeline scripts, not the edge function.

**Sprint 4 (Demo) closed 2026-04-14.** 515/515 wines Grade A, $37.44 spent. Full details in `data/sprints/demo/`.

Live tracker: `data/dashboard.html`.

**Roadmap:** Build (done) → Audit (done) → Fix (done) → Demo (done) → AI Bakeoff (done) → **Sprint 6 (next): producer dedup.** Re-enrichment + prompt v2 + L3 gate follow in a later sprint.

**Core product insight (2026-04-12):** Loam's product is STRUCTURED DATA RENDERED
CLEARLY, not AI prose. The magic is: look up any wine → see organized, trustworthy,
connected data → same fields every time. AI enrichment supports this but is not the
primary value.

**Strategic pivot (2026-04-13):** After 3 sprints with 0 user lookups and 96.7% Grade F
wines, the priority shifts from infrastructure to showing the product to real humans.
Sprint 4 picks producers the user owns wines from, enriches ALL their wines + reference
entities, and shares with 5+ real people. Future sprint scope driven by demo feedback.

### Sprint 4 Tracks

0. **Quick fixes** — ~~wire wine_lookups, merge 3 duplicate producers, drop temp tables, producer search_vector~~ **DONE S4.1**
1. **Wine selection + manifest** — ~~compile wines, map reference entity dependencies~~ **DONE S4.1** (518 wines, 14 producers, manifest at `data/sprints/demo/manifest.json`)
2. **Reference enrichment cascade** — countries → regions → appellations → grapes (top-down, each layer feeds context to next)
3. **Producer enrichment** — build `producer_insights.py` + populate structured producer fields
4. **Wine enrichment** — iterative voice calibration via human review, cascade-grounded prompts
5. **Frontend polish + deploy + feedback** — show to real humans, collect feedback

### Demo Producer Set

**User's collection:** Stag's Leap Wine Cellars (49), Fort Ross (28), López de Heredia (18, merged S4.1), CIRQ (4, merged S4.1), Ridge Vineyards (110, merged S4.1 + internal dedup)
**French recommendations:** Tempier (19), E. Guigal (30), Trimbach (75), Huet (58)
**Benchmarks:** DRC (14), Krug (71), Giacomo Conterno (32), Château Margaux (4), Château Latour (3)
**Total: 515 wines across 14 producers.** Reference dependencies: 4 countries, 20 regions, 51 appellations, 41 grapes.

**Grape display names (S4.1):** Country-based "what's on the bottle" display via `grape_synonyms.is_primary_in_country`. US wines show Zinfandel (not Primitivo), Petite Sirah (not Durif), Carignane (not Carignan Noir). WinePage.tsx chains a synonym lookup after grape load. Same wine can have multiple LWINs in `external_ids` (confirmed S4.1 — Lytton Springs carries both LWIN 1135828 + 1123148).

### Enrichment Cascade Design

```
Countries → Regions → Appellations → Grapes → Producers → Wines
```
Each layer provides context to the next. Wine enrichment prompts include the already-enriched
appellation overview, grape flavor profile, and producer winemaking style. This grounds wine
content in specific, validated reference data rather than relying solely on LLM training data.

### Business Context (S2.9 findings + S4.0 strategic assessment)

**ICP (defined 2026-04-13):** Wine enthusiast who wants a great reference tool — someone who opens a bottle and wants to understand what they're drinking. Mix of wine-savvy friends and casual enthusiasts.

**Competitive position — honest assessment:** Winning on structured data breadth (156K wines), TTB/LWIN backbone linking, appellation weather data, label image archive. Losing on enrichment depth (most wines show identity-only shells), score/price coverage (~5%), producer metadata (0%), user features (0 accounts, 0 lookups), mobile experience, and content freshness. Direct LLM queries (ChatGPT, Perplexity) currently answer most wine questions faster with no lookup friction — Loam's moat is structured data that LLMs can't reliably produce. **Sprint 4 tests this thesis with real humans.**

**Legal/licensing:** All integrated sources are either public domain (TTB CC0), Creative Commons (LWIN), or scraped from public-facing retail/importer catalogs. No licensed score data (Wine Spectator, Parker, CellarTracker). Score coverage is community/competition data only.

**Cost model:** Sprint 4 budget ~$20-25 for enrichment. Full corpus enrichment (~$620-700) deferred to post-demo. No monetization model exists yet. Revenue model deferred to post-demo sprint.

**ENRICHMENT_ENABLED feature flag stays OFF** for the edge function. Sprint 4 enrichment runs via Python pipeline scripts, not the edge function.

### Schema Hardening (complete — see `docs/HISTORY.md` for detail)
3 rounds of hardening applied. Key infrastructure: `set_updated_at()` triggers on 36 tables, `validate_polymorphic_fks()` orphan checker, enrichment_log with cost/model tracking, `appellation_rules` table. `wine_vintage_scores` and `wine_vintage_prices` have `wine_vintage_id` FK (preferred join path). `retailers` table seeded with 13 retailers (all price sources).

### Technical Debt (pre-frontend)
- **RLS policies:** ✅ COMPLETE. All public tables have RLS enabled (S3.7b added 3 utility tables: lwin_class_map, lwin_region_map, specs_producer_bridge; dropped 6 temp tables that lacked RLS). Policy pattern: `public_read_*` (anon+authenticated SELECT), `service_write_*` (service_role ALL). wine_lookups also has `anon_insert` for anonymous page views.
- **Search infrastructure:** ✅ COMPLETE. `search_vector` tsvector columns + GIN indexes on wines, producers, appellations, regions, grapes. Trigram indexes on all searchable name columns. Auto-update triggers on INSERT/UPDATE. Two RPC functions: `search_catalog(query, limit, entity_types[])` for unified cross-entity search bar, `search_wines(query, filter_*, sort_by, limit, offset)` for filtered wine browse. Both granted to anon+authenticated.
- **API views:** 4 views created: `wine_detail_view`, `producer_detail_view`, `wine_vintage_detail_view`, `wine_search_view`.
- **Alias tables:** ✅ SEEDED. region_aliases, label_designation_aliases, appellation_aliases.
- **JSONB metadata:** ✅ CLEAN. All promotable fields moved to proper columns. Remaining metadata is appropriate for JSONB (import provenance, cooperage, clones, narrative notes).
- **Direct Postgres connection:** ✅ `get_conn()` in `pipeline/lib/db.py` via session pooler (psycopg2). Eliminates HTTP/2 ConnectionTerminated crashes. `batch_matcher.py` and `ttb_grape_promote.py` migrated. `get_supabase()` still works for light reads.
- **Nightly agent (Riddler):** DELETED. Do not revive.
- **Session prompts:** `data/session_prompts/` for passing focused work instructions to new sessions.
- **Migrations in git:** `supabase/migrations/` directory created (S3.5). All DDL via Supabase MCP; migration files for DDL tracking going forward.
- **Centralized model IDs:** `pipeline/lib/models.py` — HAIKU_MODEL, SONNET_MODEL, OPUS_MODEL constants. All 33 pipeline scripts import from here (S3.5). Edge function model IDs in `supabase/functions/enrich-wine/index.ts`.
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
- ~~Vivino re-scraping~~ — **CLOSED**: xwines_* tables deleted (S3.1). No re-scrape needed.
- TTB AVA shapefiles at https://www.ttb.gov/ava — research for boundary data
- Tech sheet extraction tool for winery PDFs — design and build

---

## Key Phrases

- **"wrap up"** — End-of-session routine: **consider every doc file** for updates, then commit and push. Go through this checklist — skip only if genuinely nothing changed for that doc:
  - `data/dashboard.html` — **always update first** (metrics, track checklist, session log)
  - `CLAUDE.md` — always update (current state, what was accomplished)
  - `docs/DECISIONS.md` — append if any decisions were made
  - `data/sprints/current.json` + `data/sprints/<name>/` — update sprint state (sessions.json, budget.json, journal.md)
  - `docs/SCHEMA.md` — update if schema changed (CREATE/ALTER/DROP)
  - `docs/SOURCES.md` — update if source status changed (new source, fetcher built, data loaded)
  - `docs/PRINCIPLES.md` — update if product philosophy changed
  - `docs/VOICE.md` — update if tone/content guidance changed
- **"log that"** — Force a DECISIONS.md entry.
- **"briefing"** — Give current state summary anytime mid-session.
