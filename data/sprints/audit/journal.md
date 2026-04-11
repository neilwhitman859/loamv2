# Sprint 2 — Audit Journal

Read-only multi-expert audit producing a prioritized Sprint 3 fix backlog. Sessions log entries here as they run.

---

## S2.1 — 2026-04-11

**Expert:** db_canonical
**Status:** done
**Budget:** $0.00 (no AI calls, pure SQL via Supabase MCP)

### Phase A — Sprint 2 open

Stood up `data/sprints/audit/` with meta.json, sessions.json (9 sessions pre-listed), budget.json ($25 ceiling / $15 target), status.md (~1pg plan), journal.md, prompts/, findings/. Updated `data/sprints/current.json` to point at `audit`. Updated `data/stats/loam_roadmap.json` — reshaped phases 3-6 into Sprint 2 (Audit, active), Sprint 3 (Execute), Sprint 4 (Reference Design), Sprint 5 (Reference Enrichment), and pushed the pre-existing phases 4-9 (Frontend / Input Methods / Data Expansion / Quality / Product / Launch) back. Rewrote `memory/project_sprint_model_and_rf_direction.md` so it no longer claims Sprint 2 = Reference-First (that was superseded on 2026-04-11) — it now covers sprint/session structure and dashboards only, pointing at `project_quality_before_enrichment.md` for the authoritative sprint sequence. Updated `MEMORY.md` index. Moved `data/session_prompts/s2_1_db_canonical.md` → `data/sprints/audit/prompts/s2_1_db_canonical.md`. Verified both dashboards: `powershell -File scripts/dash.ps1` renders Sprint 2 active with S2.1 in progress; `-All` shows S1 archived + S2 active; `python -m pipeline.analyze.loam_roadmap` picks up Sprint 2 banner and live metrics.

### Phase B — DB canonical audit

~50 queries against public canonical + reference tables via Supabase MCP `execute_sql`. Ground-truthed every CLAUDE.md number before trusting it (several were stale — logged as F28). Stayed read-only throughout — no DDL, no DML, no pipeline runs.

**34 findings written to `findings/findings_db_canonical.md`:**

- **P0 (6):** True-dup clusters (1,686 groups / 3,534 wines after filtering terroir-variant false positives), search_vector coverage broken (67% wines NULL, 100% producers NULL), producer metadata near-zero (1 website out of 10,676 producers, 0 coordinates, 0 year_established, 0 parent/child), all producer relationship tables empty (aliases, farming certs, winemakers, timeline, insights), depth-loss from 30K rebuild (archive→public: scores 80% lost, prices 83%, label designations 80%, farming certs 87%, ABV 72%, pH 82%), grape synonym table has 919 primary-name collisions (Syrah-as-synonym-of-Durif class — same time bomb that caused the S1.11 Riesling incident).
- **P1 (12):** dedup normalization key missing terroir (Fourrier Vieille Vigne × 14 grand crus), wine_vintages chemistry near-zero coverage, 77% of wines with no grape links, score/price rollup columns 100% NULL (never backfilled), only 118 numeric critic scores exist, price coverage dropped 5.21%→1.8%, COLA dupes across wines (23,987), dual LWIN systems coexist (lwin + lwin_7 with 10,499 overlap), wines.lwin column is dead (15 rows vs 170K in external_ids), 80% of appellation_grapes rows lack provenance, 69% of appellations have zero wines, label_designation_rules unique constraint allows duplicate NULL-appellation rows.
- **P2 (11):** 46 zero-wine producers, 2 vintage-year outliers (1085, 2099), 30 Grade F wines with insights, 2,394 wines with NULL color, 7 underscore-prefixed temp tables still in public schema with no RLS (135K + 48K + 39K + 110K + 10K + 35 + 13 rows), 1 empty-slug Cyrillic appellation, SCHEMA.md drift in ≥3 places, wine_grapes.grape_id missing FK index, CLAUDE.md stats stale in ≥6 places, vineyard system empty (archive has 815), 12 wine-level join tables empty.
- **P3 (5):** 462 appellation_rules without last_verified_at, self-parent grape MALEGUE 742-22, 1,265 wines on catch-all regions, 57 external_ids pointing to soft-deleted wines, 665 appellations with no lat/long (blocks weather drip).

### Meta-patterns surfaced

Three patterns worth escalating to S2.9 synthesis:

1. **The 30K rebuild depth loss is bigger than CLAUDE.md acknowledges.** Archive vs public: `wine_vintage_scores` 27K→5.4K, `wine_vintage_prices` 140K→23K, `wine_label_designations` 59K→12K, `wine_farming_certifications` 9.4K→1.3K, `wine_vintages.abv` 175K→49K. The LWIN bridge in S1.6 recovered depth for the ~50K LWIN-paired wines; the 100K Phase B + long-tail wines got no bridge. **A non-LWIN archive depth bridge (producer name_normalized + wine name_normalized match) is the single highest-ROI Sprint 3 task.**
2. **Reference layer is in better shape than the canonical wine/producer layer.** Countries (68), regions (389), appellations (3,661), grapes (9,694), appellation_rules (1,165), appellation_weather_years (134,912) have real coverage, clean FKs, and real provenance (post-2026-04-05). The problem lives downstream. This validates Sprint 4 Reference Design as the right structural container for the enrichment pivot — the reference layer is already close to production-ready.
3. **Schema doc drift is ubiquitous.** Every hardcoded count in CLAUDE.md / SCHEMA.md / memory I checked had drifted. Sprint 3 should strip hardcoded counts from doc files and point at live dashboards; Sprint 4 should lock a generator script that writes SCHEMA.md from `information_schema.columns`.

### Scope-breaker check

None of the findings require a Sprint 3 rewrite. All execute inside the existing audit→fix→redesign sequence. No escalation to the user needed under the recalibration clause.

### Deliverables

- `data/sprints/audit/findings/findings_db_canonical.md` — full 34-finding report with evidence, proposed fixes, effort, dependencies
- `data/sprints/audit/` directory fully stood up
- Dashboard + roadmap reflect Sprint 2 active
- Memory cleaned up, prompt moved into sprint

---

## S2.2 — 2026-04-11

**Expert:** db_staging
**Status:** done
**Budget:** $0.00 (no AI calls, pure SQL via Supabase MCP)

### Scope

All 32 `source_*` staging tables — merge state, `processed_at` distribution, value-left-on-table vs canonical, data quality (duplicates, encoding, outliers, staleness), audit trail (`match_decisions`), schema conventions. Canonical content was S2.1 and was deliberately out of scope.

### Method

~35 read-only SQL queries via Supabase MCP `execute_sql`. No DDL, no DML, no pipeline runs, no AI calls. `source_kermit_lynch_growers` flagged as producer-only (no `canonical_wine_id` column); `source_types` is a 30-row taxonomy, not a staging table — excluded.

### 31 findings written to `findings/findings_db_staging.md`

- **P0 (6):**
  - **F1** — 286,918 wine_id pointers are dangling archive references across 29 of 31 wine-bearing staging tables (everything except `source_ttb_colas` and `source_lwin`, which were relinked in S13). All 286K UUIDs are confirmed to exist in `archive.wines` but not `public.wines`. Root cause of the S2.1 F6 depth loss — the S2.1 framing of "promotion never ran" is wrong; promotion did run, then 30K rebuild hard-discarded the target wine_ids. **This is the single highest-ROI Sprint 3 workstream**: mechanical bulk-UPDATE using S13's pattern.
  - **F2** — `processed_at` is never written in 14 of 32 sources (pro_platform, tabc, kansas_brands, wv_abca, horizon, openfoodfacts, skurnik, kermit_lynch, kermit_lynch_growers, winebow, european_cellars, empson, polaner, winedeals). Can't distinguish "never processed" from "processed and no match." Silent re-match hazard.
  - **F3** — Value-left-on-table: ~52K prices (75,100 linked in staging vs 23,220 in canonical `wine_vintage_prices`), ~48K scores/medals (53K linked vs 5,420 promoted), ~215K ABV text values, ~168K linked vintages, ~190K linked appellation strings, ~42K UPC barcodes. All locked behind F1.
  - **F4** — `source_systembolaget` and `source_lcbo` are exactly 2× duplicated from a double-load: systembolaget 6,298 distinct product_ids × 2 = 12,646, lcbo 3,494 distinct SKUs × 2 = 7,030. Half of each table is noise.
  - **F5** — `match_decisions` (7,641 rows) is 100% `source_a='wines' AND source_b='wines'` self-dedup. Zero cross-source merge decisions. The audit trail was designed for staging→canonical attribution but has never been used for that purpose.
  - **F6** — `source_ttb_colas.abv` (text field) has 93,407 malformed values: HTML entities (`&lt;14%`, `&gt;12%`, `&quot;Table&quot;`), negatives (`-14%`), missing leading zeros (`.12`), format errors (`12..5`, `!3.5`, `%12.5`).
- **P1 (11):**
  - **F7** — source_texsom has 802 double-encoded mojibake rows in producer/wine_name (Gewürztraminer → `Gew?ÃÂºrztraminer`, smart apostrophes → `Ã¢â¬Å¡ÃâÃÂ´`). 92% of those are among the already-matched rows, so enrichment will surface garbage.
  - **F8** — 3 dead/stale sources: WV ABCA (API dead), Horizon (API dead, also 0 prices/ABVs despite schema columns), OpenFoodFacts (upstream 3× larger now).
  - **F9** — source_lwin has 69,470 unmatched rows (36.7% of LWIN backbone). CLAUDE.md claim "189K, all promoted" is wrong by 37%.
  - **F10** — Schema convention drift: `match_confidence` on 16/32, `match_status` on 1/32, `updated_at` on 6/32. No shared merge-state contract.
  - **F11** — 14 of 32 staging tables lack `canonical_wine_id` index. F1 relink will sequential-scan the heaviest missing-index tables (pro_platform 346K, tabc 183K, wv 55K, texsom 47K, specs 22K, wallys 19K).
  - **F12** — `retail_promote` shortfall beyond F1: source_systembolaget 12,460 linked → 644 promoted, source_enofile 9,115 → 412, source_pa 5,905 → 728, source_winedeals 3,145 → 7. The 2026-04-04 bulk-SQL rewrite was only partial.
  - **F13** — Wine-match-without-producer-match pattern: systembolaget 8,396 orphaned-producer wines, enofile 5,637, pa 3,101, flatiron 1,992, winedeals 1,805. Fuzzy wine match without producer confirmation is not trustworthy.
  - **F14** — Importer depth sources (empson, winebow, european_cellars, kermit_lynch) all 93-100% orphaned — 890 KL wines + 238 EC wines + 284 winebow wines + 221 empson wines carry fermentation/oak/pH/RS/vinification/vineyard depth that never reached canonical.
  - **F15** — `source_ttb_colas.qualifications` is narrative TTB label text, 2.47M populated, 66,605 wine-linked, zero downstream use. Natural Grade C facts-packet input.
  - **F16** — 14 staging tables need `updated_at` column + `set_updated_at` trigger for freshness detection.
  - **F28** — `source_claude_knowledge` is the ONLY staging table with full merge-state discipline (100% processed_at, match_status, match_notes, promoted_at columns). It's the template to replicate.
- **P2 (9):** WV 10,208 dupe TTB IDs · Kansas 6,886 legit multi-licensee dupes · F18 natural-key uniqueness audit · F19 retail_promote rewrite plan · F20 utah_dabs second-wave never promoted · F21 26/32 missing updated_at · F22 kermit_lynch_growers 77% unmatched · F23 Berliner 4,198 medal shortfall · F24 label image URL doc drift.
- **P3 (5):** F29 tabc/texsom refresh not visible in timestamps · F30 pro_platform.states 346K unused · F31 staging estate size 7 GB (TTB 86%) · processed_at F2 backfill · F25/F26 OFF/Horizon audit notes.

### Meta-patterns surfaced

Three for S2.9 synthesis:

1. **The 30K rebuild's true damage lives in staging, not canonical.** Sprint 3's archive depth bridge must start with a staging relink, then promote. F1 alone unlocks the entire F3 value-left-on-table backlog.
2. **Staging has never had a unified contract.** F10/F21 unification migration (one day) is a prerequisite for any reliable merge engine in Sprint 3.
3. **`match_decisions` is real, well-designed, and never used for its original purpose.** Sprint 3's merge engine must write to it on every decision or the audit trail ends at git history.

### Scope-breaker check

None. F1 is a scope refinement, not a rewrite. Sprint 3 remains "execute prioritized fixes from the backlog." Staging relink slots in as the first step of the archive depth recovery workstream.

### Deliverables

- `data/sprints/audit/findings/findings_db_staging.md` — 31-finding report with SQL evidence, proposed fixes, effort, dependencies
- `data/sprints/audit/sessions.json` S2.2 marked done
- `data/sprints/audit/budget.json` S2.2 $0 recorded

---

## S2.3 — 2026-04-11

**Expert:** wine_canonical
**Status:** in progress
**Budget:** $18.00 hard-stop (pre-auth logged below BEFORE any AI calls)

### Budget pre-spend justification

Per sprint discipline (`status.md`: "any session spending > $15 or approaching the ceiling gets justified in `journal.md` BEFORE spending"), logging the justification for the S2.3 spend in advance of the first Sonnet call.

- **Target:** $12 expected · **Hard-stop:** $18 (set in code)
- **Budget state entering S2.3:** $0 / $25 ceiling · target $15 · combined Sprints 2+3 ceiling $50 still intact
- **Why this spend:** S2.3 is the main AI-spend session of Sprint 2 per the locked plan in `status.md`. It is the only session in Sprint 2 that requires fact-checking content against external knowledge — the DB audits (S2.1, S2.2) and the code/meta audits (S2.5, S2.8) are pure SQL or file reads. A human sommelier bar cannot be simulated without a language model's world knowledge.
- **Why Sonnet 4.6 and not Haiku:** S1.10 enrichment audit (S10 session) ran Haiku on 50 Grade C + 20 Grade B, scored them 2.48/5 and 2.65/5, and found 111 + 91 factual_error tags. That audit used the wrong model — Haiku is a writer, not a fact-checker. S1.12 Stage 2 validated that Sonnet works reliably for the L3 fact-check role. S2.3 is exactly the L3 role applied to the existing content corpus.
- **Why 150 records:** stratified 100-wine + 50-producer sample, per `status.md`. Big enough to find patterns, small enough to fit the budget.
- **Cost model:**
  - 100 wines × ~$0.10 average (input ~800 tokens of canonical facts + insights, output ~600 tokens of structured findings) = $10.00
  - 50 producers × ~$0.08 (smaller facts packets) = $4.00
  - 25 website spot-checks × ~$0.04 (cached content, quick verify pass) = $1.00
  - Headroom for reruns / deep dives = $3.00
  - **Total: $18.00 hard ceiling**
- **Cost model is conservative:** I'll track actual spend every 10 records and hard-stop if the rolling estimate shows the full sample exceeding $18.
- **Cost-benefit for the sprint:** the entire Sprint 3 fix backlog depends on knowing what's factually wrong at the wine-page level. Without S2.3 we would execute Sprint 3 fixes on depth recovery (F1 staging relink, importer depth promotion) without knowing which wines are currently wrong at the sommelier bar. Finding out after we've promoted ~280K staging rows is the wrong order.

**Approved:** user pre-authorized $18 hard-stop in chat before this entry was written.

### Sample strategy (locked)

Wine sample — **100 stratified**, no Grade F identity-only (S2.1 already found that gap):
- 30 × Grade B (audit post-S12 voice-rules content on the 105 available)
- 50 × Grade C (audit pre-S12 voice content on the 4,973 rows — this is what users see today)
- 10 × Grade F **with full facts packet** (LWIN + producer + appellation + grapes set; audit the facts themselves, not the empty insights)
- 10 × marquee (DRC, Lafite, Screaming Eagle, Sassicaia, Vega Sicilia, Opus One, Penfolds Grange, Henschke Hill of Grace, Dom Pérignon, Egon Müller — hardest-to-get-right cases)

Country cross-cut target: US 35 / FR 30 / IT 15 / ES 10 / DE 5 / ROW 5 (approximate, sample what exists)

Producer sample — **50 stratified**:
- 15 × famous (cross-cut marquee list + additional Burgundy/Barolo/Napa icons)
- 20 × mid-tier (KL Growers 120 pool + importer-depth wines that landed in S1.8)
- 15 × long-tail (LWIN-only identity pages — audit whether the identity-only page is CORRECT even if thin)

### Method

1. **Build samples** via SQL stratified selection. Save to `data/stats/s23_sample_wines.json` + `s23_sample_producers.json` for reproducibility.
2. **For each wine:** pull the full facts packet (wines + producer + appellation + region + country + grapes + external_ids + wine_vintages + wine_insights if grade ≥ C), format as structured JSON, call Sonnet 4.6 with explicit uncertainty-flagging instructions ("if you cannot verify a claim from training knowledge with high confidence, mark it `unknown`, NOT `disputed`"), parse response into `{ wine_id, verdict: pass|warn|fail, error_class, evidence, confidence }`.
3. **For producers:** same pattern, producer facts packet.
4. **Website spot-checks:** for the 10 marquee + 15 famous, add a second Sonnet pass with the producer's official website content fetched via `WebFetch`, prompting "compare Loam's claims to the official source."
5. **Rollup into error-class findings:** aggregate all the per-record verdicts by error class (wrong_vintage, wrong_grape, wrong_appellation, oversimplified, missing_context, fact_invented, stale_designation, etc.) with 3-5 representative examples per class.
6. **Findings format:** same `F1..FN` severity-tagged shape as S2.1/S2.2.

### Discipline

- Skip re-logging already-known issues from S2.1/S2.2. Only log novel wine-page-level findings.
- "Sonnet disputes" without primary-source confirmation → mark as `warn` not `fail`. Primary-source confirmation elevates to `fail`.
- Hard-stop at $18 spent.
- Sonnet uncertainty counts as "no signal," not as a pass OR fail.

### Pivot mid-session: inline Opus vs Sonnet API

After building the stratified sample, I pivoted from "build a Python script that calls the Anthropic API on each wine" to "fact-check inline in the conversation using my own Opus 4.6 training knowledge + WebFetch for primary-source spot-checks." Rationale: **Opus 4.6 is more capable than Sonnet 4.6 for wine fact-checking**; the $18 Sonnet budget was based on the assumption that a sub-flagship model would do the work, but with the 1M-context Opus session available, the project saves $18 AND gets higher-quality audit signal. I noted the pivot here BEFORE starting any audit reasoning so the discipline doesn't slip retroactively.

The $18 S2.3 pre-auth does not expire — it rolls forward to Sprint 3 F10 (the L3 re-fact-check pass on the existing 5,108 wine_insights rows), which is a natural place for the Sonnet spend to happen.

### Deliverables

- `data/sprints/audit/findings/findings_wine_canonical.md` — 22 findings (9 P0 / 8 P1 / 4 P2 / 1 P3) with SQL evidence, 3 primary-source WebFetch verifications, proposed fixes, effort, dependencies
- `data/stats/s23_sample_wines.json` + `s23_sample_producers.json` — stratified sample saved for reproducibility and for any Sprint 3 re-audit
- `data/stats/s23_build_sample.py` — one-shot sample builder (not reusable)
- Sprint state files (sessions.json S2.3 → done, budget.json S2.3 $0 + pre-auth rolled forward, journal.md this section)

### Scope-breaker check

None. All 22 findings execute inside the Sprint 3 envelope. The largest workstream is F10's re-fact-check pass on 5,108 rows, estimated $30-50 — inside the Sprint 2+3 combined $50 ceiling when combined with $0 from S2.1/S2.2/S2.3.

One sequencing recommendation: Sprint 3 should run in order (a) S2.2 F1 staging relink → (b) S2.3 F3 producer seed file → (c) F2/F7/F8 grape repair → (d) F6 color+country repair → (e) F10 L3 re-fact-check → (f) content regeneration. Skipping any of (a)-(e) will re-contaminate (f).
