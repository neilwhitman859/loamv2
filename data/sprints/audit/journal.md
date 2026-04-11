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

---

## S2.4 — 2026-04-11

**Expert:** wine_reference
**Status:** in progress
**Budget:** $0 expected (Opus inline per the S2.3 pattern — ratified as the default for audit/reasoning work in `docs/DECISIONS.md` 2026-04-11 and `memory/feedback_opus_inline_reasoning.md`)

### Scope

Hand-verified content correctness of the reference layer:

- **`appellation_rules`** (1,165 rows) — min ABV, yield caps, grape requirements, aging minimums, elevation, established year. Sample against INAO / DOCG / TTB AVA primary sources.
- **`appellation_grapes`** (10,414 rows) — required/permitted/dominant flags per appellation. Start with the appellations that matter most (Burgundy grand crus, Bordeaux AOCs, Barolo, Champagne, Napa AVAs).
- **`grape_synonyms`** (34,820 rows) — verify known-correct mappings and hunt for the root cause of S2.3 F2 (Chardonnay/Pinot Blanc linkage bug affecting 2,743 of 2,809 Chardonnay-named wines). S2.1 F7 already flagged 919 primary-name collisions — cross-reference.
- **`varietal_categories`** (161 rows) — category structure and membership.
- **`grapes` parent/child** — check for cycles, invalid parents, and a spot-check of known parentage (Cabernet Sauvignon = Cab Franc × Sauv Blanc, etc.).
- **`appellation_soils`** (930 links across 304 appellations) + `soil_types` (39 rows) — verify soil-to-appellation associations against primary sources (TTB AVA docs, INAO, regional councils). S2.3 F14 flagged fabricated soil claims in AI content ("Hunter Valley volcanic", "Santa Ynez Franciscan shale") — but those lived in `wine_insights`. S2.4 audits whether the structured reference soil data is any better.
- **Spot-check:** `tasting_descriptors` (304), `farming_certifications` (21), `biodiversity_certifications` (7), `label_designations`, and alias tables (`region_aliases`, `appellation_aliases`).

S2.1 already covered the reference layer **structurally** (FK integrity, orphan checks, duplicate detection, staleness). S2.4 is **content correctness** — is the data actually right.

### Method

Opus inline, same pattern as S2.3. Query the DB for real data, reason across it using training knowledge, WebFetch primary sources (INAO, DOCG, TTB AVA regulations, regional wine council sites, Wikipedia for spot-checks) when a finding needs external corroboration. No Haiku/Sonnet API calls. Target $0 actual spend.

### Discipline

- **Read-only.** No DDL, no DML, no fixes. Findings only.
- **Skip already-known issues** from S2.1 / S2.2 / S2.3 unless S2.4 adds new content-correctness evidence.
- **Chardonnay/Pinot Blanc root cause is in scope** — if the bug lives in `grape_synonyms`, that's a S2.4 finding; if it's in name-matching code, that's a S2.5 finding but I note the boundary.
- **Sample, don't enumerate** — 10,414 appellation_grapes rows and 34,820 synonyms are too many to check one by one. Query distributions, pull 20-50 spot-checks from high-signal appellations (Burgundy grand crus, Bordeaux crus classés, Barolo, Napa Cabernets, Champagne, Rioja Gran Reserva, Mosel Grosses Gewächs).
- **Each finding needs concrete evidence** — SQL query + result + primary-source link where relevant — and a proposed fix.

### Method

~25 read-only SQL queries via Supabase MCP `execute_sql`. 1 WebFetch to Wikipedia to verify the La Tâche area figure (5.03 ha vs suspected 6.06 ha — Wikipedia confirmed 5.03, so I dropped that as a finding). All audit reasoning done inline via Opus 4.6 + training knowledge. $0 project spend.

### 30 findings written to `findings/findings_wine_reference.md`

- **P0 (8):**
  - **F1** — varietal_categories has 5+ confirmed wrong-grape links: Merlot → GROLLEAU NOIR (Loire grape, not Merlot), Riesling → CROUCHEN (historical "Cape Riesling" misnomer), Verdejo → TROUSSEAU NOIR (wrong country, wrong color), Greco → ALBANA BIANCA (different variety), St. Laurent → MUSCAT ST. LAURENT (pattern-matched to wrong grape).
  - **F2** — Root cause of S2.3 F2 Chardonnay/Pinot Blanc bug: PINOT BLANC grape row has VIVC synonyms `PINOT CHARDONNAY`, `CHARDONNET PINOT BLANC`, `PINOT BLANC CHARDONNET`, plus `PINOT GRIGIO` / `P. GRIGIO` (which is actually Pinot Gris, a distinct variety). Any name resolver finds Chardonnay matching both CHARDONNAY BLANC (correct) and PINOT BLANC (via synonym). Fix: delete the 4 polluting synonyms + audit resolver preference order (S2.5 hand-off).
  - **F3** — 121 appellations have slash-concatenated alias names stored in `appellations.name`: `Hermitage / Ermitage / L'Hermitage / L'Ermitage`, `Porto / Port`, `Clos de Vougeot / Clos Vougeot`, `Priorat / Priorato`, etc. Breaks UI display, wine linkage, alias tables already exist for this purpose.
  - **F4** — French AOC names missing diacritics: `Echezeaux` (should be Échézeaux), `Grands-Echezeaux`, 6 Saint-Emilion variants (should be Saint-Émilion).
  - **F5** — Pauillac `rules.classification` is internally contradictory: names 3 Premier Crus (Lafite, Latour, Mouton) but says "1 Premier Cru"; claims "1 Troisième" (Pauillac has 0); "5 Quatrième" (Pauillac has 1, Duhart-Milon). Total 18 is right, breakdown wrong in 3 of 5 tiers. Primary source URL is real INAO PDF — transcription error.
  - **F6** — `grapes.name` uses VIVC cépage+suffix form (CHARDONNAY BLANC, MERLOT NOIR, TEMPRANILLO TINTO, CARIGNAN NOIR, MONASTRELL used on French CdP). Cascades into every downstream table. Fix: populate existing `display_name` column with common English names; update frontend to prefer display_name.
  - **F7** — 921 grape-synonym primary-name collisions confirmed (re-ran S2.1 F7 with higher precision). Worst offender: `AGLIANICO` is a synonym of 4 different primary grapes (Magliocco Dolce, Aglianicone, Lambrusco Maestri, Negro Amaro) plus being its own primary. 551 grapes affected across 657 distinct colliding synonyms.
  - **F8** — **S2.3 F7 correction:** GARRO is a real VIVC grape #7326 (Spanish crossing, species=vinifera, parentage_confirmed=true, origin Spain). Not invented. Messina Hof error lives in wine_grapes linkage, not in the grapes table — the Papa Paulo Port wine was matched to the wrong grape_id. Actual grape is Lenoir/Black Spanish (present in Loam as BALDWIN LENOIR and JACQUEZ).
- **P1 (14):**
  - **F9** — `established_year` poisoned with fake 1973 default on 240 French AOCs + 105 Italian DOCs (345 rows). Chambertin actual 1936, Barolo DOC 1966, Brunello DOCG 1980, Champagne 1936.
  - **F10** — `classification_level` dominated by German einzellage (1,179 of 1,743 populated, 67.6%). Only 2 rows tagged grand_cru, 8 tagged aoc, 0 tagged docg/doc/ava/cru_classe/premier_cru.
  - **F11** — `appellation_rules.rules` JSONB has no stable schema: `area_acres` vs `area_ha`, `elevation_range_ft` vs `elevation_range_m` vs `elevation_max_m+min_m`, `established_date` vs `established_year` vs `established`, `min_abv` vs `min_alcohol_pct`. Extreme case: Petit Chablis uses `{"Chardonnay": 100}` while Chambertin uses `{"principal": "Pinot Noir"}`.
  - **F12** — Yield and ABV extraction sparse in appellation_rules: only 80 of 1,165 rows (6.9%) have max_yield and 59 (5.1%) have min_alcohol at top level.
  - **F13** — 434 of 1,165 rules (37%) have ≤3 top-level keys. Burgundy Grand Crus (Chambertin, Charmes-Chambertin, Bonnes-Mares, Montrachet, Musigny) got bare stubs while Italian DOCGs got deep disciplinare data. Depth inverted from user expectation.
  - **F14** — Châteauneuf-du-Pape appellation_grapes uses Spanish grape names (GARNACHA BLANCA/ROJA/TINTA, MONASTRELL) on a French appellation. Also missing Picardan (one of 13/18 authorized varieties per INAO CDC, present in rules JSONB but not materialized into appellation_grapes).
  - **F15** — Grape name inversion: `VERDOT PETIT`, `MESLIER PETIT` exist as canonical names in the grapes table. S2.3 F9 flagged wine-level counts (284, 1,001 links); S2.4 confirms the root cause is in `grapes.name` itself.
  - **F16** — Chambertin / Charmes-Chambertin / Bonnes-Mares missing accessory grapes (Chardonnay, Pinot Blanc, Pinot Gris at ≤15%) in appellation_grapes. Inconsistent with La Tâche which has them loaded correctly. Same Burgundy Grand Cru rule applies.
  - **F17** — `appellation_soils` has ZERO provenance columns (only 2 columns: appellation_id, soil_type_id). Cannot audit or re-verify 930 soil links.
  - **F18** — Hunter Valley linked to Basalt in appellation_soils, likely reinforcing S2.3 F14 "Hunter Valley volcanic" confabulation. Hunter Valley's dominant soils are alluvial/clay/sandstone, not basalt. Santa Ynez has zero rows in appellation_soils (S2.3 F14 "Franciscan shale" claim was pure AI confabulation with no structured anchor).
  - F19 — `soil_types` has one junk row: "Ite" ("-ite" is a suffix, not a soil type). Description admits "used broadly to describe."
  - F20 — `source_organization` has massive string fragmentation (INAO in 20+ variants, MASAF 15+, SAWIS/WOSA 15+, MAPA 8+, Wines of Greece 20+). Cannot group by source for coverage reports.
  - F21 — TTB-sourced appellation_rules rows 98% unverified (4 of 188 have last_verified_at); split across two string-variant buckets, one 100% verified (50/50), other 2%.
  - F22 — Rioja appellation_grapes lists `TROUSSEAU NOIR` as required — technically correct-by-DNA (Maturana Tinta = Trousseau Noir per 2008 DNA study) but misleading canonical form for Spanish context.
- **P2 (7):** F23 Margaux communes duplicate Cantenac · F24 Napa Valley missing California 100% county rule · F25 six correct-but-confusing varietal_categories synonym routings (Zinfandel→Primitivo, Shiraz→Syrah, etc.) · F26 tasting_descriptors mixes structural/palate/flavor · F27 farming/biodiversity certs clean (positive finding) · F28 parentage_confirmed lacks parentage_source column · F29 appellations structured columns drift from rules JSONB.
- **P3 (1):** F30 source_url primary-source coverage 81%, Wikipedia only 2% (positive finding — provenance infrastructure is good).

### Meta-patterns surfaced

Four for S2.9 synthesis:

1. **The reference layer's provenance infrastructure is good; content loads are uneven.** `appellation_rules` has 13 well-designed columns including provenance. The problem is inside the JSONB (F11 schema drift, F13 37% thin stubs). Sprint 4 should keep the schema and fix the loader.
2. **Canonical grape naming is the single biggest cross-cutting issue.** F1, F2, F6, F7, F8, F14, F15, F22 are all facets of: `grapes.name` uses VIVC cépage+suffix form, downstream tables inherit it, resolvers have no disambiguation policy. **Fix grapes table first, then cascade.** Pre-Sprint-3 workstream.
3. **Appellation naming has parallel issues.** F3, F4, F9, F10, F29 come from the same root: one loader per source, each with its own naming convention, results landed in shared columns without normalization.
4. **S2.3 findings traceable to S2.4 data issues:** S2.3 F2 → F2 root cause. S2.3 F7 → F8 correction. S2.3 F9 → F15 root cause. S2.3 F14 → F18 structured-data reinforcement. Validates the S2.3→S2.4 ordering.

### Sprint 3 sequence refined

Previous (from S2.3): (a) staging relink → (b) producer seed → (c) grape repair → (d) color/country → (e) L3 re-fact-check → (f) content regeneration

S2.4 refines step (c) grape repair into:
- **3a** — grapes.name canonical cleanup + display_name populated (F6, F15)
- **3b** — grape_synonyms collision resolution (F7) + delete PINOT BLANC's 4 polluting synonyms (F2)
- **3c** — fix varietal_categories 5+ wrong grape_id links (F1)
- **3d** — re-run grape resolver against wine_grapes to fix 2,743 Chardonnay/Pinot Blanc mismatches (S2.3 F2)
- **3e** — only then run F11-F13 JSONB content backfill + F14 appellation_grapes language/Picardan fixes + F17 appellation_soils provenance schema

Total S2.4 findings blocking Sprint 3: **8 P0 + 14 P1 = 22 items**.

### Scope-breaker check

None. All findings slot into existing Sprint 3 envelope; several graduate to Sprint 4 reference redesign. Recalibration: Sprint 3 scope grows ~20 items, Sprint 4 scope grows by F11/F29 canonical rules schema workstream.

### Deliverables

- `data/sprints/audit/findings/findings_wine_reference.md` — 30-finding report (8 P0, 14 P1, 7 P2, 1 P3)
- `data/sprints/audit/prompts/s2_4_wine_reference.md` — session prompt (written at session start for reproducibility)
- `data/sprints/audit/sessions.json` — S2.4 → done, $0 spend
- `data/sprints/audit/budget.json` — S2.4 entry, running total $0.00 / $25.00
- `data/sprints/audit/journal.md` — this section

---

## S2.5 — 2026-04-11

### Session
Sprint 2 Session 5. Expert hat: **code** (pipeline scripts, shared libs, edge functions, scheduled tasks, conventions, error handling, dead code). Opus 4.6 inline + MCP edge function reads + Supabase SQL verification queries. $0 actual project spend. Continues the ratified Opus-inline pattern from S2.3/S2.4.

### Scope audited

- `pipeline/` — 265 Python files, 77,560 LOC across `fetch`, `load`, `promote`, `enrich`, `identity`, `reference`, `geo`, `analyze`, `vivino`, `lib`
- `pipeline/lib/` — shared libs: `db.py`, `normalize.py`, `resolve.py`, `importer.py`, `merge.py`
- `supabase/functions/` — 2 deployed edge functions: `enrich-wine` + `describe-chemical` (read via MCP `get_edge_function`)
- `scripts/` — dev tooling (dash.ps1, legal source fetchers, misc SQL)
- `scripts_archive/node/` — 116 archived .mjs files
- `frontend/src/` — grep for grape display field usage (confirms frontend uses display_name correctly)
- Nightly scheduled task — `open_meteo_weather.py` drip behavior

Scoped out: frontend UX (S2.7), voice/editorial (S2.6), docs/memory drift (S2.8), business/synthesis (S2.9). Also scoped out: re-auditing the reference data layer (already covered by S2.4).

### Method

- Opus 4.6 inline (ratified by DECISIONS.md 2026-04-11 + memory/feedback_opus_inline_reasoning.md)
- Glob/Grep across pipeline tree
- Targeted Read of hot-path files: `db.py`, `normalize.py`, `resolve.py`, `merge.py`, `batch_pipeline.py`, `lwin_long_tail.py`, `ttb_grape_promote.py`, `grape_blend_promote.py`, `grape_from_name.py`, `haiku_grape_extract.py`, `relink_staging_to_current.py`, `importer_grape_promote.py`, `import_lwin.py`, `build_display_name.py`, `fix_batch0_display.py`, `open_meteo_weather.py`, `importer.py`
- MCP `get_edge_function` to retrieve both edge function source trees
- Targeted SQL queries via MCP `execute_sql` to verify code assumptions against live DB (e.g., `grapes.display_name` coverage: 9,692 of 9,694 populated; Chardonnay wine cohort grape distribution)
- Traced S2.3 F2 Chardonnay/Pinot Blanc bug through the actual DB state of one specific wine (042abb2f-6cce-4623-baec-90a18e60f4ac, De Bortoli "17 Trees") — found 4 TTB COLAs (Chardonnay/Cab/Shiraz/Shiraz) linked to ONE canonical wine
- Read-only throughout. No pipeline runs, no DDL, no DML, no fixes

### 32 findings written to `findings/findings_code.md`

- **P0 (9):**
  - **F1** — `describe-chemical` edge function is DEPLOYED, ACTIVE, `verify_jwt: false`, uses the shared `ANTHROPIC_API_KEY`, and has **zero wine logic** — it's a "chemical industry analyst" prompt from an unrelated project. Unauthenticated credential burn. Delete.
  - **F2** — Code root cause of S2.3 F2 Chardonnay/Pinot Blanc bug identified: `batch_pipeline._match_ttb_to_wine` (pipeline/identity/batch_pipeline.py:693-716) matches only on `fanciful_name`, falls through to `"if len(wines) == 1, assume match"`, causing 4 grape-specific COLAs to collapse onto 1 canonical wine for ~2,700 wines. Combined with `DISTINCT ON (canonical_wine_id)` in `ttb_grape_promote` (F17) and multi-run accumulation, wines acquire CHARDONNAY BLANC + PINOT BLANC + PINOT NOIR + CAB SAUV stacks. Grape resolver in `pipeline/lib/resolve.py` itself is correct — the bug is upstream in wine-identity dedup.
  - **F3** — `relink_staging_to_current.py` Session-13 one-off script: its `STAGING_TABLES_WINE` const lists ONLY `source_ttb_colas` with an unresolved `# Add others if they have canonical_wine_id columns` TODO. 29 OTHER staging tables still hold dangling archive wine_id pointers. This is the code-level cause of S2.2 F1 (286,918 dangling wine_id pointers). Sprint 3's #1 blocker.
  - **F4** — `enrich-wine` edge function (supabase/functions/enrich-wine/index.ts) builds prompts using `grapes(name)` — the VIVC cépage form ("CHARDONNAY BLANC", "MERLOT NOIR") — not `grapes(display_name)`. Frontend correctly uses display_name; enrichment prompts inherit wrong labels. Every Grade B enrichment sees garbage grape names. Pipeline enrich scripts (`appellation_insights.py`, `country_insights.py`, `region_insights.py`) have the same bug.
  - **F5** — Model version drift: `describe-chemical` uses `claude-haiku-4-5-20251001`, `enrich-wine` uses `claude-sonnet-4-20250514` (stale), 12 pipeline scripts hardcode `claude-sonnet-4-20250514` while 3 use `claude-sonnet-4-6`. Three coexisting model IDs, no central config.
  - **F6** — `grape_from_name.py` builds `grapes_raw = {normalize(row[1]): ... for row in cur.fetchall()}` — if `display_name` is NULL, `normalize(None)` → `""` and all NULL rows collapse to one key. Silent-failure time bomb.
  - **F7** (overlaps P0/P1 — logged P1) — `haiku_grape_extract.resolve_grape` uses 70%-coverage containment match over all 34,820 grapes+synonyms, O(n*m) per call, with edge cases where short grape names match longer synonyms unexpectedly.
  - **F8** (logged P1) — `BATCH_0_PRODUCERS` hardcoded in `batch_pipeline.py` source code instead of `data/roster/batch_0.json`.
  - **F9** (logged P1) — `batch_pipeline._load_reference_data()` loads synonyms into `self.grapes` dict WITHOUT separating from primary names — synonyms can overwrite primary name mappings for collision cases (920 collisions per S2.4 F7).
- **P1 (14):** F3, F7, F8, F9 (listed above), plus F10 open-meteo drip has no error-logging side channel, F11 4+ duplicate grape lookup implementations across pipeline, F12 `grape_from_name.py` dict overwrite silent collision, F13 `except Exception:` used 422 times (silent error swallowing), F14 `get_conn()` not used as context manager (potential leaks), F17 `DISTINCT ON (canonical_wine_id)` picks arbitrary TTB row, F18 `lwin_long_tail.py` inserts wines without `display_name` (50,908 wines affected, biasing S2.3 F2 cohort measurement), F24 no `supabase/migrations/` directory, F31 edge function source not in git.
- **P2 (7):** F15 202x `sys.path.insert` hacks, F16 13 separate `INSERT INTO wines` sites, F19 `scripts/` has 11 numbered legal-source batch files no ownership, F20 CLAUDE.md claims `data-accuracy-agent` scheduled task exists but not in repo, F21 three overlapping dedup scripts, F22 `pipeline/promote/` 55 files no README, F23 `pipeline/vivino/` marked archive but in active tree, F25 143 hardcoded grape aliases + 32 region aliases in `resolve.py` duplicating DB alias tables, F26 `resolve_grape` step 5 suffix fallback overmatching, F27 `batch_matcher.py` vs `generic_matcher.py` unclear current, F28 producer-specific scrapers (ridge/stags_leap/tablas_creek) not migrated to generic Haiku scraper.
- **P3 (2):** F29 `pipeline/analyze/winetest/` sub-package is a tool not an analyzer, F30 `scripts_archive/node/` 116 files no manifest, F32 no CI/pre-commit checks.

### Key data-backed SQL verifications

1. **`grapes.display_name` coverage:** 9,692 of 9,694 grapes have populated display_name. S2.4 F6's wording implied "add display_name column" but the column exists and is 99.98% populated. The real finding is a frontend/edge-function bug (F4) — code reads `name` not `display_name`.
2. **De Bortoli "17 Trees" (wine_id 042abb2f-6cce-4623-baec-90a18e60f4ac):** 4 TTB COLAs linked, grape_varietals are "Chardonnay", "Cabernet Sauvignon", "Shiraz", "Shiraz". Current wine_grapes show CHARDONNAY BLANC + PINOT BLANC linked. Zero lwin records, so this wine came from TTB-linking alone via `batch_pipeline`. Proves F2 multi-COLA collapse.
3. **Chardonnay cohort grape distribution** (WHERE `display_name ILIKE '%chardonnay%'`, 2,796 wines with grapes, 5,400 total links, 1.93 avg links per wine): PINOT BLANC 2,743, CHARDONNAY BLANC 2,434, PINOT NOIR 83, CAB SAUV 19, VIOGNIER 12, SYRAH 11. Confirms most wines have BOTH wrong grapes stacked.
4. **`name ILIKE '%chardonnay%'` wines: 7,466, but `display_name ILIKE '%chardonnay%'` wines: 2,809 — only 18 overlap.** 7,448 of the 7,466 have NULL display_name. These are the LWIN long-tail wines from `lwin_long_tail.py` which doesn't populate display_name (F18). The S2.3 F2 audit was SELECTION-BIASED toward BATCH_0 wines which inflated the bug rate — but the bug is real.

### Meta-patterns surfaced (for S2.9 synthesis)

1. **Every code bug traces back to "too many grape resolvers."** F6, F7, F9, F11, F17, F26 are facets of the same problem: grape resolution is duplicated across 4-5 sites with subtly different semantics. Consolidating on `ReferenceResolver.resolve_grape()` closes all of them at once. Pre-Sprint-3 workstream: refactor grape resolution to ONE site.

2. **Infrastructure-as-code gap is systemic.** F1 (rogue edge function), F24 (no migrations dir), F31 (no edge function source in git), F32 (no CI), F19 (`scripts/` untracked ownership) are symptoms of "the git repo is not a complete picture of the live system." Sprint 3 should land `supabase/migrations/` and `supabase/functions/` at minimum.

3. **Wine creation is a 13-site operation.** F16 (13 INSERT sites), F18 (display_name bifurcation), F8 (hardcoded roster) all point at the same root: no `create_wine()` factory. Sprint 3 F3 producer seed is the natural landing place for `pipeline/lib/wines.py::create_wine()`.

4. **S2.3 F2 Chardonnay/Pinot Blanc is a CODE + DATA compound.** Data side alone (S2.4 F2) is necessary but not sufficient — F2 (multi-COLA collapse) + F17 (DISTINCT ON arbitrary pick) + F11 (resolver duplication) are the other half. Sprint 3 grape workstream must include code fixes running in parallel with data fixes.

5. **S2.2 F1 staging archive-ID relink has a specific code owner and a 2-hour fix.** F3 identifies `relink_staging_to_current.py::STAGING_TABLES_WINE` needing extension from 1 to 30 tables. Blocks ~52K prices + ~48K scores + ~200K vintage-grade fields.

### Sprint 3 sequence refined (code items added)

Previous (post-S2.4): (a) S2.2 F1 staging relink → (b) S2.3 F3 producer seed → (c) refined grape-repair workstream (3a-3e) → (d) F6 color+country repair → (e) F10 L3 re-fact-check → (f) content regeneration

S2.5 additions:
- **(a) staging relink** now has a concrete code handle — extend `STAGING_TABLES_WINE` in `relink_staging_to_current.py` from 1 to 30 tables (S2.5 F3). Still the #1 Sprint 3 task.
- **(c) grape repair** gains 3 sub-tasks: **3c.5** fix multi-COLA collapse in `batch_pipeline._match_ttb_to_wine` (S2.5 F2), **3c.6** fix `DISTINCT ON` arbitrary pick in `ttb_grape_promote` (S2.5 F17), **3c.7** consolidate grape resolvers on `ReferenceResolver` (S2.5 F11).
- **NEW pre-req — code hygiene:** Delete `describe-chemical` edge function (S2.5 F1), vendor `enrich-wine` source into `supabase/functions/` (S2.5 F31), centralize Anthropic model IDs via `pipeline/lib/models.py` (S2.5 F5). All ~30 min combined.

Total S2.5 findings blocking Sprint 3: **9 P0 + 14 P1 = 23 items added to backlog.**

### Scope-breaker check

None. All findings slot into existing Sprint 3 envelope. F2 changes the sequence of grape-repair sub-steps but doesn't expand total scope — the S2.4 data fixes still need to happen, now with co-running code fixes. F24 (migrations dir) and F31 (edge function source) are Sprint 3 pre-requisites that cost <1 hour each.

### Deliverables

- `data/sprints/audit/findings/findings_code.md` — 32-finding report (9 P0, 14 P1, 7 P2, 2 P3)
- `data/sprints/audit/prompts/s2_5_code.md` — session prompt (written at session start for reproducibility)
- `data/sprints/audit/sessions.json` — S2.5 → done, $0 spend
- `data/sprints/audit/budget.json` — S2.5 entry, running total $0.00 / $25.00
- `data/sprints/audit/journal.md` — this section

