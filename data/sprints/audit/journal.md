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

---

## S2.6 — 2026-04-11

### Session
Sprint 2 Session 6. Expert hat: **voice** (editorial correctness, prompt discipline, cliché density, confabulation resistance). Opus 4.6 inline + MCP edge function read + LIKE-scan quantification across the full enriched corpus. $0 actual project spend. Continues the ratified Opus-inline pattern from S2.3/S2.4/S2.5.

### Scope audited

- `docs/VOICE.md` — read as the yardstick for every finding
- **Prompt source files:**
  - `pipeline/enrich/enrich_prompts.py` (the new tightened Grade B/C wine prompts, `VOICE_RULES_BLOCK` with 15 hedging words + 20 sommelier-theater phrases + NEVER INVENT block)
  - `pipeline/enrich/appellation_insights.py` (reference-layer, weak 8-word banlist)
  - `pipeline/enrich/region_insights.py` (same pattern)
  - `pipeline/enrich/country_insights.py` (same pattern)
  - `pipeline/enrich/grape_insights.py` (same pattern, but best food-pairing rules in Loam)
  - `supabase/functions/enrich-wine/index.ts` (live, version 3, read via MCP `get_edge_function` since source not in git per S2.5 F31)
- **Enriched corpus (5,454 rows total):**
  - `wine_insights` — 5,108 rows (46 Grade B, 5,062 Grade C; all enriched 2026-04-10)
  - `wine_vintage_tasting_insights` — 5,164 rows (spot-checked; structural audit deferred to S2.7)
  - `region_insights` — 202 rows, 15 countries, all 2026-03-06
  - `appellation_insights` — 82 rows, 100% US AVAs, all 2026-03-06
  - `country_insights` — 62 rows, all 2026-03-06
  - `grape_insights` — 0 rows (never run)
  - `wine_food_pairings` — 0 rows (CLAUDE.md claims 809, stale; archive has 809)

Scoped out: frontend rendering (S2.7), docs/memory drift (S2.8), business positioning (S2.9), code quality beyond the enrichment prompts (already covered S2.5).

### Method

- Opus 4.6 inline (ratified by DECISIONS.md 2026-04-11 + memory/feedback_opus_inline_reasoning.md)
- 4 parallel `Read` calls on the pipeline/enrich/*.py prompt files + 1 MCP `get_edge_function` on enrich-wine
- ~20 Supabase MCP `execute_sql` queries: corpus inventory by tier/country/date, enrichment_log model breakdown, stratified Grade B + Grade C content samples, full-corpus LIKE scans on banned-word patterns (hedging, sommelier theater, generic filler), receipt queries for confabulation evidence (Chardonnay+Pinot Blanc contamination pool, Knights Valley → Beringer Alluvium feedback loop, DRC Corton-Charlemagne MLF claim, Schramsberg J Schram rosé/sparkling mix-up)
- Read-only throughout. No pipeline runs, no DDL, no DML, no fixes.

### 32 findings written to `findings/findings_voice.md`

- **P0 (9):**
  - **F1** — 346 reference-insight rows written with weak reference-layer prompts. LIKE scan: 71% of 82 appellation soil profiles contain "well-draining", 62% "ancient", 60% "volcanic" (many incorrectly), 20% use "force vines/roots to struggle/dig deep" template. 50% of appellations, 37% of regions, 44% of countries contain "elegant/elegance" — default adjective crutch. "enrich_prompts.py" is the only tightened voice source in the codebase; 5 other prompt locations are behind it.
  - **F2** — `enrich-wine` edge function (live, version 3) ships a weak voice preamble with zero banned-word list + stale `claude-sonnet-4-20250514` + reads `grapes.name` not `display_name` (overlap S2.5 F4/F5/F31). Grade B LIKE scan proves the edge function is behind `enrich_prompts.py`: 59% "likely", 52% "showcases", 41% "premium", 39% "elegant" (vs Grade C under tightened prompt: 2%, 1%, 3.9%, 16%). 10-50x rate deltas are the cleanest signal that two different prompt systems produced the two tiers.
  - **F3** — Tightened voice rules do NOT prevent factual confabulation. 5 random Grade C hook samples, every one contains an invented fact: Schramsberg J Schram described as "a still rosé from a sparkling house" (J Schram is Schramsberg's top-tier sparkling); Pax Obsidian "likely 14.5%+" ABV + "Durif, a Rhône outlier rarely seen in California" (Durif IS Petite Sirah, ubiquitous); Perrin CdP Les Sinards narrated as "white wine masquerading under red grape varieties" (rationalization of Spanish grape names on French CdP per S2.4 F14); Merry Edwards "40 years refining Pinot Noir" (founded 1997, 30 years max); DRC Corton-Charlemagne Grade B "They avoid malolactic fermentation in most vintages" (factually wrong). **Session-10 feature flag is still the correct call.**
  - **F4** — 487 Chardonnay+Pinot Blanc wines (S2.3 F2 / S2.5 F2 contamination pool) have wine_insights where Claude invents rationales for impossible grape data instead of flagging it. Waterbrook Icon Chardonnay hook: "blends 100% Chardonnay with 75% Pinot Blanc — an unusual high-proportion white blend" (175% total rationalized into narrative). Ceritas Porter-Bass: "blends Chardonnay with Pinot Blanc to chase salinity and tension over richness — a deliberate restraint that reads as intelligence rather than timidity" (pure invented backstory). 9.5% of the enriched corpus.
  - **F5** — Contamination feedback loop traced end-to-end. Edge function `assembleContext()` reads appellation_insights.ai_terroir / ai_climate / ai_style and region_insights.ai_terroir / ai_climate and injects them into Grade B wine prompts as "Appellation Context" / "Region Context" sections. Knights Valley appellation_insight (2026-03-06) confabulates "volcanic soils from ancient Mayacamas Mountain eruptions" → Beringer Alluvium Grade B wine_insight (2026-04-10) inherits claim and extends with DIFFERENT wrong volcano ("Mount St. Helena eruptions"). Beringer's own summary contradicts itself within 3 sentences. Parallel hits: RRV "volcanic ash from ancient eruptions" (false), Sonoma Coast "volcanic intrusions" (false), Howell Mountain "tufa and obsidian" (tufa is sedimentary limestone, Claude meant "tuff"). Sprint 5 regeneration order must be reference-first, wine-second.
  - **F6** — `grape_insights` table has 0 rows despite `grape_insights.py` containing the BEST food-pairing prompt in Loam. The food-pairing field spec is 130 words of VOICE.md-compliant structured guidance (classics first, name cuisines, full table range, flavor logic, banned patterns, no cop-outs) — never run. Frontend GrapePage has no narrative today.
  - **F7** — 5,003 of 5,062 Grade C wines (99%) have no food-pairing prose because `GRADE_C_FIELDS` schema in `enrich_prompts.py` explicitly drops the `food_pairing` slot. VOICE.md: "These rules apply everywhere food pairing content appears." 98.8% of enriched wines violate by omission.
  - **F8** — `wine_food_pairings` structured table is empty (0 rows). CLAUDE.md claim "809 structured links + 203 text descriptions from Empson" is stale — archive has 809, public has 0, 30K rebuild wiped it. Edge function `assembleContext` silently gets empty results for the "Existing Food Pairings" prompt context.
  - **F9** — 82/82 appellation_insights are US AVAs. Zero Chambertin, zero Barolo, zero Champagne, zero Rioja, zero Chablis, zero Burgundy grand crus or Bordeaux communes. Region_insights has some European coverage (21 IT, 19 FR, 15 ES, 13 DE) but no marquee appellation depth. Explains why DRC Corton-Charlemagne Grade B got no terroir context and fell back to Claude training knowledge for its confabulated MLF claim.
- **P1 (14):** F10 stale `claude-sonnet-4-20250514` hardcoded in 4 reference scripts + edge function (overlaps S2.5 F5), F11 reference prompts lack retrieval-grounded NEVER INVENT block, F12 "elegant/elegance" is default adjective across 15% of enriched corpus (37-50% of reference insights), F13 "marry/marries/marriage of" VOICE.md-banned phrase leaks through (enrich_prompts.py bans it but validator is warning-only), F14 "showcases" 52% Grade B vs 1% Grade C confirms edge function did not use enrich_prompts.py, F15 59% Grade B "likely" validates Session-10 feature flag, F16 "suggests" not on any banned list (4.5% of Grade C), F17 formulaic soil-profile templates (71/62/60% "well-draining/ancient/volcanic" tics), F18 "modern revival" trope on 27% of regions, F19 formulaic food pairing format (100% em-dash template, 71% "cuts through"), F20 confabulated-narrative response to bad facts not prevented by any rule — no "FACTS_PACKET_INCONSISTENT" escape hatch in the prompt, F21 volcanic narrative applied to ~29 wrong AVAs, F22 Grade C schema drops 5 of 8 narrative fields (no terroir/vinification/food/cellar), F23 hyperbolic marketing language unchecked at country level ("France: wine's eternal reference point", "Italy: ancestral homeland").
- **P2 (7):** F24 BANNED_WORDS validators are warning-only not rejecting, F25 DRC has 1/3 marquee wines enriched (Corton-Charlemagne with wrong MLF; Echezeaux + La Tâche NULL fields), F26 wine_vintage_tasting_insights sensory grid not audited structurally, F27 food-pairing palette only 3 rhetorical verbs, F28 corpus is two one-shot batches (2026-03-06 + 2026-04-10), no refresh discipline, F29 reference insights have no enrichment_log entries, F30 `country_insights.ai_regulatory_overview` is the only reference field that hits VOICE.md baseline (positive finding — preserve as template).
- **P3 (2):** F31 Grade C `style_profile` is the strongest fingerprint of enrich_prompts.py working (positive finding), F32 comparable_wines field sometimes invents producers in direct violation of COMPARABLES CRITICAL RULE (Domaine Huet attached to Morey-Saint-Denis; Scharffenberger described as same producer as Schramsberg).

### Key data-backed SQL verifications

1. **Enriched corpus timing:** wine_insights Grade B (46 rows) + Grade C (5,062 rows) ALL written 2026-04-10 (one-shot batch). region_insights (202) + appellation_insights (82) + country_insights (62) ALL written 2026-03-06 (separate one-shot batch). Corpus is two batches, no ongoing refresh.
2. **enrichment_log model breakdown:** claude-haiku-4-5-20251001 completed 5,067 Grade C, errored on 226; claude-sonnet-4-20250514 completed 105 Grade B, errored on 3. Grade C used tightened prompt with correct Haiku 4.5 model; Grade B used stale Sonnet 4 model via weak edge function prompt.
3. **Voice-violation LIKE scans:**
   - Grade B hedging: 27/46 "likely" (59%), 13/46 "typically" (28%), 12/46 "suggests" (26%), 7/46 "often" (15%)
   - Grade C hedging: 106/5062 "likely" (2.1%), 288/5062 "typically" (5.7%), 226/5062 "suggests" (4.5%)
   - Grade B sommelier theater: 18/46 "elegant" (39%), 7/46 "harmonious" (15%), 3/46 "marry/marriage" (7%)
   - Grade B generic filler: 24/46 "showcases" (52%), 19/46 "premium" (41%), 6/46 "legendary" (13%), 4/46 "remarkable" (9%)
   - Reference appellation soils: 58/82 "well-draining" (71%), 51/82 "ancient" (62%), 49/82 "volcanic" (60%), 16/82 "force vines/roots to struggle" (20%)
   - Regions: 74/202 "elegant" (37%), 55/202 revival tropes (27%), 60/202 "diurnal" (30%)
4. **Chardonnay+Pinot Blanc contaminated wine_insights:** 487 wines with both `display_name ILIKE '%chardonnay%'` and a PINOT BLANC link that also have wine_insights rows. 9.5% of enriched corpus carries rationalized invented narratives for the S2.3 F2 grape bug.
5. **Knights Valley feedback loop:** Queried appellation_insight (2026-03-06 "volcanic from Mayacamas eruptions") → Grade B wine_insight on Beringer Alluvium (2026-04-10 "volcanic from Mount St. Helena eruptions"). Contradiction confirmed at DB level.
6. **DRC coverage:** `SELECT ai_terroir_expression, ai_vinification_summary FROM wine_insights wi JOIN wines w ... WHERE w.display_name ILIKE '%romanée-conti%'` — only Corton-Charlemagne has populated narrative fields, Echezeaux Grand Cru + La Tâche Grand Cru both NULL.

### Meta-patterns surfaced (for S2.9 synthesis)

1. **Prompt drift is THE voice problem.** `enrich_prompts.py` is the only tightened voice source; 5 other prompt locations are strictly weaker. Consolidating to one shared `pipeline/lib/voice.py` module closes 14 of the 32 findings simultaneously. Highest-leverage S2.6 cleanup (~4-6 hours effort, Sprint 3 pre-req).

2. **Voice rules cannot prevent factual confabulation.** Tightening hedging reduced "likely" usage 10-30x Grade C vs Grade B, but receipts show Grade C still invents facts (175% blends, non-existent wines described, wrong grape characterizations). Retrieval-grounded facts packet + L3 fact-check gate (already scaffolded in `enrich_prompts.build_retry_prompt`) is the structural fix. Sprint 5 MUST make L3 non-optional.

3. **Contamination feedback loop is real and one-directional.** Reference insights contaminate wine prompts via `assembleContext`. Wine insights do NOT flow backward to reference insights. Sprint 5 regeneration order is locked: reference first, wine second.

4. **Reference corpus is US-biased AND structurally formulaic.** Appellation soil profiles use a 4-marker template ("ancient" + "well-draining" + "volcanic" + "force vines to struggle") that reads as specific but is filler at 60-71% rates. Plus: zero European marquee appellations in the corpus. Sprint 5 scope must add ~500 European appellation_insights.

5. **Grade C is voice-audited but food-blind.** 99% of enriched wines lack food-pairing output because `GRADE_C_FIELDS` schema is 3 fields. VOICE.md has a whole Food Pairings section with 6 structural rules; Loam deploys none of them on 99% of enriched wines. The grape_insights.py food-pairing prompt is the best in Loam AND is never run (F6).

### Sprint 3 sequence refined (voice items added)

Previous (post-S2.5): (a) S2.2 F1 staging relink → (b) S2.3 F3 producer seed → (c) refined grape-repair workstream (3a-3e + 3c.5-3c.7) → (d) F6 color+country repair → (e) F10 L3 re-fact-check → (f) content regeneration. Pre-Sprint-3 hygiene: describe-chemical delete + vendor enrich-wine + model IDs.

S2.6 additions:

- **Sprint 3 pre-req — voice module consolidation (F1, F2):** Create `pipeline/lib/voice.py` with shared VOICE_RULES_BLOCK (upgraded from `enrich_prompts.py:41-59` with F12/F13/F16/F23 additions) and NEVER INVENT block (extended with F11 reference-specific rules + F20 contradiction-escape-hatch). Rewrite 4 reference prompts + vendored edge function to import it. ~4-6 hrs. **Closes 14 of 32 S2.6 findings.** Must land before any regeneration.
- **Sprint 3 — restore wine_food_pairings from archive (F8):** Bulk UPDATE restoring 809 rows from `archive.wine_food_pairings` via the same normalized-key match as staging relink.
- **Sprint 5 prep — L3 fact-check gate (F3):** Re-scope the $18 S2.3 rolled-forward pre-auth from "re-fact-check existing rows" to "build L3 gate that blocks writes without fact-check". Estimated ~$40-80 at Sprint 5 regeneration scale, inside combined ceiling.
- **Sprint 5 — reference regen first, wine regen second (F5):** sequencing constraint, not additional scope.
- **Sprint 5 — widen GRADE_C_FIELDS (F7, F22):** add food_pairing + cellar_recommendation + shortened terroir_expression to Haiku schema. ~$20-30 retrofit cost.
- **Sprint 5 — port grape_insights.py food-pairing rules upstream (F19):** 30-min prompt edit. Single source of truth for all food-pairing surfaces.
- **Sprint 5 — expand reference coverage beyond US AVAs (F9):** ~500 new European appellation_insights + 150 new region_insights. Sonnet cost ~$15-20.

Total S2.6 findings blocking Sprint 3: **9 P0 + 14 P1 = 23 items**. Overlaps with S2.5 (F2 ↔ S2.5 F31 vendor; F10 ↔ S2.5 F5 models; F2 ↔ S2.5 F4 grape display_name) → net ~20 new items added to backlog.

### Scope-breaker check

None. All findings slot into the Sprint 3 pre-req + Sprint 5 regen envelope that was already planned. F5 sequencing constraint (reference first) was already implicit in the sprint model. F9 European coverage expansion is the only finding that materially grows Sprint 5 scope; cost growth is <$30 at Sonnet rates.

**The `ENRICHMENT_ENABLED=false` feature flag on the `enrich-wine` edge function must stay OFF through Sprint 3 and into Sprint 5.** Do not flip it until F1 (shared voice module) + F2 (edge function aligned) + F3 (L3 gate) + F4 (grape repair) all land. S2.6 evidence strongly validates the Session-10 decision.

### Deliverables

- `data/sprints/audit/findings/findings_voice.md` — 32-finding report (9 P0, 14 P1, 7 P2, 2 P3)
- `data/sprints/audit/prompts/s2_6_voice.md` — session prompt (written at session start for reproducibility)
- `data/sprints/audit/sessions.json` — S2.6 → done, $0 spend
- `data/sprints/audit/budget.json` — S2.6 entry, running total $0.00 / $25.00
- `data/sprints/audit/journal.md` — this section

---

## S2.7 — UX / Frontend Audit (2026-04-11)

**Expert hat:** UX / frontend — page types, data-to-UI integrity, empty-state handling, routing, a11y, mobile-first, Principle #9 compliance
**Model:** Opus 4.6 (1M context) — ratified inline pattern per DECISIONS.md 2026-04-11
**Budget:** $0 spent / $0 expected / $25 ceiling
**Duration:** ~2 hours
**Deliverable:** `data/sprints/audit/findings/findings_ux.md` (32 findings: 9 P0, 14 P1, 7 P2, 2 P3)

### Method

Read-only static analysis of the `frontend/` React app. No dev server runs, no screenshots, no browser automation. Scope: 9 consumer pages (`frontend/src/pages/consumer/`), 9 shared components (`frontend/src/components/`), 3 hooks, 14 dev-explorer pages (`frontend/src/pages/data/`), routing (`App.tsx`), layouts (`ConsumerLayout`, `DataLayout`, `DevLayout`), Supabase client (`lib/supabase.ts`). Cross-referenced against S2.1-S2.6 findings to quantify UI symptoms of data-layer bugs.

~12 verification SQL queries via Supabase MCP:
1. Core wine/producer/vineyard counts by data_grade (confirmed 155,623 active / 150,512 F / 104,727 NULL display_name)
2. `wines.name` vs `display_name` NULL distribution + sample (confirmed 12,083 with NULL name, 0 with null both, sample of 8 marquee Burgundy wines with NULL name)
3. Chardonnay + Pinot Blanc UI reach (confirmed 2,914 active wine pages)
4. Producer metadata coverage (confirmed 0/10,676 have hectares/production/address/coords/description/philosophy/year/parent/appellation, 1 has website, 1 has type)
5. F-grade appellation inflation (confirmed 95.7% F-grade for wines with appellation_id)
6. Producers with only F wines (confirmed 9,274/10,676 = 86.9%)
7. Duplicate wine names per producer (confirmed 5,573 rows in 2,404 groups across 1,145 producers)
8. `country_insights` column inventory + live PGRST 42703 error reproduction for CountryPage.tsx:40
9. Insight table row counts (region 202, appellation 82, country 62, grape 0, producer 0)
10. search_catalog RPC on 'vega sicilia' and 'romanee-conti' (confirmed S2.3 F1 at the UI layer: marquee wines unfindable or wrong-named in results)
11. Volcanic soil profile UI reach (confirmed 16,429 active wine pages render `appInsight.ai_soil_profile` containing 'volcanic')
12. Confabulated wine_insights on Chardonnay+Pinot Blanc pool (confirmed 493, matches S2.6 F4 ± 6)

Zero API calls to Haiku/Sonnet. Zero DB writes. Read-only discipline held.

### Headline findings

**F1 — 12,083 wine pages render empty `<h1></h1>`.** `WinePage.tsx:175` fetches `name` not `display_name`, and 7.8% of active wines have `name IS NULL`. Live samples include Ropiteau Pommard Premier Cru, Mommessin Châteauneuf-du-Pape, Ligeret Chambertin-Clos de Bèze Grand Cru — marquee Burgundy appellations rendering blank titles. 5-minute fix.

**F2 — CountryPage is silently broken on 100% of country pages.** Line 40 selects `ai_signature_grapes` which does not exist; the actual column is `ai_signature_styles` (plural). Verified by running the exact query via MCP and seeing `ERROR: 42703: column "ai_signature_grapes" does not exist`. Because there's no `.catch()` anywhere in consumer pages (F9), the failure is silent — every country page visit fetches nothing, renders no overview, even though 62 `country_insights` rows exist. Additionally, `ai_wine_history`, `ai_key_regions`, `ai_regulatory_overview` (S2.6 F30's VOICE.md-compliant field) are not fetched at all; only `ai_overview` is rendered out of 5 available fields. 1-minute typo fix + 10-minute render expansion.

**F3 — 2,914 wine pages render Chardonnay + Pinot Blanc grape chip bug.** Live-verified UI manifestation of S2.3 F2 / S2.5 F2. 493 of those also render confabulated `wine_insights.ai_hook` ("blends 100% Chardonnay with 75% Pinot Blanc" — per S2.6 F4). The consumer WinePage correctly reads `grapes.display_name` so the UI layer is faithfully rendering wrong data. Fix is in Sprint 3 data layer (grape-repair workstream) + stale content gating in Sprint 3 + regeneration in Sprint 5.

**F4 — ProducerPage is structurally empty across 100% of producers.** Verified by direct SQL: 0 producers have `hectares_under_vine`, `total_production_cases`, `address`, `latitude`, `description`, `philosophy`, `year_established`, `parent_producer_id`, `parent_company`, or `appellation_id`. 1 has `website_url`. 1 has `producer_type`. The Section "Details" header renders above an empty FactGrid on every producer page. The Philosophy section and Estates & Labels section NEVER render anywhere in the app (dead JSX). S2.3 F3 flagged 15 marquee producers — the actual scope is **every producer**, all 10,676 of them. Sprint 3 needs a broader producer metadata strategy, not a 15-producer seed.

**F5 — AI content rendered as plain text with zero confidence / disclaimer / source attribution.** `ConfidenceBadge.tsx` exists and is wired into `InsightsPanel.tsx` — but only used by the dev `/data/*` explorer, not the consumer pages. Confirmed via grep: consumer pages have no imports of ConfidenceBadge or InsightsPanel. The `confidence` column exists on all 5 insight tables and is fetched by zero consumer pages. No "AI-generated" badge, no enriched_at timestamp, no "Some content on this page is AI-generated" disclaimer. Users have no way to tell that "volcanic soils from ancient Mayacamas eruptions" is Claude-invented.

**F6 — 16,429 active wine pages render contaminated volcanic soil claims at the wine level.** `WinePage.tsx:551-555` renders `appInsight.ai_soil_profile` under MiniLabel "Soil" as if about the wine's own vineyard. Live query: 16,429 active wines are linked to an appellation whose `ai_soil_profile` contains "volcanic", 49 appellations affected. Per S2.6 F5, ~14 of those appellations have **false** volcanic claims (Knights Valley, RRV, Sonoma Coast, Howell Mountain, Hunter Valley, etc.). Users read actively wrong geology on those wine pages, with no attribution saying "from the appellation profile."

**F7 — Footer About link is broken.** `ConsumerLayout.tsx:84` links to `/about` but App.tsx has no `/about` consumer route; only `/dev/about` exists. Clicking goes to a blank screen (compounded by F10: no 404 catch-all). 1-minute fix.

**F8 — `/vineyard/:id` route is dead.** `vineyards` table is 0 rows, `search_catalog` RPC doesn't include vineyard entity type, no page navigates to `/vineyard/:id`. 232 LOC `VineyardPage.tsx` is unreachable. Park for Sprint 4.

**F9 — Zero error handling in consumer pages.** Grep: no `.catch()` calls in `frontend/src/pages/consumer/`. No error boundary in `main.tsx` wrapping `<App/>`. No try/catch. The `useEntityDetail.ts` hook handles errors correctly but is ONLY used by the dev explorer. Every Supabase failure (network, RLS block, bad query like F2) fails silent — the page just doesn't render those sections. This is the structural reason F2 silently shipped.

### Key P1s

- **F10** — no 404 catch-all route in App.tsx
- **F11** — Dashboard stats list `producer_insights` which has 0 rows (table exists but never populated)
- **F12** — wineCount displayed across 5 page types is inflated by F-grade empty shells (96.7% of appellation wines are F; HomePage also missed the `deleted_at` filter)
- **F13** — 5,573 wines in 2,404 duplicate-name groups on 1,145 producer pages; users can't distinguish them
- **F14** — Producer website URL is non-clickable plain text on consumer pages (WinePage + ProducerPage); the dev ProducerDetail.tsx:83 does it correctly with `target="_blank" rel="noopener noreferrer"`
- **F15** — dev WineDetail.tsx:37 reads `grapes.name` not `display_name` (S2.5 F4 extension to dev explorer)
- **F16-F19** — **8 `ai_*` fields fetched by consumer pages and never rendered.** AppellationPage drops `ai_overview`, `ai_key_grapes`, `ai_notable_producers_summary` (3 of 7). CountryPage drops `ai_wine_history`, `ai_key_regions` (plus F2 typo, net 2 of 5 real fields rendered). RegionPage drops `ai_overview` (1 of 5). GrapePage drops `ai_overview`, `ai_regions_of_note` (2 of 6, though moot while `grape_insights` is 0 rows). If Sprint 5 generates perfect ai_overview for Chambertin appellation, the current code will fetch-and-discard it.
- **F20** — zero aria-current, aria-live, aria-labelledby, htmlFor, or role attributes anywhere in consumer pages. Only 3 aria-label total (all on mobile hamburger toggles). WCAG 1.3.1, 2.4.6, 4.1.3 violations.
- **F21** — heading hierarchy skips h1 → h3 in every detail page (WinePage/ProducerPage/Appellation/Region/Country/Grape/Vineyard). No h2s. WCAG 1.3.1 violation.
- **F22** — WinePage food pairing section invisible on 99% of Grade C wines (`GRADE_C_FIELDS` drops the field per S2.6 F7)
- **F23** — classification `system_name` captured into state at `WinePage.tsx:258-266` but only `level_name` rendered. User sees "Premier Cru" with no "Saint-Émilion 1855" or "Burgundy 1er Cru" context

### P2 + P3 summary

- **F24** — empty Section headers visible when FactGrid filters all children to null (the rendering bug behind F4's visible impact)
- **F25** — EntityMap boundary_source rendered raw ("ldproxy_rlp", "uc_davis_ava")
- **F26** — VineyardPage map shows appellation instead of vineyard GPS point (N/A until vineyards exist)
- **F27** — ~500 LOC of Section/Tag/Fact/FactGrid/Loading/NotFound/MiniLabel duplication across 8 consumer pages, with inconsistent variant props
- **F28** — Dashboard fires N+1 count queries uncached on every load
- **F29** — HomePage `autoFocus` triggers mobile keyboard popup on load, causing layout shift
- **F30** — GrapePage wineCount inflated by Pinot Blanc contamination bug
- **F31** — `LandingPage.tsx` is dead code (exists in src, not routed)
- **F32** — build timestamp shown only on dev layouts, not ConsumerLayout footer

### Sprint 3 impact

**Pre-req UI hygiene bundle (~3-4 hours total):** F1 (5 min), F2 (1 min + 10 min render expansion), F7 (1 min), F8 (park), F9 (2 hours for error boundary + .catch), F10 (5 min), F11 (1 min), F14 (5 min), F15 (1 min), F16-F19 (30 min to render dead fetches), F20 (2 hours a11y baseline), F21 (5 min single-file change once F27 lands), F22 (1 hour wire structured wine_food_pairings path), F23 (5 min), F24 (15 min Section fix), **F27 (half day — consolidate shared consumer components; optional but 8x multiplier on all other UI fixes)**.

**Data-dependent:** F3 (Chardonnay+Pinot Blanc data fix), F4 (producer metadata strategy call), F6 (contaminated appellation_insights cleanup or wait for Sprint 5 regen), F12 (honest display now), F13 (fuzzy merge pass).

**Sprint 5 constraints locked:** AI disclaimer + confidence badge must ship before `ENRICHMENT_ENABLED` flag flips (F5); every new `ai_*` field in Sprint 5 prompts must have matching consumer render (F16-F19 lesson); reference-first regeneration confirmed at the UI layer (F6 quantifies 16,429 wine-page reach).

### Cross-references

- S2.1 F28 → F12 (UI extension of count drift)
- S2.3 F1 → F1 (empty h1), also verified by search_catalog reproducing marquee breakage at UI
- S2.3 F2 / S2.5 F2 → F3 (Chardonnay+Pinot Blanc quantified as 2,914 UI pages)
- S2.3 F3 → F4 (producer metadata scope corrected from 15 to 10,676)
- S2.4 F10 → F23 (classification German einzellage)
- S2.5 F4 → F15 (dev WineDetail uses grapes.name)
- S2.5 F18 → F1 (inverse NULL display_name / name scope)
- S2.6 F3/F4 → F3 + F5 (confabulation renders; no disclaimer)
- S2.6 F5 → F6 (16,429 contaminated wine-page soil renders)
- S2.6 F7 → F22 (food pairing absent on 99% of C)
- S2.6 F8 → F22 (wine_food_pairings structured table empty; no UI render path)
- S2.6 F9 → F16 (US-only appellation_insights compounded with dead-fetch ai_overview)

Total S2.7 findings blocking Sprint 3: **9 P0 + 14 P1 = 23 items**. Overlaps with prior sessions on F3/F6/F15 — net ~20 new items added to backlog.

### Scope-breaker check

None. S2.7 findings are all fit-and-finish UI fixes or mandate confirmations of existing Sprint 3/5 plan. The only shift: Sprint 3 grows a "UI hygiene" pre-req bundle (~3-4 hours) before the data work begins, because F9 error handling is foundational for Sprint 3 to be able to trust its own fixes.

**F2 is the session's single cheapest high-impact finding** — 1-character typo that invalidates 100% of country pages. It's been silently shipping because of F9. Both must land before any other UI work can be relied on.

### Deliverables

- `data/sprints/audit/findings/findings_ux.md` — 32-finding report (9 P0, 14 P1, 7 P2, 2 P3)
- `data/sprints/audit/prompts/s2_7_ux.md` — session prompt (written at session start for reproducibility)
- `data/sprints/audit/sessions.json` — S2.7 → done, $0 spend
- `data/sprints/audit/budget.json` — S2.7 entry, running total $0.00 / $25.00
- `data/sprints/audit/journal.md` — this section

---

## S2.8 — 2026-04-11

**Expert:** meta
**Status:** done
**Budget:** $0.00 (Opus inline per ratified S2.3–S2.7 pattern; no Haiku/Sonnet API calls)

### Scope

The meta layer is what every other audit stands on. S2.8 audited:

- All of `docs/*.md` (14 root files + 2 in `docs/reference/`)
- Empty scaffolded dirs `docs/architecture/`, `docs/pipelines/`
- `CLAUDE.md` (611 lines)
- Every file under `memory/` (19 files including MEMORY.md index)
- `data/stats/loam_roadmap.json` + `pipeline/analyze/loam_roadmap.py`
- `data/sprints/current.json` + all of `data/sprints/audit/`
- `data/sprints/_archive/30k/` and its journal.md pointer state
- `data/sessions.md` (whiteboard format + ballooning check)
- `data/stats/` contents (~55 files)
- `data/session_prompts/` legacy dir
- `scripts/dash.ps1` (cross-check against sprint state)
- Scheduled task state via `mcp__scheduled-tasks__list_scheduled_tasks`
- Edge function deployment state via `mcp__c4a52b5c-67f7-4804-8f3b-e9e5c906b1fd__list_edge_functions`
- Live DB drift verification via ~15 `execute_sql` calls (hardcoded counts, table row counts, information_schema columns, external_ids system distribution)

### Method

Opus 4.6 inline + Supabase MCP `execute_sql` + `list_scheduled_tasks` + `list_edge_functions`. Read-only throughout — no DDL, no DML, no pipeline runs, no file writes other than the findings file and sprint-state updates. Every claim in the findings has either a `file:line` cross-ref or an MCP query result as paired evidence.

### 32 findings written to `findings/findings_meta.md`: 9 P0, 14 P1, 7 P2, 2 P3

**Headline P0s (4 of 9):**

1. **F1 — CLAUDE.md `## Current Focus` is 5+ sessions stale and internally contradicts `## Current State`.** Line 520 still reads "Session 14 housekeeping interregnum … Sprint 2 = Reference-First Enrichment (planning session 15, execution sessions 16+)." Line 109–247 (`## Current State`) correctly tracks Sprint 2 as the Audit with S2.1–S2.7 done. Same file, opposite forward frames. A session briefing reading top-down gets the Reference-First frame; reading `## Current State` gets the Audit frame. The only reason this session isn't on the wrong foot is the user typed "execute s2.8" explicitly.

2. **F2 — `memory/30k_status.md:29` ships the same pre-pivot Reference-First claim into every conversation via `MEMORY.md`.** MEMORY.md auto-loads. The S14 Phase A prompt (s2_1_db_canonical.md A9) correctly rewrote `project_sprint_model_and_rf_direction.md` but missed this file. Fix is one paragraph rewrite.

3. **F3 — `docs/SOURCES.md:33` mis-documents `external_ids` Backbone ID storage.** Doc says `id_type = 'ttb_cola' | 'lwin' | 'upc'`. Verified via MCP `information_schema.columns`: the column is `system` (not `id_type`) and the COLA value is `cola` (not `ttb_cola`). Also `lwin_7` (50,908 rows, distinct from `lwin`) isn't mentioned. Any pipeline author writing a query from this doc as-written gets a column-does-not-exist error; a more charitable author writing `WHERE system = 'ttb_cola'` gets zero rows silently. Three errors in one sentence on the canonical source-of-truth doc. 2-minute fix.

4. **F4 — CLAUDE.md internal contradiction on vineyards count.** Line 334 (Aspirational) says `public.vineyards is empty post-rebuild` (verified 0 live). Line 487 (Major Gaps) says `vineyards has 815 rows` (false — 0 public, 881 archive). Three nested errors in one file: wrong public count, wrong archive count, and the contradiction between the two lines. Same class as F1 (self-contradicting file).

**5 more P0s (F5–F9):**

- F5 loam_roadmap.json Sprint 2 sub-tasks all 7 states behind (S2.1 still "in_progress", S2.2–S2.9 "planned"); `dash.ps1` reads live sessions.json so two-dashboards-two-answers drift
- F6 docs/30K_PLAN.md header status "Session 4 DONE — GO for Batch 1" (10 sessions out of date) + 10 broken `data/sprints/30k/` path references (dir moved to `_archive`)
- F7 docs/BACKLOG.md is orphan from CLAUDE.md Docs index yet hardcoded into `sprint_dashboard.py:49` as BACKLOG_PATH — active reader + zero doc awareness, plus 6 CLOSED items never pruned per its own workflow
- F8 docs/AUDIT_2026-04-01.md is a superseded audit still in docs/ root — its "canonical db is PERFECT, zero FK violations" contradicts S2.1's 34-finding audit of the same layer
- F9 docs/architecture/ and docs/pipelines/ are empty scaffolded dirs created 2026-03-05, never populated

**14 P1 findings (F10–F23):**

- F10 CLAUDE.md "Next Steps (cleaned 2026-04-03)" block is pre-30K and references 8 dead artifacts (Phase B wines that don't exist, 6,767-wine backlink count, etc.)
- F11 docs/MERGE_STRATEGY.md still frames Python migration as pending + plans Ollama local matching that never materialized + references 3 non-existent files (lib/merge.mjs, lib/import.mjs, sync_project_context.py "to build")
- F12 docs/ENRICHMENT.md contradicts S2.6/S2.7 — MVP "deployed" framing omits ENRICHMENT_ENABLED=false feature flag; Grade C Haiku batch documented as forward goal even though DECISIONS.md 2026-04-11 deprecated it; "All enrichment prompts must follow VOICE.md" is aspirational not enforced at the code layer
- F13 docs/SOURCES.md "Last updated: 2026-03-25" is 17 days stale (missing TABC refresh, knowledge seed pipeline, barcode scan completion)
- F14 docs/VOICE.md (2026-03-12) is older than the problem — S2.6 F3 proved voice rules don't prevent factual confabulation; doc needs a "Never Invent" section and L3 fact-check gate cross-ref
- F15 docs/PATH_A_ROLLBACK.md (719 lines unused rollback SQL) belongs in `docs/reference/`
- F16 docs/IDENTITY_RULES.md is a "Session 2 design spec" not listed in CLAUDE.md Docs index despite being core to Sprint 3's grape-repair compound
- F17 docs/DECISIONS.md is 1,559 lines / 271 entries append-only with no SUPERSEDED markers; Josh Test v1→v2→directional entries all live alongside each other with no archive strategy
- F18 data/sessions.md has session entries that are single paragraphs of 8,830 chars (largest line); 69 lines file but ~45K bytes; balloons on every audit session that writes its detail inline
- F19 memory/vivino-pipeline.md has no frontmatter (only memory file missing `---name/description/type` block)
- F20 memory/product-architecture.md uses "Tier 0-3" nomenclature superseded by F/D/C/B/A per ENRICHMENT.md
- F21 memory/workflow_session_tips.md has stale "CLAUDE.md Hygiene flagged 2026-04-03" (cleanup already happened in S14 Phase A) + "Next Steps IS the task queue" (pre-sprint-model)
- F22 memory/project_sprint_model_and_rf_direction.md filename still contains "rf_direction" even after content was rewritten to drop the RF-as-Sprint-2 claim — filename misleads
- F23 CLAUDE.md hardcoded numbers drift — wine_grapes 47,035 vs live 46,028, color 153,311 vs live 153,229, archive vineyards 815 vs live 881 (down from S2.1 F28 baseline of ≥6 but not eliminated)

**7 P2 findings (F24–F30):**

- F24 loam_roadmap.json has no METRIC_DISPATCH dispatcher for the Sprint 2 (audit) phase — no live metrics render under Phase 3
- F25 loam_roadmap.json Phase 10 lists 1 of 4 paused scheduled tasks (`data-accuracy-agent` mentioned; `loam-stats`, `loam-data-quality`, `nightly-schema-audit` invisible)
- F26 data/stats/ has 55 files including an ad-hoc Python script (`s23_build_sample.py` in a stats dir), stdout dumps from specific sessions, multi-version pass1/pass2 snapshots
- F27 data/session_prompts/ mixed live/dead state — `cron_loop_template.md` is actively referenced by CLAUDE.md:148; other 7 files are pre-sprint-model legacy
- F28 scripts/fetch_legal_sources_batch{2,3,4,5,8,10}.py gaps (no batch 1,6,7,9) suggest abandoned iteration
- F29 docs/HISTORY.md has no TOC for its 366 lines
- F30 CLAUDE.md "Reference Tables (complete)" heading is misleading given S2.4 found 30 reference-content issues including 8 P0 wrong-grape links

**2 P3 findings:**

- F31 data/stats/loam_roadmap.md is a git-tracked auto-generated dump that drifts when `--save` isn't run
- F32 MEMORY.md longest entry is 530 chars vs 150-char limit documented in CLAUDE.md memory instructions

### Cross-session meta-patterns for S2.9 synthesis

Five patterns worth escalating:

1. **Doc staleness is systematic, not ad-hoc.** S2.1 F28 + S2.7 F2 (dead column reference) + S2.8 F1/F3/F4 are all the same class. Common thread: nothing enforces doc freshness except manual discipline at session wrap-up. Sprint 3 should add a "drift check" step to the wrap-up checklist.

2. **The sprint-model pivot (DECISIONS.md 2026-04-11) was incompletely executed.** Three files reflect it correctly: CLAUDE.md `## Current State`, `memory/project_quality_before_enrichment.md`, `memory/project_sprint_model_and_rf_direction.md` (content). Three files still carry the pre-pivot frame: CLAUDE.md `## Current Focus`, `memory/30k_status.md`, `docs/ENRICHMENT.md` Grade C path. One file is ambiguous: `docs/30K_PLAN.md` header + broken paths. The fix was one-session-wide and missed half the surface area.

3. **docs/ root has 6 files that should move to `docs/reference/`.** AUDIT_2026-04-01.md, 30K_PLAN.md, PATH_A_ROLLBACK.md, IDENTITY_RULES.md, BACKLOG.md, MERGE_STRATEGY.md. Current `docs/reference/` has 2 files (LWIN_STRATEGY, SCHEMA_ASSESSMENT). The move pattern is established; several other candidates haven't been moved yet.

4. **Empty dirs and orphan scripts indicate scaffold intent that never landed.** `docs/architecture/`, `docs/pipelines/`, `scripts/fetch_legal_sources_batch*.py` gaps, `data/stats/s23_build_sample.py` in a stats directory. Sprint 3 structural cleanup pass.

5. **Two dashboards = two answers.** `loam_roadmap.py` reads `loam_roadmap.json` sub_tasks (stale); `dash.ps1` reads sprint dir directly (live). For phases that map to sprints, the roadmap should delegate phase-level state to the sprint dir.

### Cross-references added to Sprint 3 backlog

- S2.1 F28 → F23 (hardcoded count drift, compound with this session's finer-grained scope)
- S2.5 F1 (describe-chemical still deployed) → this session VERIFIED still deployed via `list_edge_functions` (version 5, ACTIVE, verify_jwt=false). Pre-Sprint-3 hygiene item not yet executed.
- S2.6 F1/F2 (prompt drift) → F12 (ENRICHMENT.md doc-level mirror of the same gap) + F14 (VOICE.md-level gap)
- S2.7 F2 (dead column reference in code) → F3 (dead column reference in doc) — same class at different layers

Total S2.8 findings blocking Sprint 3: **9 P0 + 14 P1 = 23 items**. No overlaps with prior sessions (meta layer is structurally distinct). Net 23 new items.

### Scope-breaker check

None. S2.8 findings are all doc-layer patches, memory edits, archive moves, and dir cleanups. All highly parallel to Sprint 3 data work. Sprint 3 gets a "doc hygiene" pre-req bundle (~2-3 hours) to run alongside S2.7's "UI hygiene" bundle (~3-4 hours) — combined ~5-6 hours before the data work begins.

**Cumulative Sprint 2 pre-req hygiene:** ~6 hours of doc + UI cleanup before any Sprint 3 execution can start. Still cheap relative to the cost of Sprint 3 inheriting the drift.

### Deliverables

- `data/sprints/audit/findings/findings_meta.md` — 32-finding report (9 P0, 14 P1, 7 P2, 2 P3)
- `data/sprints/audit/prompts/s2_8_meta.md` — session prompt (written at session start for reproducibility)
- `data/sprints/audit/sessions.json` — S2.8 → done, $0 spend
- `data/sprints/audit/budget.json` — S2.8 entry, running total $0.00 / $25.00
- `data/sprints/audit/journal.md` — this section
- `CLAUDE.md` — Current State updated with S2.8 done + 245 running totals
- `memory/project_sprint2_findings.md` — updated with S2.8 cross-references
- `data/sessions.md` — whiteboard entry added under Done

**Tables touched:** NONE (read-only audit; all reads via MCP and filesystem).

**Running Sprint 2 totals:** 245 findings across S2.1+S2.2+S2.3+S2.4+S2.5+S2.6+S2.7+S2.8, still **$0.00 / $25.00** ceiling. S2.9 (synthesis + Sprint 3 backlog) is the final planned session.

---

## S2.9 — 2026-04-11

**Expert:** business (capstone — positioning, monetization, ICP, competitive parity, moat, unit economics, risk)
**Status:** done
**Budget:** $0.00 (Opus 4.6 inline + 8 Supabase MCP verification queries, zero API calls)

### Scope

Two deliverables: (1) **findings_business.md** — a business-layer audit reading Loam as a go-to-market object rather than a technical artifact; (2) **synthesis.md** — the primary Sprint 2 deliverable, a deduped and prioritized Sprint 3 backlog drawn from all 9 audit sessions.

### Method

- Opus 4.6 inline reasoning (ratified pattern per DECISIONS.md 2026-04-11, S2.3-S2.8 precedent)
- 8 `execute_sql` verification queries against the live DB for business-relevant signals: `wine_lookups` (user telemetry), `enrichment_log` (cost-to-date + unit economics + error rate), actual price/score coverage via join (not staging counts), `archive.wine_vintage_prices` vs `public.wine_vintage_prices` delta (staging relink unlock potential), `accuracy_audit` (feedback loop state)
- `list_edge_functions` re-verification for the third time across Sprint 2 that `describe-chemical` is still deployed
- Grep across consumer frontend for monetization/pricing/billing terms (zero results)
- Competitive parity table built from training knowledge (explicit where specific; treated as point-in-time April 2026)
- Read-only — no DB writes, no prompt runs, no new scraping, no code changes
- No WebFetch — the business audit is entirely inline reasoning over prior findings + live DB queries + training knowledge

### Key live DB verifications

| Signal | Value | Implication |
|---|---|---|
| `public.wine_lookups` | **0 rows** | Zero user telemetry ever captured — "on-demand enrichment" has never fired |
| `public.enrichment_log` | 5,401 rows, $16.19 total | 5,067 Haiku Grade C ($14.85) + 105 Sonnet Grade B ($1.35) + 229 errors (4.24% rate) |
| Unit cost (Haiku Grade C) | **$0.00293/wine** | Full 155K Grade C coverage = ~$456 one-shot |
| Unit cost (Sonnet Grade B) | **$0.01286/wine** | Full 155K Grade B coverage = ~$2,003 one-shot |
| Live price coverage | **1.81%** (2,815 of 155,623 active) | CLAUDE.md claim of 5.21% is stale — 30K rebuild wiped the joins |
| Live score coverage | **1.30%** (2,029 of 155,623) | Not "2.24% of the corpus" as claimed |
| `archive.wine_vintage_prices` | **139,937 rows** | 116,717 prices waiting behind S2.2 F1 / S2.5 F3 relink |
| `archive.wine_vintage_scores` | **27,325 rows** | 21,905 scores waiting behind the same relink |
| `list_edge_functions` | `describe-chemical` ACTIVE v5 | S2.5 F1 → S2.8 → **S2.9 re-verified for 3rd time**, still not deleted |
| `public.wine_food_pairings` | **0 rows** | S2.6 F8 confirmed, `archive.wine_food_pairings` has 809 |
| `public.accuracy_audit` | 34 rows total | Feedback loop almost entirely unused |

### 30 business findings written to `findings/findings_business.md`

- **P0 (8):** F1 no monetization model exists anywhere in the architecture; F2 zero user wine lookups ever logged (instrumentation exists, never wired); F3 Loam's moat is INVERTED today — direct LLM queries return more factual answers than Loam's enriched pages on marquee wines; F4 ICP undefined (architecture serves "any of the above, partially"); F5 terroir positioning is 10x more polished than data supports (0 vineyards, 1/10,676 producers with website, 49/82 appellation_insights with volcanic confabulation); F6 `describe-chemical` still deployed (third audit cycle verification); F7 competitive parity table shows Loam losing on 6 columns and winning on 4 (weather + appellation rules + geographic boundaries + backbone IDs), wedge is narrow but defensible; F8 sprint sequence delays user-visible signal by 1-2 quarters without a parallel signal-collection track.
- **P1 (14):** F9 no feedback loop instrumentation; F10 no affiliate revenue architecture despite 82K+ prices + 14 retailers; F11 B2B API licensing unexplored (highest-LTV path ignored); F12 brand voice vs AI voice conflated (AI voice has factual errors, brand voice doesn't exist); F13 "Loam" and "wine intelligence" have trademark/SEO collisions (Wine Intelligence Ltd. since 2002); F14 no SEO strategy (empty h1 on 12K pages, country pages 100% broken, no sitemap.xml); F15 no press kit / media surface; F16 unit economics are fine — Sprint 5 at $620-700 total is negligible; F17 enrichment cost decoupled from revenue model (supply-side-only pipeline); F18 legal/licensing status of 32 scraped sources undefined; F19 no competitor pricing intelligence informs (nonexistent) pricing strategy; F20 "why now" thesis only partially valid post-audit — narrow niche is defensible, general thesis isn't.
- **P2 (6):** F21 PWA/mobile-first choice not research-backed; F22 no "first 100 users" plan; F23 pricing accuracy/staleness risk; F24 a11y/ADA compliance is legal risk; F25 no localization strategy (EN-US only caps market at 20% of wine buyers); F26 no "data freshness" communication on consumer pages.
- **P3 (2):** F27 Sprint 5 has no measurable done criterion; F28-F30 branding polish (loam = soil type unexplained, "wine intelligence platform" generic + collides, Wine.com/Total Wine partnerships unexplored).

### Cross-session dedupe — 9 compound groups

The raw P0+P1 count across S2.1-S2.8 is ~170 findings; after dedupe into compound tracks, **net Sprint 3 scope is ~38 items organized into 9 tracks**:

1. **Chardonnay/Pinot Blanc contamination** — 12 findings across 5 experts (S2.3 F2 + S2.4 F1/F2 + S2.5 F2/F4/F11/F17/F18 + S2.6 F4 + S2.7 F3/F15/F30) collapse into ONE compound grape repair (Sprint 3 Track 3).
2. **Staging archive relink** — 4 findings (S2.1 F6 + S2.2 F1 + S2.5 F3 + S2.9 F3) collapse into ONE data unlock (Sprint 3 Track 2).
3. **Volcanic soil confabulation** — 4 findings (S2.3 F14 + S2.4 F18 + S2.6 F5 + S2.7 F6) collapse into Sprint 5 reference regen (flagged, not fixed in Sprint 3).
4. **Producer metadata corpus-wide** — 3 findings (S2.1 F4 + S2.3 F3 + S2.7 F4 + S2.9 F5) collapse into Sprint 3 Track 4.
5. **Edge function + voice module hygiene** — 7 findings (S2.5 F1/F4/F5/F31 + S2.6 F1/F2 + S2.9 F6) collapse into Sprint 3 Track 1 (highest priority).
6. **Food pairings** — 4 findings (S2.6 F6/F7/F8 + S2.7 F22) collapse into Sprint 3 Track 8.
7. **Doc drift / meta hygiene** — 23 findings from S2.8 + S2.1 F28 collapse into Sprint 3 Track 0A.
8. **UI hygiene bundle** — 15 findings from S2.7 collapse into Sprint 3 Track 0B.
9. **AI safety rail / brand voice** — 4 findings (S2.7 F5 + S2.9 F12/F15/F26) collapse into Sprint 3 Track 6.

### Sprint 3 backlog (synthesis.md is authoritative)

**10 recommended sessions, $50-200 estimated spend:**

- **S3.1** — Code/voice hygiene (Track 1 + Track 0A partial) — delete `describe-chemical`, vendor `enrich-wine`, create `pipeline/lib/voice.py` + `pipeline/lib/models.py`, rewrite 4 reference enrichment scripts
- **S3.2** — Doc hygiene finish + UI hygiene P0s (Track 0A + Track 0B P0s)
- **S3.3** — Staging archive relink (Track 2) — price coverage 1.81% → ~35%
- **S3.4 + S3.5** — Grape repair compound (Track 3) — drop Chardonnay+Pinot Blanc false positives from 2,743 to ≤ 50
- **S3.6** — UI hygiene P1s + AI safety rail (Track 0B P1s + Track 6)
- **S3.7** — Producer metadata strategy + seed run (Track 4) — 300+ producers with website + year + coords
- **S3.8** — L3 fact-check gate build (Track 7) — rescopes the $18 S2.3 pre-auth into gate infrastructure
- **S3.9** — Signal collection (Track 5) + food pairings restore (Track 8)
- **S3.10** — Sprint 3 exit review + Sprint 4 scoping

### Sprint 3 done criteria (12)

Sprint 3 closes only when ALL 12 are measurably true, including: `describe-chemical` deleted, `enrich-wine` vendored with voice module, Chardonnay/Pinot Blanc false-positive count ≤ 50 (down from 2,743), price coverage > 12.8%, UI P0s shipped, ≥300 producers with metadata, L3 fact-check gate calibrated, `<AIBadge>` shipped, doc hygiene closed, `wine_lookups` has ≥ 10 rows from real sessions, `docs/MONETIZATION.md` exists, `ENRICHMENT_ENABLED` feature flag still OFF.

### Key deferrals (explicit, documented)

- **Sprint 4:** `appellation_rules` JSONB redesign, `appellation_grapes`/`appellation_soils` provenance, French AOC diacritics, 1855 classification fix, slash-concatenated aliases, grapes.name VIVC cleanup, retailer affiliate architecture, pricing freshness, SEO hygiene
- **Sprint 5:** reference regen (49 contaminated appellation_insights), European coverage (~500 new rows), wine regen (Grade B on top 10K + Grade C on full corpus), L3 gate application to existing 5,108 wine_insights rows
- **Sprint 6+:** B2B API, affiliate revenue, partnerships, monetization execution, localization, mobile app, content marketing

### Meta-patterns

1. **Correctness-constrained, not cost-constrained.** F16 evidence removes the cost anxiety. Sprint 5's full coverage is $620-700 total. The real constraint is fact-check quality, voice consolidation, grape repair.
2. **Demand signal before enrichment signal.** F2's zero wine_lookups is the most under-weighted audit finding. Sprint 3 wires instrumentation + signal collection in parallel with the technical work.
3. **Wedge is narrow but defensible.** F7 + F20 combined: "wine intelligence platform" is too broad to win against Vivino/Wine-Searcher/LLMs; "terroir-grade wine data for trade/sommelier audience" is narrow enough to win because of the 4 star columns Loam holds (weather + appellation rules + geographic boundaries + backbone IDs).
4. **Three recurring re-verifications across Sprint 2.** (a) `describe-chemical` still deployed (S2.5/S2.8/S2.9 — 3 cycles); (b) CLAUDE.md doc drift (S2.1/S2.7/S2.8); (c) Chardonnay/Pinot Blanc contamination (S2.3/S2.4/S2.5/S2.6/S2.7). Sprint 3 should close all three in Session 1-2.
5. **Sprint 3 is 80% dedupe, 20% new work.** 170 raw findings → 38 net items → 9 tracks → 10 sessions. The synthesis value is in the dedupe, not in copying P0s forward.
6. **Business findings reprioritize some technical P0s.** Empty h1 on 12K pages is P0 by severity but has 0 traffic. The sommelier-demo critical path (delete describe-chemical + staging relink + grape repair + producer metadata + voice consolidation) is the actual Sprint 3 critical path.
7. **Sprint 5 is not "run enrichment";** Sprint 5 is "validate the quality gate by running enrichment through it on a bounded sample." Sprint 3 builds the gate.
8. **Every sprint exits with a measurable done criterion.** Sprint 2 did (245 findings → synthesis.md). Sprint 3+ inherits the pattern.

### Scope-breaker check

**One soft reframing, no hard break.** The technical audit's implicit "fix every P0/P1" framing would take 20+ sessions before any user-visible value lands. Sprint 3 should be scoped around "unblock Sprint 5 + unlock staging depth + restore first-impression credibility on the ~500 wines a sommelier demo would hit" — not around "close every P0 by raw count." This is operationalized in synthesis.md's Tier 1-6 structure. No escalation to user required; recalibration clause applied.

### Deliverables

- `data/sprints/audit/findings/findings_business.md` — 30-finding business audit (8 P0, 14 P1, 6 P2, 2 P3)
- `data/sprints/audit/findings/synthesis.md` — **Sprint 2 primary deliverable**: deduped Sprint 3 backlog with 9 tracks, 10-session sequence, 12 done criteria, explicit deferrals to Sprint 4/5/6+, risk tracking
- `data/sprints/audit/prompts/s2_9_business_synthesis.md` — session prompt
- `data/sprints/audit/sessions.json` — S2.9 → done, $0 spend, Sprint 2 complete
- `data/sprints/audit/budget.json` — S2.9 entry, Sprint 2 closed at $0.00 / $25.00
- `data/sprints/audit/journal.md` — this section + Sprint 2 closing summary below
- `data/sprints/current.json` — Sprint 2 closed, Sprint 3 (execute) next
- `CLAUDE.md` — Current State updated with Sprint 2 closed + pointer to synthesis.md
- `memory/project_sprint2_findings.md` — updated with S2.9 summary + synthesis.md pointer
- `data/sessions.md` — whiteboard entry moved to Done

**Tables touched:** NONE (read-only audit; all writes are to sprint infra + docs + memory, no DB/schema changes).

---

## Sprint 2 — Closed 2026-04-11

**9 sessions, 275 findings, $0.00 actual spend against $25.00 ceiling.**

| Session | Expert | Findings (P0/P1/P2/P3) | Headline |
|---|---|---|---|
| S2.1 | db_canonical | 34 (6/12/11/5) | 80%+ depth lost in 30K rebuild; search_vector missing on 67% wines; 919 grape synonym collisions |
| S2.2 | db_staging | 31 (6/11/9/5) | 286,918 dangling wine_id pointers (29 of 31 tables); ~52K prices locked behind relink |
| S2.3 | wine_canonical | 22 (9/8/4/1) | 8/10 marquee wines broken; Chardonnay/Pinot Blanc bug systemic; 15 famous producers 0 metadata |
| S2.4 | wine_reference | 30 (8/14/7/1) | Root cause of C/PB bug (PINOT BLANC synonyms); 5+ wrong grape links in varietal_categories |
| S2.5 | code | 32 (9/14/7/2) | describe-chemical rogue deploy; batch_pipeline multi-COLA collapse; 4+ duplicate grape resolvers |
| S2.6 | voice | 32 (9/14/7/2) | Prompt drift end-to-end; contamination feedback loop reference→wine; 5/5 Grade C hooks have invented facts |
| S2.7 | ux | 32 (9/14/7/2) | CountryPage 100% broken via typo; 12K empty h1s; 8 ai_* fields fetched and never rendered |
| S2.8 | meta | 32 (9/14/7/2) | CLAUDE.md internal contradictions; sprint pivot incompletely executed; doc drift systematic |
| **S2.9** | **business** | **30 (8/14/6/2)** | **No monetization; 0 user lookups; moat inverted vs LLMs today; wedge narrow but defensible** |

**Critical Sprint 3 pre-reqs identified (addressed in synthesis.md Tier 1):**
- Code/voice hygiene bundle (Track 1) — closes 21 findings across S2.5/S2.6/S2.9 in 1-2 sessions
- Staging archive relink (Track 2) — unlocks ~116K prices + ~22K scores from archive tables
- Grape repair compound (Track 3) — closes Chardonnay/Pinot Blanc contamination at all 5 layers
- Doc hygiene (Track 0A) + UI hygiene (Track 0B) — closes drift surface area blocking every future session briefing

**Sprint 2 structural observations for the sprint sequence:**
- **Opus inline reasoning pattern ratified across 9 sessions at $0.** Every session that pre-authorized Haiku/Sonnet budget ended up not needing it; the $18 S2.3 pre-auth rolls forward to Sprint 3 Track 7 (L3 fact-check gate build), rescoped from "re-fact-check existing prose" to "build the gate that blocks writes without fact-check."
- **Every session stayed read-only.** No DB writes, no DDL, no pipeline runs, no prompt executions. The audit discipline held for 9 consecutive sessions.
- **Three findings were re-verified across sessions** (describe-chemical deployment, CLAUDE.md drift, Chardonnay/Pinot Blanc) — Sprint 3 opens with these as the first-minute wins.
- **Sprint 3 scope is ~8-12 sessions + $50-200 actual budget.** Combined Sprint 2+3 ceiling should be extended from $50 to $250 at Sprint 3 mid-point if Track 4 (producer metadata) runs expensive; Sprint 5 enrichment budget is separate and sized around $500-1,500.
- **`ENRICHMENT_ENABLED=false` feature flag stays OFF** through Sprint 3 and into Sprint 5 until voice + L3 gate + grape repair + AI-disclaimer UI all land. This is a cross-session locked decision from S2.5/S2.6/S2.7/S2.9.

**Sprint 2 exits clean.** No outstanding blockers, no escalations, no structural pivots. Sprint 3 opens whenever the user is ready; synthesis.md is the authoritative starting point.


