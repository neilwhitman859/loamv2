# Sprint 3 Backlog — Sprint 2 Synthesis

**Authored:** 2026-04-11 (S2.9)
**Author:** Opus 4.6 inline, capstone session
**Source findings:** 275 total across 9 sessions (S2.1 DB canonical, S2.2 DB staging, S2.3 wine canonical, S2.4 wine reference, S2.5 code, S2.6 voice, S2.7 UX, S2.8 meta, S2.9 business)
**Sprint 2 budget:** $0.00 / $25.00 (closed, well under ceiling)

This is Sprint 2's primary deliverable: a deduped, prioritized, business-informed backlog Sprint 3 can execute from. It replaces "scan the 9 findings files and pick P0s" with "execute the tiered list below, in order, with explicit dependencies and a measurable done criterion."

**Sprint 3 thesis** (from S2.9 F3 + F7 + F16):
> Sprint 3 unblocks Sprint 5 by (1) eliminating the contamination vectors that would re-infect regenerated content, (2) unlocking the staging-locked data depth that's waiting in archive tables, and (3) repairing first-impression credibility on the ~500 wines a sommelier demo is likeliest to hit. Sprint 3 is not "close every P0 by raw count" — it's "make Sprint 5 worth running."

---

## Cross-session dedupe — compound bugs and overlapping findings

Before listing the backlog, this table collapses findings that appear across multiple experts into single compound fixes. Raw P0+P1 across 9 sessions ≈ **170+ items**; net Sprint 3 items after dedupe ≈ **38 items** organized into **9 tracks**.

### Compound #1 — Chardonnay/Pinot Blanc grape contamination (5 experts, 10+ findings)

| Layer | Finding | Essence |
|---|---|---|
| Data (reference) | **S2.4 F2** | PINOT BLANC grape has VIVC synonyms `PINOT CHARDONNAY`, `CHARDONNET PINOT BLANC`, `PINOT BLANC CHARDONNET`, `PINOT GRIGIO` |
| Data (canonical) | **S2.3 F2** | 2,743 of 2,809 Chardonnay-named wines (97.6%) have Pinot Blanc linked |
| Data (reference) | **S2.4 F1** | `varietal_categories` has 5+ wrong-grape links (Merlot→Grolleau Noir, Riesling→Crouchen, Verdejo→Trousseau Noir, Greco→Albana Bianca, St. Laurent→Muscat St. Laurent) |
| Code (upstream dedup) | **S2.5 F2** | `batch_pipeline._match_ttb_to_wine` collapses 4 grape-specific COLAs onto 1 canonical wine for ~2,700 wines |
| Code (promotion picker) | **S2.5 F17** | `ttb_grape_promote` `DISTINCT ON (canonical_wine_id) ORDER BY ttb_id` picks arbitrary TTB row per wine |
| Code (resolver duplication) | **S2.5 F11** | 4+ duplicate grape lookup implementations across `batch_pipeline`, `haiku_grape_extract`, `grape_from_name` |
| Code (identity) | **S2.5 F4** | `enrich-wine` edge function reads `grapes.name` (VIVC form) not `grapes.display_name` (common English) |
| Code (identity) | **S2.5 F18** | `lwin_long_tail.py` inserts 50,908 wines without `display_name` |
| Content (AI confabulation) | **S2.6 F4** | 487 Chardonnay+Pinot Blanc wines have Claude-invented rationales in wine_insights |
| UI (user-visible) | **S2.7 F3** | 2,914 live wine pages render the Chardonnay+Pinot Blanc chip combination |
| UI (dev explorer) | **S2.7 F15** | dev WineDetail.tsx uses `grapes.name` not `display_name` |
| UI (grape wineCount) | **S2.7 F30** | GrapePage wineCount inflated by the Pinot Blanc bug |

**Net:** **ONE compound repair**, Sprint 3 Track 3 below. The fix sequence is: (a) delete PINOT BLANC's 4 polluting synonyms; (b) fix `varietal_categories` wrong links; (c) consolidate grape resolvers on `ReferenceResolver`; (d) fix `_match_ttb_to_wine` multi-COLA collapse; (e) fix `ttb_grape_promote` DISTINCT ON; (f) fix `enrich-wine` edge function to read `display_name`; (g) re-run wine_grapes resolver on affected wines; (h) fix `lwin_long_tail` display_name backfill.

### Compound #2 — Staging archive relink (data + code owner)

| Layer | Finding | Essence |
|---|---|---|
| Data | **S2.2 F1** | 286,918 wine_id pointers across 29 of 31 wine-bearing staging tables are dangling archive references |
| Code | **S2.5 F3** | `relink_staging_to_current.py::STAGING_TABLES_WINE` lists only 1 of 30 staging tables — unresolved TODO comment |
| Data | **S2.1 F6** | depth loss isn't "promotion never ran"; it's "promotion ran, then wine_ids went dead in 30K rebuild" |
| Business | **S2.9 F3** (headline) | Live: 2,815 wines have prices (1.81%) vs archive.wine_vintage_prices 139,937 rows. Relink potential unlock: ~35% price coverage. |

**Net:** **ONE compound fix, Sprint 3 Track 2 below**. Extend STAGING_TABLES_WINE list from 1 to 30 entries, mirror the existing producer-mapping pattern for wines, use `archive.wines → public.wines` join via LWIN/display_name/producer_name. ~4-6 hours total.

### Compound #3 — Volcanic soil confabulation (4 experts, cascading into 16K UI reach)

| Layer | Finding | Essence |
|---|---|---|
| Data (reference) | **S2.4 F18** | Hunter Valley linked to Basalt in `appellation_soils`; 60% of appellations contain "volcanic" in ai_soil_profile |
| Content (reference) | **S2.6 F5** | Knights Valley / RRV / Sonoma Coast / Howell Mountain confabulate volcanic origin stories (primary-source false) |
| Content (reference→wine) | **S2.6 F5** (same F) | Edge function `assembleContext` injects contaminated appellation insight into Grade B wine prompts → inherited and extended with different wrong volcanoes (Beringer Alluvium adds "Mount St. Helena eruptions") |
| Data (canonical) | **S2.3 F14** | Hunter Valley "volcanic" + Santa Ynez "Franciscan shale" confabulation in wine-level AI content |
| UI (user-visible) | **S2.7 F6** | 16,429 active wine pages render contaminated "volcanic" soil claim at the wine level under MiniLabel "Soil" |

**Net:** **ONE compound fix, partly Sprint 3 and partly Sprint 5**. Sprint 3 = (a) audit the 49 volcanic-tagged appellations against primary sources, mark the ~30 confabulated rows as `fact_check_status='failed'`, (b) build the L3 fact-check gate so Sprint 5 regen can't re-ship the same errors. Sprint 5 = regenerate the flagged rows reference-first, then allow wine-level regen. DO NOT attempt to fix the wine-level content in Sprint 3 — that's Sprint 5 scope.

### Compound #4 — Producer metadata (corpus-wide, not marquee)

| Layer | Finding | Essence |
|---|---|---|
| Data | **S2.3 F3** | All 15 marquee producers (DRC, Lafite, Latour, Margaux, Haut-Brion, Pétrus, Gaja, etc.) have zero metadata |
| UI / Data | **S2.7 F4** | **Corpus-wide**: 0/10,676 producers have hectares/production/address/coords/description/philosophy/year; 1 has website; 1 has producer_type. Dead sections: Philosophy + Estates & Labels NEVER render |
| Data | **S2.1 F4** | 1 website out of 10,676 producers (the count baseline S2.7 extended) |
| Business | **S2.9 F5** | "Terroir" positioning is 10x more polished than data supports; first-impression credibility is actively damaged |

**Net:** **ONE compound fix, Sprint 3 Track 5**. Sprint 3 DOES NOT manually seed 15 famous producers — it builds a corpus-wide producer metadata strategy: (a) a decision session on data source (Haiku extraction from retailer sites + producer websites, or manual curation of top 300-500, or a hybrid); (b) a pipeline script; (c) a verification pass for the top 50 marquee producers. Budget ~$50-100 Haiku.

### Compound #5 — Edge function hygiene + voice module consolidation

| Layer | Finding | Essence |
|---|---|---|
| Code (hygiene) | **S2.5 F1** + **S2.8** + **S2.9 F6** | `describe-chemical` edge function still deployed ACTIVE, `verify_jwt=false`, unauthenticated credit burn risk. Triple-verified across audit sessions. |
| Code (hygiene) | **S2.5 F31** | Edge function source code not in git — `supabase/functions/` doesn't exist in repo |
| Code (hygiene) | **S2.5 F4** / **S2.6 F2** | `enrich-wine` reads `grapes.name` (wrong); ships weak preamble without voice rules; uses stale `claude-sonnet-4-20250514` |
| Code (hygiene) | **S2.5 F5** | 3 Anthropic model IDs coexist without central config (haiku-4-5 / sonnet-4 stale / sonnet-4-6) |
| Content (voice) | **S2.6 F1** | 4 reference-layer enrichment scripts (`appellation_insights.py`, `region_insights.py`, `country_insights.py`, `grape_insights.py`) use weak 8-word BANNED_WORDS list without NEVER INVENT rules |
| Content (voice) | **S2.6 F2** | `enrich-wine` edge function ships the same weak preamble as the 4 reference scripts |

**Net:** **ONE compound fix, Sprint 3 Track 1 (highest priority)**. Close in opening 4-6 hours of Sprint 3: (a) delete `describe-chemical`; (b) vendor `enrich-wine` source into `supabase/functions/`; (c) create `pipeline/lib/models.py` with HAIKU/SONNET/OPUS constants; (d) create `pipeline/lib/voice.py` with VOICE_RULES_BLOCK + NEVER INVENT + banned-word lists; (e) rewrite `enrich_prompts.py` and 4 reference enrichment scripts to import `voice.py`; (f) rewrite vendored `enrich-wine` index.ts to inline the same voice rules + read `grapes.display_name`; (g) redeploy `enrich-wine`. **Closes 14 of 32 S2.6 findings + 8 of 32 S2.5 findings in one session.**

### Compound #6 — Food pairing (empty table + no UI gate)

| Layer | Finding | Essence |
|---|---|---|
| Data | **S2.6 F8** | `wine_food_pairings` public table is empty (0 rows); `archive.wine_food_pairings` has 809 rows — 30K rebuild wiped public; CLAUDE.md claim of "809 structured links" is stale |
| Data | **S2.6 F6** | `grape_insights` table has 0 rows despite `grape_insights.py` containing the best food-pairing prompt in Loam |
| Content (schema) | **S2.6 F7** | `GRADE_C_FIELDS` schema in `enrich_prompts.py` drops the food_pairing field — 5,003 of 5,062 Grade C wines lack food-pairing prose |
| UI | **S2.7 F22** | WinePage food pairing section invisible on 99% of Grade C wines because `insight?.ai_food_pairing` is NULL |

**Net:** **ONE compound fix, Sprint 3 Track 8**. (a) Restore `wine_food_pairings` from `archive.wine_food_pairings` via bulk UPDATE (~15 min); (b) widen `GRADE_C_FIELDS` in `enrich_prompts.py` to include food_pairing; (c) port `grape_insights.py` food-pairing rules upstream into the shared voice block (depends on Track 1 voice module).

### Compound #7 — Doc drift / meta hygiene (systematic, not ad-hoc)

| Layer | Finding | Essence |
|---|---|---|
| Meta (docs) | **S2.1 F28** | ≥6 places in CLAUDE.md have hardcoded counts that drift from live DB |
| Meta (frontend→docs) | **S2.7 F2** | Dead column pattern at the code layer — CountryPage selects non-existent `ai_signature_grapes` |
| Meta (docs→pipeline) | **S2.8 F3** | `docs/SOURCES.md` documents `id_type='ttb_cola'` — actual column is `system`, value is `'cola'`, and `lwin_7` missing entirely. Dead-column pattern at doc layer. |
| Meta (CLAUDE.md internal) | **S2.8 F1**, **F4** | CLAUDE.md `## Current Focus` is 5+ sessions stale + internally contradicts `## Current State`; vineyards count contradiction (815 vs 881 vs 0) |
| Meta (memory) | **S2.8 F2** | `memory/30k_status.md` ships pre-pivot Reference-First claim via MEMORY.md auto-load into every conversation |
| Meta (roadmap) | **S2.8 F5** | `loam_roadmap.json` Sprint 2 sub_tasks are 7 session states behind; two dashboards reading two different truth surfaces |
| Meta (hardcoded counts) | **S2.8 F23** | wine_grapes 47,035 vs live 46,028; color 153,311 vs live 153,229; archive vineyards 815 vs live 881 |
| Meta (dead docs) | **S2.8 F6**, **F8**, **F9**, **F11** | 30K_PLAN.md stale + broken paths; AUDIT_2026-04-01.md superseded; empty architecture/ pipelines/ dirs; MERGE_STRATEGY.md references non-existent files |
| Meta (memory files) | **S2.8 F19**, **F20**, **F21**, **F22** | vivino-pipeline.md has no frontmatter; product-architecture.md uses Tier 0-3 nomenclature; workflow_session_tips.md has stale sections; project_sprint_model_and_rf_direction.md filename misleading |

**Net:** **ONE compound doc hygiene bundle, Sprint 3 Track 0** (because it's cheap, clears context drift for every subsequent session, and blocks nothing). ~2-3 hours combined. Closes 23 S2.8 findings.

### Compound #8 — UI hygiene bundle

| Layer | Finding | Essence |
|---|---|---|
| UI (P0) | **S2.7 F1** | 12,083 wine pages render empty `<h1></h1>` (fetches `wine.name` not `display_name`) |
| UI (P0) | **S2.7 F2** | 100% of country pages silently fail (selects non-existent `ai_signature_grapes`) |
| UI (P0) | **S2.7 F5** | Zero AI disclaimer / confidence badge / source attribution in consumer pages |
| UI (P0) | **S2.7 F7** | Footer /about link goes to blank screen |
| UI (P0) | **S2.7 F8** | `/vineyard/:id` route is dead (0 rows + no search RPC support) |
| UI (P0) | **S2.7 F9** | Zero `.catch()` in consumer pages, no error boundary at App level — reason F2 silently ships |
| UI (P0) | **S2.7 F10** | No 404 catch-all route |
| UI (P0) | **S2.7 F11** | Dashboard queries empty `producer_insights` table |
| UI (P1) | **S2.7 F16-F19** | 8 `ai_*` fields fetched by consumer pages and never rendered (AppellationPage, CountryPage, RegionPage, GrapePage) |
| UI (P1) | **S2.7 F20**, **F21** | A11y baseline broken — zero aria attrs, h1→h3 heading skip |
| UI (P1) | **S2.7 F23**, **F24** | Classification system_name never rendered; Section header renders above empty FactGrid |
| UI (P2) | **S2.7 F27** | 500 LOC of Section/Tag/Fact/Loading/NotFound duplication across 8 pages |

**Net:** **ONE compound UI hygiene bundle, Sprint 3 Track 0** (alongside doc hygiene). ~3-4 hours combined if F27 consolidation is NOT done; ~6-7 hours if F27 consolidation is done (optional but 8x multiplier on all other UI fixes).

### Compound #9 — AI safety rail / brand voice

| Layer | Finding | Essence |
|---|---|---|
| UI | **S2.7 F5** | No AI disclaimer / confidence badge / source attribution |
| Content | **S2.9 F12** | Brand voice and AI voice conflated; no `/about`, no `/known-issues`, no "who is Loam" surface |
| Content | **S2.9 F26** | No "last updated" chip anywhere |
| Business | **S2.9 F15** | No press kit / media surface |

**Net:** **ONE compound UI/copy track, Sprint 3 Track 6**. Ship `<AIBadge confidence enrichedAt>` component + `/about` + `/known-issues` + `<LastUpdatedChip>`. 3-4 hours combined.

---

## Sprint 3 Backlog — 9 tracks, tiered by "unblocks what"

### Tier 1 — Sprint 5 contamination blockers (skip any, and Sprint 5 re-contaminates)

#### Track 1 — Code/voice hygiene bundle [2 sessions, ~6-8 hours total]

**Dependencies:** none — first Sprint 3 work
**Unblocks:** Track 3 (grape repair), Tier 2 staging unlock, all Sprint 5 regeneration
**Budget:** $0 (code-only work, no AI calls)

1. **S3.T1.1** — Delete `describe-chemical` edge function. `supabase functions delete describe-chemical` (or MCP equivalent). Verify via `list_edge_functions`. [Closes S2.5 F1, S2.8 edge function re-verification, S2.9 F6.] **Trivial, 5 minutes, Sprint 3 Session 1 Minute 1.**
2. **S3.T1.2** — Create `supabase/functions/` dir. Vendor `enrich-wine` source from MCP `get_edge_function` into git. Commit. [Closes S2.5 F31.]
3. **S3.T1.3** — Create `pipeline/lib/models.py` with `HAIKU = "claude-haiku-4-5-20251001"`, `SONNET = "claude-sonnet-4-6"`, `OPUS = "claude-opus-4-6"` constants. Grep-and-replace the 15+ hardcoded model ID sites in `pipeline/enrich/*.py`, `pipeline/promote/*.py`, etc. [Closes S2.5 F5.]
4. **S3.T1.4** — Create `pipeline/lib/voice.py`. Export `VOICE_RULES_BLOCK` (banned words, sommelier-theater phrases, hedging rules) and `NEVER_INVENT_BLOCK` (fact-check discipline). Source from `pipeline/enrich/enrich_prompts.py` — the only tightened voice source that exists today. [Closes S2.6 F1.]
5. **S3.T1.5** — Rewrite 4 reference-layer enrichment scripts (`appellation_insights.py`, `region_insights.py`, `country_insights.py`, `grape_insights.py`) to import from `pipeline/lib/voice.py` and `pipeline/lib/models.py`. Remove their inline 8-word BANNED_WORDS lists. [Closes S2.6 F1, partial.]
6. **S3.T1.6** — Rewrite vendored `enrich-wine/index.ts` to inline the same voice rules + read `grapes.display_name` instead of `grapes.name` in `assembleContext()`. Redeploy. [Closes S2.5 F4, S2.6 F2.]
7. **S3.T1.7** — Sanity-test: run the updated `appellation_insights.py` in `--dry-run` mode on Chambertin to verify the new prompt includes VOICE_RULES_BLOCK + NEVER_INVENT_BLOCK + correct grape display names. Do NOT write to DB. [Verification step.]

**Done criterion:** `list_edge_functions` shows `describe-chemical` absent; `enrich-wine` present and `verify_jwt=true`; `supabase/functions/enrich-wine/index.ts` exists in git; `pipeline/lib/voice.py` exists and imports cleanly from 4 reference scripts + edge function; dry-run Chambertin prompt contains "CHARDONNAY" (not "CHARDONNAY BLANC") + "NEVER INVENT" block.

#### Track 3 — Grape repair compound [2-3 sessions, ~8-12 hours total]

**Dependencies:** Track 1 (voice module + model IDs), optional but ideal
**Unblocks:** Sprint 5 wine-layer regeneration
**Budget:** $0-30 (mostly code, maybe $10-20 for resolver regression test on stratified sample via Haiku)

1. **S3.T3.1** — Delete PINOT BLANC's 4 polluting synonyms from `grape_synonyms`: `PINOT CHARDONNAY`, `CHARDONNET PINOT BLANC`, `PINOT BLANC CHARDONNET`, `PINOT GRIGIO`. [Closes S2.4 F2.]
2. **S3.T3.2** — Audit + fix `varietal_categories` 5+ wrong-grape links (Merlot→Grolleau Noir, Riesling→Crouchen, Verdejo→Trousseau Noir, Greco→Albana Bianca, St. Laurent→Muscat St. Laurent). Update via `apply_migration`. [Closes S2.4 F1.]
3. **S3.T3.3** — Consolidate grape resolvers on `pipeline/lib/resolve.py::ReferenceResolver.resolve_grape()`. Replace the 4 duplicate implementations in `batch_pipeline._load_reference_data`, `haiku_grape_extract.build_grape_lookup`, `grape_from_name`, and any leftover. All 13 `INSERT INTO wine_grapes` call sites should call one resolver. [Closes S2.5 F11.]
4. **S3.T3.4** — Fix `pipeline/identity/batch_pipeline._match_ttb_to_wine` to match on `grape_varietals` compatibility in addition to `fanciful_name`. Stop collapsing 4 grape-specific COLAs onto 1 canonical wine. [Closes S2.5 F2.]
5. **S3.T3.5** — Fix `pipeline/promote/ttb_grape_promote.py` to either drop `DISTINCT ON (canonical_wine_id)` or detect conflict + require manual review. [Closes S2.5 F17.]
6. **S3.T3.6** — Re-run grape resolver on affected wines: `SELECT wine_id FROM wine_grapes WHERE grape_id IN (PINOT_BLANC_ID, CHARDONNAY_BLANC_ID) AND wine.display_name ILIKE '%chardonnay%'` — re-resolve. Expect ~2,700 wines to move from Pinot Blanc → Chardonnay. [Closes S2.3 F2 at the data level.]
7. **S3.T3.7** — Fix `pipeline/load/lwin_long_tail.py` to populate `display_name` on the 50,908 long-tail wines. Backfill existing rows via `UPDATE public.wines SET display_name = name WHERE display_name IS NULL AND external_id IS NOT NULL AND id IN (...)`. [Closes S2.5 F18.]
8. **S3.T3.8** — Regression test on a 50-wine stratified sample (marquee + mid-tier + long-tail) to verify the Chardonnay/Pinot Blanc fix holds AND no new regressions appear. Opus inline, $0.

**Done criterion:** `SELECT count(*) FROM wines w JOIN wine_grapes wg ON wg.wine_id=w.id JOIN grapes g ON g.id=wg.grape_id WHERE w.display_name ILIKE '%Chardonnay%' AND g.display_name='Pinot Blanc'` returns ≤ 50 rows (down from 2,743); Bogle Phantom Chardonnay resolves to Chardonnay only (no Pinot Blanc); Vega Sicilia Único is present or flagged (S2.3 F1 tail); `varietal_categories` Merlot/Riesling/Verdejo/Greco/St. Laurent rows are correct; `pipeline/lib/resolve.py::resolve_grape()` is the only grape-lookup entrypoint.

### Tier 2 — Massive existing-data unlock (highest-ROI user-visible change available)

#### Track 2 — Staging archive relink [1 session, ~4-6 hours]

**Dependencies:** none — can run in parallel with Track 1
**Unblocks:** Tier 3 Track 4 (producer metadata can reference unlocked catalog data), Tier 3 first-impression credibility (price coverage goes 1.81% → ~35%)
**Budget:** $0 (code + SQL only)

1. **S3.T2.1** — Extend `pipeline/promote/relink_staging_to_current.py::STAGING_TABLES_WINE` from 1 entry to 30 entries. Mirror the existing producer-mapping table pattern for wines: use `archive.wines → public.wines` join via `external_ids.value` matched on LWIN/COLA, fall back to normalized display_name + producer_name fuzzy match.
2. **S3.T2.2** — Run `relink_staging_to_current.py --dry-run` to measure how many of the 286,918 dangling pointers will be restored. Target: ≥ 80% restore rate.
3. **S3.T2.3** — Run `relink_staging_to_current.py --execute` on wines. Log success count.
4. **S3.T2.4** — Re-run `pipeline/promote/retail_promote.py` (or equivalent bulk SQL) to push staging prices → `public.wine_vintage_prices` now that the joins work. Expect +30K-100K prices restored.
5. **S3.T2.5** — Repeat for scores, vintages, ABVs, UPCs that were blocked by the same issue.
6. **S3.T2.6** — Measure: `SELECT count(*) FROM public.wine_vintage_prices` should jump from 23,220 to 50-100K+. `SELECT count(distinct wine_id) FROM public.wine_vintage_prices` should jump from 2,815 to 25K-50K.

**Done criterion:** `SELECT count(*) FROM public.wine_vintage_prices` > 50,000; `SELECT count(distinct wine_id) FROM public.wine_vintage_prices` > 20,000 (i.e., price coverage > 12.8% of active wines, up from 1.81%); staging tables have fewer than 10,000 dangling wine_id pointers (down from 286,918); `SELECT count(distinct wine_id) FROM public.wine_vintage_scores` > 10,000.

### Tier 3 — First-impression credibility (pre-demo polish)

#### Track 0A — Doc hygiene bundle [1 session, ~2-3 hours]

**Dependencies:** none — cheapest Sprint 3 work, clears drift for every subsequent session
**Unblocks:** every future session briefing reads correctly
**Budget:** $0

1. **S3.T0A.1** — Rewrite `CLAUDE.md` `## Current Focus` section (~20 min). Point at Sprint 3, reference synthesis.md. [Closes S2.8 F1.]
2. **S3.T0A.2** — Update `memory/30k_status.md` Next section (5 min). Delete Reference-First claim. [Closes S2.8 F2.]
3. **S3.T0A.3** — Fix `docs/SOURCES.md:33` external_ids storage typo — `id_type` → `system`, `ttb_cola` → `cola`, add `lwin_7` (2 min). [Closes S2.8 F3.]
4. **S3.T0A.4** — Delete `CLAUDE.md` "vineyards has 815 rows" line + unify the two vineyard sections (2 min). [Closes S2.8 F4.]
5. **S3.T0A.5** — Update `data/stats/loam_roadmap.json` Sprint 2 sub_tasks to reflect S2.1-S2.9 done (5 min). [Closes S2.8 F5.]
6. **S3.T0A.6** — Archive `docs/30K_PLAN.md` → `docs/reference/` + fix `CLAUDE.md:306` broken path (10 min). [Closes S2.8 F6.]
7. **S3.T0A.7** — Add `docs/BACKLOG.md` to CLAUDE.md doc index + prune 6 closed items (10 min). [Closes S2.8 F7.]
8. **S3.T0A.8** — Archive `docs/AUDIT_2026-04-01.md` → `docs/reference/` (2 min). [Closes S2.8 F8.]
9. **S3.T0A.9** — `git rm -r docs/architecture/ docs/pipelines/` (1 min). [Closes S2.8 F9.]
10. **S3.T0A.10** — Rewrite CLAUDE.md pre-30K "Next Steps" block (15 min). [Closes S2.8 F10.]
11. **S3.T0A.11** — Mark `docs/MERGE_STRATEGY.md` as retrospective or move to reference (30 min). [Closes S2.8 F11.]
12. **S3.T0A.12** — Add Status banner to `docs/ENRICHMENT.md` (10 min). [Closes S2.8 F12.]
13. **S3.T0A.13** — Update `docs/SOURCES.md` last-updated header (10 min). [Closes S2.8 F13.]
14. **S3.T0A.14** — Add Never-Invent section + L3 gate cross-reference to `docs/VOICE.md` (30 min). [Closes S2.8 F14.]
15. **S3.T0A.15** — Archive `docs/PATH_A_ROLLBACK.md` (2 min). [Closes S2.8 F15.]
16. **S3.T0A.16** — Add `docs/IDENTITY_RULES.md` to CLAUDE.md doc index (2 min). [Closes S2.8 F16.]
17. **S3.T0A.17** — Add SUPERSEDED markers or split `docs/DECISIONS.md` current/archive (1 hr judgment-heavy). [Closes S2.8 F17.]
18. **S3.T0A.18** — Prune `data/sessions.md` Sprint 1 entries + adopt one-line format (15 min). [Closes S2.8 F18.]
19. **S3.T0A.19** — Add frontmatter to `memory/vivino-pipeline.md` (2 min). [Closes S2.8 F19.]
20. **S3.T0A.20** — Update `memory/product-architecture.md` Tier 0-3 → F/D/C/B/A (5 min). [Closes S2.8 F20.]
21. **S3.T0A.21** — Delete stale sections of `memory/workflow_session_tips.md` (5 min). [Closes S2.8 F21.]
22. **S3.T0A.22** — Rename `memory/project_sprint_model_and_rf_direction.md` → `project_sprint_model_and_dashboards.md` + MEMORY.md edit (1 min). [Closes S2.8 F22.]
23. **S3.T0A.23** — Strip 4 CLAUDE.md hardcoded counts + point at `dash.ps1` (5 min). [Closes S2.8 F23.]

**Done criterion:** CLAUDE.md `## Current Focus` reads "Sprint 3 executing synthesis.md backlog"; `MEMORY.md` links to renamed `project_sprint_model_and_dashboards.md`; `docs/architecture/` + `docs/pipelines/` do not exist; `docs/30K_PLAN.md` is at `docs/reference/`; `docs/SOURCES.md:33` reads `system='cola'`; no hardcoded `815 vineyards` in CLAUDE.md; `loam_roadmap.json` Sprint 2 sub_tasks all show `done`.

#### Track 0B — UI hygiene bundle [1-2 sessions, ~3-6 hours]

**Dependencies:** none — can run in parallel with Track 0A and Track 1
**Unblocks:** Sprint 5 AI content rendering (without F16-F19 fixed, Sprint 5 content is invisible), Tier 3 first-impression credibility
**Budget:** $0

1. **S3.T0B.1** — Fix `CountryPage.tsx:40` column typo: `ai_signature_grapes` → `ai_signature_styles` (1 min). [Closes S2.7 F2.]
2. **S3.T0B.2** — Add `display_name` to `WinePage.tsx:175` SELECT + render fallback chain (5 min). [Closes S2.7 F1.]
3. **S3.T0B.3** — Fix footer `/about` link — route exists (built in Track 6) or temporarily link to a static markdown (1 min). [Closes S2.7 F7.]
4. **S3.T0B.4** — Park `/vineyard/:id` route or delete (5 min). [Closes S2.7 F8.]
5. **S3.T0B.5** — Add error boundary in `main.tsx` wrapping `<App/>` + `.catch()` on every consumer-page fetch OR migrate consumer pages to `useEntityDetail` hook (2 hours). [Closes S2.7 F9.]
6. **S3.T0B.6** — Add 404 catch-all route in `App.tsx` (5 min). [Closes S2.7 F10.]
7. **S3.T0B.7** — Remove `producer_insights` from Dashboard N+1 queries (table is empty; 1 min). [Closes S2.7 F11.]
8. **S3.T0B.8** — Make producer `website_url` clickable with `target="_blank" rel="noopener noreferrer"` on consumer pages (5 min). [Closes S2.7 F14.]
9. **S3.T0B.9** — Fix dev `WineDetail.tsx:37` to use `display_name` (1 min). [Closes S2.7 F15.]
10. **S3.T0B.10** — Render the 8 dead-fetch `ai_*` fields on AppellationPage (3), CountryPage (2), RegionPage (1), GrapePage (2). ~30 min total. [Closes S2.7 F16-F19.]
11. **S3.T0B.11** — A11y baseline: aria-current on nav, aria-live on loading states, htmlFor on form labels (where any exist), role attributes on landmark elements. ~2 hours. [Closes S2.7 F20.]
12. **S3.T0B.12** — Change `<h3>` to `<h2>` in shared Section component so heading hierarchy is h1→h2 not h1→h3. 5 min if F27 (Track 0B.15) is already done; 8x more per-page without. [Closes S2.7 F21.]
13. **S3.T0B.13** — Render `classification_system_name: level_name` on WinePage classification (5 min). [Closes S2.7 F23.]
14. **S3.T0B.14** — Section component conditional render — no Section header above empty FactGrid (15 min). [Closes S2.7 F24.]
15. **S3.T0B.15 (OPTIONAL but recommended)** — Consolidate Section/Tag/Fact/Loading/NotFound into `frontend/src/components/consumer/primitives.tsx` (half day). 8x multiplier on F21/F24/F23. [Closes S2.7 F27.]

**Done criterion:** CountryPage renders `ai_overview` on Italy/France/USA/Spain sample; WinePage renders non-empty `<h1>` on 155K wines (display_name fallback works); `/about` returns a page (not blank); `.catch()` or ErrorBoundary catches a deliberate 500 and shows an error state; 8 ai_* fields are visible on Appellation/Country/Region/Grape pages; heading hierarchy is h1→h2 not h1→h3; lighthouse a11y score > 85.

#### Track 4 — Producer metadata strategy [1-2 sessions, ~4-8 hours + $50-100 AI budget]

**Dependencies:** Track 0A (docs), Track 1 (model IDs centralized)
**Unblocks:** Sprint 5 wine-content quality (wines inherit producer metadata for context), first-impression credibility on marquee wines
**Budget:** $50-100 Haiku/Sonnet for metadata extraction

1. **S3.T4.1** — Decision session (30 min): source strategy. Options: (a) Haiku extraction from retailer catalogs in staging (KL, Empson, Skurnik, Winebow, European Cellars — already have detailed producer-level data in `source_*` tables); (b) Haiku extraction from producer websites via scraper (existing `pipeline/fetch/producer_site_scrape.py`); (c) manual curation of top 300 via Wikipedia + official sites; (d) hybrid — manual top 50 + Haiku top 500. **Recommend (d) hybrid.**
2. **S3.T4.2** — Build `pipeline/promote/producer_metadata_seed.py`. Input: list of top N producers by wine_count + LWIN backbone status + marquee-flag. For each: (a) check `source_*` staging for existing metadata; (b) extract via Haiku prompt; (c) write with `data_provenance` trail.
3. **S3.T4.3** — Run on top 50 (manual verification via Opus inline fact-check).
4. **S3.T4.4** — Run on top 500 (Haiku extraction, ~$20-50 budget).
5. **S3.T4.5** — Re-run S2.7 F4 verification query: `SELECT count(*) FROM producers WHERE website_url IS NOT NULL AND year_established IS NOT NULL AND latitude IS NOT NULL`. Target: ≥ 300 (up from 1).

**Done criterion:** ≥ 300 producers have (website_url AND year_established AND latitude), including all 15 marquee producers from S2.3 F3 (DRC, Lafite, Latour, Margaux, Haut-Brion, Pétrus, Gaja, Conterno, Giacosa, Tenuta San Guido, Screaming Eagle, Harlan Estate, Ridge, Henri Jayer, Leroy). Philosophy and Estates & Labels sections render on ProducerPage for at least 50 producers.

### Tier 4 — Voice/content gate (Sprint 5 prerequisite)

#### Track 6 — AI safety rail + brand voice [1 session, ~3-4 hours]

**Dependencies:** Track 0B (consumer page hygiene — F9 error boundary must exist first)
**Unblocks:** `ENRICHMENT_ENABLED` feature flag flip in Sprint 5
**Budget:** $0

1. **S3.T6.1** — Build `<AIBadge confidence enrichedAt>` component. Renders a "Generated by Loam's AI" badge + confidence level + "as of" date under every `ai_*` field. [Closes S2.7 F5 + part of S2.9 F12.]
2. **S3.T6.2** — Wire `<AIBadge>` into every `ai_*` render across consumer pages (WinePage, AppellationPage, CountryPage, RegionPage, GrapePage, ProducerPage).
3. **S3.T6.3** — Build `<LastUpdatedChip>` reading `enriched_at` / `updated_at`. [Closes S2.9 F26.]
4. **S3.T6.4** — Write `/about` page: "Loam is a personal project building structured wine data. Sources: TTB, LWIN, Open-Meteo, Claude AI synthesis. Known gaps: see `/known-issues`." [Closes S2.9 F12 + S2.9 F15 partial.]
5. **S3.T6.5** — Write `/known-issues` page rendering the Sprint 3 backlog from `synthesis.md` — automatically updated via a build step OR manually refreshed at each sprint wrap. [Closes S2.9 F12 + S2.9 F15.]
6. **S3.T6.6** — Write footer "early access" disclaimer on every page. [Closes S2.9 F12.]
7. **S3.T6.7** — Rewrite `CLAUDE.md:3` product pitch to avoid the "wine intelligence" collision (S2.9 F13/F20/F29). Suggested: "Loam is a terroir-grade wine data platform — backbone identifiers, appellation rules, vintage weather, and structured facts." Update memory files to match. Add tagline "Loam — rooted in the soil" to homepage header. [Closes S2.9 F13/F20/F28/F29.]

**Done criterion:** `<AIBadge>` renders on every `ai_*` field in consumer pages; `/about` page exists and loads; `/known-issues` page exists and shows a reasonable subset of Sprint 3 backlog; CLAUDE.md:3 no longer reads "wine intelligence platform"; homepage has a tagline.

#### Track 7 — L3 fact-check gate [1 session, ~4-6 hours + ~$30-50 AI budget]

**Dependencies:** Track 1 (voice module), Track 3 (grape repair so fact-checks aren't distracted by bad grape labels)
**Unblocks:** Sprint 5 regeneration discipline (gate blocks writes without fact-check)
**Budget:** $30-50 Sonnet/Opus for gate calibration — this is the rescoped $18 S2.3 pre-auth

1. **S3.T7.1** — Design L3 gate contract. Input: `(entity_type, entity_id, proposed_content, context)`. Output: `(status='passed'|'failed'|'flagged', confidence, errors[])`. Gate is a separate function in `pipeline/lib/fact_check.py`; enrichment scripts call it before writing to DB.
2. **S3.T7.2** — Implement gate via Opus 4.6 with a structured prompt: "Here's proposed content for {entity}. List every factual claim. For each, is it verifiable? Is it in the provided source facts? If not, mark it INVENTED. If verifiable but unsupported, mark it UNSUPPORTED. Return JSON." Use current S2.3/S2.6 L3 examples as calibration.
3. **S3.T7.3** — Calibration: run gate on S2.3's 99-wine sample + S2.6's 5 Grade C hooks that failed audit. Expect 8-15 failures. Measure false-positive rate.
4. **S3.T7.4** — Integrate into `enrich_prompts.py` + `enrich-wine` edge function: before writing, call gate. If failed, log to `enrichment_log.stale_reason='fact_check_failed'` and skip write.
5. **S3.T7.5** — Don't run on existing corpus yet — that's Sprint 5. Just build and test the gate infrastructure.

**Done criterion:** `pipeline/lib/fact_check.py` exists with `run_l3_gate()` function; gate returns structured failures on S2.3 + S2.6 calibration sample; gate is wired into `enrich_prompts.py` and blocks writes when failed; ENRICHMENT_ENABLED flag on `enrich-wine` is CONFIRMED STILL OFF (Sprint 3 does not flip it).

### Tier 5 — Business-signal collection + hygiene

#### Track 5 — Signal collection [1 session, ~3 hours]

**Dependencies:** Track 0B (UI hygiene so landing page doesn't render with bugs)
**Unblocks:** Sprint 5 prioritization by demand, Sprint 6 monetization decisions
**Budget:** $0

1. **S3.T5.1** — Build `useWineLookupLog()` hook that writes `(wine_id, wine_vintage_id, source='web', looked_at=now())` to `public.wine_lookups` on WinePage mount. [Closes S2.9 F2.]
2. **S3.T5.2** — Add a landing page at `/` with a clear "what is Loam" sentence + email sign-up form (Buttondown free tier or Substack — user picks). [Partial S2.9 F8.]
3. **S3.T5.3** — Write 1 "building Loam in the open" blog post with 3 screenshots (marquee wine page + appellation page + producer page post-Track-4-metadata). Ship on `/blog/1` or a subdomain. [Partial S2.9 F8.]
4. **S3.T5.4** — Send preview-access DMs to 10 sommelier/wine-trade contacts (user-driven, not code). Log in `data/stats/sprint3_outreach.md`. [Partial S2.9 F8 + S2.9 F22.]
5. **S3.T5.5** — Monetization direction decision — NOT execution. 30-min decision session. Output: one-page `docs/MONETIZATION.md` picking a default direction. Recommendation from F1/F4/F19: **free consumer tier + $9/mo Pro + $149/yr Trade** with Trade as the Sprint 5 beachhead ICP. [Closes S2.9 F1 as a decision; execution is Sprint 6.]

**Done criterion:** `wine_lookups` table has ≥ 10 rows from real user sessions; landing page exists at `/` with email signup; at least 1 blog post shipped; `docs/MONETIZATION.md` exists with a picked direction.

### Tier 6 — Session-tail hygiene (optional; defer to Sprint 4 if Sprint 3 runs long)

#### Track 8 — Food pairings restoration [partial work, ~1 hour]

1. **S3.T8.1** — Restore `public.wine_food_pairings` from `archive.wine_food_pairings` via bulk INSERT. [Closes S2.6 F8.]
2. **S3.T8.2** — Widen `GRADE_C_FIELDS` in `enrich_prompts.py` to include food_pairing. [Partial closes S2.6 F7.]
3. **S3.T8.3** — DO NOT re-run enrichment — that's Sprint 5. Just make the schema + restoration ready.

**Done criterion:** `SELECT count(*) FROM public.wine_food_pairings` > 800; `GRADE_C_FIELDS` schema includes food_pairing; no re-run of `enrich_prompts.py` yet.

---

## Sprint 3 session sequence (recommended)

| # | Session | Tracks | Budget | Deliverable |
|---|---|---|---|---|
| S3.1 | **Open + code/voice hygiene** | Track 1 + Track 0A partial | $0 | `describe-chemical` deleted, `enrich-wine` vendored, `pipeline/lib/models.py` + `pipeline/lib/voice.py` exist, 4 reference scripts rewritten, doc hygiene half-done |
| S3.2 | **Doc hygiene + UI hygiene P0s** | Track 0A finish + Track 0B P0s | $0 | All CLAUDE.md drift fixed, CountryPage + WinePage + footer + error boundary + 404 catch-all shipped |
| S3.3 | **Staging archive relink** | Track 2 | $0 | Prices/scores/vintages/UPCs unlocked from archive — price coverage 1.81% → ≥ 12.8% |
| S3.4 | **Grape repair compound part 1** | Track 3 T1-T4 | $0 | PINOT BLANC synonyms deleted, varietal_categories fixed, resolvers consolidated, batch_pipeline fixed |
| S3.5 | **Grape repair compound part 2 + verification** | Track 3 T5-T8 | $0-20 | wine_grapes re-resolved, lwin_long_tail display_name backfilled, regression sample passes |
| S3.6 | **UI hygiene P1s + AI safety rail** | Track 0B P1s + Track 6 | $0 | 8 ai_* fields rendered, a11y baseline, AIBadge + /about + /known-issues shipped |
| S3.7 | **Producer metadata strategy + seed run** | Track 4 | $20-100 | Top 50 marquee + top 500 via Haiku, ProducerPage renders Philosophy section |
| S3.8 | **L3 fact-check gate build** | Track 7 | $30-50 | `pipeline/lib/fact_check.py` exists, gate calibrated on S2.3/S2.6 sample, wired into enrich_prompts.py |
| S3.9 | **Signal collection + food pairings restore** | Track 5 + Track 8 | $0 | `wine_lookups` instrumented, landing page + email signup live, `wine_food_pairings` restored, `docs/MONETIZATION.md` written |
| S3.10 | **Sprint 3 exit review + Sprint 4 scoping** | cross-track verification | $0-30 | Re-run S2.3 regression sample, measure done criteria from Sprint 3, scope Sprint 4 reference redesign |

**Estimated sessions:** 10 (buffered from 8 minimum).
**Estimated Sprint 3 budget:** $50-200 actual (mostly Track 4 producer seed + Track 7 L3 gate calibration + Track 5 signal collection).
**Combined Sprint 2+3 ceiling:** $50 → may need to extend to $250 at Sprint 3 mid-point if Track 4 runs expensive.

---

## Sprint 3 done criteria

Sprint 3 closes when **all of these** are measurably true:

1. **`describe-chemical` is deleted.** `list_edge_functions` does not return it.
2. **`enrich-wine` is vendored + voice-module-aware + grape-display-name-aware.** `supabase/functions/enrich-wine/index.ts` exists in git; it imports voice rules; it reads `grapes.display_name`.
3. **`pipeline/lib/voice.py` + `pipeline/lib/models.py` exist and are used by all 4 reference enrichment scripts + `enrich_prompts.py` + vendored `enrich-wine`.**
4. **Chardonnay/Pinot Blanc bug is fixed at the data layer.** `SELECT count(*) FROM wines w JOIN wine_grapes wg ON wg.wine_id=w.id JOIN grapes g ON g.id=wg.grape_id WHERE w.display_name ILIKE '%Chardonnay%' AND g.display_name='Pinot Blanc'` returns ≤ 50 (down from 2,743).
5. **Staging archive relink executed.** `SELECT count(distinct wine_id) FROM public.wine_vintage_prices` > 20,000 (up from 2,815). Dangling wine_id pointers in staging drop to < 10,000 (down from 286,918).
6. **UI P0s shipped.** CountryPage renders ai_overview; WinePage renders non-empty h1 on display_name fallback; `/about` exists; error boundary catches failures; 8 ai_* fields render.
7. **Producer metadata ≥ 300 producers have (website_url + year_established + latitude).** All 15 marquee producers from S2.3 F3 populated.
8. **L3 fact-check gate exists + calibrated.** `pipeline/lib/fact_check.py` imports cleanly; calibration run on S2.3/S2.6 sample passes with measured false-positive rate < 10%.
9. **AI safety rail shipped.** `<AIBadge>` + `<LastUpdatedChip>` render on every `ai_*` field. `/known-issues` page exists.
10. **Doc hygiene bundle closed.** CLAUDE.md `## Current Focus` updated; all S2.8 findings addressed.
11. **Signal collection started.** `public.wine_lookups` has ≥ 10 rows; landing page shipped; `docs/MONETIZATION.md` exists with a picked direction.
12. **`ENRICHMENT_ENABLED` feature flag is STILL off on `enrich-wine` edge function.** (Sprint 3 is NOT the flag flip; that's Sprint 5 exit.)

If any criterion is missed, Sprint 3 does not close — extend sessions or explicitly defer to Sprint 4 with a journal entry.

---

## What Sprint 3 deliberately defers

### To Sprint 4 (Reference redesign)

- **`appellation_rules` JSONB schema canonicalization** (S2.4 F11) — large rewrite, needs Sprint 4 design work
- **`appellation_grapes` language + provenance cleanup** (S2.4 F12-F16) — depends on rules schema
- **`appellation_soils` provenance schema** (S2.4 F17) — 2 columns → proper provenance model
- **French AOC diacritics restoration** (S2.4 F4) — Sprint 4 cleanup pass
- **`established_year` poisoning fix** (S2.4 F9) — 345 rows, Sprint 4 data hygiene
- **Pauillac 1855 classification tier counts** (S2.4 F5) — Sprint 4 reference pass
- **121 slash-concatenated appellation aliases** (S2.4 F3) — Sprint 4 alias normalization
- **grapes.name VIVC cépage suffix form cleanup** (S2.4 F6) — deprioritized since display_name already populated (S2.5 verified)
- **Retailer affiliate link architecture** (S2.9 F10) — depends on Track 2 staging relink
- **Pricing freshness architecture** (S2.9 F23) — Sprint 4 data hygiene
- **SEO hygiene items** (S2.9 F14) — sitemap.xml, robots.txt, og tags; partial overlap with Track 0B but not blocking

### To Sprint 5 (Reference + wine enrichment execution)

- **Regeneration of 49 contaminated appellation_insights** (S2.6 F5) — Sprint 5, reference-first
- **European appellation_insights coverage** (S2.6 F9) — ~500 new rows, ~$15-20 budget
- **Wine-layer regeneration** — dependent on Track 1 voice + Track 3 grape repair + Track 7 fact-check gate
- **Re-fact-check existing 5,108 wine_insights rows** — Sprint 5 L3 gate application
- **Expand `GRADE_C_FIELDS`** — food_pairing added in Track 8, other fields Sprint 5

### To Sprint 6+ (Productization)

- **B2B API** (S2.9 F11) — REST API with auth + rate limiting + pricing
- **Affiliate link revenue** (S2.9 F10) — depends on staging relink + retailer partnerships
- **Wine.com/Total Wine/K&L partnerships** (S2.9 F30) — business development, not code
- **Monetization paywall/subscription execution** (S2.9 F1) — Sprint 6+ decision + Stripe/Lemonsqueezy/Paddle implementation
- **Localization / i18n** (S2.9 F25) — Sprint 7+ market expansion
- **Mobile native app vs PWA decision** (S2.9 F21) — Sprint 7+, needs demand data from Sprint 3 signal collection
- **Content marketing / press kit / outreach campaign** (S2.9 F15/F22) — Sprint 6+ distribution

### Dropped / not doing

- **Re-run pre-Sprint-5 enrichment on existing corpus** — Sprint 5 regen + L3 gate replaces this; the existing content is a known-quality baseline, not a fix target
- **Dev explorer investment** (S2.7 F14/F15/useEntityDetail) — dev tool is better than consumer per S2.7 meta-pattern 5; Sprint 3 should decommission the dev explorer or mark it "internal — inconsistencies expected," not improve it
- **Vivino archive data re-scrape** — CLAUDE.md open-question; xwines_* is enough
- **OCR bulk label extraction** — tabled 2026-04-07 per CLAUDE.md, correct call

---

## Risk tracking

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Track 2 staging relink has lower than 80% restore rate | medium | high (defines Sprint 3's biggest unlock) | Dry-run first (S3.T2.2); if rate < 80%, extend Sprint 3 by 1 session to investigate edge cases |
| Track 3 grape repair doesn't fully fix the Chardonnay/Pinot Blanc bug (more root causes exist beyond S2.5 F2/F17) | medium | high (Sprint 5 blocker) | Regression sample at S3.T3.8 catches residuals; extend Sprint 3 to find residual root causes |
| Track 4 producer metadata Haiku extraction produces garbage | medium | medium (fixable by falling back to manual top 50) | Manual verification pass at S3.T4.3 before the Haiku run; abort Haiku if top 50 manual validation fails |
| Track 7 L3 gate is too aggressive / too permissive | medium | medium | Calibration pass at S3.T7.3 measures false-positive + false-negative rate; iterate until < 10% FP |
| Sprint 3 budget overruns $50 Sprint 2+3 ceiling | medium | low | F16 evidence shows unit costs are tiny; extend ceiling to $250 at Sprint 3 mid-point with a journal entry |
| Sprint 3 sessions blow past 10 | high | medium | Sprint 3 can close at any milestone with "Sprint 3 done" criteria met; Sprint 4 starts with a fresh backlog |
| Track 5 signal collection attracts no users | high | medium | Signal collection is a floor not a ceiling; 10 wine_lookups is the minimum target, not 1000 |
| Sprint 5 decision to flip ENRICHMENT_ENABLED flag gets deferred indefinitely | medium | high | Sprint 3 exit review (S3.10) must make a recommendation to flip OR define measurable criteria to flip in Sprint 5 |

---

## Pointer to execution

- **Sprint 3 sessions:** open `data/sprints/execute/sessions.json` at Sprint 3 kickoff (create new sprint dir)
- **Sprint 3 prompts:** write per-session prompts to `data/sprints/execute/prompts/s3_N_<track>.md`
- **Sprint 3 journal:** `data/sprints/execute/journal.md`
- **Sprint 3 status:** `data/sprints/execute/status.md` — reference this `synthesis.md` as the primary scope doc
- **Sprint 3 budget:** `data/sprints/execute/budget.json` — start with $0 against the remaining $50 Sprint 2+3 ceiling + $200 extension
- **Sprint 3 opening session:** S3.1 = Track 1 (code/voice hygiene) starting with `describe-chemical` delete (S3.T1.1) as the smallest-possible Sprint 3 win

This synthesis.md is the authoritative Sprint 3 scope document. If priorities change mid-Sprint 3, update this file explicitly with a dated revision note rather than letting the backlog drift.
