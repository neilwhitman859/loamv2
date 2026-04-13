# Sprint 3 — Fix

**Created:** 2026-04-12 (S2.10)
**Goal:** Clean up the project, correct data bugs, unlock archive data, make pages render structured data correctly.
**Budget:** $0 (no AI spend — pure code, SQL, and file operations)
**Scope:** ~179 of 275 Sprint 2 findings. ~96 deferred to Sprint 4+.
**Authoritative source:** This file. Dashboard tracks progress. Synthesis.md is the audit reference.

---

## Track 1 — Clean house (~1 session)

Delete cruft from earlier iterations. Deduplicate docs so they stop going stale. Structural rule: no hardcoded DB counts in any doc except dashboard.html.

### Steps

**Tables to drop:**
- xwines_wines, xwines_vintages, xwines_producers, xwines_ratings (and any other xwines_* tables)
- 7 unnamespaced temp/helper tables in public schema (S2.1 F22) — identify via `SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE '_tmp%'`
- Drop dead column `wines.lwin` (15 rows populated vs 170K in external_ids) (S2.1 F15)

**Scheduled tasks to delete:**
- Riddler / data-accuracy-agent

**Directories to delete:**
- `git rm -r docs/architecture/ docs/pipelines/` (S2.8 F9)

**Docs to archive (move to docs/reference/):**
- `docs/30K_PLAN.md` (S2.8 F6)
- `docs/PATH_A_ROLLBACK.md` (S2.8 F15)
- `docs/AUDIT_2026-04-01.md` (S2.8 F8)
- `docs/MERGE_STRATEGY.md` (S2.8 F11)
- `docs/BACKLOG.md` (S2.8 F7)

**Docs to delete:**
- `docs/WORKFLOW.md` — superseded by CLAUDE.md session routines
- `docs/ENRICHMENT.md` — contradicts current state (S2.8 F12), enrichment deferred

**Files/dirs to clean:**
- `data/session_prompts/` — keep only cron_loop_template.md, delete the 7 pre-sprint-model files (S2.8 F27)
- `data/stats/` — delete ad-hoc scripts (s23_build_sample.py), old stdout dumps, multi-version snapshots (S2.8 F26)
- `data/stats/loam_roadmap.md` — delete auto-generated dump (S2.8 F31)
- `scripts/fetch_legal_sources_batch{2,3,4,5,8,10}.py` — archive or delete abandoned batch scripts (S2.8 F28)
- `pipeline/vivino/` — move to archive (xwines being deleted) (S2.5 F23)
- Evaluate `scripts/` 11 .py files outside pipeline/ (S2.5 F19)
- Evaluate 3 overlapping dedup scripts: wine_merge.py / dedup_wines.py / seed_strict_dupes.py (S2.5 F21)
- Evaluate batch_matcher.py vs generic_matcher.py (S2.5 F27)
- Evaluate producer-specific scrapers: ridge.py, stags_leap.py, tablas_creek.py (S2.5 F28)
- Delete `LandingPage.tsx` dead code (S2.7 F31)

**Memory files to delete:**
- `30k_status.md` (S2.8 F2 — ships wrong frame)
- `feedback_frontend_pause.md` (frontend unpausing)
- `product-architecture.md` (S2.8 F20 — uses old Tier 0-3)
- `workflow_session_tips.md` (S2.8 F21 — stale)
- `project_sprint_model_and_rf_direction.md` (S2.8 F22 — wrong filename, overlaps dashboard)
- `vivino-pipeline.md` (S2.8 F19 — xwines being deleted)
- `workflow_sprint_session_naming.md` (simple enough, doesn't need a file)
- `project_sprint2_findings.md` (synthesis.md is authoritative)

**Memory files to update:**
- `project_quality_before_enrichment.md` — update to reflect simplified roadmap
- `MEMORY.md` — rewrite index after deletions

**Session whiteboard:**
- Prune `data/sessions.md` — compress S2.1-S2.9 entries to one-line format (S2.8 F18)

**Staging dedup:**
- Fix source_systembolaget and source_lcbo 2x duplication (S2.2 F4)

### Findings addressed
S2.1: F15, F19, F22, F29. S2.2: F4, F8. S2.5: F19-F21, F23, F27-F28. S2.7: F31. S2.8: F1-F2, F5-F9, F15, F18-F22, F26-F28, F32. S2.9: F6 (describe-chemical in Track 5).

### Done when
- xwines_* tables don't exist
- Riddler task doesn't exist
- docs/ has only: DECISIONS, SCHEMA, PRINCIPLES, VOICE, SOURCES, HISTORY, IDENTITY_RULES, reference/
- No hardcoded DB counts in CLAUDE.md
- Memory files reduced from 19 to ~9
- data/session_prompts/ has ≤2 files
- data/stats/ pruned of ad-hoc scripts and dumps

---

## Track 2 — Staging repair + relink (~2 sessions)

Fix staging data quality issues, then relink the 286K dangling pointers and re-promote prices/scores/vintages to canonical tables. Biggest single user-visible improvement.

### Steps

**Fix staging data quality first:**
1. Fix 93,407 malformed ABV values in source_ttb_colas (S2.2 F6)
2. Fix 802 mojibake rows in source_texsom (S2.2 F7)
3. Fix 10,208 duplicate TTB IDs in source_wv_abca (S2.2 F16)
4. Fix 6,886 duplicate COLA numbers in source_kansas_brands (S2.2 F17)
5. Add canonical_wine_id indexes to 14 staging tables that lack them (S2.2 F11)
6. Add processed_at writes to the 14 sources that never set it (S2.2 F2)
7. Standardize natural key uniqueness across sources (S2.2 F18)
8. Add updated_at column to 26 staging tables that lack it (S2.2 F21)

**Relink:**
9. Extend `relink_staging_to_current.py::STAGING_TABLES_WINE` from 1 to 30 entries (S2.5 F3)
10. Execute relink — target: restore >80% of 286K dangling pointers (S2.2 F1)

**Re-promote:**
11. Re-run price promotion: Spec's (S2.2 F12), Utah DABS (S2.2 F20), retail_promote expansion to 14 sources (S2.2 F19)
12. Re-run score promotion from relinked staging
13. Re-run vintage/ABV promotion from relinked staging

**Schema cleanup:**
14. Fix staging schema convention drift (S2.2 F10, F29, F30)
15. Clean horizon source (dead API, ABV/price unpopulated) — mark archival (S2.2 F26)
16. Clean TABC source (no grape/appellation structure) — note limitation (S2.2 F27)

### Findings addressed
S2.1: F6, F12. S2.2: F1-F2, F6-F7, F10-F12, F16-F21, F26-F27, F29-F31. S2.5: F3.

### Done when
- `SELECT count(distinct wine_id) FROM public.wine_vintage_prices` > 20,000 (from 2,818)
- Price coverage > 12% of active wines
- Dangling staging wine_id pointers < 10,000 (from 286,918)
- All 30 staging tables in STAGING_TABLES_WINE
- All staging tables have canonical_wine_id index

---

## Track 3 — Grape + data repair (~3 sessions)

Fix the Chardonnay/Pinot Blanc compound bug plus all related grape, wine, and reference data corrections. This is the largest track.

### Sub-track 3A: Grape compound fix

1. Delete PINOT BLANC's 4 polluting synonyms (S2.4 F2)
2. Fix varietal_categories 5+ wrong-grape links (S2.4 F1)
3. Fix 921 synonym-primary name collisions (S2.1 F7 / S2.4 F7)
4. Fix self-parent grape MALEGUE 742-22 (S2.1 F27)
5. Fix GARRO grape link — real grape, wrong wine linkage (S2.4 F8 correcting S2.3 F7)
6. Fix grape display name inversions: VERDOT PETIT → Petit Verdot, etc. (S2.3 F9 / S2.4 F15)
7. Fix grapes.name cépage suffix form where display_name is NULL (S2.4 F6)

### Sub-track 3B: Pipeline grape code fixes

8. Consolidate 4 duplicate grape resolver implementations onto `ReferenceResolver.resolve_grape()` (S2.5 F11)
9. Fix `batch_pipeline._match_ttb_to_wine` multi-COLA collapse (S2.5 F2)
10. Fix `ttb_grape_promote` DISTINCT ON arbitrary pick (S2.5 F17)
11. Fix `grape_from_name` NULL display_name collapse (S2.5 F6)
12. Fix `haiku_grape_extract` 70% containment overmatch (S2.5 F7)
13. Fix `grape_from_name` dict collision (last-write-wins) (S2.5 F12)
14. Fix `batch_pipeline._load_reference_data` synonym blending (S2.5 F9)
15. Fix `batch_pipeline` hardcoded BATCH_0_PRODUCERS roster (S2.5 F8)

### Sub-track 3C: Wine data corrections

16. Re-run grape resolver on affected wines — expect ~2,700 Chardonnay wines to fix (S2.3 F2)
17. Backfill display_name on 50,908 LWIN long-tail wines (S2.5 F18)
18. Fix 8 marquee wines: DRC/Lafite wrong color, Opus One/Dom Perignon/Screaming Eagle NULL display_name, Grange wrong match, Hill of Grace wrong match (S2.3 F1)
19. Fix Domaine Leroy region = Beaujolais → Burgundy (S2.3 F4)
20. Fix Catena / Alta Vista producer collision (S2.3 F5)
21. Fix wrong colors on clearly-typed wines (S2.3 F6)
22. Fix wrong grape links on varietal wines (S2.3 F8)
23. Investigate unverified wine names/SKUs (S2.3 F15)
24. Fix producer misattribution patterns beyond Catena (S2.3 F16)
25. Fix 1,647 fortified wines with NULL color (S2.1 F21)
26. Fix 2 vintage_year outliers (1085, 2099) (S2.1 F20)
27. Delete 30 Grade F wines that have wine_insights (shouldn't exist) (S2.1 F20)
28. Fix 57 external_ids pointing to soft-deleted wines (S2.1 F33)
29. Fix 1,265 wines parked on catch-all regions (S2.1 F32)
30. Delete 46 producers with zero wines (S2.1 F19)
31. Fix sample builder ORDER BY bug (S2.3 F22)
32. Fix 3,534 true-duplicate wine groups (S2.1 F1) — evaluate scope, may be partial
33. Clean 23,987 shared COLA IDs across multiple wines (S2.1 F13) — evaluate scope
34. Reconcile dual LWIN systems (lwin vs lwin_7 in external_ids, 10,499 overlap) (S2.1 F14)

### Sub-track 3D: Reference data corrections

35. NULL out 345 fake 1973 established_years (S2.4 F9)
36. Fix 121 slash-concatenated appellation aliases (S2.4 F3)
37. Restore French AOC diacritics (S2.4 F4)
38. Fix Pauillac 1855 classification tier counts (S2.4 F5)
39. Fix appellation_grapes Spanish→French name issues (S2.4 F14)
40. Add missing accessory grapes to Chambertin/Charmes/Bonnes-Mares (S2.4 F16)
41. Add provenance columns to appellation_soils (S2.4 F17)
42. Fix Hunter Valley Basalt link (S2.4 F18)
43. Delete junk "Ite" soil type (S2.4 F19)
44. Mark unverified TTB appellation_rules (S2.4 F21)
45. Fix Margaux duplicate commune (S2.4 F23)
46. Fix Napa Valley grape rule: add California 100% county requirement (S2.4 F24)
47. Fix empty Bulgarian slug (S2.1 F23)
48. Fix 46 label_designation_rules NULL appellation (S2.1 F18)
49. Fix 2,526 appellations with zero wines — verify if real or data quality issue (S2.1 F17)
50. Restore wine_food_pairings from archive (S2.6 F8)
51. Delete 487 confabulated Chardonnay/Pinot Blanc wine_insights (S2.6 F4)

### Sub-track 3E: Indexes + rollups

52. Add wine_grapes.grape_id index (S2.1 F25)
53. Regenerate search_vector on wines and producers (S2.1 F3)
54. Compute critic_score_avg / critic_score_count rollups (S2.1 F10)

### Findings addressed
S2.1: F1, F3, F7, F9-F10, F13-F15, F17-F21, F23, F25, F27, F32-F33. S2.3: F1-F2, F4-F9, F15-F16, F22. S2.4: F1-F9, F14-F19, F21, F23-F24. S2.5: F2, F6-F9, F11-F12, F17-F18. S2.6: F4, F8.

### Done when
- Chardonnay+Pinot Blanc false-positive ≤ 50 (from 2,743)
- display_name NULL < 55K (from 104K)
- search_vector coverage > 60% (from 32%)
- No 1973 established_years
- No wrong-grape links in varietal_categories
- Domaine Leroy region = Burgundy
- wine_food_pairings > 800 rows (from 0)
- 487 confabulated wine_insights deleted

---

## Track 4 — UI hygiene (~1-2 sessions)

Fix all frontend rendering bugs so pages display structured data correctly.

### Steps

**P0 fixes:**
1. Fix CountryPage.tsx:40 column typo: `ai_signature_grapes` → `ai_signature_styles` (S2.7 F2)
2. Add display_name fallback chain to WinePage.tsx (S2.7 F1)
3. Fix footer /about link (S2.7 F7) — route to static page or remove link
4. Park/delete /vineyard/:id route (S2.7 F8)
5. Add error boundary in main.tsx + .catch() on consumer page fetches (S2.7 F9)
6. Add 404 catch-all route (S2.7 F10)
7. Add AI disclaimer component on ai_* fields (S2.7 F5) — simple "AI-generated" label

**P1 fixes:**
8. Remove producer_insights query from Dashboard (S2.7 F11)
9. Fix wineCount inflation — filter out F-grade on entity pages (S2.7 F12)
10. Make producer website URL clickable (S2.7 F14)
11. Fix dev WineDetail grapes.name → display_name (S2.7 F15)
12. Render 8 dead-fetch ai_* fields: AppellationPage (3), CountryPage (2), RegionPage (1), GrapePage (2) (S2.7 F16-F19)
13. A11y baseline: aria-current on nav, aria-live on loading, role on landmarks (S2.7 F20)
14. Fix heading hierarchy h1→h2 (not h1→h3) (S2.7 F21)
15. Show food pairing section when data exists (S2.7 F22)
16. Render classification system_name alongside level_name (S2.7 F23)

**P2 fixes:**
17. Section component conditional render — hide when FactGrid empty (S2.7 F24)
18. Clean EntityMap boundary_source display (S2.7 F25)
19. Consolidate Section/Tag/Fact/Loading/NotFound primitives (~500 LOC dedup) (S2.7 F27)
20. Cache Dashboard count queries or use a view (S2.7 F28)
21. Remove HomePage autoFocus (S2.7 F29)
22. Add build timestamp to ConsumerLayout (S2.7 F32)

### Voice cleanup in existing content (no re-generation):
23. Identify and flag "elegant"/"showcases" density in existing region_insights (S2.6 F12/F18)
24. Identify and flag "marry/marries" in existing content (S2.6 F13)
25. Flag hyperbolic language in country_insights (S2.6 F23)
26. Flag formulaic food pairing patterns (S2.6 F19/F27)
27. Add BANNED_WORDS validation to existing content scan (S2.6 F24)
28. Log voice scan results for Sprint 4 voice module input (S2.6 F28/F29)

### Findings addressed
S2.7: F1-F2, F5, F7-F12, F14-F25, F27-F29, F31-F32. S2.6: F10, F12-F13, F18-F19, F23-F24, F27-F29.

### Done when
- CountryPage renders ai_overview on Italy/France/USA
- WinePage never renders empty h1
- Error boundary catches deliberate 500
- 8 ai_* fields visible on entity pages
- Heading hierarchy is h1→h2
- Lighthouse a11y score > 85
- No dead components in consumer pages

---

## Track 5 — Edge function + code cleanup (~1 session)

Delete rogue edge function, vendor source code, centralize configuration, clean up pipeline code.

### Steps

1. Delete `describe-chemical` edge function (S2.5 F1, S2.9 F6)
2. Vendor `enrich-wine` source into `supabase/functions/enrich-wine/` (S2.5 F31)
3. Fix `enrich-wine` to read `grapes.display_name` not `grapes.name` (S2.5 F4)
4. Create `pipeline/lib/models.py` with HAIKU/SONNET/OPUS constants (S2.5 F5)
5. Grep-and-replace hardcoded model IDs across pipeline/ (S2.5 F5)
6. Fix stale model ID references in reference enrichment scripts (S2.6 F10)
7. Clean up `sys.path.insert` usage across pipeline (S2.5 F15) — evaluate, may be partial
8. Consolidate 13 `INSERT INTO wines` call sites — add shared helper or document convention (S2.5 F16)
9. Create `supabase/migrations/` directory for DDL tracking (S2.5 F24)
10. Clean up CLAUDE.md Riddler reference (S2.5 F20)

### Findings addressed
S2.5: F1, F4-F5, F15-F16, F20, F24, F31. S2.6: F10. S2.9: F6.

### Done when
- `list_edge_functions` does not return `describe-chemical`
- `supabase/functions/enrich-wine/index.ts` exists in git
- `pipeline/lib/models.py` exists and is imported by all scripts that use Anthropic
- ENRICHMENT_ENABLED flag confirmed still OFF

---

## Track 6 — Doc hygiene (~1 session)

Fix all remaining doc drift. Must be LAST track — update docs to reflect the post-fix state of everything else.

### Steps

1. Rewrite CLAUDE.md — strip all hardcoded DB counts, point at dashboard (S2.8 F1, F4, F10, F23, F30)
2. Fix docs/SOURCES.md external_ids section: `id_type` → `system`, `ttb_cola` → `cola`, add `lwin_7` (S2.8 F3)
3. Update docs/SOURCES.md last-updated header (S2.8 F13)
4. Add IDENTITY_RULES.md to CLAUDE.md doc index (S2.8 F16)
5. Update docs/SCHEMA.md drift points (S2.1 F24)
6. Update loam_roadmap.json Sprint 2 sub_tasks (S2.8 F5)
7. Rewrite MEMORY.md index after memory consolidation (S2.8 F32)
8. Fix CLAUDE.md "Reference Tables (complete)" heading — add "(audited, corrections pending)" (S2.8 F30)

### Business-layer doc items:
9. Define ICP in CLAUDE.md or a new doc (S2.9 F4) — "enthusiast + beverage director"
10. Note legal/licensing status of scraped sources (S2.9 F18)
11. Note the "terroir positioning vs data reality" gap honestly (S2.9 F5)
12. Fix CLAUDE.md product description to avoid "wine intelligence" collision (S2.9 F13/F29)
13. Note Sprint 5 done criterion (S2.9 F27)
14. Note enrichment cost is decoupled from revenue — acknowledged (S2.9 F17)
15. Note competitive parity honestly — winning on 4, losing on 6 (S2.9 F16/F20)

### Findings addressed
S2.1: F24, F28. S2.8: F1, F3-F5, F10, F12-F13, F16, F23, F30, F32. S2.9: F4-F5, F8-F9, F12-F14, F16-F20, F22, F26-F29.

### Done when
- Zero hardcoded DB counts in CLAUDE.md
- docs/SOURCES.md external_ids section matches live schema
- MEMORY.md has ≤ 12 entries, all < 150 chars
- CLAUDE.md product description doesn't say "wine intelligence platform"

---

## Execution order

Tracks can partially overlap but dependencies constrain the sequence:

1. **Track 1** (Clean house) — first, clears debris
2. **Track 5** (Code cleanup) — second, fixes pipeline code that Track 3 depends on
3. **Track 2** (Staging relink) — can run in parallel with Track 3
4. **Track 3** (Data repair) — depends on Track 5 (resolver consolidation)
5. **Track 4** (UI hygiene) — after Track 3 (pages show correct data)
6. **Track 6** (Doc hygiene) — LAST (docs reflect final state)

Within a conversation, auto-continue between tracks. Write a handoff note to the dashboard before each transition so compaction doesn't lose context.

---

## What Sprint 3 deliberately defers (~96 findings)

### To Sprint 4 (Deepen)
- Producer metadata seeding ($50-100)
- Voice module consolidation (pipeline/lib/voice.py)
- L3 fact-check gate build
- AI safety rail (AIBadge component, /about, /known-issues)
- Signal collection (landing page, email signup, wine_lookups)
- Food pairings Grade C field widening
- appellation_rules JSONB schema redesign (S2.4 F11-F13)
- appellation_grapes provenance cleanup (S2.4 F16)
- classification_level coverage beyond German einzellage (S2.4 F10)
- DECISIONS.md archive strategy (S2.8 F17)
- VOICE.md Never-Invent section (S2.8 F14)
- HISTORY.md TOC (S2.8 F29)

### To Sprint 5 (Enrich)
- Regenerate 49 contaminated appellation_insights (S2.6 F5/F21)
- European appellation coverage (S2.6 F9)
- Wine-layer content regeneration
- L3 gate application to existing wine_insights
- AI prose voice quality improvements (S2.6 F1-F3, F14-F17, F22)
- Contamination feedback loop fix — reference-first regen (S2.6 F5)

### To Sprint 6+ (Productize)
- Monetization model (S2.9 F1)
- B2B API (S2.9 F11)
- Affiliate revenue architecture (S2.9 F10)
- Localization (S2.9 F25)
- PWA mobile research (S2.9 F21)
- Content marketing (S2.9 F15/F22)
- Wine.com/Total Wine partnerships (S2.9 F30)
