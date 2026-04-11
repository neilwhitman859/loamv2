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
