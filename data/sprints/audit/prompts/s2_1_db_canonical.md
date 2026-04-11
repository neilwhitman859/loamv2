# S2.1 — Sprint 2 Open + DB Canonical Audit

**Sprint:** 2 (Audit)
**Session:** 1 of ~9-11 (S2.1)
**Expert hat:** DB — canonical and reference tables (structural + coverage + integrity)
**Budget:** ~$0 expected (SQL-driven, no AI calls needed)
**Primary deliverable:** `data/sprints/audit/findings/findings_db_canonical.md`
**Secondary deliverable:** `data/sprints/audit/` directory fully stood up, `current.json` updated, Sprint 2 active

---

## Context

- **Sprint 1 (30K Plan) closed** at end of S1.14 on 2026-04-11. Final state: 155,623 wines, 10,676 producers, Josh Test 84%, budget $23.33/$175. Archive at `data/sprints/_archive/30k/`.
- **Sprint 2 (Audit) is a multi-expert read-only audit** designed to produce a prioritized Sprint 3 fix backlog. The thesis: previous enrichment attempts failed because the foundation wasn't ready. Fix foundation before running heavy enrichment.
- **Plan:** ~9-11 sessions across DB, Wine, Code, Voice, UX, Meta, and Business experts. Target spend $15-25, ceiling $25 ($50 combined ceiling across Sprints 2+3).
- **Wine expert bar = working sommelier.** Reference tables are a through-line but NOT the sole focus — wines and producers get equal weight.
- **S2.1 = canonical + reference (structural).** S2.2 = staging tables. Don't confuse the two — this session stays out of `source_*` tables entirely.
- **READ-ONLY.** No fixes this sprint. Everything broken becomes a finding, not a patch.
- **Session naming:** `S{sprint}.{session}`. This is S2.1. Overflow sessions become S2.1.5, etc.

## Read first

Load these before starting work:

- `CLAUDE.md` — always
- `docs/SCHEMA.md` — table reference (critical for this session)
- `docs/DECISIONS.md` — data model history and rationale
- `docs/PRINCIPLES.md` — product philosophy (informs severity judgments)
- Memory: `project_quality_before_enrichment.md` and `workflow_sprint_session_naming.md`
- `data/sprints/_archive/30k/meta.json` — confirm what just closed
- `data/sprints/current.json` — current state (`between_sprints`, pointing at stale `reference_first` — this session renames it to `audit`)

## Objectives

1. **Open Sprint 2** by creating `data/sprints/audit/` and updating `current.json`
2. **Run the DB canonical + reference structural audit** across the scope in Phase B
3. **Produce `findings_db_canonical.md`** with severity-tagged findings (P0-P3), each with evidence + proposed fix
4. **Update session/budget/journal state** and commit
5. **Verify the dashboard** renders Sprint 2 as active (`powershell -File scripts/dash.ps1`)

---

## Phase A — Open Sprint 2

### A1. Create the sprint directory

```
data/sprints/audit/
├── meta.json
├── sessions.json
├── budget.json
├── journal.md
├── status.md
├── prompts/
│   └── s2_1_db_canonical.md  (moved from data/session_prompts/ at end of session)
└── findings/
```

### A2. `meta.json` template

```json
{
  "name": "audit",
  "display_name": "Sprint 2 — Audit",
  "sprint_number": 2,
  "started": "<today ISO>",
  "status": "active",
  "goal": "Multi-expert read-only audit (DB, wine, code, voice, UX, meta, business) to produce a prioritized Sprint 3 fix backlog. Get Loam in a better place before going heavy on enrichment. Reference tables are a through-line; wines and producers get equal weight.",
  "budget_total": 25.00,
  "budget_target": 15.00,
  "expert_bar": {
    "wine": "working sommelier — a certified somm reading a Loam page should not find factual errors or simplistic claims"
  },
  "discipline": [
    "Read-only (no fixes this sprint, all fixes go to Sprint 3)",
    "Severity-tagged findings (P0-P3)",
    "Flexible session order (overflow to S2.X.5 if needed)",
    "Recalibrate after each session if findings are significantly worse or better than expected",
    "Budget overages justified in journal before spending"
  ],
  "primary_deliverable": "Prioritized Sprint 3 fix backlog in findings/synthesis.md (written in S2.9)",
  "sprint_file_index": {
    "sessions": "sessions.json",
    "budget": "budget.json",
    "journal": "journal.md",
    "status": "status.md",
    "prompts_dir": "prompts/",
    "findings_dir": "findings/"
  }
}
```

### A3. `sessions.json` template

```json
{
  "sessions": [
    {"id": 1, "name": "Sprint 2 open + DB canonical audit", "expert": "db_canonical", "status": "in_progress", "date": "<today>", "ai_spend": null, "notes": "Creates data/sprints/audit/, runs DB canonical + reference structural audit"},
    {"id": 2, "name": "DB staging audit", "expert": "db_staging", "status": "not_started", "date": null, "ai_spend": null, "notes": "All 32 staging tables: merge state, processed_at distribution, value-left-on-table"},
    {"id": 3, "name": "Wine expert — canonical sample", "expert": "wine_canonical", "status": "not_started", "date": null, "ai_spend": null, "notes": "100-wine + 50-producer stratified sample, Sonnet fact-checking, sommelier bar — main AI spend (~$15-18)"},
    {"id": 4, "name": "Wine expert — reference content", "expert": "wine_reference", "status": "not_started", "date": null, "ai_spend": null, "notes": "appellation_rules, appellation_grapes, grape synonyms, categorizations, soils — hand-verified content correctness"},
    {"id": 5, "name": "Code expert audit", "expert": "code", "status": "not_started", "date": null, "ai_spend": null, "notes": "pipeline/ scripts, edge function, shared libs, scheduled tasks, conventions, error handling, dead code"},
    {"id": 6, "name": "Voice / editorial audit", "expert": "voice", "status": "not_started", "date": null, "ai_spend": null, "notes": "2 enriched wines + enrichment prompts + existing insight tables (346 rows) vs VOICE.md"},
    {"id": 7, "name": "UX / frontend audit", "expert": "ux", "status": "not_started", "date": null, "ai_spend": null, "notes": "Every page type, data-to-UI integrity, empty state handling"},
    {"id": 8, "name": "Meta audit", "expert": "meta", "status": "not_started", "date": null, "ai_spend": null, "notes": "docs/*.md, CLAUDE.md, memory files, loam_roadmap.json, scheduled infra"},
    {"id": 9, "name": "Business + synthesis + Sprint 3 backlog", "expert": "business", "status": "not_started", "date": null, "ai_spend": null, "notes": "Competitive positioning, monetization, value prop. Dedupe findings across experts, produce Sprint 3 backlog."}
  ]
}
```

Note: order is flexible. If during Sprint 2 we want to reorder (e.g., run UX earlier because it informs code scope), just update the session rows — the plan is not locked.

### A4. `budget.json` template

```json
{
  "total_budget": 25.00,
  "total_spent": 0.00,
  "target_spent": 15.00,
  "ceiling": 25.00,
  "by_session": {},
  "by_model": {},
  "closed": false,
  "notes": "Part of a $50 combined ceiling across Sprints 2+3. Main AI spend expected in S2.3 (wine expert fact-checking) — ~$15-18 for 100-wine + 50-producer stratified sample with Sonnet validation. Other sessions should be ~$0. Any overage above $15 per sprint gets justified in the journal before spending."
}
```

### A5. `status.md`

Write a ~1 page sprint plan summarizing the locked strategy. Include:
- Thesis (quality before enrichment)
- 9-session table of experts and scope
- Methodology (sequential expert-hat passes, read-only, severity-tagged)
- Budget framing
- Flexibility clauses (S2.X.5 overflow, recalibrate after each session)
- Primary deliverable (Sprint 3 backlog)
- Link to this prompt

### A6. `journal.md`

Scaffold with `# Sprint 2 — Audit Journal` and a `## S2.1 — <date>` section where this session's work gets logged.

### A7. Update `data/sprints/current.json`

Replace the `between_sprints` state with:

```json
{
  "name": "audit",
  "status": "active",
  "sprint_number": 2,
  "previous": {
    "name": "30k",
    "closed_at": "2026-04-11",
    "archive_path": "data/sprints/_archive/30k/"
  },
  "next": {
    "name": "execute",
    "status": "planned",
    "notes": "Sprint 3 — execute prioritized fixes from Sprint 2's backlog. Scope depends on findings; size TBD."
  },
  "notes": "Sprint 2 (Audit) active. ~9-session multi-expert read-only audit producing Sprint 3 backlog. Reference redesign and enrichment execution deferred to Sprints 4 and 5 per strategy locked in S1.14."
}
```

### A8. Update `data/stats/loam_roadmap.json`

Read the current file to see its shape, then update the phase structure to reflect the new sprint order:

- Sprint 1 (30K Plan) — done
- Sprint 2 (Audit) — active
- Sprint 3 (Execute) — planned
- Sprint 4 (Reference Design) — planned
- Sprint 5 (Reference Enrichment) — planned, scope TBD after Sprint 4

Don't break `python -m pipeline.analyze.loam_roadmap`.

### A9. Memory cleanup

Edit `~/.claude/projects/C--Users-neilw-Documents-GitHub-loamv2/memory/project_sprint_model_and_rf_direction.md` so it no longer claims Sprint 2 = Reference-First. Either merge its still-relevant content into `project_quality_before_enrichment.md` and delete it, or rewrite it to simply point at the superseding memory. Keep `MEMORY.md` index consistent.

### A10. Move this prompt into the sprint

Move `data/session_prompts/s2_1_db_canonical.md` → `data/sprints/audit/prompts/s2_1_db_canonical.md`.

### A11. Verify the dashboard

Run `powershell -File scripts/dash.ps1` — should now show Sprint 2 as active with S2.1 in progress. Run `powershell -File scripts/dash.ps1 -All` to confirm Sprint 2 appears in the overview.

---

## Phase B — DB Canonical Audit

Once Phase A is done, run the audit. If Phase A consumed significant context, consider wrapping S2.1 here and opening S2.1.5 for Phase B.

### B0. First queries (ground truth)

Query the DB for live counts before trusting any doc or memory number:

```sql
SELECT count(*) FROM wines WHERE deleted_at IS NULL;
SELECT count(*) FROM producers WHERE deleted_at IS NULL;
SELECT count(*) FROM wine_vintages;
SELECT data_grade, count(*) FROM wines WHERE deleted_at IS NULL GROUP BY data_grade ORDER BY data_grade;
```

Use `get_conn()` from `pipeline/lib/db.py` (direct Postgres via session pooler) for big queries. Supabase MCP `execute_sql` is fine for small/single queries.

### B1. Core canonical

**`wines` (≈155,623 active):**
- Grade distribution (B/C/D/F), null grade check
- Coverage gaps by grade: color, country_id, region_id, appellation_id, producer_id (NULL counts)
- `external_ids` by system: LWIN, lwin_7, COLA, UPC, QR — counts, duplicates within system
- Near-duplicate detection: same `producer_id` + same `name_normalized` across different `wine_id`s
- Orphaned rows: wines without `producer_id`, wines without any external_id, wines with producer_id pointing to deleted producer
- `data_grade` correctness: do wines with `wine_insights` rows actually have Grade B/C? Do F wines have anything beyond identity?
- Name field sanity: `name`, `name_normalized`, `display_name` — NULLs, anomalies, Unicode corruption (U+FFFD, accent mojibake)
- `wine_type` distribution (red/white/rosé/sparkling/fortified) vs `color` consistency
- Soft-deleted count: `deleted_at IS NOT NULL` — sanity on historical merges

**`producers` (≈10,676):**
- Metadata completeness by field: country_id, region_id, website, year_established, winemaker_id
- Duplicate/near-duplicate detection: same name or fuzzy-matched across different producer_ids, filter out known parent/child
- `producer_aliases` join integrity: orphans, duplicates
- Parent/child relationships: cycles, self-references, orphaned child with missing parent
- Farming/biodiversity certification linkage: how many producers have at least one cert? How many cert assignments are orphaned?
- Producers with 0 wines (known pattern: LWIN no-match grocery brands) — count and categorize
- Slug collision handling: any duplicate slugs?

**`wine_vintages`, `wine_grapes`, `wine_vintage_scores`, `wine_vintage_prices`:**
- Sparsity: per-wine averages, overall coverage %
- FK normalization: `wine_vintage_id` adoption vs legacy `wine_id + vintage_year` — % backfilled, orphans on either path
- Vintage year sanity: out-of-range (< 1800 or > 2027), NULL handling, NV convention correctness
- Chemistry sanity (where populated): ABV / pH / TA / RS / VA / SO2 — impossible values (negative, > 100, etc.)
- Grape percentage sanity: wines where grapes sum to > 100% or < 0% (the S1.14 W6 fix handled a big batch; verify clean state)
- `wine_vintage_scores` source distribution — who scored what
- `wine_vintage_prices` currency distribution, outliers (suspiciously high or $0)

**`external_ids`:**
- System distribution and totals
- Orphans: external_ids pointing to missing wine_ids
- Duplicates within system (same system + same value for different wines — shouldn't happen for LWIN/COLA)
- Format sanity per system (LWIN 7-digit, COLA 15-digit, UPC 12-13-digit)

### B2. Reference layer — structural only

For each table below, check: row count, FK integrity, orphan check, duplicate/near-duplicate detection, `updated_at` staleness, unused indexes. **DO NOT audit content correctness** (that's S2.4, wine expert reference content).

- `countries` (68), `regions` (389), `appellations` (3,662)
- `grapes` (9,693), `grape_synonyms` (34,820), `varietal_categories` (161)
- `appellation_rules` (1,165), `appellation_grapes` (10,414), `appellation_vintages` (134,877), `appellation_soils` (930)
- `label_designations`, `tasting_descriptors` (304), `farming_certifications` (21), `biodiversity_certifications` (7), `soil_types` (39)
- `region_aliases`, `appellation_aliases`, `label_designation_aliases`

Key structural questions:
- Does every appellation have at least a country and a region (or is it parked on a catch-all)?
- Does every region belong to a country?
- Are grape parent/child relationships a DAG (no cycles)?
- Are synonyms bidirectional or one-way?
- Do `appellation_rules` FKs into appellations all resolve?
- Any duplicate appellation names within the same region?
- `appellation_vintages` temporal coverage per appellation (2,997 appellations × 45 years expected)

### B3. Join path traversal

Pick representative wines at each grade and traverse the full graph from wine → producer, vintage → scores/prices/grapes, appellation → rules/grapes/vintages/soils. Note where the traversal breaks.

- Does `wine_detail_view` return complete data for sample wines? Where are the NULLs that should not be NULLs?
- Does `producer_detail_view` surface farming certs, aliases, parent/child?
- Does `wine_vintage_detail_view` join all the chemistry fields?
- `wine_search_view` — is the tsvector populated and correct?
- Any missing FK indexes causing slow joins on 155K wines?
- RLS enabled on all 94+ canonical tables? (Previously confirmed, re-verify — schema may have drifted.)

### B4. Empty canonical tables

Query `information_schema` or `pg_class` to find all canonical tables with `n_live_tup = 0`. For each:
- Is it intentional (scaffolded for future work) or accidental (should have data but doesn't)?
- One-line rationale per table
- Severity of the emptiness: P0 (should have data for user-visible features), P2 (scaffolded, not yet populated), P3 (deprecated or aspirational)

Known empties at last check (verify and expand):
- `vineyards` (rebuilt in 30K, archive_vineyards preserved with 815 rows)
- `wine_vintage_descriptors`, `wine_vintage_nv_components`
- `producer_timeline`, `water_bodies`
- Several insight tables (`wine_insights` has 5,108 rows, but most others are 0)

### B5. Discipline

- **READ-ONLY.** No DDL, no DML, no pipeline runs, no fixes.
- Use TodoWrite to track progress through B1→B4.
- Flag everything that looks off. Tag severity honestly — don't sandbag for optics.
- Each finding needs concrete evidence (SQL query + result summary) and a proposed fix.
- No AI calls expected — this is a SQL session.
- If you find something that threatens Sprint 2 or Sprint 3 scope (e.g., a structural rewrite is needed before any fix makes sense), stop and flag to the user immediately per the recalibration clause.

### B6. Findings format

Write to `data/sprints/audit/findings/findings_db_canonical.md`. Structure:

```markdown
# DB Canonical Audit — Findings

**Session:** S2.1
**Date:** <date>
**Expert:** db_canonical
**Scope:** canonical wines/producers/vintages/grapes/scores/prices/external_ids + reference layer structural + join paths + empty table categorization

## Summary

- Total findings: N
- P0: n · P1: n · P2: n · P3: n
- Biggest risks: ...
- Biggest wins (things that are correct and didn't need flagging): ...

## Findings

## F1 — <short title>

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT ...
    ```
    Result: ...
- **Why it matters:** ...
- **Proposed fix:** ...
- **Effort:** medium
- **Dependencies:** none
- **Related findings:** —

## F2 — ...
```

**Severity scale:**
- **P0** — broken or incorrect, user-visible or correctness-critical
- **P1** — significant gap or risk, must fix before enrichment
- **P2** — improvement, not blocking
- **P3** — nice to have, park for later

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

---

## Exit criteria

- [ ] `data/sprints/audit/` directory created with meta, sessions, budget, journal, status, prompts/, findings/
- [ ] `data/sprints/current.json` updated (renamed `reference_first` → `audit`, status `active`)
- [ ] `data/stats/loam_roadmap.json` reflects new sprint sequence
- [ ] Memory updated (`project_sprint_model_and_rf_direction.md` superseded or rewritten)
- [ ] `findings_db_canonical.md` written with findings covering all B1-B4 categories (target: ≥ 20 findings; fewer is fine if the audit is clean, more is fine if the data is rough)
- [ ] `sessions.json` S2.1 entry marked `done` with date + ai_spend
- [ ] `journal.md` S2.1 section written with summary of work + findings count + anything that needs escalation
- [ ] This prompt moved to `data/sprints/audit/prompts/s2_1_db_canonical.md`
- [ ] `CLAUDE.md` "Current State" section updated (Sprint 1 → Sprint 2 transition)
- [ ] `data/sessions.md` whiteboard updated
- [ ] Dashboard verified: `powershell -File scripts/dash.ps1` shows Sprint 2 active with S2.1 done
- [ ] Commit: `S2.1: Sprint 2 open + DB canonical audit — N findings`

## Starting moves

1. Read CLAUDE.md, docs/SCHEMA.md, docs/DECISIONS.md, docs/PRINCIPLES.md, memory files, `_archive/30k/meta.json`, `current.json`
2. TodoWrite the Phase A steps (A1-A11)
3. Execute Phase A sequentially
4. Run `powershell -File scripts/dash.ps1` to verify Sprint 2 is active
5. TodoWrite the Phase B scope categories (B1-B4)
6. Execute Phase B: SQL queries via `get_conn()`, capture findings as you go
7. Write `findings_db_canonical.md` with the severity-tagged format above
8. Wrap up per exit criteria checklist
9. Commit

---

## Notes for the agent running S2.1

- **Don't fix things.** You will find broken stuff. Log it as a finding, move on. Fixes are Sprint 3.
- **Sample, don't exhaustively enumerate.** 155K wines is too many to look at one by one. Query distributions and aggregates. Pull 10-20 specific examples for each anomaly class.
- **Trust the DB, not the docs.** CLAUDE.md and memory have point-in-time snapshots that may be stale. Your queries are ground truth.
- **Flag severity honestly.** If something is P0, call it P0. Don't soften to P1 because "we'll get to it eventually."
- **Stop and escalate if you find a scope-breaker.** If the audit reveals a structural issue that would require rewriting Sprint 3, tell the user immediately. Recalibration is a feature, not a failure.
- **S2.1 can split.** If Phase A + Phase B is too much for one session, wrap after Phase A and open S2.1.5 for Phase B. The sessions.json pre-lists S2.1 only — add S2.1.5 if you split.
- **Frontend is paused but in audit scope.** S2.7 handles frontend. Don't touch it this session.
- **No parallelism.** No background tasks, no parallel subagents, no cron loops. Sequential SQL.
- **Budget should be $0 or near.** If you find yourself wanting AI help to classify something, ask the user first — budget overages need pre-justification.

Good luck. Produce a clear, honest, severity-tagged findings file that Sprint 3 can execute from.
