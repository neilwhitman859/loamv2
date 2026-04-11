# S2.5 — Code Expert Audit

**Sprint:** 2 (Audit)
**Session:** 5 of ~9 (S2.5)
**Expert hat:** Code — pipeline scripts, shared libs, edge functions, scheduled tasks, conventions, error handling, dead code
**Budget:** $0 expected (Opus inline per ratified S2.3/S2.4 pattern)
**Primary deliverable:** `data/sprints/audit/findings/findings_code.md`

---

## Context

S2.1-S2.4 audited the database layer. S2.5 audits everything that *writes to* or *derives from* that data:

- `pipeline/` — all Python pipeline scripts (fetchers, loaders, promoters, enrichers, analyzers)
- `pipeline/lib/` — shared libraries (db.py, normalize.py, resolve.py, importer.py, merge.py)
- `supabase/functions/` — edge functions (enrich-wine)
- `scripts/` — dashboards, helpers, ad-hoc tools
- `scripts_archive/` — retired Node.js and Python scripts (is it really dead?)
- Scheduled tasks (midnight Riddler, open-meteo drip, data-accuracy-agent)
- Conventions: CLI surface, argparse patterns, logging, error handling, connection handling

Several prior findings point at the code:

- **S2.3 F2 / S2.4 F2** — Chardonnay/Pinot Blanc grape resolver bug. Data root cause identified in S2.4 (polluting PINOT BLANC synonyms). The *resolver logic* that picks wrong on tie is S2.5 scope.
- **S2.2 F1** — 286,918 dangling archive wine_id pointers across 29 of 31 staging tables. The *loaders* that did not rewrite pointers on wine merge are S2.5 scope.
- **S2.2 F-processed-at** — `processed_at` silently never written in 14 of 32 sources. The promoter scripts that forgot to mark rows are S2.5 scope.
- **S2.1 F-search-vector** — `search_vector` missing on 67% of wines / 100% of producers. The *trigger or cron* that should populate it is S2.5 scope.
- **S2.3 F10** — AI content confabulates when inputs are wrong. The *enrichment prompt construction + input validation* is S2.5 scope (enrich-wine edge function).

## Read first

- `CLAUDE.md` — always
- `docs/SCHEMA.md` — field reference, especially merge-tracking columns
- `docs/MERGE_STRATEGY.md` — Python migration rationale, merge layer sequencing
- `data/sprints/audit/findings/findings_db_canonical.md` — S2.1
- `data/sprints/audit/findings/findings_db_staging.md` — S2.2 (F1 especially)
- `data/sprints/audit/findings/findings_wine_canonical.md` — S2.3
- `data/sprints/audit/findings/findings_wine_reference.md` — S2.4
- `data/sprints/audit/status.md` — sprint plan
- `memory/feedback_opus_inline_reasoning.md` — ratified pattern
- `memory/gotchas_pipeline.md`, `memory/gotchas_supabase.md`, `memory/gotchas_ttb.md`, `memory/gotchas_batch_pipeline.md` — known code pitfalls

## Objectives

1. **Inventory** `pipeline/`, `scripts/`, `supabase/functions/`, `scripts_archive/` — what exists, what runs, what's dead
2. **Shared libs audit** — db.py, normalize.py, resolve.py, importer.py, merge.py, any other lib code
3. **Error handling patterns** — connection handling, retries, transaction boundaries, UnicodeDecode handling, Supabase 1K pagination
4. **Promotion/merge script audit** — do loaders update merge-tracking columns? Do promoters respect idempotency? How is staging→canonical linkage maintained when canonical rows merge?
5. **Enrichment pipeline** — edge function code (supabase/functions/enrich-wine/), prompt construction, cost tracking, input validation
6. **Scheduled tasks** — open-meteo drip, midnight agent, anything else hooked to cron or Supabase scheduled functions
7. **Convention consistency** — CLI surface (--dry-run vs --execute vs --commit), argparse patterns, logging, exit codes, resume semantics
8. **Dead code scan** — scripts that reference retired tables, reference obsolete APIs (WV ABCA, Horizon, OFF stale), scripts with no recent modifications and no callers
9. **Cross-reference S2.1-S2.4 data findings to their code root causes** — grape resolver, name cleanup, TTB promotion, staging relink
10. **Write `findings_code.md`** with severity-tagged findings

## Method

- Opus 4.6 inline — no Haiku/Sonnet API calls
- Glob/Grep/Read across pipeline/ and scripts/ — do not execute pipelines
- Read-only — no DDL, DML, fixes, or script runs
- Sample, don't exhaustively enumerate. pipeline/ has ~100+ Python files; focus on hot paths and failure modes
- Reference the gotcha memory files — those already encode known pitfalls; the audit should confirm they are still true and surface new ones

## Severity scale

- **P0** — broken or user-visible correctness issue rooted in code
- **P1** — significant risk, must fix before Sprint 3 runs at scale
- **P2** — improvement, not blocking
- **P3** — nice to have

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

## Exit criteria

- [ ] `findings_code.md` written with severity-tagged findings
- [ ] S2.3 F2 / S2.4 F2 grape resolver code root-cause identified (even if fix stays in data layer)
- [ ] S2.2 F1 staging archive-ID relink code path mapped (where SHOULD the pointer rewrite happen?)
- [ ] `sessions.json` S2.5 entry marked `done` with $0 ai_spend
- [ ] `journal.md` S2.5 section completed
- [ ] `budget.json` S2.5 entry at $0
- [ ] `CLAUDE.md` Current State updated (S2.5 done + finding count)
- [ ] `memory/project_sprint2_findings.md` updated with S2.5 cross-references
- [ ] `data/sessions.md` whiteboard entry moved to Done
- [ ] Commit: `S2.5: Code expert audit — N findings`
