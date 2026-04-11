# S2.8 — Meta Audit

**Sprint:** 2 (Audit)
**Session:** 8 of ~9 (S2.8)
**Expert hat:** Meta — docs, memory, CLAUDE.md, roadmap, sprint infra, scheduled tasks
**Budget:** $0 expected (Opus inline per ratified S2.3–S2.7 pattern)
**Primary deliverable:** `data/sprints/audit/findings/findings_meta.md`

---

## Context

S2.1–S2.7 audited the data layer, code layer, content layer, and UI layer. S2.8 is the meta layer — the docs, memory, roadmap, and sprint infrastructure that every other audit stands on. Every session starts by reading CLAUDE.md and memory files. Every sprint dashboard reads `loam_roadmap.json` and `current.json`. Every finding ends up cross-referenced against these same surfaces. When the meta layer drifts, the whole next-session loop inherits the drift.

**Why this matters:** S2.1 F28 found ≥6 hardcoded CLAUDE.md counts that had drifted from live DB state. S2.7 F2 found a doc-level reference (column name) that didn't exist in the actual schema. Both are symptoms of the same root cause — nothing enforces doc freshness except manual discipline at session wrap-up. S2.8 surfaces the full surface area of that drift: contradictions inside single files, memory files carrying stale forward frames, doc indexes that don't match the filesystem, empty scaffolded dirs, abandoned iteration scripts, orphan files that used to be authoritative and now mislead.

Several prior findings already point at the meta layer:

- **S2.1 F28** — hardcoded counts drift from live DB state. Goes beyond counts; this session tests whether the drift is systemic.
- **S2.7 F2** — `CountryPage.tsx:40` selects a non-existent column. The dead-column class of bug exists in docs too.
- **S2.6 F1/F2** — prompt drift between `VOICE.md` (source-of-truth) and the actual prompts deployed to the edge function. The gap between "what the doc says to do" and "what the code does" is a meta-layer symptom.
- **S2.5 F1** — `describe-chemical` edge function is DEPLOYED with `verify_jwt=false`, unauthenticated. S2.8 verifies whether it's still deployed (yes, confirmed via MCP) and whether any tracking surface catches that it should have been deleted.
- **S2.3 F3** — 15 famous producers have 0 metadata. S2.7 extended to all 10,676 producers. S2.8 checks whether the doc layer flags this anywhere.

Additionally, the sprint pivot on 2026-04-11 (DECISIONS.md "Sprint model + Reference-First pivot") was a sizable doc surgery — S2.8 checks whether it was completely executed or whether some files still carry the pre-pivot frame.

## Read first

- `CLAUDE.md` — always
- `data/sprints/audit/status.md` — sprint plan
- `data/sprints/audit/findings/findings_db_canonical.md` — S2.1 (especially F28, the hardcoded-counts finding)
- `data/sprints/audit/findings/findings_wine_canonical.md` — S2.3 (marquee wine breakage informs "Reference Tables complete" wording)
- `data/sprints/audit/findings/findings_wine_reference.md` — S2.4 (reference content issues that might have leaked into doc claims)
- `data/sprints/audit/findings/findings_code.md` — S2.5 (code-level staleness baselines)
- `data/sprints/audit/findings/findings_voice.md` — S2.6 (VOICE.md drift vs actual prompts)
- `data/sprints/audit/findings/findings_ux.md` — S2.7 (dead fetches = doc-style staleness in code)
- `memory/feedback_opus_inline_reasoning.md` — ratified pattern
- `memory/project_quality_before_enrichment.md` — authoritative sprint sequence
- `docs/DECISIONS.md` — 2026-04-11 "Sprint model + Reference-First pivot" entry

## Objectives

1. **Inventory all meta-layer files.**
   - `docs/*.md` — every file, including subdirs
   - `CLAUDE.md`
   - `memory/*.md` — every file
   - `data/stats/loam_roadmap.json` + `pipeline/analyze/loam_roadmap.py`
   - `data/sprints/current.json` + `data/sprints/audit/{meta,sessions,budget,journal,status}.*`
   - `data/sessions.md`
   - `data/stats/` contents (what's journal / live / stale?)
   - `data/session_prompts/` (legacy dir vs current sprint-specific prompts/)
   - `scripts/dash.ps1`

2. **Cross-reference every hardcoded number in CLAUDE.md against the live DB.** Use MCP `execute_sql`. Flag drift. S2.1 F28 is a prior baseline; compare how much cleanup has happened since.

3. **Check CLAUDE.md for internal contradictions.** Same file, two sections, opposite claims. This exists (F1, F4 found in S2.8).

4. **Verify the sprint pivot (DECISIONS.md 2026-04-11) was fully executed.** Every file that pre-dates the pivot should either:
   - Correctly reflect the new Sprint 2 = Audit frame
   - Be marked as archive/historical
   - Be moved to `docs/reference/`

   Flag any file that still carries the pre-pivot frame as a forward directive.

5. **Audit memory/ files against the format convention.**
   - Frontmatter present? (`name`, `description`, `type` block)
   - Still current? (compare to live file layout, sprint state, DECISIONS.md)
   - MEMORY.md index lines under ~150 chars?
   - Nomenclature current? (e.g., F/D/C/B/A grades vs "Tier 0-3" from older memory)

6. **Audit scheduled infra via MCP `list_scheduled_tasks`.** Which tasks are active? Which are paused? Which are orphaned (referenced in docs but not live)? Which are live but not documented?

7. **Audit `docs/architecture/` and `docs/pipelines/` — are they populated or empty scaffolds?**

8. **Audit `data/session_prompts/` — legacy dir vs current sprint-specific `prompts/` dirs.** Flag the mixed state.

9. **Audit `data/sessions.md` — format and size.** Session entries are supposed to be short whiteboard lines. Check if they've ballooned.

10. **Audit `data/stats/` contents.** Distinguish journals (live), dashboards (regenerated), and one-off run outputs (should probably move to archive).

11. **Verify edge function deployment state via MCP `list_edge_functions`.** Cross-check against S2.5 F1 (`describe-chemical` should have been deleted) — is it still deployed?

12. **Check `loam_roadmap.json` sub_tasks against live sprint sessions.json.** Two dashboards reading two different truth surfaces is a drift vector.

13. **Cross-reference `docs/SOURCES.md` and similar canonical-storage claims against live `information_schema`.** Doc claims that can't be queried are doc-level bugs.

14. **Write `findings_meta.md`** with severity-tagged findings, concrete file:line or MCP evidence, proposed fixes, effort estimates.

## Method

- Opus 4.6 inline — no Haiku/Sonnet API calls
- `Read` / `Glob` / `Grep` / `Bash` for file inspection
- `mcp__c4a52b5c-67f7-4804-8f3b-e9e5c906b1fd__execute_sql` for cross-ref against live DB and `information_schema`
- `mcp__scheduled-tasks__list_scheduled_tasks` for scheduled task state
- `mcp__c4a52b5c-67f7-4804-8f3b-e9e5c906b1fd__list_edge_functions` for deployment state
- No screenshots, no browser, no server runs — static analysis is sufficient
- For each violation, quote the specific file:line and the live DB state (or MCP result) as paired evidence

## Severity scale

- **P0** — broken or user-visible correctness (at the meta layer: a session briefing inherits wrong state; a doc tells pipeline authors to write queries that return zero rows; internal contradictions inside a single file)
- **P1** — significant risk, must fix before Sprint 3 starts (a section is pre-pivot and misleads forward planning; a dir is orphaned from CLAUDE.md index while being live; a memory file carries stale nomenclature)
- **P2** — improvement, not blocking (cleanup, reorganization, archive moves, convention tightening)
- **P3** — nice to have (formatting, line length, optional TOCs)

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

## Scope boundaries

- **In scope:** all docs, CLAUDE.md, memory, roadmap, sprint infra, scheduled tasks, edge function deployment cross-ref, data/stats/ organization, data/session_prompts/ legacy, scripts/ cleanup candidates
- **Out of scope:** frontend code (S2.7 already covered), pipeline code (S2.5 already covered), DB content (S2.1/S2.2 already covered), wine content correctness (S2.3/S2.4/S2.6 already covered), business positioning (S2.9 coming)
- **Partial scope:** DECISIONS.md content rewrite (flag the class; don't rewrite individual decisions); SCHEMA.md content verification (only flag gross mismatches, not every column drift)

## Exit criteria

- [ ] `findings_meta.md` written with severity-tagged findings
- [ ] Every finding has at least one concrete file:line or MCP-evidenced claim
- [ ] Every docs/*.md file inventoried
- [ ] Every memory file inventoried
- [ ] CLAUDE.md hardcoded counts cross-checked against live DB (at least 8 metrics)
- [ ] loam_roadmap.json state cross-checked against sprint dir state
- [ ] `describe-chemical` + `enrich-wine` deployment state verified via MCP
- [ ] Scheduled task state verified via MCP
- [ ] Cross-references to S2.1–S2.7 findings explicit where relevant
- [ ] `sessions.json` S2.8 entry marked `done` with $0 ai_spend
- [ ] `journal.md` S2.8 section completed
- [ ] `budget.json` S2.8 entry at $0
- [ ] `CLAUDE.md` Current State updated (S2.8 done + finding count)
- [ ] `memory/project_sprint2_findings.md` updated with S2.8 cross-references
- [ ] `data/sessions.md` whiteboard entry added under Done
- [ ] This prompt moved to `data/sprints/audit/prompts/s2_8_meta.md`
- [ ] Commit: `S2.8: Meta audit — N findings`

## Starting moves

1. Read CLAUDE.md, status.md, sessions.json, MEMORY.md, all S2.1–S2.7 finding files
2. Inventory docs/*.md via Glob + Bash stat
3. Inventory memory/*.md via Glob + Bash stat
4. TodoWrite the audit scope categories
5. Sequentially check each surface (docs / CLAUDE.md / memory / roadmap / sprint / scheduled / edge / data-stats)
6. For each suspected drift, prove it with a live query or file:line cross-ref
7. Write findings_meta.md with severity-tagged format
8. Wrap up per exit criteria checklist
9. Commit

## Notes for the agent running S2.8

- **Don't fix things.** This is an audit session. Everything broken becomes a finding, not a patch. Fixes are Sprint 3.
- **Trust the live state, not the docs.** When CLAUDE.md says one thing and `execute_sql` says another, CLAUDE.md is wrong. Same for memory, roadmap, and any hardcoded numbers.
- **Flag severity honestly.** If a single paragraph misdirects every session briefing, that's P0 regardless of how small the fix is.
- **This session can split.** If the surface area is larger than expected, open S2.8.5 for the overflow.
- **No parallelism.** No background tasks, no parallel subagents, no cron loops. Sequential reading and querying.
- **Budget should be $0.** Opus inline is the ratified pattern. Any AI escalation needs pre-justification.
- **Cross-session coherence is the point.** S2.1–S2.7 found issues at their respective layers; S2.8 finds the infrastructure-level symptoms of those same issues. Where a prior finding has a meta-layer counterpart (doc that flags it, memory that tracks it, roadmap that schedules it), call out the gap.

Produce a clear, honest, severity-tagged findings file that Sprint 3 can execute from — and that makes Sprint 3 planning less likely to inherit the same drift.
