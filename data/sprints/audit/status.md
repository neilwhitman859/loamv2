# Sprint 2 — Audit

**Status:** active
**Started:** 2026-04-11
**Previous:** 30K Plan (Sprint 1) — closed 2026-04-11, archived at `data/sprints/_archive/30k/`
**Next:** Sprint 3 (Execute) — planned, scope derived from this sprint's findings

## Thesis

**Quality before enrichment.** Previous enrichment attempts struggled because the foundation wasn't ready. Grade C voice regression, thin canonical depth, and factual errors at the page level all taught the same lesson: enrichment inherits the errors of its inputs. The fix isn't better prompts — it's better foundation. Zoom out before chugging through.

Sprints 2 (audit) and 3 (execute) are the foundation-hardening phase. Sprint 4 redesigns the reference layer. Sprint 5 executes reference enrichment. Enrichment at scale does not start until Sprint 5.

## Methodology

Sequential expert-hat passes. Each session wears ONE expert hat and files findings as `findings/findings_<expert>.md`. Every finding is severity-tagged (P0-P3) with concrete evidence (SQL / file reference / content sample) and a proposed fix. Synthesis and the Sprint 3 backlog come last.

- **Read-only.** No DDL, DML, prompt runs, or data pipeline work.
- **Findings-only.** Everything broken becomes a finding, not a patch. Fixes happen in Sprint 3.
- **Reference tables are a through-line, not the only focus.** Wine expert (S2.3) reads canonical wines + producers with the same weight as reference content (S2.4).
- **Sommelier bar.** Wine expert findings are written to the standard of a certified sommelier reading a live page — factual errors, simplistic claims, or missing context are all in scope.

## Session Plan (~9 sessions, flexible order)

| Session | Expert | Scope |
|---|---|---|
| S2.1 | db_canonical | Sprint 2 open + canonical wines/producers/vintages/grapes/scores/prices/external_ids + reference layer structural + join paths + empty table categorization |
| S2.2 | db_staging | All 32 `source_*` staging tables: merge state, processed_at distribution, value-left-on-table |
| S2.3 | wine_canonical | Stratified 100-wine + 50-producer sample, Sonnet fact-checking against primary sources, sommelier bar — main AI spend |
| S2.4 | wine_reference | `appellation_rules`, `appellation_grapes`, grape synonyms, categorizations, soil links — hand-verified content correctness |
| S2.5 | code | `pipeline/` scripts, edge function, shared libs, scheduled tasks, conventions, error handling, dead code |
| S2.6 | voice | 2 enriched wines + enrichment prompts + existing insight tables vs `docs/VOICE.md` |
| S2.7 | ux | Every page type, data-to-UI integrity, empty state handling |
| S2.8 | meta | `docs/*.md`, CLAUDE.md, memory files, `loam_roadmap.json`, scheduled infra |
| S2.9 | business | Competitive positioning, monetization, value prop. Dedupe findings across experts. Produce Sprint 3 backlog. |

Session order is flexible. If UX findings should inform the code audit, reorder. If one session overflows, split into `S2.X.5`. Recalibrate after each session.

## Budget

- **Target:** $15.00 (planned)
- **Ceiling:** $25.00 (hard stop)
- **Combined Sprints 2+3 ceiling:** $50.00
- **Main spend:** S2.3 wine expert sample (~$15-18 on Sonnet fact-checking of 100-wine + 50-producer stratified sample)
- Any session spending > $15 or approaching the ceiling gets justified in `journal.md` BEFORE spending.

## Discipline

- **Read-only.** Findings are outputs, not DB writes.
- **P0-P3 severity** on every finding. P0 = broken/incorrect + user-visible/correctness-critical, P1 = significant gap, must fix before enrichment, P2 = improvement, P3 = nice to have.
- **Evidence-backed.** Each finding cites a query, file path, or concrete example. "Feels wrong" is not a finding.
- **Proposed fixes.** Every finding includes an effort estimate and a proposed fix, even if just a sketch.
- **Flexibility.** `S2.X.5` overflow sessions are allowed. Session order is not locked. Recalibration is a feature.
- **Scope escalation.** If audit findings reveal a structural issue that would require rewriting Sprint 3, stop and escalate to the user immediately.

## Primary Deliverable

A prioritized **Sprint 3 fix backlog** at `data/sprints/audit/findings/synthesis.md` (written in S2.9). Sprint 3 executes from it top-down, closing P0 and P1 strictly before any P2/P3 work.

## This Prompt

`data/sprints/audit/prompts/s2_1_db_canonical.md` (moved from `data/session_prompts/` at end of S2.1).
