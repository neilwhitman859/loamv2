# Session 10.8 - selector proof failure analysis

## Goal

Explain exactly why Session 10.7 failed so the project can decide whether
`identity_dossier_select_v1` still has a narrow honest continuation or whether
Sprint 7 should freeze at the current no-go checkpoint.

## Primary deliverable

A durable failure-analysis memo that:

- groups the Session 10.7 misses into concrete failure families
- explains why the selector undercalled `SAME_AS` and `RELATED_BUT_DISTINCT`
- explains why the cheap path collapsed all frontier `UNSURE` cases to
  non-`UNSURE` labels
- explains the Phase D contradictory overwrite attempts
- states whether there is a narrow `v1.1` continuation worth attempting or
  whether Sprint 7 should freeze

## In scope

- `data/sprints/identity-er/proof/selector_proof_execution_summary_v1.json`
- `selector_proof_scorecard_v1.md`
- `selector_proof_go_no_go_memo_v1.md`
- normalized and raw Phase A / Phase B result files
- frozen proof packets and hidden key
- `pipeline/identity/selector_proof_v1.py`
- `data/sprints/identity-er/selector_harness_v1.md`
- `data/sprints/identity-er/escalation_dossier_v1.md`
- `data/sprints/identity-er/accepted_edge_rules_v1.md`
- `data/dashboard.html`
- `AGENTS.md`
- `data/sessions.md`
- Sprint 7 bookkeeping files

## Out of scope

- new model calls or reruns
- shortlist-builder implementation
- builder rollout or merge execution
- DB writes or schema changes
- policy widening
- benchmark mutation

## Budget

Target: `$0`. Use inline reasoning over the frozen artifacts before approving
any more API spend.

## Questions this session should answer

1. Which exact `SAME_AS` misses and `RELATED_BUT_DISTINCT` misses share the
   same underlying selector failure mode?
2. Did the frontier collapse come from prompt discipline, packet shape,
   reason-code incentives, or scorer interaction?
3. Are the Phase D contradictions a storage-rule problem or just a downstream
   symptom of bad selector / escalation behavior?
4. Is there a narrow honest continuation worth one more bounded session, or is
   Sprint 7 already telling us to freeze?

## Recommended posture

Treat Session 10.8 as a failure-analysis session, not a redesign or execution
session. Use the exact frozen packets plus Session 10.7 outputs, name the
smallest real failure families you can defend, and do not propose another run
unless the memo can point to one narrow change with a believable upside.

## Stop rule

Stop once the failure-analysis memo exists and the next required user decision
is explicit.
