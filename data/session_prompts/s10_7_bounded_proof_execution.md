# Session 10.7 - bounded proof execution

## Goal

Run the first real bounded proof execution implied by Session 10.6 against the
frozen `selector_proof_v1` bundle so Sprint 7 can decide whether
`identity_dossier_select_v1` has earned continuation beyond local scaffolding.

## Primary deliverable

A durable bounded-proof run package:

- normalized Phase A selector results against the frozen `48` packets
- normalized Phase B escalation results against the frozen `8` frontier packets
- scorer output and filled scorecard
- explicit note on whether Phase C shortlist smoke is runnable from existing
  code or is still blocked
- a go / no-go memo for Sprint 7 continuation

## In scope

- frozen proof artifacts under `data/sprints/identity-er/proof/`
- `pipeline/identity/selector_proof_v1.py`
- minimal local runner code needed to execute the frozen Phase A / Phase B
  packets and normalize outputs into the stored schema
- scorer execution and scorecard fill
- `data/dashboard.html`
- `AGENTS.md`
- `data/sessions.md`
- Sprint 7 bookkeeping files

## Out of scope

- DB writes, migrations, or merge execution
- changing the proof set, packet shape, or scoring gates
- widening shortlist scope, escalation scope, or label policy
- adding a second heavy pass
- mutating `benchmark_v1`
- broader builder rollout beyond what is required to run the bounded proof

## Budget

Target: stay under `$10` and hard-stop before exceeding the remaining Sprint 7
Phase 1 ceiling.

## Questions this session should answer

1. Can the frozen Phase A selector packets produce normalized real outputs that
   the scorer accepts without hand patching?
2. Can the frozen Phase B escalation packets recover enough frontier cases
   without reopening unsafe positives?
3. Is Phase C shortlist smoke runnable from existing shortlist code, or is that
   layer still blocked pending real builder work?
4. Given actual proof results, should Sprint 7 continue into real builder
   implementation, stay in proof mode, or stop?

## Recommended posture

Treat Session 10.7 as the first real execution of the frozen bundle, not as a
design session. Start with the exact packets already built in Session 10.6,
write only normalized result files plus the scorecard and memo, and keep the
implementation thin. If Phase C cannot be run honestly from existing code,
state that explicitly in the memo instead of inventing a shortcut.

## Stop rule

Stop once the proof run has produced scored Phase A / Phase B results and a
clear go / no-go memo, or earlier if a hard gate fails clearly enough that the
recommendation would no longer change.
