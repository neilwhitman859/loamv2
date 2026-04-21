# Session 10.6 - bounded proof artifact build

## Goal

Build the local proof bundle and first scaffolding implied by Session 10.5 so
Sprint 7 can test `identity_dossier_select_v1` without reopening policy:
freeze the hidden answer key, visible packets, scorer, and accepted-edge /
frontier write simulation for the bounded proof.

## Primary deliverable

A durable local proof bundle for `selector_proof_v1`:

- hidden proof key for the fixed 48-case selector proof
- frozen Phase A selector packets
- frozen Phase B escalation packets for the 8 frontier cases
- Phase C shortlist-integration manifest
- normalized run-result schema and scorer
- accepted-edge / frontier-record write simulator
- scorecard template or build memo for the later proof run

## In scope

- `data/sprints/identity-er/selector_harness_v1.md`
- `data/sprints/identity-er/escalation_dossier_v1.md`
- `data/sprints/identity-er/accepted_edge_rules_v1.md`
- `data/sprints/identity-er/selector_proof_v1.md`
- local proof artifacts under `data/sprints/identity-er/proof/`
- local helper code or schemas needed to validate packets and simulate writes
- `data/dashboard.html`
- `AGENTS.md`
- `docs/DECISIONS.md` if a real new decision is forced
- `data/sessions.md`
- Sprint 7 bookkeeping files

## Out of scope

- running model calls
- DB writes or migrations
- merge execution
- benchmark edits to `benchmark_v1`
- widening shortlist scope, escalation scope, edge policy, or proof-set size
- adding a second heavy pass or a fifth label

## Budget

`$0`

## Questions this session should answer

1. What exact local file layout will hold the proof key, visible packets, and
   results?
2. Can the packet schemas and JSON-path citation rules be validated before any
   model call?
3. Can accepted-edge and frontier writes be simulated cleanly from normalized
   outputs without contradictions?
4. Is the proof bundle complete enough that a later session can run it without
   inventing policy?

## Recommended posture

Keep Session 10.6 as an artifact-build session, not an adjudication-run
session. Build the hidden key, packets, scorer, and write simulator first;
prove the scaffolding is coherent locally before spending any model budget or
writing to the DB. If implementation reveals a missing policy field, stop and
open a new design session instead of extending the spec ad hoc.

## Stop rule

Stop once the bounded proof bundle exists locally with schema validation,
packet validation, and accepted-edge/frontier write simulation ready, such that
the next execution-oriented session could run the proof without reopening
selector semantics, escalation policy, or edge-write rules.
