# Session 10.5 - control layer design and proof prep

## Goal

Turn the fixed Sprint 7 selector harness into the broader control layer for
`identity_dossier_select_v1`: exactly when `UNSURE` cases escalate, what extra
evidence the heavier pass may see, which selector outcomes become durable graph
edges versus unresolved frontier cases, and what proof/implementation guardrails
must already be fixed before Session 10.6 starts building artifacts.

## Primary deliverable

A durable Sprint 7 control-package for `identity_dossier_select_v1`:

- `escalation_dossier_v1.md`
- `accepted_edge_rules_v1.md`
- `selector_proof_v1.md` or equivalent bounded-proof spec
- explicit escalation triggers keyed off `selector_harness_v1`
- explicit storage / blocking behavior for `SAME_AS`,
  `RELATED_BUT_DISTINCT`, `NONE`, and `UNSURE`
- explicit proof-set / scorecard / implementation-handoff guardrails so Session
  10.6 can build without reopening policy questions

## In scope

- `data/sprints/identity-er/plan.md`
- `data/sprints/identity-er/producer_dossier_v1.md`
- `data/sprints/identity-er/source_signal_inventory.md`
- `data/sprints/identity-er/edge_taxonomy_v1.md`
- `data/sprints/identity-er/shortlist_generation_v1.md`
- `data/sprints/identity-er/selector_harness_v1.md`
- relevant Sprint 6 benchmark/evaluation references if useful for proof-shape
  rigor:
  - `data/sprints/dedup/benchmark_v1.json`
  - `data/sprints/dedup/metrics_and_goals.md`
  - `data/sprints/dedup/session4_bakeoff_design.md`
- new escalation / edge spec files under `data/sprints/identity-er/`
- `data/dashboard.html`
- `AGENTS.md`
- `docs/DECISIONS.md`
- `data/sessions.md`
- `data/sprints/identity-er/journal.md`
- `data/sprints/identity-er/sessions.json`
- `data/sprints/identity-er/budget.json`

## Out of scope

- selector implementation
- assembling the full proof corpus
- running model calls
- merge execution
- full graph schema implementation
- reopening shortlist scope unless the selector contract makes a tiny correction
  unavoidable

## Budget

`$0`

## Questions this session should answer

1. Which `UNSURE` cases deserve escalation versus immediate abstention?
2. What exact extra evidence blocks may the escalation dossier add?
3. How should escalation output differ from cheap-selector output, if at all?
4. Which selector outcomes become durable accepted edges, and which remain
   unresolved?
5. What graph rules must block unsafe transitivity or duplicate edge drift?
6. What exact bounded-proof artifact should validate the selector +
   escalation-control layer before implementation work starts?
7. Which implementation guardrails should Session 10.6 inherit so it builds
   artifacts instead of silently making new policy?

## Recommended posture

Broaden the session around one coherent control-package, not around extra
implementation. Keep escalation narrow and candidate-focused, but use the same
session to freeze the proof-prep and handoff rules that would otherwise leak
into Session 10.6. Do not reopen shortlist scope or selector semantics unless a
small clarification is truly unavoidable. Accepted-edge rules should stay
conservative: `SAME_AS`, `RELATED_BUT_DISTINCT`, and `NONE` may become durable
graph facts only when they clear their own acceptance rules; `UNSURE` should
remain a frontier state, not a soft stored verdict.

## Stop rule

Stop once Sprint 7 has a frozen control-package strong enough that Session 10.6
can build the bounded proof artifacts and/or first implementation scaffolding
without reopening selector semantics, escalation policy, edge policy, or proof
rules.
