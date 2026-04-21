# Session 10.5 - escalation layer and accepted-edge rules

## Goal

Turn the fixed Sprint 7 selector harness into the next control layer:
exactly when `UNSURE` cases escalate, what extra evidence the heavier pass may
see, and which selector outcomes become durable graph edges versus unresolved
frontier cases.

## Primary deliverable

A durable escalation-and-edges spec package for `identity_dossier_select_v1`:

- `escalation_dossier_v1.md`
- `accepted_edge_rules_v1.md`
- explicit escalation triggers keyed off `selector_harness_v1`
- explicit storage / blocking behavior for `SAME_AS`,
  `RELATED_BUT_DISTINCT`, `NONE`, and `UNSURE`

## In scope

- `data/sprints/identity-er/plan.md`
- `data/sprints/identity-er/producer_dossier_v1.md`
- `data/sprints/identity-er/source_signal_inventory.md`
- `data/sprints/identity-er/edge_taxonomy_v1.md`
- `data/sprints/identity-er/shortlist_generation_v1.md`
- `data/sprints/identity-er/selector_harness_v1.md`
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
- building the proof corpus
- running model calls
- merge execution
- full graph schema implementation

## Budget

`$0`

## Questions this session should answer

1. Which `UNSURE` cases deserve escalation versus immediate abstention?
2. What exact extra evidence blocks may the escalation dossier add?
3. How should escalation output differ from cheap-selector output, if at all?
4. Which selector outcomes become durable accepted edges, and which remain
   unresolved?
5. What graph rules must block unsafe transitivity or duplicate edge drift?

## Recommended posture

Keep escalation narrow and candidate-focused. Do not reopen shortlist scope.
Escalate only when Session 10.4's selector contract says extra evidence could
plausibly change the answer. Accepted-edge rules should stay conservative:
`SAME_AS`, `RELATED_BUT_DISTINCT`, and `NONE` may become durable graph facts
only when they clear their own acceptance rules; `UNSURE` should remain a
frontier state, not a soft stored verdict.

## Stop rule

Stop once Sprint 7 has a frozen escalation trigger model and accepted-edge
policy strong enough that Session 10.6 can build the bounded proof without
reopening selector semantics or graph semantics.
