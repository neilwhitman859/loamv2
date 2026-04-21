# Session 10.4 - selector harness design

## Goal

Turn the fixed Sprint 7 dossier and shortlist contracts into a selector-harness
spec: exactly what the model sees, exactly what it must return, how `none`
competes against real candidates, and what structure the bounded proof should
score before any implementation or scale-up.

## Primary deliverable

A durable selector-spec package for `identity_dossier_select_v1`:
- `selector_harness_v1.md`
- explicit selector input shape built on `producer_dossier_v1` +
  `shortlist_generation_v1`
- exact output contract and reason-code families
- bounded-proof evaluation frame for shortlist selection

## In scope

- `data/sprints/identity-er/plan.md`
- `data/sprints/identity-er/producer_dossier_v1.md`
- `data/sprints/identity-er/source_signal_inventory.md`
- `data/sprints/identity-er/edge_taxonomy_v1.md`
- `data/sprints/identity-er/shortlist_generation_v1.md`
- new selector spec files under `data/sprints/identity-er/`
- `data/dashboard.html`
- `AGENTS.md`
- `docs/DECISIONS.md`
- `data/sessions.md`
- `data/sprints/identity-er/journal.md`
- `data/sprints/identity-er/sessions.json`
- `data/sprints/identity-er/budget.json`

## Out of scope

- implementing selector code
- running model calls or proofs
- escalation-dossier design
- accepted-edge storage implementation
- merge execution

## Budget

`$0`

## Questions this session should answer

1. What exact packet does the selector see for the anchor producer?
2. How should each shortlisted candidate be represented so the model compares
   identities rather than raw source dumps?
3. What exact output schema should the selector emit?
4. How should `none` and `unsure` be distinguished at the selector layer?
5. What bounded-proof rubric should score selector quality before any
   implementation sprint?

## Recommended posture

Keep the selector packet lean and auditable: one anchor dossier, at most `12`
candidate mini-cards, explicit `none` as a first-class option, and patterned
reason codes instead of long prose. The selector should choose the best match
or `none` first; the four-label edge taxonomy (`SAME_AS`,
`RELATED_BUT_DISTINCT`, `NONE`, `UNSURE`) should remain the verdict layer
above that choice.

## Stop rule

Stop once the selector input, output, and proof rubric are durable enough that
the next session can design escalation or prototype implementation without
re-opening shortlist scope.
