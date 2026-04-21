# Session 10.3 - shortlist generation design

## Goal

Turn the fixed Sprint 7 dossier contract into a shortlist-generation spec:
which sources create candidates, which signals only re-rank candidates, how
small the shortlist must stay, and what suppression rules should prevent
obvious false candidates before model calls.

## Primary deliverable

A durable shortlist-spec package for `identity_dossier_select_v1`:
- `shortlist_generation_v1.md`
- explicit seed-source tiers and candidate caps
- suppression / pruning rules for obvious non-candidates

## In scope

- `data/sprints/identity-er/plan.md`
- `data/sprints/identity-er/producer_dossier_v1.md`
- `data/sprints/identity-er/source_signal_inventory.md`
- `data/sprints/identity-er/edge_taxonomy_v1.md`
- new shortlist spec files under `data/sprints/identity-er/`
- `data/dashboard.html`
- `AGENTS.md`
- `docs/DECISIONS.md`
- `data/sessions.md`
- `data/sprints/identity-er/journal.md`
- `data/sprints/identity-er/sessions.json`
- `data/sprints/identity-er/budget.json`

## Out of scope

- implementing shortlist builders
- writing SQL or Python candidate-generation code
- selector prompt or scoring design
- benchmark proofs
- merge execution

## Budget

`$0`

## Questions this session should answer

1. Which signals are allowed to create shortlist candidates in v1?
2. Which signals are enrich-only or re-rank-only, but must not seed candidates?
3. What is the maximum shortlist size per anchor producer?
4. What suppression rules should drop obvious false candidates before selector work?
5. What recall gaps are acceptable for a first bounded proof?

## Recommended posture

Use only the Tier A stack as seed generators: canonical lexical blocking,
LWIN producer-name variants, TTB brand/applicant variants, and state
registration variants. Importer, retailer, and profile sources can help
re-rank or break ties, but they should not seed candidates by themselves in
`v1`.

## Stop rule

Stop once the shortlist source stack, candidate caps, ranking order, and
suppression rules are durable enough that the next session can design the
selector harness against them without re-opening dossier scope.
