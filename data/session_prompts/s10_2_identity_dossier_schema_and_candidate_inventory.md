# Session 10.2 — identity dossier schema and candidate inventory

## Goal

Turn the new Sprint 7 method from a name into a usable spec by defining the
minimum producer dossier object, the edge taxonomy, and the candidate sources
that should feed shortlist generation.

## Primary deliverable

A durable spec package for `identity_dossier_select_v1`:
- `producer_dossier_v1.md`
- a source / signal inventory for dossier fields
- explicit output labels and edge semantics

## In scope

- `data/sprints/identity-er/plan.md`
- new dossier spec files under `data/sprints/identity-er/`
- `data/dashboard.html`
- `AGENTS.md`
- `data/sessions.md`
- `data/sprints/identity-er/journal.md`
- `data/sprints/identity-er/sessions.json`
- `data/sprints/identity-er/budget.json`

## Out of scope

- implementing dossier builders
- running benchmark proofs
- merge execution
- reopening Sprint 6 pairwise bakeoff work

## Budget

`$0`

## Questions this session should answer

1. What are the minimum useful fields for `producer_dossier_v1`?
2. Which fields belong in the cheap dossier vs the escalation dossier?
3. What exact output labels should the selector emit?
4. Which existing sources in Loam can populate each field today?
5. Which source gaps are acceptable for a first proof?

## Recommended posture

Keep `v1` lean. If a field does not clearly help distinguish `SAME_AS` from
`RELATED_BUT_DISTINCT`, leave it out of the cheap dossier and reserve it for
escalation.

## Stop rule

Stop once the dossier schema, edge taxonomy, and source inventory are durable
enough that the next session can design shortlist generation against them.
