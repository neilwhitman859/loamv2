# `accepted_edge_rules_v1`

## Purpose

`accepted_edge_rules_v1` defines what the Sprint 7 control layer is allowed to
write as durable graph facts and what must remain a frontier record.

The core distinction is:

- accepted edges are durable pairwise facts
- frontier cases are durable unresolved work records
- neither one is the same thing as an executed producer merge

---

## V1 doctrine

1. `SAME_AS` is the only merge-eligible edge type.
2. `RELATED_BUT_DISTINCT` and `NONE` are both hard merge barriers.
3. `UNSURE` is never a durable accepted edge.
4. Empty-shortlist outcomes are case closures, not negative pair edges.
5. No optimistic transitivity outside accepted `SAME_AS` components.
6. New evidence may challenge an accepted edge later, but it may not silently
   overwrite it.

---

## Three record classes

### 1. `identity_case_runs`

Audit log of every selector or escalation decision.

Properties:

- append-only
- keeps packet version, method version, label, reason codes, and evidence refs
- is not authoritative for graph state by itself

### 2. `identity_edges_accepted`

Durable pairwise graph facts.

Allowed `edge_type` values:

- `SAME_AS`
- `RELATED_BUT_DISTINCT`
- `NONE`

### 3. `identity_frontier_cases`

Durable unresolved records for cases that ended in `UNSURE` or in a bounded
`no_candidate_found` stop state.

These records are operational memory, not accepted graph truth.

---

## Acceptance matrix

| Final stage + label | Durable pairwise write | Frontier write | Merge effect |
| --- | --- | --- | --- |
| selector `SAME_AS` | one accepted `SAME_AS` edge for anchor <-> selected candidate | none | merge-eligible later |
| selector `RELATED_BUT_DISTINCT` | one accepted `RELATED_BUT_DISTINCT` edge for anchor <-> selected candidate | none | hard merge barrier |
| selector `NONE` with `candidates_present` | accepted `NONE` edge from anchor to each shortlist candidate shown in the packet | none | hard merge barrier for those pairs |
| selector `NONE` with `no_candidate_found` | no pair edge | closed case run only | no pairwise graph change |
| selector `UNSURE` with candidate | no accepted edge | frontier case on anchor + candidate | not merge-eligible |
| selector `UNSURE` with none | no accepted edge | frontier case on anchor only | not merge-eligible |
| escalation `SAME_AS` | one accepted `SAME_AS` edge for anchor <-> selected candidate | close frontier | merge-eligible later |
| escalation `RELATED_BUT_DISTINCT` | one accepted `RELATED_BUT_DISTINCT` edge for anchor <-> selected candidate | close frontier | hard merge barrier |
| escalation `NONE` in `candidate_frontier` | one accepted `NONE` edge for anchor <-> frontier candidate | close frontier | hard merge barrier |
| escalation `NONE` in `shortlist_gap_probe` | no pair edge | close frontier as no-candidate stop | no pairwise graph change |
| escalation `UNSURE` | no accepted edge | frontier remains unresolved | not merge-eligible |

### Why selector-side `NONE` writes multiple negative edges

`selector_harness_v1` makes `NONE` a packet-level stop decision rather than a
candidate-specific label. When `shortlist_status = candidates_present`, the
model has explicitly reviewed that bounded shortlist and concluded that none of
those candidates deserves `SAME_AS` or `RELATED_BUT_DISTINCT`.

So in `v1`, a selector-side `NONE` writes a negative edge from the anchor to
every candidate shown in that packet.

This rule does **not** apply when the shortlist was empty.

---

## Minimal accepted-edge shape

```json
{
  "edge_version": "accepted_edge_rules_v1",
  "edge_type": "SAME_AS|RELATED_BUT_DISTINCT|NONE",
  "producer_id_low": "uuid",
  "producer_id_high": "uuid",
  "decision_stage": "selector|escalation",
  "case_id": "uuid",
  "packet_id": "uuid",
  "primary_reason_code": "string",
  "secondary_reason_codes": ["string"],
  "rule_hypotheses": ["11.4.h"],
  "support_ref_paths": ["string"],
  "conflict_ref_paths": ["string"],
  "status": "accepted"
}
```

### Edge requirements

1. pair key must be canonicalized as low/high producer id
2. `support_ref_paths` must resolve into the stored packet
3. `SAME_AS` and `RELATED_BUT_DISTINCT` require one explicit selected
   candidate
4. `NONE` may be emitted either:
   - per-candidate from selector-side packet-wide `NONE`, or
   - as a single pair from escalation-side `candidate_frontier` resolution

---

## Minimal frontier-case shape

```json
{
  "frontier_version": "accepted_edge_rules_v1",
  "case_id": "uuid",
  "anchor_producer_id": "uuid",
  "frontier_candidate_id": "uuid|null",
  "frontier_kind": "candidate_frontier|shortlist_gap_probe",
  "status": "unresolved|closed_no_candidate",
  "reason_code": "string",
  "escalation_focus": ["string"],
  "last_decision_stage": "selector|escalation"
}
```

### Frontier rules

1. `UNSURE` after selector creates an unresolved frontier case.
2. `UNSURE` after escalation keeps the frontier unresolved and closes automatic
   processing for `v1`.
3. `NONE` with an empty shortlist closes the case but does not create a pairwise
   graph barrier.

---

## Acceptance checks by edge type

### `SAME_AS`

Write an accepted `SAME_AS` edge only when all of these are true:

1. final label is `SAME_AS`
2. output schema is valid
3. selected candidate exists in the packet
4. support refs are valid
5. `needs_escalation = false`
6. there is no already-accepted `NONE` or `RELATED_BUT_DISTINCT` barrier
   between the current `SAME_AS` components
7. the evidence is not permit-only, facility-only, importer-only, merchant-only,
   or shared-surname-only

### `RELATED_BUT_DISTINCT`

Write an accepted `RELATED_BUT_DISTINCT` edge only when all of these are true:

1. final label is `RELATED_BUT_DISTINCT`
2. output schema is valid
3. selected candidate exists in the packet
4. reason code is from the related-family set
5. support refs are valid
6. there is no already-accepted `SAME_AS` union tying the endpoints together

### `NONE`

Write accepted `NONE` edges only when all of these are true:

1. final label is `NONE`
2. output schema is valid
3. `needs_escalation = false`
4. support refs are valid
5. the negative edge targets are explicit from the packet:
   - every shortlist candidate in a selector-side `NONE` packet, or
   - the single frontier candidate in an escalation-side `candidate_frontier`
     packet

Never write a `NONE` edge for an unseen candidate or an empty-shortlist case.

---

## Graph blocking rules

### Rule 1: only `SAME_AS` forms identity components

Accepted `SAME_AS` edges may later be unioned into producer identity
components.

`RELATED_BUT_DISTINCT` and `NONE` never create components.

### Rule 2: `RELATED_BUT_DISTINCT` and `NONE` both block later `SAME_AS`

Before accepting a new `SAME_AS` edge between producer `A` and producer `B`,
the writer must inspect the current `SAME_AS` components containing `A` and
`B`.

If **any** accepted `RELATED_BUT_DISTINCT` or `NONE` edge exists across those
two components, the new `SAME_AS` edge is blocked and recorded as a conflict
event instead of being written.

### Rule 3: no hopeful transitivity

These inferences are forbidden:

- `A SAME_AS B` plus `B RELATED_BUT_DISTINCT C` does **not** imply anything
  positive about `A` and `C`
- `A SAME_AS B` plus `B NONE C` does **not** let `A` bypass the barrier to `C`
- `A RELATED_BUT_DISTINCT B` plus `B RELATED_BUT_DISTINCT C` does **not**
  imply `A RELATED_BUT_DISTINCT C`

### Rule 4: one active accepted edge type per pair

For a canonical producer pair, `identity_edges_accepted` may have only one
active edge type.

If a later run proposes a different edge type:

- do not overwrite the accepted edge
- create a conflict record tied to the new case run
- require a later explicit retirement/supersession process to change the graph

### Rule 5: rerun agreement extends provenance, not edge count

If a later run reaches the same accepted edge type for the same canonical pair,
append the new provenance to the existing edge record rather than writing a
parallel duplicate row.

---

## Duplicate-edge and barrier-drift rules

The Session 10.6 implementation must preserve these invariants:

1. component lookups must happen before a new `SAME_AS` write
2. a selector-side `NONE` packet may generate multiple negative edges, but they
   must all point to candidates actually shown in that packet
3. unresolved frontier records must never be misread as accepted negative edges
4. empty-shortlist case closures must never create pairwise barriers
5. contradictory writes become conflict events, not silent graph mutations

---

## What Session 10.6 is allowed to build

Session 10.6 may build:

- local schemas or JSON fixtures for `identity_case_runs`
- local schemas or JSON fixtures for `identity_edges_accepted`
- local schemas or JSON fixtures for `identity_frontier_cases`
- proof-time edge-write simulation

Session 10.6 may **not**:

- execute producer merges
- add a fifth label
- let `UNSURE` create an accepted pairwise edge
- let empty shortlist outcomes create `NONE` edges
- bypass component-level barrier checks for `SAME_AS`

---

## Bottom line

`accepted_edge_rules_v1` keeps Sprint 7 conservative:

- accepted edges are narrow and explicit
- frontier cases stay visible without pretending to be graph truth
- `SAME_AS` can grow components only when no accepted barrier already says stop
