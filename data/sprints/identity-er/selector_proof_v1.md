# `selector_proof_v1`

## Purpose

`selector_proof_v1` is the bounded proof spec for the full Sprint 7 control
layer:

- cheap selector behavior
- escalation behavior
- shortlist integration
- accepted-edge write simulation

The proof exists to answer one question:

> is `identity_dossier_select_v1` safe and structured enough to justify
> implementation work beyond packet/scorer scaffolding?

This is a proof of the method contract, not a proof of full-corpus readiness.

---

## What Session 10.6 must build

Session 10.6 should build a frozen local proof bundle, not a production
pipeline.

Required proof artifacts:

1. hidden answer key for the cheap selector cases
2. frozen Phase A selector packets
3. frozen Phase B escalation packets for the frontier subset
4. shortlist-integration manifest for the same proof cases
5. normalized run-result schema and scorer
6. accepted-edge / frontier-record write simulator
7. scorecard memo that reports whether the control layer passes or fails

Recommended local artifact names:

- `data/sprints/identity-er/proof/selector_proof_cases_v1.jsonl`
- `data/sprints/identity-er/proof/phase_a_selector_packets/`
- `data/sprints/identity-er/proof/phase_b_escalation_packets/`
- `data/sprints/identity-er/proof/phase_c_shortlist_manifest.json`
- `data/sprints/identity-er/proof/selector_proof_results_v1.jsonl`
- `data/sprints/identity-er/proof/selector_proof_scorecard_v1.md`

The exact file names can differ, but the separation of hidden answer key,
visible packets, run results, and scorecard must remain.

---

## Frozen proof shape

The proof has four phases.

### Phase A: selector-only cheap-path proof

Use frozen hand-built shortlist packets so we test the selector contract in
isolation from retrieval bugs.

Fixed composition: **48 cases**

| Stratum | Count | Purpose |
| --- | ---: | --- |
| `same_as_core` | `16` | alias, orthographic, historical-form, merchant-prefix, and global-brand recoveries already visible in Tier A data |
| `related_controls` | `12` | holdco, shared-family, facility, JV, sub-brand, and auction/negociant adjacency |
| `none_controls` | `12` | lexical collisions, weak fuzzy survivors, and shortlist noise |
| `unsure_frontier` | `8` | thin or contradictory cases where the cheap path should abstain and request escalation |

This is the same Phase A shape frozen in `selector_harness_v1`.

### Phase B: escalation replay on the frontier subset

Take the same **8** `unsure_frontier` cases from Phase A and build bounded
`escalation_dossier_v1` packets for them.

Fixed expected outcome mix for those 8 cases:

- `3` should resolve to `SAME_AS`
- `2` should resolve to `RELATED_BUT_DISTINCT`
- `1` should resolve to `NONE`
- `2` should remain `UNSURE`

Reason: the heavy path should prove both that it can recover some true
frontiers and that it can also abstain honestly when even richer evidence is
not enough.

### Phase C: shortlist integration smoke

Run the same 48 logical cases through the real shortlist builder and compare
its output with the frozen Phase A expectations.

Purpose:

- verify the expected candidate is actually surfaced
- verify the shortlist cap remains bounded
- keep retrieval quality separate from selector quality

### Phase D: accepted-edge write simulation

Feed the normalized outputs from Phases A and B into a local writer that
simulates:

- `identity_case_runs`
- `identity_edges_accepted`
- `identity_frontier_cases`

Purpose:

- prove that the control-layer outputs can be written without contradictions
- catch illegal `SAME_AS` unions and illegal negative-edge writes before any DB
  work exists

---

## Hidden answer-key fields

The hidden proof key should stay outside model-visible packets.

### Phase A / Phase C key

```json
{
  "proof_version": "selector_proof_v1",
  "case_id": "selector_proof_001",
  "risk_tier": "core|tail",
  "pattern_family": "11.4.h",
  "world_relationship": "SAME_AS|RELATED_BUT_DISTINCT|NONE",
  "expected_selector_label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "acceptable_candidate_ids": ["uuid"],
  "shortlist_expectation": "candidate_present|empty_shortlist_ok|frontier_gap_ok",
  "escalation_expected": true
}
```

### Phase B escalation key

```json
{
  "proof_version": "selector_proof_v1",
  "case_id": "selector_proof_041",
  "expected_escalation_label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "expected_escalation_choice_type": "candidate|none",
  "acceptable_candidate_ids": ["uuid"],
  "allowed_escalation_blocks": [
    "web_identity",
    "profile_snippets"
  ],
  "expected_case_resolution": "accepted_edge|frontier_unresolved|closed_no_candidate"
}
```

Why separate expected cheap-path and escalation labels:

- some real-world matches should still be `UNSURE` on the cheap path
- the scorer must reward honest abstention before escalation

---

## Phase A gates

Phase A keeps the gates already frozen in `selector_harness_v1`.

The harness is not ready unless all of these pass:

1. `false_same_as = 0`
2. `false_related <= 1`
3. `unsafe_frontier_resolution = 0`
4. `missed_same_as <= 4` out of `16`
5. `missed_related <= 4` out of `12`
6. `over_escalation <= 8` out of `40` non-frontier cases
7. `schema_valid_rate = 1.00`
8. `choice_valid_rate = 1.00`
9. `evidence_ref_integrity_rate >= 0.95`

Phase A is a hard gate. If it fails, stop before worrying about escalation.

---

## Phase B escalation gates

Phase B validates the heavy path on the 8 frontier cases.

### Metrics

- `false_same_as_after_escalation`
- `false_related_after_escalation`
- `unsafe_resolution_of_expected_unsure`
- `exact_escalation_label_hits`
- `resolvable_frontier_recovery`
- `escalation_schema_valid_rate`
- `escalation_choice_valid_rate`
- `escalation_evidence_ref_integrity_rate`
- `escalation_block_scope_valid_rate`

### Gates

1. `false_same_as_after_escalation = 0`
2. `false_related_after_escalation = 0`
3. `unsafe_resolution_of_expected_unsure = 0`
4. `exact_escalation_label_hits >= 5` out of `8`
5. `resolvable_frontier_recovery >= 4` out of `6`
6. `escalation_schema_valid_rate = 1.00`
7. `escalation_choice_valid_rate = 1.00`
8. `escalation_evidence_ref_integrity_rate >= 0.95`
9. `escalation_block_scope_valid_rate = 1.00`

Interpretation:

- the heavy path must not create any unsafe positive resolution
- it does not need to solve every frontier case
- it must use only the escalation blocks frozen in `escalation_dossier_v1`

---

## Phase C shortlist-integration gates

Phase C keeps the shortlist smoke metrics already defined in
`selector_harness_v1`.

### Metrics

- `gold_candidate_present_rate`
- `gold_candidate_top_3_rate`
- `shortlist_cap_breaches`
- `none_control_median_candidate_count`
- `empty_shortlist_correct_rate`

### Gates

1. `gold_candidate_present_rate >= 0.90`
2. `gold_candidate_top_3_rate >= 0.75`
3. `shortlist_cap_breaches = 0`
4. `none_control_median_candidate_count <= 3`
5. `empty_shortlist_correct_rate >= 0.75`

If Phase C fails, the control layer is still not implementation-ready even if
Phase A and B pass, because retrieval is hiding or flooding cases.

---

## Phase D accepted-edge write-simulation gates

Phase D is the control-layer addition that Session 10.4 did not yet freeze.

### Metrics

- `accepted_edge_schema_valid_rate`
- `frontier_record_schema_valid_rate`
- `contradictory_edge_overwrite_attempts`
- `same_as_component_barrier_conflicts`
- `illegal_negative_edge_from_empty_shortlist`
- `invalid_selector_none_fanout_count`

### Gates

1. `accepted_edge_schema_valid_rate = 1.00`
2. `frontier_record_schema_valid_rate = 1.00`
3. `contradictory_edge_overwrite_attempts = 0`
4. `same_as_component_barrier_conflicts = 0`
5. `illegal_negative_edge_from_empty_shortlist = 0`
6. `invalid_selector_none_fanout_count = 0`

Interpretation:

- accepted edges must be writable without ambiguity
- unresolved frontier outcomes must stay outside the accepted-edge graph
- selector-side `NONE` fanout is allowed only to actual packet candidates

---

## Implementation guardrails for Session 10.6

Session 10.6 should build artifacts, not new policy.

Guardrails:

1. do not add a new label, a new escalation mode, or a second heavy pass
2. do not widen the shortlist cap beyond `12`
3. do not promote new source families onto the cheap path
4. do not mutate `benchmark_v1`; `selector_proof_v1` is its own frozen proof
5. do not run production DB writes, migrations, or merge execution
6. do not let `UNSURE` create accepted pairwise edges
7. do not let empty-shortlist cases create `NONE` edges
8. do not change the proof set size or label mix without a new design session
9. validate packet schema and JSON-path integrity before any model call
10. if implementation reveals a missing policy field, stop and log a new
    design session instead of extending the spec ad hoc

---

## Go / no-go interpretation

The control layer passes only if **all four phases** pass.

### If all phases pass

Session 10.7 may evaluate whether Sprint 7 should continue into real builder
implementation and broader proof expansion.

### If any phase fails

Do not expand scope quietly.

Open the next session as a failure-analysis or redesign session targeted at the
specific failed layer:

- selector contract
- escalation policy
- shortlist generation
- accepted-edge write logic

---

## Bottom line

`selector_proof_v1` keeps Session 10.6 honest:

- prove the cheap selector first
- prove the heavy path on a bounded frontier
- prove retrieval does not sabotage the packet
- prove accepted-edge writes can be simulated without contradiction
