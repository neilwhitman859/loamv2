# selector_proof_scorecard_v1

- model: `claude-sonnet-4-6`
- total spend (estimated from Anthropic usage): `$2.0829`
- overall verdict: `NO_GO`

## Phase A

| Metric | Value | Gate |
| --- | --- | --- |
| false_same_as | 0 | 0 |
| false_related | 1 | <= 1 |
| unsafe_frontier_resolution | 8 | 0 |
| missed_same_as | 7 | <= 4 / 16 |
| missed_related | 12 | <= 4 / 12 |
| over_escalation | 1 | <= 8 / 40 |
| schema_valid_rate | 1.0 | 1.00 |
| choice_valid_rate | 1.0 | 1.00 |
| evidence_ref_integrity_rate | 0.9583 | >= 0.95 |

## Phase B

| Metric | Value | Gate |
| --- | --- | --- |
| false_same_as_after_escalation | 0 | 0 |
| false_related_after_escalation | 1 | 0 |
| unsafe_resolution_of_expected_unsure | 2 | 0 |
| exact_escalation_label_hits | 4 | >= 5 / 8 |
| resolvable_frontier_recovery | 4 | >= 4 / 6 |
| escalation_schema_valid_rate | 1.0 | 1.00 |
| escalation_choice_valid_rate | 1.0 | 1.00 |
| escalation_evidence_ref_integrity_rate | 0.75 | >= 0.95 |
| escalation_block_scope_valid_rate | 0.75 | 1.00 |

## Phase C

- status: `blocked`
- note: No reusable `shortlist_generation_v1` runner exists yet outside the proof-bundle scaffolding. The repo has the frozen manifest plus build-time helper internals in `pipeline/identity/selector_proof_v1.py`, but no standalone shortlist builder that can be run honestly on the 48 proof anchors without inventing new code mid-proof.

## Phase D

| Metric | Value | Gate |
| --- | --- | --- |
| accepted_edge_schema_valid_rate | 1.0 | 1.00 |
| frontier_record_schema_valid_rate | 1.0 | 1.00 |
| contradictory_edge_overwrite_attempts | 4 | 0 |
| same_as_component_barrier_conflicts | 0 | 0 |
| illegal_negative_edge_from_empty_shortlist | 0 | 0 |
| invalid_selector_none_fanout_count | 0 | 0 |
