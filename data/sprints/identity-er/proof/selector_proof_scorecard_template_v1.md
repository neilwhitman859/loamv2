# selector_proof_scorecard_v1

## Phase A

| Metric | Value | Gate |
| --- | --- | --- |
| false_same_as | TBD | 0 |
| false_related | TBD | <= 1 |
| unsafe_frontier_resolution | TBD | 0 |
| missed_same_as | TBD | <= 4 / 16 |
| missed_related | TBD | <= 4 / 12 |
| over_escalation | TBD | <= 8 / 40 |
| schema_valid_rate | TBD | 1.00 |
| choice_valid_rate | TBD | 1.00 |
| evidence_ref_integrity_rate | TBD | >= 0.95 |

## Phase B

| Metric | Value | Gate |
| --- | --- | --- |
| false_same_as_after_escalation | TBD | 0 |
| false_related_after_escalation | TBD | 0 |
| unsafe_resolution_of_expected_unsure | TBD | 0 |
| exact_escalation_label_hits | TBD | >= 5 / 8 |
| resolvable_frontier_recovery | TBD | >= 4 / 6 |
| escalation_schema_valid_rate | TBD | 1.00 |
| escalation_choice_valid_rate | TBD | 1.00 |
| escalation_evidence_ref_integrity_rate | TBD | >= 0.95 |
| escalation_block_scope_valid_rate | TBD | 1.00 |

## Phase C

| Metric | Value | Gate |
| --- | --- | --- |
| gold_candidate_present_rate | TBD | >= 0.90 |
| gold_candidate_top_3_rate | TBD | >= 0.75 |
| shortlist_cap_breaches | TBD | 0 |
| none_control_median_candidate_count | TBD | <= 3 |
| empty_shortlist_correct_rate | TBD | >= 0.75 |

## Phase D

| Metric | Value | Gate |
| --- | --- | --- |
| accepted_edge_schema_valid_rate | TBD | 1.00 |
| frontier_record_schema_valid_rate | TBD | 1.00 |
| contradictory_edge_overwrite_attempts | TBD | 0 |
| same_as_component_barrier_conflicts | TBD | 0 |
| illegal_negative_edge_from_empty_shortlist | TBD | 0 |
| invalid_selector_none_fanout_count | TBD | 0 |
