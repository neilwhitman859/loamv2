# Session 10.8 - visible-signature promotion bakeoff

- Generated: 2026-04-21T21:12:07-04:00
- Run name: `session10_8_visible_signature_promotion_v1`
- Frozen control: `session9_7_layered_safety_sonnet_r2_narrow` / `layered_safety_sonnet_r2_narrow_v1`
- Term set: `visible_signature_terms_v1`
- New model spend: `$0.00`

## Goal

Test whether the frozen Session 9.7 fallback control can be lifted to production quality by a tiny visible-signature promotion layer learned from the existing packet surface, without changing the scorer, the benchmark, or the negative-control base.

## Method

1. Keep the Session 9.7 control fixed as the base decision layer.
2. Extract only visible packet features and citeable refs.
3. Mine conjunction rules from control misses, but reject any rule that hits a skip row on the training slice.
4. Select a small rule set that covers as many training misses as possible.
5. Promote only control non-merge rows that match one of those clauses.

## Full-Fit Result

Full-fit status: production `pass`, fallback `pass`.

- Counts: false merge `0`, hard missed `0`, soft missed `1`, safe flag `17`.
- Rates: exact acc `0.8816`, merge capture `0.9804`, flag rate `0.1184`.
- Promoted cases: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_016`, `blind_core_audit_019`, `blind_core_audit_024`, `known_missed_merge_patterns_001`, `known_missed_merge_patterns_008`, `known_missed_merge_patterns_011`

Selected full-fit rules:
- `family=same_country_lexical_alias`, `large_le_12`, `no_shared_surname_split`, `not_same_region` -> `blind_core_audit_012`, `known_missed_merge_patterns_001`, `known_missed_merge_patterns_011`
- `anchor1`, `containment=b_in_a`, `large_le_12`, `overlap0`, `ref=geo_same_region` -> `blind_core_audit_001`, `blind_core_audit_016`
- `containment=a_in_b`, `no_secondary_b1` -> `blind_core_audit_019`
- `no_secondary_a1`, `ref=risk_shared_surname_split` -> `known_missed_merge_patterns_008`
- `anchor0`, `containment=none`, `not_same_region`, `trigram>=0.6` -> `blind_core_audit_024`, `known_missed_merge_patterns_011`

## Out-Of-Fold Confirmation

OOF status: production `fail`, fallback `fail`.

- Counts: false merge `11`, hard missed `4`, soft missed `3`, safe flag `16`.
- Rates: exact acc `0.7763`, merge capture `0.8627`, flag rate `0.1250`.
- OOF promoted cases: `blind_core_audit_012`, `blind_core_audit_062`, `blind_core_audit_068`, `blind_core_audit_081`, `known_false_merge_patterns_002`, `known_false_merge_patterns_008`, `known_false_merge_patterns_015`, `known_missed_merge_patterns_011`, `tail_random_sample_010`, `tail_random_sample_012`, `tail_random_sample_018`, `tail_random_sample_019`, `tail_random_sample_020`

Fold summaries:
- Fold `1`: train misses `5`, covered train misses `4`, test promotions `2`.
- Fold `2`: train misses `7`, covered train misses `7`, test promotions `4`.
- Fold `3`: train misses `8`, covered train misses `7`, test promotions `3`.
- Fold `4`: train misses `7`, covered train misses `6`, test promotions `1`.
- Fold `5`: train misses `9`, covered train misses `8`, test promotions `3`.

Most frequent OOF rules:
- `4` folds: `containment=a_in_b`, `no_secondary_b1`
- `2` folds: `family=same_country_lexical_alias`, `large_le_12`, `no_shared_surname_split`, `not_same_region`
- `2` folds: `anchor1`, `containment=b_in_a`, `large_le_12`, `overlap0`, `ref=geo_same_region`
- `1` folds: `family=same_country_lexical_alias`, `no_secondary_a1`
- `1` folds: `no_secondary_a1`, `not_same_region`
- `1` folds: `no_secondary_a1`, `ref=risk_shared_surname_split`
- `1` folds: `anchor0`, `containment=none`, `ref=lex_near_exact`
- `1` folds: `containment=b_in_a`, `overlap0`, `ref=geo_same_region`
- `1` folds: `country_conflict`, `large_le_12`, `ref=lex_near_exact`
- `1` folds: `large_le_12`, `not_same_region`, `shared_core=2`
- `1` folds: `anchor0`, `containment=none`, `not_same_region`, `trigram>=0.6`
- `1` folds: `anchor0`, `has_secondary_a1`, `no_shared_surname_split`, `not_same_region`
- `1` folds: `anchor1`, `containment=b_in_a`, `large_le_12`, `overlap0`
- `1` folds: `containment=none`, `family=same_country_lexical_alias`, `large_le_12`, `not_same_region`

## Recommendation

- Status: `promising_but_unconfirmed`
- Reason: The full-fit result clears the frozen production gate, but the out-of-fold confirmation did not. Treat this as a promising method family, not as a production-ready win.
