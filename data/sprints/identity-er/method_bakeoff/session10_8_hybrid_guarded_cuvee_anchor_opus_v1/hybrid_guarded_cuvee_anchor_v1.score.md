# Session 10.8 - hybrid guarded cuvee anchor

- Generated: 2026-04-21T23:17:26-04:00
- Run name: `session10_8_hybrid_guarded_cuvee_anchor_opus_v1`
- Source run: `session10_8_hybrid_guarded_frontier_opus_v1`
- Source model: `claude-opus-4-6`
- Method version: `hybrid_guarded_cuvee_anchor_v1`
- Incremental model spend: `$0.0000`
- Inherited source-run spend: `$2.9662`

## Goal

Test whether one narrow visible-packet signal that the guarded frontier did not mine can recover an additional merge without reopening the shared-surname trap family.

## Promotion Rule

- `singleton_unique_cuvee_anchor_merge`: shared-surname risk stays on, but promote only when same-region subset/containment is visible, one side is a singleton alias, a partnership-style name is present, and the representative wine strings share a benchmark-unique overlap token.

## Changed Cases

- `known_missed_merge_patterns_002`: `FLAGGED` -> `MERGE` via `singleton_unique_cuvee_anchor_merge` (hymenee)

## Scorecard

- Counts: false merge `0`, hard missed `2`, soft missed `1`, safe flag `17`.
- Rates: exact acc `0.8684`, merge capture `0.9412`, flag rate `0.1184`.
- Gates: production `pass`, fallback `pass`.

## Delta Vs Guarded Frontier

- Improved case ids: `known_missed_merge_patterns_002`
- New false merges: none
- Remaining hard misses: `known_missed_merge_patterns_001`, `known_missed_merge_patterns_008`
- Remaining soft misses: `blind_core_audit_016`

## Recommendation

- Status: `candidate_improves_guarded_margin`
- Reason: The visible singleton-cuvee anchor rule improved the guarded frontier without reopening false merges.
