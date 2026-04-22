# Session 10.8 - hybrid guarded place alias

- Generated: 2026-04-21T23:41:55-04:00
- Run name: `session10_8_hybrid_guarded_place_alias_opus_rerun_v1`
- Source run: `session10_8_hybrid_guarded_cuvee_anchor_opus_rerun_v1`
- Source model: `claude-opus-4-6`
- Method version: `hybrid_guarded_place_alias_v1`
- Incremental model spend: `$0.0000`
- Inherited source-run spend: `$2.9964`

## Goal

Probe one final visible-packet family: municipal/institutional place aliases like `Stadt Krems` -> `Krems`.

## Promotion Rule

- `municipal_prefix_place_alias_merge`: keep the shared-surname caution, but allow merge when the longer side begins with an institutional prefix (`Stadt`/`Weingut`/`Winzer`), the shorter side is exactly the shared token, and the packet already shows same-region subset/containment support.

## Changed Cases

- `blind_core_audit_016`: `FLAGGED` -> `MERGE` via `municipal_prefix_place_alias_merge` (stadt)

## Scorecard

- Counts: false merge `0`, hard missed `2`, soft missed `0`, safe flag `17`.
- Rates: exact acc `0.8750`, merge capture `0.9608`, flag rate `0.1118`.
- Gates: production `pass`, fallback `pass`.

## Delta Vs Cuvee Anchor

- Improved case ids: `blind_core_audit_016`
- New false merges: none
- Remaining hard misses: `known_missed_merge_patterns_001`, `known_missed_merge_patterns_008`
- Remaining soft misses: none

## Recommendation

- Status: `candidate_improves_cuvee_anchor_margin`
- Reason: The institutional-prefix place-alias probe improved the cuvee-anchor leader without reopening false merges.
