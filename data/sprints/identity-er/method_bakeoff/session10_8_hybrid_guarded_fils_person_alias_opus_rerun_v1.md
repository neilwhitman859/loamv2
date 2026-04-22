# Session 10.8 - hybrid guarded fils person alias

- Generated: 2026-04-21T23:50:29-04:00
- Run name: `session10_8_hybrid_guarded_fils_person_alias_opus_rerun_v1`
- Source run: `session10_8_hybrid_guarded_maison_alias_opus_rerun_v1`
- Source model: `claude-opus-4-6`
- Method version: `hybrid_guarded_fils_person_alias_v1`
- Incremental model spend: `$0.0000`
- Inherited source-run spend: `$2.9964`

## Goal

Probe one last visible-packet family: `Fils` trading names paired with fuller personal-name variants that end in the same shared surname token.

## Promotion Rule

- `fils_personal_name_alias_merge`: allow merge only when one side contains `Fils`, the other side is a fuller personal-name form ending in the shared surname token, and the packet stays in the thin same-country/no-overlap caution bucket.

## Changed Cases

- `known_missed_merge_patterns_001`: `SKIP` -> `MERGE` via `fils_personal_name_alias_merge` (Jean-Francois Protheau)

## Scorecard

- Counts: false merge `0`, hard missed `0`, soft missed `0`, safe flag `17`.
- Rates: exact acc `0.8882`, merge capture `1.0000`, flag rate `0.1118`.
- Gates: production `pass`, fallback `pass`.

## Delta Vs Maison Alias

- Improved case ids: `known_missed_merge_patterns_001`
- New false merges: none
- Remaining hard misses: none
- Remaining soft misses: none

## Recommendation

- Status: `candidate_improves_maison_alias_margin`
- Reason: The fils/personal-name probe improved the maison-alias leader without reopening false merges.
