# Session 10.8 - hybrid guarded maison alias

- Generated: 2026-04-21T23:47:23-04:00
- Run name: `session10_8_hybrid_guarded_maison_alias_sonnet_v1`
- Source run: `session10_8_hybrid_guarded_place_alias_sonnet_v1`
- Source model: `claude-sonnet-4-6`
- Method version: `hybrid_guarded_maison_alias_v1`
- Incremental model spend: `$0.0000`
- Inherited source-run spend: `$0.5392`

## Goal

Probe one final visible-packet family: article-wrapped estate aliases like `de la Cabotte` when that side's own representative wines explicitly say `Maison de la Cabotte`.

## Promotion Rule

- `maison_article_estate_alias_merge`: allow merge only when one side is exactly `de la <token>`, the other side already shares that core token, that article-wrapped side's representative wines explicitly use `Maison de la <token>`, and the packet still sits in the thin same-country/no-overlap caution bucket.

## Changed Cases

- `known_missed_merge_patterns_008`: `SKIP` -> `MERGE` via `maison_article_estate_alias_merge` (maison de la cabotte)

## Scorecard

- Counts: false merge `0`, hard missed `1`, soft missed `0`, safe flag `18`.
- Rates: exact acc `0.8750`, merge capture `0.9804`, flag rate `0.1184`.
- Gates: production `pass`, fallback `pass`.

## Delta Vs Place Alias

- Improved case ids: `known_missed_merge_patterns_008`
- New false merges: none
- Remaining hard misses: `known_missed_merge_patterns_001`
- Remaining soft misses: none

## Recommendation

- Status: `candidate_improves_place_alias_margin`
- Reason: The maison/article estate-alias probe improved the place-alias leader without reopening false merges.
