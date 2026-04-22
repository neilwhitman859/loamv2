# ground_truth_seed_v1 summary

Generated from the validated Sprint 6 execution ledger plus the frozen `benchmark_v1` subset.

## Live corpus snapshot

- producers: `33281`
- wines: `225281`
- wine_vintages: `125934`
- producer_dedup_pairs: `600103`
- unlabeled producer_dedup_pairs: `151150`

## Seed inventory

- pair records: `422`
- scoreable pair records: `417`
- singleton sanity records: `71`
- frozen benchmark overlap: `152`

## Pair label mix

- `DEFERRED`: `5`
- `NONE`: `275`
- `RELATED_BUT_DISTINCT`: `46`
- `SAME_AS`: `96`

## Scoreable gap to 1,000 pairs

- total scoreable gap: `+583`
- `SAME_AS`: 96 current, need `+204` to reach `300`
- `RELATED_BUT_DISTINCT`: 46 current, need `+154` to reach `200`
- `NONE`: 275 current, need `+225` to reach `500`

## Tier mix

- `core`: `143`
- `mid`: `138`
- `tail`: `141`

## Top countries in current pair seed

- `FR`: `230`
- `US`: `55`
- `IT`: `30`
- `ES`: `18`
- `AU`: `8`
- `PT`: `7`
- `PT/FR`: `5`
- `AT`: `4`
- `DE`: `4`
- `ZA`: `4`

## Notes

- `DEFERRED` records stay in the ledger but do not count toward the scoreable target.
- Singleton `KEEP_AS_IS` records are useful sanity checks for producer-card correctness, not pairwise scoring.
- The live `producer_dedup_pairs` table is not treated as truth because `verdict_source` is still blank on all rows.
