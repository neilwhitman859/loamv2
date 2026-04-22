# Session 10.8 - guarded frontier holdout manifest

- Benchmark id: `producer_dedup_guarded_frontier_holdout_v1`
- Case count: `24`
- Purpose: fresh confirmation slice outside `benchmark_v1` for the current guarded-frontier candidate.

## Why This Exists

The guarded frontier candidate already clears the frozen production gate on `benchmark_v1`, but same-benchmark reruns are not enough to trust it. This manifest defines a deterministic outside-benchmark slice from the frozen Sprint 6 execution ledger so the next confirmation run can use fresh labeled pairs without moving the gateposts.

## Summary

- Verdicts: MERGE `10`, SKIP `14`.
- Tiers: core `7`, mid `16`, tail `1`.

## Selection Buckets

| Bucket | Verdict | Tiers | Patterns | Count | Pair IDs |
|---|---|---|---|---:|---|
| `shared_surname_skip_core` | SKIP | core | 11.4.m | 4 | 3324, 18759, 20583, 62419 |
| `shared_surname_skip_mid` | SKIP | mid | 11.4.m | 4 | 2546, 2655, 3323, 14708 |
| `related_holdco_skip_core` | SKIP | core | 11.4.g, 11.4.j | 3 | 14624, 94073, 122651 |
| `related_holdco_skip_mid` | SKIP | mid | 11.4.g, 11.4.j | 3 | 2507, 30822, 31289 |
| `alias_merge_mid` | MERGE | mid | 11.4.h | 6 | 24821, 29246, 29968, 39875, 43596, 54827 |
| `misc_positive_merge` | MERGE | mid, tail | 11.4.f, 11.4.p | 4 | 25553, 156959, 2188, 7326 |

## Cases

| Case ID | Pair | Verdict | Tier | Pattern | Names |
|---|---:|---|---|---|---|
| `shared_surname_skip_core_3324` | 3324 | SKIP | core | `11.4.m` | Willi Brundlmayer / Josef & Philip Brundlmayer |
| `shared_surname_skip_core_18759` | 18759 | SKIP | core | `11.4.m` | Albert Ponnelle / Perre Ponnelle |
| `shared_surname_skip_core_20583` | 20583 | SKIP | core | `11.4.m` | Januik / Andrew Januik |
| `shared_surname_skip_core_62419` | 62419 | SKIP | core | `11.4.m` | Theulot Juillot / Michel Juillot |
| `shared_surname_skip_mid_2546` | 2546 | SKIP | mid | `11.4.m` | J. Boigelot / Charles Boigelot |
| `shared_surname_skip_mid_2655` | 2655 | SKIP | mid | `11.4.m` | Karine Lauverjat / Christian Lauverjat |
| `shared_surname_skip_mid_3323` | 3323 | SKIP | mid | `11.4.m` | Brundlmayer / Josef & Philip Brundlmayer |
| `shared_surname_skip_mid_14708` | 14708 | SKIP | mid | `11.4.m` | Michel Caillot / Roger Caillot |
| `related_holdco_skip_core_14624` | 14624 | SKIP | core | `11.4.g` | Mouton Baron Philippe / Baron Philippe de Rothschild |
| `related_holdco_skip_core_94073` | 94073 | SKIP | core | `11.4.g` | Lafite Rothschild / Barons Rothschild Lafite Reserve Speciale Medoc |
| `related_holdco_skip_core_122651` | 122651 | SKIP | core | `11.4.g` | Cono Sur / Maycas del Limari |
| `related_holdco_skip_mid_2507` | 2507 | SKIP | mid | `11.4.g` | Château Lafite Rothschild / Barons Rothschild Lafite Legende Blanc |
| `related_holdco_skip_mid_30822` | 30822 | SKIP | mid | `11.4.g` | Mathilde Chapoutier / Beates-Chapoutier |
| `related_holdco_skip_mid_31289` | 31289 | SKIP | mid | `11.4.g` | Mathilde Chapoutier / Pic & Chapoutier |
| `alias_merge_mid_24821` | 24821 | MERGE | mid | `11.4.h` | Lunelli / Tenute Lunelli |
| `alias_merge_mid_29246` | 29246 | MERGE | mid | `11.4.h` | Francesco Sobrero / Sobrero |
| `alias_merge_mid_29968` | 29968 | MERGE | mid | `11.4.h` | Monlot / Monlot Capet |
| `alias_merge_mid_39875` | 39875 | MERGE | mid | `11.4.h` | Familia Eguren / Eguren |
| `alias_merge_mid_43596` | 43596 | MERGE | mid | `11.4.h` | Comtesse de Cherisey / Martelet de Cherisey |
| `alias_merge_mid_54827` | 54827 | MERGE | mid | `11.4.h` | Dutraive / Jean-Louis Dutraive |
| `misc_positive_merge_25553` | 25553 | MERGE | mid | `11.4.f` | Marey & Liger-Belair / Comte Liger Belair |
| `misc_positive_merge_156959` | 156959 | MERGE | mid | `11.4.f` | Francois Gerard / Xavier Gerard |
| `misc_positive_merge_2188` | 2188 | MERGE | mid | `11.4.p` | Duchesse de Magenta / du Duc de Magenta (Louis Jadot) |
| `misc_positive_merge_7326` | 7326 | MERGE | tail | `11.4.f` | Mouton Baron Philippe / Mouton Baronne Philippe |
