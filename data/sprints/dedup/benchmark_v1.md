# Benchmark v1

Frozen benchmark for the producer-dedup merge-only rebuild.

## Freeze rule

Do not change this benchmark during the bakeoff except for clear factual mistakes, corrupted metadata, or accidental duplicated cases.

## Size by stratum

| Stratum | Cases | Product risk covered |
|---|---:|---|
| `blind_core_audit` | 100 | Core visible-producer precision and recall. |
| `known_false_merge_patterns` | 16 | Protection against the most expensive failure: merging distinct producers. |
| `known_missed_merge_patterns` | 16 | Protection against real duplicates that the ladder tends to skip or down-rank. |
| `tail_random_sample` | 20 | Long-tail conservatism check without blowing up benchmark size. |
| **Total** | **152** | |

## Verdict mix

- `MERGE`: 51
- `SKIP`: 101
- `PARENT_CHILD`: excluded by design for this rebuild

## What is frozen here

- The benchmark is merge-only.
- `PARENT_CHILD` stays out of scope.
- The core stratum is a documented reconstruction from the final Chrome-validated core ledger because the repo references a 100-pair blind core audit but does not store that exact 100-case merge-only slice as a standalone file.

## Weak spots / bias still remaining

- The benchmark is still Burgundy/France-heavy because many audited failure modes clustered there.
- The tail sample is intentionally small and conservative; it is a smoke test, not a full long-tail recall study.
- This benchmark is review-queue-derived, so it is stronger on adjudication quality than on candidate-generator recall.
- Cross-country global-brand misses are covered, but only through a compact representative set, not exhaustively.
