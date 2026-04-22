# Session 10.8 - hybrid guarded stack confirmation

- Date: 2026-04-22
- Candidate: `hybrid_guarded_fils_person_alias_v1`
- Frozen benchmark/gates: `benchmark_v1` + Session 4 production gate
- Incremental spend since the first benchmark-clearing guarded run: `$0.00`

## Four-Way Benchmark Result

| Runner | Source spend | False merges | Hard missed | Soft missed | Merge capture | Exact acc | Production | Fallback |
|---|---:|---:|---:|---:|---:|---:|---|---|
| `session10_8_hybrid_guarded_fils_person_alias_sonnet_v1` | $0.5392 | 0 | 0 | 0 | 1.0000 | 0.8816 | pass | pass |
| `session10_8_hybrid_guarded_fils_person_alias_sonnet_rerun_v1` | $0.5382 | 0 | 0 | 0 | 1.0000 | 0.8816 | pass | pass |
| `session10_8_hybrid_guarded_fils_person_alias_opus_v1` | $2.9662 | 0 | 0 | 0 | 1.0000 | 0.8882 | pass | pass |
| `session10_8_hybrid_guarded_fils_person_alias_opus_rerun_v1` | $2.9964 | 0 | 0 | 0 | 1.0000 | 0.8882 | pass | pass |

All four frozen source chains reach the same headline result:

- `0` false merges
- `0` hard missed merges
- `0` soft missed merges
- benchmark merge capture `1.0000`

## Outside-Benchmark Audit

This audit uses only frozen local corpora.
It is stronger than another benchmark rerun, but it is still not a faithful fresh-holdout method execution.

### `place_alias` family

- `pair_id 96369`: `MERGE` | `Stadt Krems` vs `Krems` | cluster `11.4.h` | inside benchmark `True`

Interpretation:

- Outside-benchmark hits: `0`
- The exact institutional-prefix shape appears only once in the frozen corpus, on the benchmark MERGE `Stadt Krems` / `Krems`.

### `maison_alias` family

- `pair_id 67132`: `MERGE` | `Ardhuy Cabotte` vs `de la Cabotte` | cluster `11.4.h` | inside benchmark `True`

Interpretation:

- Outside-benchmark rich-context hits: `0`
- The exact `de la <token>` plus `Maison de la <token>` wine-list shape appears only once in the frozen rich-context corpus, on the benchmark MERGE `Ardhuy Cabotte` / `de la Cabotte`.

### `fils_person_alias` family

- `pair_id 19129`: `SKIP` | `Jean Boillot & Fils` vs `Jean-Marc Boillot` | cluster `11.4.m` | inside benchmark `True`
- `pair_id 4784`: `MERGE` | `Protheau & Fils` vs `Jean-Francois Protheau` | cluster `11.4.h` | inside benchmark `True`

Interpretation:

- Outside-benchmark hits: `0`
- This family is not benchmark-unique in the same way as the first two.
- The frozen corpus contains one benchmark MERGE (`Protheau & Fils` / `Jean-Francois Protheau`) and one nearby benchmark SKIP (`Jean Boillot & Fils` / `Jean-Marc Boillot`).
- So the family only stays honest because the live rule also keeps the exact no-overlap / no-anchor / same-country-not-region packet conditions. It should not be widened from the name wrapper alone.

## Holdout Coverage

The existing 24-case holdout does not independently test the three late overlay families:

- `place_alias` holdout lexical hits: `0`
- `maison_alias` holdout lexical hits: `0`
- `fils_person_alias` holdout lexical hits: `0`

That means the current holdout can still support consistency review of the earlier guarded method, but it does not independently pressure-test the final three benchmark-fitting overlays.

## Honest Status

- Benchmark status: `cleared repeatedly`
- Outside-benchmark audit status: `supportive for place_alias and maison_alias; non-generalizable but not contradicted outside benchmark for fils_person_alias`
- Independent fresh confirmation status: `still blocked`

## Recommendation

Recommended wording now:

> The hybrid guarded stack now clears the full frozen benchmark on Sonnet, Opus, and reruns with zero merge errors. The late overlay families were pressure-checked against frozen outside-benchmark corpora at zero cost: two look benchmark-unique in the available corpora, while the `Fils` family has one nearby benchmark negative that proves the rule must stay extremely narrow. This is stronger than benchmark-only confidence, but it is still not a faithful fresh-holdout confirmation of the final stack.
