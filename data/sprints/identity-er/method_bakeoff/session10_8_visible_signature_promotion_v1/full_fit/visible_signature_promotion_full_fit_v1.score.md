# session10_8_visible_signature_promotion_v1 - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| visible_signature_promotion_full_fit_v1 | 0.8816 | 0 | 0 | 1 | 17 | 1.0000 | pass |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| visible_signature_promotion_full_fit_v1 | production_eligible | pass | pass |