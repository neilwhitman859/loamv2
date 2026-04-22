# session10_8_visible_signature_promotion_v1 - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| visible_signature_promotion_oof_v1 | 0.7763 | 11 | 4 | 3 | 16 | 1.0000 | fail |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| visible_signature_promotion_oof_v1 | ineligible | fail | fail |