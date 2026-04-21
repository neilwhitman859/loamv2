# session9_3_full_rerun_if_approved - adjudication bakeoff v2

- Generated: 2026-04-21T09:04:35-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | 0.3487 | 12 | 33 | 16 | 38 | 1.0000 | 0.3553 | fail | fail |
| sonnet_guardrailed_v2 | 0.5592 | 1 | 5 | 44 | 17 | 1.0000 | 0.4013 | fail | fail |
| gemini_guardrailed_v2 | 0.5526 | 0 | 9 | 42 | 17 | 1.0000 | 0.3882 | fail | fail |
| sonnet_gemini_consensus_v2 | 0.4868 | 0 | 4 | 47 | 27 | 1.0000 | 0.4868 | fail | fail |
