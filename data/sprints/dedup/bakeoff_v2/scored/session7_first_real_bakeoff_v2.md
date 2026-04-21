# session7_first_real_bakeoff_v2 - adjudication bakeoff v2

- Generated: 2026-04-21T00:10:44-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | 0.1447 | 46 | 2 | 30 | 52 | 1.0000 | 0.5395 | fail | fail |
| sonnet_guardrailed_v2 | 0.7434 | 26 | 10 | 1 | 2 | 1.0000 | 0.0197 | fail | fail |
| gemini_guardrailed_v2 | 0.7500 | 31 | 5 | 1 | 1 | 1.0000 | 0.0132 | fail | fail |
| sonnet_gemini_consensus_v2 | 0.6447 | 18 | 5 | 8 | 23 | 1.0000 | 0.2039 | fail | fail |
