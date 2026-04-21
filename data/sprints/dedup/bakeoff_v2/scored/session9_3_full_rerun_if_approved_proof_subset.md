# session9_3_full_rerun_if_approved_proof_subset - adjudication bakeoff v2

- Generated: 2026-04-21T08:30:05-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 36 / 152
- Full benchmark run: no

| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | 0.4167 | 1 | 8 | 6 | 6 | 1.0000 | 0.3333 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_guardrailed_v2 | 0.3889 | 0 | 3 | 11 | 8 | 1.0000 | 0.5278 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| gemini_guardrailed_v2 | 0.3056 | 0 | 5 | 10 | 10 | 1.0000 | 0.5556 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_gemini_consensus_v2 | 0.2778 | 0 | 2 | 13 | 11 | 1.0000 | 0.6667 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
