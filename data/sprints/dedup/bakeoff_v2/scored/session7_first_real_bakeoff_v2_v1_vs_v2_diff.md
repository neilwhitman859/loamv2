# session7_first_real_bakeoff_v2 - v1 vs v2 diff

- Baseline: `session6_first_real_bakeoff_v1`

| Contender | Baseline | dExact acc | dFalse merge | dHard missed | dSoft missed | dSafe flag | dSchema valid | dAuditability | dFlag rate | Prod gate | Fallback gate |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | deterministic_control_v1 | -0.2171 | -3 | -29 | +25 | +40 | +0.0395 | +0.0395 | +0.4277 | fail -> fail | fail -> fail |
| sonnet_guardrailed_v2 | sonnet_single_v1 | +0.0066 | +15 | +1 | -9 | -8 | +0.1118 | +0.1118 | -0.1119 | fail -> fail | fail -> fail |
| gemini_guardrailed_v2 | gemini_single_v1 | +0.1579 | +21 | +1 | -12 | -34 | +0.2895 | +0.2895 | -0.3026 | fail -> fail | fail -> fail |
| sonnet_gemini_consensus_v2 | haiku_gemini_consensus_v1 | +0.4408 | +18 | +5 | -41 | -49 | +0.7303 | +0.7303 | -0.5922 | fail -> fail | fail -> fail |
