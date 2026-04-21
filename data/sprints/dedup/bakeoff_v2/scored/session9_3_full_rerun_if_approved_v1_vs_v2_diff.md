# session9_3_full_rerun_if_approved - v1 vs v2 diff

- Baseline: `session6_first_real_bakeoff_v1`

| Contender | Baseline | dExact acc | dFalse merge | dHard missed | dSoft missed | dSafe flag | dSchema valid | dAuditability | dFlag rate | Prod gate | Fallback gate |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | deterministic_control_v1 | -0.0131 | -37 | +2 | +11 | +26 | +0.0395 | +0.0395 | +0.2435 | fail -> fail | fail -> fail |
| sonnet_guardrailed_v2 | sonnet_single_v1 | -0.1776 | -10 | -4 | +34 | +7 | +0.1118 | +0.1118 | +0.2697 | fail -> fail | fail -> fail |
| gemini_guardrailed_v2 | gemini_single_v1 | -0.0395 | -10 | +5 | +29 | -18 | +0.2895 | +0.2895 | +0.0724 | fail -> fail | fail -> fail |
| sonnet_gemini_consensus_v2 | haiku_gemini_consensus_v1 | +0.2829 | +0 | +4 | -2 | -45 | +0.7303 | +0.7303 | -0.3093 | fail -> fail | fail -> fail |
