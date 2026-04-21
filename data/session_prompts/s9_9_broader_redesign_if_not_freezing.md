# Session 9.9 - broader redesign only if freeze is rejected

Use this only if you explicitly do **not** want to freeze at the Session 9.7
layered fallback endpoint.

Goal:
Scope the smallest broader redesign that is still honest about the Session 9.8
finding that no narrow recall-only continuation can recover production
readiness.

Primary deliverable:
- one design memo under `data/sprints/dedup/` that:
  - starts from `session9_7_layered_safety_sonnet_r2_narrow` as the fixed safety base
  - identifies which new family or signature groups would need positive-control treatment
  - defines the smallest benchmark-valid proof that is broader than Session 9.8 but still bounded
  - states expected upside, expected new safety risk, and expected spend before any proof is run

In scope:
- `data/sprints/dedup/session9_8_recover_production_from_layered_fallback.md`
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.{json,md}`
- `data/sprints/dedup/benchmark_v1.json`
- existing packet / normalized artifacts needed to map miss signatures to skip controls
- dashboard / AGENTS / sessions / sprint bookkeeping updates

Out of scope:
- queue-building
- all-pairs work
- benchmark mutation
- Session 4 gate changes
- automatic proof execution

Budget:
- target actual spend: `$0`
- hard cap before explicit approval of any proof: `$0`

Stop rule:
- publish the redesign memo and stop
- do not run a proof unless the user explicitly approves a broader proof after reviewing the memo
