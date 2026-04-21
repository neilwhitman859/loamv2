# Session 9.8 - recover production readiness from the layered fallback

Goal:
Use the new Session 9.7 layered fallback contender as the fixed safety base and
see whether one more narrow redesign can reduce the remaining production-blocking
misses without reintroducing false merges.

Primary deliverable:
- one focused memo under `data/sprints/dedup/` that:
  - audits the remaining misses in
    `session9_7_layered_safety_sonnet_r2_narrow`
  - identifies the smallest credible recall-only continuation
  - runs at most one bounded proof if the continuation is concrete enough
  - states clearly whether the adjudication path is now production-ready,
    fallback-only, or should be frozen

In scope:
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.{json,md}`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow_memo.md`
- `pipeline/identity/bakeoff_layered_safety_gate.py`
- existing Session 9.3 / 9.6 / 9.7 packet and normalized artifacts as needed
- dashboard / AGENTS / sessions / sprint bookkeeping updates

Out of scope:
- queue-building
- all-pairs work
- benchmark mutation
- Session 4 gate changes
- undoing the new deterministic anti-trap vetoes
- broad new model bakeoffs

Budget:
- target actual spend: `$0-3`
- hard cap: `$10`

Stop rule:
- publish the memo and stop
- if no narrow recall-only continuation is credible, say so explicitly and
  recommend freezing at the Session 9.7 fallback state

