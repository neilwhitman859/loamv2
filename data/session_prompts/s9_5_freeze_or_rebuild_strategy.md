# Session 9.5 - freeze or rebuild strategy

Goal:
Review the Session 9.4 post-rerun failure audit and make the next high-level
Sprint 6 decision: freeze the current adjudication path as non-execution-ready,
or define a larger replacement strategy that does not rely on one more narrow
packet-side rerun.

Primary deliverable:

- one decision memo under `data/sprints/dedup/` that:
  - accepts or rejects the Session 9.4 freeze recommendation
  - if freezing: records the rationale and the exact handoff state
  - if continuing: proposes one larger redesign family and explains why it is
    materially different from the failed narrow-fix path

In scope:

- `data/sprints/dedup/session9_4_post_rerun_failure_audit.md`
- `data/sprints/dedup/session4_bakeoff_design.md`
- `data/sprints/dedup/rebuild_roadmap.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved*.{json,md}`
- dashboard / AGENTS / sessions updates

Out of scope:

- queue-building
- producer execution
- another rerun
- changing `benchmark_v1`
- changing the frozen Session 4 hard gates

Questions to answer:

1. Should the current adjudication path be frozen as a benchmark artifact and
   removed from the execution-critical path?
2. If not, what larger redesign family is justified:
   - pattern-family specialists,
   - stronger deterministic evidence synthesis,
   - or benchmark/gate redesign?
3. What exact user approval is required before any further Sprint 6 spend?

Stop rule:

- publish the decision memo and stop
- do not rerun
- do not queue-build
