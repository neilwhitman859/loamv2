# Session 9.5 - larger redesign selection under 152-case cap

Goal:
Review the Session 9.4 post-rerun failure audit and choose one materially
different redesign family to test on the frozen 152-case benchmark, with a hard
exploration cap before any all-pairs scale-up.

Primary deliverable:

- one design memo under `data/sprints/dedup/` that:
  - names the single redesign family recommended next
  - explains why it is materially different from the failed narrow-fix path
  - defines the smallest 152-case proof needed to validate it
  - estimates spend and keeps that proof within the user's $20 exploration cap
    before any all-pairs run

In scope:

- `data/sprints/dedup/session9_4_post_rerun_failure_audit.md`
- `data/sprints/dedup/session4_bakeoff_design.md`
- `data/sprints/dedup/rebuild_roadmap.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved*.{json,md}`
- `docs/DECISIONS.md` entries from 2026-04-21 about exploration spend
- dashboard / AGENTS / sessions updates

Out of scope:

- queue-building
- producer execution
- any all-pairs run
- running another rerun unless the design memo proves it is the right next proof
- changing `benchmark_v1`
- changing the frozen Session 4 hard gates

Questions to answer:

1. Which larger redesign family is most justified next:
   - pattern-family specialists,
   - stronger deterministic evidence synthesis,
   - or benchmark/gate redesign?
2. What is the smallest proof on the 152-case segment that would genuinely
   de-risk that redesign?
3. Can that proof be done inside the user's $20 exploration cap?
4. What exact user approval is needed before any all-pairs scale-up?

Stop rule:

- publish the design memo and stop
- do not rerun
- do not queue-build
