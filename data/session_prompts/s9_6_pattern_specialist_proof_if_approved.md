# Session 9.6 - pattern-family specialist proof (if approved)

Goal:
Test one materially different redesign family on the frozen 152-case benchmark
without drifting into queue-building or all-pairs work.

Primary deliverable:

- one proof memo under `data/sprints/dedup/` that:
  - reports the composite 152-case scorecard for the routed-specialist proof
  - shows whether false merges stayed at zero
  - shows how many blind-core merge misses were recovered
  - states whether the redesign is strong enough to justify any further build

In scope:

- `data/sprints/dedup/session9_5_freeze_or_rebuild_strategy.md`
- `data/sprints/dedup/benchmark_v1.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved.{json,md}`
- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py`
- any new small helper needed for routed proof-only scoring
- dashboard / AGENTS / sessions / sprint bookkeeping updates

Out of scope:

- queue-building
- any all-pairs run
- changing `benchmark_v1`
- changing the frozen Session 4 gates
- broad packet redesign outside the routed specialist proof

Required proof design:

1. Keep `gemini_guardrailed_v2` as the conservative base path.
2. Add a routed proof layer for only these families:
   - `11.4.h`
   - `11.4.f`
   - `11.4.n`
   - `11.4.p`
3. Route only the 73 benchmark cases in those families through specialist
   logic.
4. Keep the remaining 79 benchmark cases on the base path.
5. Score the composite result across all 152 cases.

Success bar:

- `0` false merges overall
- `0` blind-core false merges
- blind-core missed merges `<=5`
- at least `30 / 47` targeted merge cases recovered inside the routed bundle
- full-benchmark `flag_rate_total <= 0.25`

Budget:

- external API spend hard-capped at `$20`
- target actual spend: `$5-10`

Stop rule:

- publish the proof memo and stop
- do not queue-build
- do not scale beyond the 152-case benchmark
