# Session 9.10 - method bakeoff proof subset if approved

Goal:
Implement the broader method-bakeoff design from Session 9.9 and test the new
method classes on a bounded proof subset before any full 152-case rerun.

Primary deliverable:
- one proof memo under `data/sprints/dedup/` that:
  - names the implemented method contenders
  - defines the proof subset composition
  - reports proof results against the frozen Session 9.7 control
  - downselects at most three contenders for any later full rerun
  - states clearly whether Sprint 6 should proceed to a full method bakeoff or
    freeze at the Session 9.7 fallback state

In scope:
- `data/sprints/dedup/session9_9_method_bakeoff_design.md`
- `data/sprints/dedup/session9_8_recover_production_from_layered_fallback.md`
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.{json,md}`
- `pipeline/identity/` bakeoff helpers needed to implement the new method contenders
- proof artifacts under `data/sprints/dedup/bakeoff_v2/`
- dashboard / AGENTS / sessions / sprint bookkeeping updates

Out of scope:
- queue-building
- all-pairs execution
- benchmark mutation
- Session 4 gate changes
- full 152-case rerun unless the proof memo itself recommends it and there is
  still budget headroom

Budget:
- target actual spend: `$0-5`
- hard cap: `$8`

Stop rule:
- publish the proof memo and stop
- if no contender survives the proof subset cleanly, say so explicitly and
  recommend freezing at `session9_7_layered_safety_sonnet_r2_narrow`
