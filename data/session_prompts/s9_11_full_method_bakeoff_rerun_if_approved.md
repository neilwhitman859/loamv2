# Session 9.11 - full method bakeoff rerun if approved

Goal:
Run the full 152-case method bakeoff using only the three Session 9.10 proof
survivors and publish a scorecard plus go/no-go memo under the frozen gates.

Primary deliverable:
- one rerun memo under `data/sprints/dedup/` that:
  - reports the full 152-case results for `merge_proposer_plus_veto_v1`,
    `expanded_layered_router_v1`, and `evidence_digest_then_judge_v1`
  - compares those results against the frozen Session 9.7 fallback control
  - names the best surviving artifact, if any
  - states clearly whether Sprint 6 should proceed toward queue-building or
    freeze at the best non-production artifact

In scope:
- `data/sprints/dedup/session9_10_method_bakeoff_proof_subset.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_10_method_bakeoff_proof_subset.{json,md}`
- `pipeline/identity/bakeoff_method_bakeoff_proof.py` and closely related
  bakeoff helpers needed to scale the proof contenders to the full benchmark
- full-rerun scored artifacts under `data/sprints/dedup/bakeoff_v2/`
- dashboard / AGENTS / sessions / sprint bookkeeping updates

Out of scope:
- queue-building
- benchmark mutation
- Session 4 gate changes
- any new contender beyond the three Session 9.10 survivors
- all-pairs execution

Budget:
- target actual spend: `$0`
- hard cap: `$0` unless the user explicitly approves converting a survivor into
  a paid model-backed rerun

Stop rule:
- publish the rerun memo and stop
- if all three survivors fail the frozen production gate, recommend freezing at
  the best surviving non-production artifact rather than opening another
  redesign in the same session
