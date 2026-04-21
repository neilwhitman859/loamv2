# Session 7 - adjudication bakeoff v2 design

Goal:
Turn the failed `session6_first_real_bakeoff_v1` result into a tightly scoped v2 bakeoff plan that has a plausible path to clearing the Session 4 production and fallback gates without changing `benchmark_v1` or pretending v1 was good enough.

Primary deliverable:
A durable v2 design memo that explains why v1 failed, names the exact changes to test next, and defines the minimum next run needed before any queue-building work can start.

In scope:
- read and synthesize:
  - `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1.md`
  - `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1.json`
  - `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1_error_ledger.jsonl`
  - `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1_manifest.json`
  - raw + normalized contender outputs for the same run
  - `data/sprints/dedup/session4_bakeoff_design.md`
  - `data/sprints/dedup/evidence_packet_v1.md`
  - `pipeline/identity/bakeoff_packet_v1.py`
  - `pipeline/identity/bakeoff_harness_v1.py`
  - `pipeline/identity/bakeoff_run_v1.py`
- identify failure modes by contender:
  - false-merge patterns
  - invalid/schema-invalid output patterns
  - over-flag / missed-merge patterns
  - packet-evidence blind spots
- propose the smallest viable v2 changes:
  - packet changes, if any
  - runner/prompt changes, if any
  - contender-lineup changes, if justified
  - consensus logic changes, if justified
- write one recommended v2 run spec and one backup option

Out of scope:
- re-running the bakeoff
- changing `benchmark_v1`
- changing Session 4 score math or hard gates unless there is a very strong documented reason and it is explicitly surfaced as a proposal rather than silently applied
- queue-building
- merge SQL
- execution against real producer rows

Required outputs:
- a markdown memo under `data/sprints/dedup/` or `data/sprints/dedup/bakeoff_v1/` that includes:
  - v1 failure summary
  - contender-by-contender diagnosis
  - cross-contender failure clusters
  - recommended v2 changes
  - exact next-run spec
  - explicit statement on whether queue-building remains blocked
- dashboard + session log updates

Guardrails:
- preserve `session6_first_real_bakeoff_v1` as the canonical v1 artifact
- do not silently soften the gates just to manufacture a winner
- if proposing contender changes, explain exactly why the frozen v1 lineup failed and what the replacement is expected to fix
- if proposing packet changes, keep hidden benchmark overlay out of model-visible requests
- prioritize auditable, reproducible improvements over ad hoc judgment calls

Stop when:
- the repo contains a clear v2 design artifact that the next session can execute without reopening first principles
- the wrap-up plainly states whether queue-building is still blocked
