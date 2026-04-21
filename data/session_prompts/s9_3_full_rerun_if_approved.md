# Session 9.3 - full 152-case rerun on proof-cleared `v2.1` packets

Goal:
Run the full 152-case adjudication bakeoff rerun using the proof-cleared `session9_2_unresolved_official_backstop` setup, but only after explicit user approval.

Primary deliverable:

- one fresh full-run artifact set under `data/sprints/dedup/bakeoff_v2/`
- updated score JSON / Markdown / diff outputs
- a short memo stating whether any contender now clears the frozen Session 4 production or fallback gates

In scope:

- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py`
- full-run artifacts under `data/sprints/dedup/bakeoff_v2/`
- dashboard / AGENTS / sessions updates

Out of scope:

- changing `benchmark_v1`
- changing Session 4 hard gates
- queue-building
- producer execution
- any new redesign work unless the rerun exposes a fresh blocker

Required setup:

1. Reuse the proof-cleared Session 9.2 code path.
2. Force packet rebuild so the run definitely uses fresh `v2.1` packets.
3. Run the full rerun only after confirming the user explicitly approved it in-thread.

Run shape:

- command should be the full rerun variant of `python -m pipeline.identity.bakeoff_run_v2`
- include `--run-name session9_3_full_rerun_if_approved`
- include `--force-rebuild-packets`
- include `--full-after-proof`

Stop rule:

- if proof unexpectedly fails again, stop and do not continue into the 152-case rerun
- if the full rerun completes, publish the scorecard / diff and stop before any queue-building
