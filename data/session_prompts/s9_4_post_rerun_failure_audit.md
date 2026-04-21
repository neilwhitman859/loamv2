# Session 9.4 - post-rerun failure audit

Goal:
Audit the Session 9.3 full-rerun result and decide whether there is a small, evidence-backed redesign left that could clear the frozen gates without reintroducing unsafe false merges.

Primary deliverable:

- one markdown audit memo under `data/sprints/dedup/` that:
  - identifies the dominant remaining failure clusters from `session9_3_full_rerun_if_approved`
  - separates "fixable with one narrow change" from "structural wall / stop here"
  - recommends either one minimal next redesign or a freeze/pivot decision

In scope:

- `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved*.{json,md}`
- normalized outputs and request wrappers for Session 9.3
- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py`
- dashboard / AGENTS / sessions updates

Out of scope:

- queue-building
- producer execution
- changing `benchmark_v1`
- changing Session 4 hard gates
- running another full rerun unless the audit proves a very small fix and the user explicitly approves it

Questions to answer:

1. Are the remaining misses/flags dominated by one or two packet-side patterns, or is the failure now broadly distributed?
2. Is the post-backstop problem mainly:
   - hard missed merges,
   - soft missed merges / over-flagging,
   - or both?
3. Is there one minimal redesign with a credible chance of improving recall while preserving the new false-merge safety?
4. If not, should the current adjudication path be treated as a non-execution-ready artifact and paused?

Stop rule:

- publish the audit memo and stop
- do not queue-build
- do not rerun unless the user explicitly approves a proven narrow next change
