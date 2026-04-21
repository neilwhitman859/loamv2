# Session 9 - v3 continuity audit and redesign

Goal:
Audit the false-merge clusters from `session7_first_real_bakeoff_v2`, identify exactly which official-continuity / alias heuristics are over-permissive, and lock the minimum v3 redesign needed before any further proof rerun.

Primary deliverable:
One durable design memo under `data/sprints/dedup/` that:
- buckets the v2 false merges by continuity failure mode
- distinguishes packet-side retrieval/resolution failures from adjudicator reasoning failures
- proposes the minimum packet/guardrail changes for v3
- states whether a v3 proof subset should reuse the current 28-case proof slice or add targeted continuity cases

In scope:
- `data/sprints/dedup/bakeoff_v2/`
- `data/sprints/dedup/session7_bakeoff_v2_design.md`
- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py`
- `docs/IDENTITY_RULES.md` only if a rule clarification is clearly needed
- dashboard + session-log updates

Out of scope:
- running another full bakeoff
- queue-building
- merge SQL
- producer-row execution
- changing `benchmark_v1` or the frozen Session 4 gates

What to audit first:
1. Cases where `official_continuity_shared_domain` or `official_continuity_alias_*` appeared in false merges.
2. Whether same-domain resolution is collapsing importer/merchant/brand-family pages into false continuity.
3. Whether alias continuity should require stronger evidence than name mention in title/snippet.
4. Whether the merge veto list should expand beyond the current three high-risk patterns.

Success condition:
The repo contains a clear v3 redesign memo with concrete packet/guardrail changes and a recommendation for the next proof rerun scope. Do not jump straight into another rerun without first locking the continuity-evidence fix.
