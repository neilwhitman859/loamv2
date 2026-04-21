# Session 9.1 - implement v3 continuity tightening and rerun proof only

Goal:
Implement the minimum v3 continuity redesign from `data/sprints/dedup/session9_v3_continuity_audit.md`, then rerun an expanded proof subset only. Do not run the full 152-case bakeoff unless the proof passes cleanly and the user explicitly asks for the full rerun.

Primary deliverable:

- updated v3 packet/guardrail code
- proof-run artifacts for the expanded continuity-focused proof subset
- one short run memo stating pass/fail and whether a full rerun is now justified

In scope:

- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py` only as needed for proof subset selection / naming
- `data/sprints/dedup/bakeoff_v2/` proof artifacts
- dashboard/session-log/AGENTS updates

Out of scope:

- changing `benchmark_v1`
- changing the frozen Session 4 gates
- queue-building
- producer execution
- a full 152-case rerun without a clean proof pass and explicit user go-ahead

Required changes:

1. Split continuity into hard vs soft.
   - Only hard continuity may waive the merge veto.
2. Stop treating `serper.organic.domain_match` as official-domain resolution for continuity purposes.
   - Keep those hits as secondary evidence only.
3. Tighten alias continuity.
   - Require full normalized other-name phrase on a hard-official page.
   - Do not mint alias continuity from surname-only token overlap.
4. Downgrade shared-domain continuity unless page-level brand identity also aligns.
5. Add one narrow risk flag for ownership / operator / acquisition relationships that do not prove identity.
6. Expand the proof subset from 28 to 36 cases by adding these 8 targeted continuity cases:
   - `blind_core_audit_041`
   - `blind_core_audit_067`
   - `blind_core_audit_048`
   - `blind_core_audit_052`
   - `known_false_merge_patterns_005`
   - `tail_random_sample_008`
   - `blind_core_audit_076`
   - `blind_core_audit_080`

Proof pass criteria:

- no hidden-field leaks
- no schema-validity failures
- zero false merges on the 8-case continuity add-on
- clear false-merge improvement vs the current proof behavior

If the proof fails:

- stop after publishing the proof artifacts and diagnosis
- do not run the full 152

If the proof passes:

- publish the proof memo
- stop and tee up the next session for explicit user approval on the full rerun
