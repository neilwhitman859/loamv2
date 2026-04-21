# Session 9.2 - unresolved-official backstop for secondary-driven false merges

Goal:
Design and implement the minimum follow-on guardrail needed after `session9_v3_continuity_proof_subset` failed on three residual add-on cases, then rerun proof only again.

Primary deliverable:

- one narrow packet/harness change that blocks the remaining unresolved-official false merges
- fresh proof-only artifacts showing whether cases `blind_core_audit_048`, `blind_core_audit_080`, and `tail_random_sample_008` are now clean
- a short memo saying pass/fail and whether another proof-only cycle is still needed

In scope:

- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py` only as needed for proof gating or memo output
- fresh proof artifacts under `data/sprints/dedup/bakeoff_v2/`
- dashboard / AGENTS / sessions updates

Out of scope:

- changing `benchmark_v1`
- changing Session 4 hard gates
- queue-building
- producer execution
- any 152-case rerun without explicit user approval after a clean proof

Known residual pattern from Session 9.1:

- bogus hard continuity is mostly gone
- the remaining false merges are secondary/catalog overreach under unresolved-official evidence
- `blind_core_audit_048` still false-merges for Sonnet, Gemini, and consensus
- `blind_core_audit_080` still false-merges for Sonnet
- `tail_random_sample_008` still false-merges for Gemini

Suggested focus:

1. inspect those three packets and normalized outputs side by side
2. decide whether the fix belongs in:
   - a broader unresolved-official merge veto,
   - a new narrow family/portfolio risk flag,
   - or a stricter rule on when secondary evidence may support `MERGE`
3. keep the change as small as possible
4. rerun proof only

Stop rule:

- if proof still fails, publish the diagnosis and stop
- do not run the 152-case rerun
