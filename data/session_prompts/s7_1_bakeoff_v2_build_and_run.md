# Session 8 - implement adjudication bakeoff v2 and rerun

Goal:
Implement the packet/runner changes locked in `data/sprints/dedup/session7_bakeoff_v2_design.md`, prove the new contract on a small failure-focused subset, and then run the full 152-case v2 bakeoff without changing `benchmark_v1` or the frozen Session 4 gates.

Primary deliverable:
A canonical `session7_first_real_bakeoff_v2` artifact set under `data/sprints/dedup/bakeoff_v1/` or a sibling `bakeoff_v2/` directory containing:
- packet artifacts
- request artifacts
- raw outputs
- normalized outputs
- scored summary
- error ledger
- v1 vs v2 diff table

In scope:
- `data/sprints/dedup/session7_bakeoff_v2_design.md`
- `data/sprints/dedup/benchmark_v1.json`
- `data/sprints/dedup/evidence_packet_v1.md`
- `pipeline/identity/bakeoff_packet_v1.py`
- `pipeline/identity/bakeoff_harness_v1.py`
- `pipeline/identity/bakeoff_run_v1.py`
- new v2 packet / harness / runner modules if cleaner than mutating v1
- dashboard + session-log + sprint journal updates

Out of scope:
- changing `benchmark_v1`
- softening Session 4 score math or hard gates
- queue-building
- merge SQL
- producer-row execution
- reopening `PARENT_CHILD`

Exact required changes:
1. Build packet v2 with a flat citeable evidence ledger.
   - Every fact the adjudicator is expected to cite must have a legal `ref_id`.
   - Cover lexical, geography, catalog, contradiction, and retrieval facts.
   - Hidden benchmark overlay must still be fully stripped from model-visible packets.
2. Add real official-domain retrieval at packet-build time.
   - For each side, either store 1-2 official hits with claim summaries or store an explicit unresolved record.
   - Another 152/152 `retrieval = missing` run is not acceptable.
3. Make the adjudicator prompt ref-safe.
   - Include the exact allowed `ref_id`s for that packet.
   - Show one tiny bad citation example and one tiny good citation example.
4. Rebuild consensus on normalized child rows, not raw child outputs.
   - Child schema-invalid rows should become child failures, not consensus evidence.
   - `MERGE` requires both valid children to agree on verdict and survivor.
   - Otherwise return a contract-valid `FLAGGED`.
5. Add a merge veto for the highest-risk contradiction patterns.
   - Do not allow final `MERGE` when shared-surname split risk, holdco/product-tier risk, or country conflict is present unless the packet also contains explicit official-domain continuity support.

Minimum contender lineup for the full v2 run:
- `deterministic_control_v1` (shadow only)
- `sonnet_guardrailed_v2`
- `gemini_guardrailed_v2`
- `sonnet_gemini_consensus_v2`

Backup swap if Gemini still looks too merge-happy on the proof subset:
- replace `gemini_guardrailed_v2` with `gpt5mini_guardrailed_v2`
- keep packet v2 constant

Required execution order:
1. Build packet v2 on all 152 benchmark cases.
2. Run a 20-30 case proof subset focused on v1 false merges, v1 missed merges, and a few clean controls.
3. Stop if:
   - any hidden-field leak appears
   - any contender has `schema_valid_rate < 1.0` on the proof subset
   - consensus still inherits child ref breakage
4. If proof passes, run the full `session7_first_real_bakeoff_v2`.
5. Publish a v1 vs v2 diff table.

Success condition:
The repo contains a full v2 bakeoff artifact set and a plain statement of one of two outcomes:
- one production path plus one fallback path cleared the frozen gates, or
- queue-building remains blocked

Do not stop after implementation only. The session is only complete when the v2 proof subset and full rerun are done, or when a concrete blocker with artifacts is documented.
