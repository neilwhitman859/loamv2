# Session 6 - first real adjudication bakeoff

Goal:
Run the first full producer-dedup adjudication bakeoff through the new Session 5 packet/request/normalize/score path so the frozen Session 4 contender set produces a real winner-selection table instead of just a deterministic proof sample.

Primary deliverable:
A completed v1 bakeoff run with normalized outputs, scored summaries, error ledger, and winner-selection table for the frozen contender set.

In scope:
- use the existing `pipeline/identity/bakeoff_packet_v1.py` and `pipeline/identity/bakeoff_harness_v1.py` plumbing rather than redesigning the contract
- implement the real contender runners/adapters needed for:
  - `haiku_single_v1`
  - `gemini_single_v1`
  - `gpt5mini_single_v1`
  - `haiku_gemini_consensus_v1`
  - `sonnet_single_v1`
- write raw contender outputs into the harness-compatible JSONL shape
- normalize every contender output through the fail-closed Session 4 contract
- score the full 152-case benchmark and publish:
  - overall summary
  - core/tail breakdown
  - stratum breakdown
  - error ledger
  - winner-selection table
- stop if any frozen contender model is unavailable; do not silently substitute another model

Out of scope:
- changing the frozen contender set
- changing `benchmark_v1`, `evidence_packet_v1`, score math, hard gates, or `FLAGGED` semantics
- expanding `PARENT_CHILD`
- building execution queues
- merge SQL
- redesigning retrieval or packet schema mid-run

Required inputs:
- `data/sprints/dedup/session4_bakeoff_design.md`
- `data/sprints/dedup/evidence_packet_v1.md`
- `data/sprints/dedup/benchmark_v1.json`
- `pipeline/identity/bakeoff_packet_v1.py`
- `pipeline/identity/bakeoff_harness_v1.py`
- `data/sprints/dedup/bakeoff_v1/` Session 5 proof artifacts
- `docs/DECISIONS.md`
- `AGENTS.md`

Implementation targets:
- add runner code or small runner modules for each frozen contender
- materialize full request / raw / normalized / scored artifacts under `data/sprints/dedup/bakeoff_v1/`
- produce one canonical run name for the first real bakeoff and keep all outputs grouped under it

Guardrails:
- `benchmark_overlay` may exist only in stored packet rows, never in model-visible requests
- invalid outputs must normalize to `FLAGGED`, not disappear
- no silent model substitution
- consensus output must still normalize through the exact same `adjudication_output_v1` contract as single-model outputs
- if a contender fails badly, record that in the score output; do not drop it from the lineup

Suggested execution order:
1. Rebuild or verify packets with `python -m pipeline.identity.bakeoff_packet_v1`
2. Prepare request wrappers for the frozen contenders
3. Implement and run each contender into raw JSONL outputs
4. Normalize all contender outputs
5. Score the full benchmark
6. Publish the markdown scorecard + winner-selection table + error ledger

Stop when:
- all frozen contenders have either completed real runs or explicitly failed for an auditable reason
- normalized result rows exist for every attempted contender
- the scored output can answer whether any production path and fallback path actually pass the Session 4 gates
- the next session can move to queue-building only if the gates truly clear
