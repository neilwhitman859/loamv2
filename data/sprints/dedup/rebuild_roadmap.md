# Producer Dedup Rebuild Roadmap

Working plan from the 2026-04-20 Codex audit. This is a focused rebuild for **production-ready merge quality**, not a broad re-architecture of Loam.

## Progress Tracker

- [x] Freeze direction: merge-only rebuild
- [x] Tighten `docs/IDENTITY_RULES.md` in place
- [x] Redefine `core` vs `tail` around product risk
- [x] Audit current pair corpus / blocking strategies
- [x] Freeze benchmark set
- [x] Design evidence-packet schema
- [x] Design broad bakeoff on methods and models
- [x] Build packet generator + evaluation harness
- [ ] Build core adjudication queue
- [ ] Build tail adjudication queue
- [ ] Publish pre-execution scorecard

## Session Rule

Use **one primary deliverable per session**. A session is successful if it produces a durable artifact the next session can build on: a table, a benchmark file, a spec, a report, or a queue. Do not mix pair-corpus work, model bakeoff work, and execution prep in the same session unless one is tiny and clearly subordinate.

## Goal

Ship producer dedup with these quality bars:
- Core false merges: `0`
- Core missed merges: `<=3%`
- Tail false merges: `<=1%`
- Tail missed merges: `<=10%`

Execution scope is **merge-only**. `PARENT_CHILD` can return later as a metadata enhancement once merge quality is no longer the blocker.

## What We Keep

- `docs/IDENTITY_RULES.md` as the rulebook, corrected in place rather than replaced wholesale.
- The existing `producer_dedup_pairs` corpus as a benchmark artifact.
- The Phase 1 ledger and sampled Chrome work as evidence about failure modes.
- Deterministic normalization primitives that are already useful.
- The existing core/tail concept, but redefined around product risk.

## Ground Truth

Ground truth for adjudication is:
1. Official-domain evidence.
2. Local wine-list coherence.
3. Deterministic identity rules.

Search is for retrieval, not truth by itself.

## Core vs Tail

Use the production-risk definition in `metrics_and_goals.md`.

Short version:
- **Core** = producers with meaningful visibility or blast radius.
- **Tail** = thin-signal long-tail producers.

## Phases

### Phase 1 - Rules and benchmark set

Deliverables:
- Tightened `docs/IDENTITY_RULES.md`
- Frozen benchmark set combining:
  - the 100-pair blind core audit already completed,
  - known false-merge patterns,
  - known missed-merge patterns,
  - a clean random sample of tail pairs

Gate:
- We can explain exactly why each benchmark verdict is right.

### Phase 2 - Pair-corpus rebuild

Deliverables:
- Audit of current blocking strategies for recall and reviewability
- New candidate strategy spec
- Comparison report: old pair corpus vs rebuilt corpus

Design principle:
- Candidate generation should over-capture legitimate duplicates without flooding review with low-value junk.
- The current corpus is a benchmark, not sacred input.

Gate:
- Known duplicate clusters are caught.
- Review volume is materially more defensible than the current corpus.

### Phase 3 - Evidence packet

Deliverables:
- Standard per-pair evidence packet with:
  - row names and normalized forms
  - country and region summary
  - local wine-list summary
  - contradiction flags
  - official-domain evidence when available
  - survivor recommendation when merge is supported

Design principle:
- The model should judge from evidence we trust, not from free-form intuition.

Gate:
- A human can audit a pair quickly from the packet alone.

### Phase 4 - Broad bakeoff

Compare both **methods** and **models**, not just models.

Bake off:
- Deterministic control vs grounded adjudication
- Single-model adjudicators
- Cross-family consensus adjudication
- Cost / auditability tradeoffs under one frozen packet

Frozen contender set:
- `deterministic_control_v1` (shadow baseline only)
- `haiku_single_v1`
- `gemini_single_v1`
- `gpt5mini_single_v1`
- `haiku_gemini_consensus_v1`
- `sonnet_single_v1`

Score on:
- false merges,
- missed merges,
- survivor correctness,
- cost per pair,
- auditability of reasoning.

Gate:
- Pick one production path and one fallback, with measured tradeoffs and explicit queue-building gates.

### Phase 5 - Core adjudication

Deliverables:
- Core merge queue
- Core flagged queue
- Core skip queue

Rule:
- High-visibility pairs get the best evidence and the strictest review.

Gate:
- Core pre-execution scorecard passes.

### Phase 6 - Tail adjudication

Deliverables:
- Tail merge queue
- Tail flagged queue
- Tail skip queue

Rule:
- Tail stays conservative. When in doubt, skip or flag.

Gate:
- Tail sample meets error targets.

### Phase 7 - Pre-execution scorecard

Deliverables:
- Single markdown report with:
  - measured false-merge rate,
  - measured missed-merge rate,
  - biggest-risk flagged pairs,
  - survivor audit summary,
  - recommendation on whether execution is safe

Rule:
- No DB execution before this report is clean enough.

## Focus Rules

- One deliverable per phase.
- No new execution code until the evaluation harness is in place.
- No `PARENT_CHILD` detours.
- No assumption that old Claude outputs are ground truth.
- Keep historical artifacts for regression comparison, not authority.

## Immediate Next Steps

1. Run the first adjudication bakeoff against the frozen contender set and choose one production path plus one fallback.
2. Build execution queues only if the bakeoff clears the hard gates.
3. Publish the pre-execution scorecard only after the winning path survives the hard gates.
