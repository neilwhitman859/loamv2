# Sprint 6: Dedup (Producers)

**Status:** Opening. Detailed design deferred to B6.1 (planning session).
**Opened:** 2026-04-16
**Sprint number:** 6

---

## Goal (user directive, 2026-04-16)

> Dedup all 10,683 producers. Have a good method for deduping producers added
> in the future. Throw the kitchen sink at this — use AI in new and creative ways.

**Out of scope** (per B5.7 and B5.8 framing):
- Wine dedup (Sprint 7)
- Prompt v2 + L3 fact-check gate (Sprint 8)
- Re-enrichment (Sprint 8)
- Demo sharing (later)

---

## Sequencing ahead (confirmed 2026-04-16)

```
Sprint 6: Producer dedup (this one)
Sprint 7: Wine dedup (~4,079 suspected, 30-35 dangerous false-positive patterns from Q3 audit)
Sprint 8: Prompt v2 + L3 fact-check gate + re-enrichment + share
```

Re-enrichment is deliberately deferred until prompt + gate work lands, so we
don't lock in the current-prompt ceiling for the whole corpus. See
`bakeoff/scores/tournament_results.md` for the rationale.

---

## Detailed design deferred to B6.1

B5.8 (this block) is a sprint-boundary close/open only. The real planning
happens in B6.1.

**Session prompt for B6.1:** `data/session_prompts/b6_1_planning.md`

---

## What's been gathered already (so B6.1 doesn't have to re-research)

### Current producer state

- **10,683 producers** total (active, no dedup-flag column on producers table — only wines have `duplicate_of`)
- **67 exact-name duplicate groups** → 69 removable producers by exact normalized match
- Producers merged in S4.1 as test cases: Ridge (61+68→113), López de Heredia (8+11→18), CIRQ (4+1→4)

### Dedup model reference (Task 1 bake-off, 200 labeled pairs)

| Model | Acc | FPR | FNR | $/4K pairs |
|-------|----:|----:|----:|-----------:|
| claude-sonnet-4.6 | 94.5% | 2.1% | 14.0% | $13.04 |
| claude-haiku-4.5 | 93.5% | **0.7%** | 21.1% | $3.25 |
| gpt-5.4-mini | 93.5% | 4.2% | 12.3% | $1.22 |
| gemini-3-flash-preview | 92.5% | 9.1% | 3.5% | $0.45 |
| gemini-3.1-flash-lite | 92.5% | 5.6% | 12.3% | $0.07 |
| deepseek-v3.2 | 89.0% | **14.7%** | 1.75% | $0.35 (unsafe for dedup) |

**Key finding:** Haiku has 3× lower FPR than next-best — safest for dedup where false merges are permanent. Cheap models (flash-lite) viable for first-pass triage if paired with a safer second-pass model.

### Existing tooling (don't rebuild from scratch)

- `pipeline/promote/dedup_wines.py` — exact-match wine dedup, 0-vintage case (reference pattern)
- `pipeline/promote/wine_merge.py` — merge executor (529 lines, proven)
- `pipeline/promote/batch_matcher.py` — fuzzy producer matcher (1,417 lines)
- `pipeline/promote/generic_matcher.py` — generic name matching
- `pipeline/identity/dedup_deterministic.py` — skeleton (NotImplementedError, placeholder)
- `match_decisions` table — audit trail for cross-source matching
- `match_producer_fuzzy()` RPC — pg_trgm similarity search

### Open design questions for B6.1

1. **Signal fusion logic** — how to combine trigram / prefix-normalization / wine-overlap / region-overlap / external-ID-overlap / website signals into a merge decision
2. **Blocking strategy** — same country? Same first-N chars? What fraction of producers have NULL country?
3. **Embedding-based clustering** — worth adding as a first-class signal? (~$1 total via OpenAI ada)
4. **Two-stage LLM ladder** — cheap triage (flash-lite ~$0.07) → Haiku safety pass on ambiguous — or single-pass Haiku on everything
5. **Web-grounded verification** — for high-stakes UNCERTAIN pairs only, or broader?
6. **S4.1 merges as validation gold** — held-out test set, few-shot examples, or both
7. **Human-in-the-loop review gate** — threshold at which merges require explicit user approval
8. **Parent/child relationships** — how to distinguish "duplicate" from "related entities" (LVMH ↔ Krug is related, NOT duplicate)
9. **Merge reversibility** — is `wine_merge.py` merge reversible if we discover a wrong call later?
10. **"Ongoing pipeline for future imports"** — in-sprint deliverable or follow-up?

---

## Budget

TBD in B6.1. Preliminary range: $5-15 depending on which "kitchen sink" signals and LLM passes are wired in.
