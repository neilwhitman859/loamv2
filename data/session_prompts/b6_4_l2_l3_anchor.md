# B6.4 — L2 Haiku rich-prompt + L3 Sonnet web-grounded + Anchor Set + Ablation

You are opening B6.4 of Sprint 6 (Producer Dedup). B6.3 completed: schema + IDENTITY_RULES §11 + blocking (151,150 pairs, 10 strategies) + L1 Haiku batched on all pairs. L1 produced verdict + confidence per pair written to `producer_dedup_pairs` with `method_name='l1_haiku_batch'`.

Full plan: [`data/sprints/dedup/plan.md`](data/sprints/dedup/plan.md). Journal: [`data/sprints/dedup/journal.md`](data/sprints/dedup/journal.md). B6.3 log: [`data/stats/b6_3_l1_full.log`](data/stats/b6_3_l1_full.log).

---

## Pre-B6.4 state (done in B6.3, not your job)

1. **Schema ready:** `producer_dedup_pairs` extended with producer_id_a/_b, method_name, confidence, reasoning, cost_cents, signals/ttb_evidence/web_evidence jsonb, flag_reason. `producer_merge_history` created (empty, for B6.6 merge audit).
2. **IDENTITY_RULES §11 live** at [`docs/IDENTITY_RULES.md`](docs/IDENTITY_RULES.md#11-producer-identity-rules). Reviewed + approved by user in B6.3. 11.4.g has "label-appearance carve-out" for holdco-style names.
3. **Blocking done** — 151,150 pairs in `producer_dedup_pairs` with method_name='blocking'. Eight active strategies (S1-S2-S5-S6-S7-S8-S9-S10-S11; S3 embedding + S4 first-3-char dropped). See [`pipeline/identity/producer_blocking.py`](pipeline/identity/producer_blocking.py).
4. **L1 done** — all ~151K pairs processed by claude-haiku-4-5 via Anthropic SDK direct with prompt caching on §11 preamble. Written to `producer_dedup_pairs` with method_name='l1_haiku_batch'. Pilot accuracy 7/7 on anchors. Cost: approximately $75-90.
5. **L1 cache behavior confirmed:** Section 11 preamble cached at ephemeral TTL. After initial 8 concurrent writes, cache hits dominated (~90% discount on preamble tokens).

---

## Scope of B6.4 (in order)

### Part A: Anchor set — ~50 hand-labeled pairs

Per plan, anchor set has 4 tiers:

1. **Tier 1 — S4.1 re-verification (3 pairs):** Ridge, López de Heredia, CIRQ. These were merged in S4.1 — resurrect the pre-merge pair identities via `producer_aliases` alias_type='merged_from' records. Verify the MERGE was correct (don't assume).
2. **Tier 2 — Demo producers (~14 pairs):** Take the S4 demo producer set (Stag's Leap Wine Cellars, Fort Ross, Tempier, Guigal, Trimbach, Huet, DRC, Krug, Conterno, Château Margaux, Château Latour, plus S4.1 three). Pair each with a suspected dup from the L1 MERGE or PARENT-CHILD set.
3. **Tier 3 — Stress-test cases (~15 pairs):** abbreviation (DRC↔Romanée-Conti), translation, parent-child, accent, private-label, rename, importer-prefix, retailer-as-producer, commune overlap, region-collision, accent-preservation.
4. **Tier 4 — Stratified random (~20 pairs):** 5 per US/FR/IT/ES-other, pulled from L1 verdicts with confidence 0.80-0.90 (the uncertain-but-not-UNCERTAIN cluster).

User labels each pair MERGE / PARENT_CHILD / SKIP / UNCERTAIN (Claude does research first + proposes). Store in `data/sprints/dedup/anchor_set.json`.

### Part B: Threshold decision — user review of L1 distribution

The pilot in B6.3 suggested symmetric threshold around 0.92 (escalate everything with confidence < 0.92 to L2). User said "run L1, decide threshold at B6.4 with full distribution in hand."

Now that L1 is complete, produce the full-corpus distribution:
- Verdict counts (MERGE / PARENT_CHILD / SKIP / UNCERTAIN)
- Confidence histogram per verdict (buckets <0.70, 0.70-0.79, 0.80-0.84, 0.85-0.89, 0.90-0.94, ≥0.95)
- Cross-strategy agreement: how often does L1's verdict match the multi-strategy-blocking signal?
- Anchor-set recall: how many of the ~50 anchor pairs did L1 classify correctly?

User picks threshold for L2 escalation. Candidate thresholds (from B6.3 analysis):
- 0.85 symmetric: ~4% escalation (minimal)
- 0.90 symmetric: ~26% escalation
- 0.92 symmetric: ~35% escalation (fits $250 ceiling)
- 0.93 symmetric: ~50% escalation (slightly over $250)

### Part C: L2 — Haiku 4.5 with rich prompt + batching 5/call

Build `pipeline/identity/producer_dedup_l2.py`:

- **Input:** pairs from L1 matching the user-chosen threshold rule (`verdict IN ('UNCERTAIN')` OR `confidence < THRESHOLD`).
- **Model:** `claude-haiku-4-5` direct Anthropic.
- **Prompt (richer than L1):** IDENTITY_RULES §11 + full TTB fingerprint (all brand names, not just top 10) + full wine catalog (top 20 wines with appellations) + `producer_aliases` history + `parent_producer_id` chain + external IDs + producer metadata (year, website, address).
- **Batching: 5 pairs per call** (smaller because richer context per pair).
- **Output:** JSON array per pair, same schema as L1.
- **Write:** method_name='l2_haiku_rich' to producer_dedup_pairs.

### Part D: L3 — Sonnet 4.6 with Anthropic native web_search tool

Build `pipeline/identity/producer_dedup_l3.py`:

- **Input:** pairs from L2 where verdict IN ('MERGE', 'UNCERTAIN') — the rigor tier.
- **Model:** `claude-sonnet-4-6` direct Anthropic.
- **Web tool:** Anthropic native `web_search_20250305` (NOT OpenRouter :online). Fee: $10/1K searches.
- **Per-pair prompt:** same IDENTITY_RULES §11 preamble + L1 and L2 verdicts + reasoning + directive to verify via public web sources (Wine-Searcher, Wikipedia, producer website, LWIN, Liv-ex).
- **NOT batched** — each pair needs its own web searches.
- **Output:** `{verdict, confidence, reasoning, web_evidence: [{url, snippet, used_for}]}`.
- **Write:** method_name='l3_sonnet_web' to producer_dedup_pairs; web_evidence jsonb populated.

### Part E: Ablation tests (Gate 6 — critical, ~$2-3, 30 min)

Run BEFORE scaling L2/L3 to full volume:

1. **L2 with/without web search on 50 anchor pairs** — does Haiku + web catch cases Haiku alone misses? (We'd add web to L2 if +3 points on anchor set.)
2. **L3 with/without web search on 50 anchor pairs** — does web add value at the rigor tier?
3. **Cache hit rate check** — verify L2 preamble is cached (expect >70% cache_read on second+ call).

Decision rules:
- L2 web delta < 3 points → drop web from L2 (saves ~$20-30)
- L3 web delta < 2 points → drop web from L3 (saves ~$30-40)
- Both help → scale up

### Part F: Safety Net A — Unblocked spot-check (~$1, 30 min)

Select 200 random pairs that are NOT in the blocking candidate list. Run them through L1 prompt. If any come back MERGE with confidence > 0.85, add a blocking rule or loosen a threshold — we missed them.

### Part G: Cross-method agreement matrix

After L1+L2+L3 all run, produce the agreement matrix: for each pair, how do verdicts compare across methods? This drives B6.6 auto-apply rules:
- Auto-apply set: L1+L2+L3 all vote MERGE AND confidence > 0.85 on all three → B6.6 auto-merge
- Toughest pairs: methods disagree, confidence mid-range, policy edge case, or wine count > 20

Write `data/sprints/dedup/agreement_matrix.md` summarizing.

---

## Do NOT do in B6.4

- Merge execution (B6.6)
- Toughest-pairs review (B6.5 — this is the user-review session, separate from anchor labeling)
- L4 Opus audit (B6.5)
- Production merges

---

## Acceptance gate for B6.4

1. Anchor set (~50 pairs) written to `data/sprints/dedup/anchor_set.json` with user labels
2. Full L1 distribution produced; user has picked symmetric threshold for L2 escalation
3. `pipeline/identity/producer_dedup_l2.py` implemented; L2 run complete on escalated pairs
4. `pipeline/identity/producer_dedup_l3.py` implemented; L3 run complete on L2 escalations
5. Ablation tests run; web-grounding decision logged to DECISIONS.md
6. Safety Net A executed; any missed-pair patterns logged
7. Agreement matrix produced
8. Budget tracked; logged to `data/sprints/dedup/budget.json`

Budget for B6.4 (projected per B6.3 pilot extrapolation at 0.92 symmetric threshold):
- L2 (~50K pairs at 0.14¢/pair): ~$70-80
- L3 (~3-5K pairs at 1-2¢/pair): ~$40-60
- Ablation + safety net: ~$5
- Total B6.4: ~$115-145

Note: this is higher than plan's $40-95 B6.4 estimate — threshold choice has expanded L2 volume. Still inside the $250 sprint ceiling.

---

## Close-out

1. Update `data/sprints/dedup/journal.md` with B6.4 entry
2. Update `data/sprints/dedup/sessions.json` + `budget.json`
3. Update `data/dashboard.html`
4. Update `CLAUDE.md` Current Focus (B6.4 done, B6.5 next)
5. Update `data/sessions.md`
6. Write `data/session_prompts/b6_5_audit_review.md`
7. Commit + push: "B6.4: L2 rich-prompt + L3 web-grounded + anchor + ablation"

---

## Key context files

- `data/sprints/dedup/plan.md` — full sprint plan
- `data/sprints/dedup/journal.md` — B6.1 + B6.2 + B6.3 history
- `docs/IDENTITY_RULES.md` §11 — producer identity rules (L2/L3 prompts embed verbatim)
- `pipeline/identity/producer_blocking.py` — S2-S11 blocking (reference only; no new blocking in B6.4)
- `pipeline/identity/producer_dedup_l1.py` — L1 pipeline (reference for L2/L3 structure)
- `pipeline/lib/models.py` — HAIKU_MODEL, SONNET_MODEL constants
- `producer_dedup_pairs` table — 151K blocking rows + 151K l1_haiku_batch rows at B6.4 start
