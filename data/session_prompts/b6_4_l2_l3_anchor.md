# B6.4 — Calibration + L2 Haiku rich + L2.5 Gemini + L3 Sonnet web-grounded + Ablation

You are opening B6.4 of Sprint 6 (Producer Dedup). B6.3 completed: schema + IDENTITY_RULES §11 + blocking (151,150 pairs, 10 strategies) + full L1 Haiku run. L1 produced verdict + confidence per pair written to `producer_dedup_pairs` with `method_name='l1_haiku_batch'`.

Full plan: [`data/sprints/dedup/plan.md`](data/sprints/dedup/plan.md). Journal: [`data/sprints/dedup/journal.md`](data/sprints/dedup/journal.md). B6.3 log: [`data/stats/b6_3_l1_full.log`](data/stats/b6_3_l1_full.log).

---

## Pre-B6.4 state (from B6.3)

1. **Schema ready:** `producer_dedup_pairs` extended + `producer_merge_history` created.
2. **IDENTITY_RULES §11 live** at [`docs/IDENTITY_RULES.md:767`](docs/IDENTITY_RULES.md) with user-reviewed 11.4.g holdco carve-out. Embedded verbatim in every LLM prompt.
3. **Blocking done:** 151,150 pairs with `method_name='blocking'`. Eight active strategies (S1, S2, S5, S6, S7, S8, S9, S10, S11).
4. **L1 done:** ~151K pairs processed by claude-haiku-4-5. 7/7 anchor accuracy in pilot. Verdict distribution heavily SKIP-dominated.

---

## User-decided ladder architecture (from B6.3 threshold discussion)

Verification grid with cross-model agreement at each Haiku-based tier:

```
L1     Haiku basic          151K pairs
         ≥0.97 auto-accept
         0.92-0.97 → L1.5
         <0.92 or UNCERTAIN → L2

L1.5   Gemini basic cross-check on 0.92-0.97 L1 band
         agree → auto-accept
         disagree → L3 directly (Gemini-aggressive + Haiku-cautious = likely FNR)

L2     Haiku rich prompt on L1<0.92 + L1.5 disagreements
         ≥0.96 auto-accept
         0.90-0.96 → L2.5
         <0.90 or UNCERTAIN → L3

L2.5   Gemini rich prompt cross-check on 0.90-0.96 L2 band
         agree → auto-accept
         disagree → L3

L3     Sonnet 4.6 + Anthropic native web_search_20250305
         Rigor tier for: L2<0.90, L2.5 disagreements, L1.5 disagreements

L4     Opus inline-1M audit across all verdicts
         Systematic pattern check

User review: 50-150 toughest pairs

Safety Net A: unblocked spot-check (200 random pairs not in blocking)
Safety Net B: post-execution leftover scan
```

**Threshold caveat:** Those numbers (0.97/0.92/0.96/0.90) are **defaults**, not commitments. B6.4 calibration phase will adjust based on measured accuracy-per-confidence curves. Thresholds are post-processing filters — no need to re-run models to change them.

---

## Scope of B6.4 (in order)

### Phase A: Build synthetic ground truth (~500-700 pairs, $4-10)

**No user hand-labeling.** Use L3 Sonnet + web search as the "oracle" on a curated sample, plus multi-strategy agreement as proxy ground truth for the easy ends.

Test set composition:
1. **100 near-certain MERGE pairs:** multi-strategy blocking agreement (≥3 signals), OR previously merged in S4.1 (reverse-engineer via producer_aliases), OR shared ≥5 wine-LWIN_7s
2. **100 near-certain SKIP pairs:** different countries, zero external_id overlap, trigram <0.3, no shared wines, different communes/regions
3. **100 borderline MERGE/PC pairs:** S6 BW-permit sharing with different brand names, L1 confidence 0.87-0.93
4. **100 borderline SKIP pairs:** L1 high trigram but different regions, L1 confidence 0.88-0.95
5. **200 L3-oracle random sample:** pull random pairs from each L1 verdict band, gold-label via Sonnet + web search

Build script: `pipeline/identity/build_calibration_set.py` — queries producer_dedup_pairs, applies stratification rules, writes to `data/sprints/dedup/calibration_set.json`.

Gold labeler: run L3 Sonnet + `web_search_20250305` on the 200 random-sample pairs. Store in calibration_set.json as `gold_verdict` field. Cost: ~$4.

**Output:** 500-700 pair calibration set with verdict + confidence fields for each (gold) + empty slots for L1/L1.5/L2/L2.5 results to be filled.

### Phase B: L1 calibration analysis (post-processing on existing data, $0)

L1 has already run on the full corpus. For the calibration set, L1 verdicts are already in `producer_dedup_pairs`. Just analyze:

1. **L1 accuracy by confidence bucket:** bucket L1 output by confidence (0.85-0.89, 0.90-0.94, 0.95+). Compute agreement with gold.
2. **L1 MERGE precision at each threshold:** of L1 pairs auto-accepted at ≥X confidence, what % are actually MERGE per gold? Sweep X from 0.90 to 0.99.
3. **L1 SKIP recall at each threshold:** of true-MERGE pairs per gold, what % did L1 correctly NOT call SKIP at confidence ≥X?
4. **Precision-recall curve.** Plot and write to `data/sprints/dedup/l1_calibration.md`.

### Phase C: L1.5 Gemini basic + cross-model agreement analysis (~$1)

1. **Run Gemini 3 Flash Preview basic prompt on calibration set** (identical prompt structure to L1). Write to `producer_dedup_pairs` with `method_name='l1_gemini_basic_calibration'`.
2. **Cross-model agreement analysis:**
   - When Haiku and Gemini both say MERGE at conf ≥X, what's actual accuracy per gold?
   - When they disagree, who's right per gold?
   - Does Gemini-aggressive + Haiku-cautious disagreement correlate with missed merges?
3. **L1.5 threshold optimization:** grid-search L1.5 band boundaries (0.85-0.97 at the low end, 0.92-0.97 at the high end). Pick thresholds that maximize accuracy while keeping costs reasonable.

### Phase D: L2 Haiku rich prompt (on real L1 output, not just calibration set)

Build `pipeline/identity/producer_dedup_l2.py`:

- **Input:** pairs from L1 matching the calibrated L1 routing rule.
  - After Phase B/C, this might be `verdict='UNCERTAIN' OR confidence < 0.92`, or calibration might shift to 0.90 or 0.94. Read from `data/sprints/dedup/l1_thresholds.json`.
- **Model:** `claude-haiku-4-5` direct Anthropic.
- **Prompt (richer than L1):**
  - IDENTITY_RULES §11 verbatim
  - Full TTB fingerprint (all brand names, all addresses, all permittees, not capped)
  - Full wine catalog (up to top 20 wines with appellations and LWIN)
  - `producer_aliases` history for both producers
  - `parent_producer_id` chain
  - Full external_ids
  - Producer metadata (year_established, website, philosophy, hectares, total_production_cases)
  - Geographic proximity (if latitude/longitude available)
- **Batching:** 5 pairs per call (smaller because prompt is richer, ~8-12K tokens/call)
- **Output:** JSON array per pair with verdict + confidence + reasoning
- **Write:** `method_name='l2_haiku_rich'`

**First run on calibration set (~200 pairs, ~$0.50)**, measure L2 calibration, THEN run on full escalated set.

### Phase E: L2 calibration analysis (post-L2 calibration run, $0)

Same analysis as Phase B but for L2 output:
- L2 accuracy by confidence bucket against gold labels
- Does L2 at 0.92 equal L1 at 0.95 in reliability (validates "richer context" hypothesis)?
- Precision-recall curves per confidence
- Grid-search L2 auto-accept ceiling (0.93, 0.94, 0.95, 0.96, 0.97) and L2.5 floor (0.85, 0.87, 0.90)

### Phase F: L2.5 Gemini rich prompt cross-check (~$2)

Build `pipeline/identity/producer_dedup_l2_5.py`:

- Same prompt as L2 but Gemini 3 Flash Preview
- Input: L2 output in the calibrated 0.90-0.96 band (or wherever Phase E calibration lands)
- Batched 5/call
- Cross-model agreement analysis against gold labels

### Phase G: L3 Sonnet + web search + ablation (~$30-45)

Build `pipeline/identity/producer_dedup_l3.py`:

- **Input:** pairs meeting any of:
  - L1 UNCERTAIN
  - L2 UNCERTAIN or confidence < 0.90
  - L2.5 disagrees with L2
  - L1.5 Gemini said MERGE but L1 Haiku said SKIP at high confidence (FNR candidate)
- **Model:** `claude-sonnet-4-6` direct Anthropic
- **Tool:** Anthropic native `web_search_20250305`
- **Not batched** — each pair gets its own web searches
- **Write:** `method_name='l3_sonnet_web'`, populate `web_evidence` jsonb

**Gate 6 ablation BEFORE full L3 run:**
1. **L2 with/without web search on 50 calibration pairs.** Does Haiku rich + web catch cases it misses without web? (If +3 points, consider adding web to L2.)
2. **L3 with/without web search on 50 calibration pairs.** Does web add value at rigor tier?
3. **Cache hit rate check** — verify prompt preamble caching works at L2.

**Decisions logged to DECISIONS.md:**
- If L2 web delta <3 pts → L2 stays web-less (save ~$20-30)
- If L3 web delta <2 pts → L3 drops web (save ~$20-30)
- If both help → scale up as planned

### Phase H: Cross-method agreement matrix

After all tiers (L1 + L1.5 + L2 + L2.5 + L3) run, produce the agreement matrix on the calibration set:

| Scenario | Count | Accuracy vs gold | Action |
|---|---|---|---|
| L1+L2+L3 all MERGE at conf ≥0.85 | ? | ? | **auto-apply** (B6.6) |
| L1+L2+L3 all SKIP at conf ≥0.85 | ? | ? | **auto-skip** |
| Models disagree | ? | ? | **user review** (B6.5) |
| UNCERTAIN propagated through all tiers | ? | ? | **user review** |

Write `data/sprints/dedup/agreement_matrix.md`.

### Phase I: Safety Net A — Unblocked spot-check (~$1, 30 min)

Pull 200 random pairs NOT in the candidate list (use random producer combinations not captured by blocking). Run through L1 + L1.5. If any come back MERGE with conf >0.85, we missed them — add a blocking rule or loosen a threshold.

### Phase J: Final threshold commitment + held-out validation

1. Write final thresholds to `data/sprints/dedup/final_thresholds.json` with justification from calibration data.
2. Sample 100 pairs NOT used in calibration set (held-out). Apply full ladder with final thresholds. Measure final error rate.
3. Report to user before B6.5 user review begins.

---

## Do NOT do in B6.4

- Merge execution (B6.6)
- Toughest-pairs user review (B6.5)
- L4 Opus audit (B6.5)
- Production merges
- Hand-label pairs (use L3 oracle instead)

---

## Acceptance gate for B6.4

1. Calibration set built, gold-labeled via L3 oracle — written to `data/sprints/dedup/calibration_set.json`
2. L1 calibration analysis done — `l1_calibration.md`
3. L1.5 Gemini run + cross-model agreement analysis done
4. L2 Haiku rich-prompt run on full escalated set
5. L2 calibration analysis + threshold optimization done — `l2_calibration.md`
6. L2.5 Gemini rich-prompt run on 0.90-0.96 L2 band (or calibrated band)
7. L3 Sonnet + web run on escalated set
8. Ablation tests done + web-grounding decisions logged
9. Safety Net A run + any missed-pair patterns logged
10. Cross-method agreement matrix produced — `agreement_matrix.md`
11. Final thresholds committed + held-out validation complete — `final_thresholds.json`
12. Budget tracked + logged

**Budget for B6.4 (based on Scenario E + user's tightened thresholds + calibration work):**

| Phase | Projected cost |
|---|---:|
| A: calibration set + L3 oracle on 200 pairs | ~$4 |
| C: L1.5 Gemini on calibration set | ~$0.50 |
| D: L2 Haiku rich on ~31K pairs | ~$44 |
| F: L2.5 Gemini rich on ~14K pairs | ~$2.50 |
| G: L3 Sonnet + web on ~8K pairs | ~$30-45 |
| H: agreement matrix | $0 |
| I: Safety Net A (200 pairs L1) | ~$0.10 |
| J: held-out validation | ~$0.50 |
| **B6.4 total** | **~$80-95** |

**Sprint total (L1 + B6.4):**
- L1: ~$77
- B6.4: ~$80-95
- **Sprint total: ~$157-172**
- Reserve for B6.5 + B6.7: **~$78-93** under $250 ceiling

---

## Close-out

1. Update `data/sprints/dedup/journal.md` with B6.4 entry
2. Update `data/sprints/dedup/sessions.json` + `budget.json`
3. Update `data/dashboard.html`
4. Update `CLAUDE.md` Current Focus (B6.4 done, B6.5 next)
5. Update `data/sessions.md`
6. Write `data/session_prompts/b6_5_audit_review.md`
7. Commit + push: "B6.4: calibration + L2/L2.5/L3 + agreement matrix"

---

## Key context files

- `data/sprints/dedup/plan.md` — full sprint plan
- `data/sprints/dedup/journal.md` — B6.1 + B6.2 + B6.3 history
- `docs/IDENTITY_RULES.md` §11 — producer identity rules (LLM prompts embed verbatim)
- `pipeline/identity/producer_blocking.py` — S2-S11 blocking (reference only)
- `pipeline/identity/producer_dedup_l1.py` — L1 Haiku classifier (structure for L2/L2.5/L3)
- `pipeline/lib/models.py` — HAIKU_MODEL, SONNET_MODEL constants
- `producer_dedup_pairs` table — 151K blocking + 151K l1_haiku_batch rows at B6.4 start

## Notable context from B6.3

- User rejected asymmetric thresholds ("do it right the first time")
- User emphasized calibration-first approach over hand-labeling
- Cross-model verification (L1.5, L2.5) earned endorsement via:
  - Haiku has Task 1 FPR 0.7% / FNR 21% (cautious)
  - Gemini 3 Flash has Task 1 FPR 9% / FNR 3.5% (aggressive)
  - Complementary error profiles → joint agreement ≈ 0.06% FPR / 0.7% FNR
- L3 gets extra routing: L1.5 disagreements where Gemini=MERGE but Haiku=SKIP at high conf (FNR candidates needing web grounding)
- L4 Opus audit still scheduled for B6.5 per original plan
- User is not a wine expert for obscure producers — calibration uses L3+web oracle, not hand-labels
