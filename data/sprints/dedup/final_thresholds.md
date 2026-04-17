# B6.4 Committed thresholds (final)

**Committed:** 2026-04-17, user-approved after calibration review.
**Calibration source:** `data/sprints/dedup/calibration_set.json` — 600 pairs, 367 gold-labeled (200 proxy + 167 Sonnet+web oracle).
**Symmetric thresholds per user preference** ("do it right the first time").

---

## Stage 1 — L1 + L1.5 cross-check (runs on ALL 151K)

| Rule | Threshold | Calibration precision |
|---|---|---|
| **Auto-MERGE** | L1 Haiku MERGE >= **0.88** AND L1.5 Gemini basic MERGE >= **0.88** | 100% at >=0.85 (70/70 gold); 0.88 = user safety margin |
| **Auto-SKIP** | L1 Haiku SKIP >= 0.97 AND L1.5 Gemini basic SKIP >= 0.97 | ~99% (L1 alone 100% @0.97, L1.5 alone 96.2% @0.97) |

**Escalate to Stage 2:**
- All L1 PARENT_CHILD at any confidence (PC precision 6.7% — too low for auto)
- All L1 UNCERTAIN
- L1 MERGE < 0.88 OR L1.5 MERGE < 0.88
- L1 SKIP < 0.97 OR L1.5 SKIP < 0.97
- L1+L1.5 disagreement

---

## Stage 2 — L2 + L2.5 cross-check (on Stage 1 escalations)

| Rule | Threshold | Calibration precision |
|---|---|---|
| Auto-MERGE | L2 Haiku rich MERGE >= 0.90 AND L2.5 Gemini rich MERGE >= 0.90 | 100% (78/78 gold) |
| Auto-SKIP | L2 Haiku rich SKIP >= 0.95 AND L2.5 Gemini rich SKIP >= 0.95 | ~93% |

**Escalate to Stage 3:**
- L2 PARENT_CHILD at any confidence (PC precision 9.9%)
- L2 UNCERTAIN
- L2+L2.5 disagreement
- L2 MERGE or SKIP below auto-thresholds

---

## Stage 3 — L3 Sonnet 4.6 rigor tier (on Stage 2 escalations)

### Web-search decision: **deferred, made mid-run based on residual count**

| L3 input size | Decision |
|---|---|
| < 1,000 pairs | Use web ($15-30) |
| 1,000-3,000 pairs | Use web ($30-90, may need budget headroom) |
| 3,000-10,000 pairs | L3 no-web bulk + web on sub-sample of disagreements |
| > 10,000 pairs | Stop — revisit Stage 2 thresholds before running L3 |

### L3 verdict handling

| Verdict | Action |
|---|---|
| MERGE at >=0.92 | Auto-MERGE |
| SKIP at >=0.90 | Auto-SKIP |
| PARENT_CHILD (any conf) | User review |
| UNCERTAIN | User review |
| MERGE at 0.70-0.92 | User review |

---

## L4 Opus-inline audit (after Stage 3)

Runs as part of the current conversation at **$0 marginal cost**. Context: full L1+L1.5+L2+L2.5+L3 output across all ~151K pairs. Looks for:

- MERGE pairs that look suspicious in aggregate (5 MERGEs in a row sharing a pattern §11 doesn't cover)
- Inconsistent handling of structurally-similar pairs (3 same-shape pairs, 2 MERGE + 1 SKIP)
- PARENT_CHILD patterns suggesting an IDENTITY_RULES §11 gap
- Suspicious auto-SKIPs that should have been MERGEs (known 3.2% FN tail)

L4 flags add to the user review pile.

---

## SKIP audit spot-check (B6.5a, ~$5)

**Purpose:** verify 4-way SKIP FN rate at production scale (calibration showed 3.2% FN on 156 pairs — may be lower at scale).

**Method:** after Stage 1, sample 200 random auto-SKIP pairs, run through L2 + L3 no-web.

| Measured FN | Action |
|---|---|
| < 2% | Thresholds OK |
| 2-5% | Tighten SKIP to >=0.98 or require both tiers >=0.99 |
| > 5% | Re-run ladder with stricter thresholds |

---

## PARENT_CHILD routing (refined)

**Principle:** basic-prompt PC verdicts (L1 Haiku, L1.5 Gemini basic) are noise across all confidence bands — 6-9% precision regardless of confidence. Higher confidence doesn't reliably improve PC precision. What does help: (a) cross-tier agreement, (b) rich-prompt source.

### Calibration PC precision by tier × band

| Tier | Conf ≥0.97 | 0.92-0.97 | 0.85-0.92 |
|---|---|---|---|
| L1 Haiku PC | — | 1/2 = 50% (n=2) | 4/69 = 5.8% |
| L1.5 Gemini basic PC | 0/1 = 0% | 4/27 = 14.8% | 2/37 = 5.4% |
| L2 Haiku rich PC | — | 7/69 = 10.1% | — |
| L2.5 Gemini rich PC | 2/2 = 100% (n=2) | 7/69 = 10.1% | 0/21 = 0% |

### Refined PC rule — route to user review IFF any of:

1. **2+ tiers emit PC at any confidence** — cross-tier PC agreement is the real signal
2. **L2 Haiku rich OR L2.5 Gemini rich emits PC at conf ≥0.90** — rich prompts have the full TTB + wine catalog + metadata context that legitimately distinguishes PC from MERGE/SKIP
3. **L3 Sonnet emits PC at any confidence** — rigor tier always routes PC to user

### Otherwise

Single-tier basic-prompt PC (L1 or L1.5 alone below rich-tier agreement) is treated as noise. The pair follows the non-PC tier consensus:
- If L1.5 + L2 + L2.5 all say SKIP at cross-family thresholds → auto-SKIP
- If L1.5 + L2 + L2.5 all say MERGE at cross-family thresholds → auto-MERGE
- If disagreement among non-PC verdicts → falls through to L3

### Edge case

L1 PC + L1.5 MERGE + L2 SKIP + L2.5 SKIP → cross-tier majority says SKIP → auto-SKIP. If L1's PC was right, we miss a real parent-child link. Calibration suggests this is <1% of L1 PC pairs and acceptable.

---

## User review rules

Any pair meets any of:
- PC routing (above)
- L3 UNCERTAIN
- L3 MERGE at conf 0.70-0.92
- Unresolved disagreements after L3
- L4 Opus audit flagged the pair

Target review pile: **1,500-3,000 pairs** (refined from 3,000-5,000 with new PC rule). We'll design a batched review flow when we see the actual count.

---

## B6.5 split

- **B6.5a (automated, $35-80, ~3h):** L1.5 full run → Stage 1 sort → SKIP audit → L2+L2.5 → Stage 2 sort → L3 web decision → L3 run → L4 Opus audit → review pile prepared
- **B6.5b (interactive, $0-5, 1-3h):** batched user review with Claude context packs → merge queue for B6.6

---

## Caveats

1. Oracle hit budget cap at 167/400 hard-case pairs. T4 and T5 are partial.
2. L3 ablation thin (4 overlaps, 100% web vs no-web). Defer until Stage 2 residual known.
3. Same-family MERGE had 1 FP each (L1 Haiku × L2 Haiku rich; L2 × L2.5 same-family blip). Cross-family is the anchor.
4. 4-way SKIP FN rate ~3.2% in calibration. SKIP audit validates at scale.
5. Review pile estimate is wide (1,500-5,000). Claude curates batched presentation in B6.5b.
