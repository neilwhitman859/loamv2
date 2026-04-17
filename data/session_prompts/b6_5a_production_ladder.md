# B6.5a — Production ladder run on full 151K corpus

You are opening B6.5a of Sprint 6 (Producer Dedup). B6.4 committed thresholds based on 600-pair calibration + 367 gold labels. B6.5a applies those thresholds at production scale. All tier scripts exist in `pipeline/identity/` from B6.4.

**Full context:** [`data/sprints/dedup/b6_4_analysis.md`](data/sprints/dedup/b6_4_analysis.md). **Committed thresholds:** [`data/sprints/dedup/final_thresholds.json`](data/sprints/dedup/final_thresholds.json).

---

## Pre-B6.5a state (from B6.4)

- `producer_dedup_pairs` has **151,150 `blocking` rows + 151,120 `l1_haiku_batch` rows**.
- 600 pairs in `data/sprints/dedup/calibration_set.json` have verdicts across L1 / L1.5 Gemini basic / L2 Haiku rich / L2.5 Gemini rich, plus 167 L3 Sonnet+web oracle gold labels.
- Scripts:
  - `pipeline/identity/producer_dedup_l1.py` (done)
  - `pipeline/identity/producer_dedup_gemini.py --mode basic|rich` (production-ready)
  - `pipeline/identity/producer_dedup_l2.py` (production-ready)
  - `pipeline/identity/producer_dedup_l3.py` (production-ready, `--no-web` and default-web)
  - `pipeline/identity/sync_calibration.py` (used only for calibration subset)
  - `pipeline/identity/agreement_matrix.py` (can adapt for production bucket sort)

- **Committed thresholds (symmetric, cross-family):**
  - Stage 1: L1 MERGE >=0.88 AND L1.5 MERGE >=0.88 → auto-MERGE; L1 SKIP >=0.97 AND L1.5 SKIP >=0.97 → auto-SKIP.
  - Stage 2: L2 MERGE >=0.90 AND L2.5 MERGE >=0.90; L2 SKIP >=0.95 AND L2.5 SKIP >=0.95.
  - Stage 3 (L3 Sonnet): MERGE >=0.92 → auto; SKIP >=0.90 → auto; else user review.
  - PARENT_CHILD routing: route to user review iff (a) 2+ tiers emit PC at any conf, OR (b) L2 Haiku rich or L2.5 Gemini rich PC at conf ≥0.90, OR (c) L3 Sonnet PC at any conf. Otherwise treat single-tier basic-prompt PC as noise and follow cross-tier non-PC consensus.

---

## Scope of B6.5a (in order)

### Step 1 — L1.5 Gemini basic on all 151K pairs (~$3-4, ~30 min)

```
python -m pipeline.identity.producer_dedup_gemini --mode basic --execute \
    --budget 5 --workers 4 --method-name l1_gemini_basic
```

Writes to `producer_dedup_pairs` with `method_name='l1_gemini_basic'`. Verify:
- ~150K rows written
- Verdict distribution matches calibration shape (~25-30% MERGE, ~20% PC, ~50% SKIP)
- No parse failures above 0.1%

### Step 2 — Build Stage 1 routing buckets (SQL only, $0)

Materialize a view or working table that labels each pair with its Stage-1 action. Use the committed thresholds.

```sql
CREATE TABLE producer_dedup_routing_stage1 AS
SELECT
  l1.producer_id_a, l1.producer_id_b,
  l1.verdict AS l1_verdict, l1.confidence AS l1_conf,
  g.verdict AS l1_5_verdict, g.confidence AS l1_5_conf,
  CASE
    WHEN l1.verdict='MERGE' AND l1.confidence>=0.88
         AND g.verdict='MERGE' AND g.confidence>=0.88
      THEN 'stage1_auto_merge'
    WHEN l1.verdict='SKIP' AND l1.confidence>=0.97
         AND g.verdict='SKIP' AND g.confidence>=0.97
      THEN 'stage1_auto_skip'
    -- NOTE: PC routing is now applied AFTER Stage 2, not at Stage 1.
    -- Stage 1 treats PC like any other verdict; if L1 + L1.5 agree PC at any conf,
    -- the pair still goes through L2+L2.5 to see if rich tiers confirm PC.
    WHEN l1.verdict='PARENT_CHILD' AND g.verdict='PARENT_CHILD'
      THEN 'escalate_pc_consensus'  -- 2 tiers agree PC, likely user review
    WHEN l1.verdict='PARENT_CHILD' OR g.verdict='PARENT_CHILD'
      THEN 'escalate_pc_single'     -- single-tier PC, may be noise; let L2+L2.5 decide
    WHEN l1.verdict='UNCERTAIN' OR g.verdict='UNCERTAIN'
      THEN 'escalate_uncertain'
    WHEN l1.verdict <> g.verdict
      THEN 'escalate_disagreement'
    ELSE 'escalate_lowconf'
  END AS stage1_action
FROM producer_dedup_pairs l1
JOIN producer_dedup_pairs g
  ON g.producer_id_a=l1.producer_id_a
 AND g.producer_id_b=l1.producer_id_b
 AND g.method_name='l1_gemini_basic'
WHERE l1.method_name='l1_haiku_batch';
```

Report Stage 1 bucket distribution. Projections:
- Auto-MERGE: 1,500-2,200
- Auto-SKIP: 100,000-125,000
- Escalate: 24,000-49,000

If auto-SKIP <80,000 or escalate >60,000 → pause and re-examine; something is off.

### Step 3 — SKIP audit spot-check (~$5)

Sample 200 random auto-SKIP pairs from the Stage 1 bucket. Run them through L2 Haiku rich + L3 no-web.

```
python -m pipeline.identity.producer_dedup_l2 --execute --budget 0.5 \
    --l1-verdicts SKIP --limit 200 --method-name l2_skip_audit

python -m pipeline.identity.producer_dedup_l3 --no-web --execute --budget 3 \
    --limit 200 --method-name l3_skip_audit
```
(Adapt args — these scripts currently filter on L1 verdict; may need a small arg addition to pin to specific pair IDs, OR sample them beforehand into a temp table.)

Measure: of 200 auto-SKIPs, how many did L2 or L3 flag as MERGE? Compare to calibration's 3.2% FN rate.

| Measured FN | Action |
|---|---|
| < 2% | Thresholds OK, continue |
| 2-5% | Tighten SKIP to >=0.98 or require both tiers >=0.99; re-run Step 2 |
| > 5% | Re-run ladder with stricter SKIP threshold |

Log the FN rate + any flagged pairs in `data/stats/b6_5a_skip_audit.md`.

### Step 4 — L2 Haiku rich on Stage 1 escalations (~$25-40)

```
python -m pipeline.identity.producer_dedup_l2 --execute --budget 50 --workers 6 \
    --l1-verdicts MERGE,PARENT_CHILD,SKIP,UNCERTAIN \
    --l1-conf-max 0.97  # everything below L1 auto-skip threshold
```

(Refine filter: pick up any pair with `stage1_action` IN ('escalate_*') from the routing table.)

Writes `method_name='l2_haiku_rich'` in production. Target ~30-40K rows.

Budget guard at $50 — if it blows through, something is wrong with volume.

### Step 5 — L2.5 Gemini rich on same escalation set (~$3-5)

```
python -m pipeline.identity.producer_dedup_gemini --mode rich --execute \
    --budget 8 --workers 6 --method-name l2_gemini_rich
```

Same source pairs as L2 Haiku rich (the Stage 1 escalations).

### Step 6 — Build Stage 2 routing buckets (SQL)

Same pattern as Step 2 but looking at L2 + L2.5:

```sql
-- producer_dedup_routing_stage2: stage1_action='escalate_*' rows get re-bucketed
-- auto_merge / auto_skip / escalate_to_l3
```

Thresholds: L2+L2.5 MERGE >=0.90 → auto, SKIP >=0.95 → auto.

**PC routing at Stage 2 (refined):** route to user review iff:
- 2+ tiers (of L1, L1.5, L2, L2.5) emit PC at any confidence (cross-tier agreement), OR
- L2 Haiku rich OR L2.5 Gemini rich emits PC at conf ≥0.90 (rich-prompt high-conf PC)

Otherwise (single-tier basic-prompt PC, both rich tiers non-PC below 0.90): treat PC as noise, follow cross-tier non-PC consensus:
- If non-PC verdicts from L1.5/L2/L2.5 reach cross-family MERGE agreement → auto-MERGE
- If non-PC verdicts reach cross-family SKIP agreement → auto-SKIP
- Else → escalate to L3

Any disagreement (beyond PC noise cases) → L3.

Report Stage 2 bucket distribution. Projection: ~3,000-10,000 go to Stage 3.

### Step 7 — L3 web-vs-no-web decision point (mid-run)

**Based on Stage 2 residual count:**

| L3 input size | Decision |
|---|---|
| < 1,000 pairs | L3 with web, full run (~$15-30) |
| 1,000-3,000 | L3 with web, full run (~$30-90) |
| 3,000-10,000 | L3 no-web on bulk + L3 web on ~500 sub-sample of disagreements (~$30-80) |
| > 10,000 | STOP — Stage 2 thresholds are wrong. Tighten and re-run Step 4-6. |

Document decision + rationale in `data/sprints/dedup/b6_5a_journal.md`.

### Step 8 — L3 Sonnet run on Stage 2 residual

```
# Web mode (default):
python -m pipeline.identity.producer_dedup_l3 --execute --workers 3 \
    --budget <decided> --l2-verdicts MERGE,PARENT_CHILD,UNCERTAIN \
    --l2-conf-max 0.90 --method-name l3_sonnet_web

# No-web mode (if large residual):
python -m pipeline.identity.producer_dedup_l3 --no-web --execute --workers 4 \
    --budget <decided> --method-name l3_sonnet_noweb
```

### Step 9 — Build Stage 3 routing: final bucket assignment

```sql
-- producer_dedup_routing_final:
--   final_action IN ('auto_merge', 'auto_skip', 'user_review')
-- auto_merge: L3 MERGE >=0.92 OR (already in Stage 1/2 auto_merge)
-- auto_skip: L3 SKIP >=0.90 OR (Stage 1/2 auto_skip)
-- user_review: L3 PC, L3 UNCERTAIN, L3 MERGE 0.70-0.92, plus inherited PC flags
```

Report final distribution. Target: 1,500-5,000 user review.

### Step 10 — L4 Opus-inline audit (~$0)

In this session, pull the full routing table + per-pair reasoning + the flagged pairs, and do a cross-pair pattern audit. Flag:

1. MERGE pairs that look suspicious in aggregate (e.g., 10 MERGEs share a shape §11 doesn't cover)
2. Inconsistent handling (3 same-shape pairs, 2 MERGE + 1 SKIP → flag all 3 for review)
3. PC patterns suggesting §11 amendments
4. Suspicious unanimous-auto-SKIPs that should have been MERGEs

L4 flags add pairs to the user review pile. Write findings to `data/sprints/dedup/b6_5a_l4_audit.md`.

### Step 11 — Prepare review pile for B6.5b

Output `data/sprints/dedup/review_queue.json` with one entry per pair:
- Pair identity + both producer details
- Full tier verdicts + reasoning
- Claude's recommendation + rationale
- Context pack URLs (website, Wikipedia, LWIN, TTB, sample wines)
- Pattern category (parent-child candidate / rename / private-label / accent / importer-prefix / cross-country / etc.)

---

## Do NOT do in B6.5a

- Merge execution (that's B6.6)
- User review (that's B6.5b)
- Apply changes to `producers` or `wines` tables

---

## Budget for B6.5a

| Step | Cost |
|---|---|
| L1.5 Gemini basic on 151K | $3-4 |
| SKIP audit | $5 |
| L2 Haiku rich on escalations | $25-40 |
| L2.5 Gemini rich on escalations | $3-5 |
| L3 Sonnet (web or no-web) | $20-40 |
| L4 Opus audit | $0 |
| **B6.5a total** | **$56-94** |

Projected sprint total through B6.5a: $78.44 (B6.3) + $24.51 (B6.4) + $56-94 (B6.5a) = **$159-197 of $250 ceiling**.

---

## Close-out

1. Update `data/sprints/dedup/journal.md` with B6.5a entry (volumes, residuals, L3 web decision, L4 findings)
2. Update `data/sprints/dedup/sessions.json` + `budget.json`
3. Update `data/dashboard.html`
4. Write `data/session_prompts/b6_5b_interactive_review.md` (already stubbed)
5. Commit: "B6.5a: production ladder on 151K + L4 Opus audit, review pile prepared"

---

## Key context files

- `data/sprints/dedup/plan.md` — sprint plan
- `data/sprints/dedup/final_thresholds.json` — committed thresholds
- `data/sprints/dedup/b6_4_analysis.md` — calibration rationale
- `docs/IDENTITY_RULES.md` §11 — producer identity rules
- `pipeline/identity/` — all tier scripts (L1, L1.5 Gemini, L2, L2.5 Gemini, L3)
