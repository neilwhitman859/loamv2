# Sprint 6 — Metrics & Goals (locked 2026-04-18)

**Status:** locked after user sign-off on Step 7 planning session.
**Budget:** $450 ceiling (raised from $300). Spent through Step 6: $231.67. Projected Steps 7-11: ~$162. Projected final: ~$394.

---

## Terminology (adopted project-wide, all sprints going forward)

| New term | Old stats term | Meaning |
|---|---|---|
| **False Merges** | Precision problem | Merges applied that were actually two DIFFERENT producers. Permanent data corruption. Want 0. |
| **Missed Merges** | Recall problem | Real duplicates left as two rows. Annoying but reversible via Safety Net B / future dedup pass. Want few. |
| **False PC** | PC precision problem | PARENT_CHILD links created where no ownership exists, OR with wrong direction. Reversible metadata error. |
| **Missed PC** | PC recall problem | Real ownership relations not recorded. Invisible to users (rows work fine); fully recoverable later. |

## Core vs Tail split (adopted project-wide)

### Core definition (Sprint 6 version: "Expanded Core")

A producer is **Core** if ANY of:
- Country = US, OR
- Has ≥1 wine linked via US retailer/importer staging (Spec's, Wally's, Flatiron, Skurnik, Kermit Lynch, Empson, Winebow, European Cellars, Polaner, Domestique, Best Wine Store, Firstleaf, Last Bottle, WineDeals), OR
- Linked to any TTB COLA, OR
- Has ≥5 wines in canonical DB, OR
- Has any `wine_vintage_scores` row, OR
- Has any `wine_vintage_prices` row, OR
- Has ≥1 wine in international retailer staging (LCBO, Systembolaget, BC Liquor, PA PLCB), OR
- Has ≥1 wine in competition data (Berliner, TEXSOM, Enofile)

**Tail = everything else.**

Implementation: `sprint6_core_producers` table in DB, populated by `scripts/sprint6_lock_core.py`. Each row carries a `reasons[]` array tagging which criteria qualified it.

### Current split (as of 2026-04-18)

| | Producers | Wines | % of wines |
|---|---:|---:|---:|
| Core | 14,042 (42%) | 192,148 | **85%** |
| Tail | 19,239 (58%) | 33,133 | 15% |

Tail is dominated by low-wine-count producers: 15,070 have 1-2 wines (most of Tail).

## Quality targets

### Core (Sprint 6 targets)

| Metric | Target | Validated by |
|---|---|---|
| False Merges | **0** | Phase 3a + Step 7 web validation on all MERGE candidates |
| Missed Merges | **≤3%** (~97% catch) | Step 7 escalation coverage + Step 10 stratified audit |
| False PC | **0** | Step 7 web validation on all PC candidates + Step 10 direction audit |
| Missed PC | ≤5% | Web validation of PC-flagged pairs; accept some Missed PC as recoverable |
| Web-validation coverage | ~100% of escalations, PC, and uncertain; stratified audit of auto-SKIPs | Step 7 |

### Tail (Sprint 6 targets)

| Metric | Target | Validated by |
|---|---|---|
| False Merges | **≤1%** | Only auto-merge when high confidence; bias to SKIP |
| Missed Merges | ≤10% | Step 7 covers escalation pile; auto-SKIPs trusted on B6.4 calibration |
| PC correctness | Deferred | Default-SKIP Tail PC; handle in a later metadata sprint |
| Web-validation coverage | ≥20% (escalations only) | Step 7 |

## Defaults & conventions carried forward to Sprint 7+

1. **Terminology:** False Merges / Missed Merges / False PC / Missed PC. Never "precision/recall" in user-facing docs.
2. **Core/Tail split** applied at the start of every dedup sprint.
3. **Spending priority:** disproportionate Core investment; Tail gets post-execution safety nets.
4. **Human review cap:** ~50 pairs per sprint max. AI+web is the reliability floor (see `memory/feedback_user_review_scale.md`).
5. **Haiku+Serper** as default rigor tier (vs L3 Sonnet+Anthropic-web): ~25× cheaper, measured more accurate on merchant/shared-surname/collab patterns (see `memory/feedback_opus_inline_reasoning.md` and Sprint 6 Phase 3a findings).
6. **Quality measurement:** Opus inline audits a stratified random sample at sprint close (~400-600 pairs). Scorecard published with sprint close.

## Plan lock — Sprint 6 Steps 7-11

### Step 7 — Expand web-validation coverage ($142)
- **Core escalations + PC + missing** (15,428 pairs × $0.006 = $93)
- **Tail escalations** (6,190 pairs × $0.006 = $37)
- **Core auto-SKIP stratified audit** (2,000 pairs × $0.006 = $12)
- Parallel at 6 workers: ~2-3 hours runtime.

### Step 8 — Build routing_stage3 table ($0, SQL)
3-tier consensus logic, with web verdict authoritative at ≥0.90 confidence. Auto-apply MERGE only when web says MERGE at conf ≥0.90. Auto-apply SKIP when web says SKIP at conf ≥0.90. PC at any conf → flag for review if inconsistent; else auto-apply PC link with direction.

### Step 9 — Opus pattern audit + user sanity check ($0-2)
Opus inline reviews routing_stage3 + web verdicts. Produces 10 pattern clusters × 5 example pairs = ~50 pairs for user thumbs-up. Pattern clusters inform §11.4.m-q amendments.

### Step 10 — Execute merges + measure ($12-18)
Apply auto-apply decisions; log to `producer_merge_history`. Run Safety Net B (post-execution rescan for missed duplicates). Opus inline stratified audit on 200 Core-merge + 200 Core-skip + 100 Tail-merge + 100 Tail-skip = 600 pairs to score False/Missed Merges.

### Step 11 — Sprint close ($0)
Dashboard update, CLAUDE.md, DECISIONS.md, sprint archive, hand to Sprint 7.

## Budget ledger

| Item | Cost |
|---|---:|
| B6.1 through B6.5a-partial (Steps 1-6) | $231.67 |
| Step 7 web validation | $142 |
| Step 8 routing table | $0 |
| Step 9 pattern audit | $0-2 |
| Step 10 execute + measure | $12-18 |
| Step 11 close | $0 |
| **Projected Sprint 6 total** | **$386-394** |
| Cushion of $450 ceiling | $56-64 |
