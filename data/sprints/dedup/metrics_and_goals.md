# Sprint 6 — Metrics & Goals (locked 2026-04-18)

**Status:** historical benchmark document. The 2026-04-18 Step 7-11 plan is preserved for audit comparison, but as of 2026-04-20 the working path is a merge-only Codex rebuild.
**Budget:** $450 ceiling (raised from $300). Spent through Step 6: $231.67. Projected Steps 7-11: ~$162. Projected final: ~$394.

---

## Terminology (adopted project-wide, all sprints going forward)

| New term | Old stats term | Meaning |
|---|---|---|
| **False Merges** | Precision problem | Merges applied that were actually two DIFFERENT producers. Permanent data corruption. Want 0. |
| **Missed Merges** | Recall problem | Real duplicates left as two rows. Annoying but reversible via Safety Net B / future dedup pass. Want few. |
| **False PC** | PC precision problem | PARENT_CHILD links created where no ownership exists, OR with wrong direction. Reversible metadata error. |
| **Missed PC** | PC recall problem | Real ownership relations not recorded. Invisible to users (rows work fine); fully recoverable later. |

## Core vs Tail split (going forward)

Historical note: Sprint 6's "Expanded Core" remains a useful benchmark artifact. Going forward, core/tail should be defined by **product risk**, not by whichever bucket an earlier pipeline happened to produce.

### Core definition (production-risk core)

A producer is **Core** if ANY of:
- Has any **US-market signal**: country = US, any linked TTB COLA, or any wine linked via US retailer/importer staging (Spec's, Wally's, Flatiron, Skurnik, Kermit Lynch, Empson, Winebow, European Cellars, Polaner, Domestique, Best Wine Store, Firstleaf, Last Bottle, WineDeals), OR
- Has any **shopper-visible commercial signal** outside the US: any `wine_vintage_scores`, any `wine_vintage_prices`, any international retailer presence (LCBO, Systembolaget, BC Liquor, PA PLCB), or any competition presence (Berliner, TEXSOM, Enofile), OR
- Has **catalog blast radius**: at least 5 canonical wines, OR
- Is manually promoted into core because it is marquee, demo-visible, previously flagged high-risk, or part of a known family/brand cluster where a bad merge would be unusually visible.

**Tail = everything else**: usually LWIN-only or near-LWIN-only producers with thin catalogs and few external visibility signals.

Operational rule: recompute and then freeze the core/tail split at the start of a dedup sprint. Keep the frozen split for evaluation and scorecards so quality metrics remain comparable within that sprint.

### Current split (as of 2026-04-18)

| | Producers | Wines | % of wines |
|---|---:|---:|---:|
| Core | 14,042 (42%) | 192,148 | **85%** |
| Tail | 19,239 (58%) | 33,133 | 15% |

Tail is dominated by low-wine-count producers: 15,070 have 1-2 wines (most of Tail).

## Quality targets

Parent/child modeling is intentionally removed from the production-readiness gate below. The gating question is whether merge decisions are safe enough for Loam's credibility targets.

### Core

| Metric | Target | Validated by |
|---|---|---|
| False Merges | **0** | Independent evidence review on all execution-bound core merges + pre-execution scorecard |
| Missed Merges | **≤3%** | Stratified audit of core skips plus targeted recall probes on known duplicate clusters |
| Unresolved coverage | High-risk ambiguous core pairs should be `FLAGGED`, not force-merged | Review queue audit |
| Survivor correctness | Survivor must match current US-market label form | Pre-execution survivor audit |

### Tail

| Metric | Target | Validated by |
|---|---|---|
| False Merges | **≤1%** | Conservative thresholds, bias to SKIP, stratified audit of execution-bound tail merges |
| Missed Merges | **≤10%** | Random tail-skip audit plus targeted sampling of known hard patterns |
| Unresolved coverage | Tail may tolerate a larger `FLAGGED` queue than core | Review queue audit |

## Defaults & conventions carried forward to Sprint 7+

1. **Terminology:** False Merges / Missed Merges / False PC / Missed PC. Never "precision/recall" in user-facing docs.
2. **Core/Tail split** is a frozen product-risk tier for the sprint, not a live moving bucket.
3. **Spending priority:** disproportionate Core investment; Tail gets stricter bias-to-skip behavior and sampled audits.
4. **Execution scope:** merge-only first; parent/child is optional later metadata work.
5. **Human review cap:** keep review tight and high-signal; ambiguous rows should become `FLAGGED`, not a second full manual dedup project.
6. **Quality measurement:** publish a pre-execution scorecard before any DB mutation.

## Working rebuild note

The current working plan for the Codex rebuild lives in `data/sprints/dedup/rebuild_roadmap.md`. The Step 7-11 section below remains as the historical Sprint 6 execution plan that was audited, not the new baseline.

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
