# B6.5a Stage 2 Analysis & Recommendations

**Date:** 2026-04-18 · **Status:** Stopped after L2+L2.5 per user directive for reassessment · **Spend:** ~$177 of $250 sprint ceiling

---

## Executive Summary

**Stage 2 is built.** 57,555 Stage 1 escalations processed through L2 Haiku rich + L2.5 Gemini rich. Distribution:
- **Auto-MERGE:** 676 pairs (1.2%) — ⚠️ 10% L3-confirmed FP rate on probe
- **Auto-SKIP:** 34,078 pairs (59.2%) — ✓ 1% L3 FN rate, acceptable
- **User review (PC):** 3,226 pairs (5.6%)
- **Residual → L3:** 19,575 pairs (34.0%) — ❌ over 10K threshold, same SKIP-threshold issue as Stage 1
- **Missing tier:** 255 pairs (0.4%) — L2 or L2.5 didn't write

**Three headline findings:**

1. **Stage 2 auto-MERGE has a real ~10% FP rate.** L3 no-web probe on 100 random Stage 2 auto-MERGEs found 10 L3-SKIP + 7 L3-UNCERTAIN. Real patterns L3 catches that cross-family misses: shared-surname splits (Faustino vs Faustino Rivero Ulecia), Hospices de Beaune négociant variants, same-name cross-country (McPherson US/AU), collaboration wines (Savart & Chartogne). **Auto-applying these 676 would merge ~68 wrong producers.**

2. **Residual-bucket SKIP threshold is too tight, same as Stage 1.** 88% of residual (17,214 pairs) is L2+L2.5 both-SKIP consensus below the 0.95/0.95 joint threshold. Haiku's median rich-SKIP conf is 0.94. **Dropping Stage 2 SKIP threshold to 0.93/0.93 would drop residual from 19,575 to ~7,330 (in-range for L3)** with measured 2% FN rate — same safety profile as Stage 1 at 0.95.

3. **L3 web earns marginal value on random residuals but high value on specific patterns.** Web vs no-web A/B on 20 random residuals: 4 MERGE (web) vs 3 MERGE (no-web) — 5% delta. But web on cross-family disagreements: 67% L3 MERGE yield. Recommend hybrid, not full web.

**Recommended next-step call: pause for user decision between three paths (detailed below).**

---

## Stage 2 Routing Distribution

### Final bucket table

| Stage 2 Action | Count | % of escalations | Cost-at-exec |
|---|---:|---:|---|
| `stage2_auto_merge` | 676 | 1.2% | Would merge, ⚠️ 10% suspect FP |
| `stage2_auto_skip` | 34,078 | 59.2% | No action, ~1% FN (340 pairs) |
| `stage2_user_review_pc` | 3,226 | 5.6% | To B6.5b review pile |
| `escalate_to_l3` | 19,575 | 34.0% | L3 rigor tier |
| `stage2_missing_tier` | 255 | 0.4% | Script gap — L2 or L2.5 didn't write |

### Cross-family agreement (L2 × L2.5, all 57,555 both-scored pairs)

| L2 | L2.5 | n | % |
|---|---|---:|---:|
| SKIP | SKIP | 51,357 | 89.2% |
| SKIP | PC | 1,389 | 2.4% |
| PC | PC | 1,174 | 2.0% |
| MERGE | MERGE | 1,097 | 1.9% |
| SKIP | MERGE | 1,055 | 1.8% |
| PC | SKIP | 507 | 0.9% |
| UNCERTAIN | MERGE | 208 | 0.4% |
| UNCERTAIN | SKIP | 189 | 0.3% |
| MERGE | PC | 162 | 0.3% |
| MERGE | SKIP | 152 | 0.3% |
| PC | MERGE | 133 | 0.2% |
| other | | ~132 | <0.3% |

**Observations:**
- **MERGE cross-family consensus: 1,097 pairs.** Only 676 cleared the 0.90 auto-apply threshold on both tiers — 421 consensus-MERGE pairs below threshold are currently in residual. L3 should catch these.
- **PC consensus: 1,174** — all routed to user review.
- **Diagonal disagreements:** 1,055 L2-SKIP × L2.5-MERGE (Gemini aggressive) + 152 L2-MERGE × L2.5-SKIP (Haiku aggressive). **Gemini-side is 7x larger**, consistent with its known aggressive bias from B6.4 calibration.

### Residual composition (19,575 pairs, why so high?)

| L2 verdict | L2.5 verdict | n | Notes |
|---|---|---:|---|
| SKIP | SKIP | 17,214 | Both SKIP but one/both under 0.95 |
| SKIP | MERGE | 1,052 | **Aggressive Gemini bucket** — 60% L3 MERGE yield per probe |
| MERGE | MERGE | 420 | Consensus MERGE below 0.90 threshold |
| UNCERTAIN | MERGE | 208 | Gemini MERGE, Haiku unsure |
| UNCERTAIN | SKIP | 186 | |
| MERGE | SKIP | 151 | Reverse disagreement |
| other | | 344 | Smaller buckets |

Of the 17,214 both-SKIP residuals:
- Both ≥0.93: 12,248 (71%)
- Both ≥0.90: 16,337 (95%)
- Median L2 conf 0.94, median L2.5 conf 0.98

---

## Probe Results (470 no-web + 65 web pairs, $16.01 spent)

### Probe 1 — Stage 2 SKIP residual @ 0.93-0.94 band (100 pairs)

L3 no-web on pairs where both tiers SKIP at 0.93-0.94:

| L3 verdict | n | % |
|---|---:|---:|
| SKIP | 96 | 96% |
| MERGE | 2 | **2%** |
| PC | 1 | 1% |
| UNCERTAIN | 1 | 1% |

**Interpretation:** Dropping Stage 2 SKIP threshold to 0.93/0.93 would auto-skip ~12,000 more pairs with a measured 2% L3-MERGE rate = ~240 missed merges (FN). Same safety profile Stage 1 had at 0.95. Safety Net B catches the long tail post-execution.

### Probe 2 — Stage 2 auto-SKIP validation (100 pairs, control)

| L3 verdict | n | % |
|---|---:|---:|
| SKIP | 96 | 96% |
| MERGE | 1 | **1%** |
| UNCERTAIN | 3 | 3% |

Current Stage 2 auto-SKIP bucket (34,078 pairs) has ~1% FN rate → ~340 pairs silently merging-wrongly. Acceptable, covered by Safety Net B.

### Probe 3 — Stage 2 auto-MERGE validation (100 pairs) ⚠️

| L3 verdict | n | % |
|---|---:|---:|
| MERGE | 81 | 81% |
| SKIP | **10** | **10%** |
| PC | 2 | 2% |
| UNCERTAIN | 7 | 7% |

**🚨 10% L3 flip to SKIP.** These are not low-conf cases — they're pairs L2 and L2.5 both said MERGE ≥0.90. L3 Sonnet disagrees. Examined the 10 disagreements:

| Pair | Pattern | L3 reason |
|---|---|---|
| Faustino Rivero Ulecia / Faustino | shared-surname-split | "two distinct Spanish producers" |
| Berry Bros. & Rudd (Nicolas Potel) / Nicolas Potel (Maison Roche Bellene) | négociant-prefix | "distinct brand identities" |
| Janisson Baradon / Janisson | shared-surname-split | "distinct Champagne producers" |
| McPherson US / McPherson AU | same-name-cross-country | "Texas High Plains vs SE Australia" |
| Tertre Daugay / Daugay | shared-surname-split | "two distinct Saint-Emilion estates" |
| Hospices de Beaune (Morey Blanc) / HdB (Cecile Tremblay) | négociant-variant | "§11.4.l, joint-venture brand" |
| Andre Lorentz / Lorentz | shared-surname-split | "zero shared wines across catalogs" |
| Hospices de Beaune (Lejeune) / Hospice de Beaune | négociant-variant | "negociant name integral to label" |
| Savart & Chartogne / Savart | collaboration | "collaboration label" |
| Weingut Johannisberg / Johannisberg | formal-vs-casual | (Schloss Johannisberg ambiguity) |

Plus 7 UNCERTAIN cases showing similar patterns (Pazo Senorans/Pazo Senoras, Stables Ngatarawa/Ngatarawa, Vray Canon Boyer/Canon Boyer, etc.).

**This is the most important finding of the analysis.** Stage 2 auto-MERGE cross-family agreement at ≥0.90 is not as precise as B6.4 calibration suggested (98.7% → actually 81-90% in production). Reasons:
1. Calibration was 600 stratified pairs; production has a long-tail of edge cases calibration didn't sample.
2. Specific pattern classes (shared-surname-split, HdB négociant variants, same-name-cross-country, collaboration labels) trip both Haiku and Gemini together but not L3 Sonnet.
3. These are exactly the §11 edge cases the rules try to handle but tier-2 classifiers can't always tell the wine catalogs apart.

**Recommendation: do NOT auto-apply Stage 2 auto-MERGE without L3 confirmation or user review.**

### Probe 4 — Random residual preview (100 pairs)

| L3 verdict | n | % |
|---|---:|---:|
| MERGE | 6 | 6% |
| PC | 3 | 3% |
| SKIP | 87 | 87% |
| UNCERTAIN | 4 | 4% |

Extrapolated to full residual of 19,575: **~1,175 MERGEs + ~590 PCs + ~785 UNCERTAINs** that L3 no-web would surface. Bulk is confirmed-SKIP.

### Probe 5 — Cross-family disagreement (L2 SKIP × L2.5 MERGE, 50 pairs)

| L3 verdict | n | % |
|---|---:|---:|
| MERGE | 30 | **60%** |
| UNCERTAIN | 11 | 22% |
| SKIP | 7 | 14% |
| PC | 2 | 4% |

**Gemini is right on these 60% of the time.** 1,055 such pairs in production → ~633 real merges Haiku missed. These need L3 arbitration — can't drop them without losing real merges.

### Web probes (65 pairs, $9.01)

**Probe 6 — Random residual A/B (20 pairs):**
- No-web: 3 MERGE / 17 SKIP
- Web: 4 MERGE / 16 SKIP
- **Web delta on random residuals: 1 additional MERGE = 5%**

**Probe 7 — Négociant patterns (20 pairs, web-only):**
- 4 MERGE / 16 SKIP = **20% MERGE rate**
- Web confirms the négociant variants in this bucket are mostly SKIPs (80%)

**Probe 8 — Cross-family disagreement (15 pairs, web-only):**
- 10 MERGE / 5 SKIP = **67% MERGE rate**
- Web strongly arbitrates these disagreements

**Probe 9 — Cross-country same-brand (10 pairs, web-only):**
- 3 MERGE / 7 SKIP = **30% MERGE rate**
- Web catches some globals (Gallo brands, etc.) but most are distinct country producers

**Web vs no-web cost/value analysis:**

| Bucket | Pairs | No-web | Web | Web earns? |
|---|---:|:---:|:---:|:---:|
| Random residual (bulk) | ~18K | 6% MERGE | ~7% MERGE | ❌ Marginal |
| Négociant patterns | ~1-2K | — | 20% MERGE | 🟡 Moderate |
| Cross-family disagreement | 1,055 | — | 67% MERGE | ✅ Strong |
| Cross-country same-brand | ~100-500 | — | 30% MERGE | 🟡 Moderate |

**Web costs 11x more per pair.** Only worth it where it shifts the verdict, which is disagreements and specific patterns — NOT random residuals.

---

## Critical Finding: §11 Pattern Amendments Surfaced

The probe data reveals several patterns that §11 doesn't cleanly cover or that Haiku+Gemini trip on. Candidates for §11.4 amendments:

| Pattern | Example | Current rule | Proposed amendment |
|---|---|---|---|
| Shared-surname-split | Faustino / Faustino Rivero Ulecia | §11.4.h (accent) doesn't cover | Add §11.4.m: "If pair shares one distinguishing word but zero shared wines, SKIP regardless of substring/trigram" |
| HdB négociant variants | HdB (Lejeune) / HdB (Bichot) | §11.4.i covered, but tiers don't apply | Escalate HdB-pattern to L3 always |
| Same-name cross-country | McPherson US / McPherson AU | §11.4.j commune overlap | Extend to cross-country: different country_id = SKIP unless shared LWIN/TTB |
| Collaboration-label | Savart & Chartogne / Savart | §11.4.l JV | Reinforce: "&", "and", "+", "with" in name → never auto-merge |
| Second-label short-form | Schloss Johannisberg / Johannisberg | §11.4.e | Often ambiguous; flag for user review not auto-merge |

**These are not hypothetical — each shows up in the 10-17 L3-SKIP/UNCERTAIN disagreements.** Recommend a pre-B6.6 §11 amendment pass to lock these in before execution.

---

## Recommendations

### Recommendation 1: Do NOT auto-apply Stage 2 auto-MERGE without L3 validation
**Action:** Route all 676 Stage 2 auto-MERGE pairs through L3 no-web before execution.
- Cost: 676 × $0.013 = **$8.79**
- Validates at most ~610 as true MERGEs
- Flags ~68 for user review (the 10% L3-SKIP/UNCERTAIN rate)
- Risk without: ~68 wrong merges applied to production. With L3 validation: near-zero.

### Recommendation 2: Lower Stage 2 SKIP threshold to 0.93/0.93
**Action:** Re-run Stage 2 routing SQL with loosened SKIP threshold.
- Auto-skip jumps from 34,078 → ~46,000
- Residual drops from 19,575 → ~7,300 (in-range per session prompt)
- Measured FN rate: 2% (same as Stage 1 at 0.95 — acceptable)
- Cost: $0 (SQL only)
- **Session prompt's "residual >10K = tighten and re-run" rule is satisfied by loosening SKIP, not tightening MERGE.**

### Recommendation 3: L3 strategy for ~7,300 residual
Given web delta is only 5% on random residuals but 67% on disagreements:

**Option A — Hybrid (my top pick):**
- L3 no-web on bulk residual (~6,500 pairs): ~$85
- L3 web on cross-family disagreements (~800 pairs): ~$118
- L3 web on négociant-pattern residuals (~200 pairs): ~$30
- **Total: ~$233**
- Expected: ~500-700 auto-apply MERGEs, ~1,200-1,500 user review pairs

**Option B — No-web only + user review:**
- L3 no-web on all 7,300: ~$95
- Skip web entirely; user review handles disagreements via context pack
- **Total: ~$95**
- Expected: ~400-600 auto-apply MERGEs, ~1,800-2,200 user review pairs (larger pile)

**Option C — Skip L3 entirely:**
- All 7,300 residual → user review
- **Total: $0**
- Review pile: 7,300 + 3,226 PC = **10,526 pairs** (user: do you want to review 10K pairs?)
- Cheapest but largest manual load

### Recommendation 4: Cost-optimize L3 for future sprints

Based on web-search research done during this session:
- **Anthropic's native `web_search_20250305` costs $0.147/pair** (search fee + Sonnet tokens)
- **Pre-fetch with Serper.dev ($1/1K queries) + Haiku rich** would cost ~$0.008/pair — **18x cheaper**
- For wine dedup (Sprint 7, estimated 300-500K pairs), this approach would save $600-1,200
- Worth investing 1 session to build in Sprint 7 opening

### Recommendation 5: §11 amendment pass before B6.6
Before executing merges in B6.6, update IDENTITY_RULES §11 with:
- 11.4.m — shared-surname-split rule
- 11.4.n — cross-country same-name rule strengthening
- 11.4.o — collaboration-label rule
- L4 Opus audit should specifically scan for these patterns in the final review pile

---

## Budget Accounting

| Item | Cost |
|---|---:|
| B6.3 L1 Haiku batched | $78.44 |
| B6.4 Calibration + thresholds | $24.51 |
| B6.5a L1.5 Gemini basic (151K) | $32.13 |
| B6.5a SKIP audit (L2+L3 no-web, 600 pairs) | $6.57 |
| B6.5a L2 Haiku rich (57K) | $38.04 |
| B6.5a L2.5 Gemini rich (57K) | $13.10 |
| **B6.5a Stage 2 analysis probes** | **$16.01** |
| **Sprint 6 total so far** | **~$208.80** |
| Sprint ceiling | $250 |
| **Remaining** | **~$41.20** |

**If we proceed with Option A (hybrid L3 + Stage 2 auto-MERGE validation):**
- Stage 2 auto-MERGE L3 validation: $8.79
- Option A L3 hybrid on 7,300 residual: $233
- B6.5a L4 Opus audit: $0
- B6.5b interactive review + context packs: $0-5
- **Projected sprint total: ~$451** — **over $250 ceiling by $200**

**If we proceed with Option B (no-web only):**
- Stage 2 auto-MERGE L3 validation: $8.79
- L3 no-web on 7,300 residual: $95
- **Projected sprint total: ~$313** — over ceiling by $63

**If we proceed with Option C (skip L3):**
- Stage 2 auto-MERGE L3 validation: $8.79
- **Projected sprint total: ~$218** — UNDER ceiling, but 10.5K user review pile

**Budget decision needed.**

---

## Suggested Next Step

**Decision required from user:** Pick one of three paths.

**Path 1 — Pragmatic minimum (~$218 total, within ceiling):**
- Lower Stage 2 SKIP to 0.93/0.93 (drops residual to 7.3K)
- L3 no-web validate the 676 Stage 2 auto-MERGEs ($9)
- Skip the rest of L3
- User reviews 7,300 + 3,226 + flagged = ~11K pairs in B6.5b with context packs
- Quality bar met via user review and Safety Net B
- Largest review burden but cheapest and in-budget

**Path 2 — Balanced (~$313, $63 over ceiling, request raise to $320):**
- Lower Stage 2 SKIP to 0.93/0.93
- L3 no-web validate 676 auto-MERGEs
- L3 no-web on 7,300 residual ($95)
- Auto-apply where L3 MERGE ≥0.92; residual after L3 → user review
- Expected user review ~2-3K pairs, much more manageable
- Recommended if budget can flex

**Path 3 — Rigor (~$451, needs $500 ceiling raise):**
- Full hybrid: L3 no-web bulk + L3 web on cross-family disagreements + négociants
- Near-zero FP, smallest user pile (~1.5-2K)
- Overkill for Sprint 6 unless quality bar is absolute

**My recommendation:** **Path 2.** The L3 no-web pass on residual is cheap ($95) and meaningfully reduces B6.5b review burden while catching the 60% L3-MERGE yield on the 1,055 cross-family disagreement bucket that's currently in residual. Web pre-fetch optimization can wait for Sprint 7 where it pays off more.

---

## Files produced during B6.5a

- `data/sprints/dedup/b6_5a_routing_sql.md` — Stage 1/2/3 routing SQL reference
- `data/sprints/dedup/b6_5a_l4_audit_queries.sql` — L4 audit query templates (unused, kept for B6.5a continuation)
- `data/sprints/dedup/b6_5a_stage1_escalations.json` — 57,810 pair IDs routed from Stage 1 to L2/L2.5
- `data/sprints/dedup/b6_5a_skip_audit_pair_ids.json` — 600 band-stratified Stage 1 SKIP audit sample
- `data/sprints/dedup/b6_5a_probe_*.json` — probe pair-ID lists (9 files)
- `data/stats/spend_ledger.md` — running spend ledger
- `pipeline/analyze/session_tokens.py` — session-token computation
- `pipeline/analyze/update_dashboard.py` — dashboard auto-update script
- **`data/sprints/dedup/b6_5a_stage2_analysis.md`** — this report

## Database tables created

- `producer_dedup_routing_stage1` — 151,150 pairs with Stage 1 action
- `producer_dedup_routing_stage2` — 57,810 escalations with Stage 2 action
- `_b6_5a_skip_audit_sample` — 600 stratified audit sample (can drop)

## Method names added to `producer_dedup_pairs`

| method_name | rows | notes |
|---|---:|---|
| `l1_gemini_basic` | 150,885 | Stage 1 cross-family check |
| `l2_haiku_rich` | 57,459 production + 600 cal | Stage 2 Haiku rich |
| `l2_gemini_rich` | 57,570 production | Stage 2 Gemini rich |
| `l2_skip_audit` | 600 | Stage 1 SKIP audit |
| `l3_skip_audit` | 600 | Stage 1 SKIP audit |
| `l3_probe_noweb` | 470 | Stage 2 analysis probes |
| `l3_probe_web` | 65 | Stage 2 analysis probes |
