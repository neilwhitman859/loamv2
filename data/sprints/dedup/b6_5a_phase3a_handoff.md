# B6.5a Phase 3a Handoff

**Date:** 2026-04-18 · **Session:** long — compacted after this doc written · **Next block:** Phase 4-8 (Stage 3 routing → execution)

---

## Where we are

**B6.5a-partial v2 complete.** Phase 3a ran Haiku+Serper (new tier built this session) on 3,403 critical pairs. Result: 358 wrong merges prevented + 505 real merges recovered at $20 cost. Sprint 6 total spend: ~$231.67 of $250 ceiling.

The producer dedup pipeline has now:
1. Blocked 151K candidate pairs (B6.3)
2. Run L1 Haiku batched + L1.5 Gemini basic on all 151K (B6.3 + B6.5a Steps 1-2)
3. Built Stage 1 routing at 0.95/0.95 (91,555 auto-skip, 1,520 auto-merge, 57,810 escalated)
4. Run L2 Haiku rich + L2.5 Gemini rich on 57,810 escalations (B6.5a Steps 4-5)
5. Built Stage 2 routing (34,078 auto-skip, 676 auto-merge, 3,226 PC review, 19,575 residual)
6. **NEW:** Phase 3a — Haiku+Serper (pre-fetched Google search + Haiku 4.5 rich prompt) on the 3,403 highest-stakes pairs:
   - 1,520 Stage 1 auto-merges — **validated** (210 FPs caught → now SKIP/PC)
   - 676 Stage 2 auto-merges — **validated** (148 FPs caught → now SKIP/PC)
   - 1,207 cross-family disagreements (L2 SKIP × L2.5 MERGE) — **arbitrated** (505 real merges found)

## Key finding: Haiku+Serper is the right tool

Serper.dev ($1/1K queries) + Haiku 4.5 rich prompt with pre-fetched web snippets = ~$0.006/pair. Validated against L3 Sonnet+Anthropic-web on 65 pairs + broader 1,199-pair stratified sample. **Haiku+Serper is both cheaper (25-30×) AND more accurate** than L3 Sonnet+web on merchant/négociant patterns — it applies §11 strictly (cites 11.4.d, 11.4.f, 11.4.i) and catches patterns Sonnet's tool-use approach missed.

**Implications for Sprint 7 wine dedup:** Haiku+Serper would cost ~$2-3K on 300-500K wine pairs vs $44K+ for L3 Sonnet+Anthropic-web. Build this once, reuse forever.

## FP rates measured at scale (Phase 3a results on full 3,403 pairs)

| Bucket | Pairs | Confirmed | Flipped to SKIP | Flipped to PC | FP rate |
|---|---:|---:|---:|---:|---:|
| Stage 1 auto-merge | 1,517 | 1,281 (84.4%) | 210 (13.8%) | 23 (1.5%) | **15.6%** |
| Stage 2 auto-merge | 676 | 502 (74.3%) | 148 (21.9%) | 24 (3.6%) | **25.7%** |
| Cross-family disagree | 1,205 | (merge yield 505 = 41.9%) | 647 | 53 | — |

**Without Phase 3a, the pipeline would have applied ~358 wrong merges** to the producers table. These included high-visibility ones like Jordan ZA vs Jordan CA, Chapoutier's Australian ventures conflated with French M. Chapoutier, Miguel Torres Chile vs Torres Spain, Taylor's Port vs Taylors Australia, Mouton Rothschild vs the distinct Château Paveil de Luze, etc.

## US-market audit (150 producers across 4 tiers)

Tested whether wines Americans actually drink are handled correctly:

- **Tier A (mass market, 26 IDs):** 0 wrong merges. Cupcake/19 Crimes/Coppola internal dupes staged for MERGE correctly.
- **Tier B (premium American, 35 IDs):** 1 wrong merge caught (Jordan ZA/CA). All demo producers (Stag's Leap WC, Ridge, Caymus, Silver Oak, etc.) cleanly handled.
- **Tier C (collector fine wine, 27 IDs):** 2 wrong merges caught (Mouton Rothschild/Luze, HdB/DRC). DRC split correctly merged.
- **Tier D (European imports, 57 IDs):** 10 wrong merges caught — this is where the real complexity lives (family empire splits, négociant variants, cross-country same-family operations).

Real merges recovered from cross-family disagreements: Antinori/Santa Cristina, Fonseca & Zeller / Fonseca, Cupcake internal dupes, Penfolds internal dupes, Waltraud Riesling → Torres (sub-brand consolidation).

**The DB is in good shape for US-market queries once Phase 4-8 complete.**

## Decisions made

1. **Serper as web-grounding vendor, Haiku 4.5 as rigor tier** (replaces L3 Sonnet for Sprint 7 forward)
2. **Skip Phase 3b residual bulk sweep** — the 19.5K residual is 68% thin LWIN long-tail; Safety Net B handles post-execution. Phase 3a already caught the highest-risk pairs.
3. **Stage 2 SKIP threshold stays at 0.95/0.95** (earlier decision — Serper economics flipped the 0.93 cost argument)
4. **Keep dashboard auto-update every 10 min** per user preference; don't narrate updates (saved to memory)
5. **Quality bar:** ~100% final-state correctness on producers table, validated via Phase 3a on auto-merge buckets + safety nets for long tail

## Open decisions for Phase 4+

None blocking. User approved proceeding with Phase 4-8 after commit + compact.

## What's next (remaining phases)

### Phase 4 — Stage 3 routing ($0, SQL only)

Build `producer_dedup_routing_stage3` table combining L2, L2.5, and Haiku+Serper verdicts (method_name `l2_haiku_rich_web`):

**Auto-apply rules (3-tier consensus, near-zero FP):**
- L2 MERGE ≥0.90 AND L2.5 MERGE ≥0.90 AND Haiku+Serper MERGE ≥0.90 → auto-apply MERGE
- L2 SKIP AND L2.5 SKIP AND Haiku+Serper SKIP → auto-apply SKIP
- Haiku+Serper PC at any conf → user review
- Anything else → user review

Expected: ~1,000 confirmed auto-merges, ~2,500 user-review pile.

### Phase 5 — L4 Opus-inline audit ($0, chat)

Pull full routing table into Opus context. Look for:
- Inconsistent handling of same-shape pairs
- Cross-pair patterns suggesting §11 amendments
- Suspicious unanimous-auto-merges
- Edge cases that should flag to user review

### Phase 6 — §11 amendments ($0)

Add to `docs/IDENTITY_RULES.md` §11.4:
- **11.4.m** — shared-surname-split rule (Faustino vs Faustino Rivero Ulecia; Barbi vs Podere Barbi; Giordano Giovanni vs Giovanni Giordano) — when pair shares surname but zero shared wines and different web-confirmed websites → SKIP
- **11.4.n** — cross-country same-name rule (McPherson US/AU; Jordan ZA/US; Miguel Torres ES/CL) — same name in different countries = distinct unless shared LWIN/TTB
- **11.4.o** — collaboration/JV label rule ("&", "and", "+" in name; Savart & Chartogne; Fonseca & Van Zeller) — never auto-merge with the single-name variant
- **11.4.p** — wine merchant reinforcement (strengthens 11.4.d) — BBR, Lay & Wheeler, The Wine Society, Jeroboams, Corney & Barrow, Sotheby's, Justerini & Brooks are merchants, never producers
- **11.4.q** — Hospices de Beaune négociant variants — HdB (X) vs HdB (Y) where X≠Y are distinct négociant bottlings, SKIP by default

### Phase 7 — B6.5b interactive review ($0-5)

Claude presents ~2-3K curated pairs by pattern cluster. User signs off per-batch.

### Phase 8 — B6.6 execution + B6.N close ($5-10)

Apply merges, populate `producer_merge_history`, run Safety Net B leftover scan, close sprint.

## Budget state

| Category | Amount |
|---|---:|
| B6.3 | $78.44 |
| B6.4 | $24.51 |
| B6.5a Step 1 (L1.5 Gemini) | $32.13 |
| B6.5a SKIP audit | $6.57 |
| B6.5a L2 production | $38.04 |
| B6.5a L2.5 production | $13.10 |
| B6.5a analysis probes | $16.01 |
| Serper validation runs (65 + 269 + 930) | ~$2.00 |
| **Phase 3a Haiku+Serper on 3,403 pairs** | **~$20.00** |
| Demo set probe | $0.79 |
| **Sprint 6 total** | **~$231.67** |
| Remaining of $250 ceiling | ~$18.33 |
| Phase 4-8 projected | ~$5-15 |
| **Projected sprint total** | **~$245** — within ceiling |

## DB state at handoff

### Tables created (B6.5a working)
- `producer_dedup_routing_stage1` — 150,885 rows with Stage 1 action
- `producer_dedup_routing_stage2` — 57,810 escalations with Stage 2 action
- `_b6_5a_skip_audit_sample` — 600 rows (can drop)

### Method names added to `producer_dedup_pairs`
| method_name | rows | purpose |
|---|---:|---|
| `l1_haiku_batch` | 151,120 | L1 from B6.3 |
| `l1_gemini_basic` | 150,885 | L1.5 from B6.5a Step 1 |
| `l2_haiku_rich` | 57,459 production + 600 cal | L2 rich from B6.5a Steps 4 |
| `l2_gemini_rich` | 57,570 | L2.5 rich from B6.5a Steps 5 |
| `l2_skip_audit` + `l3_skip_audit` | 600 + 600 | Stage 1 SKIP audit |
| `l3_probe_noweb` + `l3_probe_web` | 470 + 65 | Stage 2 analysis probes |
| `l3_probe_demo` | 44 | Demo-set probe |
| `l2_web_validate` + `l2_web_broad_valid` + `l2_web_broad_valid_3x` | 65 + 269 + 930 | Serper+Haiku validation runs |
| **`l2_haiku_rich_web`** | **3,398** | **Phase 3a production — authoritative tier** |

### Files produced this session
- `pipeline/lib/serper.py` — Serper.dev wrapper (new)
- `pipeline/identity/producer_dedup_l2_web.py` — Haiku+Serper classifier (new)
- `pipeline/analyze/session_tokens.py` + `update_dashboard.py` — session token tracker + dashboard auto-updater (new)
- `data/sprints/dedup/b6_5a_stage2_analysis.md` — Stage 2 analysis (earlier)
- `data/sprints/dedup/b6_5a_phase3a_handoff.md` — this doc
- `data/sprints/dedup/us_market_producers.json` + `us_market_matches.json` — 150 target names + their DB IDs
- `data/sprints/dedup/b6_5a_phase3a.json` + `b6_5a_probe_*.json` + `b6_5a_broad_validation*.json` — pair ID lists
- `data/stats/spend_ledger.md` — running spend ledger
- `data/stats/b6_5a_*.log` — 15+ run logs

## Pick-up instructions for fresh session

1. Read this file + `docs/SCHEMA.md` (§16 search infrastructure) + `docs/IDENTITY_RULES.md` §11
2. Check current sprint state: `cat data/sprints/dedup/budget.json`
3. Verify Phase 3a is written: `SELECT COUNT(*) FROM producer_dedup_pairs WHERE method_name='l2_haiku_rich_web'` should be ~3,398
4. Phase 4 starting point: build `producer_dedup_routing_stage3` table joining L2 + L2.5 + `l2_haiku_rich_web` verdicts per pair
5. Apply Stage 3 auto-apply rules (see Phase 4 section above)
6. Then Phase 5 L4 Opus audit → Phase 6 §11 amendments → Phase 7 review queue prep → Phase 8 execution

## User preferences to preserve

- Dashboard at `data/dashboard.html`: **light mode, auto-updates every 10 min** via `pipeline/analyze/update_dashboard.py`. Don't narrate updates (per `memory/feedback_silent_dashboard_updates.md`).
- Opus inline reasoning preferred over scripted Haiku for audit/reasoning work (`memory/feedback_opus_inline_reasoning.md`).
- Quality bar: final-state ~100% correctness on producers table. Wrong merges are worse than missed merges.
- No parallel Supabase API calls (use Python direct psycopg2 via `get_conn()`).
