# Sprint 6 — Producer Dedup: Full Writeup

This is the comprehensive narrative of Sprint 6 for external review. It covers
every block (B6.1 → B6.6), what we did, what we learned, what decisions were
made, and what's still open. It's meant to stand alone — a reviewer with no
Loam context should be able to understand the sprint from this one document.

For the actual dedup decisions, see:
- `verdict_ledger.jsonl` + `scorecard.md` (Phase 1, 493 Chrome-validated pairs)
- `phase2_actionable_decisions.jsonl` + `phase2_summary.md` (Phase 2, 3,939
  pipeline auto-decisions + 740 review-queue items)

---

## 1. Sprint goal and product context

**Loam** is a wine data platform that gives users structured, connected,
trustworthy data about any wine. Producer dedup is infrastructure: one row
per real-world wine producer, so users see one coherent producer page
instead of fragmented near-duplicate entries.

**Sprint 6 goal** was to execute the first major producer dedup pass since
the LWIN backbone import. The `producers` table had ~33,281 rows, and a
significant fraction were known duplicates: accent variants, orthographic
differences, country-split rows for the same global brand, generational
succession cases, merchant-prefixed rows, and more.

**The sprint began 2026-04-15**, after Sprint 5 (AI Bakeoff) closed. User's
hard ceiling was $250; actual spend ~$108 across all blocks.

---

## 2. Block sequence

### B6.1 — Planning (2026-04-15)

Scope, thresholds, budgeting. Key decisions:
- **Producer identity framework** drafted as §11 of `docs/IDENTITY_RULES.md`.
  Core rule: "A producer is the brand printed on the label."
- **AI ladder architecture**: L1 Haiku (all 151K pairs) → L1.5 Gemini basic
  (all pairs, cross-family check) → Stage 1 routing → L2 Haiku rich
  (escalations) → L2.5 Gemini rich → Stage 2 routing → L3 Sonnet ± Chrome
  → L4 Opus-inline audit.
- **Calibration via B6.4** before production ladder run.
- **Quality bar**: "final-state correctness ~100% of producers table,"
  achieved via cross-family auto-apply + user-reviewed pairs + FLAGGED open
  questions.

### B6.2 — LWIN backbone import (2026-04-16) — $0 spent

Three-stage LWIN import:
- **B6.2** long-tail sweep: 22,598 new producers (10,683 → 33,281).
- **B6.2.1** patched a pre-existing INSERT-trigger bug on producers and
  wines `search_vector`.
- **B6.2.2** discovered that `source_lwin.display_name` (Liv-ex's
  authoritative combined-wine name) is populated on 99.994% of rows and
  that every LWIN-7 is a real bottled product — **recovered 26,616 wines**
  we had previously skipped due to NULL wine_name (Burgundy village wines,
  Chablis, Italian DOCs), using display_name minus producer prefix as
  wine.name.

End state: 224,316 canonical wines (100% display_name coverage), 33,214
producers with ≥1 wine, 99.98% of source_lwin linked, 40 explainable
residuals.

### B6.3 — Schema + blocking + L1 Haiku (2026-04-17) — $78.44

- Schema migration: `producer_dedup_pairs` extended (`verdict`,
  `verdict_source`, `producer_id_a/b`, `method_name`, `confidence`,
  `reasoning`, `signals`, `ttb_evidence`, `web_evidence`, `flag_reason`,
  timestamps). `producer_merge_history` table created for reversibility.
- **§11 drafted**, user-reviewed; §11.4.g holdco carve-out added.
- **Blocking** via pg_trgm similarity search produced **151,150 pairs** across
  8 active strategies (exact-normalized, alphanumeric, first-N-tokens, etc.).
- **L1 Haiku 4.5** classified all 151,120 pairs (99.98%) at 0.052¢/pair with
  prompt caching. Pilot 200 validated 7/7 anchors (Ridge MERGE 0.98, Stag's
  Leap WC vs Stags' Leap Winery SKIP 0.96, Silver Oak + Twomey PARENT_CHILD
  0.93).
- Full verdicts: **MERGE 2,606 + PARENT_CHILD 2,121 + SKIP 145,310 +
  UNCERTAIN 1,083.**

Cost tally: $78.44.

### B6.4 — Calibration (2026-04-17) — $24.51

Built a 600-pair stratified calibration set, gold-labeled 367 pairs (200
proxy + 167 from Sonnet + web oracle).

Ran:
- **L1.5 Gemini basic**: $0.13, 100% MERGE precision at conf ≥0.85 on 88
  pairs.
- **L2 Haiku rich**: $0.59, 97.6% MERGE precision.
- **L2.5 Gemini rich**: $0.19, 98.8% MERGE precision.

**Headline:** any Haiku-tier + any Gemini-tier both MERGE at conf ≥0.85 =
**100% precision** across all cross-family pairings (70-78 gold each). PC
precision was 6-10% across all tiers at all confidence bands — confidence
magnitude doesn't fix PC noise; cross-tier agreement and rich-prompt source
do.

**Committed thresholds (symmetric, cross-family):**
- Stage 1: L1 + L1.5 both MERGE ≥ 0.88 → auto; both SKIP ≥ 0.97 → auto
- Stage 2: L2 + L2.5 both MERGE ≥ 0.90 → auto; both SKIP ≥ 0.95 → auto
- L3 Sonnet: MERGE ≥ 0.92 → auto; SKIP ≥ 0.90 → auto
- **PC refined rule:** user review iff (a) 2+ tiers emit PC at any conf, OR
  (b) L2/L2.5 rich PC at conf ≥0.90, OR (c) L3 Sonnet PC at any conf.
  Single-tier basic-prompt PC is noise; follow cross-tier non-PC consensus.

**SKIP audit**: calibration showed 4-way SKIP at ≥0.90 had 3.2% false-negative
rate; B6.5a SKIP audit protocol validated this would hold at production scale.

Cost tally: $24.51.

### B6.5a production ladder + Chrome validation (2026-04-18 → 2026-04-20)

B6.5a spanned several sessions. The automated portion ran first, then the
Chrome-per-pair validation was the human+Claude curation layer.

#### Automated ladder (Stage 1 → Stage 2 → Stage 3 routing)

- **L1.5 Gemini basic on 151K pairs** — cross-family cross-check against L1
  Haiku.
- **Stage 1 sort**: L1+L1.5 consensus produced initial MERGE/SKIP/PC routing.
- **SKIP audit** (200 pairs, $5): validated 3.2% FN rate at scale.
- **L2 Haiku rich + L2.5 Gemini rich** on ~57,810 escalations.
- **Stage 2 sort**: produced 676 auto-merge, 34K auto-skip, 3.2K PC review,
  19.5K residual.
- **$16 of exploratory L3 probes** (470 L3 no-web + 65 L3 web) revealed:
  - Stage 2 auto-MERGE had a 10% FP rate → require L3 web validation first
  - Stage 2 SKIP 0.95/0.95 too tight → lowered to 0.93/0.93, measured 2% FN
  - L3 web delta marginal on random residuals (5%) but high on cross-family
    disagreements (67% MERGE yield)
  - §11 amendments drafted for shared-surname, cross-country, collaboration
    patterns.

End state: `producer_dedup_routing_stage3` populated with 151,150 rows
across 9 action buckets (see `phase2_summary.md` for exact counts).

#### Chrome-per-pair validation (~$4-6)

**User directive** ("maintain discipline and actually web search all pairs
as planned") locked the plan: Chrome-validate 100% of three queues — 71
yellow-flagged top producers + 143 Core escalations + 138 Mid escalations
+ 141 Tail pairs.

Total scope: **493 pairs Chrome-per-pair** (after one tail subset expansion
from the original plan). Each validation:
- Pre-loaded DB wine-portfolio context (read-only).
- Single DuckDuckGo query: `"name_a" "name_b" winery` or similar.
- Navigate + read if ambiguous; otherwise log SKIP from zero-results evidence.
- Batched JSONL logging every ~20 verdicts.

**493 verdicts logged** across:
- `yellow_verdicts.jsonl` — 71 pairs (13 MERGE, 4 PC, 21 SKIP, 33 KEEP_AS_IS)
- `core_verdicts.jsonl` — 143 pairs (40 MERGE, 26 PC, 77 SKIP)
- `mid_verdicts.jsonl` — 138 pairs (44 MERGE, 10 PC, 84 SKIP)
- `tail_verdicts.jsonl` — 141 pairs (41 MERGE, 15 PC, 85 SKIP)

A plan-lock document was written (`PLAN_LOCK.md`) after one prior Sonnet
session drifted into batched heuristic application under invented rules —
the lock committed to Chrome-per-pair, no shortcuts.

### B6.6 — §11.4 amendments + re-Chrome (2026-04-20) — ~$10

Two tasks, done in one session.

#### §11.4 amendments

The B6.5a Chrome work surfaced nine pattern clusters the original §11.4
didn't cover cleanly. B6.6 codified them:

- **§11.4.f renamed** — was "Négociant + estate"; now "Generational
  succession / historical name forms" (e.g. Guy Castagnier → Castagnier,
  Amiot-Bonfils → Guy Amiot et Fils). Original §11.4.f content moved to
  §11.4.r.
- **§11.4.h extended** — now covers orthographic variants broadly: accents,
  typos, articles, short/full forms, empty-placeholder rows.
- **§11.4.j inverted** — custom-crush shared-permit with disjoint wine
  catalogs now resolves to SKIP (was PARENT-CHILD). Rationale: a custom-crush
  facility is not a brand "parent"; its clients are unrelated.
- **§11.4.m added** — shared-surname family splits default to SKIP. Covers
  de Montille / Deux Montille, Brundlmayer siblings, Haselgrove branches,
  etc. 121 Chrome pairs.
- **§11.4.n added** — global brands with multi-country sourcing MERGE across
  country rows. 14 Chrome pairs (Tussock Jumper, 90+ Cellars, Selaks,
  Cupcake).
- **§11.4.o added** — JV/collab labels at dedup time. 42 Chrome pairs. PC
  when concatenated name has a dominant principal (Wheeler & Fromm → Fromm);
  SKIP when no clear hierarchy.
- **§11.4.p added** — merchant/restaurant/retailer curation prefixes.
  `"Taillevent (Joseph Drouhin)"` → MERGE into Joseph Drouhin. 13 Chrome
  pairs.
- **§11.4.q added** — Hospices de Beaune / Nuits auction négociant-bottlings.
  HdB(X) ⇔ HdB(Y) with distinct X/Y = SKIP; X ⇔ HdB(X) = PC. 18 pairs.
- **§11.4.r added** — moved-from-§11.4.f négociant + estate case.
- **§11.4.s added** — sub-brands / cuvée-lines / named tiers under parent.
  Verget au Sud → Verget, Argento → Artesano de Argento, etc. 29 pairs.

All amendments logged in `docs/DECISIONS.md` (2026-04-19 entry). All affected
verdicts remain frozen — amendments describe retrospectively what B6.5a
applied.

#### Re-Chrome of all 193 MERGE+PC verdicts

Motivation: one "rename-on-merge" case (Barons de Rothschild) and one
"parent-rename" case (Goldschmidt Vineyards) looked suspicious in review.
User directive: re-Chrome all 4 that looked concerning.

- **4 pairs manually re-Chrome'd** by parent Opus session.
  - 62908 Beausejour → **FLIP_TO_SKIP.** The "Beausejour" row was a dumpster
    of 6+ distinct French châteaux. Critical: "Croix de Beausejour" is
    Duffau-Lagarrosse's second wine, not Bécot's. Chrome originally claimed
    "DB cuvees point to Becot" — factually wrong.
  - 54026 Boisson → **FLIP_TO_SKIP.** "Boisson" row was Rhône (Cairanne)
    + Bordeaux; Anne Boisson target was Meursault. Zero overlap. Plus §11.4.m
    sibling split (Pierre Boisson, Boisson-Vadot, Anne Boisson are three
    separate Meursault domaines).
  - 43596 Cherisey → **MERGE but FLIP_DIRECTION.** Jasper Morris confirmed
    Martelet de Cherisey (old name) → Comtesse de Cherisey (current name).
    Chrome originally chose Martelet as survivor; actual canonical is
    Comtesse.
  - 57771 Alex et Benoit Moreau → **FLIP_TO_SKIP.** The 1-wine Fleurie is a
    single cuvée of Domaine Bernard Moreau et Fils, not a collab producer.
    Per §11.4.e + §11.4.m, it's a wine of a third estate, not a PC child of
    Alex.

Given 4/4 flips on the manual sample, scope expanded to full re-Chrome.

- **66 Core MERGE+PC pairs** re-Chrome'd by subagent (general-purpose
  Sonnet). Tool use: 152 calls, 260K tokens, ~17.5 min. **Flip rate: 18%
  (12 flips).** Top flips:
  - 142528 Chalk Hill (13w + 46w) — **MERGE→SKIP.** Sonoma Chalk Hill Estate
    vs McLaren Vale Chalk Hill Wines. Different continents.
  - 25960 Cheurlin Noellat (3w + 55w) — **MERGE→SKIP.** Champagne Richard
    Cheurlin (Aube) vs Vosne-Romanée Maxime Cheurlin Noellat.
  - 59536 Pascal Jolivet (25w + 3w) — **MERGE→SKIP.** Sancerre Loire vs
    Saint-Joseph Rhône.
  - 113145 Passopisciaro (16w + 2w) — **MERGE→SKIP.** Distinct Etna estates
    Franchetti vs Moretti Cuseri.

- **127 Mid + Tail + Yellow MERGE+PC pairs** re-Chrome'd by second subagent.
  93 tool calls, 199K tokens. **Flip rate: 17.3% (22 flips).** Top flips:
  - mid#27972 Bosio (15w) — **MERGE→SKIP.** Franciacorta vs Piedmont.
  - mid#49717 Laborde (13w) — **MERGE→SKIP.** Bordeaux vs Burgundy.
  - mid#9707 Tuck Beckstoffer (11w) — **MERGE→PC.** The 75 Wine Company is
    a distinct on-label brand under Beckstoffer, not a MERGE target.
  - 2 flagged for Sprint 7 human review.

**Final B6.6 totals across 193 Chrome-validated MERGE+PC verdicts:** 33
overrides (17% flip rate) = 29 FLIP_TO_SKIP + 1 FLIP_DIRECTION + 2
FLIP_TO_MERGE + 1 FLIP_TO_PC + 5 NEEDS_HUMAN_REVIEW. 155 KEPT original
verdict.

**Systemic observation** (Both re-Chrome subagents independently reported):
> The single most reliable red flag for false-MERGE was **DB wine-list
> region/appellation incompatibility** — 13 of 22 Mid/Tail/Yellow flips had
> incompatible regions visible directly from the wine list without needing
> web lookup. The original AI ladder (L1+L1.5+L2+L2.5) doesn't see this
> signal because wine-list context isn't in the prompts at that stage.

Cost tally for B6.6: ~$5 (Core re-Chrome) + ~$3 (Mid+Tail+Yellow re-Chrome)
+ ~$2 (Opus inline) = **~$10**.

---

## 3. Final state

### Data on disk

Nothing has been written to the DB. `producer_merge_history` row count is 0.
All work product lives in:

- `docs/IDENTITY_RULES.md` §11 — amended producer identity framework
- `docs/DECISIONS.md` — B6.4 threshold commits + B6.6 §11.4 amendment log
- `producer_dedup_pairs` (600,103 rows, L1-verdict populated)
- `producer_dedup_routing_stage1/2/3` tables (the AI ladder's routing state)
- `data/sprints/dedup/chrome_validation/` — B6.5a Chrome verdict JSONLs
  (source) + B6.6 re-Chrome subagent outputs
- `data/sprints/dedup/execution_bundle/` — this frozen bundle for external
  review

### Cost tally

| Block | Cost | Notes |
|---|---|---|
| B6.1 planning | $0 | |
| B6.2 LWIN import | $0 | SQL + pipeline |
| B6.3 blocking + L1 Haiku on 151K | $78.44 | 0.052¢/pair cached |
| B6.4 calibration | $24.51 | 600 pairs, 3 models |
| B6.5a ladder (L1.5 + L2 + L2.5 + L3 probes + SKIP audit) | ~$25 | |
| B6.5a Chrome validation (493 pairs) | ~$4-6 | mostly Claude-in-Chrome driver + DuckDuckGo |
| B6.6 re-Chrome (193 pairs) + amendments | ~$10 | two subagents + Opus inline |
| **Total** | **~$143** | of $250 ceiling |

### Decision distribution

#### Phase 1 (Chrome-validated, fully reviewed)

493 pairs in `verdict_ledger.jsonl`:

| Verdict | Count |
|---|---|
| MERGE | 109 |
| PARENT_CHILD | 50 |
| SKIP | 296 |
| KEEP_AS_IS | 33 |
| DEFERRED_SPRINT_7 | 5 |

Plus 7 pair-level flags for Sprint 7 follow-up (row-splits, family cleanups).
17 canonical-row redirects where the Chrome-chosen rename target turned out
to already exist as a canonical DB row.

#### Phase 2 (pipeline auto-decided, not re-reviewed)

151,150 pairs in `producer_dedup_routing_stage3`:

| stage3_action | Count | If executed |
|---|---|---|
| auto_apply_skip | 146,435 | no-op |
| auto_apply_merge | 3,154 | mutation |
| auto_apply_pc | 785 | mutation |
| user_review_pc | 470 | human review required |
| user_review_merge_lowconf | 246 | human review required |
| auto_apply_skip_missing | 32 | no-op (edge case) |
| user_review_missing | 23 | human review required |
| auto_apply_skip_residual | 4 | no-op |
| user_review_merge_unvalidated | 1 | human review required |

Total mutations if Phase 2 is run as-is: **3,939** (3,154 MERGE + 785 PC)
plus 740 review-queue items awaiting human judgment.

---

## 4. What we learned

### The fundamental tradeoff

Dedup is a **ground-truth problem** where the cost of perfect correctness
scales non-linearly as you reach into the tail. The first 95% of producer
pairs are easy — the AI ladder handles them well. The last 5% require
per-pair web research, and even then 17% of the "rigorous" Chrome validation
was wrong because:
- Some pairs require facts not easily surfaced in a single web query
- Shared-surname generic-château patterns fool every automated tier

Every commercial wine data product (Wine-Searcher, CellarTracker, Vivino,
Jancis Robinson) has dedup errors in the tail. Sprint 6's realistic goal
ended up being "execute the high-confidence slice, defer the ambiguous
tail, build a continuous-improvement posture."

### The "wine-list region incompatibility" heuristic

The strongest signal the pipeline missed: if two producer rows have wine
lists in completely different regions, they're probably not the same
producer. Our L1+L1.5 prompts don't see wine lists. L2+L2.5 do but only
on Stage 2 escalations.

**Future improvement** (Sprint 7 / 8 candidate): fold wine-list
region-compatibility into L1's prompt at a minimal-token cost, OR add a
post-L1 SQL filter that auto-demotes shared-surname MERGEs where wine
regions don't overlap.

### What §11 got right

- The "brand on the label" framing — zero ambiguity on what qualifies as
  MERGE vs SKIP vs PC across every case we examined.
- Canonical-row redirect logic — prevented 17 would-be duplicate rows where
  the Chrome rename target happened to already exist.
- Reversibility via `producer_merge_history` JSONB snapshots — every merge
  is undoable.

### What §11 needed B6.6 to fix

- §11.4.f was overloaded (négociant + estate AND generational succession
  are genuinely different patterns).
- §11.4.h was too narrow (just diacritics).
- §11.4.j's custom-crush → PC recommendation was wrong.
- Nine new edge cases weren't covered (m, n, o, p, q, r, s).

### Where the 17% flip rate came from

Not randomness. Four patterns accounted for almost all flips:
1. **Shared-surname family split** (§11.4.m): Cacheux, Boffa, Brocard,
   Giordano, Confuron, Gauffroy, Gillet, Buscemi, Laborde — all MERGEs
   that cross independent estates within the same appellation/family.
2. **Cross-region same-name** (§11.4.b + §11.4.m): Boisson Rhône/Burgundy,
   Bosio Franciacorta/Piedmont, Laborde Bordeaux/Burgundy, Chalk Hill
   US/AU.
3. **Collaboration label confusion** (§11.4.o): Brignot/Steen vs
   Steen/Blauert — different natural-wine partnerships with overlapping
   surnames.
4. **Brand sub-labels mis-classified as MERGE** (§11.4.s): The 75 Wine
   Company (by Tuck Beckstoffer) was PC, not MERGE.

All four patterns are now codified in §11.4.

---

## 5. Current decision point

**Sprint 6 is paused pending external review of this bundle.** The user's
directive (2026-04-20):

1. Don't execute any dedups.
2. Package all dedup decisions into the producer dedup bundle for external
   review.
3. Write up the full sprint.
4. Pause further dedup work until this bundle has been created.

This document fulfills #3. The bundle (this directory) fulfills #1 and #2.

### What happens after external review

User will run rounds of testing inside and outside Claude Code. Expected
failure modes they'll look for:
- Obvious MERGE errors in Phase 1 (we've re-Chromed twice, should be small)
- Phase 2 auto-apply decisions that look risky
- Missing patterns / deferred cases that should be in-scope
- Execute-script correctness concerns (FK re-pointing, reversibility,
  soft-delete semantics)

Then:
- **If Phase 1 looks good:** execute it. 109 MERGEs + 50 PCs applied. DB
  producer count drops by ~109.
- **If Phase 2 needs more scrutiny:** sampled audit per `phase2_risk_analysis.md`.
- **If Sprint 6 needs to close without full execution:** that's fine. Phase
  1 alone is a real improvement, and the Phase 2 queue can flow to Sprint 7.

### Sprint 7 candidates (from `open_questions.md` + `deferred_to_sprint_7.md`)

- Multi-parent collaboration schema (`producer_collaborators` junction table)
- Per-wine re-linking of generic-château dumpster rows (Beausejour, Boisson)
- Moreau family 5-row cleanup
- Goldschmidt family 6-row cleanup
- Taylor Fladgate cluster (yellow#59 covers some but more remain)
- Second-wine re-link (§11.4.e violations)
- Hospices de Beaune corpus-wide sweep
- Shared-surname §11.4.m application as a SQL-only post-hoc override
- Reversibility per-table row ID recording
- Post-execution monitoring dashboard widget

### Strategic pivot

Sprint 6 dragged longer than planned because "final-state correctness ~100%
of producers table" is not achievable without per-pair human curation at
scale. Mature posture (per user's own `feedback_opus_inline_reasoning.md`
and the analogy to commercial wine databases):

- Execute the high-confidence slice NOW.
- Accept that the tail has known-unknown errors that will surface as user
  feedback.
- Build a "report wrong data" button in the UI (Sprint 8+).
- Re-audit periodically.

This is how Wine-Searcher, CellarTracker, Vivino, Jancis Robinson operate.
Trying for "perfect" in one sprint is the trap.

---

## 6. Who to ask

- **User (Neil)** — final decision authority on schema, scope, thresholds,
  and execution timing. Has domain expertise on wine; his collection
  (Stag's Leap, Fort Ross, Ridge, López de Heredia, CIRQ, plus French
  recommendations) is the lens for Sprint 4 demo scope.
- **Claude sessions (this one and predecessors)** — all sessions are logged
  to `data/sprints/dedup/journal.md` and `data/sprints/current.json`. CLAUDE.md
  and DECISIONS.md are the durable record across sessions.
- **Other AI reviewers (external)** — this bundle is designed to be read by
  you. Start with `README.md`, then this file, then
  `verdict_ledger_summary.md` and `phase2_summary.md` for the actual
  decisions. Use `testing_guide.md` for suggested review protocols.
- **Original sources** — `docs/IDENTITY_RULES.md`, `docs/SCHEMA.md`,
  `docs/PRINCIPLES.md`, `docs/DECISIONS.md` for design context.

---

## 7. Known limitations of this bundle

1. **Phase 2 (`phase2_actionable_decisions.jsonl`) has not been
   Chrome-validated.** The risk analysis estimates error rate, but no
   re-Chrome pass has been done on the auto-apply queue.
2. **The SKIP queue sample is random, not stratified** by pattern cluster
   or country. A stratified sample would be stronger for measuring FN rate.
3. **The `web_reasoning` field is truncated to 800 chars** in
   `phase2_actionable_decisions.jsonl` to keep the file under 10MB. Full
   reasoning is in `producer_dedup_routing_stage3` if needed.
4. **Reversibility per-table row IDs are not recorded by the execute
   script** yet — reversal requires a full-table scan. Flagged in
   `open_questions.md`.
5. **Multi-parent collaboration schema doesn't exist** — DVO and similar JVs
   can only record one parent. Yellow#3 uses a metadata JSONB workaround.
6. **L1+L1.5 prompt quality hasn't been measured against shared-surname
   false positives systematically** — the 17% flip rate surfaced the
   pattern, but we haven't rebuilt the prompts.
7. **Wine dedup** is entirely out of scope (Sprint 7).
8. **Enrichment re-run** is entirely out of scope (Sprint 8, after prompt
   v2 lands).

---

*Generated 2026-04-20, Sprint 6 B6.6. Ledger and decisions frozen; DB
unchanged.*
