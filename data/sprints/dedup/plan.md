# Sprint 6: Producer Dedup — Evaluation + Execution (LWIN-first)

**Status:** Active. Plan locked by B6.1 (2026-04-16).
**Opened:** 2026-04-16
**Sprint number:** 6
**Shape:** Single sprint covering LWIN import + evaluation + execution. No merges applied until evaluation gate clears in-sprint.

---

## Goal (user directive + B6.1 resolution)

> Dedup all 10,683 producers. Have a good method for deduping producers added
> in the future. Throw the kitchen sink at this — use AI in new and creative
> ways. Target: close to 100% final-state correctness.

**B6.1 interpretation:** "Close to 100%" = **final-state correctness of the
producers table** after Sprint 6 closes. Achieved via:

1. Pipeline auto-applies merges only where L1+L2+L3 agree MERGE at confidence > 0.85 → estimated ~0 FPR on applied set
2. User reviews 50–150 curated "toughest pairs" with Claude-presented recommendations + evidence + `flag-for-later` option
3. UNCERTAIN pairs flagged as known-open — NOT auto-merged
4. User sign-off required before any execution

**Prerequisite discovered in B6.1:** ~24,762 LWIN producers sit unlinked in `source_lwin` (69,444 wine rows). Prior import (lwin_long_tail.py, Session 13) only covered US + intl≥8 wines via exact normalized name matching. Running Sprint 6 dedup without finishing the LWIN import means deduping on an incomplete universe. Fix: B6.2 is a dedicated LWIN import block that runs BEFORE dedup begins.

---

## Sprint sequence

```
Sprint 6 (this):   LWIN import + producer dedup (evaluation + execution)
Sprint 7 (later):  Wine dedup (~4,079 suspected + 30-35 dangerous FP patterns)
Sprint 8 (later):  Prompt v2 + L3 fact-check gate + re-enrichment + sharing
```

Re-enrichment deferred so we don't lock in the current-prompt ceiling.

---

## Block cadence

```
B6.1 (done)   Planning & design                              $0
B6.2          LWIN producer import (24K+ merged in)          ~$0
B6.3          Schema + IDENTITY_RULES (producer section) +
              blocking dry-run + L1 Haiku batched            $60-130
B6.4          L2 Haiku batched + L3 Sonnet web-grounded +
              anchor set + ablation tests                    $40-95
B6.5          L4 Opus-inline audit + toughest-pairs review   $0-10
B6.6          Execution (auto-apply + reviewed pairs +
              producer_merge_history)                        $0
B6.7+         Iterate if quality gate unmet                  reserve $30-60
B6.N          Close + evaluation.md + handoff to Sprint 7    $0
```

**Budget: $100-220 projected, $250 ceiling.** Cost not a constraint; quality is.

---

## B6.2 — LWIN Producer Import

### Goal

Close the LWIN import gap before Sprint 6 dedup starts. After B6.2, every `source_lwin` row has `canonical_producer_id` set and every distinct LWIN producer either links to an existing canonical row or has its own new canonical row.

### Method

**Simple matching, same as prior `lwin_long_tail.py` approach** (per B6.1 user decision — "same as prior method that way the dedup has a similar starting point"):

```
For each distinct LWIN (producer_name, country) combination:
  normalize producer_name (lowercase, strip accents, collapse spaces)
  resolve country → country_id
  look up producers WHERE name_normalized = <norm> AND country_id = <cid> AND deleted_at IS NULL
  if match → link (update source_lwin.canonical_producer_id for all rows)
  if no match → create new producer row (name, name_normalized, slug, country_id, region_id)
```

No AI calls during import. No fuzzy matching. No LLM judgment. Intentional — Sprint 6's dedup cascade (B6.3-B6.6) handles the nuanced matching. Creating some dupes at import is fine because dedup will find them.

### Differences from prior run

Prior `lwin_long_tail.py` only processed:
- All US producers (minus junk filter)
- International producers with ≥8 LWIN wines

B6.2 extends to include the long tail:
- **All distinct (producer_name, country) combinations** with `processed_at IS NULL`
- No wine-count threshold
- No country restriction

Expected delta: ~24,762 more producers processed, ~69,444 more source_lwin rows linked.

### Pre-B6.2 decision: junk filter

`pipeline/promote/lwin_long_tail.py` has a junk-filter option reading `data/stats/lwin_us_junk_classifications.json`. Default is DISABLED (inclusive mode). **Keep disabled for B6.2** — we prefer importing a possibly-junk producer (which dedup will catch) over rejecting a real producer.

### Expected outcome

- `producers` table grows from 10,683 to ~25,000-35,000 rows
- `source_lwin.canonical_producer_id` populated for all ~189K rows
- `external_ids` gains LWIN_7 codes for each LWIN wine linked (already partially populated — 170K rows)
- Some dupes created (same-entity under different spellings) — Sprint 6 dedup catches them

### Acceptance gate

Before moving to B6.3:
- `SELECT COUNT(DISTINCT producer_name) FROM source_lwin WHERE canonical_producer_id IS NULL` returns 0 or near-0
- `producers` table count is between 20,000 and 40,000 (sanity check — if way outside this range, something went wrong)
- Spot-check 10 newly-created producer rows for sanity

---

## B6.3 — Schema, IDENTITY_RULES, Blocking, L1

### Schema changes

```sql
-- Extend existing scaffolded table
ALTER TABLE producer_dedup_pairs
  ADD COLUMN producer_id_a uuid REFERENCES producers(id),
  ADD COLUMN producer_id_b uuid REFERENCES producers(id),
  ADD COLUMN method_name text,
  ADD COLUMN confidence numeric,
  ADD COLUMN reasoning text,
  ADD COLUMN cost_cents numeric,
  ADD COLUMN signals jsonb,
  ADD COLUMN ttb_evidence jsonb,
  ADD COLUMN web_evidence jsonb,
  ADD COLUMN flag_reason text,
  ADD COLUMN created_at timestamptz DEFAULT now();

CREATE UNIQUE INDEX producer_dedup_pairs_pair_method_uq
  ON producer_dedup_pairs (producer_id_a, producer_id_b, method_name);

-- Reversibility
CREATE TABLE producer_merge_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  merged_producer_id uuid NOT NULL,
  survivor_producer_id uuid NOT NULL REFERENCES producers(id),
  merged_producer_json jsonb NOT NULL,
  repointed_rows jsonb NOT NULL,
  match_decision_id uuid REFERENCES match_decisions(id),
  merged_at timestamptz NOT NULL DEFAULT now(),
  reversed_at timestamptz,
  reversed_by text,
  reversal_notes text
);
```

Parent-child via existing `producers.parent_producer_id` (already in schema, currently unused). Dedicated `producer_relationships` table deferred post-Sprint-6.

Verdict values in `producer_dedup_pairs.verdict`: `MERGE`, `PARENT_CHILD`, `SKIP`, `UNCERTAIN`, `FLAGGED` (for "punt on this pair, review later").

### IDENTITY_RULES.md extensions

`docs/IDENTITY_RULES.md` already exists (Session 2, 2026-04-08) — covers wine identity + display names + cuvée extraction. B6.3 **appends a new Section 11: Producer Identity Rules** covering:

- Producer definition = **brand on label**
- MERGE / PARENT-CHILD / SKIP operational test ladder
- Edge cases (renames, dissolved+reopened, private-labels, retailers-never-producers, accents, second-wines, négociant+estate, joint ventures, sold estates)
- Survivor-selection rules (accent preservation, label-form preference, prefer more complete metadata)
- When to `flag-for-later` vs assign a verdict

**Process:** Claude drafts Section 11 from B6.1 decisions. User reviews at end of B6.3 before L1 Haiku runs. Section 11 gets embedded verbatim in every L1/L2/L3 prompt.

### Blocking — union of 9 strategies (lever 4: blocking first, see actuals)

Each strategy is a SQL query producing candidate pairs. Every pair in the UNION becomes a candidate for the AI ladder.

| # | Strategy | Catches | Est. volume (post-LWIN) |
|---|---|---|---|
| 1 | Same country + exact normalized name | Trivial dupes, case/spacing | ~1K-3K |
| 2 | Same country + trigram ≥ 0.3 | Typos, abbreviations | 50K-120K |
| 3 | Same country + embedding cosine ≥ 0.5 | Translations, semantic variants | 25K-70K |
| 4 | Same country + first-3-char + token overlap | Word-reordering variants | 12K-35K |
| 5 | Shared external_id (LWIN_7, website host) | External-system-confirmed | 2K-5K |
| 6 | Shared TTB permittee_basic_permit | Same US federal permit | 1K-5K |
| 7 | Cross-country w/ strong signal (shared LWIN OR ≥5 matching wines) | International dupes | <1K |
| 8 | Wine-catalog overlap ≥ 30% shared named wines | Import-source duplicates | 5K-15K |
| 9 | Producer-name substring containment | "X" ⊂ "X et Fils" | 8K-20K |

**Expected union size: 100K-250K pairs (post-LWIN).**

**Lever 4 — run blocking first, report actuals:** B6.3's first step runs only the blocking queries (no LLM). Reports:
- Total unique pairs in union
- Per-strategy contribution + unique catches
- Anchor-set recall (does union contain all 15+ known positives?)
- Estimated L1 cost at measured pair count

If actual count is above 250K, tighten thresholds (raise trigram to 0.35, embedding to 0.6) and re-run. If below 150K, proceed immediately.

### TTB data injected into every prompt (US pairs)

`source_ttb_colas.permittee_basic_permit` is federally unique per entity. Pre-compute TTB fingerprint per producer (permittee_basic_permit, permittee_name, permittee_address, distinct brand_names, COLA count). Inject verbatim into every LLM prompt for pairs where at least one producer has TTB data.

### L1 — Haiku 4.5 with cached prefix + batching (lever 3)

- Model: Haiku 4.5 via Anthropic SDK direct (no OpenRouter)
- Prompt caching: ~2K token static prefix (IDENTITY_RULES Section 11 + schema + few-shot) cached with 1-hour TTL
- **Batching: 10 pairs per call** — amortizes per-call overhead, maximizes cache hits
- Inputs per pair: producer names, countries, wine counts, TTB fingerprints, signal metadata (which blocking rules caught it), sample wine names
- Output: JSON array of `{pair_id, verdict, confidence, reasoning}`

**Gate 5 (pre-commit to full run):** L1 pilot on 200 pairs (anchor + random). Inspect verdict distribution, actual cost per pair, sample reasoning. Extrapolate full-corpus cost.

---

## B6.4 — L2, L3, Anchor Set, Ablation Tests

### Anchor set (~50 hand-labeled pairs, built at start of B6.4)

- **Tier 1 — S4.1 re-verification (3 pairs):** Ridge, López de Heredia, CIRQ — previously merged, NOT assumed correct. Re-verified in B6.4 using pipeline's own signals.
- **Tier 2 — Demo producers (~14):** Stag's Leap WC, Fort Ross, Tempier, Guigal, Trimbach, Huet, DRC, Krug, Conterno, Château Margaux, Château Latour, plus S4.1 three.
- **Tier 3 — Stress-test cases (~15):** abbreviation, translation, parent-child, accent, private-label, rename, importer-prefix, retailer-as-producer, commune overlap, region-collision, accent-preservation.
- **Tier 4 — Stratified random (~20):** pulled from L1 output, 5 per US/FR/IT/ES-other.

Total: ~50 pairs, hand-labeled by user (aided by Claude's research).

### L2 — Haiku 4.5 with rich prompt + batching (lever 3)

- Model: Haiku 4.5 direct Anthropic
- Richer prompt than L1: full TTB fingerprint, full wine list (not just samples), producer metadata, IDENTITY_RULES Section 11
- **Batching: 5 pairs per call** (smaller because prompt is richer)
- Input: pairs where L1 voted UNCERTAIN or where L1 MERGE confidence was <0.85
- Output: JSON array of verdicts

### L3 — Sonnet 4.6 with Anthropic native web_search tool

- Model: Sonnet 4.6 direct Anthropic
- **Web search tool: Anthropic native `web_search_20250305`** ($10/1K searches, more reliable than OpenRouter :online)
- L3 runs on pairs where L2 voted MERGE or UNCERTAIN (the rigor tier)
- Every L3 pair gets a web-grounded verdict — not batched (each pair needs its own search)
- Web search prompts model to look up producer websites, Wikipedia, Wine-Searcher, LWIN records
- Output: `{verdict, confidence, reasoning, web_evidence: [{url, snippet, used_for}]}`

### Gate 6 — Ablation tests (critical, $2-3, 30 min)

Before scaling L2 and L3 to full volume:

1. **L2 with/without web search on 50 anchor pairs** — does Haiku + tool-use web catch cases Haiku alone misses?
2. **L3 with/without web search on 50 anchor pairs** — does web add value on the rigor tier?
3. **Cache hit rate** — verify prompt caching actually fires at L1/L2 (<70% = debug before full run)

Decision rules:
- If L2 web delta < 3 points: drop web from L2
- If L3 web delta < 2 points: drop web from L3
- If both fail: revert to no-web ladder (saves $30-60)
- If both help: scale up

### Cross-method agreement matrix

After L1+L2+L3 all run, produce agreement matrix: for each pair, how do verdicts compare across methods? This drives auto-apply:

- **Auto-apply set:** L1 + L2 + L3 all vote MERGE, all confidence > 0.85 → goes straight to B6.6
- **Toughest pairs set:** methods disagree, confidence mid-range, policy edge case, or wine count > 20

---

## B6.5 — L4 Audit + Toughest Pairs Review

### L4 — Opus-inline-1M audit (runs in-session, ~$0 project cost)

Opus-inline-1M reads the full pair-level output (up to ~200K pairs with verdicts + reasoning) in a single context window. Cross-pair pattern recognition tasks:

- Find any MERGE in auto-apply set that looks wrong in aggregate
- Flag pairs the pipeline handled inconsistently (e.g. "these 3 pairs have the same shape but got 2 MERGE + 1 SKIP")
- Identify IDENTITY_RULES gaps surfaced by edge cases
- Produce a "find these suspicious patterns" list for toughest-pairs review

Per `memory/feedback_opus_inline_reasoning.md`: cross-record pattern recognition is exactly what Opus inline does that scripted per-pair Haiku/Sonnet can't.

### Toughest-pairs curation + review

Claude curates 50-150 pairs for user review:
- AI methods disagree
- Mid-range confidence (0.5-0.85)
- Policy edge case (parent-child, rename, private-label, accent variant)
- Wine count > 20 (high-stakes)
- L4 Opus-inline flagged anything suspicious

**Review presentation format (upgrade 1 — context pack per pair):**

For each pair:
- Both producer records side-by-side (names, countries, wine counts, TTB fingerprints)
- Web-grounded evidence gathered by L3 (URLs, snippets, source attribution)
- Each method's verdict + confidence + reasoning
- Claude's own recommendation (MERGE / PARENT-CHILD / SKIP / FLAG) with "why"
- User's decision options: accept, override, flag-for-later

**Review batching by pattern (upgrade 2):** pairs grouped by policy type, not random order:
- Batch A: parent-child candidates
- Batch B: rename / historical-name candidates
- Batch C: private-label questions
- Batch D: commune / region coincidence cases
- Batch E: everything else

**Decision log (upgrade 3):** every review call captured with 1-line reason in `producer_dedup_pairs.reasoning`. Feeds future IDENTITY_RULES amendments + few-shot examples.

**Flag-for-later (upgrade 4 — approach A + C):**
- Verdict = `'FLAGGED'` with `flag_reason` text column (approach A)
- Pattern-level observations captured in `data/sprints/dedup/open_questions.md` by hand (approach C)

---

## B6.6 — Execution

### Gate 7 — Merge execution dry-run on Supabase branch (free, 20 min)

Before applying production merges:
1. Create Supabase dev branch
2. Apply 3 test merges via `apply_merge()` on the branch
3. Verify: `producer_merge_history` row captured correctly, wines re-pointed, aliases populated, `reverse_merge()` works, no orphans
4. Roll back branch
5. Only then apply in production

### Execution steps

1. Auto-apply unanimous-MERGE set (L1+L2+L3 agreement, confidence > 0.85)
2. Apply user-signed-off pairs from B6.5 review
3. Every merge writes a `producer_merge_history` row (full JSON snapshot + repointed rows list)
4. Old names written to `producer_aliases` with `alias_type = 'merged_from'` or `'historical_name'`
5. `parent_producer_id` populated for PARENT_CHILD verdicts
6. Staging tables repointed via bulk UPDATE (same pattern as `wine_merge.py`)

### Safety nets (both per B6.1 user dialog)

**Safety net A — Unblocked spot-check (cost ~$1, runs in B6.4):** 200 random pairs NOT in the candidate list run through Haiku to catch blocking misses. If any come back MERGE → add blocking rule or loosen threshold.

**Safety net B — Post-execution leftover scan (cost $5-10, runs in B6.N):** lightweight sweep comparing remaining producers by wine-catalog signature. Catches any dupes the pipeline missed.

---

## Review upgrades (per B6.1 user dialog)

1. **Prefetched context pack per pair** ✓ — website/Wikipedia/LWIN/TTB/sample wines included in every review presentation
2. **Batched by pattern, not random** ✓ — parent-child/rename/private-label batches
3. **Decision log** ✓ — one-line reason per call, feeds future rule updates
4. **Flag-for-later (A + C)** ✓ — `'FLAGGED'` verdict + open_questions.md pattern log
5. **Calibration exercise before B6.3** ✓ — 10-15 sample pairs walked through with user before full cascade

---

## Cost-reduction levers applied

| Lever | Decision | Savings |
|---|---|---|
| 1 — Auto-apply exact matches without LLM | **OFF** (user: risk of commune-overlap misses) | — |
| 2 — Opus-inline pre-filter L3 | **OFF** (user: undermines rigor tier) | — |
| 3 — Batching at L1 (10/call) and L2 (5/call) | **ON** | $20-35 |
| 4 — Run blocking first, see actuals | **ON** | variable |

L3 web search: Anthropic native `web_search_20250305` tool (not OpenRouter :online — more reliable, first-party).

---

## Budget

**Projected: $100-220.** **Ceiling: $250.**

| Phase | Projection |
|---|---:|
| B6.1 planning | $0 (done) |
| B6.2 LWIN import | ~$0 |
| B6.3 schema + blocking + L1 (batched) | $60-130 |
| B6.4 L2 + L3 + anchor + ablation | $40-95 |
| B6.5 L4 + review | $0-10 |
| B6.6 execution | $0 |
| B6.7+ iteration reserve | $30-60 |

Per B6.1 user framing: cost is not the constraint; quality is. If iteration pushes past $250, we raise the ceiling in that session's prompt.

---

## Quality gate (evaluation → execution trigger at end of B6.5)

Before B6.6 execution, verify:

1. **Anchor-set recall 100%** — every hand-labeled positive is in the candidate list
2. **Cross-method auto-apply safety** — unanimous MERGE + confidence > 0.85 sample (100 pairs) hand-verified by Claude+user, zero FPs
3. **Toughest-pairs reviewed** — user signed off on every curated pair
4. **No unresolved policy edge case** — renames, private-labels, parent-child all have explicit verdicts
5. **Gate 6 ablation passed** — cache hit rate > 70%, L3 web-grounding earns its cost (or was dropped)

If any fails → B6.7+ iteration.

---

## Acceptance criteria for Sprint 6 close

1. `pipeline/identity/producer_dedup.py` exists, importable, covers all 4 levels + signal collection + apply + reverse
2. `docs/IDENTITY_RULES.md` Section 11 (producer identity rules) exists and has been reviewed
3. `producer_merge_history` populated for every merge
4. `producer_dedup_pairs` populated with per-pair per-level verdicts
5. All source_lwin rows have `canonical_producer_id` (B6.2 outcome)
6. All auto-apply merges applied; all user-reviewed pairs resolved
7. `producer_aliases` populated from MERGE old-names; `parent_producer_id` populated for PC verdicts
8. `data/sprints/dedup/evaluation.md` written with list size, per-strategy contribution, per-level FPR/FNR, cost actuals, merges applied, open UNCERTAIN count
9. `data/sprints/dedup/open_questions.md` populated with flagged patterns (if any)
10. User sign-off in journal

---

## Session prompts

- B6.1 (done): `data/session_prompts/b6_1_planning.md`
- B6.2 (next): `data/session_prompts/b6_2_lwin_import.md` (written at B6.1 close)

Block-level prompts for B6.3-B6.N written at the close of their predecessor blocks.
