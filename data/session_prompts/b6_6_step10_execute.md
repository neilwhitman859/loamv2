# B6.6 — Sprint 6 Step 10 execution (apply §11.4 + run merges + close)

You are opening B6.6 of Sprint 6 (Producer Dedup). B6.5a Phase B (Claude-in-Chrome per-pair validation) is **COMPLETE at 100% rigor**: 493/493 pairs have 2+ content-page evidence with URL citations. All remaining work is local SQL + file editing — **$0 AI spend**.

---

## Pre-B6.6 state

**Chrome validation ledger (authoritative):**
- `data/sprints/dedup/chrome_validation/yellow_verdicts.jsonl` — 71 pairs
- `data/sprints/dedup/chrome_validation/core_verdicts.jsonl` — 143 pairs
- `data/sprints/dedup/chrome_validation/mid_verdicts.jsonl` — 138 pairs
- `data/sprints/dedup/chrome_validation/tail_verdicts.jsonl` — 141 pairs

**Final verdict distribution (493 total):**
- MERGE: 138 (change canonical data — re-point FKs, delete loser row)
- PARENT_CHILD: 55 (set `producers.parent_id`, preserve both rows)
- SKIP: 267 (no action)
- KEEP_AS_IS: 33 (Yellow-tier only; no action)

**Each verdict record has:**
```json
{
  "pair_id": <int>,
  "name_a": <str>, "name_b": <str>,
  "verdict": "MERGE" | "PARENT_CHILD" | "SKIP" | "KEEP_AS_IS",
  "survivor_name": <str, if MERGE>,
  "parent_name": <str, if PARENT_CHILD>,
  "pattern_cluster": "11.4.f" | "11.4.g" | "11.4.h" | "11.4.m" | "11.4.n" | "11.4.o" | "11.4.p" | "11.4.q" | "11.4.s",
  "evidence_url_a": <url>, "evidence_a": <content snippet>,
  "evidence_url_b": <url>, "evidence_b": <content snippet>,
  "reasoning": <str>
}
```

**Pattern clusters used during Chrome validation (need §11.4 codification):**
- `§11.4.f` — generational succession (same estate, parent→child winemaker label evolution)
- `§11.4.g` — holdco distinct labels (same owner, different brand-on-label → SKIP)
- `§11.4.h` — orthographic/article variants (with/without "de", "la", typos, short forms)
- `§11.4.m` — shared-surname family split (Burgundy/Bordeaux common surname, distinct branches)
- `§11.4.n` — global brand with multi-country sourcing (Tussock Jumper, Cupcake, Prophecy, Pieroth, Thomson & Scott, Bernard Magrez)
- `§11.4.o` — JV/collab labels (two principals on bottle)
- `§11.4.p` — retailer/merchant private-label (Club des Sommeliers, Berry Bros & Rudd own-label, Vivino-style)
- `§11.4.q` — Hospices de Beaune / Hospices de Nuits auction négociant-bottling pattern
- `§11.4.s` — sub-brand / cuvée / named-line under a parent (Austin Hope → Quest, DBR Lafite → Les Légendes, Manzanos → Dinastia Manzanos, Goldschmidt → Chelsea/Katherine)

**31 verdict corrections discovered during re-Chrome** — listed in the Chrome validation session's final summary. Most common pattern: SKIP → MERGE when same-family succession surfaced, SKIP → PARENT_CHILD when parent-line relationship became clear, MERGE → SKIP when sparse web couldn't confirm single identity.

**DO NOT re-litigate verdicts.** The Chrome work is locked. Step 10 applies the decisions already made.

---

## Scope of B6.6 (in order)

### Step 1 — §11.4 amendments to `docs/IDENTITY_RULES.md` (~30 min, $0)

Read the current `docs/IDENTITY_RULES.md`. §11 was drafted in B6.3; §11.4 sub-sections already exist. Audit them against the nine pattern clusters above. For each cluster:

- If §11.4.X exists and matches how it was applied in Chrome validation → no change.
- If §11.4.X exists but text diverges from applied pattern → update text, cite representative pair IDs from the verdict files.
- If cluster isn't codified yet → add subsection.

Spec: lead with the rule, give the applied decision (MERGE / SKIP / PARENT_CHILD), add 2-3 representative pair examples with pair_id and producer names. Keep total §11.4 under ~400 lines.

Commit this change as part of COMMIT #1 (not separately).

### Step 2 — Write `scripts/sprint6_step10c_scorecard.py` (~60 min, $0)

Pre-execution simulation. Does NOT mutate DB. Reads the four verdict JSONL files and for every MERGE + PARENT_CHILD produces:

- `producer_id_a`, `producer_id_b` resolved from `producer_dedup_pairs` (or re-resolved via name+country if needed)
- For MERGE: which producer_id is survivor (by US-market-label rule per `memory/feedback_producer_survivor_selection.md` and explicit `survivor_name` in JSONL when present)
- Wine counts: `wines_a_count`, `wines_b_count`, `total_wines_after_merge`
- Other FK impacts: `wine_vintages`, `external_ids`, `data_provenance`, `wine_lookups`, `wine_grapes`
- Parent-child routing for PC verdicts (no row delete, just `parent_id` set)

Flag any pair where:
- Either `producer_id_a` or `producer_id_b` is NULL (pair references a producer that no longer exists)
- Survivor can't be resolved (missing `survivor_name` on MERGE or ambiguous US-label heuristic)
- A MERGE pair's survivor is already a merge target in another pair (chain merges need ordering)

Output: `data/sprints/dedup/chrome_validation/step10c_pre_scorecard.md` — human-readable with tables:
1. Summary: N MERGE, N PARENT_CHILD, N wines affected, N producers deleted
2. Flags (if any) needing resolution before execution
3. Pattern-cluster breakdown
4. Top 20 largest merges by wines affected (manual sanity-check list)

### Step 3 — User review + signoff on scorecard

Show the scorecard. Wait for explicit approval before proceeding.

### Step 4 — COMMIT #1

```
B6.6 Step 10 prep: §11.4 amendments + Chrome verdicts + pre-execution scorecard

- docs/IDENTITY_RULES.md: §11.4 codified against 493 validated pairs
- data/sprints/dedup/chrome_validation/{yellow,core,mid,tail}_verdicts.jsonl (frozen)
- scripts/sprint6_step10c_scorecard.py
- data/sprints/dedup/chrome_validation/step10c_pre_scorecard.md
```

### Step 5 — Write `scripts/sprint6_step10_execute.py` (~90 min, $0)

The actual DB mutation script. Dry-run by default, `--execute` to apply.

For each MERGE verdict:
1. Resolve `survivor_producer_id` and `loser_producer_id` (from JSONL + DB + survivor-selection rule).
2. In single transaction:
   - Re-point FKs: `UPDATE wines SET producer_id = survivor WHERE producer_id = loser` and the same for `wine_vintages.producer_id` if present, `external_ids.producer_id`, `data_provenance.producer_id`, `wine_lookups.producer_id`, `wine_grapes.producer_id`, anywhere else FK-referenced (query `information_schema` once at script start to enumerate).
   - Move `external_ids` rows from loser → survivor (deduplicate on `(source, external_id, identifier_type)`).
   - Merge `producers.aliases` arrays (add loser's `name` to survivor's aliases).
   - Merge other arrays: `farming_certifications`, `biodiversity_certifications`, etc.
   - Prefer survivor's non-null scalar fields; fall back to loser's non-null values.
   - Insert `producer_merge_history` row: `(source_producer_id, survivor_producer_id, verdict, pattern_cluster, evidence_url_a, evidence_url_b, reasoning, pair_id, merged_at)`.
   - `DELETE FROM producers WHERE id = loser_producer_id`.
3. Commit.

For each PARENT_CHILD verdict:
1. Resolve `parent_producer_id` and `child_producer_id`.
2. `UPDATE producers SET parent_id = parent_producer_id WHERE id = child_producer_id`.
3. No row delete. No FK re-point. Insert `producer_merge_history` with `verdict='PARENT_CHILD'`.

For SKIP / KEEP_AS_IS: no-op.

Handle chain merges: if A→B and B→C both MERGE, execute in dependency order so A ends up pointing at C.

Script output:
- Per-pair pass/fail
- Summary: N merges executed, N FK updates, N producer rows deleted, N `producer_merge_history` rows
- Any skipped-due-to-error pairs listed with reason

### Step 6 — Execute merges (Step 10a)

```
python scripts/sprint6_step10_execute.py --execute
```

Record final counts. Expected: ~138 producer rows deleted + ~55 parent_id updates + ~193 `producer_merge_history` rows.

### Step 7 — Step 10b Safety Net B rescan (~15 min, $0)

Re-run the same blocking strategies that produced the original 151K pairs. Compare against post-merge `producers` table:

- Expected: fewer candidate pairs (since merged duplicates now collapsed to single rows).
- Flag: any new Core pair (max_wc ≥10) that wasn't in the original pool — this means blocking missed it first time and the merge surfaced it.
- Flag: any pair where both producer_ids are now NULL (shouldn't happen but sanity check).

### Step 8 — Step 10c-post verification audit (~30 min, $0)

Sample 100 applied merges (stratified: 40 Core, 30 Mid, 20 Tail, 10 Yellow). For each:
- Confirm loser producer row is deleted
- Confirm survivor row retains the canonical name
- Confirm wines now point to survivor
- Confirm `producer_merge_history` row exists

Output: `data/sprints/dedup/chrome_validation/step10c_post_audit.md` with any discrepancies.

### Step 9 — COMMIT #2

```
B6.6 Step 10 execute: 193 merges applied + producer_merge_history + Safety Net B

- scripts/sprint6_step10_execute.py
- producer_merge_history populated (193 rows)
- producers row count: <before> → <after>
- wines FK re-point: <N> rows updated
- data/sprints/dedup/chrome_validation/step10c_post_audit.md
- Safety Net B rescan: <N> pairs total, <N> new Core surfaced
```

### Step 10 — Sprint 6 close

- Update `data/dashboard.html` — final Sprint 6 metrics, move Step 10 to done, show producer row count before/after.
- Archive `data/sprints/dedup/` → `data/sprints/_archive/dedup/` and update `data/sprints/current.json` to point to next sprint (likely `wine-dedup` pre-planning).
- Update `CLAUDE.md` "Current Focus" — Sprint 6 CLOSED, move to Sprint 7 wine dedup planning.
- Append to `memory/` any durable lessons — candidates: "Chrome-per-pair rigor is non-negotiable when user commits to Path A" (reinforces existing memory), "Re-Chrome surfaced 31 corrections on 191 batch-logged pairs — 16% correction rate when batching is applied to family-split pattern clusters".

---

## Budget

$0 expected. All work is local SQL + Python + markdown editing. If anything requires AI reasoning (e.g. resolving a survivor-name ambiguity surfaced by the scorecard), do it Opus-inline in the current session per `memory/feedback_opus_inline_reasoning.md` — do not propose a Haiku/Sonnet batch.

---

## Discipline reminders

- **Do not re-open any verdict.** If a verdict looks wrong during scorecard review, flag it for the user's decision — don't flip it unilaterally.
- **Plan-lock still applies.** `data/sprints/dedup/chrome_validation/PLAN_LOCK.md` governs the Chrome outputs; downstream work doesn't re-examine them.
- **User reviews the scorecard before execution.** Do not run `--execute` on `sprint6_step10_execute.py` without explicit signoff.
- **Single transaction per merge pair.** If any merge fails mid-batch, the script should continue with the next pair rather than rolling back everything.
- **Commit twice, not once.** COMMIT #1 captures prep + scorecard; COMMIT #2 captures the actual mutation + audit. If anything goes wrong during execution, COMMIT #1 is a recoverable checkpoint.

---

## Files to read at session start

1. `CLAUDE.md` — project context + current focus
2. `data/dashboard.html` — live sprint state
3. `data/sprints/dedup/chrome_validation/PLAN_LOCK.md` — the locked plan that governed Phase B
4. `docs/IDENTITY_RULES.md` — §11.4 current state (for amendment drafting)
5. The four verdict JSONL files — skim for pattern_cluster distribution

---

## What success looks like

End-of-session state:
- §11.4 amendments merged to `docs/IDENTITY_RULES.md`
- `producer_merge_history` populated with ~193 rows
- Producer count down by ~138 (MERGE deletions)
- ~55 `producers.parent_id` values set
- 100-pair post-audit shows 100/100 clean
- Sprint 6 archived, dashboard final, `CLAUDE.md` updated to Sprint 7
- Two clean commits pushed
