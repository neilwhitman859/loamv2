# B6.5b — Interactive user review + merge queue

You are opening B6.5b. B6.5a just produced `data/sprints/dedup/review_queue.json` containing 1,500-5,000 pairs that need user review per committed thresholds. B6.5b is the interactive review session that resolves them.

**Full context:** [`data/sprints/dedup/b6_4_analysis.md`](data/sprints/dedup/b6_4_analysis.md), [`data/sprints/dedup/final_thresholds.json`](data/sprints/dedup/final_thresholds.json).

---

## Pre-B6.5b state (from B6.5a)

- Production ladder completed: L1+L1.5+L2+L2.5+L3 run on 151K pairs.
- Final routing (with refined PC rule: 2+ tier PC OR L2/L2.5 rich PC ≥0.90 OR L3 PC any conf):
  - ~2,300-4,700 auto-MERGE (ready for B6.6)
  - ~116,000-159,000 auto-SKIP (dropped from queue)
  - ~1,500-3,000 pairs in `review_queue.json` — mix of confirmed-PC cases, L3 UNCERTAIN, L3 low-conf MERGE, disagreements, L4-flagged outliers
- L4 Opus audit flagged cross-pair patterns + outliers — those are in the review queue marked `l4_flag=true`.

---

## Scope of B6.5b

### Step 1 — Inspect review queue and design batching

Load `review_queue.json`. Expect it already has pattern categories per pair. Common categories:

- Parent-child candidate (shared TTB permit, distinct brands)
- Rename / historical name (Domaine X → Domaine Y, same brand on label)
- Private label (Charles Shaw, Kirkland, etc.)
- Accent / diacritic variants (Château vs Chateau)
- Importer prefix ("a Becky Wasserman Selection")
- Commune collision (Latour vs Latour-Martillac)
- Cross-country same name
- Low-confidence MERGE from L3 (0.70-0.92)
- UNCERTAIN propagated through all tiers

Batch by pattern. User signs off fastest when all pairs in a batch share the same decision logic.

### Step 2 — Propose review format

Target: display 5-10 pairs at a time, grouped by pattern. Each pair shows:

```
[pair_id] <producer_a_name> / <producer_b_name>  [country]
  Wines A: <top 5 wines>
  Wines B: <top 5 wines>
  TTB: <summary of overlap or distinctness>
  Website: <producer A website> | <producer B website>
  LWIN: <Liv-ex records shared or distinct>
  L1/L1.5/L2/L2.5/L3 verdicts (confidences + short reasoning)
  L3 web evidence URLs (if available)
  L4 audit flag: <pattern> (if any)

  Claude recommendation: MERGE | PARENT_CHILD | SKIP | FLAG-FOR-LATER
    Rationale: <1-2 sentences citing specific evidence>

  User decision: [ ] MERGE  [ ] PC  [ ] SKIP  [ ] FLAG
```

Decisions captured to `producer_dedup_pairs.verdict_source='b6_5b_user_review'`.

### Step 3 — Walk user through batches

Order suggestion:
1. **Pattern: obvious renames** (likely fast MERGE signoff): Claude recommends MERGE with rename evidence, user signs off batch.
2. **Pattern: parent-child candidates** (shared TTB + distinct brands): user decides MERGE or PARENT-CHILD per case; logs to `parent_producer_id`.
3. **Pattern: private labels**: Charles Shaw / Kirkland / Cameron Hughes — typically same producer (Bronco, Rombauer) but may need case-by-case.
4. **Pattern: commune collisions**: usually SKIP but Claude presents evidence; one-pass.
5. **Pattern: L3 low-conf MERGE**: harder cases, go slower, 1-2 per screen.
6. **Pattern: UNCERTAIN through all tiers**: likely FLAG-FOR-LATER with reason.
7. **L4 Opus-flagged outliers**: 1-2 per screen with Claude's concern highlighted.

### Step 4 — Log decisions

For every reviewed pair:
- Update `producer_dedup_pairs`: `verdict`=<user decision>, `verdict_source`='b6_5b_user_review', `reasoning`=<user note if any>
- If verdict=FLAGGED: add to `data/sprints/dedup/open_questions.md` with pattern + reason
- If verdict=PARENT_CHILD: record survivor direction (a→b or b→a) for B6.6 to populate `parent_producer_id`

### Step 5 — Finalize merge queue for B6.6

After all reviewable pairs processed:

```sql
CREATE TABLE producer_merge_queue AS
SELECT producer_id_a, producer_id_b, verdict, verdict_source,
       -- survivor selection per §11.6
       CASE WHEN verdict='MERGE' THEN <survivor_id> END AS survivor_id,
       CASE WHEN verdict='PARENT_CHILD' THEN <parent_id> END AS parent_producer_id
FROM producer_dedup_pairs
WHERE verdict IN ('MERGE','PARENT_CHILD')
  AND verdict_source IN ('auto','b6_5b_user_review');
```

Expected size:
- MERGE: 2,300-4,700 (auto + user-approved)
- PARENT_CHILD: 200-800 (all user-reviewed)

### Step 6 — Quality gate for B6.6

Verify before closing B6.5b:
- All user_review pairs have a verdict (no blanks)
- FLAGGED pairs logged in `open_questions.md`
- Survivor selection per §11.6 applied to all MERGE pairs
- Parent direction recorded for all PC pairs
- `producer_merge_queue` row count matches MERGE+PC total

---

## Do NOT do in B6.5b

- Execute merges (that's B6.6)
- Modify `producers.id`, `wines.producer_id`, or any production data
- Drop pairs without user sign-off

---

## Review upgrade ideas (if pile > 2,000)

1. **Pattern auto-confirm:** if 50 consecutive same-pattern pairs all get MERGE from user, offer "apply same to remaining N in pattern" bulk action
2. **Diff-only display:** only show fields that differ between A and B (names + any unique wines)
3. **Claude first-filter:** before showing, run Claude with "if you were 99% confident, what would you say? Otherwise show user." Auto-accept Claude's 99% decisions with a daily summary for audit.
4. **Per-pattern IDENTITY_RULES proposal:** if a pattern keeps appearing with consistent user decisions, propose adding a §11 subsection so B6.7+ handles them automatically.

---

## Close-out

1. Update `data/sprints/dedup/journal.md` with B6.5b entry
2. Update `data/sprints/dedup/sessions.json` + `budget.json`
3. Update `data/dashboard.html`
4. Write `data/session_prompts/b6_6_execution.md`
5. Commit: "B6.5b: user review complete, merge queue ready for B6.6"

---

## Budget for B6.5b

~$0-5 (Claude time only, no new API calls beyond the context-pack enrichment which should already be in `review_queue.json` from B6.5a).

Expected session length: 1-3 hours interactive with user.
