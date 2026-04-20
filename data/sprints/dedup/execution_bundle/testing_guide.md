# Testing Guide

This bundle is frozen but unexecuted. Before approving execution, you should
test it from multiple angles. This guide enumerates the tests I recommend.

## What's already been tested

- **Chrome-per-pair validation (B6.5a):** all 493 verdicts were originally
  web-verified with specific URLs logged.
- **B6.6 re-Chrome:** 4 manual + 66 Core + 127 Mid/Tail/Yellow MERGE+PC
  verdicts were re-verified with fresh Chrome queries. Evidence URLs are in
  `rechrome_flips.md`.
- **Canonical-row lookup:** every MERGE and PC verdict was cross-checked
  against existing DB rows to detect "rename target already exists as a
  separate canonical row" cases.
- **Schema compatibility:** the proposed SQL has been validated against the
  current `producers`, `producer_merge_history`, and FK-referencing tables.

## What you should test

### 1. Sample the ledger

Open `verdict_ledger.jsonl` and pick ~20 random entries. For each, verify:
- The `ledger_key` matches the file tier (e.g. `core#12345` is in
  `core_verdicts.jsonl`)
- The `original_verdict` matches what's in the source file
- If an override was applied, the `override_source` and `override_reasoning`
  are present and make sense
- The `final_verdict` is consistent with the action

### 2. Spot-check 10 MERGEs by hand

Pick 10 MERGE entries. For each:
- Open the Chrome evidence URL (if present) and confirm it says what the
  reasoning claims.
- Look up the producer names on Wine-Searcher. Do they resolve to the same
  real-world estate?
- If `canonical_redirect_id` is set, look up that producer in the DB —
  does it make sense as the real survivor?

### 3. Spot-check 5 PARENT_CHILDs

Same protocol as MERGEs. Additionally: does the parent actually own the
child? Is the "distinct brands, one owns the other" rule satisfied?

### 4. Verify the scorecard

Open `scorecard.md` and check:
- Total verdict counts match the ledger (grep the ledger by `final_verdict`)
- Top 20 largest merges look sane — no obvious factual errors
- FK surface counts are plausible (don't expect `wines.producer_id` > 2000
  on a 138-merge batch)

### 5. Outside-of-Claude tests

Paste the bundle into another AI session (ChatGPT, Gemini, fresh Claude) and
ask:
- "Review this bundle and tell me if any decisions look wrong."
- "Pick 5 verdicts at random and critique the reasoning."
- "What's missing from the methodology?"

This catches blind spots the original session might have. Don't ask leading
questions; let the new AI form its own view.

### 6. Dry-run the execute script

```
python scripts/sprint6_step10_execute.py --ledger data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
```

(No `--execute` flag.) This will:
- Re-read the ledger
- Resolve all producer_ids and confirm they still exist
- Print the transaction plan per pair without applying
- Exit with a summary of proposed operations

Compare the summary to `scorecard.md`. They should match.

### 7. Query the DB for the affected producers

For each of the top 20 MERGE losers (biggest wine counts on the row to be
absorbed), look up the producer page in the DB and hand-inspect:
```
SELECT * FROM wine_detail_view WHERE wine_id IN (
  SELECT id FROM wines WHERE producer_id = :loser_id
) LIMIT 10;
```

Does the wine data match the producer name you're about to merge it under?

### 8. Sanity-check deferred pairs

Open `deferred_to_sprint_7.md` and confirm each deferral is reasonable. The
deferred set is expected to be small (~a handful at most) — these are pairs
where neither Chrome nor re-Chrome could reach a confident decision.

### 9. Reversibility walkthrough

Read `reversibility.md` and pick one merge from the ledger. Mentally walk
through the reversal SQL. Confirm that:
- The `merged_producer_json` snapshot would contain enough to restore the
  loser's metadata
- The FK re-points are recorded with enough per-table detail (if not, flag
  this — the execute script may need extending)
- The soft-delete + restore pattern is clean

### 10. Safety Net B: post-blocking rescan

After execution (not now), run the blocking SQL that originally produced the
600K pair pool. Verify:
- Fewer new pairs surface after merging (duplicates collapsed)
- No new Core pair (max wc ≥ 10) surfaces that wasn't in the original pool
  (a new Core pair suggests the original blocking missed it — worth
  investigating)

## Red flags to watch for

If you see any of these while reviewing, stop and flag:

- A MERGE verdict where the two producers' wine lists span different
  wine regions (e.g., one side has only Napa wines, other side has only
  Bordeaux). This was the #1 failure mode in B6.5a validation.
- A PARENT_CHILD verdict where the "parent" has fewer wines than the child
  and no canonical_redirect to an existing row. The parent should usually
  be the larger, more-established entity.
- A MERGE with `survivor_name` that looks nothing like either original name
  AND no `canonical_redirect_id`. This would create a new name for a
  producer; rare but possible.
- An override with no `chrome_url` or `chrome_evidence`. Every B6.6 flip
  should have cited evidence.
- `canonical_redirect_id` pointing to a producer row with 0 wines. Usually
  the redirect should point to a row that already has at least as many
  wines as either pair side.

## Walking back from "execute"

If you've run `--execute` and regret it:
1. Read `reversibility.md` top to bottom.
2. Check how long ago execution happened (`SELECT max(merged_at) FROM
   producer_merge_history WHERE method_name = 'B6.6 Chrome-validated'`).
3. Plan the reversal in reverse chronological order (LIFO).
4. Reverse in small batches (say 10 at a time), verifying after each.
5. If reversal leaves orphan data (wines pointing at nothing), investigate
   before continuing.
