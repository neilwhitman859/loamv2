# Session 13 — LWIN long-tail producer promotion sweep

**Type:** Hands-off deterministic work. Zero AI cost. Long run.
**Model:** Opus (for the planning/script work at the start); the actual promotion run is just Python.
**Prereqs:** Read `memory/30k_status.md` and the Session 12 entry in `data/stats/30k_journal.md` first.
**Do NOT touch:** Any enrichment work (`pipeline/enrich/*`), `wine_insights` content, or the voice-rules fix. Session 12 left Grade C in a known-broken state; a separate future session will handle that.

---

## Goal

Promote LWIN producers in the **10–19 wine bucket** (the "long tail") into the canonical tables. This is the backlog item logged at the top of `docs/BACKLOG.md` as the **[2026-04-10] LWIN long-tail promotion sweep (10+ wine producers)** entry.

The 30K Plan (Sessions 1–9) prioritized top-volume LWIN producers — the ~2,530 producers with 20+ wines each. That cut off at a clean boundary but left thousands of real, well-known producers sitting in `source_lwin` unpromoted. Fort Ross Vineyard was the prototype test case in Session 11 (0 → 15 canonical wines). The sweep expands that to every producer in the 10–19 wine bucket.

Expected outcome:

- **~3,230 producers** promoted into the canonical `producers` table
- **~40,847 new wines** created from `source_lwin`
- **Canonical wine count ~51,614 → ~92,000** (nearly doubles)
- Eliminates thousands of zero-result searches for legitimate producers

---

## Why this is the right next session

1. **Hands-off.** User explicitly asked for work they can walk away from. A full LWIN long-tail run will take hours.
2. **Deterministic.** No AI calls, no prompt tuning, no audit interpretation. It either works or it doesn't.
3. **Unblocks product.** Every long-tail producer promoted is a real wine someone searched for and failed to find. This is the single highest-impact product improvement that's purely a data job.
4. **Session 12 finished in a clean state.** Grade B pipeline works, Grade C is broken but *isolated to `pipeline/enrich/*`* — nothing in the LWIN promotion path touches it.

---

## Do this in order

### 1. Session briefing (5 min)

Query the live DB for:

- `SELECT COUNT(*) FROM wines WHERE duplicate_of IS NULL AND deleted_at IS NULL` (should be ~51,614)
- `SELECT COUNT(*) FROM producers` (should be ~2,530)
- `SELECT COUNT(*) FROM source_lwin` (should be 189,359)
- `SELECT COUNT(*) FROM source_lwin WHERE canonical_producer_id IS NOT NULL` (this is the pre-rebuild number — note that `source_lwin.canonical_*` FKs point to the archived tables; see BACKLOG)

Read `memory/30k_status.md` and the Session 12 entry in `data/stats/30k_journal.md` so you know what's off-limits.

### 2. Understand the Session 11 Fort Ross prototype (10 min)

Fort Ross was promoted as a one-off in Session 11 (narrative in the Session 11 entry in `data/stats/30k_journal.md` and `data/sessions.md`). Find the script that was used — check `git log --all --diff-filter=A --name-only -- 'pipeline/**'` around the Session 11 commits (`d1655a6 Session 11` etc.) for any new LWIN-related Python files, and skim them. You need to understand:

- How did Fort Ross's LWIN rows get matched to a new canonical producer?
- How did the new canonical wines get created (display name, grapes, color, appellation resolution)?
- Did it write to `external_ids` (lwin_7) and `data_provenance`?
- Did it update `source_lwin.canonical_producer_id` / `canonical_wine_id` to point at the new canonical rows? **Note the caveat from BACKLOG:** these FKs currently reference `archive_producers` / `archive_wines`, not the new canonical tables. Fort Ross may have worked around this by writing a parallel mapping.

If Session 11 left behind a reusable function or script, extend that. If it was a one-off inline, factor it into a proper script before running it across thousands of producers.

### 3. Build the wine-count filter (30 min)

Find the LWIN producer → wine-count query. The sweep should target producers with **10–19** LWIN wines (the 10+ bucket minus the already-promoted 20+ tier). Verify the bucket size: should be ~3,230 producers, ~40,847 wines.

Sanity checks before running:

- Sample 5 random producers in the target bucket. Are they real, recognizable producers? Any junk (test entries, "ZZZ — DO NOT USE", duplicates of already-promoted producers)?
- Spot-check that none of these producers are already in the canonical `producers` table under a slightly different name (e.g., accent differences, "Domaine" prefix stripping). If yes, we need a de-duplication check in the sweep, not blind INSERT.
- Check `source_lwin.canonical_producer_id` for the target rows — if non-NULL, they reference the OLD archived producer. Decide whether to write a new value pointing to the new canonical row or leave the legacy FK alone.

### 4. Decide on a safety mode and get user sign-off

Present the user with:

- Exact counts (producers, wines) that the sweep will create
- A sample of 5 producers + their wines so they can eyeball the quality
- The plan for `source_lwin.canonical_*` FKs (do nothing vs. write through vs. add a parallel column)
- Checkpointing strategy — the run might take hours, can it resume if it crashes?
- Whether you'll process producers in a single batch or in chunks with progress logging

**Do not start the sweep without user sign-off.** Show them a dry-run on 5 producers first if you're uncertain.

### 5. Run the sweep

- Run in background with progress logging
- Expect the run to take hours. Don't poll — the TaskNotification will fire when it completes.
- If it fails mid-run, diagnose and resume rather than starting over.

### 6. Validate

After completion:

- Row counts: `wines`, `producers`, `wine_grapes`, `external_ids` (lwin_7), `data_provenance`
- Spot-check Fort Ross still has its Session 11 data (shouldn't be affected but worth verifying)
- Pick 5 random promoted producers, inspect their wines in the DB, confirm grapes/appellations/colors resolved sensibly
- Run `search_catalog` against 5 producer names that were in the bucket but not previously in canonical — verify they now return results
- Josh Test: run `python -m pipeline.analyze.josh_test --save` and compare to the Session 11 baseline (85.0%). The sweep shouldn't affect Josh Test since it's wines Americans actually encounter, but it's a cheap check.

### 7. Wrap up

- Update `memory/30k_status.md` with new producer + wine counts
- Append a Session 13 entry to `data/stats/30k_journal.md`
- Move this session from `data/sessions.md` Active to Done with a summary
- Update `data/stats/30k_budget.json` (likely $0 new AI spend)
- Update `data/stats/30k_dashboard.md` via the dashboard regenerator if one exists
- Remove the LWIN long-tail item from `docs/BACKLOG.md` (or mark it as done in the session summary)
- Commit with a clear message: "Session 13: LWIN long-tail sweep — +N producers, +M wines"

---

## Budget

- AI: **$0 expected.** Everything is deterministic Python + SQL.
- If you find yourself wanting to use Haiku for fuzzy dedup or grape extraction mid-sweep, **stop and ask the user first.** The stated reason for this session is hands-off deterministic work.

---

## Do NOT

- Re-enrich any wines (Grade C is in a known-broken state; Grade B works but isn't the job this session)
- Touch `pipeline/enrich/*`
- Modify `wine_insights` content
- Fix the `source_lwin.canonical_*` FK bug (BACKLOG item — do it as a dedicated session once the sweep is done, or as a documented side-effect of the sweep itself with explicit user approval)
- Create cron loops (user has to explicitly request those; see CLAUDE.md)
- Try to also fix the 6,337 grape-percentage bug (P0 BACKLOG item, separate session)
- Try to also fix the 270 thin-packet Grade C wines (P1 BACKLOG item, separate session)

---

## Known pitfalls (carried from Session 11)

- **LWIN producer names often have `Domaine` / `Château` / `Chateau` / accent variants.** The existing promotion scripts handle this via `normalize_for_match` in `pipeline/identity/build_roster.py`. Use it, don't reinvent it.
- **Grape synonyms are messy.** Session 11 cleaned up ~5,000 rows of bad synonym mappings (Riesling→Crouchen, Melon→Pinot Blanc, etc.). The current `grape_synonyms` table is mostly clean but the 29 ambiguous-short-synonym cases remain (BACKLOG). Expect some borderline grape resolutions.
- **Appellation resolution may miss.** Some LWIN rows name appellations that exist in our `appellations` table under a different spelling. Expect some wines to land with `appellation_id IS NULL` and `region_id` inferred from producer location or from a looser match.
- **A producer with the same name may already exist.** The top 2,530 were promoted in Sessions 3–7. Before INSERTing a new producer row, check for an exact or fuzzy match in the current `producers` table.
- **Do not delete archive_* tables.** They are preserved as the full pre-rebuild record.

---

## Success criteria

- ≥ 3,000 new producers created
- ≥ 35,000 new wines created
- No duplicate producers (nothing already-in-canonical gets a second row)
- All new wines have a valid `external_ids.lwin_7` entry
- All new wines have at least `name`, `producer_id`, and `color` set
- `search_catalog` returns results for a sample of 10 producer names from the bucket
- Zero orphan FKs
- Josh Test unchanged or improved
