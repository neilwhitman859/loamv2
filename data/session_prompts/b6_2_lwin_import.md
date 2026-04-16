# B6.2 — LWIN Producer Import

You are opening B6.2 of Sprint 6 (Producer Dedup). B6.1 closed with the full
Sprint 6 plan locked (`data/sprints/dedup/plan.md`). Your scope is narrow:
**complete the LWIN producer import that was left partial by prior work.**

---

## Why this block exists

During B6.1, we discovered that only ~10,519 of the 32,755 distinct LWIN
producer names in `source_lwin` have been promoted to canonical. **24,762
distinct LWIN producers are unlinked**, representing 69,444 unlinked
`source_lwin` wine rows.

Prior import (`pipeline/promote/lwin_long_tail.py`, Session 13) only
processed all US producers + international producers with ≥8 LWIN wines. The
long tail (international producers with 1-7 wines, ~24K of them) was skipped.

Running Sprint 6 dedup (B6.3+) without closing this gap means deduping on an
incomplete producer universe. B6.2 closes it first.

---

## Scope (do ONLY this)

1. Complete the LWIN producer import so every distinct `(producer_name,
   country)` in `source_lwin` has `canonical_producer_id` set.
2. Use the same simple matching method as the prior run (user directive in
   B6.1): exact normalized name + same country → link; else create new
   producer row. No AI, no fuzzy matching.
3. Verify the outcome: `producers` table grows from 10,683 to ~25-35K;
   `source_lwin.canonical_producer_id` populated for nearly all rows.

---

## Do NOT do

- Any schema changes (that's B6.3)
- Any IDENTITY_RULES.md edits (that's B6.3)
- Any dedup work or LLM calls
- Any merging of existing canonical producers
- Any wine-level promotion beyond what the existing script already does

---

## Method

### Step 1: Query current LWIN state

Confirm the starting numbers before running anything:

```sql
SELECT
  (SELECT COUNT(DISTINCT producer_name) FROM source_lwin WHERE producer_name IS NOT NULL) AS distinct_lwin_producers,
  (SELECT COUNT(DISTINCT producer_name) FROM source_lwin WHERE producer_name IS NOT NULL AND canonical_producer_id IS NOT NULL) AS distinct_linked,
  (SELECT COUNT(DISTINCT producer_name) FROM source_lwin WHERE producer_name IS NOT NULL AND canonical_producer_id IS NULL) AS distinct_unlinked,
  (SELECT COUNT(*) FROM source_lwin WHERE canonical_producer_id IS NULL) AS unlinked_rows,
  (SELECT COUNT(*) FROM producers) AS total_producers;
```

Expected (verified in B6.1 2026-04-16):
- distinct_lwin_producers: 32,755
- distinct_linked: 10,519
- distinct_unlinked: 24,762
- unlinked_rows: ~69,444
- total_producers: 10,683

If numbers have drifted, proceed anyway but note in journal.

### Step 2: Review and extend `pipeline/promote/lwin_long_tail.py`

The existing script only processes US producers + intl with ≥8 wines. B6.2
needs the full long tail.

Read `pipeline/promote/lwin_long_tail.py` and identify the `fetch_eligible_producers`
function. It currently filters to:

```python
WHERE (country = 'United States')
   OR (country != 'United States' AND wines >= %s)  -- default 8
```

For B6.2 the filter should be:

```python
-- all producers with at least 1 LWIN wine and no existing canonical_producer_id
WHERE ...
  AND EXISTS (
    SELECT 1 FROM source_lwin sl
    WHERE sl.producer_name = buckets.producer_name
      AND sl.country = buckets.country
      AND sl.canonical_producer_id IS NULL
  )
```

Either:
- Add a `--all-producers` flag that bypasses the country/wine filter, OR
- Add a `--resume-unlinked` flag that only processes producers with
  unlinked source_lwin rows

The second is safer (idempotent, resume-safe). Prefer it.

**Junk filter: keep DISABLED** (inclusive mode per B6.1 decision — rather
create a possibly-junk producer that dedup catches than reject a real one).

### Step 3: Dry-run on a small sample

```bash
python -m pipeline.promote.lwin_long_tail --resume-unlinked --dry-run --sample 20
```

Inspect output. Expected behavior for each producer:
- If exact normalized name + country matches an existing canonical → report "matched"
- If no match → report "would create" with the producer_name, country, and
  inferred country_id/region_id
- If name normalization produces an empty string → report "error, skipped"

Verify the sample looks reasonable before a full execute.

### Step 4: Execute the full long-tail import

```bash
python -m pipeline.promote.lwin_long_tail --resume-unlinked --execute
```

This will take time. The script is resume-safe (checks `processed_at`), so a
crash mid-run is recoverable. Use `--progress-file data/stats/lwin_b6_2_log.txt`
for progress logging.

Expected runtime: 1-3 hours for ~25K producers.

### Step 5: Verify outcome

```sql
-- Should show 0 or near-0 unlinked
SELECT COUNT(DISTINCT producer_name) FROM source_lwin
WHERE producer_name IS NOT NULL AND canonical_producer_id IS NULL;

-- Should show 25,000 to 40,000 (up from 10,683)
SELECT COUNT(*) FROM producers;

-- Spot-check 10 newly-created producers
SELECT id, name, country_id, region_id, created_at
FROM producers
ORDER BY created_at DESC
LIMIT 10;
```

### Step 6: Sanity checks

- Verify 10 newly-created producer names look like real producers (not garbage)
- Verify no duplicate `(name_normalized, country_id)` pairs exist — the import
  itself should be idempotent, but confirm
- Verify `source_lwin` rows have matching `canonical_wine_id` where wine
  promotion happened (may be 0 if wine-level promotion is separate)

If any sanity check looks off, STOP and report to user before closing B6.2.

---

## Acceptance gate

Before closing B6.2:

1. `SELECT COUNT(DISTINCT producer_name) FROM source_lwin WHERE canonical_producer_id IS NULL` returns 0 (or <100 explained residuals)
2. `producers` table count between 20,000 and 40,000
3. 10 newly-created producer rows look sane
4. No Python exceptions during the run (or all logged + resolved)

---

## Close-out work

1. Update `data/sprints/dedup/journal.md` with B6.2 entry: row counts before/after, runtime, any issues
2. Update `data/sprints/dedup/sessions.json` with B6.2 entry (spent: ~$0)
3. Update `data/sprints/dedup/budget.json` (sessions.B6.2 = 0.00)
4. Update `data/dashboard.html` (B6.2 checkbox, producer count in vitals)
5. Update `CLAUDE.md` Key Numbers producer count
6. Update `data/sessions.md` with B6.2 entry (one line, terse)
7. Write `data/session_prompts/b6_3_schema_and_l1.md` (session prompt for B6.3)
8. Commit + push: "B6.2: LWIN producer import complete — long tail imported"

---

## Potential complications and how to handle

- **Script fails on a specific producer name:** investigate (encoding issue? weird unicode?), patch, resume
- **Country resolution fails for some LWIN country values:** the normalize layer should handle most; escalate exotic cases to user if >10 producers affected
- **Region resolution fails:** acceptable — `region_id` is nullable; proceed without
- **Run takes much longer than 3 hours:** pause, investigate, consider batching
- **Result count way off from 25-40K expected range:** stop and investigate before committing

---

## Post-B6.2 state

After B6.2 closes:
- `producers` table holds the complete producer universe (including LWIN long tail)
- Sprint 6 dedup (B6.3+) runs on the full set
- Every future LWIN refresh flows through the same script with no manual intervention
- Some dupes likely exist (simple matching = spelling variants create new rows) — B6.3-B6.6 catches them
