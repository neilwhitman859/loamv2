# Reversibility

Every MERGE in this bundle is reversible via the `producer_merge_history`
table, which records a full snapshot of the absorbed row plus every FK
re-point that was made. PARENT_CHILD operations are trivially reversible by
clearing `parent_producer_id`.

## `producer_merge_history` schema

```
id                    uuid NOT NULL (pk)
merged_at             timestamptz NOT NULL
merged_producer_id    uuid NOT NULL  -- the loser / absorbed row
survivor_producer_id  uuid NOT NULL  -- the target / surviving row
merged_producer_json  jsonb NOT NULL -- full snapshot of the loser at merge time
repointed_rows        jsonb NOT NULL -- {"wines": N, "source_ttb_colas": N, ...}
match_decision_id     uuid
method_name           text           -- "B6.6 Chrome-validated"
reasoning             text
reviewed_by           text
reversed_at           timestamptz
reversed_by           text
reversal_notes        text
```

The `merged_producer_json` snapshot contains every column of the soft-deleted
row at the moment of merge, including its `name`, `slug`, `metadata`, and
all other fields. The `repointed_rows` JSONB records how many rows were
updated in each downstream table.

## Soft delete (not hard delete)

The execute script sets `producers.deleted_at = NOW()` on the loser row.
It does NOT `DELETE FROM producers`. The row stays in the table and can be
queried with `... WHERE deleted_at IS NOT NULL`.

## Undoing a single merge

To reverse merge record `merge_history_id`:

```sql
BEGIN;

-- Read the merge record
WITH mh AS (
  SELECT * FROM producer_merge_history WHERE id = :merge_history_id AND reversed_at IS NULL
)
-- Re-point all FKs back from survivor to the restored loser row
-- (execute per table from the repointed_rows JSONB)
UPDATE wines
  SET producer_id = (SELECT merged_producer_id FROM mh)
  WHERE producer_id = (SELECT survivor_producer_id FROM mh)
    AND id = ANY((SELECT (repointed_rows->'wines_ids')::uuid[] FROM mh));
-- (Repeat for every table the merge touched. The `repointed_rows` JSONB
--  should include enough per-table detail to identify exactly which rows
--  moved — not just counts. The execute script MUST record row IDs
--  per-table for this to work.)

-- Un-soft-delete the loser row
UPDATE producers SET deleted_at = NULL
  WHERE id = (SELECT merged_producer_id FROM mh);

-- Mark the merge record as reversed
UPDATE producer_merge_history
  SET reversed_at = NOW(), reversed_by = 'human reviewer', reversal_notes = :notes
  WHERE id = :merge_history_id;

COMMIT;
```

**Note:** the schema as currently designed records row counts in
`repointed_rows` but not row IDs. The execute script should be extended to
record per-table row IDs (as arrays) in the JSONB so reversal can be surgical.
This is an open item — see `open_questions.md`.

## Undoing a PARENT_CHILD

```sql
UPDATE producers SET parent_producer_id = NULL WHERE id = :child_producer_id;
```

(The `producer_merge_history` row is still inserted for PC actions per the
execute script's design, with `merged_producer_id = child_producer_id` and
`survivor_producer_id = parent_producer_id`, differentiated by a flag in
`repointed_rows` JSONB. See the execute script.)

## Bulk reversal

To reverse an entire batch of merges (e.g., if post-execution audit reveals a
pattern of errors):

```sql
-- Find the batch
SELECT id FROM producer_merge_history
WHERE merged_at BETWEEN :start AND :end
  AND method_name = 'B6.6 Chrome-validated'
  AND reversed_at IS NULL
ORDER BY merged_at DESC;
```

Then reverse each in LIFO order (most recent first, so chain merges unwind
correctly).

## What the execute script does to enable reversibility

Before applying any merge, the script:
1. Begins a transaction.
2. Reads the full loser row, serializes to JSONB, stores in
   `merged_producer_json`.
3. Collects the IDs of every row about to be re-pointed (per table).
4. Applies the FK re-points.
5. Stores the per-table row IDs in `repointed_rows` JSONB.
6. Inserts the `producer_merge_history` row.
7. Soft-deletes the loser.
8. Commits.

If any step fails, the transaction rolls back and the merge is skipped. The
script logs all skipped merges for manual review.

## Chain merges and reversibility

Chain merges (A → B → C, where B is intermediate) get special handling:
- `producer_merge_history` records A → C (terminal survivor), not A → B.
- But the `repointed_rows` JSONB on A's history entry includes a
  `chain_intermediate_id = B` field so reversal can trace the path.
- Reversing A → C restores A pointing back at B (since B is also soft-deleted),
  which then requires a separate reversal of B → C to fully restore A's
  original state. In practice, reverse the chain in reverse order: C←B first,
  then C←A, resulting in A, B, C all restored.

## Trust but verify

After execution, run `scripts/sprint6_step10c_post_audit.py` (TBD) which
samples 100 merges and verifies:
- Loser row is soft-deleted
- Survivor row retains canonical name
- Wines point to survivor
- `producer_merge_history` row exists and is complete

Any discrepancy triggers a pause for human review before further action.
