-- B6.3: Producer dedup schema extensions
--
-- Extends producer_dedup_pairs with per-method tracking columns and creates
-- producer_merge_history for reversible merge audit. Sprint 6 (producer dedup).
--
-- Design rationale in data/sprints/dedup/plan.md. Additive-only; no existing
-- data touched. RLS follows the public_read + service_write pattern.

-- ---------------------------------------------------------------------------
-- 1. Extend producer_dedup_pairs (currently: id, name_a, name_b, country,
--    similarity, wines_a, wines_b, verdict, verdict_source; zero rows)
-- ---------------------------------------------------------------------------

ALTER TABLE producer_dedup_pairs
  ADD COLUMN IF NOT EXISTS producer_id_a uuid REFERENCES producers(id) ON DELETE CASCADE,
  ADD COLUMN IF NOT EXISTS producer_id_b uuid REFERENCES producers(id) ON DELETE CASCADE,
  ADD COLUMN IF NOT EXISTS method_name   text,
  ADD COLUMN IF NOT EXISTS confidence    numeric,
  ADD COLUMN IF NOT EXISTS reasoning     text,
  ADD COLUMN IF NOT EXISTS cost_cents    numeric(10,4),
  ADD COLUMN IF NOT EXISTS signals       jsonb,
  ADD COLUMN IF NOT EXISTS ttb_evidence  jsonb,
  ADD COLUMN IF NOT EXISTS web_evidence  jsonb,
  ADD COLUMN IF NOT EXISTS flag_reason   text,
  ADD COLUMN IF NOT EXISTS created_at    timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at    timestamptz NOT NULL DEFAULT now();

-- One row per (pair, method). Pair canonicalized so producer_id_a < producer_id_b.
CREATE UNIQUE INDEX IF NOT EXISTS producer_dedup_pairs_pair_method_uq
  ON producer_dedup_pairs (producer_id_a, producer_id_b, method_name);

CREATE INDEX IF NOT EXISTS producer_dedup_pairs_method_idx
  ON producer_dedup_pairs (method_name);

CREATE INDEX IF NOT EXISTS producer_dedup_pairs_verdict_idx
  ON producer_dedup_pairs (verdict);

-- updated_at trigger
DROP TRIGGER IF EXISTS producer_dedup_pairs_set_updated_at ON producer_dedup_pairs;
CREATE TRIGGER producer_dedup_pairs_set_updated_at
  BEFORE UPDATE ON producer_dedup_pairs
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- 2. producer_merge_history  (reversible merge audit; programmatic rollback)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS producer_merge_history (
  id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  merged_at                timestamptz NOT NULL DEFAULT now(),
  merged_producer_id       uuid NOT NULL,
  survivor_producer_id     uuid NOT NULL REFERENCES producers(id) ON DELETE CASCADE,
  merged_producer_json     jsonb NOT NULL,
  repointed_rows           jsonb NOT NULL,
  match_decision_id        uuid REFERENCES match_decisions(id) ON DELETE SET NULL,
  method_name              text,
  reasoning                text,
  reviewed_by              text,
  reversed_at              timestamptz,
  reversed_by              text,
  reversal_notes           text
);

CREATE INDEX IF NOT EXISTS producer_merge_history_survivor_idx
  ON producer_merge_history (survivor_producer_id);

CREATE INDEX IF NOT EXISTS producer_merge_history_merged_at_idx
  ON producer_merge_history (merged_at DESC);

CREATE INDEX IF NOT EXISTS producer_merge_history_reversed_idx
  ON producer_merge_history (reversed_at) WHERE reversed_at IS NOT NULL;

ALTER TABLE producer_merge_history ENABLE ROW LEVEL SECURITY;

CREATE POLICY public_read_producer_merge_history
  ON producer_merge_history FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY service_write_producer_merge_history
  ON producer_merge_history FOR ALL
  TO service_role
  USING (true) WITH CHECK (true);

-- ---------------------------------------------------------------------------
-- 3. Verification
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  missing_cols int;
BEGIN
  SELECT COUNT(*) INTO missing_cols
  FROM (VALUES
    ('producer_id_a'),('producer_id_b'),('method_name'),('confidence'),
    ('reasoning'),('cost_cents'),('signals'),('ttb_evidence'),
    ('web_evidence'),('flag_reason'),('created_at'),('updated_at')
  ) AS expected(col)
  WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='producer_dedup_pairs'
      AND column_name = expected.col
  );
  IF missing_cols > 0 THEN
    RAISE EXCEPTION 'B6.3 migration failed: % producer_dedup_pairs columns missing', missing_cols;
  END IF;
END $$;
