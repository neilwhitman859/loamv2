-- Migration: Create alias tables for regions, producers, and label designations
-- Run via Supabase MCP apply_migration or psql when available
-- Pattern follows existing appellation_aliases table

-- 1. Region aliases
CREATE TABLE IF NOT EXISTS region_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  region_id UUID NOT NULL REFERENCES regions(id) ON DELETE CASCADE,
  alias TEXT NOT NULL,
  alias_normalized TEXT NOT NULL,
  alias_type TEXT NOT NULL DEFAULT 'alternate_name'
    CHECK (alias_type IN ('alternate_name', 'translation', 'abbreviation', 'historical_name')),
  language_code TEXT,  -- ISO 639-1
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_region_aliases_normalized ON region_aliases(alias_normalized);
CREATE INDEX IF NOT EXISTS idx_region_aliases_region_id ON region_aliases(region_id);

-- 2. Producer aliases
CREATE TABLE IF NOT EXISTS producer_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  producer_id UUID NOT NULL REFERENCES producers(id) ON DELETE CASCADE,
  alias TEXT NOT NULL,
  alias_normalized TEXT NOT NULL,
  alias_type TEXT NOT NULL DEFAULT 'alternate_name'
    CHECK (alias_type IN ('alternate_name', 'abbreviation', 'previous_name', 'parent_company', 'informal')),
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_producer_aliases_normalized ON producer_aliases(alias_normalized);
CREATE INDEX IF NOT EXISTS idx_producer_aliases_producer_id ON producer_aliases(producer_id);

-- 3. Label designation aliases
CREATE TABLE IF NOT EXISTS label_designation_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  label_designation_id UUID NOT NULL REFERENCES label_designations(id) ON DELETE CASCADE,
  alias TEXT NOT NULL,
  alias_normalized TEXT NOT NULL,
  alias_type TEXT NOT NULL DEFAULT 'abbreviation'
    CHECK (alias_type IN ('abbreviation', 'alternate_spelling', 'translation', 'synonym')),
  language_code TEXT,
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_label_designation_aliases_normalized ON label_designation_aliases(alias_normalized);
CREATE INDEX IF NOT EXISTS idx_label_designation_aliases_designation_id ON label_designation_aliases(label_designation_id);
