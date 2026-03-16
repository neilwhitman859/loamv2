-- PENDING MIGRATIONS — Run when Supabase MCP reconnects
-- Created 2026-03-16 during import→harden session (MCP was offline)
-- Execute each section as a separate apply_migration call
--
-- NOTE: wine_vintages already has skin_contact_days, whole_cluster_pct,
-- lees_aging_months, yield_hl_ha, mlf, maceration_days, batonnage,
-- aging_vessel. Vineyards already has elevation_m. These were added
-- in a prior session. Migration 1 removed.

-- ============================================================
-- MIGRATION 1: Alias tables (region, producer, label designation)
-- Pattern follows existing appellation_aliases table
-- ============================================================

-- 1a. Region aliases
CREATE TABLE IF NOT EXISTS region_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  region_id UUID NOT NULL REFERENCES regions(id) ON DELETE CASCADE,
  alias TEXT NOT NULL,
  alias_normalized TEXT NOT NULL,
  alias_type TEXT NOT NULL DEFAULT 'alternate_name'
    CHECK (alias_type IN ('alternate_name', 'translation', 'abbreviation', 'historical_name')),
  language_code TEXT,
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_region_aliases_normalized ON region_aliases(alias_normalized);
CREATE INDEX IF NOT EXISTS idx_region_aliases_region_id ON region_aliases(region_id);

-- 1b. Producer aliases
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

-- 1c. Label designation aliases
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

-- ============================================================
-- MIGRATION 2: Label designations to add
-- Discovered during imports — these appear on labels but are
-- missing from our reference data
-- ============================================================

-- Nykteri (Greek Santorini traditional designation)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Nykteri', 'nykteri', 'production_method',
  (SELECT id FROM countries WHERE name = 'Greece'),
  'Traditional Santorini designation: grapes harvested at high ripeness, pressed at night. Minimum 13.5% ABV, aged minimum 3 months. From Greek "nykteri" meaning "of the night".'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'nykteri');

-- Colheita (Port — single harvest tawny, aged minimum 7 years)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Colheita', 'colheita', 'aging_tier',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Port wine from a single harvest, aged in wood for a minimum of 7 years. Displays the year of harvest on the label. Lighter and more elegant than vintage Port, with oxidative tawny character.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'colheita');

-- En Rama (Sherry — minimally filtered, "raw" style)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'En Rama', 'en-rama', 'production_method',
  (SELECT id FROM countries WHERE name = 'Spain'),
  'Sherry bottled with minimal filtration and clarification — "en rama" means "raw" or "from the branch/cask". Preserves the natural character and complexity of the solera. Seasonal releases.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'en-rama');

-- Blanc de Noirs (Champagne/sparkling — white wine from red grapes)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Blanc de Noirs', 'blanc-de-noirs', 'production_method',
  (SELECT id FROM countries WHERE name = 'France'),
  'Champagne or sparkling wine made exclusively from red grape varieties (Pinot Noir and/or Pinot Meunier). Despite being made from red grapes, the wine is white — juice is separated from skins immediately after pressing.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'blanc-de-noirs');

-- Blanc de Blancs (Champagne/sparkling — white wine from white grapes only)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Blanc de Blancs', 'blanc-de-blancs', 'production_method',
  (SELECT id FROM countries WHERE name = 'France'),
  'Champagne or sparkling wine made exclusively from white grape varieties (typically 100% Chardonnay). Known for elegance, finesse, and citrus/mineral character.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'blanc-de-blancs');

-- Vieilles Vignes / Old Vines (already may exist — check)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Vieilles Vignes', 'vieilles-vignes', 'vineyard_age',
  NULL,
  'French term for "old vines." No legal definition in most jurisdictions — typically indicates vines 40+ years old. Older vines produce lower yields and more concentrated fruit. Used worldwide in French and English ("Old Vines").'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'vieilles-vignes');

-- Goldkapsel (German — gold capsule auction selection)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Goldkapsel', 'goldkapsel', 'quality_tier',
  (SELECT id FROM countries WHERE name = 'Germany'),
  'German "Gold Capsule" — indicates a special selection of higher quality within a Prädikat level. Often sold at VDP auctions. Not legally defined but universally understood as a step above standard bottlings. Some estates use "Lange Goldkapsel" for an even higher tier.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'goldkapsel');
