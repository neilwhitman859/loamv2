-- MIGRATIONS APPLIED — All executed 2026-03-16 via Supabase MCP
-- Originally queued while MCP was offline
-- Kept for reference / git history
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

-- ============================================================
-- MIGRATION 3: New regions discovered during imports
-- Countries that only had catch-all regions but need named L1 regions
-- ============================================================

-- 3a. Lebanon — Bekaa Valley (most important wine region, ~90% of production)
INSERT INTO regions (name, slug, country_id, parent_id, level, is_catch_all)
SELECT 'Bekaa Valley', 'bekaa-valley',
  (SELECT id FROM countries WHERE name = 'Lebanon'),
  NULL, 1, false
WHERE NOT EXISTS (SELECT 1 FROM regions WHERE slug = 'bekaa-valley');

-- 3b. Lebanon — Mount Lebanon (coastal mountain wines)
INSERT INTO regions (name, slug, country_id, parent_id, level, is_catch_all)
SELECT 'Mount Lebanon', 'mount-lebanon',
  (SELECT id FROM countries WHERE name = 'Lebanon'),
  NULL, 1, false
WHERE NOT EXISTS (SELECT 1 FROM regions WHERE slug = 'mount-lebanon');

-- 3c. Lebanon — Batroun (emerging northern coastal region)
INSERT INTO regions (name, slug, country_id, parent_id, level, is_catch_all)
SELECT 'Batroun', 'batroun',
  (SELECT id FROM countries WHERE name = 'Lebanon'),
  NULL, 1, false
WHERE NOT EXISTS (SELECT 1 FROM regions WHERE slug = 'batroun');

-- ============================================================
-- MIGRATION 4: Cape Blend label designation
-- Discovered from Kanonkop import — uniquely South African designation
-- ============================================================

INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Cape Blend', 'cape-blend', 'production_method',
  (SELECT id FROM countries WHERE name = 'South Africa'),
  'South African blending designation requiring Pinotage as a significant component (typically 30-70%). Celebrates South Africa''s unique grape variety within a multi-variety blend. Recognized by the Cape Winemakers Guild and industry bodies.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'cape-blend');

-- Qvevri designation (for Georgian amber wines)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Qvevri', 'qvevri', 'production_method',
  (SELECT id FROM countries WHERE name = 'Georgia'),
  'Georgian traditional winemaking in qvevri (kvevri) — large clay vessels buried underground. Grapes fermented and aged on skins for months. UNESCO Intangible Cultural Heritage since 2013. The world''s oldest continuous winemaking tradition (8,000 years). Produces distinctive amber/orange wines from white grapes.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'qvevri');

-- Madeira sweetness designations (grape names that double as style designations)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Sercial', 'sercial-madeira', 'sweetness_style',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Driest style of Madeira wine, made from the Sercial grape. Maximum 25-45 g/L residual sugar. Intense acidity, citrus peel, almond character. Best served slightly chilled as aperitif.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'sercial-madeira');

INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Verdelho', 'verdelho-madeira', 'sweetness_style',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Medium-dry style of Madeira wine, made from the Verdelho grape. 45-65 g/L residual sugar. Smoky, caramel, and dried fruit with balancing acidity. Versatile — works as aperitif or with food.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'verdelho-madeira');

INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Bual', 'bual-madeira', 'sweetness_style',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Medium-sweet style of Madeira wine, made from the Bual (Boal) grape. 65-96 g/L residual sugar. Rich toffee, spice cake, and dried fig. Classic after-dinner Madeira.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'bual-madeira');

INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Malmsey', 'malmsey-madeira', 'sweetness_style',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Sweetest and richest style of Madeira wine, made from Malvasia (Malmsey). 96+ g/L residual sugar. Deep mahogany, molten toffee, dark chocolate, coffee. The acidity keeps it vibrant despite high sweetness.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'malmsey-madeira');

INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Terrantez', 'terrantez-madeira', 'sweetness_style',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Ultra-rare Madeira style from the nearly extinct Terrantez grape. Falls between Verdelho and Bual in sweetness. Smoky, bitter orange, incense. Among the most sought-after and expensive Madeiras.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'terrantez-madeira');

-- Rainwater (Madeira — light blended style)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Rainwater', 'rainwater-madeira', 'production_method',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Light, medium-dry style of Madeira historically associated with the American market. Name origin debated — possibly from rainwater diluting barrels on the docks, or from a particular shipper''s style. Blend of Tinta Negra, lighter bodied than noble variety Madeiras.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'rainwater-madeira');

-- Canteiro (Madeira — premium natural aging method)
INSERT INTO label_designations (name, slug, category, country_id, description)
SELECT 'Canteiro', 'canteiro', 'production_method',
  (SELECT id FROM countries WHERE name = 'Portugal'),
  'Premium Madeira aging method. Wine aged naturally in casks placed on wooden supports (canteiros) in warm upper floors of lodges, where heat from the sun slowly develops the wine. Superior to estufagem (artificial heating). Required for all noble variety 10+ year Madeiras.'
WHERE NOT EXISTS (SELECT 1 FROM label_designations WHERE slug = 'canteiro');

-- ============================================================
-- MIGRATION 5: New columns from metadata audit
-- Promoting high-frequency metadata fields to proper columns
-- ============================================================

-- 5a. wines.soil_description — free text soil description (1,489 entries in metadata)
-- Soil data ideally lives in vineyard_soils, but most wines don't have vineyard records.
-- This captures what's on the label/website: "Volcanic pumice over limestone"
ALTER TABLE wines ADD COLUMN IF NOT EXISTS soil_description TEXT;

-- 5b. wines.vine_age_description — free text vine age (1,469 entries in metadata)
-- Label-visible data: "30-70 years old", "Planted in 1946, 2003"
ALTER TABLE wines ADD COLUMN IF NOT EXISTS vine_age_description TEXT;

-- 5c. wines.vineyard_area_ha — vineyard size in hectares (1,468 entries in metadata)
ALTER TABLE wines ADD COLUMN IF NOT EXISTS vineyard_area_ha NUMERIC;

-- 5d. wines.commune — for French wines especially (53 entries)
-- The village/commune within the appellation: "Vosne-Romanée", "Meursault"
ALTER TABLE wines ADD COLUMN IF NOT EXISTS commune TEXT;

-- 5e. wines.altitude_m_low / altitude_m_high — vineyard altitude range (23 entries)
ALTER TABLE wines ADD COLUMN IF NOT EXISTS altitude_m_low INTEGER;
ALTER TABLE wines ADD COLUMN IF NOT EXISTS altitude_m_high INTEGER;

-- 5f. wines.aspect — vineyard orientation (23 entries): "South-Southwest", "Southwest"
ALTER TABLE wines ADD COLUMN IF NOT EXISTS aspect TEXT;

-- 5g. wines.slope_pct — vineyard slope percentage (22 entries)
ALTER TABLE wines ADD COLUMN IF NOT EXISTS slope_pct NUMERIC;

-- 5h. wines.monopole — boolean, is this a monopole vineyard (8 entries)
ALTER TABLE wines ADD COLUMN IF NOT EXISTS monopole BOOLEAN DEFAULT false;

-- 5i. producers.address — full address (198 entries in metadata as "location")
ALTER TABLE producers ADD COLUMN IF NOT EXISTS address TEXT;
