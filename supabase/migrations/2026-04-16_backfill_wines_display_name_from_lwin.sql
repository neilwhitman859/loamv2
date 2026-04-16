-- Migration: Backfill wines.display_name from Liv-ex source_lwin.display_name
--
-- Context
-- -------
-- source_lwin ships Liv-ex's authoritative combined-name field (`display_name`)
-- on 99.994% of rows (189,348 of 189,359). Format: "Producer[, Wine Name], Region"
-- e.g. "Muga, Prado Enea Gran Reserva, Rioja" / "Comte Senard, Meursault" /
-- "Dujac Fils et Pere, Clos de la Roche Grand Cru" (the last two being rows
-- where wine_name is NULL — LWIN encodes appellation + classification info in
-- display_name for unnamed cuvées).
--
-- The initial Session 13 + B6.2 imports populated wines.name from LWIN's
-- wine_name field but left wines.display_name untouched. This migration
-- backfills wines.display_name for every wine that carries an LWIN external_id,
-- using the authoritative Liv-ex string.
--
-- This is the LESS invasive of the two options considered: we do NOT rewrite
-- wines.name (which would churn slugs, invalidate caches, and risk uniqueness
-- collisions for zero user-visible benefit). Only the rendering field is
-- touched.
--
-- JOIN path: wines.id → external_ids.entity_id (where system='lwin') →
-- external_id = source_lwin.lwin OR source_lwin.lwin_7. A wine may carry both
-- a 7-digit and 11-digit LWIN; picking ANY match is fine since both resolve
-- to the same display_name.
--
-- Safety: only writes where display_name is currently NULL (idempotent on
-- re-run). Does not touch wines.display_name on non-LWIN-sourced wines.

WITH lwin_lookup AS (
  SELECT DISTINCT ON (ei.entity_id)
    ei.entity_id AS wine_id,
    sl.display_name
  FROM external_ids ei
  JOIN source_lwin sl
    ON sl.lwin = ei.external_id OR sl.lwin_7 = ei.external_id
  WHERE ei.entity_type = 'wine'
    AND ei.system = 'lwin'
    AND sl.display_name IS NOT NULL
    AND TRIM(sl.display_name) != ''
  ORDER BY ei.entity_id, sl.display_name
)
UPDATE wines
   SET display_name = lwin_lookup.display_name
  FROM lwin_lookup
 WHERE wines.id = lwin_lookup.wine_id
   AND wines.display_name IS NULL
   AND wines.deleted_at IS NULL;
