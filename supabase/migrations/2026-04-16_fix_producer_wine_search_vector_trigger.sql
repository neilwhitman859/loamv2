-- Migration: Fix producers + wines search_vector trigger functions
--
-- Context
-- -------
-- `update_producer_search_vector()` and `update_wine_search_vector()` called
-- `build_*_search_vector(NEW.id)`, which runs
--     SELECT ... FROM <table> WHERE id = NEW.id
-- On BEFORE INSERT the row does not yet exist in the heap, so the SELECT
-- returned zero rows and `NEW.search_vector := NULL`. Result: every inserted
-- producer + wine silently received NULL search_vector. Works fine on UPDATE
-- (row exists).
--
-- Exposed at scale by B6.2 (LWIN long-tail producer import 2026-04-16):
-- +22,598 producers + +42,095 wines all had NULL search_vector. Pre-B6.2
-- cohort was fine because prior pipelines likely updated rows post-insert,
-- firing the UPDATE path which works.
--
-- Fix
-- ---
-- Re-author the trigger functions to compute the tsvector inline from
-- `NEW.*` plus small lookups on related tables (regions / countries /
-- appellations / producers / wine_grapes) that DO exist at BEFORE INSERT
-- time. The `build_*_search_vector(uuid)` helpers are retained for ad-hoc
-- recomputes where the row already exists (e.g. backfill queries).
--
-- `appellations`, `regions`, `grapes` triggers already compute inline and
-- are left unchanged.

CREATE OR REPLACE FUNCTION public.update_producer_search_vector()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_region_name text;
  v_country_name text;
BEGIN
  IF NEW.region_id IS NOT NULL THEN
    SELECT name INTO v_region_name FROM regions WHERE id = NEW.region_id;
  END IF;
  IF NEW.country_id IS NOT NULL THEN
    SELECT name INTO v_country_name FROM countries WHERE id = NEW.country_id;
  END IF;
  NEW.search_vector :=
    setweight(to_tsvector('simple', coalesce(NEW.name, '')), 'A') ||
    setweight(to_tsvector('simple', coalesce(v_region_name, '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(v_country_name, '')), 'B');
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.update_wine_search_vector()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_producer_name text;
  v_appellation_name text;
  v_region_name text;
  v_country_name text;
  v_grape_names text;
BEGIN
  IF NEW.producer_id IS NOT NULL THEN
    SELECT name INTO v_producer_name FROM producers WHERE id = NEW.producer_id;
  END IF;
  IF NEW.appellation_id IS NOT NULL THEN
    SELECT name INTO v_appellation_name FROM appellations WHERE id = NEW.appellation_id;
  END IF;
  IF NEW.region_id IS NOT NULL THEN
    SELECT name INTO v_region_name FROM regions WHERE id = NEW.region_id;
  END IF;
  IF NEW.country_id IS NOT NULL THEN
    SELECT name INTO v_country_name FROM countries WHERE id = NEW.country_id;
  END IF;
  -- On BEFORE INSERT this returns NULL (wine_grapes rows don't exist for
  -- this wine yet). On UPDATE it returns the grape list. Matches prior
  -- behavior for the UPDATE flow.
  SELECT string_agg(g.display_name, ' ')
    INTO v_grape_names
    FROM wine_grapes wg JOIN grapes g ON g.id = wg.grape_id
   WHERE wg.wine_id = NEW.id;
  NEW.search_vector :=
    setweight(to_tsvector('simple', coalesce(NEW.name, '')), 'A') ||
    setweight(to_tsvector('simple', coalesce(v_producer_name, '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(v_appellation_name, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(v_region_name, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(v_country_name, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(v_grape_names, '')), 'C');
  RETURN NEW;
END;
$$;

-- Backfill NULLs introduced by the pre-fix bug. `build_*_search_vector`
-- helpers work correctly from the SELECT side because the row exists.
UPDATE producers
   SET search_vector = build_producer_search_vector(id)
 WHERE search_vector IS NULL
   AND deleted_at IS NULL;

UPDATE wines
   SET search_vector = build_wine_search_vector(id)
 WHERE search_vector IS NULL
   AND deleted_at IS NULL;
