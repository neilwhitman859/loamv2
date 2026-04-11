# Phase 0: Archive & Fresh Start

**Read first:** `docs/30K_PLAN.md` (the master plan)
**Read second:** `docs/DECISIONS.md` (latest entries from 2026-04-08)
**Read third:** `data/stats/30k_journal.md` (session history)

## Goal
Archive existing wine/producer canonical data to `archive_*` tables. Create fresh empty canonical tables with updated schema. Reference tables and staging tables stay untouched.

## What to do

### Step 0: Pre-flight
- Verify LWIN licensing at liv-ex.com/lwin. If restrictive, STOP and flag.
- Disable Render frontend auto-deploy (pause service or change deploy branch).
- Capture exact row counts for ALL content tables before any changes.

### Step 1: Dependency scan (DO THIS BEFORE ANYTHING ELSE)
Query the database catalog to find EVERY object referencing content tables:
```sql
-- Views
SELECT table_name, view_definition FROM information_schema.views 
WHERE view_definition LIKE '%wines%' OR view_definition LIKE '%producers%';

-- RPC functions  
SELECT routine_name, routine_definition FROM information_schema.routines
WHERE routine_schema = 'public';

-- Triggers
SELECT trigger_name, event_object_table FROM information_schema.triggers
WHERE event_object_schema = 'public';

-- FK constraints
SELECT tc.constraint_name, tc.table_name, ccu.table_name AS references_table
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY';

-- RLS policies
SELECT tablename, policyname FROM pg_policies WHERE schemaname = 'public';

-- Indexes
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'public';
```
Document the COMPLETE list. Every item must be accounted for in the rebuild.

### Step 2: Create Supabase branch for testing
Use the Supabase MCP to create a development branch. All DDL goes there first.

### Step 3: Rename canonical content tables to archive_*
See `docs/30K_PLAN.md` Phase 0 for the full list. Key tables:
- wines → archive_wines
- producers → archive_producers  
- wine_vintages → archive_wine_vintages
- wine_grapes → archive_wine_grapes
- wine_vintage_prices → archive_wine_vintage_prices
- wine_vintage_scores → archive_wine_vintage_scores
- external_ids → archive_external_ids
- (see plan for full list of ~20 tables)

### Step 4: Recreate fresh canonical tables
Same schema as originals PLUS new columns on wines:
- `display_name TEXT`
- `confirmation CHAR(1) CHECK (confirmation IN ('D','C','B','A'))`
- `completeness SMALLINT DEFAULT 0`
- `enrichment SMALLINT DEFAULT 0 CHECK (enrichment IN (0,1,2))`
- `identity_complete BOOLEAN DEFAULT FALSE`
- `blend_complete BOOLEAN DEFAULT FALSE`
- `appellation_confirmed BOOLEAN DEFAULT FALSE`
- `grapes_confirmed BOOLEAN DEFAULT FALSE`
- `color_confirmed BOOLEAN DEFAULT FALSE`

Create new tables:
- `data_provenance` (see schema in plan)
- `ai_suggestions` (see schema in plan)

Note: `completeness` is batch-recalculated by grade_calculator.py, not a trigger.

### Step 5: Rebuild everything from dependency scan
- Recreate all views (wine_detail_view, etc.)
- Recreate all RPC functions (search_catalog, search_wines, etc.)
- Recreate all triggers (search vectors, set_updated_at, etc.)
- Recreate all RLS policies
- Recreate all indexes

### Step 6: Build thirty_k_validate.py
The validation script that runs after every session. See plan for full spec.

### Step 7: Verify on branch
- Run dashboard — should show zeros for content, correct for reference
- Run validation script with `--session phase_0`
- Test search_catalog RPC returns empty (not error)
- Test wine_detail_view returns empty (not error)

### Step 8: If branch looks good, merge to production

## Do NOT
- Touch reference tables
- Touch staging tables
- Touch appellation data (rules, grapes, soils, vintages, aliases)
- Touch geographic boundary data
- Delete any data (rename only)
- Skip the dependency scan
- Rush — test everything on the branch first

## Validation (run before ending session)
```
S1.0:  Dependency scan documented — every view/RPC/trigger/FK/policy/index listed
S1.1:  archive_wines row count = [pre-archive count]
S1.2:  archive_producers row count = [pre-archive count]
S1.3:  wines table exists, 0 rows
S1.4:  producers table exists, 0 rows
S1.5:  data_provenance table exists, 0 rows
S1.6:  ai_suggestions table exists, 0 rows
S1.7:  All new columns exist on wines
S1.8:  search_catalog RPC returns empty (not errors)
S1.9:  wine_detail_view returns empty (not errors)
S1.10: Render frontend deploy is disabled
S1.11: Enrichment Edge Function noted/disabled
S1.12: Dashboard runs clean
S1.13: Validation script runs without errors
S1.14: Reference table row counts unchanged
S1.15: Staging table row counts unchanged
```

## After completing
- Run validation: `python -m pipeline.analyze.thirty_k_validate --session phase_0`
- Run dashboard: `python -m pipeline.analyze.thirty_k_dashboard`
- Update `data/stats/30k_sessions.json` (session 1 → done)
- Update `memory/30k_status.md`
- Append to `data/stats/30k_journal.md`
- Update `docs/30K_PLAN.md` Phase 0 exit criteria
- Commit and push
