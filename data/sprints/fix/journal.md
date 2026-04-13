# Sprint 3 — Fix — Journal

## S3.1 (2026-04-12)
**Track:** 1 — Clean house
**Goal:** Delete cruft, deduplicate docs, prune memory, drop dead schema

### Work log
- Dropped 13 xwines_* tables + 2 temp tables (_tmp_wine_match, producer_dedup_staging)
- Dropped wines.lwin column (15 rows vs 119,889 in external_ids), recreated wine_detail_view
- Archived 5 docs to docs/reference/ (30K_PLAN, PATH_A_ROLLBACK, AUDIT, MERGE_STRATEGY, BACKLOG)
- Deleted 2 docs (WORKFLOW.md, ENRICHMENT.md) + 2 empty dirs (architecture/, pipelines/)
- Pruned data/session_prompts/ from 8 to 1 (kept cron_loop_template.md)
- Pruned data/stats/ (3 old schema snapshots, 2 ad-hoc files)
- Deleted 6 abandoned fetch_legal_sources_batch scripts
- Archived pipeline/vivino/ (14 files) to scripts_archive/vivino/
- Deleted frontend/src/pages/LandingPage.tsx (dead code, no references)
- Deleted 8 stale memory files, rewrote MEMORY.md index (19→11 files)
- Updated project_quality_before_enrichment.md with simplified roadmap
- Deduped source_systembolaget (12,646→6,298) and source_lcbo (7,030→3,494), added unique constraints
- Stripped all hardcoded DB counts from CLAUDE.md (711→384 lines)
- Removed ~180-line "Prior Sprint 2 headline" section (superseded by synthesis.md)
- Removed ~50-line completed session logs (moved to HISTORY.md)
- Fixed stale doc references throughout

### Done criteria met
- [x] xwines_* tables don't exist
- [x] Riddler task doesn't exist
- [x] docs/ has ≤8 files + reference/
- [x] No hardcoded DB counts in CLAUDE.md
- [x] Memory files ≤11 (10 + index)
- [x] data/session_prompts/ has 1 file
- [x] data/stats/ pruned
