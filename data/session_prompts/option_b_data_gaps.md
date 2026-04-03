# Session: Finish Data Gaps (Option B)

Read CLAUDE.md first. This is a focused data work session.

## Goals (in order)

### 1. TABC Refresh
The Texas TABC API has 201K records vs our 183K (+18K new). Fetch and load:
```
python -u -m pipeline.load.tabc_staging
```
Then run batch_matcher against the refreshed data. The TABC Socrata API endpoint is `data.texas.gov/resource/2cjh-3vae.json?type=WINE`.

### 2. Grape Promotion (full catalog)
Run ttb_grape_promote with NO limit on ALL wines — not just Tier C. The script was recently migrated to psycopg2 and removes the created_at filter. Just run:
```
python -u -m pipeline.promote.ttb_grape_promote
```
Report: grape links inserted, unresolved names.

### 3. Phase B Remainder
~600 producers still need wine creation. The script is resume-safe:
```
python -u -m pipeline.promote.phase_b_wines
```
(Check if this script exists — may need to find the actual command.)

### 4. COLA ID + Vintage Promotion
Run cola_depth for any new wines from steps 2-3:
```
python -u -m pipeline.promote.cola_depth
```

### 5. Re-measure Readiness
Run 200-sample Spec's mystery shopper against the expanded catalog. Previous score: 28/100. Should be significantly higher after this session's work.

Write results to `data/stats/{date}.json`.

## Rules
- Run scripts sequentially (parallel Supabase calls crash)
- Each batch_matcher source runs in its own process
- Use psycopg2 (get_conn()) — scripts already migrated
- Commit at the end with updated CLAUDE.md
