# Producer Dedup Pipeline — Claude Code Instructions

## Context
Loam v2 wine database (Supabase project: `vgbppjhmvbggfjztzobl`). We have 30,684 producer names extracted from 100,646 wine candidates. After running pg_trgm fuzzy matching, we have **8,208 pairs** of producer names that are similar enough to potentially be duplicates. These pairs are stored in the `producer_dedup_pairs` table with `verdict = 'pending'`.

## What to do
Run `producer_dedup_pipeline.py` which:
1. Reads pending pairs from `producer_dedup_pairs` table
2. Batches them (50 pairs per API call) to Claude Haiku
3. Haiku decides: merge (same producer) or keep_separate (different producers)
4. Writes verdicts back to the table

## Setup
```bash
pip install anthropic supabase
export SUPABASE_URL=https://vgbppjhmvbggfjztzobl.supabase.co
export SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnYnBwamhtdmJnZ2ZqenR6b2JsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI1ODU1NDYsImV4cCI6MjA4ODE2MTU0Nn0.KHZiqk6B7XYDnkFcDNJtMIKoT-hf7s8MGkmpOsjgVDk
# ANTHROPIC_API_KEY should already be in your environment
```

## Run
```bash
# Test with 50 pairs first
python3 producer_dedup_pipeline.py --limit=50

# If that works, run the full pipeline
python3 producer_dedup_pipeline.py
```

## Expected
- ~165 API calls to Haiku
- ~$0.73 in Haiku costs
- Takes ~3-5 minutes (0.5s delay between batches)
- Most pairs will be "keep_separate" — the data is quite clean

## Schema: producer_dedup_pairs
| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | PK |
| name_a | TEXT | First producer name |
| name_b | TEXT | Second producer name |
| country | TEXT | Shared country |
| similarity | NUMERIC(4,3) | pg_trgm similarity score |
| wines_a | INTEGER | Wine count for producer A |
| wines_b | INTEGER | Wine count for producer B |
| verdict | TEXT | 'merge', 'keep_separate', or 'pending' |
| verdict_source | TEXT | 'haiku', 'auto_exact', 'auto_high_sim', 'manual' |

## After completion
Come back to this Claude chat and say "dedup pipeline complete" — we'll verify the results and proceed to creating the actual producer records.
