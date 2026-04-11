# 30K Plan Status

**CURRENT PHASE: Session 13 COMPLETE — LWIN long-tail sweep + dedup + TTB match**
**STATUS: Active wines 51,614 → 155,623 (~3x). Producers 2,530 → 10,676 (~4x). Fort Ross gap closed. TTB re-linked to current canonical (80K TTB rows wine-linked). Grade C voice-rules fix still deferred.**
**BLOCKER: Grade B on-demand Edge Function still disabled via feature flag. Enrichment quality fix is the remaining blocker for Phase 4 frontend resume.**

## Next Session's Goal
**Session 14: Enrichment quality fix OR frontend resume**
Two realistic paths:
1. Fix Grade C voice rules (deferred from Session 12) — loosen the anti-hedging prompt to preserve editorial voice, re-run Stage 2 validation, then decide on full corpus re-enrichment (~$30-80)
2. Resume Phase 4 frontend work — wire the Edge Function, add wine_insights JOIN to API views, test with real data. Grade B already works; Grade C is broken but only 4,857 wines out of 155,623 depend on it.

User preference determines which. Option 2 unblocks the biggest product win (real wine pages for the new 100K LWIN long-tail wines) even if Grade C narrative quality isn't yet where we want it.

## Do NOT
- Expose Grade B to users until enrichment quality fix lands
- Modify identity rules without user approval (Session 2 spec is agreed)
- Skip dry-run loop on any major step
- Touch archive_* tables (preserved, never modify)
- Touch reference tables, staging tables, or appellation data
- Create cron loops without explicit user request
- Cap per-producer wine count (user explicitly rejected this)

## Quick Progress
- [x] Sessions 1-13 complete
- [ ] Enrichment quality fix (BLOCKER for Grade B)
- [ ] Phase 4: Frontend resume

## Key Numbers (updated Session 13 complete)
- Producers: **10,676** (2,530 → +8,146 from LWIN long-tail)
- Active wines: **155,623** (51,614 → +104,009, ~3x growth)
- Soft-deleted dupes: 947 (718 from Session 13 dedup + 229 from prior)
- wine_vintages: **83,531** (+15,371 from COLA depth)
- external_ids: **119,889 LWIN** + **253,301 COLA** (+12,165 fresh COLA this session)
- TTB linked: **83,183 source_ttb_colas rows** now point at current canonical wines
- Fort Ross: **28 wines** (15 → 28, sweep picked up additional LWIN entries)
- **Data grade: B=105, C=4,857, D=155, F=~150K** (mostly the new LWIN long-tail wines — identity-only F-grade)
- Josh Test: query DB (not re-run this session)
- **Budget spent: ~$23.33 / $175** (13.3%) — Session 13 added $0.34 (Haiku dedup classifier)

## L1+L3 Enrichment Design (Session 11 validated)
- **L1 retrieval-grounded generation:** structured "facts packet" passed to prompt — wine identity + `appellation_rules` legal text + `wine_vintages` chemistry + unknowns list + pre-fetched comparables from OUR DB. Explicit "do not invent" + "safe inferences allowed" list.
- **L2 per-field constraints:** `ai_comparable_wines` is DB-only (deterministic pre-fetch, 3 matches by primary grape + appellation + price); `ai_terroir_expression` leans on `appellation_rules`; `ai_vinification_summary` uses real winemaker_notes when available
- **L3 fact-check pass** (Haiku): validates claims against ground truth, retries once if flagged
- **Budget estimate:** ~$30 (Haiku C + Sonnet B + Haiku L3) to ~$80 (all Sonnet) for full 4,962-wine corpus re-enrichment
- **Progressive rollout:** Stage 1 (34 failing samples) → Stage 2 (500-wine vertical slice) → Stage 3 (full corpus)

## Enrichment Pipeline
- **Sequential** (calibration/small runs): `pipeline/enrich/batch_enrich.py`
- **Batch API** (preferred for bulk): `pipeline/enrich/batch_api.py`
  - 50% cost discount
  - Submit thousands of requests at once
  - Processes in minutes to hours
  - No rate limiting or babysitting
- Grade C (Haiku): hook, style, sensory, comparables. ~$0.003/wine with batch.
- Grade B (Sonnet): full narrative, terroir, vinification, food pairings. ~$0.009/wine with batch.

## Batch Submission Workflow
```bash
# 1. Submit
python -m pipeline.enrich.batch_api submit --grade C --limit 5000
python -m pipeline.enrich.batch_api submit --grade B --limit 100

# 2. Check status (ends in minutes to hours)
python -m pipeline.enrich.batch_api status --batch-id msgbatch_xxx

# 3. Process results into DB
python -m pipeline.enrich.batch_api process --batch-id msgbatch_xxx --grade C
```

## Known Issues
- 169 Grade C wines failed with JSON parse errors (3.4%) — Haiku sometimes returns malformed JSON. Can retry in Session 10 with more defensive parsing or a re-run.
- comparable_wines field sometimes comes back as JSON object instead of plain text — normalization handles this.

## IMPORTANT: At session end
1. Update THIS FILE
2. Append to `data/stats/30k_journal.md`
3. Update `data/stats/30k_sessions.json`
4. Update `data/sessions.md`
5. Commit and push
