# Session 3: Batch 0 — The Prototype

**Model:** Opus
**Estimated AI cost:** ~$1
**Read first:** `docs/30K_PLAN.md`, `docs/IDENTITY_RULES.md`, `data/stats/30k_journal.md`

## Gate Check (DO FIRST)
1. Verify Session 2 is `"done"` in `data/stats/30k_sessions.json`. If not, STOP.
2. Mark Session 3 as `"in_progress"` in `data/stats/30k_sessions.json`.
3. Run dashboard: `python -m pipeline.analyze.thirty_k_dashboard`
4. Confirm: producers = 0, wines = 0 (clean slate from Phase 0).

## Goal
48 producers, ~150-250 wines through the FULL pipeline end-to-end. Stress-test every identity pattern. Small enough to review every single wine.

This is the most important session in the plan. If the prototype works, we scale. If it doesn't, we redesign.

## The 48 Producers

See `docs/IDENTITY_RULES.md` Section 8 for the full verified roster with staging source counts. Categories:

| Category | Count | Key producers |
|---|---|---|
| Bordeaux châteaux | 5 | Margaux, Lafite, Mouton, Haut-Brion, Latour |
| Napa | 5 | Ridge, Caymus, Stag's Leap, Opus One, Silver Oak |
| Italian DOCG | 5 | Conterno, Antinori, Gaja, Masseto, San Guido |
| Spanish Rioja | 5 | López de Heredia, La Rioja Alta, CVNE, Muga, Riscal |
| German Riesling | 5 | Prüm, Dr. Loosen, Dönnhoff, Egon Müller, Fritz Haag |
| Australian | 5 | Penfolds, Henschke, Torbreck, Tyrrell's, Yalumba |
| Grocery | 5 | Barefoot, Josh Cellars, Meiomi, 19 Crimes, Yellow Tail |
| Négociant | 5 | Louis Jadot, Louis Latour, Joseph Drouhin, Bouchard, Champy |
| Single-wine | 4 | Dominus, Screaming Eagle, Harlan, Scarecrow |
| Portfolio | 4+1 | Treasury*, Constellation*, Gallo*, Kendall-Jackson, Duckhorn |

*Portfolio companies: use subsidiary brand names as producers (that's what's on the label), not the parent company. See IDENTITY_RULES.md Section 8 note.

## Process (follow the iterative dry-run loop)

### Step 1: Build producers (Phase 1 for this batch)
- For each of the 48 producers:
  - Find in LWIN staging (`source_lwin.producer_name`)
  - Find in TTB permits (if fetched) or TTB COLA (`source_ttb_colas.brand_name`)
  - Find in Wikidata (if fetched)
  - Cross-reference: do names match across sources?
  - Insert to canonical `producers` table with clean name, country, region
  - Log to `data_provenance` with all source links
  - Assign confirmation grade (C if 1 source, B if 2+, A if 3+)
- **Dry run first on 5 producers (one per major category), review, then do the rest**

### Step 2: Promote wines from staging
- For each producer, find their wines in staging tables
- Follow `docs/IDENTITY_RULES.md` Section 5 for staging→canonical matching
- Match staging wine names to build identity tuples
- Filter: only wines with at least ONE commercial signal (UPC, price, score, LWIN ID, TTB COLA)
- **Dry run on Ridge (many wines, good test case), review, then scale to all 48**

### Step 3: Clean cuvée
- Follow `docs/IDENTITY_RULES.md` Section 3 (cuvée extraction algorithm)
- Apply country-specific rules from Section 2
- Strip producer prefix, appellation, grape, vintage from name
- Remainder = cuvée (or NULL)
- **Review every single cuvée value — this is the hardest part**

### Step 4: Build display names
- Follow `docs/IDENTITY_RULES.md` Section 2 (country-specific patterns)
- Use the `build_display_name` function scaffold in `pipeline/identity/build_display_name.py`
- Verify ≥ 3 distinct country patterns in the output

### Step 5: Promote depth from staging
- Vintages (real years only, NV only for actual NV)
- Grapes with percentages where known, `blend_complete` where full blend known
- Color (from appellation rules cascade or staging source)
- Geography cascade: appellation → region → country
- ABV, prices, scores where available
- Label images where COLA linked
- Log ALL to `data_provenance`

### Step 6: Calculate grades
- Run grade_calculator: confirmation, completeness (0-11), enrichment, identity_complete
- Review the distribution

### Step 7: Review everything
- Look at every wine. Is the data correct? Is the display name right?
- Check the 5 named spot-check wines specifically
- Does it feel like what a user should see?

## Named Spot-Check Wines (verify these specifically)
1. **Château Margaux** — Bordeaux pattern: display name should be appellation-first, cuvée NULL
2. **Ridge Monte Bello** — Napa pattern: cuvée "Monte Bello" + grape + AVA
3. **Barefoot Cabernet Sauvignon** — Grocery pattern: varietal from name, no specific appellation
4. **Antinori Tignanello** — Italian pattern: cuvée "Tignanello" + appellation
5. **Louis Jadot Gevrey-Chambertin** — Négociant pattern: appellation as identity, cuvée NULL

## Do NOT
- Import wines without commercial signals (no "just has a name")
- Create phantom NV vintages on vintage wines
- Set grape percentage=100 without source confirmation
- Skip the dry-run loop (5 producers first, then scale)
- Skip provenance logging
- Rush — this is the prototype, quality matters more than speed

## Validation (run before ending session)

**Universal checks (U1-U12):**
```
U1:  No duplicate producers (same normalized name)
U2:  No duplicate wines (same producer + normalized cuvée)
U3:  All wines have valid producer FK
U4:  All wine_grapes link to valid wines + valid grapes
U5:  Completeness scores recalculated and match actual data
U6:  No geographic hierarchy violations
U7:  No unresolved color/grape conflicts
U8:  All new data has provenance logged (every wine ≥ 2 entries)
U9:  No confirmation grade without external source
U10: Reference table row counts unchanged
U11: Staging table row counts unchanged
U12: Budget: cumulative AI spend documented
```

**Session-specific checks (S3.1-S3.15):**
```
S3.1:  48-52 producers in producers table
S3.2:  Wine count reasonable (~150-250)
S3.3:  Every wine has display_name (not NULL or empty)
S3.4:  Display names show ≥ 3 distinct country patterns
S3.5:  Every wine has confirmation ≥ C
S3.6:  Every producer has country
S3.7:  ≥ 80% of producers have region
S3.8:  Zero color/grape conflicts
S3.9:  Named spot-check wines all correct:
         Château Margaux — Bordeaux pattern
         Ridge Monte Bello — Napa pattern
         Barefoot Cabernet Sauvignon — grocery pattern
         Antinori Tignanello — Italian pattern
         Louis Jadot Gevrey-Chambertin — négociant pattern
S3.10: Average completeness ≥ 5/11
S3.11: ≥ 30% of wines have identity_complete = TRUE
S3.12: Every wine has ≥ 2 provenance entries with valid source_ids
S3.13: No duplicate producers
S3.14: No duplicate wines within same producer
S3.15: Budget: AI spend for this session documented
```

**If ANY check fails, fix before ending the session.**

## End of Session (non-negotiable, all 8 steps)
1. Run validation: `python -m pipeline.analyze.thirty_k_validate --session batch_0`
2. Verify ALL checks pass
3. Update `data/stats/30k_sessions.json` — mark session 3 `"done"`, fill date + ai_spend
4. Append Session 3 entry to `data/stats/30k_journal.md`
5. Update `memory/30k_status.md` — point to Session 4
6. Update `docs/30K_PLAN.md` — check off Session 3 exit criteria
7. Commit and push
8. Do NOT end until steps 1-7 are complete
