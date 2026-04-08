# The 30K Plan

**Goal:** Quality wine data for the wines American enthusiasts actually drink. When Loam finds a wine, the data is excellent — correct producer, correct grapes (complete blend), correct appellation, correct display name. Quality over coverage: 85% find rate with great data beats 95% with questionable data.

**Target:** 85% Josh Test pass rate. 10K minimum wines, likely 20-25K. Do not optimize for the number.
**Price tier coverage:** $0-10: 50% | $10-30: 80% | $30-100: 100% | $100-250: 80% | $250+: 60%
**Budget:** $175 AI (includes buffer for iteration). All data sources free/open.
**Started:** 2026-04-08
**Status:** Phase 0 DONE — Session 2 ready to start

---

## Principles

1. **Quality over coverage.** 10K excellent wines beats 500K garbage.
2. **NV only for actual NV wines.** NV Champagne, NV Port, bag-in-box. Not vintage wines whose source didn't capture the year.
3. **`name` = cuvée, nullable.** Display name is a computed column, country-aware. Many wines have no cuvée — that's correct.
4. **Geographic hierarchy is derived.** Appellation → region → country. If no appellation, region → country. If no region, just country. All valid states. `appellation_confirmed` boolean distinguishes "confirmed no appellation" from "unknown."
5. **Three-metric grading.** Confirmation (identity verified?), Completeness (how many facts?), Enrichment (AI content?). Independent metrics, different actions for each.
6. **Color consistent with grapes.** Contradictions must be resolved or explained (e.g., Blanc de Noirs).
7. **Grape links must be complete and accurate.** All grapes linked with correct percentages. `blend_complete` boolean tracks completeness. `grapes_confirmed` boolean distinguishes "confirmed these are all the grapes" from "we only found the primary." Percentage fills ALWAYS require source confirmation — no AI percentages.
8. **Producers first.** Can't fix wines without fixing producers.
9. **AI suggests, sources confirm.** Lean on Haiku/Sonnet as common sense — for matching, classification, gap identification, prioritization, cuvée identification. But AI suggestions NEVER go directly to canonical tables. Every write requires confirmation from a real source. AI + source agree = write. AI alone = log the suggestion, don't write.
10. **Nothing enters canonical without a real source.** Appellation rule cascades count as source-confirmed (legal documents). Staging data counts. AI confidence alone does not.
11. **Track provenance on every data point.** `data_provenance` audit table. Surgical rollbacks, never collateral damage.
12. **Reusable pipeline library.** Composable, idempotent routines with `--dry-run` defaults.
13. **UPC scanning works.** Core feature — scan barcode, find wine.
14. **TTB labels + COLA links are differentiating.** Link where possible, not required for all.
15. **Measure after every phase.** Dashboard, WineTest, Josh Test — not vibes.
16. **Never be afraid to start from scratch.** Don't fall in love with existing work.

---

## Grading System (Three Metrics)

Having good data is really the main part.

### 1. Confirmation (is this wine real and correctly identified?)

| Grade | Name | Criteria |
|-------|------|----------|
| D | Unverified | Has name + producer. No external validation. |
| C | Matched | Linked to 1 external source (COLA, LWIN, or UPC). |
| B | Confirmed | 2+ independent sources agree, OR AI + source cross-validated. |
| A | Verified | Human audit or 3+ sources with perfect agreement. |

### 2. Completeness (0-11 field count)

| Field | What it means |
|-------|--------------|
| Producer | Validated producer linked |
| Color | red/white/rosé/orange (color_confirmed = TRUE) |
| Grapes | At least one grape linked (grapes_confirmed = TRUE for full credit) |
| Appellation | Linked OR appellation_confirmed = TRUE with NULL (confirmed none) |
| Region | Derived from appellation, or direct assignment |
| Country | Derived from region |
| Vintage | At least one real vintage year (not gated — wines can exist without) |
| UPC | Barcode in external_ids |
| Price | At least one price |
| Score | At least one critic/competition score |
| Label Image | At least one vintage has label_image_url |

**`identity_complete` boolean:** TRUE when producer + color + grapes + (appellation OR region) + country are all confirmed. This is the "is the core identity done?" check.

### 3. Enrichment (AI-generated content)

| Grade | Name | Content |
|-------|------|---------|
| 0 | None | No AI content. |
| 1 | Basic | AI-guided gap fills confirmed by sources. |
| 2 | Full | Sonnet synthesis (tasting insights, terroir, vintage analysis). |

### Consumer-ready = Confirmation B + identity_complete = TRUE

---

## Identity Model

Wine identity is a **tuple**: **Producer + Cuvée (nullable) + Grape(s) + Appellation + Color + Classification**

### Display Name (computed, country-aware)

Build for ALL major wine-producing countries. This is cost of doing business — invest the time to get it right. Detailed rules to be designed in Session 2 (identity-by-country).

| Country/Style | Pattern | Example |
|---------------|---------|---------|
| **France** | Producer, Appellation [Classification] [Cuvée] | Domaine Leflaive, Puligny-Montrachet Premier Cru Les Folatières |
| **USA** | Producer [Cuvée] Grape, Appellation | Ridge Monte Bello Cabernet Sauvignon, Santa Cruz Mountains |
| **Spain** | Producer [Cuvée] [Classification], Appellation | López de Heredia Viña Tondonia Reserva, Rioja |
| **Italy** | Producer [Cuvée], Appellation [Classification] | Giacomo Conterno Monfortino, Barolo Riserva |
| **Germany** | Producer [Vineyard] Grape [Classification], Appellation | Joh. Jos. Prüm Wehlener Sonnenuhr Riesling Spätlese, Mosel |
| **Australia** | Producer [Cuvée] Grape, Region | Penfolds Grange Shiraz, South Australia |
| **New Zealand** | Producer [Cuvée] Grape, Region | Cloudy Bay Sauvignon Blanc, Marlborough |
| **Argentina** | Producer [Cuvée] Grape, Region | Catena Zapata Malbec, Mendoza |
| **Chile** | Producer [Cuvée] Grape, Appellation | Concha y Toro Don Melchor Cabernet Sauvignon, Puente Alto |
| **South Africa** | Producer [Cuvée] Grape, Region | Kanonkop Pinotage, Stellenbosch |
| **Portugal** | Producer [Cuvée] [Classification], Appellation | Niepoort Redoma, Douro |
| **Austria** | Producer Grape [Classification], Appellation | F.X. Pichler Riesling Smaragd, Wachau |
| **Greece** | Producer [Cuvée] Grape, Appellation | Gaia Thalassitis Assyrtiko, Santorini |
| **Fallback** | Producer [Cuvée], Appellation [Grape] | Catches everything else |

`display_name` is a stored generated column. `wines.name` holds only the cuvée.

---

## Lessons Learned (from prior work)

### What worked
- **Source-based, validated insertion** (appellation rules from legal PDFs: 1,165 rules, zero errors, 32 batches)
- **Deterministic cascades** (color from appellation rules, region from appellation — free, correct, scalable)
- **Deterministic identity signals** (UPC barcodes: 106K from label scans — zero judgment, zero hallucination)
- **Measurement** (WineTest gave honest 56/100 when we thought we were doing well)
- **Single-focus sessions** (appellation rules sessions, barcode scanning sessions — excellent results)

### What failed
- **Inference on canonical data** (18 operations reverted, 44K legit records lost as collateral — no provenance)
- **Aggressive bulk creation** (TTB Phase B: 170K empty shells, junk producers)
- **Fixing data in place** (17 rounds of follow-ups = whack-a-mole)
- **AI as data creator** (Haiku grape extraction: 2% hit rate)
- **Multi-track cron loops** (2 of 3 tracks wasted — gap analysis would have caught it)

### The Iterative Dry-Run Loop

**Core process for every major step. Not optional.**

```
1. DRY RUN on small sample (10-50 records)
2. AI AUDIT — Haiku/Sonnet reviews output for common sense
3. LEARN — what worked? what broke? what surprised us?
4. RESTART — another dry run incorporating learnings
5. ITERATE until we trust the output
6. SCALE — only then run on the full dataset
```

We are not in a hurry. If a dry run looks wrong, throw it away and try again.

---

## Risks

| # | Risk | Likelihood | Impact | Mitigation | Owner ruling |
|---|------|-----------|--------|------------|--------------|
| 1 | **Wine selection bias** — miss what Americans actually drink | HIGH | HIGH | Price-tier weighting. Josh Test. Retail data. | Quality > find rate. 10K minimum. |
| 2 | **LWIN ↔ COLA cross-link fails** | MEDIUM | HIGH | Dry run first. Don't depend on it. | UPC is the reliable join. |
| 3 | **Cuvée cleaning is subjective** | MEDIUM | MEDIUM | Dedicated design session. Dry-run loop. | Invest real time. Known hard problem. |
| 4 | **Fewer wines than expected** | MEDIUM | LOW | 10K minimum. Don't force. | Quality > quantity. |
| 5 | **Display name edge cases** | MEDIUM | MEDIUM | Build for all countries. Test 50 per country. | Cost of doing business. |
| 6 | **AI hallucination** | MEDIUM | HIGH | Suggest+confirm only. Source required. | Agreed. |
| 7 | **Session continuity drift** | HIGH | HIGH | Dashboard, memory file, session prompts. | Biggest human risk. Rigorous management. |
| 8 | **Archive migration breaks views/RPCs** | LOW | HIGH | Branch DB first. Enumerate all dependencies. | Standard engineering. |
| 9 | **Staging→canonical matching is the same old fuzzy problem** | MEDIUM | HIGH | Scoped within validated producers. Dry-run loop. | This is where the work is. |

---

## Session Management

### Persistent Dashboard
`python -m pipeline.analyze.thirty_k_dashboard --watch` — keep open in PowerShell.

### Auto-loading Memory File
`memory/30k_status.md` — loaded every session. Current phase, what to do, what not to do.

### Session Prompt Files
`data/session_prompts/30k_phase_N.md` — designed 1-2 phases ahead, not all at once.

### Model Selection

| Session | Model | Rationale |
|---------|-------|-----------|
| 1: Phase 0 | Sonnet | Mechanical DDL |
| 2: Design | **Opus** | Identity rules are foundational |
| 3: Batch 0 | **Opus** | Prototype — must be right |
| 4: Review | **Opus** | Judgment on what worked/broke |
| 5+: Batches | Sonnet (Opus for hard problems) | Execution with established patterns |
| Final: Josh Test | **Opus** | Fresh eyes on the whole thing |

### Session Discipline
- Run dashboard at start and end
- Run validation checklist at end (before moving to next step)
- Update memory file at end
- Update journal at end (`data/stats/30k_journal.md`)
- Update plan doc exit criteria at end
- Commit at end
- One batch per session — don't rush

### Validation (two layers, run after every session)

Script: `pipeline/analyze/thirty_k_validate.py --session <session_id>` (build in Phase 0)

**Layer 1: Universal checks (always run, catch regressions):**
```
U1: No duplicate producers (same normalized name)
U2: No duplicate wines (same producer + same normalized cuvée)
U3: All wines have valid producer FK
U4: All wine_grapes link to valid wines + valid grapes
U5: completeness scores recalculated and match actual data
U6: No geographic hierarchy violations
U7: No unresolved color/grape conflicts
U8: All new data has provenance logged (every wine ≥ 2 provenance entries)
U9: No confirmation grade without external source
U10: Reference table row counts unchanged from baseline
U11: Staging table row counts unchanged from baseline
U12: Budget tracking — cumulative AI spend vs estimate
```

**Layer 2: Session-specific checks (verify what just happened):**

**Session 1 (Phase 0: Archive):**
```
S1.0: Dependency scan documented — complete list of views, RPCs, triggers, FKs, policies, indexes
        that reference content tables. Every item accounted for in the rebuild.
S1.1: archive_wines row count = [exact pre-archive count, captured before rename]
S1.2: archive_producers row count = [exact pre-archive count]
S1.3: wines table exists, 0 rows
S1.4: producers table exists, 0 rows
S1.5: data_provenance table exists, 0 rows
S1.6: ai_suggestions table exists, 0 rows
S1.7: All new columns exist on wines (confirmation, completeness, enrichment, identity_complete, blend_complete, appellation_confirmed, grapes_confirmed, color_confirmed, display_name)
S1.8: search_catalog RPC returns empty results (not errors)
S1.9: wine_detail_view returns empty (not errors)
S1.10: Render frontend deploy is disabled
S1.11: Enrichment Edge Function noted/disabled
S1.12: Dashboard runs clean (zeros for content, correct for reference)
S1.13: Validation script itself runs without errors
```

**Session 2 (Identity Design):**
```
S2.1: docs/IDENTITY_RULES.md exists with rules for 13+ countries
S2.2: Josh Test sample list exists (200-300 wines, price-tier weighted)
S2.3: Josh Test sample viability: ≥ 50% of sample wines found in staging data
        (if < 50%, adjust sample or flag source gap before proceeding)
S2.4: Pipeline script files scaffolded (count matches plan)
S2.5: All 50 Batch 0 producers verified in staging (if any missing, substitutes documented)
S2.6: LWIN license verified — if restrictive, fallback plan documented
S2.7: Label regulation rule (27 CFR 4.23) documented in identity rules
S2.8: Cuvée extraction algorithm documented with edge cases
S2.9: Junk producer criteria defined explicitly:
        names < 3 chars, bare numbers, LLC/Inc/Corp, known bad patterns
```

**Session 3 (Batch 0: Prototype):**
```
S3.1: 48-52 producers in producers table (not exactly 50 — merges may reduce)
S3.2: Wine count reasonable (~150-250)
S3.3: Every wine has display_name that is not NULL or empty
S3.4: Display names show ≥ 3 distinct country patterns (not all identical format)
S3.5: Every wine has confirmation ≥ C
S3.6: Every producer has country
S3.7: ≥ 80% of producers have region
S3.8: Zero color/grape conflicts
S3.9: Named spot-check wines all correct:
        - Château Margaux (Bordeaux pattern: appellation-first, no cuvée)
        - Ridge Monte Bello (Napa pattern: cuvée + grape + AVA)
        - Barefoot Cabernet Sauvignon (grocery pattern: varietal, no appellation)
        - Antinori Tignanello (Italian pattern: cuvée + appellation)
        - Louis Jadot Gevrey-Chambertin (négociant pattern: appellation, no cuvée)
S3.10: Average completeness ≥ 5/11
S3.11: ≥ 30% of wines have identity_complete = TRUE
S3.12: Every wine has ≥ 2 provenance entries with valid source_ids
S3.13: No duplicate producers
S3.14: No duplicate wines within same producer
S3.15: Budget: AI spend for this session documented
```

**Session 4 (Batch 0 Review) — includes GO/NO-GO decision:**
```
S4.1: Mini Josh Test run — find rate and avg metrics documented
S4.2: All 10 country display name categories reviewed with examples logged
S4.3: Every cuvée value reviewed — correct/incorrect count documented
S4.4: Top 5 problems ranked by severity in journal
S4.5: Session 2 identity rule predictions compared to actual results

GO/NO-GO CRITERIA:
  GO if ALL of:
    - ≥ 80% of display names pass human spot-check
    - Average completeness ≥ 5/11
    - ≥ 90% of cuvées are correct or correctly NULL
    - No country category fails completely
  
  PARTIAL REDESIGN if:
    - 1-2 country categories need work but rest are solid
    - Fix those categories, re-run Batch 0 on the broken categories only
  
  FULL REDESIGN if ANY of:
    - < 60% of display names pass spot-check
    - Average completeness < 4/11
    - > 20% of cuvées are wrong
    - Fundamental pipeline logic is flawed

S4.6: Decision logged with rationale: GO / PARTIAL REDESIGN / FULL REDESIGN
S4.7: If GO: Batch 1 scope defined (producer count, wine target, price tiers)
S4.8: Budget: cumulative AI spend vs estimate
```

**Session 5 (Batch 1 part 1):**
```
S5.1: Producer count increased by ~500 (±10%)
S5.2: Wine count increased by ~2,500-3,500
S5.3: Regression spot-check: 5 random Batch 0 wines unchanged
S5.4: Josh Test sample: % now covered (expect improvement from Session 4)
S5.5: Average completeness ≥ 5/11 for new wines
S5.6: Price tier breakdown: what % of new wines are $30-100?
S5.7: Zero duplicate producers across Batch 0 + Batch 1
S5.8: Pipeline timing: how long per producer? Sustainable for Batch 2?
S5.9: Budget: cumulative AI spend vs estimate
```

**Session 6 (Batch 1 part 2):**
```
S6.1: Vintage coverage: X% of Batch 1 wines have ≥ 1 vintage
S6.2: Grape coverage: X% have grapes linked
S6.3: identity_complete count before vs after
S6.4: Completeness average improved (expect +2-3 points from depth)
S6.5: No new color/grape conflicts introduced
S6.6: appellation_confirmed set where applicable
S6.7: blend_complete set where full blend known
S6.8: Geographic hierarchy: zero mismatches
S6.9: Suggest+confirm yield: what % of Haiku suggestions had source confirmation?
        (if < 20%, flag — pipeline is producing too many unactionable suggestions)
S6.10: Budget: cumulative AI spend vs estimate
```

**Session 7 (Batch 2 part 1):**
```
S7.1: Producer count increased by ~2,000
S7.2: Wine count increased by ~10,000-12,000
S7.3: Regression spot-check: 20 random wines from Batches 0-1 unchanged
S7.4: Josh Test coverage improved (target: ≥ 60% of sample found)
S7.5: Price tier breakdown matches targets
S7.6: New producers validated against ≥ 1 external source
S7.7: Zero junk producer names (per Session 2 criteria)
S7.8: Cross-batch dedup check: no subtle duplicates across all batches combined
S7.9: Budget: cumulative AI spend vs estimate
```

**Session 8 (Batch 2 part 2):**
```
S8.1: Same depth checks as Session 6 but for Batch 2 wines
S8.2: Overall completeness average across all batches
S8.3: Overall identity_complete percentage
S8.4: Josh Test coverage check — are we on track for 85%?
S8.5: Budget: cumulative AI spend vs estimate
```

**Session 9 (Batch 3 / Gap fill):**
```
S9.1: Josh Test gaps specifically targeted — which missing wines were added?
S9.2: $0-10 tier coverage at target (50%)?
S9.3: $250+ tier coverage at target (60%)?
S9.4: Overall wine count in expected range
S9.5: Do we need additional sources for 85%? Flag if yes.
S9.6: Budget: cumulative AI spend vs estimate
```

**Session 10 (Enrichment sweep):**
```
S10.1: ai_suggestions table populated — counts per confidence band documented
S10.2: Source-confirmed writes have valid provenance (source_ids point to real staging rows)
S10.3: UPC lookup functional — test 50 random barcodes
S10.4: Label images linked where COLA exists (count)
S10.5: Prices promoted (count)
S10.6: Scores promoted (count)
S10.7: Budget: cumulative AI spend vs estimate (this is the most expensive session)
```

**Session 11 (Josh Test + Final Validation):**
```
S11.1: Josh Test find rate ≥ 85%
S11.2: Josh Test avg confirmation ≥ B
S11.3: Josh Test avg completeness ≥ 6/11
S11.4: Barcode spot-check: 100 UPC wines, ≥ 95% accuracy
S11.5: Display names correct: 50 per major country sampled
S11.6: No duplicate wines remaining
S11.7: Provenance coverage on all key fields
S11.8: Run full universal validation (U1-U12) one final time
S11.9: Document what the 85→95% push would require:
        which wines are missing, which sources would cover them, estimated cost
S11.10: Budget final: actual vs estimate, learnings for future budgeting
S11.11: Total wine count, producer count, grade distributions documented
```

**If ANY check fails, do NOT proceed to next session. Fix first.**

### Journal
`data/stats/30k_journal.md` — append-only narrative log. Every session records: what was attempted, what worked, what broke, surprises, numbers, decisions. This is institutional memory across sessions. Different from the plan doc (status tracking) and memory file (current state). The journal is the story of what happened and why.

---

## Plan Structure

**Sessions 1-4 are planned in detail. Everything after Session 4 is decided based on Batch 0 learnings.**

Two upfront sessions (waterfall), then iterative batches.

```
UPFRONT (waterfall):
  Session 1: Phase 0 — archive + schema
  Session 2: Identity-by-country design + pipeline architecture

ITERATIVE (batches):
  Session 3: Batch 0 — 50 producers, ~200 wines, full pipeline prototype
  Session 4: Review batch 0 + mini Josh Test + decide next steps
  
PLANNED BUT FLEXIBLE (details decided after batch 0):
  Sessions 5-6: Batch 1 — 500 producers, $30-100 tier core
  Sessions 7-8: Batch 2 — 2K producers, expanding tiers
  Session 9: Batch 3 if needed + enrichment sweep
  Session 10: Batch 3 continued or additional coverage
  Session 11: Full Josh Test + validation
```

---

## Phase 0: Archive & Fresh Start

**Status:** NOT STARTED
**Session:** 1 (Sonnet)
**AI cost:** $0

### Goal
Archive existing wine/producer canonical data. Create fresh empty canonical tables with updated schema.

### What Gets Archived (renamed to `archive_*`)

**Core content:** wines, producers, wine_vintages, wine_grapes, wine_vintage_prices, wine_vintage_scores, external_ids

**Related:** wine_label_designations, wine_farming_certifications, producer_farming_certifications, wine_vintage_formats, winemakers, producer_winemakers, match_decisions, entity_classifications, wine_insights, wine_vintage_tasting_insights, wine_lookups, vineyards + vineyard links

### What Stays As-Is

- **Reference tables:** appellations, grapes, grape_synonyms, regions, countries, varietal_categories, publications, retailers, attribute_definitions, tasting_descriptors, label_designations, farming_certifications, biodiversity_certifications, soil_types, bottle_formats
- **Appellation data:** appellation_rules, appellation_grapes, appellation_soils, appellation_vintages, aliases
- **Geographic data:** PostGIS boundaries, containment hierarchy
- **All staging tables** (source_*)

### New Schema

**`data_provenance` table:**
```sql
CREATE TABLE data_provenance (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    entity_id UUID NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT,
    source_type TEXT NOT NULL,  -- 'lwin', 'ttb_cola', 'cascade', 'manual', 'wikidata', 'osm'
    source_id TEXT,
    confidence NUMERIC(3,2),
    session_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_provenance_entity ON data_provenance(entity_type, entity_id);
CREATE INDEX idx_provenance_session ON data_provenance(session_id);
```

**`ai_suggestions` table:**
```sql
CREATE TABLE ai_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    entity_id UUID NOT NULL,
    field_name TEXT NOT NULL,
    suggested_value TEXT,
    confidence NUMERIC(3,2),
    model TEXT,              -- 'haiku', 'sonnet'
    source_checked BOOLEAN DEFAULT FALSE,
    source_confirmed BOOLEAN DEFAULT FALSE,
    session_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

**New columns on `wines`:**
- `display_name TEXT` — computed column (generation logic added in Phase 2)
- `confirmation CHAR(1) CHECK (confirmation IN ('D','C','B','A'))`
- `completeness SMALLINT DEFAULT 0` — 0-11 field count
- `enrichment SMALLINT DEFAULT 0 CHECK (enrichment IN (0,1,2))`
- `identity_complete BOOLEAN DEFAULT FALSE`
- `blend_complete BOOLEAN DEFAULT FALSE`
- `appellation_confirmed BOOLEAN DEFAULT FALSE`
- `grapes_confirmed BOOLEAN DEFAULT FALSE`
- `color_confirmed BOOLEAN DEFAULT FALSE`

### Process

1. **Verify LWIN licensing** — check liv-ex.com/lwin for commercial derivative use terms. If restrictive, pivot to TTB + Wikidata as backbone.
2. **Disable frontend auto-deploy** — pause Render service or change deploy branch. Prevent empty-table deploy.
3. **Dependency scan** — before touching any tables, query the database catalog to find EVERY object that references content tables:
   - `pg_views` / `information_schema.view_column_usage` → views referencing wines, producers, etc.
   - `pg_proc` + `pg_depend` → RPC functions (search_catalog, search_wines, etc.)
   - `pg_trigger` → triggers (search vector updates, set_updated_at, etc.)
   - `pg_constraint` → FK constraints from other tables
   - `pg_policy` → RLS policies on content tables
   - `pg_indexes` → indexes to recreate
   - Document the COMPLETE list before any rename. This prevents silent breakage.
4. Create Supabase branch for testing
5. Rename content tables to `archive_*`
6. Recreate fresh canonical tables with new columns
7. Create `data_provenance` and `ai_suggestions` tables
8. Build `thirty_k_validate.py` validation checklist script
9. Note: `completeness` column is batch-recalculated by `grade_calculator.py`, not a trigger. Stores last-calculated value.
10. Rebuild views, RPCs, search vectors, triggers on new tables
11. Update RLS policies
12. Run dashboard + validation to verify clean state
13. If branch looks good, merge to production

### Exit Criteria
- [ ] Archive tables exist with full data preserved
- [ ] Fresh canonical tables exist (empty, correct schema)
- [ ] `data_provenance` and `ai_suggestions` tables exist
- [ ] All new columns on wines table
- [ ] `search_catalog` RPC rebuilt and functional
- [ ] `wine_detail_view` and other views rebuilt
- [ ] Reference tables untouched and FK-valid
- [ ] Staging tables untouched
- [ ] RLS policies active
- [ ] Search vector triggers rebuilt
- [ ] Dashboard shows zeros for content, correct for reference
- [ ] Tested on branch before production

### Session Prompt
`data/session_prompts/30k_phase_0.md`

---

## Session 2: Identity-by-Country Design

**Status:** NOT STARTED
**Model:** Opus
**AI cost:** $0

### Goal
Design the identity rules, cuvée extraction logic, and display name patterns for every major wine country. This is the spec that all subsequent sessions follow.

### Deliverables

0. **Josh Test sample list:** Design the ~200-300 wine sample that the Josh Test will use. Source from retail bestsellers (Wine.com, Total Wine, Drizly), online wine lists, subscription boxes, grocery selection. Weight by price tier. **This sample guides wine selection in Batches 1-3** — if Josh Test wines aren't being covered, we're optimizing wrong.

1. **Identity rules per country:** For each of the 13+ countries in the display name table:
   - What components make up the wine identity?
   - What's required vs optional?
   - How to match/dedup wines from this country?
   - How to extract the cuvée from a raw wine name?
   - Display name assembly pattern with edge cases

2. **Cuvée extraction algorithm:** Detailed rules for:
   - When wine name = producer name (Opus One, Screaming Eagle)
   - When wine name = appellation (Puligny-Montrachet)
   - When wine name = grape (Cabernet Sauvignon)
   - When wine name includes all of the above concatenated (TTB-style)
   - The stripping order and decision tree

3. **The "confirmed" boolean patterns:** When to set appellation_confirmed, grapes_confirmed, color_confirmed to TRUE vs leave FALSE.

4. **Staging→canonical matching spec:** How do we match text fields in staging tables to canonical UUIDs? Per-producer clustering algorithm.

5. **Label regulation rule:** Varietal name in wine name = source-confirmed primary grape at ≥75% (27 CFR 4.23). Source_type = 'label_regulation'. Codify this for use in grape promotion.

6. **Pipeline scaffolding:** Create empty script files with function signatures, docstrings, input/output specs. Session 3 fills them in with working code.

7. **Verify Batch 0 producers in staging:** Query staging tables for all 50 planned producers. Swap any that are missing for similar producers in the same category.

### Exit Criteria
- [ ] Josh Test sample list created (~200-300 wines, price-tier weighted)
- [ ] Identity rules documented for 13+ countries
- [ ] Cuvée extraction algorithm designed with edge cases
- [ ] Display name SQL function spec'd
- [ ] Staging→canonical matching approach designed
- [ ] Label regulation rule codified
- [ ] Pipeline scripts scaffolded (empty files with signatures)
- [ ] Batch 0 producers verified in staging data
- [ ] All documented in `docs/IDENTITY_RULES.md`

---

## Session 3: Batch 0 — The Prototype

**Status:** NOT STARTED
**Model:** Opus
**AI cost:** ~$1

### Goal
50 producers, ~200 wines through the full pipeline end-to-end. Stress-test every identity pattern. Small enough to review every single wine.

### The 50 Producers (chosen to stress-test)

| Category | Count | Tests | Examples |
|---|---|---|---|
| Bordeaux châteaux | 5 | Appellation-driven, no cuvée | Margaux, Lafite, Mouton |
| Napa producers | 5 | Varietal-driven, cuvée common | Ridge, Caymus, Stag's Leap |
| Italian DOCG | 5 | Complex naming, classification | Conterno, Antinori, Gaja |
| Spanish Rioja | 5 | Reserva/Gran Reserva tiers | López de Heredia, La Rioja Alta |
| German Riesling | 5 | Prädikat, vineyard names | Prüm, Dr. Loosen, Donnhoff |
| Australian | 5 | Region-driven, brand tiers | Penfolds, Henschke, Torbreck |
| Grocery brands | 5 | High volume, $0-10 tier | Barefoot, Josh Cellars, Meiomi |
| Négociants | 5 | Same producer, many wines | Louis Jadot, Louis Latour |
| Single-wine producers | 5 | Wine = producer brand | Opus One, Dominus |
| Large portfolios | 5 | Corporate parent, many labels | Treasury Wine Estates |

### Process

Full pipeline for all 50:
1. **Producers:** Validate against LWIN + TTB + Wikidata. Insert to canonical.
2. **Wines:** Promote from staging. Match to producers. Extract cuvée. Dedup.
3. **Display names:** Generate using Session 2 rules. Review every single one.
4. **Depth:** Promote vintages, grapes, geography from staging. Cascade from appellation rules.
5. **Grade:** Assign confirmation, completeness, enrichment on all wines.
6. **Review:** Look at every wine. Is the data correct? Is the display name right? Does it feel like what a user should see?

### Exit Criteria
- [ ] 50 producers inserted and graded
- [ ] ~200 wines with identity tuples
- [ ] Display names reviewed for all 10 categories
- [ ] Completeness and confirmation scores calculated
- [ ] Problems documented (what broke, what was surprising)
- [ ] Decision: proceed to batch 1 or redesign something

---

## Session 4: Batch 0 Review

**Status:** NOT STARTED
**Model:** Opus
**AI cost:** ~$0.50

### Goal
Review batch 0 output. Run mini Josh Test. Decide what's next.

### Process

1. **Mini Josh Test:** Sample 50 wines from restaurant lists / retail / grocery. Can Loam find them? What's the quality?
2. **Display name audit:** Are all 10 country categories rendering correctly?
3. **Completeness analysis:** What's the average completeness? Where are the gaps?
4. **Cuvée quality check:** Read through all ~200 cuvée values (including NULLs). Do they make sense?
5. **Decide next steps:**
   - Is the pipeline working? Scale to batch 1.
   - Is the cuvée cleaning broken? Redesign before scaling.
   - Is the producer matching working? Or do we need more sources?
   - What's the right batch 1 size?

### Exit Criteria
- [ ] Mini Josh Test results documented
- [ ] Problems from batch 0 resolved or documented
- [ ] Batch 1 scope decided (producer count, wine target)
- [ ] Plan updated with learnings

---

## Sessions 5+: Iterative Batches

**Details decided after Session 4.** Rough outline:

### Batch 1 (~Sessions 5-6): The $30-100 Core
- ~500 producers focused on the $30-100 price tier (100% coverage target)
- ~3,000 wines through full pipeline
- Depth promotion + enrichment
- Mini Josh Test on this tier

### Batch 2 (~Sessions 7-8): Expanding Coverage
- ~2,000 producers expanding into $10-30 and $100-250 tiers
- ~12,000 wines
- Deeper enrichment pass

### Batch 3 (~Session 9-10): Fill Gaps
- Coverage gaps identified by Josh Test
- $0-10 tier grocery brands
- $250+ tier trophy wines
- Additional producer sources if needed (CA ABC, wholesale lists, etc.)

### Enrichment Sweep (~Session 10)
Across all batches:
1. **Direct staging search** for remaining gaps (FREE)
2. **Haiku suggest + source confirm** for wines where direct search finds nothing
3. **Triage ai_suggestions** — analyze confidence distribution, decide escalation
4. **Sonnet showcase** on top 500 wines with full data

Field-level confidence for Haiku (thresholds decided during triage with real data):
- Color: lower bar (usually determinable from context)
- Country: lower bar (almost always known)
- Appellation: higher bar (requires real knowledge)
- Primary grape: higher bar
- Blend percentages: source confirmation ONLY, no AI fallback

### Full Josh Test (~Session 11)
- Design test with price-tier weighting
- Sample from: retail bestsellers (Wine.com, Total Wine, Drizly), wine lists found online, subscription boxes, grocery store selection
- Report: find rate, avg confirmation, avg completeness, avg enrichment
- Compare against targets: 85% find, Confirmation B+, identity_complete = TRUE

---

## Budget Tracker

| Phase | Estimated | Actual | Notes |
|-------|-----------|--------|-------|
| Phase 0 | $0 | — | DDL only |
| Session 2: Design | $0 | — | Planning only |
| Batch 0 | $1 | — | 50 producers prototype |
| Batch 0 Review | $0.50 | — | Mini Josh Test |
| Batch 1 | $12 | — | 500 producers + depth |
| Batch 2 | $20 | — | 2K producers + depth |
| Batch 3 | $12 | — | Gap fills |
| Enrichment sweep | $36 | — | Haiku suggest $15 + Sonnet $21 |
| Josh Test | $2 | — | Full validation |
| Dry-run iterations | $10 | — | ~20% overhead |
| **Subtotal** | **~$93** | | |
| **Buffer** | **$82** | | For iteration, surprises, 95% push |
| **Total Budget** | **$175** | | |

---

## Session Log

| Session | Date | Phase | Model | Outcome |
|---------|------|-------|-------|---------|
| Planning | 2026-04-08 | — | Opus | Full plan created. Key decisions: build from scratch, archive existing, three-metric grading, identity tuple, price-tier coverage, iterative batches, AI suggest+confirm, provenance table, blend/appellation/grape confirmed booleans, Josh Test, batch 0 prototype approach. |

---

## Open Questions

- [x] ~~Start fresh?~~ **Build from scratch. Archive existing. (2026-04-08)**
- [x] ~~Provenance?~~ **`data_provenance` audit table. (2026-04-08)**
- [x] ~~Grape completeness?~~ **All grapes with %. `blend_complete` + `grapes_confirmed`. (2026-04-08)**
- [x] ~~Confidence threshold?~~ **Deferred to Phase 4 triage with real data. Field-level. (2026-04-08)**
- [x] ~~No-appellation wines?~~ **`appellation_confirmed` boolean. NULL + TRUE = confirmed none. (2026-04-08)**
- [ ] Negociant bottlings vs estate (same wine, different label)?
- [ ] LWIN ↔ COLA cross-link: dry run in batch 0 or batch 1
- [ ] NY SLA wholesale price list — accessible?
- [ ] Archived wines: hidden from search or shown with disclaimer?
- [ ] Display name: classification in display_name or referenced from label_designations? (Session 2)

---

## Pipeline Library

```
pipeline/
  identity/           # Phases 1-2
    load_ttb_permits.py
    load_wikidata.py
    producer_crossref.py
    producer_vote.py
    select_wines.py
    clean_cuvee.py
    dedup_deterministic.py
    crosslink_lwin_cola.py
    build_display_name.py
  quality/            # Phases 3-4
    promote_vintages.py
    promote_grapes.py
    validate_grapes.py
    cascade_geography.py
    cascade_depth.py
    promote_prices.py
    promote_scores.py
    haiku_suggest_confirm.py
    label_link.py
    grade_calculator.py
  fetch/              # Data sources
    ttb_permits.py
    wikidata_sparql.py
  analyze/            # Measurement
    thirty_k_dashboard.py   # (exists)
    josh_test.py            # (new)
    winetest/               # (exists)
  lib/                # Shared (existing)
    db.py
    normalize.py
    resolve.py
    merge.py
```

All scripts: idempotent, `--dry-run` default, logs to `data_provenance`.
