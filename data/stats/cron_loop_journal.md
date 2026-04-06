# Cron Loop Journal

Append-only log of automated loop runs. Each entry records what was attempted,
what worked, what was wasted, and what to skip next time. Read this BEFORE
designing a new cron loop — it prevents repeating dead-end work.

See `data/session_prompts/cron_loop_template.md` for the structural template.

---

## Run 1: 2026-04-06 Overnight — Prices + Vineyards + Data Quality

**Duration:** ~27 cycles over ~4.5 hours (*/10 cron)
**Prompt:** 3-track loop (Track A prices, Track B vineyards, Track C data quality sweeps)

### Track A: Price Coverage — LOW YIELD
- **Phase 1** (retail_promote for 5 sources): best_wine_store yielded some, rest were 0
- **Phase 2** (batch_matcher + retail_promote for 10 sources): Near-zero new matches across all sources. Prior sessions had already matched and promoted everything reachable.
- **Phase 3** (re-promote wallys/specs/systembolaget/lcbo): 0 new prices — all wines already had prices from other merchants.
- **Lesson:** Should have run a gap analysis query first. All price sources were already promoted. The entire track was wasted cycles.
- **Skip next time:** Don't re-run batch_matcher/retail_promote for sources that prior sessions already covered. Query `NOT EXISTS` counts first.

### Track B: Classified Vineyards — HIGH VALUE (0 → 815)
- **Phase 1** (26 Burgundy villages): 585 Premier Cru climats from INAO CDCs. Genuine new data creation. Required careful PDF parsing, slug conflict resolution (9 conflicts), format variation handling per village. **This was the real win.**
- **Phase 2** (Barolo MGAs): 170 from MASAF disciplinare. Clean extraction from comma-separated list in legal text. PDF had OCR artifacts (missing commas, split words) requiring judgment.
- **Phase 3** (51 Alsace Grand Crus): Straightforward — appellations already existed in DB, just needed vineyard rows linked to them.
- **Lesson:** Legal source seeding is the ideal cron loop use case. Large backlog, self-contained units, zero inference risk.

### Track C: Data Quality Sweeps — ALREADY DONE
- All 6 sweeps (C1-C6) returned 0 rows from cycle 1 onward.
- Prior sessions had already executed these exact queries.
- **Lesson:** Run all sweeps once as a verification pass. If they return 0, drop the track entirely. Don't rotate through them for 27 cycles.

### Structural Issues
- **No self-termination:** Cron kept firing after all tracks completed. Had to cancel manually.
- **Multi-track waste:** 2 of 3 tracks were exhausted from the start. Gap analysis would have caught this.
- **What to do differently:** Single-track focus on genuinely large backlog. Self-termination check at cycle start. Pre-flight gap analysis before cron creation.

### Final Numbers
| Metric | Start | End | Delta |
|---|---|---|---|
| Vineyards | 9 | 815 | +806 |
| Wines with prices | 27,264 | 27,264 | +0 |
| Colors | 282,146 | 282,146 | +0 |
| Varietal categories | 104,504 | 104,504 | +0 |

### Cycle Log (abbreviated — no per-cycle logging existed for this run)
| Cycles | Track | Item | Result |
|--------|-------|------|--------|
| 1-8 | A | Phase 1+2 price promotion (8 sources) | ~0 new prices (already promoted) |
| 9-10 | A | Phase 3 re-promote wallys/specs/syst/lcbo | 0 new prices |
| 1-19 | B | Burgundy 1er Crus (20 villages) | +446 vineyards, 9 slug conflicts resolved |
| 20-25 | B | Burgundy continued (Rully→Chablis) | +149 vineyards |
| 26 | B | Barolo MGAs | +170 vineyards |
| 27 | B | Alsace Grand Crus | +51 vineyards |
| 1-27 | C | All 6 sweeps (rotated) | 0 rows affected (all exhausted from prior sessions) |

### Remaining Backlog for Future Loops
- **Barbaresco MGAs** — MASAF disciplinare in hand, ~66 MGAs
- **Brunello/Chianti Classico/other Italian DOCG vineyards** — from MASAF sweep PDFs
- **German Einzellagen** — if VDP classification list can be sourced legally
- **More appellation_rules** — 549 done, ~3,100 appellations remain
- **Appellation_vintages weather data** — Open-Meteo integration (empty table, 0 rows)
- **Score coverage** — 2.24%, needs fuzzy matching (interactive, not loop material)
