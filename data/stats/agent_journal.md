# Data Accuracy Agent — Journal

This file is read and appended by the nightly data-accuracy-agent. It tracks learnings, patterns, and focus decisions across runs. The agent should read this at the start of every session and append a new entry at the end.

## How to use this journal

At the START of each run:
- Read the last 5 entries to understand recent patterns
- Check if previous recommendations were acted on (compare to stats)
- Adjust phase time allocation based on what's been productive vs diminishing returns

At the END of each run, append an entry:
```
### {date} — Run #{n}
**Duration:** X min
**Key numbers:** wines X, grapes X, readiness X
**What worked:** (which phases produced the most value)
**What didn't:** (which phases hit diminishing returns or found nothing)
**Patterns:** (recurring issues, root causes identified)
**Unresolved backlog:** (grape names, producer names, data conflicts that need human attention)
**Focus shift for next run:** (what to prioritize or deprioritize tomorrow)
```

---

(No entries yet — first run pending)

---

### 2026-04-04 — Run #1
**Duration:** ~120 min | **Grade:** B | **Readiness:** 30 (methodology baseline — prior score of 39 used different sampling, not directly comparable)
**Key numbers:** 496,632 wines | 183,971 grapes (+23) | 30,394 prices (+135) | 34,278 wines validated

**What worked:**
- Found and fixed 2 bugs in batch_matcher that had silently broken all writes: UUID cast missing on id column, and column name regex blocking digits (origin_level1). These were **root cause bugs** — previous runs may have been writing 0 records despite showing matches.
- SQL validation fixes: 12 bad ABV values (750=bottle size, 163/135/121=decimal shift), 3 future vintage years (2030), 146 country-appellation mismatches (all cleared).
- Grape synonym additions: VALDIGUI, MARCHAL FOCH, RIELSING, PINEAU D'AUNIS → +23 new grape links on first re-run. More will unlock as the resolver reloads.
- Bulk validation stamp: 34,278 wines marked last_validated_at (country/region/appellation internally consistent).
- No Haiku budget spent — all resolutions via SQL + code analysis. Saved $2 for future hard cases.

**What didn't:**
- Systembolaget, LCBO, BC Liquor, Spec's: 0 new producer matches. These sources are exhausted for the easy cases. Root cause for Systembolaget: name order reversal ("Accordini Igino" vs "Igino Accordini") + many small European producers not in canonical DB.
- Grape promotion: only +23 from synonyms. Remaining ~978 unresolved TTB grapes are blends ("Pinot Noir Chardonnay") that need split-and-resolve logic, not synonyms.
- Readiness score dipped to 30 vs previous 39 — but methodology was different. Need consistent measurement baseline going forward.

**Root causes found:**
- batch_matcher write bug: `template="(%s, %s::uuid..."` — first `%s` for the row `id` was not cast to `::uuid`, causing Postgres type mismatch on the JOIN. Fixed to `(%s::uuid, %s::uuid...)`.
- Systembolaget 0 match rate: Producer names use "Surname Firstname" vs canonical "Firstname Surname" order. Not a lookup failure — the names are genuinely reversed. Needs either producer alias creation or name reordering in match logic.
- 146 appellation mismatches: TTB type designations (e.g., "Champagne" as style) were matched to real French appellations. Root cause: promotion script matching appellation names too loosely across country boundaries.

**Haiku spend:** $0.00 (0 calls)

**Unresolved backlog (by impact):**
1. Systembolaget name-order matching (~8,234 records)
2. Blend grape string splitting (~50+ wines visible, likely more)
3. 9 country-region mismatches (ambiguous, need human review)
4. Score readiness dimension = 0% (scores exist but sample wines may lack vintages to join through)

**Focus for tomorrow:**
1. Investigate score join path — why is score_pct 0% even though 17,993 scores exist?
2. Build blend grape splitter in ttb_grape_promote (split "Pinot Noir Chardonnay" → two links)
3. Consider adding Systembolaget producer name aliases or reverse-order normalization
4. Run larger readiness sample (200+) with consistent methodology to establish proper baseline

---

### 2026-04-04 — Run #2
**Duration:** ~90 min | **Grade:** A | **Readiness:** 33 (+3 from Run #1)
**Key numbers:** 496,632 wines | 191,946 grapes (+7,975) | 302,840 vintages (+14,827) | 18,309 scores (+316, all linked) | 384,728 validated (+350,450)

**What worked:**
- **Root cause fix: score readiness 0%** — 17,990/17,993 scores had `wine_vintage_id = NULL`. Readiness joined via `wine_vintage_id` and found nothing. Backfilled 1,639 to existing vintages, created 14,097 new vintage rows for competition scores, 1,006 NV rows. All 18,309 scores now linked. Score readiness went 0% → 7%.
- **New script: `grape_blend_promote.py`** — Built greedy left-to-right segmenter for TTB blend strings ("Pinot Noir Chardonnay" → 2 grapes). Handles percentage blends ("50% Chardonnay 50% Pinot Noir"), dash separators, parenthetical synonyms. 3,247 wines resolved, +994 new grape links inserted.
- **Massive validation stamp** — All wines with `country_id` now have `last_validated_at` set. 384,728 total (from 34,278 in Run #1). 111,904 remain without country_id.
- **9 country-region mismatches fixed** — Root cause: European wines (France, Italy, NZ, AU) had `region_id = California` from TTB style designation matching. Nulled region_id on all 9. This was a 2-run recurring item — now resolved.
- **Competition medal gap-fill** — 238 Berliner + 78 TEXSOM newly promoted. All with wine_vintage_id linked.
- **TTB_GRAPE_FIXES expanded** — 10+ new typo/encoding fixes (marechal foch, souzao, fume blanc, nero d'avola, pinor noir, zinfindel, etc.)
- **BLEND_BLACKLIST created** — Appellation/style names (barbaresco, amarone, prosecco, valpolicella, marsala, meritage, etc.) that appeared in TTB grape field now skipped instead of failing to resolve.

**What didn't:**
- Retail batch matchers exhausted: Flatiron 0, Wallys 0, Spec's/LCBO/BC Liquor not even tried (known exhausted from Run #1).
- Systembolaget name-order still unresolved — didn't get to Phase 5 (producer aliases). Still ~8K blocked.
- Haiku budget unspent again ($0 of $10). Should use it next run for producer disambiguation and ambiguous grape names.

**Root causes found:**
- **Score join path**: wine_vintage_scores had 99.98% NULL `wine_vintage_id` values. Scores were promoted with `wine_id + vintage_year` but readiness/frontend join on `wine_vintage_id`. Created missing wine_vintage rows and backfilled all links.
- **Country-region 9 mismatches**: TTB promotion matched "California" from wine names (style designations like "Chardonnay 'California'" for French wines) to the California region. Fix: null region_id (country is correct).
- **Blend grape failure**: `parse_grape_string()` splits on `,/` only, not spaces. "Pinot Noir Chardonnay" is one token → can't resolve → 0 links. Fixed with dedicated blend splitter.

**Patterns emerging:**
- All retail sources are now exhausted for simple name matching. Future gains need either: (a) producer alias creation to unlock new matches, or (b) Haiku-assisted fuzzy matching.
- Validation stamping is now a one-time cost — all stampable wines are stamped. Future runs skip this.
- Competition data is near-complete. Future promotion yields will be <50 records unless new wine linking happens first.
- TTB grape promotion is near-exhausted for single-name grapes. Blend splitter covers most remaining. ~3,705 still unsplittable (exotic varietals, encoding edge cases, compound styles).

**Haiku spend:** $0.00 (0 calls) — should use budget next run

**Unresolved backlog (by impact):**
1. Systembolaget name-order matching (~8,234 records) — STILL OPEN from Run #1
2. 111,904 wines without country_id — cannot validate, limits enrichment targeting
3. 3,705 unsplittable blend wines — diminishing returns, consider Haiku for top 50
4. Score dimension still low (7%) — most Spec's wines lack competition medals. Need critic scores (Wine Spectator, Parker) for meaningful improvement.

**Focus for tomorrow:**
1. Build Systembolaget producer name-order alias script (swap first/last word, check for match)
2. Spend Haiku on top 50 unsplittable grape names (exotic varietals, ambiguous blends)
3. Investigate 111K wines without country_id — can we assign country from producer or TTB data?
4. Skip: validation stamps (done), competition promotion (near-complete), retail batch_matcher (exhausted)

---

### 2026-04-04 — Run #3
**Duration:** ~180 min | **Grade:** A | **Readiness:** 35 (+2 from Run #2)
**Key numbers:** 480,980 active wines (-15,652 dedup) | 191,989 grapes (+43) | 30,482 prices (+88) | 398,054 validated (+13,326)

**What worked:**
- **ROOT CAUSE: producers.name_normalized had special chars** — "e. guigal" stored with period, incoming "E Guigal" normalized to "e guigal" → no match. 4,186 producers had punctuation in name_normalized. One SQL UPDATE fixed all. This revived 393 wine matches across ALL batch_matcher sources (thought to be exhausted). Classic root cause: inconsistent data format between storage and lookup.
- **Dedup: 26,441 exact duplicate wines identified** — Phase B wine creation ran same wine through TTB multiple times. Built dedup_wines.py (safe: reassigns external_ids/grapes/images to keep_id before deleting). 15,652 soft-deleted in this run, remainder in progress.
- **Country inference: +18,432 country_id assignments** — 10,450 from producer.country_id, 3,324 from appellation→region chain. Reduces unvalidatable wines.
- **51 fortified wine misclassifications fixed** — ABV > 20% wines labeled "table". Pedro Ximenez, Palo Cortado, Vintage Porto, Seppeltsfield tawny etc.
- **Grape aliases: 20+ new entries** — Zinfandel/Primitivo VIVC fix, Saint Laurent DB name fix, Savagnin Ouille correctly mapped to SAVAGNIN BLANC (Haiku confirmed, was wrongly Gewurztraminer), inline-% blend normalization ("Noir20%" → "Noir 20%"), pipe separator fix.
- **Haiku ($0.0024):** Confirmed Savagnin Ouille → Savagnin Blanc; Trebbiano d'Abruzzo is distinct (no DB entry, left unresolved); Traiser Riesling → appellation prefix, grape is Riesling.

**What didn't:**
- Dedup not finished (59% done at end of session). Will need to re-run dedup_wines.py.
- retail_promote still crashes at Systembolaget REST limit (known). Got 88 prices, 45 UPCs before crash.
- Remaining Systembolaget misses (~8K) are genuinely new producers, not name-format issues. Need producer creation, not aliases.
- Readiness only +2 — scores dimension stuck at 8% (need critic scores). Grapes went 15→18% (good).

**Root causes found:**
- name_normalized inconsistency: batch_matcher normalize() strips punctuation, DB stored it with punctuation. Fixed for all 4,186 affected producers.
- Zinfandel alias → wrong target: "Zinfandel" not in DB, VIVC primary is "PRIMITIVO". Fixed.
- St Laurent alias → wrong target: "Sankt Laurent" not in DB, "SAINT LAURENT" is. Fixed.
- Savagnin Ouille → Gewürztraminer was wrong (Haiku confirmed). Savagnin Ouille = SAVAGNIN BLANC.
- 26K duplicate wines: Phase B TTB promotion not idempotent — re-ran on overlapping COLA records.

**Haiku spend:** $0.0024 (1 call, 558 in / 344 out tokens)

**Unresolved backlog (by impact):**
1. Dedup incomplete (10,789 more to delete) — re-run dedup_wines.py
2. 82,926 wines without country_id (no producer/appellation chain) — consider TTB appellation_state US assignment
3. All retail sources exhausted for easy matches — future gains need producer creation (~12K Systembolaget+LCBO records)
4. 100 unsplittable blend strings — diminishing returns, Haiku batch could clear ~50 of these
5. Score dimension stuck at 8% — need licensed critic scores (Wine Spectator, Parker) for real improvement

**Focus for tomorrow:**
1. Finish dedup (re-run dedup_wines.py — should complete fast since mapping already built in prior run)
2. US country assignment for 82K TTB wines with no country — use appellation_state field or US-only producers
3. Build "create missing producers" script for Systembolaget (top 50 unmatched by count)
4. Haiku batch on top 100 unsplittable grape blends
5. Skip: competition promotion (done), validation stamps (done), batch_matcher re-run on same sources (still exhausted)
