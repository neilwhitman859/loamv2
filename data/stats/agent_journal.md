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
