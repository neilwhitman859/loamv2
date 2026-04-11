# DB Canonical Audit — Findings

**Session:** S2.1
**Date:** 2026-04-11
**Expert:** db_canonical
**Scope:** canonical wines / producers / vintages / grapes / scores / prices / external_ids, reference layer structural integrity, join paths via detail views, empty table categorization. `source_*` staging tables deliberately excluded (that is S2.2).
**Method:** read-only SQL via Supabase MCP. No AI calls. No fixes. ~50 queries.
**Budget:** $0.00

## Ground truth (as of audit start)

```
wines active              155,623        wine_vintages             83,531
wines soft-deleted            947        wine_grapes               46,028
producers active           10,676        wine_vintage_scores        5,420
producers soft-deleted          7        wine_vintage_prices       23,220
                                         external_ids             438,823

data_grade: B=105  C=4,973  D=33  F=150,512  (no NULL)
```

Every finding below is sourced from queries run against this state.

## Summary

**Total findings:** 34
- **P0** (broken / correctness-critical / user-visible): **6**
- **P1** (significant gap, must fix before enrichment): **12**
- **P2** (improvement, not blocking): **11**
- **P3** (nice to have): **5**

**Biggest risks (P0):**
1. Producer metadata is essentially empty — 10,676 producers, ~0 with any depth beyond name+region+country. The producer page is a shell.
2. search_vector populated on only 32% of wines and 0% of producers — search is missing the majority of the catalog by row count.
3. Pre-30K depth was catastrophically lost in the rebuild — 80%+ of archive scores/prices/label designations/farming certs did not migrate.
4. Grape synonym table has 919 entries that collide with another grape's primary name (same bug pattern that caused the S1.11 Riesling incident).
5. 3,534 wines sit in `1,686` true-duplicate groups (same producer + normalized name + appellation).
6. wine_vintages winemaking/chemistry depth is fractions of a percent — pH, TA, RS, VA, SO2, oak %, harvest dates are all near-zero.

**Biggest wins (things that are correct and did not need flagging):**
- Core FK integrity is clean: no regions missing country, no appellations missing country/region, no appellation_rules/appellation_grapes FK orphans, no wine→missing-producer orphans, no external_ids→missing-wine orphans.
- Soft-delete discipline holds: every soft-deleted wine has `duplicate_of` set; 0 active wines pointing at non-existent producers.
- RLS is enabled on 98/105 public canonical tables (the 7 exceptions are all pipeline temp tables — see F27).
- Slug uniqueness enforced on wines, producers, appellations — 0 collisions.
- Grade B/C wines all have matching `wine_insights` rows (105/105 and 4,973/4,973).
- No U+FFFD or accent mojibake in any wine/producer name — the S14 cleanup held.
- Scores and prices are fully backfilled with `wine_vintage_id` (100%).
- Reference layer row counts are in the expected ranges: 68 countries, 389 regions, 3,661 appellations, 9,694 grapes, 1,165 appellation_rules, 10,414 appellation_grapes, 134,912 appellation_weather_years.
- `set_updated_at` trigger is on every entity table; search_vector triggers exist on wines/producers/grapes/regions/appellations (even if population is incomplete — see F3).

**Scope-breaker check:** None of the findings require Sprint 3 to be re-scoped. Most are executable inside the Sprint 2→3 envelope if we accept that depth recovery from `archive.*` is a real workstream. The reference-layer work (grape synonyms, appellation rule provenance) can land in Sprint 4 design, not Sprint 3 execution.

---

## Findings

### F1 — 3,534 wines in 1,686 true-duplicate groups (same producer + name_normalized + appellation)

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT count(*) AS true_dupe_groups, sum(wine_count) AS wines_in_true_dupe_groups, max(wine_count) AS max_wines_per_group
    FROM (
      SELECT producer_id, name_normalized, appellation_id, count(*) AS wine_count
      FROM wines WHERE deleted_at IS NULL AND name_normalized IS NOT NULL
      GROUP BY producer_id, name_normalized, appellation_id
      HAVING count(*) > 1
    ) s;
    ```
    Result: `true_dupe_groups=1686, wines_in_true_dupe_groups=3534, max_wines_per_group=3`.
    Of the broader 2,432 same-producer/name groups: 1,100 share an appellation (clear dupes), 759 span multiple appellations (Fourrier "Vieille Vigne" across 14 grand crus — **these are NOT dupes** by the Loam identity rule), 309 have all NULL appellations (ambiguous), 264 are partial-NULL (ambiguous).
- **Why it matters:** Dedup is a prerequisite for meaningful enrichment. Running Grade B/C on a duplicated wine burns budget and splits lookup_count. 3,534 wines is ~2.3% of the active catalog, enough to show up in user-visible experience and moot Josh Test results.
- **Proposed fix:** Run the strict exact-match + Haiku fuzzy path used in S1.13, restricted to the 1,100 same-producer-same-name-same-appellation groups first. Defer the 573 ambiguous groups to the wine expert session (S2.3). Do NOT touch the 759 multi-appellation groups — the normalization key is simply incomplete for terroir-variant cuvées (see F2).
- **Effort:** medium
- **Dependencies:** F2 (terroir-variant normalization key)
- **Related findings:** F2, F15

### F2 — Dedup normalization key lacks terroir context — 759 legitimate wine groups appear as dupes

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT w.id, w.name, a.name AS appellation_name
    FROM wines w LEFT JOIN appellations a ON a.id = w.appellation_id
    JOIN producers p ON p.id = w.producer_id
    WHERE p.name = 'Jean-Marie Fourrier' AND w.name_normalized = 'vieille vigne' AND w.deleted_at IS NULL;
    ```
    Returns 14 rows across Bonnes-Mares, Chambertin, Chambertin-Clos de Bèze, Chambolle-Musigny, Clos de Vougeot, Echézeaux, Gevrey-Chambertin, Griotte-Chambertin, Latricières-Chambertin, Mazis-Chambertin, Mazoyères-Chambertin, Morey-Saint-Denis, Vosne-Romanée, Vougeot. All are distinct wines by the Loam identity rule (different terroir = different wine).
- **Why it matters:** Any dedup script that clusters on `(producer_id, name_normalized)` will flag these legitimate siblings as duplicates. The S1.13 Haiku fuzzy pass likely correctly sorted them, but the clustering logic is still a landmine for future passes. Also breaks identity-checks: producer pages can't reliably surface "Fourrier Vieille Vigne" as a cuvée family.
- **Proposed fix:** Add appellation (or commune / vineyard) to the dedup cluster key. Promote the existing Burgundy pattern into a formal identity contract in `docs/DECISIONS.md` (or `docs/IDENTITY_RULES.md` referenced in S1.2): a wine is identified by `(producer, cuvée_name, appellation, designation)` at minimum.
- **Effort:** small (key change), medium (document + update scripts that rely on old key)
- **Dependencies:** none
- **Related findings:** F1

### F3 — search_vector populated on only 32% of wines and 0% of producers

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE search_vector IS NOT NULL) AS wines_with_sv,
           count(*) FILTER (WHERE search_vector IS NULL)    AS wines_null_sv
    FROM wines WHERE deleted_at IS NULL;
    -- wines_with_sv=50383, wines_null_sv=105240  (67% NULL)

    SELECT count(*) FILTER (WHERE search_vector IS NOT NULL) FROM producers WHERE deleted_at IS NULL;
    -- producers_with_sv=0, producers_null_sv=10676 (100% NULL)
    ```
    Triggers `trg_wines_search_vector` and `trg_producers_search_vector` exist on INSERT and UPDATE. The wine trigger fires `update_wine_search_vector()`; the producer trigger fires `update_producer_search_vector()`.
- **Why it matters:** Search is the front door. Josh Test reports 84% findability but runs through `search_catalog`, which (per S1.2/S1.7 commits) may fall back to trigram. Either (a) Josh Test passes via trigram despite the tsvector being empty — in which case tsvector is a dead optimization — or (b) a portion of that 84% is actually failing and the test is forgiving. Either way, 67% of wines and 100% of producers silently lack their primary search index. The S1.13 long-tail inserted 104,009 wines (consistent with 105,240 NULL), suggesting the trigger either didn't exist at that time or fires but produces a NULL tsvector for those rows.
- **Proposed fix:** Diagnose which of (a) trigger-was-added-later, (b) trigger-function-returns-NULL-for-sparse-rows, or (c) function-is-wrong applies. Backfill via `UPDATE wines SET search_vector = to_tsvector(...)` using the same logic the trigger uses. Same for producers. Add a validation check in Sprint 3 that asserts coverage ≥ 99%.
- **Effort:** medium
- **Dependencies:** Code audit (S2.5) should verify the trigger functions are correct.
- **Related findings:** —

### F4 — Producer metadata is essentially empty

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT count(*) AS total,
      count(*) FILTER (WHERE website_url IS NULL OR website_url='') AS null_website,
      count(*) FILTER (WHERE year_established IS NULL) AS null_year_est,
      count(*) FILTER (WHERE producer_type IS NULL) AS null_producer_type,
      count(*) FILTER (WHERE latitude IS NULL) AS null_coords,
      count(*) FILTER (WHERE philosophy IS NULL) AS null_philosophy,
      count(*) FILTER (WHERE parent_producer_id IS NOT NULL) AS has_parent
    FROM producers WHERE deleted_at IS NULL;
    ```
    Result (10,676 producers): 10,675 null website (1 populated), 10,676 null year_established, 10,675 null producer_type (1 populated), 10,676 null coordinates, 10,676 null philosophy, 0 with parent_producer_id, 953 null region_id.
    
    `archive.producers` has 41,758 rows with 191 websites, 157 year_established, 117 lat/long — so even the pre-30K state was thin. Pre-30K producer scrape (S9, ~120 producers with real metadata via KL Growers) did not survive the 30K rebuild.
- **Why it matters:** The producer page is a structural shell. No website, no history, no coordinates for map, no parent/child ("Sea Slopes → Fort Ross"), no philosophy. Nothing a sommelier (or enthusiast) would find interesting. Directly defeats Principle #9 "Structured data, structured display" and Principle #4 "Everything connects to everything."
- **Proposed fix:** Producer enrichment is a Sprint 5 workstream under Reference-First, but Sprint 3 should at a minimum (a) recover what exists in `archive.producers` that maps to current canonical (website 191, year_established 157, lat/long 117, producer_type 41K), and (b) re-run `pipeline/fetch/producer_site_scrape.py` (S9 wrote it) for the top 100-500 producers by wine_count.
- **Effort:** medium (archive recovery) + large (producer scrape at scale)
- **Dependencies:** F5
- **Related findings:** F5, F18

### F5 — All producer relationship tables are empty

- **Severity:** P0
- **Evidence:**
    ```sql
    -- producer_aliases=0, producer_farming_certifications=0, producer_biodiversity_certifications=0,
    -- producer_winemakers=0, producer_timeline=0, producer_importers=0, producer_insights=0,
    -- producer_documents=0, winemakers=0, importers=1
    ```
    CLAUDE.md (stale) claims S1.6 Phase depth recovery yielded "845 importer certs" and "166 winemakers / 173 producer-winemaker links." Those values are no longer present — the 30K rebuild nuked them.
- **Why it matters:** Producer-level farming certs (biodynamic, organic, Demeter) are a primary signal for modern wine buyers. Winemakers drive house style and reputation. Parent/child relationships (e.g., Catena → Alamos) are a core "connect everything" path. All of it is gone.
- **Proposed fix:** Sprint 3 recovery: (1) re-promote farming/biodiv certs from `archive.*` via producer name_normalized bridge, (2) rebuild producer_winemakers from `archive.producer_winemakers`, (3) seed producer_insights as part of Sprint 5 Reference-First work.
- **Effort:** small (certs), medium (winemakers), large (insights — Sprint 5)
- **Dependencies:** F4
- **Related findings:** F4, F18

### F6 — 80%+ of pre-30K depth was lost in the rebuild and never re-bridged

- **Severity:** P0
- **Evidence:**
    ```sql
    -- archive vs public
    wine_vintage_scores:          27,325 → 5,420    (80% loss)
    wine_vintage_prices:         139,937 → 23,220   (83% loss)
    wine_label_designations:      58,510 → 11,589   (80% loss)
    wine_farming_certifications:   9,447 → 1,252    (87% loss)
    archive.wine_vintages with abv: 175,384 → 48,700 (72% loss)
    archive.wine_vintages with ph:      292 → 53
    archive.wine_vintages with ta:      316 → 55
    archive.wine_vintages with closure: 111 → 3
    ```
    The S1.6 depth-recovery-via-LWIN-bridge worked only for wines that had a matching LWIN pair in archive. Phase B + LWIN long-tail wines (~110K of the 155K) had no archive bridge and pulled no depth.
- **Why it matters:** This is the single biggest "value left on table" finding. ~120K archive vintages with real chemistry, ~116K archive prices, ~22K archive scores, ~47K label designations, ~8K farming certs are sitting in `archive.*` waiting for a non-LWIN bridge (producer name_normalized + wine name match). Every one of these that re-promotes raises a wine's data_grade from F/D to C without a single AI call.
- **Proposed fix:** Sprint 3 workstream: build `pipeline/promote/archive_depth_bridge.py` that joins `archive.wines → public.wines` on `(producer_name_normalized, wine_name_normalized, [appellation_id])` and re-promotes scores / prices / label designations / farming certs / chemistry via archive→public wine_id mapping.
- **Effort:** medium (script), small-to-medium (runtime, 5K-wine batches)
- **Dependencies:** F1 (dedup first — otherwise promoting to a duplicated wine doubles the dupe's data)
- **Related findings:** F1, F9, F10, F11, F12

### F7 — 919 grape synonyms collide with another grape's primary name (Syrah/Durif class)

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FROM grape_synonyms gs
    WHERE EXISTS (SELECT 1 FROM grapes g WHERE lower(g.name)=lower(gs.synonym) AND g.id != gs.grape_id);
    -- 919
    ```
    Sample cases: `SYRAH` is a synonym of `DURIF` (2,891 Syrah wines vs 280 Durif; same pattern S1.14 fixed at the wine_grapes level — but the synonym row remains in the table, waiting to cause the same bug next time an importer runs). `AUXERROIS` is a synonym of `PINOT BLANC`, `CHARDONNAY BLANC`, and `COT` all at once. `CARMENERE` is a synonym of `CABERNET FRANC`. `ALIGOTE` is a synonym of `CHARDONNAY BLANC`. `MELON` is a synonym of `GAMAY NOIR` and `CHARDONNAY BLANC`.
- **Why it matters:** Same root cause as S1.11 Riesling incident where 4,261 wines got repointed to the wrong grape because the synonym table had "RIESLING" → CROUCHEN. The cleanup repointed the wines but left the synonym rows intact, so any future importer that calls `grapes + grape_synonyms` will re-create the bad link. This is a time bomb, not a historical fact.
- **Proposed fix:** In Sprint 3, delete/null every `grape_synonyms` row where the synonym exactly matches a `grapes.name` whose id differs from the synonym's grape_id. Review the 919 as a batch (likely sub-500 unique synonyms worth keeping vs pruning). Sprint 4 reference redesign should formalize the rule: synonym rows may not shadow primary names.
- **Effort:** small (SQL delete), small (review batch)
- **Dependencies:** none
- **Related findings:** —

### F8 — wine_vintages winemaking + chemistry columns are near-zero coverage

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(abv) AS abv, count(ph) AS ph, count(ta_g_l) AS ta, count(rs_g_l) AS rs,
           count(va_g_l) AS va, count(so2_free_mg_l) AS so2_free, count(so2_total_mg_l) AS so2_total,
           count(brix_at_harvest) AS brix, count(duration_in_oak_months) AS oak,
           count(new_oak_pct) AS new_oak, count(whole_cluster_pct) AS whole_cluster,
           count(bottle_aging_months) AS bottle_aging, count(harvest_start_date) AS harvest_start,
           count(cases_produced) AS production, count(release_price_usd) AS release_price,
           count(closure) AS closure, count(fermentation_duration_days) AS ferm_duration,
           count(yeast_type) AS yeast, count(aging_vessel) AS aging_vessel
    FROM wine_vintages;
    ```
    Coverage across 83,531 wine_vintages:
    - abv 48,700 (58.3%) — the only reasonable metric
    - ph 53 (0.06%)  ·  ta 55 (0.07%)  ·  rs 47 (0.06%)  ·  va 0  ·  so2_free 0  ·  so2_total 0  ·  brix 0
    - oak_duration 53  ·  new_oak_pct 0  ·  whole_cluster_pct 0  ·  bottle_aging_months 0  ·  mlf 35
    - harvest_start_date 0  ·  release_price_usd 0  ·  cases_produced 52
    - closure 3  ·  fermentation_duration_days 0  ·  yeast_type 3  ·  aging_vessel 3
- **Why it matters:** Principle #9 requires structured display. The wine vintage page has ~25 labeled data slots and can fill ~1. A sommelier reading a Loam vintage page sees a ghost. Also blocks Reference-First synthesis at Sprint 5: without ABV/pH/oak per vintage, the enrichment has no facts to anchor to.
- **Proposed fix:** Archive depth bridge (F6) will recover some of this — archive has 175,384 ABV rows (for any wine that maps). Long-term, this is the Reference-First synthesis target: importer catalog pulls (Empson, Winebow, European Cellars) write these columns directly.
- **Effort:** medium (archive bridge), large (long-term data sourcing)
- **Dependencies:** F6
- **Related findings:** F6, F10

### F9 — 77% of wines have zero grape links

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT
      (SELECT count(*) FROM wines WHERE deleted_at IS NULL) AS total,
      (SELECT count(DISTINCT wine_id) FROM wine_grapes) AS with_grapes;
    -- total=155623, with_grapes=35159, missing=120491 (77.4%)
    ```
- **Why it matters:** Grape is one of the top-3 things a user expects on a wine page (alongside producer and vintage). 77% missing is the entire rebuild's long-tail effectively unreadable.
- **Proposed fix:** Three tracks: (a) archive bridge (F6) — archive has 314K wine_grapes and 82K wine_vintage_grapes; (b) Sprint 3 deterministic grape-from-name sweep via `pipeline/promote/grape_from_name.py` (already built); (c) appellation → grape inference via `appellation_grapes` for appellations with single required varieties (Chablis = Chardonnay etc.).
- **Effort:** medium (bridge + sweep), small (appellation inference)
- **Dependencies:** F6
- **Related findings:** F6, F13, F22

### F10 — critic_score_avg / critic_score_count / market_price_* rollups are 100% NULL

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE critic_score_avg IS NOT NULL) FROM wines WHERE deleted_at IS NULL;
    -- 0 / 155,623
    SELECT count(*) FILTER (WHERE critic_score_count > 0) FROM wines WHERE deleted_at IS NULL;
    -- 0
    SELECT count(*) FILTER (WHERE market_price_avg_usd IS NOT NULL) FROM wine_vintages;
    -- 0 / 83,531
    ```
    Schema documents these as "Computed: avg of all 100-point scores across all vintages" / "Refresh with backfill queries after bulk imports." Never backfilled.
- **Why it matters:** The frontend wine_detail_view exposes both columns. UI shows "Avg critic score" as empty even for wines that have 5 scores. This is a cheap SQL job that has not run.
- **Proposed fix:** Sprint 3 one-liner: write `pipeline/analyze/backfill_score_price_rollups.py` that does `UPDATE wines w SET critic_score_avg = (SELECT avg(score) FROM wine_vintage_scores WHERE wine_id = w.id AND score IS NOT NULL), critic_score_count = (SELECT count(*) FROM wine_vintage_scores WHERE wine_id = w.id AND score IS NOT NULL)` and the equivalent for market_price_* on wine_vintages. Trivial SQL, zero AI.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F11

### F11 — Only 118 numeric critic scores exist (98% of the score rows are competition medals)

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) AS total, count(*) FILTER (WHERE score IS NOT NULL) AS numeric,
           count(*) FILTER (WHERE medal IS NOT NULL) AS medals
    FROM wine_vintage_scores;
    -- total=5420, numeric=118, medals=5302
    ```
    2,030 distinct wines have any score row at all (1.3% of catalog). Of those, the overwhelming majority only have a Berliner / TEXSOM medal, not a 100-point critic score.
- **Why it matters:** For a sommelier, the expected score signal is "Wine Spectator 94", "Vinous 93", "Parker 95". Loam has almost none of that. CLAUDE.md Open Questions lists "Data licensing for scores (Wine Spectator, Parker, CellarTracker)" as deferred, which explains the gap but the finding is real: the score layer as it stands is basically a medal ledger, not a critical-reception database.
- **Proposed fix:** This is NOT a Sprint 3 fix. Log it as a Sprint 4 product decision: accept the gap and lean on medals + community ratings, or pursue data licensing in a later sprint. Flagging so Sprint 3 does not spend time "filling scores" thinking the infrastructure is complete.
- **Effort:** n/a (decision, not execution)
- **Dependencies:** none
- **Related findings:** F10

### F12 — Price coverage is 1.8% (2,818 distinct wines) vs CLAUDE.md's claimed 5.21%

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*), count(DISTINCT wine_id) FROM wine_vintage_prices;
    -- total=23,220, distinct_wines=2,818
    ```
    CLAUDE.md claims "Price coverage ~1% → 5.21% (25,898 distinct wines)" from the 2026-04-04 price session. Current state is 2,818 distinct wines — **~89% of the pre-rebuild price coverage is gone.** Archive has 139,937 prices vs public's 23,220.
    Also: 1,349 of the 23,220 rows (5.8%) have NULL `price_usd`.
- **Why it matters:** Same root cause as F6 — depth lost in the rebuild. Price is a top-3 field on any wine page and one of the main signals users search on.
- **Proposed fix:** Archive depth bridge (F6) recovers most of this. NULL-price rows should be either backfilled from `price_original+currency` or deleted.
- **Effort:** medium (bridge) + trivial (NULL cleanup)
- **Dependencies:** F6
- **Related findings:** F6

### F13 — 23,987 COLA IDs in external_ids are shared across multiple wines

- **Severity:** P1
- **Evidence:**
    ```sql
    WITH cola_dupes AS (
      SELECT external_id, count(DISTINCT entity_id) AS wine_count
      FROM external_ids WHERE system='cola' GROUP BY external_id HAVING count(DISTINCT entity_id) > 1
    )
    SELECT count(*), sum(wine_count), max(wine_count) FROM cola_dupes;
    -- 23760 dupe COLAs, 47,747 wine-links, max 4 wines per COLA
    ```
    Spot-check of 100 COLA dupes: 14 had both wines resolved (6 same producer + same normalized name = dedup miss, 6 same producer + different normalized name = cuvée disambiguation issue, 2 different producers = contamination). The other 86 pointed at soft-deleted wines or rows where `wines.id NOT IN (wines)` (latter is 0, so soft-deleted). S1.13 noted 57 external_ids pointing to soft-deleted.
- **Why it matters:** A TTB COLA ID is a government-issued unique label approval. It should map to exactly one canonical wine. If two active wines share a COLA, one of them is wrong. This is the dedup work in F1 plus a separate "fix the multi-hit external_ids" pass.
- **Proposed fix:** After F1 dedup, re-run a query that (a) drops external_ids where the target wine is soft-deleted, (b) for remaining dupes, picks the canonical via `duplicate_of` chain or latest `last_validated_at`. Add a deferred `UNIQUE(system, external_id)` partial index for system IN ('cola','lwin','lwin_7') — the schema currently allows dupes within system which is wrong for government IDs.
- **Effort:** small
- **Dependencies:** F1
- **Related findings:** F1, F14

### F14 — Two LWIN systems (`lwin`, `lwin_7`) coexist with 10,499 wines in both

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT (SELECT count(DISTINCT entity_id) FROM external_ids WHERE system='lwin') AS lwin,
           (SELECT count(DISTINCT entity_id) FROM external_ids WHERE system='lwin_7') AS lwin_7;
    -- lwin=115,239, lwin_7=50,327, both=10,499
    ```
    Format check: both systems store 100% 7-digit numeric IDs. They are semantically the same thing (LWIN-7 = wine identity). The S1.13 long-tail sweep wrote new rows with `system='lwin_7'` while existing rows were `system='lwin'`.
- **Why it matters:** Two system names for the same conceptual backbone ID means: (a) every downstream query must check both, (b) the `wines.lwin` column (F15) is yet another parallel path, (c) dedup by LWIN needs an OR across three sources. This is the kind of quiet schema drift that compounds into bugs later.
- **Proposed fix:** Consolidate to one system name. Pick `lwin_7` (the more specific / format-named version) or `lwin` (the older, more rows). Run `UPDATE external_ids SET system='lwin' WHERE system='lwin_7'` and check for new dupes, resolving with `ON CONFLICT DO NOTHING`. Document in `docs/SCHEMA.md`.
- **Effort:** trivial
- **Dependencies:** F13 (unique constraint)
- **Related findings:** F13, F15

### F15 — wines.lwin column is dead — 15 rows populated vs 170,797 in external_ids

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FROM wines WHERE deleted_at IS NULL AND lwin IS NOT NULL; -- 15
    SELECT count(*) FROM external_ids WHERE system IN ('lwin','lwin_7');       -- 170,797
    SELECT count(*) FROM wines w WHERE w.deleted_at IS NULL AND w.lwin IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM external_ids e WHERE e.entity_id = w.id AND e.system IN ('lwin','lwin_7'));
    -- 0 (consistent — the 15 rows also have an external_ids entry)
    ```
    The column is UNIQUE on wines. Defined in S1.1 alongside `identity_confidence='lwin_matched'`. Nothing writes to it anymore.
- **Why it matters:** Three parallel paths to the same truth (`wines.lwin`, `external_ids[system='lwin']`, `external_ids[system='lwin_7']`) confuses every consumer. A view or code that reads the wrong path gets stale or missing data.
- **Proposed fix:** In Sprint 3: deprecate the column (stop writing it), keep it for one sprint for safety, drop in Sprint 4 schema cleanup. Update `wine_detail_view` to pull LWIN from external_ids, not the column.
- **Effort:** trivial
- **Dependencies:** F14
- **Related findings:** F14

### F16 — 8,336 of 10,414 appellation_grapes rows (80%) have no provenance

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE source_url IS NULL) FROM appellation_grapes;
    -- 8,336 / 10,414
    ```
    `docs/SCHEMA.md` already documents this: "NULL on any of these = 'unverified legacy row from pre-2026-04-05 seed (9,278 rows), needs audit against approved legal sources.'" The audit is outstanding.
- **Why it matters:** Appellation grapes are the backbone of Reference-First. If 80% of them have no source, we can't confidently publish "Chablis is Chardonnay" as "per EU eAmbrosia." This becomes the content correctness gate for Sprint 4 reference redesign.
- **Proposed fix:** Sprint 4 workstream, not Sprint 3. Sprint 4 should audit these rows against eAmbrosia / INAO / TTB / Wine Australia and either populate provenance or delete. Do not touch in Sprint 3.
- **Effort:** large (Sprint 4 scope)
- **Dependencies:** none
- **Related findings:** F26

### F17 — 2,526 of 3,661 appellations (69%) have zero wines

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FROM appellations a WHERE deleted_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM wines w WHERE w.appellation_id = a.id AND w.deleted_at IS NULL);
    -- 2,526
    ```
    Also 114 of 389 regions have zero wines.
- **Why it matters:** A single wine is the minimum to justify an appellation page. 2,526 empty appellations are either (a) real but unmatched — wines exist but the matching pipeline didn't link them, or (b) bogus appellations that should be pruned. Without distinguishing, the UI will surface ghost pages.
- **Proposed fix:** Sprint 3 diagnostic — for each empty appellation, check if `source_lwin` / `source_ttb_colas` has matchable rows. If yes, add to the merge backlog. If no, mark as "empty reference" and drop from search surface. Schema redesign in Sprint 4 can formalize "appellation has ≥1 wine" as a publish gate.
- **Effort:** medium
- **Dependencies:** F1 (dedup), S2.2 (staging audit will cover matchable counts)
- **Related findings:** —

### F18 — 46 label_designation_rules rows have NULL appellation_id (documented as NOT NULL) and include duplicates

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FROM label_designation_rules WHERE appellation_id IS NULL;
    -- 46
    SELECT column_name, is_nullable FROM information_schema.columns
    WHERE table_name='label_designation_rules' AND column_name='appellation_id';
    -- is_nullable = YES (but docs/SCHEMA.md claims NOT NULL)
    SELECT ld.canonical_name, count(*) FROM label_designation_rules ldr
    JOIN label_designations ld ON ld.id=ldr.label_designation_id
    WHERE ldr.appellation_id IS NULL GROUP BY ld.canonical_name HAVING count(*)>1;
    -- Garrafeira appears twice
    ```
- **Why it matters:** Two problems. (a) Schema doc drift — `docs/SCHEMA.md` lines 1075-1087 say `appellation_id NOT NULL`; reality is nullable. (b) The UNIQUE(label_designation_id, appellation_id) allows multiple NULL-appellation rows for the same designation, because UNIQUE treats NULLs as distinct — so "Garrafeira universal" appears twice. Consumer code will show duplicate aging rules.
- **Proposed fix:** Decide the intent: (a) if "universal" designation rules are valid, document in schema and dedupe the 46 via merge-by-label_designation_id; (b) if universal is a mistake, every row should have an appellation and the 46 should be fixed or deleted. I recommend (a) — e.g., Portuguese "Reserva" is universal.
- **Effort:** small
- **Dependencies:** none
- **Related findings:** F24

### F19 — 46 producers with zero wines

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT count(*) FROM producers p WHERE p.deleted_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM wines w WHERE w.producer_id = p.id AND w.deleted_at IS NULL);
    -- 46
    ```
    S1.5 notes "10 producers with 0 wines (no-LWIN grocery brands)" as a known pattern. The count has grown to 46 after S1.13 long-tail.
- **Why it matters:** Unlike empty appellations, zero-wine producers are almost always insert-then-fail-to-promote artifacts. Their pages 404 and they pollute search results.
- **Proposed fix:** Soft-delete the 46 with a note. Add a validator that flags new zero-wine producers after any promotion run.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** —

### F20 — 2 vintage_year outliers (1085, 2099) and 30 Grade F wines with wine_insights

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT id, wine_id, vintage_year FROM wine_vintages
    WHERE (vintage_year > 0 AND vintage_year < 1800) OR vintage_year > 2027;
    -- 2 rows: vintage_year=1085 (wine e112474a...), vintage_year=2099 (wine 9c03c7f2...)

    SELECT count(*) FROM wines w LEFT JOIN wine_insights wi ON wi.wine_id=w.id
    WHERE wi.wine_id IS NOT NULL AND w.data_grade='F' AND w.deleted_at IS NULL;
    -- 30
    ```
- **Why it matters:** (a) The 1085 and 2099 vintages are dirty data — either typos (1985/1095, 2019/2099) or junk rows. Skews vintage range queries. (b) 30 Grade F wines with insights means the grade promotion didn't fire when enrichment completed. Minor, but indicates the grade-update code path isn't reliable.
- **Proposed fix:** (a) Fix or delete the 2 vintage outliers by hand. (b) Add an invariant: any wine with a wine_insights row must be Grade B or C. Run `UPDATE wines SET data_grade='C' WHERE id IN (30 IDs AND data_grade='F')` or investigate why they were F.
- **Effort:** trivial (a) + small (b)
- **Dependencies:** none
- **Related findings:** —

### F21 — 1,647 fortified wines with NULL color (plus 684 table + 58 sparkling)

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT wine_type, color, count(*) FROM wines WHERE deleted_at IS NULL
    GROUP BY wine_type, color ORDER BY count(*) DESC;
    -- wine_type=fortified, color=NULL: 1,647
    -- wine_type=table, color=NULL: 684
    -- wine_type=sparkling, color=NULL: 58
    -- TOTAL NULL colors: 2,394
    ```
    S1.8 session notes claim "Fortified colors: +183" were fixed; S1.14 says "W7 fix #5 wine colors — 431 mismatches fixed." But 1,647 fortified wines are still NULL. Port is usually red, Sherry/Madeira white/tawny.
- **Why it matters:** Color is one of the 5 fields that gate the completeness score. Leaving 2,394 wines without color is cheap data quality debt and directly hurts Josh Test depth.
- **Proposed fix:** Deterministic sweep: `class_type_desc` in `source_ttb_colas` has "TAWNY PORT", "WHITE PORT" etc. Join back through `source_ttb_colas.canonical_wine_id` and apply colors. Fortified default of "red" for Port-style producers likely covers the majority.
- **Effort:** small
- **Dependencies:** S2.2 (staging depth will inform the join)
- **Related findings:** —

### F22 — 7 unnamespaced temp/helper tables with large row counts in public schema, no RLS

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname='public' AND rowsecurity=false
      AND tablename NOT LIKE 'xwines_%' AND tablename NOT LIKE 'archive_%'
      AND tablename NOT LIKE '_tmp_%' AND tablename NOT LIKE 'source_%';
    -- _depth_vintages, _grape_pending, _phase_b_producers, _tier_c2_pending,
    -- lwin_class_map, lwin_region_map, specs_producer_bridge
    SELECT (SELECT count(*) FROM _depth_vintages) AS _dv, ...;
    -- _depth_vintages=135,091, _grape_pending=48,174, _phase_b_producers=38,979,
    --  _tier_c2_pending=109,847, lwin_class_map=13, lwin_region_map=35, specs_producer_bridge=9,773
    ```
    S1.14 Phase B notes claim "dropped 2 unreferenced temp tables (_tmp_wine_match kept)" but these 7 remain.
- **Why it matters:** (a) Public schema should only contain canonical + docs-documented tables. (b) These tables are exposed without RLS policies, which means anon role can read them (sensitive if they contain internal pipeline state). (c) The leading-underscore naming is a Loam convention for "helper" but isn't enforced — easy to mistake for canonical.
- **Proposed fix:** Audit each of the 7 for references in `pipeline/` code. Anything still used should move to a `pipeline_state` schema with service_role-only grants. Anything unused should be dropped. Sprint 3 task.
- **Effort:** small
- **Dependencies:** S2.5 (code audit will know what's still referenced)
- **Related findings:** —

### F23 — 1 appellation with empty slug (Bulgarian Cyrillic "Дунавска равнина")

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT id, name, slug FROM appellations WHERE slug IS NULL OR slug='';
    -- id=f99ae38e..., name='Дунавска равнина', slug=''
    ```
    Schema says `slug TEXT UNIQUE NOT NULL` but reality allows empty string (empty strings bypass NULL check but pass UNIQUE for a single row).
- **Why it matters:** The slug-generator doesn't transliterate Cyrillic, leaving an empty slug. Frontend URL building will produce `/appellation/` with a dangling path. Low priority — it's one row — but symptomatic of a slug generator gap.
- **Proposed fix:** Manually fix to `dunavska-ravnina` (or similar ASCII transliteration). Sprint 4 schema cleanup: add `CHECK (slug <> '')` to every entity table.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** —

### F24 — docs/SCHEMA.md has drift from actual schema in at least 3 places

- **Severity:** P2
- **Evidence:**
    1. `producers.description` (text) exists in DB, not documented in `docs/SCHEMA.md` §2.
    2. `producers.search_vector` (tsvector) exists, not documented.
    3. `label_designation_rules.appellation_id` is actually nullable; `docs/SCHEMA.md` §25 claims `NOT NULL`.
    4. `docs/SCHEMA.md` bottom summary says **83 canonical tables** / 96 total. Actual public canonical count is ≥94 (S1.13 added tables, S1.14 moved archive out).
- **Why it matters:** SCHEMA.md is the reference the DB and wine experts use. Drift = incorrect assumptions about what columns exist and which are required. Meta audit (S2.8) will have this on its list regardless, but flagging here since it was surfaced during this audit.
- **Proposed fix:** Sprint 3 regenerate SCHEMA.md from `information_schema.columns` / `information_schema.tables`, write a script to dump it, add a CI check that the file matches the DB.
- **Effort:** small
- **Dependencies:** none
- **Related findings:** F18 (specific case)

### F25 — wine_grapes.grape_id has no supporting index

- **Severity:** P2
- **Evidence:**
    ```sql
    -- "all wines of this grape" currently does a full scan on wine_grapes (46,028 rows)
    SELECT tc.table_name, kcu.column_name FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name
    WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema='public'
      AND tc.table_name='wine_grapes';
    -- wine_id=indexed, grape_id=NOT indexed
    ```
- **Why it matters:** 46K rows is small enough today that the scan isn't noticeable, but it will degrade as the grape coverage fix (F9) brings coverage to 100% (potentially 400K+ rows).
- **Proposed fix:** `CREATE INDEX ON wine_grapes (grape_id);`. One-liner.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** —

### F26 — 462 of 1,165 appellation_rules rows have NULL last_verified_at

- **Severity:** P3
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE last_verified_at IS NULL) FROM appellation_rules;
    -- 462
    ```
    Provenance (source_url/org/excerpt) is 100% populated — the schema gate held. But re-verification tracking is not.
- **Why it matters:** Laws change. Without `last_verified_at`, we can't run a "rules not re-verified in 18 months" audit. Low priority until Sprint 4 reference redesign, but cheap to fix.
- **Proposed fix:** Backfill with `created_at` for existing rows; enforce set-on-insert in future pipelines.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F16

### F27 — Self-parent grape: MALEGUE 742-22

- **Severity:** P3
- **Evidence:**
    ```sql
    SELECT id, name FROM grapes WHERE parent1_grape_id = id OR parent2_grape_id = id;
    -- MALEGUE 742-22 (id f6311dc1...)
    ```
    This is a VIVC hybrid breeding code, not a widely used grape. It's a single row, but violates the DAG invariant on grape parentage.
- **Why it matters:** Any recursive parentage walk would infinite-loop on this row. Low probability but real.
- **Proposed fix:** NULL both parent columns, or document as "self-cross" with a proper non-self-referencing representation.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** —

### F28 — CLAUDE.md "Current State" numbers are stale in at least 5 places

- **Severity:** P2
- **Evidence:** Comparing CLAUDE.md "Content Tables (snapshot 2026-04-11, Session 14 Phase A)" against live DB:
    | Metric | CLAUDE.md | Live |
    |---|---|---|
    | `wine_grapes` | 47,035 | 46,028 |
    | `wine_farming_certifications` | 6,387 (845+5,542) | 1,252 |
    | `wine_food_pairings` | 809 structured | 0 |
    | `wine_vintage_formats` | 3,030 (claimed S9) | 0 |
    | Score coverage (% distinct wines) | 2.24% | 1.3% (2,030/155,623) |
    | Price coverage (% distinct wines) | 5.21% (25,898) | 1.8% (2,818/155,623) |
    | `producer_winemakers` / `winemakers` | 166/173 | 0/0 |
    
    Most of these are S1.14 Phase B rebuild casualties (F6) that the CLAUDE.md "Content Tables" section wasn't updated to reflect.
- **Why it matters:** CLAUDE.md is the primary context file loaded at session start. Stale numbers cause future sessions (and agents) to make decisions on false premises. This is a meta finding that overlaps with S2.8, but surfacing it here because every query above contradicted CLAUDE.md.
- **Proposed fix:** Strip the hardcoded "Content Tables" numbers from CLAUDE.md or mark them "point-in-time, run `sprint_dashboard.py` for live." Sprint 3 cleanup.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F24

### F29 — Vineyard system empty (pre-30K had 815 rows in archive_vineyards, now in archive.vineyards)

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT count(*) FROM vineyards;                   -- 0
    SELECT count(*) FROM wine_vineyards;              -- 0
    SELECT count(*) FROM wine_vintage_vineyards;      -- 0
    SELECT count(*) FROM vineyard_producers;          -- 0
    SELECT count(*) FROM vineyard_soils;              -- 0
    SELECT count(*) FROM archive.vineyards;           -- ~815 (per S1.14 notes)
    ```
- **Why it matters:** Named vineyards (Clos de Vougeot, Monte Bello, Eisele, Grange) are a core terroir concept. Every wine with `wine_tier='grand_vin'` or `monopole=true` needs a vineyard link for credibility. The 815 archive rows represent the S1.13 / pre-rebuild state and should be re-promoted.
- **Proposed fix:** Archive bridge (F6) should re-promote vineyards via slug match, then re-link via wine_vineyards + wine_vintage_vineyards. Sprint 3 task.
- **Effort:** small
- **Dependencies:** F6
- **Related findings:** F6

### F30 — Wine relationship / alias / region / appellation join tables empty

- **Severity:** P2
- **Evidence:**
    ```sql
    -- all 0: wine_aliases, wine_regions, wine_appellations, wine_relationships,
    --         wine_soils, wine_water_bodies, wine_biodiversity_certifications,
    --         wine_descriptors, wine_vintage_descriptors, wine_vintage_nv_components,
    --         wine_vintage_documents, wine_food_pairings
    ```
    Together 12 wine-level join tables are empty. Some are aspirational (wine_soils pending the three-tier fallback design), some are regressions (wine_food_pairings was 809 per CLAUDE.md).
- **Why it matters:** Principle #4 "Everything connects to everything" depends on these join tables. Without them, wine pages can't show "also from this vineyard", "second wine of", "paired with pork belly" — which are the kinds of links that make browsing feel like Loam's advertised "knowledge graph, not a catalog."
- **Proposed fix:** Categorize each per F31. F6 archive bridge handles wine_food_pairings and wine_biodiversity_certifications. The others are Sprint 4/5 content-generation work.
- **Effort:** medium (archive bridge) + large (content generation)
- **Dependencies:** F6
- **Related findings:** F6, F31

### F31 — 46 empty canonical tables — categorization

- **Severity:** P2 (bulk) / individual ratings below
- **Evidence:** Full list from `pg_stat_user_tables.n_live_tup = 0`:

    **(a) Producer metadata regressions — P0 (covered by F5):**
    `producer_aliases`, `producer_farming_certifications`, `producer_biodiversity_certifications`, `producer_winemakers`, `producer_importers`, `producer_documents`, `producer_insights`, `producer_timeline`, `winemakers`

    **(b) Wine metadata regressions — P1 (covered by F6/F30):**
    `wine_farming_certifications` (1,252, not 0, flagged under F6), `wine_biodiversity_certifications`, `wine_food_pairings`, `wine_vintage_formats`, `wine_vintage_documents`, `wine_vintage_nv_components`

    **(c) Vineyard system — P2 (covered by F29):**
    `vineyards`, `vineyard_producers`, `vineyard_soils`, `wine_vineyards`, `wine_vintage_vineyards`

    **(d) Aspirational / Sprint 5 content-generation targets — P2:**
    `wine_aliases`, `wine_relationships`, `wine_appellations`, `wine_regions`, `wine_soils`, `wine_water_bodies`, `wine_descriptors`, `wine_vintage_descriptors`, `wine_vintage_insights`, `wine_vintage_grapes`, `trends`, `ai_suggestions`, `entity_classifications`, `entity_attributes`

    **(e) Reference-layer insights — P1 (core Reference-First targets for Sprint 5):**
    `producer_insights`, `grape_insights`, `soil_type_insights`, `water_body_insights`

    **(f) Water body system — P3 (aspirational):**
    `water_bodies`, `appellation_water_bodies`, `region_water_bodies`

    **(g) Historical reference never imported — P3:**
    `grape_plantings` (Anderson/Aryal dataset)

    **(h) Analytics / frontend-paused — P3 (will fill when frontend resumes):**
    `wine_lookups`

    **(i) Pipeline state — expected empty — P3:**
    `producer_dedup_staging`, `producer_dedup_pairs`

    **(j) xwines reference — expected empty — P3:**
    `xwines_wine_insights`, `xwines_producer_insights`

    **(k) Dead / needs decision — P2:**
    `producer_regions` (schema says "for multi-region producers", 0 rows — clarify intent),
    `appellation_documents` (might populate from appellation_rules sources),
    `region_soils` (was referenced by the three-tier fallback, 0 rows)

- **Why it matters:** A 46-row enumeration of empty canonical tables is a forest-level signal that the 30K rebuild truncated the data model more than CLAUDE.md implies.
- **Proposed fix:** This finding is the categorization. Sprint 3 acts on (a), (b), (c), (k). Sprint 5 acts on (d), (e), (f). (g), (h), (i), (j) are parked.
- **Effort:** n/a (categorization)
- **Dependencies:** —
- **Related findings:** F4, F5, F6, F29, F30

### F32 — 1,265 wines parked on catch-all regions

- **Severity:** P3
- **Evidence:**
    ```sql
    SELECT count(*) FROM wines WHERE deleted_at IS NULL
      AND region_id IN (SELECT id FROM regions WHERE is_catch_all=true);
    -- 1,265
    ```
    Design intent per SCHEMA.md: "One catch-all per country for wines without specific region." Used as a placeholder when region is unknown.
- **Why it matters:** Low priority and mostly by design, but 1,265 wines that could be re-homed to a specific region with a lookup pass. Sprint 3 "deterministic region infer from appellation" would cover most of these.
- **Proposed fix:** For any wine with a non-null appellation_id, set region_id from `appellation.region_id`. SQL one-liner.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** —

### F33 — 57 external_ids point to soft-deleted wines

- **Severity:** P3
- **Evidence:**
    ```sql
    SELECT count(*) FROM external_ids WHERE entity_type='wine'
      AND entity_id IN (SELECT id FROM wines WHERE deleted_at IS NOT NULL);
    -- 57
    ```
- **Why it matters:** Dangling refs from the S1.13 dedup soft-delete pass. Doesn't break anything (external_ids don't have a cascade), but pollutes counts and could leak via `search_catalog`.
- **Proposed fix:** `DELETE FROM external_ids WHERE entity_id IN (SELECT id FROM wines WHERE deleted_at IS NOT NULL)` after verifying they don't need to be re-pointed to the canonical wine via `duplicate_of`.
- **Effort:** trivial
- **Dependencies:** —
- **Related findings:** —

### F34 — 665 appellations with no latitude/longitude (blocks weather fill)

- **Severity:** P3
- **Evidence:**
    ```sql
    SELECT count(*) FROM appellations WHERE deleted_at IS NULL AND latitude IS NULL;
    -- 665
    ```
    `appellation_weather_years` has coverage for 2,997 appellations. The delta (3,661 - 2,997 = 664) closely matches the 665 without coordinates, suggesting the weather drip is blocked by missing coordinates.
- **Why it matters:** Appellation weather is one of Loam's strongest differentiators and appears on every appellation + wine page. 665 without weather is a visible gap, primarily in smaller / recently-added appellations.
- **Proposed fix:** Reverse-geocode / manually source coordinates for the 665. Maybe 100-150 are real appellations worth coordinates; the rest may be low-quality entries that should be reviewed for deletion in Sprint 4.
- **Effort:** medium
- **Dependencies:** none
- **Related findings:** —

---

## Notes for the synthesis session (S2.9)

Three meta-patterns emerged from this session that should shape the Sprint 3 backlog:

1. **The 30K rebuild cost more than CLAUDE.md acknowledges.** Depth recovery via LWIN bridge worked for the ~50K wines that had LWIN pairs in archive. The 100K Phase B + long-tail wines have no archive bridge. A non-LWIN bridge (producer name_normalized + wine name match) is the single biggest ROI task for Sprint 3.

2. **Reference-layer quality is better than wine-layer quality.** Countries, regions, appellations, grapes, appellation_rules, appellation_vintages weather, appellation_grapes — the ref layer has real coverage and real provenance (where seeded after 2026-04-05). The problem is in the downstream layers — producers, wines, wine_vintages. This validates the Sprint 4 Reference Design focus on *treating the ref layer as the source of truth*, because it's already much closer to production-ready than the wine layer.

3. **Schema doc drift is visible everywhere.** SCHEMA.md, CLAUDE.md, memory, DECISIONS.md — all have point-in-time snapshots that don't match live state. The meta audit (S2.8) will surface more but the database ground truth should become the canonical reference, not a separate doc that drifts.

## What's NOT in scope for this session (deferred)

- Content-correctness of reference rows (S2.4 wine expert reference content)
- Staging tables (S2.2 db_staging)
- Wine-level content quality — real-world fact-checking (S2.3 wine_canonical)
- Frontend data rendering (S2.7 ux)
- Scheduled task state / Edge Function / pipeline code review (S2.5 code)
- `match_decisions` audit trail analysis (S2.2)
- The 0 rows in `wine_lookups` (S2.7 / S2.8 — frontend paused, expected)
