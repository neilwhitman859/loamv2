# DB Staging Audit — Findings

**Session:** S2.2
**Date:** 2026-04-11
**Expert:** db_staging
**Scope:** all 32 `source_*` staging tables — row counts, merge state (`canonical_wine_id` / `canonical_producer_id` / `processed_at`), value-left-on-table vs canonical, data quality (duplicates, encoding, outliers, staleness), audit trail (`match_decisions`), schema conventions. Canonical content excluded (that was S2.1).
**Method:** read-only SQL via Supabase MCP `execute_sql`. ~35 queries. No AI calls, no DDL/DML, no fixes.
**Budget:** $0.00

## Ground truth (as of audit start)

```
32 staging tables, 4,734,132 total rows, 7,065 MB
source_ttb_colas       3,283,319    source_kermit_lynch        1,468
source_pro_platform      346,080    source_claude_knowledge      920
source_lwin              189,359    source_winebow               536
source_tabc              182,933    source_european_cellars      443
source_berliner           73,896    source_empson                279
source_kansas_brands      65,476    source_domestique            247
source_wv_abca            55,093    source_kermit_lynch_growers  193
source_texsom             46,896    source_last_bottle           160
source_specs              21,913
source_wallys             19,446    source_types                 30  (taxonomy, NOT staging)
source_systembolaget      12,646
source_enofile             9,166
source_lcbo                7,030
source_horizon             6,441
source_pa                  5,905
source_skurnik             5,541
source_openfoodfacts       5,176
source_flatiron            4,130
source_bc_liquor           3,200
source_winedeals           3,200
source_utah_dabs           2,834
source_firstleaf           1,770
source_polaner             1,680
source_best_wine_store     1,658
```

Staging freshness: entire estate was loaded in a single 8-day window **2026-03-18 → 2026-03-25** (TTB tail through 03-25, Utah DABS added 04-06). Nothing has been re-loaded since. No incremental refresh pipeline exists.

## Summary

**Total findings:** 31
- **P0** (broken / correctness-critical / blocks merge): **6**
- **P1** (significant gap, must fix before enrichment): **11**
- **P2** (improvement, not blocking): **9**
- **P3** (nice to have): **5**

**Biggest risks (P0):**
1. **286,918 wine_id pointers in staging are dangling archive references** — 29 of 31 wine-bearing staging tables were never re-linked after the 30K rebuild. Only `source_ttb_colas` and `source_lwin` were fixed in S13. Every other source (pro_platform, tabc, texsom, specs, wallys, etc.) still points at `archive.wines` UUIDs that no longer exist in `public.wines`. **This is the single biggest blocker to Sprint 3 depth recovery.** The S2.1 F6 depth-loss finding was framed as "promotion never ran"; the true root cause is "promotion can't run because the join keys are dead."
2. **`processed_at` is silently never written in 14 of 32 sources.** source_pro_platform, source_tabc, source_kansas_brands, source_wv_abca, source_horizon, source_openfoodfacts, source_skurnik, source_kermit_lynch, source_winebow, source_european_cellars, source_empson, source_polaner, source_winedeals, source_kermit_lynch_growers all show `processed_at = NULL` for every row, including rows that were successfully matched. You cannot distinguish "unmatched because not yet processed" from "unmatched because no match found" in any of these tables. The match engine has no idempotent stop-state.
3. **Value-left-on-table is ~60K prices, ~48K scores, ~200K vintage-grade fields, ~200K ABVs, ~40K barcodes** locked behind the broken links above. Even with perfect retail_promote scripts, none of this will promote until the archive-ID relink runs.
4. **`source_systembolaget` and `source_lcbo` are exactly 2x duplicated** from a double-load. Systembolaget: 6,298 distinct product_ids × ~2 rows = 12,646. LCBO: 3,494 distinct SKUs × exactly 2 = 7,030. Half of each table is pure noise that will break any unique-key promotion and inflates "coverage" numbers.
5. **`match_decisions` is a designed-for-purpose cross-source audit trail that has never been used for cross-source matching.** 7,641 rows total, 100% `source_a='wines' AND source_b='wines'` (self-dedup from S1.13). Zero rows trace a staging→canonical merge decision. Columns `match_method`, `source_a_id`, `source_b_id`, `ai_extracted_data` are dead for the original purpose. When Sprint 3 runs the merge engine we have no reproducible trail of what decision was made where.
6. **`source_ttb_colas.abv` has 93,407 malformed text values.** HTML entities not decoded (`&lt;14%`, `&gt;12%`, `&quot;Table&quot;`), negative values (`-14`, `-14%`), missing-leading-zero decimals (`.12`, `.13`), format errors (`!3.5`, `%13.2`, `12..5`, `"table"`). The abv is stored as free-text and no normalization layer sits between staging and `wine_vintages.abv`.

**Biggest wins (things that are correct and did not need flagging):**
- **Producer linking is clean.** `canonical_producer_id` orphan checks across 11 major staging tables all return 0 — S13's producer re-link was thorough. The orphan crisis is 100% wine-side.
- **`source_ttb_colas` is the gold standard for linking discipline.** 801,258 producer matches, 83,183 wine matches, 0 archive-ID orphans, real `processed_at` coverage, 100% detail-scrape discipline, working `idx_ttb_unlinked_by_producer` partial index for resumption.
- **`source_lwin` linking survived** the 30K rebuild intact — 0 archive-ID orphans, clean 1:1 match between `processed_at` and `canonical_wine_id` on the 119,889 matched rows.
- **Encoding is mostly clean.** `source_tabc`, `source_wv_abca`, `source_kansas_brands`, `source_wallys`, `source_lwin`, `source_kansas_brands`, `source_ttb_colas.grape_varietals` all show 0 mojibake. The `source_texsom` 802-row issue is localized to one importer, not systemic.
- **No `canonical_wine_id` in staging points at a soft-deleted wine.** The 286K orphan pointers are either gone entirely or still live in `archive.wines`. No surprise soft-deletes to untangle.
- **Natural-key uniqueness is mostly enforced.** `source_ttb_colas.ttb_id`, `source_lwin.lwin`, `source_pro_platform.cola_number`, `source_tabc.ttb_number`, `source_wallys.shopify_id`, `source_kermit_lynch.kl_id` — zero within-source duplicates.

**Scope-breaker check:** None. Every finding executes inside the Sprint 3 envelope. F1 (archive-ID relink) reframes the depth-recovery workstream — instead of "write the non-LWIN archive depth bridge" it becomes "relink first, then promote." That's a scope refinement, not a rewrite.

---

## Findings

### F1 — 286,918 wine_id pointers in staging are dangling archive references (29 of 31 wine-bearing staging tables)

- **Severity:** P0
- **Evidence:**
    ```sql
    -- Confirmed all orphans live in archive.wines, not public.wines
    SELECT count(*) FROM source_pro_platform s
     WHERE s.canonical_wine_id IS NOT NULL
       AND EXISTS (SELECT 1 FROM archive.wines a WHERE a.id = s.canonical_wine_id);
    -- 84,264 (100% of pro_platform wine matches)
    ```
    Full scan across 29 tables:

    | table | matched | orphaned | orphan % |
    |---|---:|---:|---:|
    | source_pro_platform   | 84,264 | 84,264 | 100.0% |
    | source_tabc           | 52,192 | 52,192 | 100.0% |
    | source_wv_abca        | 22,161 | 22,161 | 100.0% |
    | source_texsom         | 21,676 | 19,995 | 92.2% |
    | source_specs          | 17,157 | 16,252 | 94.7% |
    | source_wallys         | 17,835 | 16,001 | 89.7% |
    | source_kansas_brands  | 12,746 | 12,746 | 100.0% |
    | source_systembolaget  | 12,460 | 12,460 | 100.0% |
    | source_enofile        |  9,117 |  8,942 | 98.1% |
    | source_pa             |  5,905 |  5,905 | 100.0% |
    | source_lcbo           |  5,268 |  5,268 | 100.0% |
    | source_berliner       |  4,878 |  4,693 | 96.2% |
    | source_horizon        |  4,566 |  4,566 | 100.0% |
    | source_flatiron       |  4,096 |  4,022 | 98.2% |
    | source_skurnik        |  4,016 |  3,584 | 89.2% |
    | source_winedeals      |  3,145 |  3,145 | 100.0% |
    | source_utah_dabs      |  2,181 |  2,181 | 100.0% |
    | source_best_wine_store|  1,657 |  1,657 | 100.0% |
    | source_bc_liquor      |  1,280 |  1,280 | 100.0% |
    | source_polaner        |  1,111 |  1,111 | 100.0% |
    | source_openfoodfacts  |  1,042 |  1,042 | 100.0% |
    | source_firstleaf      |    922 |    922 | 100.0% |
    | source_kermit_lynch   |    891 |    865 | 97.1% |
    | source_claude_knowledge |  860 |    860 | 100.0% |
    | source_winebow        |    284 |    264 | 93.0% |
    | source_domestique     |    247 |    247 | 100.0% |
    | source_european_cellars |  238 |    232 | 97.5% |
    | source_empson         |    221 |    221 | 100.0% |
    | source_last_bottle    |     41 |     41 | 100.0% |
    | **clean** | | | |
    | source_ttb_colas      | 83,183 |      0 | 0.0% |
    | source_lwin           | 119,889 |     0 | 0.0% |
- **Why it matters:** Every promotion pipeline that joins `staging.canonical_wine_id → public.wines.id` is effectively a no-op on 29 of 31 staging tables. S2.1 F6 flagged the depth-loss (`wine_vintage_scores` 27K→5.4K, `wine_vintage_prices` 140K→23K, `wine_farming_certifications` 9.4K→1.3K) and the top Sprint 3 recommendation was a non-LWIN archive depth bridge. This finding rewrites that recommendation: the bridge must start with a relink pass. You cannot promote a price linked to a wine UUID that doesn't exist.
- **Proposed fix:** Reuse the S13 pattern. Build `archive_to_current_wine_map` (archive.wines.id → public.wines.id via `archive.producers.name_normalized + archive.wines.name_normalized` → `producers.name_normalized + wines.name_normalized` match), then bulk UPDATE each of the 29 staging tables via direct Postgres (not REST). S13 did this for `source_ttb_colas` (3.28M rows) with `apply_migration` + 30-min statement_timeout in 207 seconds. The 29 tables here total ~1.6M rows (mostly pro_platform and tabc) — estimate <10 min total wall-clock. Do `_tmp_wine_match` refresh as part of the same script. Producer re-link was already done in S13 — only wine_id needs fixing.
- **Effort:** medium (1 session for script + execution + verification)
- **Dependencies:** none — `archive.wines` still exists from 30K rebuild
- **Related findings:** S2.1 F6, F18 (depth loss), F19 (staging promote gap)

### F2 — `processed_at` is silently never written in 14 of 32 staging tables

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT 'source_pro_platform' AS t,
           count(*) AS total,
           count(*) FILTER (WHERE processed_at IS NOT NULL) AS processed,
           count(*) FILTER (WHERE canonical_wine_id IS NOT NULL) AS matched
      FROM source_pro_platform;
    -- total=346080  processed=0  matched=84264
    ```
    Full list of tables where `processed_at` is 100% NULL despite having rows with `canonical_wine_id IS NOT NULL`:
    - source_pro_platform (0 processed / 84,264 matched)
    - source_tabc (0 / 52,192)
    - source_kansas_brands (0 / 12,746)
    - source_wv_abca (0 / 22,161)
    - source_horizon (0 / 4,566)
    - source_openfoodfacts (0 / 1,042)
    - source_skurnik (0 / 4,016)
    - source_kermit_lynch (0 / 891)
    - source_kermit_lynch_growers (0 / 44)
    - source_winebow (0 / 284)
    - source_european_cellars (0 / 238)
    - source_empson (0 / 221)
    - source_polaner (0 / 1,111)
    - source_winedeals (0 / 3,145)
- **Why it matters:** `processed_at` was designed as the idempotent cursor for the match engine — "skip rows where `processed_at IS NOT NULL`." In 14 sources it is dead; in others it is inconsistent (source_berliner processed=8,205 vs matched=4,878; source_specs processed=17,147 vs matched=17,157). There is no way to distinguish "never processed" from "processed and no match found." Re-running promotion on these tables will either (a) redo all the matched rows and duplicate-promote, or (b) skip the table entirely and leave fresh data unprocessed. Both are silent failures.
- **Proposed fix:** Standardize the semantic: `processed_at` means "the merge engine has touched this row." Backfill `processed_at = now()` on all rows where `canonical_wine_id IS NOT NULL` or where the promotion script completed (after F1 relink). Going forward, every promotion script must write `processed_at` at the same moment it writes `canonical_wine_id`. Enforce via a shared `pipeline/lib/merge.py` helper — there is already an `importer.py` / `merge.py` pair under `pipeline/lib/`.
- **Effort:** small (one UPDATE per table + helper function fix)
- **Dependencies:** F1 (relink must happen first, or the backfill writes `processed_at` on orphan links)
- **Related findings:** F3, F12, F23

### F3 — Value-left-on-table is enormous and almost entirely unreachable until F1 is fixed

- **Severity:** P0
- **Evidence:** Aggregated per value column, counting rows where the staging value is set AND `canonical_wine_id IS NOT NULL`:

    **Prices (staging has 75,100 linked, canonical `wine_vintage_prices` has 23,220 — shortfall ~52K):**

    | source | staging linked | promoted to canonical | shortfall |
    |---|---:|---:|---:|
    | source_wallys | 17,835 | 14,336 | 3,499 |
    | source_specs | 10,808 | 5,544 | 5,264 |
    | source_systembolaget | 12,460 | 644 | 11,816 |
    | source_enofile | 9,115 | 412 | 8,703 |
    | source_pa | 5,905 | 728 | 5,177 |
    | source_lcbo | 5,268 | 838 | 4,430 |
    | source_flatiron | 4,096 | 225 | 3,871 |
    | source_winedeals | 3,145 | 7 | 3,138 |
    | source_utah_dabs | 2,181 | 117 | 2,064 |
    | source_bc_liquor | 1,280 | 128 | 1,152 |
    | source_best_wine_store | 1,657 | 171 | 1,486 |
    | source_firstleaf | 922 | 0 | 922 |
    | source_domestique | 247 | 0 | 247 |
    | source_last_bottle | 41 | 0 | 41 |

    **Scores / medals (staging has ~53K linked, canonical `wine_vintage_scores` has 5,420 — shortfall ~48K):**

    | source | staging linked | promoted |
    |---|---:|---:|
    | source_texsom | 21,676 | 4,473 |
    | source_specs | 17,157 (rating) | 0 |
    | source_enofile | 9,117 (award) | 0 |
    | source_berliner | 4,878 (medal) | 680 |
    | source_bc_liquor | 988 (rating) | 0 |
    | source_european_cellars | 203 (scores JSON) | 0 |
    | source_winebow | 146 (scores JSON) | 0 |

    **ABV (staging has 215,993 linked, canonical `wine_vintages.abv` has 48,700):**
    - source_pro_platform: 75,257 linked
    - source_tabc: 52,171 linked
    - source_ttb_colas: 36,669 linked
    - source_wv_abca: 17,247 linked
    - source_kansas_brands: 12,745 linked
    - source_systembolaget: 12,460 linked
    - source_lcbo: 5,266 linked
    - source_winedeals: 2,045 linked
    - ... (+400 from other importers)

    **Appellation text (staging has ~190K linked with appellation string, canonical 104,788 wines have `appellation_id`):**
    - source_pro_platform: 79,591
    - source_ttb_colas: 66,097
    - source_texsom: 21,597
    - source_kansas_brands: 12,279
    - source_systembolaget: 7,400
    - source_skurnik: 3,922
    - source_winedeals: 3,144

    **Vintages (staging has ~168K linked, canonical `wine_vintages` has 83,531):**
    - source_pro_platform: 53,048
    - source_ttb_colas: 44,062
    - source_texsom: 21,675
    - source_wv_abca: 16,250
    - source_kansas_brands: 10,616
    - source_systembolaget: 10,270

    **UPC barcodes (staging has ~42K linked, canonical `external_ids.upc` has 13,162 on 6,946 wines):**
    - source_specs: 17,156
    - source_systembolaget product_number: 12,460
    - source_lcbo: 5,266
    - source_horizon: 4,566
    - source_pa: 5,905 (`upcs` jsonb array)
    - source_openfoodfacts barcode: 1,042
    - source_bc_liquor: 1,266
    - source_winedeals: 2,711
- **Why it matters:** This is the mass of the pre-30K merge work currently frozen behind F1. Sprint 3 cannot meaningfully advance price / score / ABV / appellation coverage without first unblocking F1, then re-running promotion. Rough order-of-magnitude: ~60K prices × 1, ~48K scores × 1, ~200K ABVs (dedup to maybe ~80K unique), ~100K vintage rows (dedup to ~40K unique), ~40K UPCs (dedup to maybe ~25K unique). S2.1 F11 noted only 118 numeric critic scores exist — with `source_specs` ratings alone there is 17K waiting.
- **Proposed fix:** This isn't really a separate fix. It's the measurable dividend from F1 + F19 + an updated `retail_promote.py` that handles all 14 price sources, all 8 UPC sources, the 3 score/medal sources, and the 4 importer depth sources in one integrated pass. Budget an entire Sprint 3 workstream.
- **Effort:** large (multi-session — F1 relink, then promotion rewrite, then verification)
- **Dependencies:** F1, F19, F22
- **Related findings:** F1, F18, F19, F22

### F4 — `source_systembolaget` and `source_lcbo` are 2× duplicated from a double-load

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT count(DISTINCT product_id) AS distinct_products, count(*) AS total
      FROM source_systembolaget;
    -- distinct_products=6298  total=12646   (almost exactly 2x)

    SELECT count(DISTINCT sku), count(DISTINCT upc), count(*)
      FROM source_lcbo;
    -- distinct_sku=3494  distinct_upc=3490  total=7030   (exactly 2x)
    ```
    Spot check on product_id 24451887: 4 rows with identical fields except `id` (the UUID PK) and `created_at`. Two distinct `created_at` timestamps — 2026-03-18 17:56:14 and 17:56:45 — suggesting a load that was re-run at ~30s interval.
- **Why it matters:** Every metric coming out of these two tables is inflated 2×. "Systembolaget has 12,460 linked wines" is actually 6,230. Promotion pipelines that use UPC as a natural key will either conflict-error on the dupe or write two wine_vintage_prices rows per SKU. S2.1 F28 flagged that CLAUDE.md counts were stale; this is a parallel issue — the raw staging count is stale.
- **Proposed fix:** Dedupe both tables by natural key (`product_id` for systembolaget, `sku` for lcbo) keeping the later `created_at`. Write a one-shot migration that deletes the duplicate rows, adds a `UNIQUE` index on the natural key to prevent regressions, then verifies counts.
- **Effort:** small (SQL dedup, unique constraint)
- **Dependencies:** none (staging is raw data — safe to dedup before promotion runs)
- **Related findings:** F20, F3

### F5 — `match_decisions` was designed as a cross-source audit trail and has never been used for cross-source matching

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE source_a <> source_b) AS cross_source,
           count(*) FILTER (WHERE source_a='wines' AND source_b='wines') AS self_dedup,
           count(DISTINCT source_a), count(DISTINCT source_b)
      FROM match_decisions;
    -- cross_source=0  self_dedup=7641  distinct_source_a=1  distinct_source_b=1
    ```
    Schema reminder: `source_a`, `source_a_id`, `source_b`, `source_b_id`, `entity_type`, `match_method`, `confidence`, `canonical_wine_id`, `canonical_producer_id`, `status`, `ai_model`, `ai_prompt`, `ai_response`, `ai_extracted_data`, `notes`. Clearly designed for "source X ID A matched source Y ID B, method, confidence, decision."
    Reality: 7,641 rows, 100% wines×wines self-matches from S1.13 dedup classification. Zero trace of any staging→canonical merge decision.
- **Why it matters:** When Sprint 3 runs the merge engine, we will have no reproducible trail of what got matched by what method under what confidence, no way to audit decisions after the fact, and no way to selectively reverse a bad batch. Today's pipeline writes `canonical_wine_id` directly to the staging row with no audit row — the decision is not traceable. Per the S1.13 session notes, `wine_dupe_classify.py` already uses `match_decisions` — that pattern must be lifted to the merge engine.
- **Proposed fix:** Make `pipeline/lib/merge.py` helper insert a `match_decisions` row on every staging→canonical link with `source_a='<staging_table>'`, `source_a_id=<staging PK>`, `source_b='wines'`, `source_b_id=<canonical wine id>`, `match_method=<strict_cola|fuzzy_producer_name|haiku_assisted|...>`, `confidence`, `status='accepted|rejected|flagged'`, and populate `ai_model`/`ai_extracted_data` for AI passes. Backfill is impossible (no source history), but going forward Sprint 3's merge engine leaves a clean trail.
- **Effort:** small (helper function + caller updates)
- **Dependencies:** F1, F10
- **Related findings:** F10, F12

### F6 — `source_ttb_colas.abv` contains 93,407 malformed text values

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FROM source_ttb_colas
     WHERE abv IS NOT NULL AND abv::text NOT SIMILAR TO '\d+(\.\d+)?';
    -- 93407

    SELECT DISTINCT abv FROM source_ttb_colas
     WHERE abv IS NOT NULL AND abv::text NOT SIMILAR TO '\d+(\.\d+)?' LIMIT 20;
    ```
    Sample values: `-14`, `-14%`, `!3.5`, `.12`, `.13`, `&gt;12%`, `&gt;14`, `&lt; 14`, `&lt; 14.0%`, `&lt; 14%`, `&lt;14`, `&lt;14.0`, `&lt;14%`, `&quot;table&quot;`, `&quot;Table&quot;`, `%12`, `%12.5`, `%13`, `%13.2`, `%13.5`. Another example caused a numeric cast error earlier: `12..5`.
- **Why it matters:** `abv` is stored as `text` in `source_ttb_colas` (confirmed against the column list earlier) because TTB doesn't normalize at source. Any promotion that does `UPDATE wine_vintages SET abv = source.abv::numeric` will crash on `12..5` or silently drop `<14%`, `.12`, etc. S2.1 F8 noted ABV coverage is only 58% of wine_vintages — this finding is one of the reasons it's that low.
- **Proposed fix:** Write `pipeline/lib/normalize.py::parse_abv(text)` that: (1) decodes HTML entities via `html.unescape`, (2) strips percent/`%` and whitespace, (3) drops leading `<`, `>`, `!`, `%`, (4) detects `12..5` → `12.5`, (5) applies leading-zero fix (`.12` → NULL, too ambiguous), (6) rejects negative or out-of-range (0-25), (7) rejects the "table" sentinel. Apply to staging-side before reading during promotion — do NOT rewrite the raw staging value.
- **Effort:** small
- **Dependencies:** F1
- **Related findings:** S2.1 F8, F3

### F7 — `source_texsom` has 802 rows with double-encoded mojibake in producer / wine_name

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) FROM source_texsom WHERE producer ~ 'Ã' OR wine_name ~ 'Ã';
    -- 802
    SELECT producer, wine_name FROM source_texsom WHERE producer ~ 'Ã' OR wine_name ~ 'Ã' LIMIT 10;
    ```
    Samples: `Gew?ÃÂºrztraminer` (should be Gewürztraminer), `CardinalÃ¢â¬Å¡ÃâÃÂ´s Crest` (should be Cardinal's Crest — smart apostrophe double-encoded), `Blume?ÃÂ±hof  Vineyards` (should be Blumenhof), `Sylvian?ÃÂ®` (should be Sylvian). This is the classic Windows-1252→UTF-8→Latin-1→UTF-8 double-encode.
- **Why it matters:** 802 rows is 1.7% of source_texsom but 92% of TEXSOM's 21,676 wine-links have already been set — many of these 802 are presumably among the matched rows. Any Sprint 3 work that promotes TEXSOM scores, writes them to `wine_vintage_scores.notes`, or feeds them to an enrichment prompt will surface garbage. And the same wines may appear with correct names in other sources, making dedup harder.
- **Proposed fix:** Run the ftfy-equivalent UTF-8 repair pass in-place on `source_texsom.producer` and `source_texsom.wine_name` (and `wine_appellation` — not yet checked). For consistency, run the same pass against `source_berliner` (118 affected rows found). Keep a `_raw_producer` / `_raw_name` column for audit if needed.
- **Effort:** small
- **Dependencies:** none
- **Related findings:** F3 (TEXSOM score promotion)

### F8 — Three staging sources are archival / dead (WV ABCA API, Horizon API, OpenFoodFacts stale)

- **Severity:** P1
- **Evidence:**
    - **source_wv_abca** (55,093 rows, all created 2026-03-19, never refreshed). CLAUDE.md notes the API is dead (returns empty). 10,208 duplicate TTB IDs within-table (F25).
    - **source_horizon** (6,441 rows, all created 2026-03-19, never refreshed). CLAUDE.md notes the API returns 404. `price` column exists but **0 rows** have a price populated (F29). ABV column exists but 0 rows have ABV.
    - **source_openfoodfacts** (5,176 rows, all created 2026-03-19). CLAUDE.md notes upstream now has 16K records (3x growth) — our snapshot is stale.
- **Why it matters:** These tables will never naturally improve and the value per row is already capped. OFF in particular is worth a re-pull (barcode richness is good). WV and Horizon can be promoted-then-retired. Keeping them in the active merge rotation wastes pipeline effort and makes the staging estate look larger than the real working set.
- **Proposed fix:**
  - WV ABCA: dedup the 10,208 TTB groups, promote final depth (18K ABVs + 17K varietals, all linked after F1), mark the table as `archived=true` in `source_types`, and freeze it.
  - Horizon: drop it. `price` column has 0 rows so there's nothing to promote. It's pure TTB-duplicate UPCs on the UPC side.
  - OpenFoodFacts: re-pull from current API (~16K records), load into new staging, re-merge. Old snapshot becomes `source_openfoodfacts_2026_03` archive.
- **Effort:** medium (WV promote + OFF refetch)
- **Dependencies:** F1 (promotions need live wine_ids)
- **Related findings:** F25, F29, F20

### F9 — `source_lwin` has 69,470 unmatched wines (36.7% of the LWIN backbone is still not linked to canonical)

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT count(*) total,
           count(*) FILTER (WHERE lwin_11 IS NOT NULL) AS has_lwin11,
           count(*) FILTER (WHERE lwin_18 IS NOT NULL) AS has_lwin18,
           count(*) FILTER (WHERE canonical_wine_id IS NOT NULL) AS matched
      FROM source_lwin;
    -- total=189359  has_lwin11=0  has_lwin18=0  matched=119889
    ```
    All 189,359 rows are "pure wine" (lwin_11/lwin_18 all null — no pack or vintage sub-units). 119,889 were promoted, 69,470 remain unmatched.
- **Why it matters:** CLAUDE.md claims `source_lwin (189K, all promoted)` in its Source Status section. That is ~70K rows off. S1.13 consumed the long-tail in Sessions 12-13 (adding 104K new wines), but the unmatched tail was not re-audited at close. Sprint 3 may be reasoning about LWIN coverage from a doc that is 37% wrong. Some of these 69K are probably legitimate dead ends (LWIN rows where the producer has no wines in Loam), but some are retryable.
- **Proposed fix:** Re-run `lwin_long_tail.py --analyze` to classify the 69,470 into (a) producer-exists-in-canonical-but-wine-doesn't (promote), (b) producer-missing-in-canonical (defer, producer curation), (c) permanently unlinkable (mark processed). Update CLAUDE.md "Source Status" with correct numbers after classification.
- **Effort:** small (re-run existing analyze script, decide what to promote)
- **Dependencies:** none
- **Related findings:** S2.1 F28, F19

### F10 — Schema convention drift: `match_confidence` exists on 16 tables, `match_status` on 1, every table has a different shape

- **Severity:** P1
- **Evidence:** Tables WITH `match_confidence`: source_best_wine_store, source_claude_knowledge, source_domestique, source_empson, source_european_cellars, source_firstleaf, source_kermit_lynch, source_kermit_lynch_growers, source_last_bottle, source_lcbo, source_pa, source_polaner, source_skurnik, source_systembolaget, source_utah_dabs, source_winebow. Tables WITHOUT: source_bc_liquor, source_berliner, source_enofile, source_flatiron, source_horizon, source_kansas_brands, source_lwin, source_openfoodfacts, source_pro_platform, source_specs, source_tabc, source_texsom, source_ttb_colas, source_wallys, source_winedeals, source_wv_abca. Only `source_claude_knowledge` has `match_status`. Only `source_horizon`, `source_openfoodfacts`, `source_pro_platform`, `source_tabc`, `source_winedeals`, `source_wv_abca` have `updated_at`.
- **Why it matters:** There is no shared merge-state contract. The match engine has to special-case every table. There's no way to query across all staging "show me all matches under confidence 0.7" because half the tables can't answer. The `processed_at`/`canonical_wine_id`/`canonical_producer_id`/`match_confidence`/`match_method`/`updated_at` columns should be a consistent interface enforced by the staging-table-create template.
- **Proposed fix:** Add a `supabase/migrations/NNN_unify_staging_merge_columns.sql` that adds `match_confidence numeric`, `match_status text`, `updated_at timestamptz` to every staging table missing them, with `DEFAULT now()` and a `set_updated_at` trigger. Update staging-table-create template in `pipeline/lib/importer.py` to include the unified shape. Sprint 3 work can then rely on a single contract.
- **Effort:** small (pure DDL)
- **Dependencies:** none
- **Related findings:** F2, F5, F21

### F11 — 14 of 32 staging tables lack a `canonical_wine_id` index (the F1 relink will crawl)

- **Severity:** P1
- **Evidence:** Cross-check of `pg_indexes` vs the staging table list. Tables WITH an index on `canonical_wine_id`: source_berliner, source_best_wine_store, source_domestique, source_empson, source_european_cellars, source_firstleaf, source_kansas_brands, source_kermit_lynch, source_last_bottle, source_lcbo, source_lwin, source_pa, source_polaner, source_skurnik, source_systembolaget, source_ttb_colas (3 variants), source_winebow. Tables MISSING it: source_bc_liquor, source_claude_knowledge, source_enofile, source_flatiron, source_horizon, source_openfoodfacts, source_pro_platform, source_specs, source_tabc, source_texsom, source_utah_dabs, source_wallys, source_winedeals, source_wv_abca.
- **Why it matters:** F1 relink (updating `canonical_wine_id` on ~286K rows across 29 tables) will be slow on source_pro_platform (346K), source_tabc (183K), source_wv_abca (55K), source_texsom (47K), source_specs (22K), source_wallys (19K) — collectively the six heaviest tables missing the index. Post-relink, Sprint 3 merge-aware queries (e.g., "find all unmatched pro_platform rows with grape X") will sequential-scan the table every time.
- **Proposed fix:** Single migration adding `CREATE INDEX idx_<table>_canonical_wine ON <table>(canonical_wine_id) WHERE canonical_wine_id IS NOT NULL;` for the 14 missing tables. Partial index keeps them small. Do it BEFORE F1 so the relink bulk UPDATE benefits.
- **Effort:** trivial
- **Dependencies:** none (must run before F1 for best perf)
- **Related findings:** F1

### F12 — `source_specs` promoted prices only into `wine_vintage_prices` for ~50% of linked rows, even ignoring F1

- **Severity:** P1
- **Evidence:** source_specs has 10,808 rows with `price IS NOT NULL AND canonical_wine_id IS NOT NULL`. `wine_vintage_prices` has 5,544 rows via `retailers.slug='specs'`. The 5,264 shortfall is not explained by F1 alone (since F1 would put the gap at ~16K, the full archive-orphan count). This suggests `retail_promote.py` ran once, touched half the linked rows, and stopped. Same pattern visible in source_lcbo (5,268 linked → 838 promoted, shortfall 4,430), source_systembolaget (12,460 → 644, shortfall 11,816), source_enofile (9,115 → 412, shortfall 8,703), source_pa (5,905 → 728, shortfall 5,177).
- **Why it matters:** retail_promote is an incomplete pipeline — it processed a partial slice and was never re-run. Some of that is F1 (once the archive-ID relink runs, existing linked rows point nowhere), but the systembolaget/enofile/pa gaps imply another bug. CLAUDE.md S13 2026-04-04 session notes "✅ Spec's/LCBO/BC Liquor/Systembolaget/PA/FirstLeaf prices via bulk SQL" but the shortfalls say otherwise. Either the bulk SQL had a `LIMIT` bug or those sources didn't all run.
- **Proposed fix:** After F1 is complete, re-run `retail_promote.py` (or its pure-SQL replacement) for every priced source, verifying row counts match `count(*) FILTER (WHERE price IS NOT NULL AND canonical_wine_id IS NOT NULL)` at the end. Add a post-run verification step to the script: assert promoted == linked_with_price or log the diff.
- **Effort:** small
- **Dependencies:** F1
- **Related findings:** F1, F3, F19

### F13 — Wine-match-without-producer-match is the dominant pattern in catalog retailers

- **Severity:** P1
- **Evidence:**

    | table | matched wines | matched producers | delta (wines without producer) |
    |---|---:|---:|---:|
    | source_systembolaget | 12,460 | 4,064 | 8,396 |
    | source_enofile       |  9,117 | 3,480 | 5,637 |
    | source_pa            |  5,905 | 2,804 | 3,101 |
    | source_winedeals     |  3,145 | 1,340 | 1,805 |
    | source_flatiron      |  4,096 | 2,104 | 1,992 |
    | source_specs         | 17,157 | 9,743 | 7,414 |
    | source_wallys        | 17,835 | 12,415| 5,420 |
- **Why it matters:** Having a wine match without a producer match means the matcher cheated: it resolved the wine by fuzzy name match (or backbone ID) without confirming the producer. These matches are not trustworthy — two wines named "Chablis 2020" from different producers will be collapsed to one. It also means `wines.producer_id` pointing from any newly-created wine is suspect. The systembolaget 8,396 case is the most dramatic: producer match rate 32.6% vs wine match rate 98.5% — producer matching was clearly skipped for that source.
- **Proposed fix:** Sprint 3 merge engine must enforce `(producer_matched AND wine_matched)` as the accepted outcome. Re-audit the 8,396 systembolaget + 5,637 enofile + 3,101 pa cases after F1 and validate that the producer link is retroactively derivable from the already-matched wine. If not, unlink and re-run matching with a producer-first path.
- **Effort:** medium
- **Dependencies:** F1
- **Related findings:** F19

### F14 — Importer-depth sources (empson, winebow, european_cellars, kermit_lynch) have rich chemistry/vinification that never reached canonical

- **Severity:** P1
- **Evidence:** Per-column wine-linked counts:

    | source | ferm_dur | aging | oak | closure | pairings | altitude / elev |
    |---|---:|---:|---:|---:|---:|---:|
    | source_empson            | 142 | 208 | 95  | 211 | 203 | 196 |
    | source_winebow (pH, acid, RS, aging, prod, elev) | 249 | 274 | 217 | 114 | 241 | 203 |
    | source_european_cellars  | 237 | 238 | 238 | 238 | 238 | 234 |
    | source_kermit_lynch (soil, vine_age, area, vinif, farming) | 890 | 888 | 885 | 889 | 891 | — |

    All four sources are ~100% orphaned post-30K (F1): empson 221/221, winebow 264/284, european_cellars 232/238, kermit_lynch 865/891.
    S1.8 (2026-04-03) logged: "Importer depth: 1,586 wine + 488 vintage updates (Empson/Winebow/EC/KL → fermentation, oak, chemistry, closure, serving temp, production)" — so an earlier promotion ran, but those 1,586 updates went to `archive.wines`, not `public.wines`. They're lost.
- **Why it matters:** This is the only staging source of winemaking depth that isn't regulatory or retailer metadata. Post-30K, canonical `wine_vintages` has essentially none of these fields populated (S2.1 F8: pH/TA/RS/VA/SO2/oak all <0.1% coverage). If F1 runs and `importer_depth.py` re-promotes, S2.1 F8 moves materially. These 4 sources + `source_kermit_lynch_growers` are also the only ones that carry real producer metadata (year_established, website, annual_production, winemaker).
- **Proposed fix:** After F1, re-run `pipeline/promote/importer_depth.py` across empson, winebow, european_cellars, kermit_lynch, kermit_lynch_growers. Add `source_polaner` (1,111 orphaned, natural-wine-specialist certifications) and `source_best_wine_store` (1,657 orphaned, Shopify tags) to the same pass since their shapes are similar.
- **Effort:** medium
- **Dependencies:** F1
- **Related findings:** S2.1 F4/F5 (producer metadata empty), S2.1 F8 (vintage chemistry empty), F3

### F15 — `source_ttb_colas.qualifications` holds 2.47M rows of narrative TTB label text, 66,605 wine-linked, currently unused

- **Severity:** P1
- **Evidence:**
    ```sql
    SELECT
      count(*) FILTER (WHERE qualifications IS NOT NULL AND qualifications <> '') AS has_qualifications,
      count(*) FILTER (WHERE qualifications IS NOT NULL AND qualifications <> '' AND canonical_wine_id IS NOT NULL) AS linked,
      count(*) FILTER (WHERE label_image_urls IS NOT NULL AND canonical_wine_id IS NOT NULL) AS label_image_linked,
      count(*) FILTER (WHERE detail_scraped_at IS NOT NULL) AS detail_scraped,
      count(*) FILTER (WHERE printable_scraped_at IS NOT NULL) AS printable_scraped
    FROM source_ttb_colas;
    -- has_qualifications=2476512  linked=66605  label_image_linked=66712
    -- detail_scraped=3178691  printable_scraped=1824749
    ```
- **Why it matters:** The `qualifications` field is where TTB stores the narrative label text (back label claims, appellation qualifications, vinification notes, geographic qualifications). It's one of the few structured provenance-rich text fields in the whole staging estate. 66,605 rows are already wine-linked and would flow into Grade C enrichment context directly. Currently: zero use.
- **Proposed fix:** Add `qualifications` and `class_type_desc` to the Grade C facts-packet builder in `pipeline/enrich/build_facts_packet.py`. When a wine has a linked TTB COLA with qualifications text, include it verbatim as a TRUSTED_FACT — this is exactly the kind of factual anchor S1.11 voice rules were written to leverage.
- **Effort:** small (prompt builder update)
- **Dependencies:** F1 (linked rows need valid wine_ids), enrichment pipeline live (already deployed as feature flag)
- **Related findings:** F3, S2.1 F8

### F16 — `source_wv_abca` has 10,208 duplicate TTB IDs (ratio 1.33 rows per TTB)

- **Severity:** P2
- **Evidence:** `count(DISTINCT ttb) = 41,403` vs `count(*) = 55,093` → 13,690 over-rows in 10,208 TTB groups. Since WV is a labels-registration system, multiple label_ids can legitimately share a TTB (different size, different year, different batch). `count(DISTINCT label_id) = 55,093` = total → no label_id duplicates. Each row represents a distinct labeling event.
- **Why it matters:** During TTB-keyed merge (F19), this WV dupe pattern will cause multiple promotion attempts for the same canonical wine. Needs to be grouped-and-merged before promotion.
- **Proposed fix:** In the WV promotion pass, `GROUP BY ttb` and pick a representative row (prefer the one with latest `detail_fetched_at` / populated `alcohol_percentage`). Don't delete duplicates from staging — they're legitimate historical records — just collapse at promote time.
- **Effort:** trivial (ORDER BY + LIMIT 1 in the promotion query)
- **Dependencies:** F1, F8
- **Related findings:** F8

### F17 — `source_kansas_brands` has 6,886 duplicate COLA numbers (legitimate — multi-licensee distribution in KS)

- **Severity:** P2
- **Evidence:** `count(DISTINCT ks_license) = 65,476` = total → no license_id dupes. `count(DISTINCT cola_number)` is 43,588 → 21,888 rows share a COLA with another row. Kansas requires per-licensee brand registration, so the same TTB COLA legitimately reappears once per KS licensee. The ks_license column is the true natural key.
- **Why it matters:** Not a bug — a feature of KS data. But the merge engine should key off `ks_license`, not `cola_number`, or it will conflict-error on multi-licensee wines. Worth flagging because the schema doesn't enforce the right key.
- **Proposed fix:** Add a unique constraint `UNIQUE(ks_license)` on `source_kansas_brands` to enforce the natural key explicitly. Promotion pipeline should `GROUP BY cola_number` when linking to TTB but `DISTINCT ON (ks_license)` when creating wines.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F10

### F18 — Staging natural keys are unique in some sources, completely absent in others

- **Severity:** P2
- **Evidence:** Within-source natural-key dupe check:

    | table | natural key | dupes | notes |
    |---|---|---:|---|
    | source_ttb_colas | ttb_id | 0 | clean |
    | source_pro_platform | cola_number | 0 | clean |
    | source_tabc | ttb_number | 0 | clean |
    | source_lwin | lwin | 0 | clean |
    | source_kansas_brands | ks_license | 0 | clean (F17 explains cola_number dupes) |
    | source_wv_abca | label_id | 0 | clean |
    | source_wallys | shopify_id | 0 | clean |
    | source_kermit_lynch | kl_id | 0 | clean |
    | source_systembolaget | product_id | 6,298 | **F4 double load** |
    | source_lcbo | sku / upc | 3,490 | **F4 double load** |
    | source_specs | upc | 782 | promotable with DISTINCT ON |
- **Why it matters:** Most tables are clean. The systembolaget + lcbo double-load (F4) is the main issue. Specs 782 is manageable — could be legitimate multi-variety same-UPC in rare cases, or a partial re-scrape. Worth spot-checking.
- **Proposed fix:** Covered by F4. Add `UNIQUE` constraints on natural keys across all staging tables after cleanup, enforced via `ON CONFLICT` in loaders.
- **Effort:** trivial after F4
- **Dependencies:** F4
- **Related findings:** F4, F17

### F19 — `retail_promote` pipeline never covered 14 of 14 price sources end-to-end

- **Severity:** P2
- **Evidence:** See F12 table. Summing: staging linked prices 75,100, canonical promoted 23,150 → ~52K shortfall. Of that shortfall, some is F1 (orphan wine_ids can't create wine_vintage_prices rows), but the F4 double-load masks part of it and the Specs/LCBO/Systembolaget/PA gaps suggest the pipeline also has `LIMIT` or filtering bugs.
- **Why it matters:** The gap is ~3× current price coverage. If F1 + F4 + F12 all run, canonical price coverage jumps from 5.21% to an estimated 12-15% of active wines. That's the single biggest dashboard-moving number in the merge backlog.
- **Proposed fix:** Rewrite `pipeline/promote/retail_promote.py` as a pure-SQL script with one CTE per source, explicit INSERT ... ON CONFLICT DO UPDATE, a final assertion, and a summary printed at the end showing `(linked, promoted, diff)` per source. Existing retail_promote calls REST per row — the 2026-04-04 bulk-SQL rewrite was only partial (Wally's, Spec's, Enofile, LCBO) and didn't cover systembolaget/pa/bc_liquor/best_wine_store/domestique/last_bottle/firstleaf. Finish the conversion.
- **Effort:** medium
- **Dependencies:** F1, F4, F12
- **Related findings:** F1, F3, F4, F12

### F20 — `source_utah_dabs` was added as a second wave (2026-04-06) but never promoted

- **Severity:** P2
- **Evidence:** `source_utah_dabs` created_at range is `[2026-04-06, 2026-04-06]` — a single-day load. 2,834 rows, 2,181 `canonical_wine_id` set, 2,181 `processed_at` set, 117 rows in `wine_vintage_prices` via retailer slug `utah-dabs` (per F3 table). Gap: 2,064 linked prices with no canonical row.
- **Why it matters:** Utah DABS is the only clean monthly-XLSX beverage pricing source in the whole estate. CLAUDE.md notes it was deliberately added "as backup" after Virginia ABC was found to be spirits-only. It completed the merge step but never completed price promotion. Missing ~2K clean monopoly prices.
- **Proposed fix:** Included in F19's retail_promote rewrite (add `utah-dabs` to the CTE list).
- **Effort:** trivial (after F19)
- **Dependencies:** F1, F19
- **Related findings:** F19

### F21 — 26 of 32 staging tables lack an `updated_at` column; staleness detection is impossible

- **Severity:** P2
- **Evidence:** Only `source_horizon`, `source_openfoodfacts`, `source_pro_platform`, `source_tabc`, `source_winedeals`, `source_wv_abca` have `updated_at`. The remaining 26 have only `created_at`. Cross-check: CLAUDE.md notes `source_tabc refreshed 2026-04-03` but the table's `created_at` distribution is 100% `2026-03-19`. That means the refresh was an UPSERT that didn't touch `created_at`, but `updated_at` isn't visible in my queries (not populated?).
- **Why it matters:** Without an `updated_at` column (auto-triggered via `set_updated_at()`) there's no way to detect "this row was touched in the last 7 days" for incremental re-merge. The merge engine has to re-process everything or nothing. Also complicates drift audits — "when did we last see this TTB record?" has no answer.
- **Proposed fix:** Part of F10 unification — add `updated_at timestamptz DEFAULT now() NOT NULL` + `set_updated_at` trigger to all 26 tables missing it. Backfill `updated_at = created_at` on historical rows.
- **Effort:** small (pure DDL migration)
- **Dependencies:** none
- **Related findings:** F10

### F22 — `source_kermit_lynch_growers` is producer-only (no `canonical_wine_id` column), but 77% unmatched

- **Severity:** P2
- **Evidence:** Schema query confirms `source_kermit_lynch_growers` has `canonical_producer_id` but no `canonical_wine_id`. 193 total rows, 44 matched to canonical producers, 0 orphaned (producer link is current). 149 remain unmatched despite KL being a major boutique importer. 0 rows have `processed_at` set.
- **Why it matters:** KL Growers is the richest single source of European producer metadata (14 fields: founded_year, website, annual_production, viticulture_notes, about, farming, winemaker, location). S1.8 promoted some — "KL Growers producer metadata: 120 producers with year_established, website, GPS, production, description" — but only 44 rows are linked in staging now, and 149 unmatched growers likely contain high-value producers (Marcel Lapierre, etc.) that may already exist in canonical under alternate names.
- **Proposed fix:** Re-run `batch_matcher` on the 149 unmatched growers against all European canonical producers with accent-insensitive matching, then re-run `importer_depth.py` on the whole table with `set processed_at = now()` at completion (fixing the F2 gap for this table).
- **Effort:** small
- **Dependencies:** none (producer links are clean already)
- **Related findings:** F2, F14

### F23 — `source_berliner` has 96.2% orphaned wine links but 4,878 competition medals ready to promote

- **Severity:** P2
- **Evidence:** 4,693 of 4,878 wine matches are orphaned (F1). `wine_vintage_scores` from Berliner: 680 rows. Staging has 4,878 wine-linked rows with `medal` field populated. Gap: 4,198 medals. CLAUDE.md 2026-04-04 claims "Berliner: +880 new wine matches, +3,717 competition scores promoted" — but the 3,717 count doesn't match the current 680 in the DB. This mismatch matches the F1 story (scores were promoted, then wine_ids went dead in 30K rebuild, the JOIN to wine_vintage_scores broke).
- **Why it matters:** Berliner is one of the few real score sources with 42 competitions across 17 years. With F1 + a re-run of score promotion, canonical score coverage jumps by ~4,200 from this source alone. TEXSOM would add another ~17K.
- **Proposed fix:** Include Berliner score promotion in the F19 rewrite (add a competition-score CTE).
- **Effort:** trivial (after F19)
- **Dependencies:** F1, F19
- **Related findings:** F3, F19

### F24 — `source_ttb_colas` 66,712 rows have label image URLs linked to wines — they exist but `label_image_url` coverage on canonical wines is 211,266 (S1.8 stat), leading to an inconsistency worth verifying

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE label_image_urls IS NOT NULL AND canonical_wine_id IS NOT NULL) FROM source_ttb_colas;
    -- 66,712
    ```
    CLAUDE.md Session 8 notes label images: "211,266 wines with TTB label_image_url". But source_ttb_colas has only 66,712 wine-linked label image URLs after S13's producer relink. The 211,266 number is from the pre-30K state when the archive had all the links.
- **Why it matters:** CLAUDE.md is claiming 211K wines have label images; the reality (per current canonical) is likely <70K. S2.1 F28 flagged exactly this drift pattern. Worth verifying canonical-side and updating the claim.
- **Proposed fix:** Query `wines.label_image_url IS NOT NULL` — if ~67K, update CLAUDE.md. If ~211K, then label images are on the wine row but F1 wine_id orphan in TTB is the reason the staging-side count is low. Either way, one of the numbers is wrong.
- **Effort:** trivial (1 query, 1 doc edit)
- **Dependencies:** none
- **Related findings:** S2.1 F28

### F25 — `source_openfoodfacts` has 80% unmatched (1,042/5,176) after 3 weeks; fuzzy matching was never finished

- **Severity:** P2
- **Evidence:** 5,176 rows, 1,042 matched, 4,134 unmatched. OFF is 62% French wines with barcodes. 0 rows have `processed_at` set. OFF's upstream is also 3x bigger now than the snapshot.
- **Why it matters:** Barcode sources are critical because they're the hardest to replace (UPC is the only stable cross-system identifier for mass-market wine). OFF captures the French grocery tier that TTB COLAs miss. Losing 80% of it because the matcher didn't complete is a noticeable gap for barcode-driven consumer lookups.
- **Proposed fix:** Re-pull OFF upstream (~16K records), load into fresh staging table, retire the old one, run enhanced fuzzy matching (accent-insensitive, producer-first, country-scoped). Part of F8 OFF refresh.
- **Effort:** small
- **Dependencies:** F8
- **Related findings:** F8

### F26 — `source_horizon` has an abv column and a price column — **both 0% populated**

- **Severity:** P2
- **Evidence:**
    ```sql
    SELECT count(*) FILTER (WHERE abv IS NOT NULL) FROM source_horizon;  -- 0
    SELECT count(*) FILTER (WHERE price IS NOT NULL) FROM source_horizon; -- 0
    ```
    Despite the schema including both columns (typed `numeric` and `numeric` respectively), 0 of 6,441 rows have either field set. Only `upc`, `name`, `brand`, `category`, `subcategory`, `country`, `region`, `appellation`, `varietal`, `vintage`, `size` are actually populated.
- **Why it matters:** Horizon's value prop was supposed to be UPCs + ABVs + distributor prices for Mass/RI. The UPCs exist (6,441 of them) but the price and ABV columns were never populated, presumably because the fetcher couldn't scrape them. CLAUDE.md notes the Horizon API is now dead (404), so we can't backfill. The table is effectively a UPC-only source, not a price source. Schema mislabels its contribution.
- **Proposed fix:** Document Horizon as UPC-only in `docs/SOURCES.md`. Promote the 4,566 linked UPCs to `external_ids.upc` (trivial). Deprioritize the abv/price columns in the schema — they're vestigial.
- **Effort:** trivial
- **Dependencies:** F1 (for UPC promotion)
- **Related findings:** F8

### F27 — `source_tabc` has ZERO structured grape / appellation / vintage data — it is TTB-ID-only

- **Severity:** P2
- **Evidence:** TABC schema is `ttb_number, brand_name, trade_name, alcohol_content, approval_date, tabc_certificate, permit_license, product_type`. No vintage, no appellation, no grape. It is purely a TTB-number-bearing permit registry. 52,192 wines linked, all orphaned (F1).
- **Why it matters:** CLAUDE.md lists TABC in the "Regulatory/ID sources" with "100% TTB numbers, 99.8% ABV." The ABV is the only useful data point beyond the TTB cross-ref. Once F1 + ABV promotion run, TABC is "done" as a source. The 130,741 unmatched rows are not mineable for depth; they're just TTB numbers we couldn't tie to a canonical wine. Worth understanding this so it doesn't sit on the merge backlog pretending to have more value.
- **Proposed fix:** After F1 promotion of ABV + TTB cross-ref, mark TABC as `maintenance=true` in `source_types`. Set up a lightweight re-pull schedule (annual?) but deprioritize depth work. Update `docs/SOURCES.md` with the real value footprint.
- **Effort:** trivial (doc update)
- **Dependencies:** F1, F8
- **Related findings:** F8

### F28 — `source_claude_knowledge.processed_at` is 100% populated (920/920) — the ONLY staging table with perfect merge-state discipline

- **Severity:** P3
- **Evidence:**
    ```sql
    SELECT count(*), count(*) FILTER (WHERE processed_at IS NOT NULL),
           count(*) FILTER (WHERE canonical_wine_id IS NOT NULL),
           count(*) FILTER (WHERE match_status IS NOT NULL)
      FROM source_claude_knowledge;
    -- total=920  processed=920  matched=860  status=920 (all)
    ```
    Also the only staging table with `match_status`, `match_notes`, `ai_extracted_data`, `ttb_match_confidence`, `validation_status`, `validation_notes`, `promoted_at` columns — a complete merge audit trail per row.
- **Why it matters:** This is the model to replicate. `source_claude_knowledge` was built by S1.6 knowledge_seed pipeline with a full designed audit contract (`match_status`, `match_notes`, `promoted_at`), and as a result it's the ONLY source where you can tell which row was rejected by validation vs. no-match. The other 31 sources should adopt this shape.
- **Proposed fix:** Use `source_claude_knowledge` as the template for the F10 staging-column-unification migration. Columns to add across all 31 other tables: `match_status text`, `match_notes text`, `match_confidence numeric`, `promoted_at timestamptz`, `validation_status text`, `validation_notes text` (optional per source).
- **Effort:** trivial (reuses F10 + this as the template)
- **Dependencies:** F10
- **Related findings:** F10

### F29 — `source_tabc` and `source_texsom` lack `updated_at`, but their sources DO get re-loaded (tabc 2026-04-03 refresh; texsom annual)

- **Severity:** P3
- **Evidence:** CLAUDE.md 2026-04-03: "TABC refresh: fetched 201K from Socrata, but 183K unique TTB after dedup — no net new records." The fetch ran, dedup ran, and current `source_tabc.created_at` still shows 2026-03-19. Either the refresh was discarded or `created_at` wasn't touched. Same for `source_texsom`: 2026-03-21 loaded, no `updated_at` column, no way to know if it's been refreshed.
- **Why it matters:** This is a doc/schema inconsistency, not a data loss — but it's a friction point. If you query `source_tabc` for "when did we last see this row," the only answer is "March 19" which is wrong. The freshness story in `docs/SOURCES.md` gets stale on its own.
- **Proposed fix:** F10 / F21 `updated_at` migration covers this. Plus: whenever the merge engine touches a row (F2 fix), update `updated_at`. Plus: the fetcher's load pipeline should write `updated_at = now()` on UPSERT conflicts.
- **Effort:** trivial (covered by F21)
- **Dependencies:** F21
- **Related findings:** F10, F21

### F30 — `source_pro_platform.states` is a jsonb array of distribution states, 346K rows populated, never used downstream

- **Severity:** P3
- **Evidence:** Schema includes `states` column (jsonb). `source_pro_platform` loads records via PRO Platform XLSX which includes a "States" column listing where the COLA is registered for distribution. There is no downstream promotion of this into canonical — no `wine_distribution` or `wine_states` table exists. All 346K rows have non-null `states`.
- **Why it matters:** Distribution state data is a signal for consumer search ("wines available in my state"). It's not promoted anywhere and doesn't surface in canonical. Worth noting as a future product capability — not P2 because no one is asking for it.
- **Proposed fix:** Park as a Sprint 4+ product consideration. Could feed a filter on the search page ("available in CA") or an enrichment field. Do not promote now.
- **Effort:** N/A
- **Dependencies:** product decision
- **Related findings:** —

### F31 — Staging estate total size is 7.0 GB of which `source_ttb_colas` is 6.0 GB (86%); the other 31 tables fit in <1 GB

- **Severity:** P3
- **Evidence:**
    ```sql
    -- top 5
    source_ttb_colas       6,154 MB   (5,130 MB heap + 1,024 MB indexes)
    source_pro_platform      195 MB
    source_lwin              120 MB
    source_tabc               82 MB
    source_berliner           51 MB
    -- rest sum to <200 MB
    ```
- **Why it matters:** CLAUDE.md says "Supabase Small $10/mo required for source_ttb_colas 4.7GB" — the actual current size is 6.15 GB including indexes (TTB scraping completed in the interim and added indexes). We're approaching the 6 GB total headroom on the compute tier. The next TTB refresh cycle will push over. Worth flagging for Sprint 5+ capacity planning, and worth considering archiving `source_ttb_colas.label_image_urls` text array (which could be hundreds of bytes per row × 3.2M rows = non-trivial).
- **Proposed fix:** Pre-Sprint-5 capacity audit. Either: (a) bump compute tier, (b) archive scraped-but-unlinked TTB rows (the 1.35M non-001 format rows) to a separate `archive.source_ttb_colas_non_001`, (c) compress `label_image_urls` into a linked `source_ttb_label_images` child table.
- **Effort:** small
- **Dependencies:** none for awareness; medium if we act
- **Related findings:** —

---

## Meta-patterns

Three patterns to escalate to S2.9 synthesis:

1. **The 30K rebuild's true damage lives in staging, not canonical.** S2.1 F6 identified canonical depth loss but framed it as "promotion never ran." The real story: promotion did run, wine links were set, then the 30K rebuild hard-discarded archive.wines and the staging rows became dangling pointers. S13 fixed `source_ttb_colas` + `source_lwin` but left 29 other tables orphaned. **Sprint 3's archive depth bridge must start with staging relink, then promote.** This is mechanical — reuse the S13 pattern. It is the single highest-ROI Sprint 3 workstream: ~60K prices, ~48K scores, ~200K vintages, ~200K ABVs, ~40K UPCs unlocked.

2. **Staging schema has never had a unified contract.** 32 tables, 32 different shapes. `processed_at` semantics differ by source. `match_confidence` is on 16 tables. `updated_at` is on 6. `match_status` is on 1 (the template we should be copying). The match engine has to special-case every table. Sprint 3 should lock `pipeline/lib/importer.py` to a shared template that every new staging table inherits. The F10/F21 migrations are a one-day investment that pays off every merge run afterward.

3. **`match_decisions` is a real, well-designed audit trail that has never been used for its original purpose.** It was built for cross-source merge attribution, but after 7,641 rows of dedup writes it has stayed wines×wines. Sprint 3's merge engine must populate it on every decision. Without that, Sprint 3's "audit what happened" story is just git history of the matching scripts — which is a Sprint 2 finding (S2.5 code audit) waiting to happen.

## Numbers to carry into S2.9 synthesis

**Orphan totals:** 286,918 wine_id pointers across 29 staging tables (F1). Only `source_ttb_colas` and `source_lwin` are clean.

**Value-left-on-table (gross, pre-dedup):** ~60K prices, ~48K competition scores + ratings, ~200K ABV text values, ~170K vintage integers, ~190K appellation strings, ~42K UPC strings, ~900 wine records of fermentation/oak/chemistry depth from 4 rich importers.

**Current `wine_vintage_prices` by source slug** (for the S2.9 synthesis gap narrative):
- wallys 14,336 · specs 5,544 · lcbo 838 · pa-plcb 728 · systembolaget 644 · enofile 412 · flatiron-wines 225 · best-wine-store 171 · bc-liquor 128 · utah-dabs 117 · winedeals 7 · (null retailer) 70

**Current `wine_vintage_scores` by publication**: TEXSOM International Wine Awards 4,473 · Berliner Wine Trophy 680 · Asia Wine Trophy 129 · (null) 82 · James Suckling 21 · Portugal Wine Trophy 19 · James Halliday 10 · Jancis Robinson 2 · Falstaff 2 · Burghound 1 · San Francisco Chronicle WC 1. **Total 5,420.**

**Scope-breaker check:** None. Every finding executes inside the Sprint 2 → Sprint 3 envelope. F1 reframes "write the archive depth bridge" as "relink staging first, then promote" — a scope refinement, not a rewrite. Sprint 3 continues to mean "execute prioritized fixes from the audit backlog."
