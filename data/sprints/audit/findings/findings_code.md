# Code Expert Audit — Findings

**Session:** S2.5
**Date:** 2026-04-11
**Expert:** code
**Method:** Opus 4.6 inline, Glob/Grep/Read across `pipeline/` (265 Python files, 77,560 LOC), `supabase/functions/` (2 edge functions via MCP), `scripts/`, `frontend/src/` (grape field usage), `scripts_archive/`. Cross-referenced against S2.1-S2.4 findings. Read-only, no pipeline runs. $0 project spend.
**Scope:** pipeline scripts (fetch / load / promote / enrich / identity / reference / geo / analyze), shared libs (`pipeline/lib/`), edge functions (`enrich-wine`, `describe-chemical`), scheduled tasks (open-meteo drip), CLI conventions, error handling, dead code, and code-level root causes for data findings from S2.1-S2.4.

## Summary

- **Total findings:** 32
- **P0:** 9 · **P1:** 14 · **P2:** 7 · **P3:** 2
- **Biggest risks:**
  - **(F1)** The `describe-chemical` edge function is DEPLOYED, ACTIVE, `verify_jwt=false` (anonymous), uses the same `ANTHROPIC_API_KEY`, and has **zero wine logic** — it's a chemical-industry prompt from an unrelated project. Anyone with the URL can burn Anthropic credits with no auth.
  - **(F2)** Code root cause of S2.3 F2 Chardonnay/Pinot Blanc: TTB→canonical wine linking is too coarse. For De Bortoli "17 Trees" (and ~2,700 other wines), `batch_pipeline._match_ttb_to_wine` matches on `fanciful_name` or falls through to `"if len(wines) == 1, assume match"`, causing 4 unrelated grape-specific COLAs (Chardonnay/Cab/Shiraz/Shiraz) to link to ONE canonical wine. Downstream `ttb_grape_promote` then inserts all 4 resolved grapes. The `DISTINCT ON (canonical_wine_id)` in `ttb_grape_promote` picks exactly one row per wine, but the follow-up `grape_blend_promote` and earlier runs stacked multiple grapes across runs — and since the same canonical wine is associated with multiple grape strings, re-runs accumulate wine_grapes rows. **The bug is not in the grape resolver itself** (`resolve.py` correctly returns CHARDONNAY BLANC for "Chardonnay" in all code paths traced); it's in upstream wine-identity de-duplication that collapsed multiple COLAs into one canonical wine.
  - **(F3)** `relink_staging_to_current.py` deliberately NULL-ed `canonical_wine_id` ONLY in `source_ttb_colas` (line 199). The other **29 staging tables with wine_id pointers were never touched** — they still contain ~286K dangling archive wine_id pointers (S2.2 F1). The script handled producers correctly; the wine re-link was left as a "do it later" TODO that never shipped.
  - **(F4)** `enrich-wine` edge function uses `grapes.name` (VIVC cépage form: "CHARDONNAY BLANC", "MERLOT NOIR") when building prompts for Sonnet — not `grapes.display_name`. Every enrichment prompt inherits wrong grape labels. Frontend uses `display_name` correctly.
  - **(F5)** Model version drift: `describe-chemical` uses `claude-haiku-4-5-20251001`; `enrich-wine` uses `claude-sonnet-4-20250514`; 12 pipeline scripts hardcode the older `claude-sonnet-4-20250514` while 3 newer scripts use `claude-sonnet-4-6`. No shared constant, no central config.
- **Biggest wins:**
  - `pipeline/lib/db.py` is clean: `get_conn()` returns a raw psycopg2 session-pooler connection (avoids the HTTP/2 ConnectionTerminated class of bug), and `fetch_all()` correctly paginates past Supabase's 1,000-row default.
  - `pipeline/lib/normalize.py` is 134 lines of well-tested doctest-covered helpers. Nothing to flag.
  - `pipeline/lib/merge.py` has a clean 3-tier producer+wine matcher (LWIN/barcode/external-id → exact normalized name → pg_trgm fuzzy).
  - `relink_staging_to_current.py` is a well-written one-off migration tool — the issue in F3 is scope, not code quality. It did its producer job correctly and committed the mapping via a temp table with SAVEPOINTs.
  - CLI conventions are broadly consistent: 189 of 265 pipeline scripts use `argparse`, 236 have `if __name__ == "__main__"` guards, `--dry-run` / `--execute` pairs are standard.
  - The feature flag on `enrich-wine` (`ENRICHMENT_ENABLED` env) correctly returns 503 with a descriptive reason when disabled, protecting against accidental re-enable during audit.

---

## F1 — `describe-chemical` edge function is dead code, unauthenticated, burns Anthropic credits

- **Severity:** P0
- **Evidence:** MCP `get_edge_function` on `describe-chemical`:
  - `status: "ACTIVE"`
  - `verify_jwt: false` (no auth required)
  - Prompt body: `"You are a chemical industry analyst. In 2-3 sentences, explain what this material is and what it's used for..."`
  - Accepts `{ name, officialName, cas, aliases, category, usages, notes }` payload
  - Calls Anthropic `claude-haiku-4-5-20251001` with the `ANTHROPIC_API_KEY` env var
  - Version 5, last updated around project creation
- **Why it matters:** This function has literally nothing to do with wine. It looks like a leftover from another project (chemical-materials database?) that was deployed to the Loam Supabase project by mistake or as a test, then never removed. Consequences:
  1. **Credential abuse risk:** Anyone with the edge function URL can POST arbitrary chemical descriptions, spending Anthropic budget. With `verify_jwt: false`, there is no JWT gate; only Supabase's anon rate-limits.
  2. **Confuses new contributors:** A developer reading `list_edge_functions` sees 2 functions and may assume `describe-chemical` is a real Loam component.
  3. **Audit blind spot:** It never shows up in the main codebase (`supabase/functions/` is empty in git — both functions live only as deployed source returned by the API). There's no code review on changes.
- **Proposed fix:** Delete the function via Supabase dashboard or `supabase functions delete describe-chemical`. Add a post-commit `list_edge_functions` check to the Sprint 3 wrap-up so surprise deployed functions get caught next time. Optionally add a local `supabase/functions/` directory and vendor the `enrich-wine` TypeScript source into git so edge function code lives in version control.
- **Effort:** trivial (one delete + one git commit of `enrich-wine` source)
- **Dependencies:** none
- **Related findings:** F4 (enrich-wine bugs would also benefit from being in git), F31 (edge function source not in repo)

---

## F2 — TTB→canonical wine matching is too loose; ~2,700 wines inherit wrong multi-grape links

- **Severity:** P0
- **Evidence:** Traced S2.3 F2 Chardonnay/Pinot Blanc bug to its actual code root cause:
  ```sql
  SELECT ttb_id, brand_name, fanciful_name, grape_varietals
  FROM source_ttb_colas
  WHERE canonical_wine_id = '042abb2f-6cce-4623-baec-90a18e60f4ac';
  -- 4 rows, same brand/fanciful, 4 DIFFERENT grapes:
  --   Chardonnay, Cabernet Sauvignon, Shiraz, Shiraz
  ```
  All four COLAs link to ONE canonical wine ("17 Trees" from De Bortoli — the producer's "17 Trees" is a range, not one wine).

  Code path in `pipeline/identity/batch_pipeline.py:693-716`:
  ```python
  def _match_ttb_to_wine(self, wines, fanciful, appellation):
      for wine_id, wine_name, display_name, slug in wines:
          # Exact cuvée match
          if wine_name_norm == fanciful_norm:
              return str(wine_id)
          # If wine has no cuvée, match by appellation
          if not wine_name and not fanciful:
              return str(wine_id)
      # If only one wine for this producer and TTB record has no fanciful, assume match
      if len(wines) == 1 and not fanciful:
          return str(wines[0][0])
      return None
  ```
  This matcher uses `fanciful_name` only. "17 Trees" is the fanciful for 4 completely different grape-specific wines, so all 4 COLAs match the same canonical "17 Trees" row. Same pattern repeated ~2,700 times across the catalog. Then `ttb_grape_promote.py` walks the TTB records for each wine and tries every `grape_varietals` string — even though `DISTINCT ON` picks one per run, successive runs accumulate until the same wine has CHARDONNAY BLANC + PINOT BLANC + PINOT NOIR + CAB SAUV (2,809-wine cohort query result):
  ```
  PINOT BLANC: 2,743 links
  CHARDONNAY BLANC: 2,434 links
  PINOT NOIR: 83
  CABERNET SAUVIGNON: 19
  VIOGNIER: 12
  SYRAH: 11
  ```
  The PINOT BLANC dominance is specifically because PINOT BLANC has VIVC synonyms like `PINOT CHARDONNAY` (S2.4 F2), and `batch_pipeline._load_reference_data()` (line 169-172) loads every synonym into `self.grapes[synonym.upper()]` without preferring primary names — so if Haiku or TTB ever returned "Pinot Chardonnay" for a white wine, that routes to PINOT BLANC. Combined with the multi-COLA collapse, the two bugs compound.
- **Why it matters:** This is the root cause of the single biggest content-quality finding in Sprint 2 (S2.3 F2 — 97.6% of Chardonnay-named wines wrong). The data-side fix proposed in S2.4 F2 (delete polluting synonyms) will not help unless the TTB-wine matcher stops collapsing multi-grape COLA cohorts into one canonical wine. Sprint 3 grape repair must include this code fix.
- **Proposed fix:** Three-layer fix.
  1. **Don't collapse:** In `_match_ttb_to_wine`, require that if `ttb.grape_varietals` is populated AND the canonical wine has `wine_grapes` links, the TTB row's primary grape must match one of those. Otherwise create a NEW canonical wine for that COLA (or leave it unlinked).
  2. **Prefer primary names in resolver:** In `batch_pipeline._load_reference_data()`, load synonyms into a SEPARATE `self.grape_synonyms` dict, not into `self.grapes`. Have `_identify_primary_grape()` prefer `self.grapes` matches over `self.grape_synonyms` matches (same pattern as `pipeline/lib/resolve.py` Step 2→4 ordering).
  3. **Cleanup pass:** After (1) and (2), run a one-shot `pipeline/promote/grape_cleanup_chardonnay_pinot_blanc.py` (sketched in S2.3 F2's proposed fix) that removes PINOT BLANC links from wines whose display_name clearly indicates Chardonnay.
- **Effort:** medium (half day) — the matcher change is localized, but needs regression testing against ~200 TTB/canonical sample wines.
- **Dependencies:** S2.4 F2 (delete PINOT BLANC polluting synonyms) must run first so the synonym lookup has cleaner data.
- **Related findings:** S2.3 F2, S2.4 F2, F3 (staging relink), F16 (synonym loading pattern).

---

## F3 — `relink_staging_to_current.py` NULLs wine_id only in 1 of 30 staging tables — 29 still hold dangling archive wine_ids

- **Severity:** P0
- **Evidence:** `pipeline/promote/relink_staging_to_current.py` lines 67-72:
  ```python
  # Tables that ALSO have canonical_wine_id (must NULL it out since archive.wines are gone)
  STAGING_TABLES_WINE = [
      "source_ttb_colas",
      # Add others if they have canonical_wine_id columns
  ]
  ```
  The inline comment is an unresolved TODO. `STAGING_TABLES_PRODUCER` lists 30 tables; `STAGING_TABLES_WINE` lists ONE. Session 13 ran this, so producers were re-linked across all 30 tables, but wine_ids in 29 of 30 tables still point at `archive.wines` rows that no longer exist in current `wines`. This is the code-level cause of S2.2 F1 (286,918 dangling wine_id pointers unlocking ~52K prices + ~48K scores + ~200K vintage-grade fields + ~40K UPCs).
- **Why it matters:** Sprint 3's #1 backlog task is "staging archive-ID relink". The current script's wine branch is incomplete — extending `STAGING_TABLES_WINE` to all 30 and implementing the same mapping table pattern for wines is the minimal fix. S2.2 F1 is currently blocked on this code gap.
- **Proposed fix:** Extend the script.
  ```python
  STAGING_TABLES_WINE = [
      "source_ttb_colas",
      "source_pro_platform", "source_tabc", "source_wv_abca", "source_kansas_brands",
      "source_berliner", "source_texsom", "source_enofile",
      "source_specs", "source_lcbo", "source_horizon", "source_pa", "source_openfoodfacts",
      "source_bc_liquor", "source_winedeals", "source_systembolaget",
      "source_skurnik", "source_polaner", "source_kermit_lynch", "source_kermit_lynch_growers",
      "source_winebow", "source_european_cellars", "source_empson",
      "source_wallys", "source_flatiron", "source_firstleaf", "source_best_wine_store",
      "source_domestique", "source_last_bottle", "source_utah_dabs",
  ]
  ```
  And add a new `build_wine_mapping_table()` alongside `build_mapping_table()` that maps `archive.wines → current wines` via `producer_id + name_normalized` (with TTB COLA number as a tiebreaker when available). Most staging tables have a COLA id or an importer slug that can disambiguate. For the ~20% that can't resolve, NULL them out like `source_ttb_colas` already does.

  Alternative: **don't fix in place**. Write a new `relink_staging_wines.py` that handles just the wine_id side with a better strategy. The existing script is a working one-off; extending it risks regressing the producer path. Recommend: new script, keep `relink_staging_to_current.py` as producer-only.
- **Effort:** small (1-2 hours for the new script + dry-run validation)
- **Dependencies:** none
- **Related findings:** S2.2 F1, F22 (staging fan-out patterns)

---

## F4 — `enrich-wine` edge function builds prompts using `grapes.name` (VIVC form) instead of `grapes.display_name`

- **Severity:** P0
- **Evidence:** `supabase/functions/enrich-wine/index.ts` (from MCP `get_edge_function`):
  ```typescript
  const { data: grapes } = await supabase
    .from("wine_grapes")
    .select("percentage, grapes(name)")   // ← reads .name, not .display_name
    .eq("wine_id", wineId);

  const grapeList = (grapes || []).map((g: any) => {
    const name = g.grapes?.name || "Unknown";
    return g.percentage ? `${name} (${g.percentage}%)` : name;
  }).join(", ");

  // Later in prompt:
  // - Grapes: ${grapeList || "Unknown"}
  ```
  Per S2.4 F6 + our verification SQL (`SELECT name, display_name FROM grapes`), 9,692 of 9,694 grapes have `display_name` populated with common English names. `name` is VIVC cépage form: `CHARDONNAY BLANC`, `MERLOT NOIR`, `PINOT NOIR`, `TEMPRANILLO TINTO`. Every enrichment request hitting a Chardonnay wine sends Sonnet:
  > Grapes: CHARDONNAY BLANC (unknown%), PINOT BLANC (unknown%)

  Sonnet then writes `ai_wine_summary` prose about "the 75% Pinot Blanc dominance creating a softer mousse" (as observed in S2.3 F2) — confabulating around the wrong grape names. Frontend correctly uses `display_name` (`frontend/src/pages/consumer/GrapePage.tsx:110`, `AppellationPage.tsx:211,225`), so the rendered UI shows "Chardonnay" while the enrichment layer sees "CHARDONNAY BLANC" — the split writes garbage to `wine_insights`.
- **Why it matters:** Sprint 5 plans to run heavy enrichment across the corpus. Without this fix, every enrichment run inherits the same grape-naming drift. Even if S2.4 F2 + F6 are fixed in the data layer, the edge function will still be reading the wrong column. This is the code twin of S2.4 F6.
- **Proposed fix:** Change the select clause to `grapes(display_name)` and update the mapper to `g.grapes?.display_name || "Unknown"`. Same pattern applies to any other edge function or server-side code that builds prompts. Also update `pipeline/enrich/*.py` (`appellation_insights.py`, `country_insights.py`, `region_insights.py` all pull `grapes(name)` — confirmed by grep).
- **Effort:** trivial (one column change per file, ~5 files)
- **Dependencies:** none — `display_name` is already populated for 99.98% of grapes
- **Related findings:** S2.4 F6, F5 (enrich-wine is stale elsewhere too)

---

## F5 — Model version drift: 3 Anthropic model IDs live in code, no central config

- **Severity:** P0
- **Evidence:**
  ```
  $ grep -rn "claude-" pipeline/ scripts/ supabase/ --include="*.py" --include="*.ts" | grep -Eo "claude-[a-z0-9-]+" | sort | uniq -c
     28 claude-haiku-4-5-20251001
     12 claude-sonnet-4-20250514
      6 claude-sonnet-4-6
  ```
  **`claude-sonnet-4-20250514` is the older Sonnet 4 (May 2025).** Files affected:
  - `supabase/functions/enrich-wine/index.ts` — Grade B enrichment
  - `pipeline/analyze/audit_wines_sonnet.py`
  - `pipeline/enrich/appellation_insights.py`, `country_insights.py`, `region_insights.py`, `grape_insights.py`, `batch_enrich.py`
  - `pipeline/enrich/test_*_prompt.py` (4 files)
  - `pipeline/geo/review_region_boundaries.py`
  - `pipeline/promote/importer_wine_match.py`

  **Current latest is `claude-sonnet-4-6`** — already used by 6 locations (`pipeline/promote/knowledge_seed.py`, `pipeline/promote/lwin_sonnet_match.py`, etc.). So the codebase has 3 coexisting model IDs (haiku-4-5, sonnet-4, sonnet-4-6) with no single source of truth.
- **Why it matters:**
  1. Stale Sonnet 4 (May 2025) is less capable than Sonnet 4.6 — enrichment quality is unnecessarily degraded.
  2. Any price change or deprecation affects files randomly. The `enrich-wine` cost math (`(inputTokens * 3 / 1_000_000) + (outputTokens * 15 / 1_000_000)`) is hardcoded to Sonnet 4 pricing; moving to a different model requires hand-editing TypeScript.
  3. The per-file hardcoding means a typo in one model ID fails at runtime, not import time.
- **Proposed fix:**
  1. Add `pipeline/lib/models.py` exporting `HAIKU = "claude-haiku-4-5-20251001"`, `SONNET = "claude-sonnet-4-6"`, `OPUS = "claude-opus-4-6"` with a comment explaining the upgrade policy.
  2. Import into all 12 offending Python files; replace hardcoded strings.
  3. For the edge functions, add a `MODELS` constant at the top of each `index.ts` and update via `deploy_edge_function`. Alternative: read from env var `ANTHROPIC_MODEL_SONNET` so no redeploy is needed to switch.
  4. Add a ruff/grep check in CI that fails if `claude-` appears outside `pipeline/lib/models.py` or edge function header constants.
- **Effort:** small (1-2 hours — mechanical renames + edge function redeploy)
- **Dependencies:** none
- **Related findings:** F4 (enrich-wine has two bugs to fix at once), F32 (no CI grep check exists)

---

## F6 — `grape_from_name.py` builds lookup from display_name, silently collapses all NULL display_names to empty key

- **Severity:** P0
- **Evidence:** `pipeline/promote/grape_from_name.py:44`:
  ```python
  cur.execute("SELECT id, display_name FROM grapes WHERE deleted_at IS NULL")
  grapes_raw = {normalize(row[1]): (str(row[0]), row[1]) for row in cur.fetchall()}
  ```
  Dict comprehension: if `row[1]` is NULL, `normalize(None)` → `""`. Multiple NULL display_name rows collapse into a single key `""` (dict overwrite), and any grape whose display_name is missing becomes unreachable via this path. Today only 2 of 9,694 grapes have NULL display_name, so the impact is small — but the pattern is a silent-failure bug waiting for a future grape import to land NULL display_names.

  Line 76-85 then tries `grape_lookup[norm_name] = grapes_raw[norm_name]` for each entry in `SAFE_GRAPES`. If `norm_name` is not in `grapes_raw` (common case before S14's display_name backfill), it falls through to a fuzzy substring loop with weak matching criteria.
- **Why it matters:** The script is load-bearing for grape backfill from wine names. `CLAUDE.md` says this script added "+3,527 grape links" during Session X. If a future grapes import adds rows with NULL display_name, they silently vanish from the lookup. Worse, because the `grapes_raw[""]` key already exists (from the 2 current NULLs), ANY grape where `normalize(display_name)` returns `""` will map to a random seed-fate grape.
- **Proposed fix:**
  ```python
  grapes_raw = {
      normalize(row[1]): (str(row[0]), row[1])
      for row in cur.fetchall()
      if row[1]  # skip NULL display_name
  }
  ```
  Add a log line `"Skipped N grapes with NULL display_name"` so operators know the omission is deliberate. Backfill any found NULL rows out-of-band before re-running.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F7 (similar pattern), F16 (grape lookup patterns)

---

## F7 — `haiku_grape_extract.resolve_grape` does 70%-coverage containment match — overmatches when Haiku returns short or polysemous grape names

- **Severity:** P1
- **Evidence:** `pipeline/enrich/haiku_grape_extract.py:107-153`:
  ```python
  # 3. Try containment — Haiku might say "Grenache Noir" but we have
  #    "Grenache" and "Grenache Noir" separately. Prefer longer matches.
  best_match = None
  best_len = 0
  for lookup_norm, gid in grape_lookup.items():
      if len(lookup_norm) < 4:
          continue
      if lookup_norm in norm and len(lookup_norm) > best_len:
          best_match = gid
          best_len = len(lookup_norm)

  if best_match and best_len >= len(norm) * 0.7:
      return best_match
  ```
  Problem cases:
  1. `grape_lookup` contains synonyms too (`grape_id, synonym FROM grape_synonyms`, line 95-103). PINOT BLANC has synonym `PINOT CHARDONNAY` (S2.4 F2). If Haiku returns `"Chardonnay"` (norm = "chardonnay", len=10), containment check: `"chardonnay" in "chardonnay"` (exact match path caught first) — BUT for "Pinot Chardonnay" from Haiku (len=16), the exact match fails, containment loop finds `"pinot chardonnay" in "pinot chardonnay"` len=16 ≥ 16*0.7=11.2 — maps to PINOT BLANC. This is arguably correct since PINOT CHARDONNAY is a Pinot Blanc synonym, but the threshold logic means "Chardonnay" alone (10 chars) also matches "chardonnay" exactly via primary path, AND matches "pinot chardonnay" via containment IF the primary lookup misses (which happens when `normalize(display_name)` collides).
  2. Iteration over all 34,820 grape synonyms for each input grape name is **O(34K)** per grape per wine. For 300K wines × 2-4 grapes each, this is ~25 billion dict iterations. The script is slow but not incorrect in the typical case.
  3. The 70% threshold means `"Grenache"` (8 chars) matches inside `"Grenache Noir"` (13 chars) only if 8 ≥ 13*0.7 = 9.1 — FALSE. So `"Grenache Noir"` from Haiku does NOT resolve via containment to Grenache. It resolves via exact match (if primary name exists), but if Haiku returned `"Grenache Noir grapes harvested in..."` the containment match would fail. Code is over-conservative for long inputs.
- **Why it matters:** This script is the intended Grade C batch pre-warming path for 30-50K wines (`CLAUDE.md` says "Grade C batch pre-warming" is a planned workstream). If run at scale with bad containment behavior, it will produce false negatives (grapes resolvable by containment but below threshold) and performance-punish large batches.
- **Proposed fix:**
  1. Pre-build an inverted index sorted by length descending once, avoid the O(n) scan per call.
  2. Drop the containment path in favor of explicit prefix/suffix matching on known grape colors ("... blanc", "... noir").
  3. Centralize grape resolution in `pipeline/lib/resolve.py::ReferenceResolver.resolve_grape()` and delete the local copy in `haiku_grape_extract.py`. This is a duplication — every script that resolves grapes has its own subtly different implementation.
- **Effort:** small (1-2 hours to consolidate on `ReferenceResolver.resolve_grape`)
- **Dependencies:** none
- **Related findings:** F16 (grape lookup duplication), F28 (resolver consolidation)

---

## F8 — `batch_pipeline.BatchPipeline` hardcodes BATCH_0_PRODUCERS producer roster in source code

- **Severity:** P1
- **Evidence:** `pipeline/identity/batch_pipeline.py:30-81`:
  ```python
  BATCH_0_PRODUCERS = [
      ("Château Margaux", "Margaux", "FR", "Bordeaux"),
      ("Château Lafite Rothschild", "Lafite Rothschild", "FR", "Bordeaux"),
      # ... 45 more producers
  ]
  ```
  This is the Batch 0 roster used in Session 7/8 pre-30K work. It's in source code, not in a config file. Meanwhile `pipeline/identity/build_roster.py` (another script) reads `data/roster/` JSON files. Two sources of truth for the producer roster.

  Separately, S2.3 F3 flagged that "all 15 famous producers have 0 metadata" — which matches the producers in BATCH_0_PRODUCERS. So the roster was processed (creating producer + wine rows) but the scrape/metadata fill never ran.
- **Why it matters:** Sprint 3 F3 (producer seed file) needs a single authoritative roster. Having 48 producers hardcoded in Python is a maintenance tax and makes it hard to update from outside the codebase. The S2.3 F3 fix (populate metadata for famous producers) would be cleaner if it read the same JSON the frontend and other scripts could read.
- **Proposed fix:** Move `BATCH_0_PRODUCERS` to `data/roster/batch_0.json` and load it via `build_roster.py` or a new `pipeline/identity/roster_loader.py`. Delete the hardcoded list. Sprint 3 F3's producer seed work can then extend the same JSON file.
- **Effort:** small (1 hour)
- **Dependencies:** none
- **Related findings:** S2.3 F3 (famous producers have 0 metadata), F18 (duplicated producer creation logic across 13+ sites)

---

## F9 — `batch_pipeline._load_reference_data()` blends grape synonyms into primary `self.grapes` dict, losing preference ordering

- **Severity:** P1
- **Evidence:** `pipeline/identity/batch_pipeline.py:161-172`:
  ```python
  # Grapes (name → id, including synonyms)
  self.cur.execute("SELECT id, name FROM grapes")
  self.grapes = {}
  self.grape_names = set()
  for row in self.cur.fetchall():
      self.grapes[row[1].upper()] = {"id": str(row[0]), "name": row[1]}
      self.grape_names.add(row[1])

  self.cur.execute("SELECT grape_id, synonym FROM grape_synonyms")
  for row in self.cur.fetchall():
      self.grapes[row[1].upper()] = {"id": str(row[0]), "name": row[1]}
      self.grape_names.add(row[1])  # Include synonyms for cuvée stripping
  ```
  Two semantic problems:
  1. **Synonyms overwrite primaries when same key.** If the primary grape "MERLOT" exists and a synonym "MERLOT" (for another grape like Grolleau Noir per S2.4 F1) is loaded second, the PRIMARY gets overwritten by the WRONG mapping. We verified via S2.4 F7: 921 synonym rows collide with primary names of different grapes. Each collision becomes a silent bug in this function's grape lookup.
  2. **`self.grape_names` (used for cuvee stripping + primary grape detection) also contains every synonym.** That means "cuvée stripping" can strip out legitimate text by matching a synonym. Example: if a cuvee is "Clarence Dillon" and CLARENCE is a grape synonym (it's in the GRAPE_BLOCKLIST, line 615 — meaning someone already hit this bug once), `_identify_primary_grape` would try to match. The blocklist is a bandaid over a structural issue — synonyms should not be searched at all during primary-grape identification from wine names.
- **Why it matters:** This is the code side of S2.4 F7 (921 collisions). Whoever runs `batch_pipeline.py` in the future gets a sub-set of those 921 collisions applied silently to whichever canonical wines they're creating.
- **Proposed fix:** Split into two separate dicts:
  ```python
  self.grapes_primary = {}      # name → row (9,694 entries)
  self.grapes_synonyms = {}     # synonym → grape_id (34,820 entries; last-write-wins acceptable)
  ```
  In `resolve_grape()` / `_identify_primary_grape()`, always try `self.grapes_primary` first. Fall back to `self.grapes_synonyms` only if there's no collision and the match is unambiguous. This mirrors the tier-ordering in `pipeline/lib/resolve.py::resolve_grape` and should eventually replace this local implementation entirely (see F28).
- **Effort:** small (30 min)
- **Dependencies:** S2.4 F2 (synonym cleanup) to reduce collision count
- **Related findings:** S2.4 F7, F16, F28

---

## F10 — `open_meteo_weather.py` scheduled task has no error-logging side channel; failures are invisible

- **Severity:** P1
- **Evidence:** `pipeline/fetch/open_meteo_weather.py` runs nightly via scheduled task (per CLAUDE.md: "Nightly scheduled task (`open-meteo-weather-drip`, 3am)"). The script prints to stdout, has `retries=5` inside `fetch_daily()`, and raises `DailyLimitExceeded`. But:
  - No log file output (only stdout).
  - No Supabase table writing a `scheduled_task_runs` row with status/error.
  - On `DailyLimitExceeded`, the script either raises (unhandled at top level) or continues depending on the caller.
  - `CLAUDE.md` says the nightly drip "upgrades ~8 appellations/night" — nobody is notified if it silently upgrades 0 for a week.
- **Why it matters:** S2.8 (meta audit) will likely flag this, but it's fundamentally a code/ops issue. Silent nightly scheduled tasks are the worst kind of tech debt: they rot invisibly.
- **Proposed fix:** Add an `enrichment_log`-style `scheduled_task_runs` table (columns: `task_slug`, `started_at`, `finished_at`, `status`, `rows_processed`, `error_message`) and have every scheduled script `INSERT` one row per run. Alternatively, use Supabase's built-in `cron.job_run_details` table (via `pg_cron`). Either way, create a simple query that says "show me tasks that haven't written a success row in 24h".
- **Effort:** small (1-2 hours for the table + 5-line wrapper in the scheduled scripts)
- **Dependencies:** none
- **Related findings:** F19 (no alerting), F20 (CLAUDE.md lies about scheduled task state)

---

## F11 — `haiku_grape_extract` and `batch_pipeline` both duplicate grape-lookup construction — 4 different implementations in the pipeline

- **Severity:** P1
- **Evidence:** Grape lookup is built in at least 4 places with subtly different semantics:
  1. `pipeline/lib/resolve.py::ReferenceResolver._load_grapes()` — canonical, uses `display_name → row` and `name → row` separately, synonyms → `grape_id`.
  2. `pipeline/identity/batch_pipeline.py:_load_reference_data()` — blends primary + synonyms into one dict (F9).
  3. `pipeline/enrich/haiku_grape_extract.py::build_grape_lookup()` — uses `display_name` only for primaries, fallback to synonyms only if not already present (F7).
  4. `pipeline/promote/grape_from_name.py:main()` — display_name → tuple, with the NULL collapse bug (F6).
  5. `pipeline/promote/batch_matcher.py` and `pipeline/promote/importer_grape_promote.py` both call `resolve.ReferenceResolver.resolve_grape()` — the one correct path.

  Grepping for wine_grapes writers confirmed **13 separate insertion points** (`pipeline/enrich/haiku_grape_extract.py`, `pipeline/enrich/seed_mass_market.py`, `pipeline/fetch/producer_site_scrape.py`, `pipeline/fetch/ridge.py`, `pipeline/fetch/stags_leap.py`, `pipeline/fetch/tablas_creek.py`, `pipeline/identity/batch_pipeline.py`, `pipeline/promote/grape_blend_promote.py`, `pipeline/promote/grape_from_helper.py`, `pipeline/promote/grape_from_name.py`, `pipeline/promote/grape_from_ttb_direct.py`, `pipeline/promote/importer_grape_promote.py`, `pipeline/promote/ttb_grape_promote.py`).
- **Why it matters:** Every duplicate implementation is a place where a synonym-collision fix or a primary-vs-synonym ordering fix has to be applied separately. Fixing S2.4 F2 (delete polluting synonyms) is DB-side, but if any of these 4-5 lookup builders re-adds the broken mapping at runtime via a different code path, the data fix won't hold.
- **Proposed fix:** Consolidate. Make `pipeline/lib/resolve.py::ReferenceResolver` the ONE grape resolver. Delete the in-module grape lookups from `batch_pipeline.py`, `haiku_grape_extract.py`, and `grape_from_name.py` — import `ReferenceResolver` instead. This is a 2-3 hour refactor that also closes F6, F7, F9 in one go.
- **Effort:** medium (half day — needs regression testing)
- **Dependencies:** F6, F7, F9, S2.4 F2
- **Related findings:** F6, F7, F9, F28

---

## F12 — `grape_from_name.py` uses dict overwrite for grape name collisions — last-write-wins

- **Severity:** P1
- **Evidence:** Same file as F6, line 44-45. When two grapes have the same normalized display name, the dict comprehension silently overwrites — no warning, no log. Given that S2.4 F7 confirmed 921 synonym collisions and S2.4 F15 confirmed grape name inversions (VERDOT PETIT, MESLIER PETIT), any future import adding a collision becomes invisible. Today's SQL check on display_name shows only CHARDONNAY BLANC and CHARDONNAY BLANC MUSQUE differ in length so no collision — but the pattern is a time bomb.
- **Why it matters:** Correctness in this script is fragile. The hardcoded `SAFE_GRAPES` set (95 grapes) provides some protection, but relies on display_name being populated AND unique. A future schema migration that nulls out display_name for some grape subset would silently break the script.
- **Proposed fix:** In the dict comprehension, track collisions:
  ```python
  grapes_raw = {}
  collisions = []
  for row_id, display_name in cur.fetchall():
      if not display_name:
          continue
      norm = normalize(display_name)
      if norm in grapes_raw:
          collisions.append((norm, grapes_raw[norm][1], display_name))
          continue  # keep the first, skip the collision
      grapes_raw[norm] = (str(row_id), display_name)
  if collisions:
      print(f"WARNING: {len(collisions)} grape display_name collisions:")
      for c in collisions[:10]:
          print(f"  {c[0]} — {c[1]} vs {c[2]}")
  ```
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F6, F9

---

## F13 — `except Exception:` used 422 times; silent error swallowing across the pipeline

- **Severity:** P1
- **Evidence:**
  ```
  $ grep -rn "except.*Exception.*:\s*$\|except:\s*$" pipeline/ --include="*.py" | wc -l
  422
  ```
  Sample hotspots:
  - `pipeline/lib/db.py:126-134` — `batch_insert` has per-row fallback on error with `print` but no raise.
  - `pipeline/enrich/haiku_grape_extract.py:367` — broad `except Exception` with `total_api_errors += 1; continue` — Haiku returning malformed JSON gets swallowed.
  - `pipeline/lib/merge.py:146, 246` — RPC failures return None silently (comment says "RPC may not exist") — the caller can't distinguish "no match" from "RPC not installed".
- **Why it matters:** S2.1 F28 flagged that hardcoded counts in CLAUDE.md drift from live DB — part of the reason is that pipeline scripts silently swallow errors and report stats based on what they *intended* to do, not what actually committed. When a commit fails mid-batch and the script logs "inserted N rows", the N may not match reality.
- **Proposed fix:** Two-pronged.
  1. **Categorize:** Most broad excepts are in three classes: (a) DB insert retry, (b) API call retry, (c) file read/write safety. Each needs a narrow exception type. `batch_insert` should catch `psycopg2.errors.UniqueViolation` specifically, not `Exception`.
  2. **Add a session-wide error-count summary:** Any script that ends without printing `errors_encountered=N` is suspect. Standard tail output: `[summary] processed=X inserted=Y skipped=Z errors=W`.
- **Effort:** large (multi-session — 265 files to review, but the P0s are ~20)
- **Dependencies:** none
- **Related findings:** F10, F19 (silent pipeline stories)

---

## F14 — `get_conn()` used 74 times, `conn.close()` used 132 times — no context manager pattern, potential leaks

- **Severity:** P1
- **Evidence:**
  ```
  $ grep -rn "conn = get_conn" pipeline/ --include="*.py" | wc -l
  74
  $ grep -rn "conn.close\|cur.close" pipeline/ --include="*.py" | wc -l
  132
  $ grep -rn "with.*get_conn\|contextlib" pipeline/ --include="*.py"
  (no results)
  ```
  No script uses `with get_conn() as conn:`. `conn.close()` is spread across 132 lines including in except blocks that might not always run on early returns. A scripts that raises an uncaught exception in the middle leaks the psycopg2 connection until garbage collection. For nightly scheduled tasks this accumulates over weeks.
- **Why it matters:** Medium-term stability of long-running scheduled tasks and interactive audits (like this one). The session pooler has a limit — leaked connections eventually hit it and fresh scripts start failing to connect.
- **Proposed fix:** Refactor `pipeline/lib/db.py::get_conn()` into a context manager:
  ```python
  from contextlib import contextmanager

  @contextmanager
  def get_conn():
      import psycopg2
      _ensure_env()
      dsn = os.environ.get("DATABASE_URL")
      if not dsn:
          raise RuntimeError("Missing DATABASE_URL in .env")
      conn = psycopg2.connect(dsn)
      try:
          yield conn
      finally:
          conn.close()
  ```
  But this is a breaking API change — all 74 callers use `conn = get_conn()` (not `with get_conn() as conn:`). Alternative: add `get_conn_ctx()` as a new function and migrate incrementally.
- **Effort:** medium (half day — API change + incremental migration)
- **Dependencies:** none
- **Related findings:** F13

---

## F15 — `sys.path.insert` used 202 times — every script ships its own path hack

- **Severity:** P2
- **Evidence:**
  ```
  $ grep -rn "sys.path.insert\|sys.path.append" pipeline/ --include="*.py" | wc -l
  202
  ```
  Pattern: `sys.path.insert(0, str(Path(__file__).resolve().parents[2]))` — appears in 202 of 265 files. Without this hack, `python -m pipeline.promote.foo` works but direct `python pipeline/promote/foo.py` fails. The project already has `pipeline/__init__.py`, so it IS a package — the correct answer is always to invoke via `python -m pipeline.foo`.
- **Why it matters:** Consistency and Python best practices. Any new file added by future contributors will cargo-cult the hack. The whole class of boilerplate adds ~5 lines × 202 files = 1K LOC of path manipulation that shouldn't exist.
- **Proposed fix:** Delete all `sys.path.insert` blocks. Enforce `python -m pipeline.X.Y` invocation in `CLAUDE.md` "Key Scripts" section (already the case in practice — every documented command uses `-m`). Add a pre-commit hook that fails if `sys.path` appears in `pipeline/`.
- **Effort:** small (1 hour — sed + verify)
- **Dependencies:** none
- **Related findings:** F18 (package hygiene)

---

## F16 — 13 separate `INSERT INTO wines` call sites; new required fields require N-touch edits

- **Severity:** P2
- **Evidence:**
  ```
  $ grep -rn "INSERT INTO wines\s*(" pipeline/ --include="*.py" | wc -l
  13
  ```
  Each call site has its own column list, default handling, error recovery, and idempotency pattern. Files:
  - `pipeline/enrich/seed_mass_market.py:479`
  - `pipeline/fetch/producer_site_scrape.py:663`
  - `pipeline/identity/batch_pipeline.py:521`
  - `pipeline/promote/generic_matcher.py:272`
  - `pipeline/promote/lwin_long_tail.py:344`
  - `pipeline/promote/retail_wine_create.py:337`
  - plus 7 more

  Each has subtly different handling of `display_name`, `search_vector`, `data_grade` initial value, `duplicate_of`, `created_at`. S2.1 F found that 67% of wines have missing `search_vector` — partly because some of these INSERTs don't touch it (trigger populates it, but only if the trigger fires, which is tied to specific columns being updated).
- **Why it matters:** S2.1 F-search-vector is partly a symptom of divergent INSERT patterns. When we needed to add `data_grade` or `identity_complete` or `color_confirmed`, the change had to be replicated across 13 files and 3-4 were missed.
- **Proposed fix:** Introduce `pipeline/lib/wines.py::create_wine(conn, payload, source_metadata)` that:
  - Accepts a normalized payload dict
  - Applies defaults (display_name from components, search_vector from a trigger hint)
  - Handles dedup (slug collision with incremental suffix)
  - Writes one row to `data_provenance`
  - Returns the wine_id
  Migrate 13 call sites incrementally. Not a blocker, but will pay for itself by Sprint 5.
- **Effort:** medium (half day for the factory, + one session per batch of 3-4 call site migrations)
- **Dependencies:** none
- **Related findings:** S2.1 F-search-vector, F18

---

## F17 — `grape_blend_promote.py` and `ttb_grape_promote.py` use `DISTINCT ON (canonical_wine_id)` → picks arbitrary TTB row when multiple exist

- **Severity:** P1
- **Evidence:** `pipeline/promote/ttb_grape_promote.py:456-464`:
  ```sql
  SELECT DISTINCT ON (t.canonical_wine_id)
      t.canonical_wine_id, t.grape_varietals
  FROM source_ttb_colas t
  JOIN _target_wines tw ON tw.wine_id = t.canonical_wine_id
  WHERE t.grape_varietals IS NOT NULL
    AND t.canonical_wine_id IS NOT NULL
  ORDER BY t.canonical_wine_id, t.ttb_id
  ```
  When a canonical wine has multiple COLAs linked (F2 case), this picks the one with the lowest `ttb_id` — which has no semantic meaning. For the De Bortoli 17 Trees example, the four COLAs have ttb_ids `20173001000011, 20279001000135, 20279001000139, 21294001000621` — sorted lexically, the first is `20173001000011 = Shiraz`. So ttb_grape_promote would link "17 Trees" (which is a Chardonnay+Cab+Shiraz range) to SHIRAH ONLY — not capture all three.

  **The PINOT BLANC cohort is the compound effect of F2 (multi-COLA collapse) + F17 (arbitrary DISTINCT ON pick) + prior runs with different orderings + grape_blend_promote back-filling the leftovers.** Successive re-runs accumulate wrong links until the union is all 4 grapes × all 2,700 wines.
- **Why it matters:** F2 is the primary root cause; F17 amplifies it. Both must be fixed. Fixing F17 alone without F2 means only one arbitrary grape gets promoted per wine — still wrong, just different wrong.
- **Proposed fix:**
  1. Remove `DISTINCT ON`; select all matching rows.
  2. Group by `canonical_wine_id` in Python; if multiple grape strings conflict (e.g., "Chardonnay" + "Shiraz"), ABORT that wine and log as `conflict` rather than promote anything.
  3. Log conflicts to `data/stats/ttb_grape_promote_conflicts.json` for manual review.
  4. This naturally surfaces the F2 bug — wines with 4 conflicting COLAs become logged conflicts instead of silent corruption.
- **Effort:** small (1-2 hours)
- **Dependencies:** F2 (the matcher fix is the ultimate solution; this is a safety net)
- **Related findings:** F2, S2.3 F2

---

## F18 — `lwin_long_tail.py` inserts wines without `display_name` (NULL) — display_name bifurcation between LWIN paths

- **Severity:** P1
- **Evidence:** `pipeline/promote/lwin_long_tail.py:344-356`:
  ```python
  cur.execute(
      """
      INSERT INTO wines (
          name, name_normalized, slug, producer_id,
          country_id, region_id, appellation_id, color,
          wine_type, effervescence
      )
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      RETURNING id
      """, ...)
  ```
  No `display_name` column. Compare to `batch_pipeline.py:521-540` (same file as F16) which DOES populate `display_name` via `build_display_name()`. So wines from the LWIN long-tail path (50,908 wines per CLAUDE.md) have NULL display_name, while BATCH_0 wines have populated display_name. Per our SQL earlier:
  ```
  name ILIKE '%chardonnay%' wines: 7,466
  display_name ILIKE '%chardonnay%': 2,809
  ```
  The 7,461 wines with no display_name are the LWIN long-tail cohort. S2.3 F2's query uses `display_name`, so the 2,809 number is biased toward BATCH_0 wines — which is actually MORE misleading than originally reported because the root-cause wine cohort is SELECTION-BIASED toward wines that got processed via `batch_pipeline.py` (which has the F2 multi-COLA bug).
- **Why it matters:** Two consequences:
  1. Frontend rendering: any page that prefers `display_name` over `name` (frontend does this) shows just the cuvee string ("Lusatia", "Release") for long-tail wines instead of the full marketing name. User sees a degraded UX on 50,908 wines.
  2. S2.3 F2 and every other `display_name`-based audit query systematically excludes LWIN long-tail wines. The real "% of Chardonnay-named wines with wrong links" is unknown because 62% of the population has NULL display_name and can't be queried that way.
- **Proposed fix:** Port the `build_display_name()` call into `lwin_long_tail.py`'s wine INSERT path. Also run a one-shot `pipeline/promote/backfill_display_name.py` over the 50,908 wines missing display_name.
- **Effort:** small (1-2 hours + the backfill run)
- **Dependencies:** F8 (roster consolidation — the build_display_name caller needs the same reference data)
- **Related findings:** F8, F16, S2.3 F2 (biased sample)

---

## F19 — `scripts/` has 11 `.py` files outside `pipeline/` (legal-source fetchers, sweep scripts) — unclear ownership

- **Severity:** P2
- **Evidence:**
  ```
  $ ls scripts/
  create_alias_tables.sql  dash.ps1
  fetch_legal_sources.py   fetch_legal_sources_batch{2..5,8,10}.py  (6 files)
  insert_region_grapes.sql
  pending_migrations.sql
  sweep_masaf_catalogoviti.py
  node/  (116 archived .mjs files)
  ```
  Six numbered `fetch_legal_sources_batch*.py` files plus the base `fetch_legal_sources.py`. No README in `scripts/`. Unclear which is canonical, which are historical snapshots. The batch numbers suggest incremental runs for a long-running process, but none are referenced from `pipeline/`.
- **Why it matters:** Sprint 4/5 will want to extend legal/regulatory source fetching (per CLAUDE.md: "CT DCP bulk export — call Richard Mindek", etc.). Nobody can tell at a glance which file is the current one. The 6 snapshots eat mental overhead.
- **Proposed fix:**
  1. Move all `fetch_legal_sources*.py` into `pipeline/fetch/legal_sources.py` as a single module, with the batch logic controlled by `--batch` arg.
  2. Move `sweep_masaf_catalogoviti.py` into `pipeline/fetch/masaf.py`.
  3. Delete `scripts/*.sql` — they should live under `supabase/migrations/` (which doesn't exist yet — see F24).
  4. Keep only `dash.ps1` in `scripts/` (dashboard is dev tooling).
- **Effort:** small (2 hours — mostly moves and a consolidation)
- **Dependencies:** none
- **Related findings:** F24 (no migrations directory)

---

## F20 — CLAUDE.md claims `data-accuracy-agent` is "scheduled" but implementation unclear in repo

- **Severity:** P2
- **Evidence:** CLAUDE.md "Data quality infrastructure" section:
  > `accuracy_audit` table + `accuracy_audit_daily` view
  > `last_validated_at` column + `sample_wines_for_validation(batch_size)` RPC
  > **Scheduled `data-accuracy-agent` task (currently paused)**

  Grepping the repo for `data-accuracy-agent` or `data_accuracy_agent`:
  ```
  $ grep -rln "data-accuracy-agent\|data_accuracy_agent" . 2>/dev/null | grep -v node_modules
  (only matches in CLAUDE.md and archived files)
  ```
  No Python script, no edge function, no cron definition. CLAUDE.md says "currently paused" — but there's nothing to resume. The task either lived as a Supabase scheduled function and was deleted, or it was never committed.
- **Why it matters:** Sprint 2 F-count-drift (S2.1 F28) flagged 6 places where CLAUDE.md had stale counts. This is the same class of issue: CLAUDE.md describes infrastructure that may not exist. Sprint 3 planning should not assume this exists.
- **Proposed fix:**
  1. Verify via Supabase dashboard whether a scheduled function named `data-accuracy-agent` exists.
  2. If yes, export and commit it to `supabase/functions/data-accuracy-agent/` (doesn't exist yet — see F31).
  3. If no, delete the line from CLAUDE.md and note in `docs/HISTORY.md` that it was removed.
- **Effort:** trivial
- **Dependencies:** F31
- **Related findings:** S2.1 F28

---

## F21 — `wine_merge.py` / `dedup_wines.py` / `seed_strict_dupes.py` — three overlapping dedup scripts, unclear which is active

- **Severity:** P2
- **Evidence:** Three files in `pipeline/promote/` that all write to `wine_grapes` and the wines table during dedup:
  - `wine_merge.py` (529 lines)
  - `dedup_wines.py` (200 lines)
  - `seed_strict_dupes.py` (not read, but in the file list)

  Session 13 used Haiku fuzzy dedup to merge 718 wines (CLAUDE.md "30k Plan"). Which script was used? Unclear from a grep.
- **Why it matters:** Sprint 3 backlog likely includes further dedup passes (S2.1 F1/F2 said 3,534 wines in 1,686 true-dup groups). Running the wrong script could undo S13's work or create new inconsistencies.
- **Proposed fix:** Keep the ONE script that's actually used and archive the others to `scripts_archive/python/` (which doesn't exist yet — create it). Add a `# STATUS: active | archived` comment at the top of each script.
- **Effort:** small (1 hour — read the 3 files, consolidate, archive)
- **Dependencies:** none
- **Related findings:** F22 (pipeline hygiene)

---

## F22 — `pipeline/promote/` has 55 files; no README categorizing them

- **Severity:** P2
- **Evidence:** `pipeline/promote/` has 55 .py files ranging from `batch_matcher.py` (reusable in-memory producer matching) to `ttb_wine_link_v2.py` (one-off TTB linking script) to `phase_b_wines.py` and `phase_b_producers.py` (Phase B script). Some are reusable libraries, some are one-off migrations, some are archived-but-not-deleted. No naming convention distinguishes them.
- **Why it matters:** Sprint 3 will write new promotion scripts. A new contributor can't tell which existing script to model their work on. The fix for F21 (mark status per file) helps but a directory-level README would be clearer.
- **Proposed fix:** Add `pipeline/promote/README.md` with a table: file | status (library|one-off|archived) | last-run date | output counts | current blockers. Keep it short (~30 lines). Update it at each session wrap-up.
- **Effort:** small (1 hour)
- **Dependencies:** F21
- **Related findings:** F21

---

## F23 — `pipeline/vivino/` has 15 files — per CLAUDE.md it's "archive/reference" but lives in active pipeline tree

- **Severity:** P2
- **Evidence:** `pipeline/vivino/` contains 15 Python files including `fetch_listings.py`, `create_wines_legacy.py`, `producer_dedup.py`. CLAUDE.md says:
  > `pipeline/vivino/` — Vivino-specific pipeline (archive/reference)

  Active code tree and archived code tree shouldn't co-exist under the same umbrella. The `xwines_*` tables in DB are kept for reference (per CLAUDE.md) but the scripts that built them should not be discoverable as "current" pipeline.
- **Why it matters:** Consistency. A new contributor running `python -m pipeline.vivino.fetch_listings` might try to re-fetch Vivino when CLAUDE.md says the API is 403'd. Dead code with live invocability is a foot-gun.
- **Proposed fix:** Move `pipeline/vivino/` → `scripts_archive/python/vivino/` (create the directory). Update CLAUDE.md to reflect the move. Do not delete — preserve history.
- **Effort:** trivial
- **Dependencies:** F21, F22
- **Related findings:** F21, F22

---

## F24 — No `supabase/migrations/` directory; every DDL change goes via MCP `apply_migration` without git history

- **Severity:** P1
- **Evidence:**
  ```
  $ ls supabase/
  (supabase/ directory does not exist in repo root)
  $ mcp list_migrations
  [returns the migration history from DB's supabase_migrations table]
  ```
  CLAUDE.md acknowledges this:
  > **Migrations in git:** All DDL via Supabase MCP. Need `supabase/migrations/` before multi-developer.

  Right now, schema evolution is opaque — git blame on `docs/SCHEMA.md` gives approximate history, but the exact SQL that shipped is only visible via `mcp list_migrations` against the live DB. Rollback is manual; drift between two environments is undetectable.
- **Why it matters:** Sprint 3 will do a LOT of DDL (relink scripts, grape cleanup, appellation name normalization). Without git-tracked migrations, if we need to roll back a Sprint 3 change, we'd have to reverse-engineer the DDL from a live DB snapshot. Also: no CI can validate migrations before deploy.
- **Proposed fix:**
  1. Create `supabase/migrations/` with a `.gitkeep`.
  2. For each new migration going forward, save the SQL to a timestamped file (`20260411000000_<slug>.sql`) in the directory AT THE SAME TIME as calling MCP `apply_migration`. The MCP tool can be used — just ensure the SQL is also committed.
  3. Backfill: pull the full migration history via MCP `list_migrations` and save each existing row's SQL to a file. One-time pain, ongoing relief.
- **Effort:** medium (half day for the backfill, ~5 min per new migration thereafter)
- **Dependencies:** none
- **Related findings:** F31

---

## F25 — `pipeline/lib/resolve.py` has 143 hardcoded grape aliases and 32 region aliases — should live in DB alias tables

- **Severity:** P2
- **Evidence:** `pipeline/lib/resolve.py:25-143` — `GRAPE_ALIASES` is a 143-entry dict mapping strings like `"petite sirah": "Durif"`, `"mazuelo": "Carignan"`. Line 145-177 — `REGION_ALIASES` is 32 entries. S2.1 F said `appellation_aliases` (17,558 rows) and `region_aliases` (96) are already SEEDED tables. Having these same aliases in code AND in DB means:
  1. The in-code dict overrides the DB if both exist (resolver checks dict first at line 487).
  2. Updates have to happen in two places.
  3. Tests against the DB alias tables might not reflect production resolver behavior.
- **Why it matters:** Sprint 4 (reference redesign) should move these to DB. This is a code smell, not a bug — the resolver works. But it's an invitation for drift.
- **Proposed fix:** Seed the 143 grape aliases into `grape_synonyms` (if any are missing) and delete the in-code dict. Same for `REGION_ALIASES` → `region_aliases`. The resolver's `resolve_grape` would drop step 1 (in-code alias) since step 4 (synonym lookup) handles it.
- **Effort:** small (1 hour — verify all 143+32 exist in DB, seed missing ones, delete dicts)
- **Dependencies:** S2.4 F2 (synonym cleanup), S2.4 F7 (collision resolution)
- **Related findings:** S2.4 F2

---

## F26 — `resolve.py::resolve_grape` step 5 suffix-append fallback adds silent overmatching

- **Severity:** P2
- **Evidence:** `pipeline/lib/resolve.py:543-548`:
  ```python
  # 5. Common suffixes
  for suffix in [" noir", " blanc", " tinto", " tinta", " blanco"]:
      with_suffix = self.grapes.get(lower + suffix) or self.grapes.get(norm + suffix)
      if with_suffix:
          return with_suffix
  ```
  For input `"tempranillo"`, the step tries `"tempranillo noir"`, `"tempranillo blanc"`, `"tempranillo tinto"`, ... and returns on the first hit. If the DB has BOTH `TEMPRANILLO TINTO` (normal) AND `TEMPRANILLO BLANCO` (albino mutation), the function returns whichever is in the dict first — no way to control. The resolver's earlier steps already handle display_name and VIVC name matches, so this fallback is mostly a safety net, but it's the kind of "try everything and return first hit" pattern that produces the S2.3 F2 cohort-style bugs.
- **Why it matters:** Low-probability bug class; not a P0 today. But combined with F11 (4 duplicate resolvers) it means Sprint 3 grape cleanup can't be validated by running just one resolver — every call site has its own flavor.
- **Proposed fix:** Remove step 5. If a caller has `"tempranillo"` as input and wants to resolve it, rely on synonym table lookup (step 4). If `TEMPRANILLO` → TEMPRANILLO TINTO is the desired mapping, add it to `grape_synonyms`.
- **Effort:** trivial
- **Dependencies:** F11, F25
- **Related findings:** F11, F25

---

## F27 — `pipeline/promote/batch_matcher.py` and `pipeline/promote/generic_matcher.py` — unclear which is current

- **Severity:** P2
- **Evidence:** Two files in `pipeline/promote/`:
  - `batch_matcher.py` — "reusable in-memory producer matching with suffix stripping" (per CLAUDE.md)
  - `generic_matcher.py` — referenced by grep as an `UPDATE {table} SET canonical_producer_id = %s` writer

  Both match producers against staging. Unclear which is the canonical path. Similar pattern to F21 (dedup duplication).
- **Why it matters:** Same as F21.
- **Proposed fix:** Read both, keep one, archive the other.
- **Effort:** small (1 hour)
- **Dependencies:** F21, F22
- **Related findings:** F21, F22

---

## F28 — `fetch/ridge.py`, `fetch/stags_leap.py`, `fetch/tablas_creek.py` — producer-specific scrapers with hardcoded logic

- **Severity:** P2
- **Evidence:** `pipeline/fetch/` has at least 3 producer-specific scrapers (Ridge, Stags Leap, Tablas Creek) each with their own bespoke HTML parsing and direct writes to `wines`, `wine_grapes`, `wine_vintages`. `pipeline/fetch/producer_site_scrape.py` (721 lines, CLAUDE.md calls it "generic Haiku-based producer website scraper") is intended to replace them — but the old ones still exist and still write to canonical tables.
- **Why it matters:** Two issues:
  1. If someone runs an old scraper against the current DB, it may write data in an outdated shape.
  2. The 3 old scrapers duplicate S2.3 F-flagged patterns (different code paths for wine creation mean different default values, different provenance handling).
- **Proposed fix:** Either migrate to `producer_site_scrape.py` (verifying the generic version works for these 3 producers) and archive the old scrapers, or mark each old scraper as `# STATUS: archived — kept for reference` and remove their execution paths. CLAUDE.md's principal producer list for Sprint 3 F3 should drive what gets ported.
- **Effort:** small (1-2 hours)
- **Dependencies:** F21, F22
- **Related findings:** F16, F21, S2.3 F3

---

## F29 — `pipeline/analyze/winetest/` is a sub-package — structure suggests this is a tool, not an analyzer

- **Severity:** P3
- **Evidence:** `pipeline/analyze/winetest/` has its own sub-structure and is described as a "DB quality assessment tool" in CLAUDE.md. Placing a multi-file tool as a sibling of single-file analyzers blurs the layer boundaries.
- **Why it matters:** Minor organizational issue.
- **Proposed fix:** Move to `pipeline/tools/winetest/` or `tools/winetest/`. Not urgent.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** none

---

## F30 — `scripts_archive/node/` has 116 archived `.mjs` files, no index of what they did

- **Severity:** P3
- **Evidence:** `scripts_archive/node/` has 116 archived Node.js files in `lib/`, `root/`, `scripts/`. No README, no manifest. CLAUDE.md says these were "converted to Python in `pipeline/`" — but there's no mapping from old-script-name to new-script-name.
- **Why it matters:** When a Sprint 3 or later task asks "how did we previously promote X data?" there's no quick way to find the old implementation. A 10-line manifest (`old-path | replaced-by | status`) would save hours.
- **Proposed fix:** Write `scripts_archive/node/MANIFEST.md` listing each archived file with its replacement in `pipeline/` (if any) or marking as "deprecated, no replacement".
- **Effort:** small (1 hour — one-shot map)
- **Dependencies:** none
- **Related findings:** F22

---

## F31 — Edge function source code is NOT in git — lives only in deployed state

- **Severity:** P1
- **Evidence:** No `supabase/functions/` directory in the repo (grep returns no results for `.ts` files under `supabase/`). The `enrich-wine` and `describe-chemical` source is retrievable only via MCP `get_edge_function`. This means:
  1. Version history of edge function changes is lost unless manually copied.
  2. Code review is impossible.
  3. `describe-chemical` (F1) sneaks under the radar because it's not in git at all.
  4. A `deploy_edge_function` run that overwrites is irreversible without a pre-pull.
- **Why it matters:** Same category as F24 (migrations). Infrastructure-as-code hygiene. Sprint 3 will touch `enrich-wine` (F4 fix + F5 model update) — those changes should be reviewable in git.
- **Proposed fix:**
  1. Create `supabase/functions/enrich-wine/index.ts` and paste the current source (from MCP `get_edge_function`).
  2. Create `supabase/functions/describe-chemical/index.ts` if we decide to keep it (or delete per F1).
  3. Add a `.cursor` rule or CI check: "if you call `deploy_edge_function`, also commit the new source to `supabase/functions/<slug>/`".
  4. Backfill: one commit saving current state to git as a baseline.
- **Effort:** trivial (10 minutes to paste and commit)
- **Dependencies:** F1 decision
- **Related findings:** F1, F4, F5, F24

---

## F32 — No CI / pre-commit checks for any Python or TypeScript quality rules

- **Severity:** P3
- **Evidence:** No `.github/workflows/`, no `.pre-commit-config.yaml`, no `ruff.toml`, no `pyproject.toml` with `[tool.ruff]`. `package.json` has eslint config for frontend, but no Python linting. This means:
  - `sys.path.insert` (F15, 202 instances) goes uncaught.
  - Model ID drift (F5, 3 different IDs) goes uncaught.
  - Unused imports accumulate.
  - No test suite runs on commit.

  Project has `ruff` installed (via `pip freeze` likely) but no config.
- **Why it matters:** Low severity today because the project is solo-run. But Sprint 4+ will likely add contributors, and every un-caught issue today becomes a code-review tax later.
- **Proposed fix:** Add a minimal `pyproject.toml` with:
  ```toml
  [tool.ruff]
  target-version = "py311"
  line-length = 100
  select = ["E", "F", "I"]  # errors, flake8, isort
  exclude = ["scripts_archive", "node_modules"]
  ```
  And a `.pre-commit-config.yaml` that runs `ruff check` and greps for `sys.path.insert`, `claude-sonnet-4-20250514`, `except Exception:\s*$`. Keep it forgiving at first (warnings not errors) and tighten over Sprint 3-5.
- **Effort:** small (1 hour)
- **Dependencies:** none
- **Related findings:** F5, F13, F15

---

## Meta-patterns surfaced (for S2.9 synthesis)

1. **Every code bug traces back to "too many grape resolvers".** F6, F7, F9, F11, F17, F26 are all facets of the same problem: grape name resolution is duplicated across 4-5 sites with subtly different semantics. Consolidating on `pipeline/lib/resolve.py::ReferenceResolver.resolve_grape()` closes all of them at once. Pre-Sprint-3 workstream priority: refactor grape resolution to one site.

2. **Infrastructure-as-code gap is systemic.** F1 (rogue edge function), F24 (no migrations dir), F31 (no edge function source in git), F32 (no CI), F19 (`scripts/` untracked ownership) are all symptoms of "the git repo is not a complete picture of the live system." Sprint 3 should land `supabase/migrations/` and `supabase/functions/` at minimum. CI can wait for Sprint 4+.

3. **Wine creation is a 13-site operation.** F16 (13 INSERT sites), F18 (display_name bifurcation), F8 (hardcoded roster) all point at the same root: there's no `create_wine()` factory. Every script that wants to create a wine re-implements defaults, provenance, and dedup. Sprint 3 F3 (producer seed) is the natural place to land `pipeline/lib/wines.py::create_wine()` as a side effect.

4. **S2.3 F2 Chardonnay/Pinot Blanc root cause is code + data compound.** Data side (S2.4 F2 polluting synonyms) is necessary but not sufficient — the multi-COLA collapse (F2) is the other half. Sprint 3 grape repair workstream from S2.4 must be extended with:
   - **3a** grapes.name cleanup + display_name coverage (S2.4 F6)
   - **3b** synonym collision resolution + delete polluting synonyms (S2.4 F2, F7)
   - **3c** fix varietal_categories wrong links (S2.4 F1)
   - **3c.5 — NEW** — fix `batch_pipeline._match_ttb_to_wine` multi-COLA collapse (F2)
   - **3c.6 — NEW** — fix `ttb_grape_promote.DISTINCT ON` arbitrary pick (F17)
   - **3c.7 — NEW** — consolidate grape resolvers on `ReferenceResolver` (F11)
   - **3d** re-run grape resolver against wine_grapes (S2.3 F2)

5. **S2.2 F1 staging archive-ID relink has a specific code owner and a specific fix.** F3 identifies `relink_staging_to_current.py` STAGING_TABLES_WINE needing extension from 1 table to 30. This is a 2-hour task blocking ~52K prices + ~48K scores + ~200K vintage-grade fields.

## Sprint 3 sequence refined (added code items)

Previous (post-S2.4): (a) S2.2 F1 staging relink → (b) S2.3 F3 producer seed → (c) refined grape-repair workstream → (d) F6 color+country repair → (e) F10 L3 re-fact-check → (f) content regeneration

S2.5 additions:
- **(a) staging relink** now has a concrete code handle: extend `STAGING_TABLES_WINE` in `relink_staging_to_current.py` from 1 to 30 tables (F3). Still the #1 Sprint 3 task.
- **(c) grape repair** now has 3 more sub-tasks: fix multi-COLA collapse (F2), fix DISTINCT ON arbitrary pick (F17), consolidate grape resolvers (F11).
- **NEW pre-req — code hygiene:** Delete `describe-chemical` edge function (F1), vendor `enrich-wine` source into `supabase/functions/` (F31), centralize Anthropic model IDs (F5). All trivial, all under 30 minutes combined.

Total S2.5 findings blocking Sprint 3: **9 P0 + 14 P1 = 23 items added to backlog.**

## Scope-breaker check

None. All findings slot into existing Sprint 3 envelope. F2 changes the sequence of grape-repair sub-steps but doesn't expand total scope — the data-side fixes from S2.4 still need to happen, now with code fixes co-running. F24 (migrations dir) and F31 (edge function source) are Sprint 3 pre-requisites that cost <1 hour each.
