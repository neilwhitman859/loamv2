# Loam Backlog

Append-only list of discovered issues and deferred work. Read at session start before planning new work. Move items to `data/sessions.md` Done when completed.

**Priority key:**
- `P0` — blocks a current goal
- `P1` — should be addressed this phase
- `P2` — important but deferred
- `P3` — nice to have

---

## Active

### [2026-04-11] wine_merge.py missing conflict handling for wine_vintage_tasting_insights
**Priority:** P1
**Scope:** 80 match_decisions rows (22 strict + 58 Haiku) in Session 13 failed to merge because when both survivor and dupe have a `wine_vintage_tasting_insights` row for `(wine_id, vintage_year=0)` (NV bucket), the per-group merge tries to INSERT a second row and collides with the unique key `wine_vintage_tasting_insights_wine_id_vintage_year_key`. wine_merge.py handles conflict drops for `wine_grapes`, `wine_label_designations`, `wine_insights` etc. but not `wine_vintage_tasting_insights`.
**Fix:** Add `("wine_vintage_tasting_insights", "wine_id", ["wine_id"])` to the WINE_ID_TABLES list, OR change its existing entry from `conflict_cols=None` to explicit conflict handling. Also check `wine_vintage_insights` for the same bug.
**Dependencies:** None
**Estimated effort:** 30 minutes (1 code change + retest by running wine_merge on the 80 pending match_decisions)
**Discovered in:** Session 13 (2026-04-11)

### [2026-04-11] wine_dupe_classify.py --max-dupes N broken
**Priority:** P3
**Scope:** The `--max-dupes N` flag changes the HAVING clause from `count(*) > 1` to `count(*) <= N`, which incorrectly accepts singleton groups (count=1). A Session 13 run wrote 697 match_decisions for singletons — the classifier returned "single record, no duplicate classification needed" for each but still set status to ai_accepted/flagged. All 697 had to be manually marked ai_rejected before wine_merge.
**Fix:** Change to `HAVING count(*) BETWEEN 2 AND N` or add an explicit `COUNT(*) >= 2` floor.
**Dependencies:** None
**Estimated effort:** 5 minutes
**Discovered in:** Session 13

### [2026-04-11] 895K source_ttb_colas rows unmappable (archive producers with no current match)
**Priority:** P2
**Scope:** After Session 13's staging relink, the mapping found only 10,475 archive producers with a current-canonical name_normalized match, leaving 2,482,061 source_ttb_colas rows with NULL canonical_producer_id. These represent TTB records whose producer was in archive_producers but was never recreated in the 30K rebuild or the LWIN long-tail sweep. Recovery path: `pipeline/promote/ttb_producer_bridge.py` can re-match these via normalized name matching against current canonical producers (for ones where the LWIN sweep created a matching producer AFTER the staging relink ran — since the staging relink only used the name map that existed at its start).
**Fix:** Re-run ttb_producer_bridge.py or similar name-based re-matcher. Should recover a meaningful chunk since the LWIN long-tail sweep added 8,146 new producers that weren't in the mapping during Session 13.
**Estimated effort:** 1-2 hours
**Discovered in:** Session 13

### [2026-04-10] LWIN long-tail promotion sweep (10+ wine producers)
**Priority:** ~~P1~~ **DONE in Session 13** (2026-04-10/11)
**Actual result:** Scope expanded from "10-19 wine bucket" to "all US + INTL>=8". **+8,146 producers, +104,009 wines. Active wines 51,614 → 155,623.**
**Remaining:** 69,470 LWIN rows for INTL producers with <8 wines — intentionally skipped per the US-biased scope the user chose. Could be a follow-up session if desired.
**Closed:** Session 13

### [2026-04-10] Grape synonym ambiguity — broader cleanup
**Priority:** P2
**Scope:** 29+ grapes had short-synonym collisions in `grape_synonyms`. Session 11 fixed the worst cases (Riesling, Grolleau Noir, Dolcetto/Croatina → Nebbiolo false matches, Melon → Pinot Blanc, Calabrese di Montenuovo → Sangiovese, Canari Noir → Pinot Gris) — totaling ~5,000 wine_grapes rows repointed. Remaining ambiguous synonyms: `malvasia` (29 grapes), `plant d` (19), `raisin d` (13), `alicante` (11), `muscat d` (11), `greco` (10), `malaga` (10), `tinta` (8), `tokay` (7), `bonarda` (7), `nerello` (7). For each: decide between deleting the ambiguous synonym entirely or adding region/color-aware disambiguation.
**Why:** Same bug class as the Riesling → Crouchen fix. Any wine labeled "Malvasia" could be linked to any of 29 grapes depending on row ordering. Some are legitimate historical synonyms; the fix needs judgment per-grape, not a blind delete.
**Dependencies:** Needs a disambiguation strategy (probably: delete the plain short-synonym entries, require the full grape name for matching)
**Estimated effort:** 4-6 hours (auditing + targeted cleanups) + spot-check audit after
**Discovered in:** Session 11 (Knoll/Grolleau Noir grape-link investigation)

### [2026-04-10] Durif/Petite Sirah ↔ Syrah reconciliation
**Priority:** P3
**Scope:** `DURIF` has 1,230 wine_grapes links. 1,048 have "Syrah" or "Petite Sirah" in the wine name. Durif and Petite Sirah are the same grape scientifically, so linking "Petite Sirah" to DURIF is correct. But linking "Syrah" to DURIF is wrong — they're different grapes (despite the name confusion). Audit which of the 1,048 are "Petite Sirah" (correct) vs plain "Syrah" (wrong).
**Why:** Borderline case — not blocking, but wines labeled Syrah shouldn't be linked to Durif.
**Estimated effort:** 30 minutes of SQL + spot-check
**Discovered in:** Session 11

### [2026-04-10] Schema bug: `source_lwin.canonical_*` FK points to archive_*
**Priority:** P2
**Scope:** `source_lwin.canonical_wine_id` and `source_lwin.canonical_producer_id` foreign keys reference `archive_wines` and `archive_producers` (the old tables), not the new canonical `wines` and `producers`. Leftover from the 30K rebuild — tables were renamed but FKs stayed. Same issue may affect other `source_*` tables.
**Why:** Prevents `pipeline/promote/*` scripts from marking staging rows as processed_at after promotion. Caused the Fort Ross promotion to skip the source_lwin update step.
**Fix:** Drop the old FKs, add new ones pointing to the canonical tables.
**Estimated effort:** 15 minutes audit + single migration
**Discovered in:** Session 11 (Fort Ross promotion)

### [2026-04-10] Enrichment quality fix (three-layer redesign)
**Priority:** P0 — blocks Grade B shipping to users
**Scope:** Session 10 audit found Grade C 2.48/5, Grade B 2.65/5, with 111+91 factual_error tags. Root cause: models confabulate facts when the prompt doesn't constrain them to verified data. Fix is a three-layer redesign:
- **L1:** Retrieval-grounded prompts that include a structured "facts packet" (wine identity + `appellation_rules` text + `wine_vintages` chemistry + known unknowns) and explicit "do not invent" instructions
- **L2:** Per-field constraints — `ai_comparable_wines` pulls ONLY from our own DB via deterministic query; `ai_terroir_expression` leans on `appellation_rules`; `ai_vinification_summary` leans on real winemaker notes
- **L3:** Post-generation fact-check pass (Haiku) that validates claims against ground truth; retry once if flagged
**Dependencies:** None — we have all the source data (`appellation_rules` 549 rows, `wine_grapes`, `wine_vintages`, producer site scrapes for 84 producers)
**Estimated cost:** ~$50-80 for full re-enrichment of 105 Grade B + 4,857 Grade C with Batch API
**Validation plan:** Build minimal L1 prototype, re-enrich 10 worst audit samples, re-audit, decide on L3
**Discovered in:** Session 10, designed in Session 11
**Status:** Validation test being built this session

### [2026-04-10] `wine_detail_view` and `wine_vintage_detail_view` don't expose `wine_insights`
**Priority:** P1
**Scope:** Frontend API views don't LEFT JOIN `wine_insights` or `wine_vintage_tasting_insights`, so the frontend can't access ai_hook, ai_wine_summary, ai_terroir_expression, etc. Fine for F/D page rendering (structured fields only), but blocks Grade B narrative display.
**Why:** Natural time to add the JOIN is when the enrichment quality fix lands — we shouldn't expose the current 2.48-2.65/5 quality content to users anyway.
**Estimated effort:** 1 hour (ALTER VIEW + test)
**Discovered in:** Session 11 (view verification)

### [2026-04-10] Session 10 S11.6 misclassification (false positive)
**Priority:** P3 — already corrected in Session 11 followup
**Scope:** Session 10's S11.6 check reported 2,272 "real duplicate groups" — this was a false positive. The grouping used `name_normalized` which for mass-market producers is the cuvée LINE name (e.g., Dark Horse "One Horse Town") not the varietal. Distinct SKUs were being grouped as dupes. True count with strict grouping (producer + display_name + appellation) was 181, now merged. The `pipeline/analyze/thirty_k_validate.py` U2 check logic should be updated to use the stricter grouping.
**Estimated effort:** 20 minutes
**Discovered in:** Session 11 (user question about 2,272 dedup groups)

### [2026-04-09 carryover] 2,682 unclear dedup groups
**Priority:** P2
**Scope:** From 2026-04-08 dedup session: 2,682 wine_merge groups in `match_decisions` with decision=unclear. Session 11 Haiku reclassified 2,017 of them (1,982 true_duplicate, 35 distinct_wines), leaving 665 still unclear. Those need human review or a stronger classifier.
**Discovered in:** Session 11 (inherited)

### [2026-04-06 carryover] 10,469 ai_accepted dedup groups unprocessed
**Priority:** P3
**Scope:** Wait — these were actually processed in the 2026-04-08 dedup merge session. Verify this is fully closed.
**Status:** Likely done, needs verification

### [2026-04-10] Wine_grapes has 6,337 wines with impossible grape percentages (>100% total)
**Priority:** P0 — blocks enrichment quality on ~12% of the corpus
**Scope:** 6,337 wines (12.3% of 51,614 active) have `wine_grapes.percentage` values that sum to more than 100%. Typical pattern: 275% = three grapes each at 100% + 75%, or 200% = two grapes each at 100%. This happens when multiple conflicting grape-assignment sources each set `percentage = 100` independently rather than normalizing to fractions of a blend. Affected wines include Kumeu River Hunting Hill (Chardonnay 100% + Pinot Noir 75% = 175%, but it's actually a 100% Chardonnay wine), Krug Clos du Mesnil, Joseph Phelps Eisele, and 6,334 others.
**Why:** The enrichment pipeline reads `wine_grapes` as ground truth. A 275% total causes Haiku/Sonnet to faithfully describe nonsense blends as if they were real ("pairs Pinot Noir with Chardonnay"). The auditor correctly flags these as factual errors, and Stage 1 pass 2 showed ~30% of the 34 fail wines had this bug. **No amount of prompt engineering can fix enrichment on these wines** — the data is wrong.
**Discovery:** Stage 1 pass 2 analysis of Kumeu River Hunting Hill, confirmed by population query in Session 12.
**Fix strategy (proposed, not agreed):**
- For wines with exactly 1 grape_link and percentage=100: OK, leave alone
- For wines where SUM(percentage) > 100: either (a) reset all percentages to NULL for that wine (conservative), (b) keep only the single-highest-confidence grape, or (c) re-derive from a stronger source (LWIN percentage field if present)
- Best approach needs a session-level discussion. Do NOT autonomously fix in bulk without review — the repair policy is product-critical.
**Estimated effort:** 2-4 hours investigation + fix + validation
**Discovered in:** Session 12 (Stage 1 pass 2 diagnosis)

### [2026-04-10] 270 Grade C wines have thin facts packets (<3 facts) despite Grade C assignment
**Priority:** P1 — blocks enrichment ceiling on thin-packet wines
**Scope:** 270 wines assigned `data_grade = 'C'` have fewer than 3 of the 5 canonical facts (grape, appellation, vintage, score, price). 28 have only 1 fact, 242 have exactly 2. Example: Quinta do Noval Black (fortified red, no grapes, no appellation, no vintage, no score). For these wines, Grade C enrichment can only produce "identity stub" content ("X is a red wine from Y") that scores 1-2/5 on the voice audit regardless of model choice. This suggests the data_grade='C' assignment is not strictly gated on packet richness — some thin-packet wines slipped through.
**Why:** These wines are dragging down the Grade C population average on any population-level audit. Either (a) they should be demoted to Grade D until more data is promoted, or (b) Grade C should write purely structured output for thin-packet wines instead of attempting prose.
**Fix strategy:** Add a `richness_score` column or view that counts non-NULL facts; re-run the grade assignment with the richness floor as a gate; or downgrade these 270 specifically.
**Estimated effort:** 1 hour (SQL audit + grade reassignment) + reruns
**Discovered in:** Session 12 (Stage 1 pass 2 diagnosis — Quinta do Noval Black case)

---

## Workflow

**How to use this file:**
1. Read at session start before planning new work
2. When you discover an issue mid-session that's out of scope, append here with date, priority, scope, discovery context
3. When completing an item, move its entry to `data/sessions.md` Done under the session that closed it, and delete from this file
4. Prune stale items every ~5 sessions (ones that have been deprioritized or are no longer relevant)
5. Don't let this file grow past ~30 items. If it does, we're not triaging hard enough.
