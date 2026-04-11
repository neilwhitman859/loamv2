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

### [2026-04-10] Grape synonym ambiguity — broader cleanup
**Priority:** P2
**Scope:** 29+ grapes had short-synonym collisions in `grape_synonyms`. Session 11 fixed the worst cases (Riesling, Grolleau Noir, Dolcetto/Croatina → Nebbiolo false matches, Melon → Pinot Blanc, Calabrese di Montenuovo → Sangiovese, Canari Noir → Pinot Gris) — totaling ~5,000 wine_grapes rows repointed. Remaining ambiguous synonyms: `malvasia` (29 grapes), `plant d` (19), `raisin d` (13), `alicante` (11), `muscat d` (11), `greco` (10), `malaga` (10), `tinta` (8), `tokay` (7), `bonarda` (7), `nerello` (7). For each: decide between deleting the ambiguous synonym entirely or adding region/color-aware disambiguation.
**Why:** Same bug class as the Riesling → Crouchen fix. Any wine labeled "Malvasia" could be linked to any of 29 grapes depending on row ordering. Some are legitimate historical synonyms; the fix needs judgment per-grape, not a blind delete.
**Dependencies:** Needs a disambiguation strategy (probably: delete the plain short-synonym entries, require the full grape name for matching)
**Estimated effort:** 4-6 hours (auditing + targeted cleanups) + spot-check audit after
**Discovered in:** Session 11 (Knoll/Grolleau Noir grape-link investigation)

### [2026-04-10] Schema bug: `source_lwin.canonical_*` FK points to archive_*
**Priority:** P2 — scheduled for Session 14 Phase B
**Scope:** `source_lwin.canonical_wine_id` and `source_lwin.canonical_producer_id` foreign keys reference `archive_wines` and `archive_producers` (the old tables), not the new canonical `wines` and `producers`. Leftover from the 30K rebuild — tables were renamed but FKs stayed. Same issue may affect other `source_*` tables.
**Why:** Prevents `pipeline/promote/*` scripts from marking staging rows as processed_at after promotion. Caused the Fort Ross promotion to skip the source_lwin update step.
**Fix:** Drop the old FKs, add new ones pointing to the canonical tables.
**Estimated effort:** 15 minutes audit + single migration
**Discovered in:** Session 11 (Fort Ross promotion)
**Update:** Session 13 fixed the bug on all 30 non-LWIN `source_*` staging tables via a bulk relink (`relink_staging_to_current.py`) — `source_lwin` itself still points at `archive_*`. Scheduled for Session 14 Phase B W5.

### [2026-04-10] `wine_detail_view` and `wine_vintage_detail_view` don't expose `wine_insights`
**Priority:** P1 — scheduled for Session 14 Phase B
**Scope:** Frontend API views don't LEFT JOIN `wine_insights` or `wine_vintage_tasting_insights`, so the frontend can't access ai_hook, ai_wine_summary, ai_terroir_expression, etc. Fine for F/D page rendering (structured fields only), but blocks Grade B narrative display.
**Why:** Will be moot under the Reference-First sprint if wine pages stop consuming wine-level AI content, but still worth fixing — the view should be correct regardless.
**Estimated effort:** 1 hour (ALTER VIEW + test)
**Discovered in:** Session 11 (view verification)

### [2026-04-10] Wine_grapes has 6,337 wines with impossible grape percentages (>100% total)
**Priority:** P0 — blocks enrichment quality on ~12% of the corpus. **Scheduled for Session 14 Phase B W6 (audit-first with user review gate).**
**Scope:** 6,337 wines (12.3% of the pre-S13 51,614 active corpus; new S13 long-tail wines mostly have no grape percentages yet) have `wine_grapes.percentage` values that sum to more than 100%. Typical pattern: 275% = three grapes each at 100% + 75%, or 200% = two grapes each at 100%. This happens when multiple conflicting grape-assignment sources each set `percentage = 100` independently rather than normalizing to fractions of a blend. Affected wines include Kumeu River Hunting Hill (Chardonnay 100% + Pinot Noir 75% = 175%, but it's actually a 100% Chardonnay wine), Krug Clos du Mesnil, Joseph Phelps Eisele, and 6,334 others.
**Why:** The enrichment pipeline reads `wine_grapes` as ground truth. A 275% total causes Haiku/Sonnet to faithfully describe nonsense blends as if they were real ("pairs Pinot Noir with Chardonnay"). The auditor correctly flags these as factual errors, and Stage 1 pass 2 showed ~30% of the 34 fail wines had this bug. **No amount of prompt engineering can fix enrichment on these wines** — the data is wrong.
**Fix strategy:** Session 14 Phase B builds `pipeline/analyze/audit_grape_percentages.py` (read-only) and surfaces per-pattern breakdown (275 / 200 / 150 / etc.) with LWIN availability hints. User picks strategy per pattern (NULL-out / keep-highest / re-derive from LWIN) at the review gate; then `fix_grape_percentages.py` runs with `--dry-run` default.
**Estimated effort:** 2-4 hours investigation + fix + validation
**Discovered in:** Session 12 (Stage 1 pass 2 diagnosis)

### [2026-04-11] `wines.color` contradicts `appellation_rules.allowed_colors` on ~895 wines
**Priority:** P2 — scheduled for Session 14 Phase B W7 (fix #5)
**Scope:** Roughly 895 wines carry a color that's not permitted for their appellation under current legal rules. Typical offenders catalogued during Path A seeding: 800 Champagne red, 50 Chablis red, 2 Chianti Classico white, 2 Pommard non-red, 9 Barolo rosé, 5 Barolo white, 7 Sauternes red, 2 Barsac red, plus smaller counts elsewhere.
**Fix:** Session 14 Phase B `UPDATE wines SET color = NULL WHERE color NOT IN allowed_colors(appellation)` + data_provenance log.
**Discovered in:** Path A seeding batch 5 (2026-04-05)

### [2026-04-11] Durif/Petite Sirah ↔ Syrah reconciliation (~1,048 wines)
**Priority:** P3 — scheduled for Session 14 Phase B W7 (fix #6)
**Scope:** `DURIF` has 1,230 wine_grapes links. 1,048 of those have "Syrah" or "Petite Sirah" in the wine name. Durif and Petite Sirah are the same grape scientifically, so "Petite Sirah" → DURIF is correct. "Syrah" → DURIF is wrong (despite name confusion). Phase B splits into buckets A (Petite Sirah, keep) / B (Syrah only, delete) / C (both phrases, spot-check).
**Discovered in:** Session 11

### [2026-04-11] Session 14 Phase A — Housekeeping interregnum
**Priority:** P0 — active this session
**Scope:** Dashboard redesign (sprint + project), repo cleanup, backlog consolidation, tiny fixes #1-#4, CLAUDE.md aggressive rewrite. Natural commit point between Phase A and Phase B.
**Status:** In progress

### [2026-04-11] Session 14 Phase B — DB cleanup + P0 fix + bigger fixes + 30K closure
**Priority:** P0 — active this session
**Scope:** Drop temp tables, move `archive_*` to archive schema, fix source_lwin FKs, add wine_detail_view JOINs, read-only reference-insight audit, P0 grape percentage repair (audit-first with user review gate), bug fixes #5-#6, 30K sprint formal closure (Josh Test re-run, budget freeze, archive move, final commit).
**Status:** Pending Phase A completion

### [2026-04-11] CLAUDE.md leaked bug: 66 producers named as appellations
**Priority:** P3
**Scope:** About 66 producers in `producers` are actually appellation names (e.g., "Chianti Classico" as a producer). They act as magnet wines for the rest of the pipeline — anything that can't resolve the real producer gets attached to these. Concrete observed harm: ~71 staging rows attached to appellation-as-producer rows.
**Fix:** Needs a dedup logic design. Not an autonomous fix — product-critical.
**Estimated effort:** 2-3 hours investigation + 1 hour fix
**Discovered in:** 2026-04-04 follow-up pass (inherited from CLAUDE.md)

### [2026-04-11] CLAUDE.md leaked bug: `batch_matcher.match_wine` loose substring collapses distinct wines
**Priority:** P2
**Scope:** `pipeline/promote/batch_matcher.py:match_wine()` uses a loose bidirectional substring check that collapses distinct wines from the same producer. Known ~170 collisions across Skurnik/Empson/European Cellars (e.g. two different Domaine X cuvées both match "Domaine X"). Fix path: `retail_wine_create.py` needs to be responsible for creating missing canonicals instead of letting `match_wine` bind to the wrong existing row.
**Estimated effort:** 4-6 hours (code refactor + regression test on the known collisions)
**Discovered in:** 2026-04-04 follow-up pass (inherited from CLAUDE.md)

### [2026-04-11] CLAUDE.md leaked backlog: 665 unclear dedup groups remaining
**Priority:** P2
**Scope:** From the 2026-04-08 dedup session: 2,682 wine_merge groups in `match_decisions` had decision='unclear'. Session 11 Haiku reclassified 2,017 of them (1,982 true_duplicate, 35 distinct_wines), leaving 665 still unclear. Those need either a stronger classifier or human review.
**Fix:** Re-run Haiku dedup classifier with tighter prompt, OR manual review, OR Sonnet pass.
**Discovered in:** 2026-04-06 (inherited)

### [2026-04-06 carryover] 10,469 ai_accepted dedup groups unprocessed
**Priority:** P3
**Scope:** These were processed in the 2026-04-08 dedup merge session + Session 13 strict + Haiku passes. Likely fully closed — verify on next dedup session.
**Status:** Likely done, needs verification

---

## Workflow

**How to use this file:**
1. Read at session start before planning new work
2. When you discover an issue mid-session that's out of scope, append here with date, priority, scope, discovery context
3. When completing an item, move its entry to `data/sessions.md` Done under the session that closed it, and delete from this file
4. Prune stale items every ~5 sessions (ones that have been deprioritized or are no longer relevant)
5. Don't let this file grow past ~30 items. If it does, we're not triaging hard enough.
