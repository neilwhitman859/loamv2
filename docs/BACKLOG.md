# Loam Backlog

Append-only list of discovered issues and deferred work. Read at session start before planning new work. Move items to `data/sessions.md` Done when completed.

**Priority key:**
- `P0` — blocks a current goal
- `P1` — should be addressed this phase
- `P2` — important but deferred
- `P3` — nice to have

---

## Active

### [2026-04-10] LWIN long-tail promotion sweep (10+ wine producers)
**Priority:** P1
**Scope:** 3,230 LWIN producers with 10-19 wines each, 40,847 total wines not yet in canonical. 83% of the 10-19-wine bucket is missing. Would take active wines 51,599 → ~92,000.
**Why:** The 30K plan intentionally prioritized top-volume producers, leaving a long tail. Real producers like Fort Ross Vineyard, Littorai, etc. sit in this gap. On-demand creation is the planned fallback but a bulk sweep would eliminate thousands of zero-result searches in one pass.
**Dependencies:** None — LWIN promotion script exists (`pipeline/promote/lwin.py`). Needs a minor extension to filter by producer wine count.
**Estimated cost:** $0 (deterministic), 2-3 hours
**Tradeoff:** Dilutes average completeness per wine. These producers have fewer enrichment sources than the top 2,500.
**Discovered in:** Session 11 (Fort Ross gap investigation)

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

---

## Workflow

**How to use this file:**
1. Read at session start before planning new work
2. When you discover an issue mid-session that's out of scope, append here with date, priority, scope, discovery context
3. When completing an item, move its entry to `data/sessions.md` Done under the session that closed it, and delete from this file
4. Prune stale items every ~5 sessions (ones that have been deprioritized or are no longer relevant)
5. Don't let this file grow past ~30 items. If it does, we're not triaging hard enough.
