# S2.6 — Voice / Editorial Audit

**Sprint:** 2 (Audit)
**Session:** 6 of ~9 (S2.6)
**Expert hat:** Voice — editorial correctness, prompt discipline, cliché density, confabulation resistance
**Budget:** $0 expected (Opus inline per ratified S2.3/S2.4/S2.5 pattern)
**Primary deliverable:** `data/sprints/audit/findings/findings_voice.md`

---

## Context

S2.1-S2.5 audited the DB layers and the code that writes to them. S2.6 audits everything that's downstream of the data: the prose, the prompts that produce it, and how it measures against `docs/VOICE.md`.

This matters because voice is THE user-facing artifact — every search result, every wine page, every producer profile ultimately renders this content. If the voice is broken, no amount of schema hardening fixes the product. Sprint 5 (Reference Enrichment) is the planned execution phase, which means S2.6 must pin down exactly which prompts, rules, and content rows need to change before Sprint 5 runs at scale.

Several prior findings point at the voice/prompt layer:

- **S2.3 F10** — AI content confabulates narratives when inputs are wrong (Joseph Phelps Eisele "claims Phelps bought Eisele in 2013"). Primary-source verified wrong. S2.6 tests whether this pattern extends across the full 5,108-row enriched corpus.
- **S2.3 F14** — AI content confabulates geology (Hunter Valley "volcanic", Santa Ynez "Franciscan shale"). S2.6 checks if prose confabulation is reinforced by reference-layer prose.
- **S2.4 F18** — Hunter Valley → Basalt in `appellation_soils` is confabulation at the structured layer. S2.6 should find whether the reference-insight prose layer is similarly polluted and whether it FEEDS into wine prompts.
- **S2.5 F4** — `enrich-wine` edge function reads `grapes.name` (VIVC form) not `display_name`. Every Grade B prompt inherits wrong grape labels.
- **S2.5 F5** — Three Anthropic model IDs coexist. S2.6 checks which ones actually shipped into which enrichment surfaces.
- **Session 10 audit (2026-04-10, pre-Sprint-2)** found Grade B at 2.65/5 with 91 factual_error tags. The `ENRICHMENT_ENABLED=false` feature flag on the edge function dates from that audit. S2.6 validates whether the flag is still the right call.

## Read first

- `CLAUDE.md` — always
- `docs/VOICE.md` — the yardstick. Every finding references back to it.
- `docs/ENRICHMENT.md` — letter-grade model + the prompt architecture story
- `data/sprints/audit/findings/findings_db_canonical.md` — S2.1 (empty insight tables, producer metadata near-zero)
- `data/sprints/audit/findings/findings_db_staging.md` — S2.2 (contamination source for wine facts)
- `data/sprints/audit/findings/findings_wine_canonical.md` — S2.3 (F10 confabulation pattern, F14 geology)
- `data/sprints/audit/findings/findings_wine_reference.md` — S2.4 (F1 varietal_categories, F18 appellation_soils)
- `data/sprints/audit/findings/findings_code.md` — S2.5 (F4 grape.name bug, F5 model drift, F31 edge function source not in git)
- `data/sprints/audit/status.md` — sprint plan
- `memory/feedback_opus_inline_reasoning.md` — ratified pattern

## Objectives

1. **Inventory the voice corpus.** All insight tables (wine_insights, region_insights, appellation_insights, country_insights, grape_insights, wine_vintage_tasting_insights). Count by tier, by country, by enriched_at.
2. **Read every enrichment prompt.** 4 reference scripts (`appellation_insights.py`, `region_insights.py`, `country_insights.py`, `grape_insights.py`), the new `enrich_prompts.py` (Grade B/C wine), the live `enrich-wine` edge function via MCP `get_edge_function`. Compare the voice-rules blocks. Document which prompts are behind the current `enrich_prompts.py` bar.
3. **Audit enriched content against VOICE.md section-by-section.** Pull stratified samples from each insight table. Check for: hedging words, sommelier theater, generic filler, performative enthusiasm, overly poetic language. Quantify violations via LIKE scans on the full corpus.
4. **Audit food pairing content.** `wine_food_pairings` structured table + `ai_food_pairing` prose. Match against VOICE.md Food Pairings section (classics first, real dishes, cuisines, cover the table, flavor logic, banned patterns).
5. **Trace confabulation chains.** Reference insights → wine insights. Does the edge function's `assembleContext` inject reference-layer prose as context? If so, does confabulation propagate?
6. **Document pattern density.** Structural clichés and templates that aren't individual banned words but become tics at scale (e.g. "force vines to struggle", em-dash food pairing format, "elegant" as default adjective).
7. **Write `findings_voice.md`** with severity-tagged findings, concrete receipts, proposed fixes.

## Method

- Opus 4.6 inline — no Haiku/Sonnet API calls
- Supabase MCP `execute_sql` for corpus inventory, sample pulls, LIKE scans
- Supabase MCP `get_edge_function` for live enrich-wine source
- `Glob` / `Read` across `pipeline/enrich/*.py` and `supabase/functions/` (note: supabase/functions/ is not in git per S2.5 F31, use MCP)
- Read-only — no DDL, no DML, no fixes, no prompt runs
- For each violation, pull a concrete row with `SELECT ... LIMIT` and quote the actual prose in the finding
- Cross-reference S2.3/S2.4/S2.5 — don't re-log known findings, extend them with voice-layer evidence

## Severity scale

- **P0** — broken or user-visible correctness issue rooted in voice/prompt/content
- **P1** — significant risk, must fix before Sprint 5 runs at scale
- **P2** — improvement, not blocking
- **P3** — nice to have

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

## Scope boundaries

- **In scope:** all enrichment prompts, all insight tables (prose), food pairing content (structured + prose), the edge function's prompt construction
- **Out of scope:** frontend rendering (S2.7), docs/memory drift (S2.8), business positioning (S2.9), code quality beyond the enrichment prompts (S2.5 already covered)
- **Partial scope:** `wine_vintage_tasting_insights` sensory grids are fact-grid content, not prose — spot-check but defer structural audit to S2.7

## Exit criteria

- [ ] `findings_voice.md` written with severity-tagged findings
- [ ] Every finding has at least one concrete SQL-evidenced quote from the actual corpus
- [ ] Prompt drift quantified (which prompts are ahead of / behind `enrich_prompts.py`)
- [ ] Confabulation feedback loop traced end-to-end with at least one concrete example
- [ ] Session 10 feature flag decision validated or revised
- [ ] `sessions.json` S2.6 entry marked `done` with $0 ai_spend
- [ ] `journal.md` S2.6 section completed
- [ ] `budget.json` S2.6 entry at $0
- [ ] `CLAUDE.md` Current State updated (S2.6 done + finding count)
- [ ] `memory/project_sprint2_findings.md` updated with S2.6 cross-references
- [ ] `data/sessions.md` whiteboard entry moved to Done
- [ ] Commit: `S2.6: Voice expert audit — N findings`
