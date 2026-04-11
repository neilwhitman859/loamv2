# S2.4 — Wine Expert: Reference Content Audit

**Sprint:** 2 (Audit)
**Session:** 4 of ~9 (S2.4)
**Expert hat:** Wine — reference content correctness
**Budget:** $0 expected (Opus inline per S2.3 pattern; ratified as default in DECISIONS.md 2026-04-11)
**Primary deliverable:** `data/sprints/audit/findings/findings_wine_reference.md`

---

## Context

S2.1 covered the reference layer **structurally** (FK integrity, orphans, duplicates, staleness). S2.4 is **content correctness** — is the reference data factually right?

Reference tables are the Sprint 2-3 through-line, and the S2.1 meta-pattern #2 said the reference layer is in better shape than the canonical wine/producer layer. S2.4 stress-tests that claim against primary sources.

S2.3 uncovered three findings that point at the reference layer:

- **F2 (P0):** Chardonnay/Pinot Blanc grape linkage bug affects 2,743 of 2,809 Chardonnay-named wines (97.6%). The `grapes` table has both `CHARDONNAY BLANC` and `PINOT BLANC` as separate entries. The root cause may be in `grape_synonyms` (a bad synonym row), in `varietal_categories` (a bad category link), or in name-matching code (S2.5 scope). S2.4 owns the data side.
- **F7 (P0):** Invented grape "GARRO" on Messina Hof Papa Paulo Port; primary-source says the grape is Lenoir/Black Spanish. Check whether "GARRO" exists in the `grapes` table.
- **F14 (P1):** AI content confabulated geology ("Hunter Valley volcanic", "Santa Ynez Franciscan shale"). Those claims lived in `wine_insights` prose, but S2.4 checks whether the structured `appellation_soils` data is any better.

## Read first

- `CLAUDE.md` — always
- `docs/SCHEMA.md` — reference table field reference
- `data/sprints/audit/findings/findings_db_canonical.md` — S2.1 structural findings on reference layer (F7 919 grape synonym primary-name collisions, F16 appellation_grapes provenance, F20-23 reference layer structure)
- `data/sprints/audit/findings/findings_wine_canonical.md` — S2.3 findings (especially F2, F7, F14)
- `data/sprints/audit/status.md` — sprint plan
- `memory/feedback_opus_inline_reasoning.md` — ratified pattern for this kind of session

## Objectives

1. Audit `appellation_rules` content correctness — sample 30-50 rules against INAO/DOCG/TTB AVA primary sources
2. Audit `appellation_grapes` required/permitted lists against the same primary sources, with focus on famous appellations
3. Audit `grape_synonyms` for root cause of S2.3 F2, plus spot-check the 919 primary-name collisions flagged in S2.1 F7
4. Audit `varietal_categories` structure + membership
5. Audit `grapes` parent/child relationships for content correctness (not just structural integrity)
6. Audit `appellation_soils` + `soil_types` content against primary sources (TTB AVA docs, INAO, regional councils)
7. Spot-check `tasting_descriptors`, `farming_certifications`, `biodiversity_certifications`, alias tables
8. Write `findings_wine_reference.md` with severity-tagged findings

## Method

- Opus 4.6 inline — no Haiku/Sonnet API calls
- Query DB for ground truth via Supabase MCP `execute_sql`
- WebFetch primary sources when a finding needs external corroboration
- Sample high-signal appellations: Burgundy grand crus, Bordeaux crus classés, Barolo/Barbaresco, Napa Cabernet AVAs, Champagne, Rioja Gran Reserva, Mosel GG, Chianti Classico, Rhône crus
- Read-only — no DDL, no DML, no fixes. Findings only.

## Severity scale

- **P0** — broken/incorrect, user-visible or correctness-critical
- **P1** — significant gap or risk, must fix before enrichment
- **P2** — improvement, not blocking
- **P3** — nice to have

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

## Exit criteria

- [ ] `findings_wine_reference.md` written with severity-tagged findings
- [ ] Root cause of S2.3 F2 (Chardonnay/Pinot Blanc) identified or explicitly escalated to S2.5 code scope
- [ ] `sessions.json` S2.4 entry marked `done` with $0 ai_spend
- [ ] `journal.md` S2.4 section completed
- [ ] `budget.json` S2.4 entry at $0
- [ ] `CLAUDE.md` Current State updated (S2.4 done + finding count)
- [ ] `memory/project_sprint2_findings.md` updated with S2.4 cross-references
- [ ] `data/sessions.md` whiteboard entry moved to Done
- [ ] Commit: `S2.4: Wine expert reference content audit — N findings`
