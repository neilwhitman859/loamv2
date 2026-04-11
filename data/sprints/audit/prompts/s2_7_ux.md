# S2.7 — UX / Frontend Audit

**Sprint:** 2 (Audit)
**Session:** 7 of ~9 (S2.7)
**Expert hat:** UX / frontend — page types, data-to-UI integrity, empty state handling, routing, accessibility, mobile-first behavior
**Budget:** $0 expected (Opus inline per ratified S2.3-S2.6 pattern)
**Primary deliverable:** `data/sprints/audit/findings/findings_ux.md`

---

## Context

S2.1-S2.6 audited the data, code, and content layers. S2.7 is the first audit session that sees the product as a user sees it — the frontend React app at `frontend/`, deployed at loam.onrender.com (currently paused per `memory/feedback_frontend_pause.md`).

Why this matters: the quality-before-enrichment thesis ultimately has to cash out in what renders on screen. Every P0 in S2.1-S2.6 (broken wines, wrong grapes, empty reference tables, contaminated insights, absent food pairings) produces a specific UI symptom. S2.7 documents the symptoms AND catches UI-level issues that the data audits can't see: empty states, missing loading indicators, accessibility gaps, broken routes, unhandled errors, mobile-first drift, principle-violating layouts.

Several prior findings point at the UI layer:

- **S2.1 F28** — hardcoded counts drift from live DB state. UI pages showing counts have the same risk.
- **S2.3 F1** — 8/10 marquee wines broken. User experience is "search Vega Sicilia, get nothing / get wrong wine / get empty shell."
- **S2.3 F2 / S2.4 F2 / S2.5 F2** — 2,743 Chardonnay wines show Pinot Blanc as a grape. Every one of those wine pages renders the wrong grape.
- **S2.4 F1** — varietal_categories has wrong grape links. Grape browse pages show wrong varietals.
- **S2.5 F18** — 50,908 LWIN long-tail wines lack `display_name`. UI either renders the cuvee-only name, falls back to NULL, or crashes.
- **S2.6 F4** — 487 wines have confabulated Chardonnay+Pinot Blanc editorial. Rendered in wine_insights on the wine page.
- **S2.6 F8** — `wine_food_pairings` is EMPTY. Every food-pairing section on every wine page is empty.
- **S2.6 F9** — zero European appellation_insights. Chambertin/Barolo/Champagne pages show empty reference content.

Additionally, the frontend is paused — but pausing is not the same as decommissioning. The product vision remains: users search for a wine and get the full story. S2.7 checks whether the frontend, as built, supports that vision or whether Sprint 4 (Reference Redesign) should include a UI redesign.

## Read first

- `CLAUDE.md` — always
- `docs/PRINCIPLES.md` — especially Principle #9 (structured data → structured display, never buried in prose)
- `docs/VOICE.md` — voice on screen
- `data/sprints/audit/findings/findings_db_canonical.md` — S2.1 (empty tables, search_vector, producer metadata zero)
- `data/sprints/audit/findings/findings_wine_canonical.md` — S2.3 (marquee wine breakage, missing display_name, color wrongness)
- `data/sprints/audit/findings/findings_wine_reference.md` — S2.4 (reference content issues)
- `data/sprints/audit/findings/findings_code.md` — S2.5 (enrich-wine edge function, grapes.name bug)
- `data/sprints/audit/findings/findings_voice.md` — S2.6 (what the UI shows for enriched content)
- `data/sprints/audit/status.md` — sprint plan
- `memory/feedback_frontend_pause.md` — pause rationale
- `memory/feedback_opus_inline_reasoning.md` — ratified pattern

## Objectives

1. **Inventory the frontend.** All pages under `frontend/src/pages/`, all reusable components under `frontend/src/components/`, routing in `frontend/src/App.*`, data access in `frontend/src/lib/` or equivalent.
2. **Audit each page type against its data.** Wine, Producer, Appellation, Region, Grape, Country, Vineyard, Home, Search. For each: what does the page try to display? What happens when the underlying columns are NULL? What happens when the joined tables are empty? Does it match Principle #9 (structured fact grids, never buried in prose)?
3. **Empty state handling.** Does every page type gracefully handle the F-grade identity-only wine? The producer with zero vintages? The appellation with zero producers? The grape with zero wines?
4. **Principle #9 compliance.** Review every page template for "structured DB fields → labeled fact grid" adherence. Flag any prose-buried numbers, dates, percentages, or enums.
5. **Routing and error boundaries.** Not-found pages, invalid UUIDs, failed fetches, empty search results. Does the UI handle them all, or does it crash/blank-screen?
6. **Data integrity in the UI.** Cross-check UI data access against known data issues: does it read `grapes.name` or `grapes.display_name`? Does it filter active wines? Does it handle NULL color? Does it show the Chardonnay+Pinot Blanc bug?
7. **Accessibility + mobile-first.** Spot-check heading structure, alt text, color contrast (design tokens), touch targets, responsive behavior.
8. **Stale code / dead code in frontend.** Dev tools under `/data/*` and `/dev/*`, any unused components, stale API calls, anything mentioned in CLAUDE.md that no longer matches the live codebase.
9. **Cross-reference S2.1-S2.6.** For each prior finding, is there a UI symptom? Document it as a UX finding that compounds (not replaces) the data finding.
10. **Write `findings_ux.md`** with severity-tagged findings, concrete receipts (file:line, data examples), proposed fixes.

## Method

- Opus 4.6 inline — no Haiku/Sonnet API calls
- `Read` / `Glob` / `Grep` across `frontend/src/`
- Supabase MCP `execute_sql` for corpus checks (e.g., "what fraction of wines would show NULL color on the wine page")
- No screenshots, no dev server runs — read-only static analysis is sufficient for an audit
- For each violation, quote the actual code with file:line and the actual data state with a SELECT
- Cross-reference S2.1-S2.6 findings — don't re-log, extend with UI evidence

## Severity scale

- **P0** — broken or user-visible correctness issue at the UI layer
- **P1** — significant risk, must fix before frontend resumes
- **P2** — improvement, not blocking
- **P3** — nice to have

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

## Scope boundaries

- **In scope:** frontend React components, pages, routing, data access patterns, empty states, accessibility, mobile-first, UX symptoms of prior findings
- **Out of scope:** backend code (S2.5 already covered), docs/memory drift (S2.8), business positioning (S2.9), detailed design system redesign (belongs in Sprint 4)
- **Partial scope:** performance (spot-check only), specific design critique (focus on data integrity over aesthetics)

## Exit criteria

- [ ] `findings_ux.md` written with severity-tagged findings
- [ ] Every finding has at least one concrete file:line or SQL-evidenced quote
- [ ] Every page type inventoried
- [ ] Empty state handling audited for every page type
- [ ] Principle #9 compliance checked page-by-page
- [ ] Cross-references to S2.1-S2.6 findings explicit
- [ ] `sessions.json` S2.7 entry marked `done` with $0 ai_spend
- [ ] `journal.md` S2.7 section completed
- [ ] `budget.json` S2.7 entry at $0
- [ ] `CLAUDE.md` Current State updated (S2.7 done + finding count)
- [ ] `memory/project_sprint2_findings.md` updated with S2.7 cross-references
- [ ] `data/sessions.md` whiteboard entry moved to Done
- [ ] Commit: `S2.7: UX / frontend audit — N findings`
