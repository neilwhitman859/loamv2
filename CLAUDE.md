# Loam v2 — Claude Context

Loam is a wine intelligence platform. Users look up a wine and get the full story — place, vintage weather, soil, grapes, producer choices. All the scattered information brought together and connected by AI synthesis. The name is a soil type. Terroir is central.

**Supabase project:** `vgbppjhmvbggfjztzobl` (us-east-1)
**GitHub:** github.com/neilwhitman859/loamv2
**Stack:** Supabase (Postgres), Node.js scripts, Anthropic Claude, Open-Meteo, Vite/React frontend

---

## Docs — When to Consult Each

- `docs/SCHEMA.md` — Table-by-table field reference. Read when working with DB structure or writing queries.
- `docs/PRINCIPLES.md` — Product philosophy. Read when making judgment calls about what to build or how.
- `docs/DECISIONS.md` — Append-only log of human decisions with reasoning. Read when you need to understand why something was done a certain way. Never re-litigate settled decisions without the user raising it.
- `docs/VOICE.md` — Voice, tone, and food pairing guidance for all AI-generated content. Read before writing any enrichment prompts or insight content.
- `docs/WORKFLOW.md` — Human-facing session checklist. You don't need to read this, but follow the behavioral instructions below.

---

## Behavioral Instructions

### Session Briefings
When starting a session or recovering from compaction, give a medium briefing:
```
SESSION BRIEFING
- Last session: [what was accomplished]
- Current DB state: [query the DB for row counts — never rely on hardcoded numbers]
- Open items: [anything left mid-stream]
- Suggested next step: [what makes sense to pick up]
```
Query the database for current state. Do not guess or use stale numbers from this file.

### Auto-Update CLAUDE.md
Update this file at natural breakpoints — after a pipeline run, a schema change, a significant decision, or when wrapping up a session. Tell the user what changed: "Updated CLAUDE.md with [summary]."

### Auto-Log Decisions
When the user makes a judgment call (choosing between options, setting a direction, defining how something should work), append it to `docs/DECISIONS.md` automatically. Notify briefly: "Logged to DECISIONS.md: [one-line summary]."

If the user says **"log that"**, force an entry even if you didn't think it was significant.

### Auto-Update SCHEMA.md
When you modify the database schema (CREATE TABLE, ALTER TABLE, DROP, etc.), update `docs/SCHEMA.md` to reflect the change, including the reasoning.

### Commit at Milestones
When something is important enough to update CLAUDE.md, it's important enough to commit. Commit with a clear message after meaningful milestones.

### Nudge the User
If the user is going a long stretch without wrapping up, if decisions are being made but not logged, or if a session is ending without updating files — say something. Be direct: "We've made some decisions this session that aren't logged yet. Want me to update DECISIONS.md and CLAUDE.md before we stop?"

---

## Current State

### Architecture
The database has two layers:
- **Canonical tables** (`producers`, `wines`, `wine_vintages`, etc.) — curated, high-quality data. Currently small (3 producers, 267 wines) from trial scraping. Quality bar is high.
- **xwines_* tables** — bulk X-Wines dataset dump (~530K wines, ~2.2M vintages, ~32K producers). Kept as reference but not actively maintained. Data quality is lower.

### Reference Tables (complete)
Countries (62), regions (373 — 62 catch-all, 215 L1 named, 96 L2), appellations (3,206), grapes (709), varietal categories (154), source types (27), publications (68), farming certifications (18), biodiversity certifications (7), soil types (39).

Regions rebuilt from scratch (2026-03-12): two-level hierarchy sourced from WSET L3 spec + Federdoc/MAPA/official wine authorities. All X-Wines leftover regions purged. Data file: `data/regions_rebuild.json`.

Appellation→region attribution 96.4% complete (3,089/3,206). Three-pass strategy: Pass 1 containment trace (1,915), Pass 3 direct lookup (1,174). 117 remain on catch-all by design. L2 attribution fixed (2026-03-12): 33 empty L2 regions resolved — appellations moved to lowest-level region (e.g., Napa Valley AVAs → Napa County L2, not California L1). 0 empty L2 regions remain.

### Insights (partially populated)
Grape insights (707), region insights (202 — 126 deleted with leftover regions), appellation insights (82), country insights (62). Producer insights and wine insights are empty.

### Geographic Data
Geographic boundaries with PostGIS geometry. Appellation containment hierarchy (2,158 relationships).

**Region boundaries (2026-03-12):** 310/311 named regions have geographic data (99.7%). Full rebuild from scratch:
- **Official:** 30 regions (copied from wine authority appellation boundaries — UC Davis, Wine Australia, IPONZ, Eurac)
- **Derived:** 181 regions (ST_Union of child appellation polygons — most accurate for wine platform)
- **Approximate:** 81 regions (Nominatim admin boundaries + EU PDO copied from appellations)
- **Geocoded:** 18 regions (centroid-only — mostly SA wine wards with no polygon source)
- **No data:** 1 (South Eastern Australia — cross-state super-zone, skipped by design)

**Wine expert Sonnet review completed:** All 311 regions reviewed by country. Report at `data/region_review_report.json`. Key findings: 115 potential corrections identified (38 auto-fixable, 77 need review). Main structural issues flagged in South Africa (missing regions), Canada (needs L2 subregions), Portugal (missing regions), Croatia/Hungary (needs restructuring). Germany, Slovenia, Czech Republic passed with no issues.

Scripts: `scripts/geocode_regions.mjs` (Nominatim geocoding), `scripts/fix_region_geocodes.mjs` (Swiss/AR fixes), `scripts/review_region_boundaries.mjs` (Sonnet review).
Data files: `data/region_nominatim_queries.json` (Nominatim query overrides), `data/region_review_report.json` (full Sonnet review report).

### What's Not There Yet
- Most insight tables empty (wine, producer, soil, water body)
- All weather data (appellation_vintages)
- All document tables
- All soil/water body link tables
- Scores and pricing in canonical tables
- Enrichment log

---

## Current Focus

Region boundaries complete (310/311 regions, 99.7%). Sonnet review identified 115 items for follow-up — next step is triaging the review report and applying the safe corrections.

### Open Questions
- Producer import strategy: how to systematically populate canonical producer/wine/vintage tables
- Data licensing for scores (Wine Spectator, Parker, CellarTracker)
- Real-time vs batch enrichment for the frontend
- Frontend timeline: soft goal of something live for friends, but data quality comes first

### Enrichment Priority (when ready)
1. AI insights for existing reference entities (appellations, grapes, regions, countries)
2. Geographic enrichment (appellation lat/lng, water bodies, soils)
3. Weather data (Open-Meteo, needs lat/lng first)
4. Scores and pricing (needs data source strategy)

---

## Key Phrases

- **"wrap up"** — End-of-session routine: update CLAUDE.md, update DECISIONS.md if needed, commit and push.
- **"log that"** — Force a DECISIONS.md entry.
- **"briefing"** — Give current state summary anytime mid-session.
