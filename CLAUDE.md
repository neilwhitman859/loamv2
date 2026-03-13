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

Regions rebuilt from scratch (2026-03-12): two-level hierarchy sourced from WSET L3 spec + Federdoc/MAPA/official wine authorities. 81 new L1 regions added (2026-03-12) to cover appellations in all countries: 25 US states, 10 Swiss cantons, 6 Spanish autonomous communities, 6 Argentine provinces, 6 Slovak wine regions, 5 Austrian provinces, 5 Hungarian districts, 3 Moldovan IGPs, 2 Portuguese macro-regions, 2 Georgian regions, 2 Greek regions, 2 Romanian regions, 2 Brazilian regions, 2 Japanese prefectures, 1 each for Chile, Australia, Canada.

Appellation→region attribution 96.4% complete (3,089/3,206). Three-pass strategy: Pass 1 containment trace (1,915), Pass 3 direct lookup (1,174). 117 remain on catch-all — South Africa outside WC (20), Hungary minor districts (15), Morocco (15), US multi-state AVAs (14), Swiss minor cantons (11), plus small countries without regions (Cyprus, Belgium, Netherlands, Serbia, etc.).

### Insights (partially populated)
Grape insights (707), region insights (202 — 126 deleted with leftover regions), appellation insights (82), country insights (62). Producer insights and wine insights are empty.

### Geographic Data
Geographic boundaries (2,937 — 47 deleted with leftover regions) with PostGIS geometry. Appellation containment hierarchy (2,158 relationships).

### What's Not There Yet
- Most insight tables empty (wine, producer, soil, water body)
- All weather data (appellation_vintages)
- All document tables
- All soil/water body link tables
- Scores and pricing in canonical tables
- Enrichment log

---

## Current Focus

Region rebuild complete. Next: appellation→region mapping (reassign 2,828 appellations from catch-all to proper named regions).

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
