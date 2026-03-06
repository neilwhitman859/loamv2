# Loam — Wine Intelligence Platform

Loam is a wine intelligence platform. You look up a wine and get the full story of *why* it tastes the way it does — the place, the vintage weather, the soil, the grapes, the producer's choices. All the information that's scattered across tech sheets, producer sites, weather databases, and appellation regulations, brought together in one place and connected by AI synthesis.

The name is a soil type. Terroir is central to the product's identity.

---

## The Problem

There's a surprising amount of depth out there about any given wine — blend breakdowns on tech sheets, weather data from climate APIs, soil profiles from appellation authorities, critic scores, production methods. But finding it is a slow, manual process. No single source connects all of it, and none of them explain *why* a wine tastes the way it does.

Loam streamlines access to that data and adds the connective tissue.

---

## How It Works

**On-demand enrichment.** A user searches for a wine. Loam confirms they're looking at the right one, then the pipeline enriches it — pulling structured data from available sources and generating AI synthesis that ties it all together.

**Lazy loading.** Basic identifying info appears immediately. Richer data (weather, soil, AI insights) loads as the pipeline completes. Target: 10–30 seconds for a meaningful result, under a minute for full enrichment.

**Show the source, then add to it.** When a producer publishes a tech sheet, Loam displays that data directly — it's the ground truth. AI doesn't replace it. AI contextualizes it: cross-referencing weather conditions with harvest timing, soil drainage with rainfall, blend changes across vintages. The data sheet is the foundation; the synthesis is the layer on top.

**Drillable.** Every entity connects to every related entity. Click from a wine to its appellation, to other wines from that appellation, to the region, to the soil type. Smooth exploration is a core UX requirement — it's what v1 got wrong.

---

## Who It's For

**Right now:** Me and my friends. We look up wines we're drinking or considering, and Loam shows us everything about them. It's a personal tool and a party trick — "look at all this about the wine you're holding."

**Next:** Wine enthusiasts who've moved past "I like red wine" and want to understand why they prefer certain bottles. People who are curious about terroir but don't want to dig through tech sheets and weather data themselves.

**Eventually:** Wine industry professionals — sommeliers, buyers, educators, writers — who need structured, trustworthy wine data. The schema is designed to be credible at that level even though we're not targeting it yet.

---

## Revenue Path

**Phase 1 — Personal project.** Build it right. Get the schema and pipeline working. Populate with enough wines to demonstrate value. Share with friends. Affiliate links can go in early since they cost nothing and don't change the experience.

**Phase 2 — Grow the user base.** Affiliate revenue from wine retailer links — when someone discovers a wine through Loam, they can buy it. Revenue scales with traffic.

**Phase 3 — Data as product.** API licensing, dataset sales to industry buyers. The structured data, source provenance, and clean fact/AI separation make this possible without a rebuild.

The schema doesn't need to be sale-ready today. It just needs to be flexible enough that we're not throwing it out when things evolve. No painting into corners.

---

## Foundational Principles

### Don't Create When Content Already Exists

Producer-written content is always better than AI-generated prose.

1. **Show original content** — tech sheets, fact sheets, vintage narratives displayed on the site
2. **Extract facts into structured fields** — blend percentages, alcohol, harvest dates
3. **AI fills gaps and connects dots** — clearly marked, carries confidence scores

### Model Industry Norms

Use industry-standard vocabulary throughout. Align with how Wine-Searcher, CellarTracker, and other major databases model data. No invented terminology. Field names self-explanatory to a wine professional.

### Fact and AI Stay Separate

Factual data on core tables. AI synthesis in dedicated insights tables. Source provenance on every field. This enables re-enrichment, honest confidence reporting, and future dataset sales.

---

## Tech Stack

- **Database:** Supabase (Postgres)
- **Enrichment pipeline:** Node.js scripts
- **AI models:** Anthropic Claude (Haiku for extraction/dedup, Sonnet/Opus for synthesis)
- **Weather data:** Open-Meteo ERA5 historical climate API
- **Elevation data:** Elevation API (coordinate-based)
- **Fuzzy matching:** Postgres pg_trgm extension
- **Frontend:** Separate codebase, consumes Supabase directly
- **Version control:** GitHub (github.com/neilwhitman859/loamv2)

---

## Docs

- **docs/schema.md** — Table-by-table field reference (the active schema spec)
- **docs/strategy/** — Dated brainstorm/planning docs from voice chats
- **docs/old/** — Archived docs (schema decision log, region drafts, completed pipeline instructions)
- **session-context.md** — Running session context for Claude Code continuity
