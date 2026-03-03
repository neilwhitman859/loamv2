# Loam — Project Vision & Goals

---

## What Is Loam

Loam is a wine intelligence platform. Every wine record tells the full story of *why* it tastes the way it does — the place, the vintage weather, the soil, the grapes, the producer's choices. It connects terroir, climate data, and winemaking decisions to sensory outcomes in a way that no existing wine database does.

The name is a soil type. Terroir is central to the product's identity.

Loam is not a review aggregator, a marketplace, or a social network. It is structured wine intelligence — data that explains wine, not just describes it.

---

## Who It's For

**Primary audience: mid-level wine enthusiasts.** People who've moved past "I like red wine" and want to understand *why* they prefer certain bottles. They know what Napa Cabernet tastes like but can't explain how a hot vintage or volcanic soil changes the glass. Loam bridges that gap.

**Secondary audience: wine industry professionals.** Sommeliers, buyers, educators, and writers who need structured, trustworthy wine data for their work. Loam's data quality and industry-standard vocabulary make it a credible professional tool, not just a consumer product.

---

## What Makes Loam Different

Most wine databases catalog wines. Loam explains them.

**The core insight:** Wine is the intersection of place, weather, and human decisions. A 2022 Stags Leap Cabernet tastes the way it does because of the district's volcanic soils, that year's heat spikes, and the winemaker's choice to harvest early. Loam connects these layers structurally — not as prose in a review, but as queryable, comparable data.

**What this looks like in practice:**
- Every wine links to its appellation's weather history, soil composition, and elevation
- Vintage conditions are cross-referenced with winemaking decisions (harvest timing, oak treatment, blend changes)
- AI synthesizes these connections into structured insights, but never replaces producer voice — original content is always prioritized
- Sensory profiles, chemical data, and production methods are captured with full source provenance

---

## Revenue Path

Loam grows in three phases:

**Phase 1 — Personal project / build the product.** Get the data model right. Build a compelling frontend experience. Populate the database with enough wines to demonstrate value. Validate that the terroir-to-glass story resonates with real users.

**Phase 2 — Consumer product with affiliate revenue.** Grow the user base around the free wine intelligence experience. Monetize through affiliate links to wine retailers — when a user discovers a wine through Loam, they can buy it. Revenue scales with traffic and conversion.

**Phase 3 — API, data licensing, and dataset sales.** The structured dataset becomes the product. License the API to other wine apps, sell curated datasets to industry buyers (importers, distributors, educators, media). The schema is designed from day one to support this — industry-standard vocabulary, source provenance on every field, clean separation of factual data from AI-generated content.

---

## Foundational Principles

### Don't Create When Content Already Exists

Producer-written content is always better than AI-generated prose. A winemaker's harvest story, terroir narrative, and tasting notes are authentic and authoritative. AI should never replace them.

**The hierarchy:**
1. **Link to original content** — tech sheets, fact sheets, vintage narratives
2. **Extract facts into structured fields** — blend percentages, alcohol, harvest dates
3. **AI fills gaps only** — clearly marked, carries confidence scores

AI is for cross-referencing (weather x soil x blend → insight), not for replacing producer voice.

### Model Industry Norms

Loam's data must be recognizable and trustworthy to wine professionals. This means:
- Industry-standard vocabulary throughout (WSET for sensory, standard appellation names, recognized certifications)
- Alignment with how major wine databases model data (Wine-Searcher, CellarTracker)
- No invented terminology when an industry term exists
- Field names self-explanatory to a wine professional without a data dictionary

### Fact and AI Must Stay Separate

Factual, queryable data lives on core tables. AI-synthesized analysis lives in dedicated insights tables. The line is always clear — if it's in an insights table, it's AI-generated. Source provenance tracks where every fact came from and at what confidence level.

This separation enables clean re-enrichment, honest confidence reporting, and dataset sales where buyers know exactly what they're getting.

---

## Tech Stack

- **Database:** Supabase (Postgres)
- **Enrichment pipeline:** Node.js scripts
- **AI models:** Anthropic Claude (Haiku for extraction and dedup, Sonnet/Opus for synthesis)
- **Weather data:** Open-Meteo ERA5 historical climate API
- **Elevation data:** Elevation API (coordinate-based)
- **Fuzzy matching:** Postgres pg_trgm extension
- **Frontend:** Separate codebase, consumes Supabase directly
- **Version control:** GitHub (github.com/neilwhitman859/loamv2)

---

## Project Documents

- **schema-decisions.md** — Complete log of every schema decision with rationale. The spec for implementation.
- **schema-summary.md** — Consolidated table-by-table field reference. Quick lookup for implementation.
- **This document** — Vision, goals, and principles. The why behind the what.
