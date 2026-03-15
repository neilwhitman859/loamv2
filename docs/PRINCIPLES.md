# Loam — Core Principles

These principles guide every decision — what we build, how we write, what we prioritize, and how we talk about wine. They're the filter for prompting, strategy, execution, and any content Claude generates. Read these at the start of every session.

---

### 1. Source first, AI second

When a producer has already written about their wine, that's the truth. Show it. AI contextualizes and connects dots — it never replaces or rewrites what already exists. A producer's tech sheet, a winemaker's vintage narrative, an appellation authority's regulatory doc — these are primary sources. AI's job is to cross-reference (weather x soil x blend), fill gaps where no source exists, and explain *why* things matter. It never speaks over the people who made the wine.

### 2. Spend a little to get it right

Use AI for classification, dedup, quality checks — anywhere that human judgment at scale would be slow and error-prone. A $0.14 Haiku run beats hours of manual cleanup. Don't be cheap with AI when data quality is on the line. Getting it right the first time costs a fraction of going back through later.

### 3. Zero tolerance for dirty data

Every foreign key verified. Zero orphans. Zero mismatches. If something looks off, stop and fix it before moving on. Bad data compounds — one wrong appellation link cascades into wrong weather data, wrong soil associations, wrong AI insights. The database is only as trustworthy as its weakest join.

### 4. Everything connects to everything

The schema is a knowledge graph, not a catalog. Wine → appellation → region → soil → weather → vintage. Every entity drillable to every related entity. Smooth exploration is the core UX — the thing that makes Loam more than a search engine. If a user can't click from a wine to its appellation to the region's climate profile to other wines from the same soil type, we haven't built it right.

### 5. Fact and AI stay separate

Factual data lives on core tables. AI synthesis lives in dedicated insights tables. Source provenance tracks every field. This separation is what makes the data trustworthy, re-enrichable, and eventually sellable. A user should always be able to tell: is this a fact from a producer tech sheet, or is this AI connecting dots? Both have value. Conflating them destroys trust.

### 6. Model the industry, don't invent

Use real wine vocabulary — AOC, not "designation level 2." WSET scales for sensory data. Standard appellation hierarchies. Recognized certification names. Follow how Wine-Searcher, CellarTracker, and major databases model their data. No invented terminology when an industry term exists. Field names should be self-explanatory to a sommelier without a data dictionary. The schema is designed to be credible at a professional level today, even if the current audience is friends at a dinner table. No painting into corners.

### 7. Passionate, grounded, real

Great winemakers talk about their work with love and deep knowledge. They know their soil composition, their harvest dates, their vine ages — and they share it because they genuinely care about this stuff. Wine is an art, and the people who make it well treat it that way. Loam should have that same energy. Be specific, let the real love for wine and place come through, and never let it slide into marketing fluff.

This especially applies to all AI-generated content — wine summaries, appellation overviews, producer profiles, tasting contexts. When Claude writes for Loam, it should sound like someone who loves wine talking to someone who wants to understand it.

*See [VOICE.md](VOICE.md) for the full voice and tone guide.*

### 8. Monitor cheap, not loud

When a pipeline is running in the background, don't poll with tools that dump the entire output buffer into the conversation. Every token of log output that enters the context gets re-read on every subsequent turn — and it adds up fast. The appellation insights run cost ~$7.63 in Sonnet API calls but nearly $1 in Claude Code monitoring just from re-reading progress logs.

**The rule:** Use `tail -n 5` (or similar) on the output file to check progress. You get the last few lines — current position, success/fail count, time estimate — which is all that's needed. Save full output reads for when the task is done and you need the summary. The goal is reassurance that it's still working and a rough sense of how far along, not a replay of every line.

### 9. Don't create new data from training data

Training data (Claude's built-in knowledge) should only be used for **validation** — confirming, cross-referencing, and auditing data that came from authoritative sources. It should never be used to **generate** new factual content: scores, tasting notes, production figures, vintage details, or any data that would end up in canonical tables. If it didn't come from a primary source (producer website, government registry, official publication), it doesn't go in the database. Training data is the second opinion, not the source of truth.
