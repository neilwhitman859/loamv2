# S2.10 — Dashboard + Reprioritization

**Sprint:** 2 (Audit) — reopened for one strategic session
**Session:** 10 (unplanned addition)
**Purpose:** Establish the working dashboard, then reprioritize Sprint 3 against the strategic decisions made on 2026-04-12

---

## What happened on 2026-04-12

After Sprint 2 closed (275 findings, 9 sessions, $0 spend), a strategic session surfaced 14 open questions. The user answered them, and several answers fundamentally change Sprint 3 priorities. Key decisions logged in `docs/DECISIONS.md` under "2026-04-12":

### The 6 decisions that reshape everything

1. **Loam's core product is STRUCTURED DATA RENDERED CLEARLY, not AI prose.** The user described the product magic as: "Look up any wine → see organized, trustworthy, connected data → the same fields every time → educational and informative." The example that resonated: seeing that a cool 2019 Barolo vintage pushed Nebbiolo toward higher acidity, with the weather data right there on the page. The user explicitly chose structured data over AI-generated narrative. **This means: the enrichment pipeline (Grade C/B AI prose) drops in priority. The join logic, data connections, and frontend rendering of structured facts ARE the product.**

2. **Loam is a venture long-term, personal project short-term.** Core thesis: world-class wine dataset + reference lookup tool. All future paths require this foundation first.

3. **ICP = enthusiast + beverage director.** Dual audience. Not trade-only, not B2B-only.

4. **"Producers are the artists."** Producer metadata needs to be deep — founding story, winemaker philosophy, vineyard holdings, production methods — not just 3 database columns.

5. **Delete xwines_* tables and Riddler scheduled task.** Stop preserving old state that creates confusion.

6. **Sprint 5 enrichment budget rescoped to $60-100** (friends-and-family scope), not $620-700 (full corpus). Open to $300+ once data quality is high-confidence.

### The 3-step plan the user endorsed

1. Firm up workflow and dashboard
2. Fix the bugs discovered in Sprint 2
3. Reprioritize next steps

In that order. Dashboard before fixes (so we can track whether fixes move the needle). Fixes before reprioritization (because the product looks fundamentally different with clean data).

### What the user flagged about Sprint 2's synthesis

- The synthesis.md Sprint 3 backlog was written before "structured data, not AI prose" was decided. Half the tracks (voice module, L3 fact-check gate, enrichment prompt rewrite) are about AI prose quality — which is no longer the core product.
- The 5-sprint sequence (30K → Audit → Execute → Reference Design → Reference Enrichment) is confusing. User wants a clearer, simpler roadmap.
- The metrics and routines need to be simple enough to remember and follow every session.

## S2.10 objectives

### Part 1 — Dashboard

Create `data/dashboard.md`. This is the new single-source-of-truth for sprint progress and metrics. Requirements:
- Readable in Notepad++ (user keeps it open with auto-reload)
- Updated at session start and before every commit (add to CLAUDE.md behavioral rules)
- Contains: current state, 4 core metrics, sprint progress checklist, budget, open questions
- Does NOT replace CLAUDE.md narrative — it supplements it

The 4 core metrics (proposed, validate with live DB queries):
- **Findability** — can you find the wine you type?
- **Page quality** — is the page useful when you land on it?
- **Accuracy** — is the structured data correct?
- **Coverage** — how much of the corpus has depth beyond identity?

### Part 2 — Reprioritize Sprint 3

Re-sort the synthesis.md backlog against "structured data is the product":

**Goes UP in priority:**
- Staging archive relink (Track 2) — unlocks 116K prices + 22K scores. This is the single biggest user-visible improvement available. Structured data, not prose.
- UI rendering of cross-entity connections — wine → appellation → weather → soil → grape displayed as structured facts on the page
- Grape repair (Track 3) — structural data correctness
- Producer metadata (Track 4) — "producers are the artists"
- UI hygiene P0s (Track 0B) — pages need to actually work

**Goes DOWN in priority:**
- Voice module consolidation (Track 1 items 4-6) — about AI prose quality
- L3 fact-check gate (Track 7) — about AI prose quality
- AI safety rail / AIBadge (Track 6) — about AI prose quality
- Signal collection / email signup (Track 5) — premature before fixes land

**Stays the same:**
- Delete describe-chemical (Track 1 item 1) — still a live credit-burn risk regardless
- Doc hygiene (Track 0A) — still needed for session accuracy
- Vendor enrich-wine into git (Track 1 item 2) — good hygiene regardless
- Centralize model IDs (Track 1 item 3) — good hygiene regardless

**Question to resolve:** Does the voice module / enrichment work stay in Sprint 3 at lower priority, or does it get deferred entirely to a later sprint? The user said structured data is primary, but didn't say AI prose is zero.

### Part 3 — Simplify the roadmap

Replace the 5-sprint technical sequence with something the user can hold in their head. Propose a simpler framing. Get user buy-in.

### Part 4 — Session/sprint routines

Write the session open/close and sprint open/close routines into CLAUDE.md. Keep them short — 3-4 steps each, memorable.

## Read first

- `CLAUDE.md` — current state
- `docs/DECISIONS.md` — 2026-04-12 entries (the 14 decisions)
- `data/sprints/audit/findings/synthesis.md` — the Sprint 3 backlog being reprioritized
- `data/sprints/current.json` — sprint state

## Method

- Opus 4.6 inline
- Live DB queries for dashboard baseline metrics
- No code changes, no DB writes — this is a planning/dashboard session
- File writes: `data/dashboard.md`, updates to `CLAUDE.md` behavioral rules, possibly a simplified roadmap file

## Exit criteria

- [ ] `data/dashboard.md` exists with live metrics and sprint progress
- [ ] CLAUDE.md has dashboard update rule in behavioral instructions
- [ ] Sprint 3 backlog is re-sorted against "structured data is the product"
- [ ] Roadmap is simplified and user-approved
- [ ] Session/sprint routines written into CLAUDE.md
- [ ] Commit

## Key constraint

**Do not rush to execution.** The user explicitly asked to slow down and think deeper. S2.10 is a thinking session, not a doing session. Ask clarifying questions. Push back on vague answers. The output should be a clear plan the user actually believes in, not a plan the model generated and the user didn't object to.
