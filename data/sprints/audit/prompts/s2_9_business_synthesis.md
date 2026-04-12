# S2.9 — Business Audit + Sprint 2 Synthesis + Sprint 3 Backlog

**Sprint:** 2 (Audit)
**Session:** 9 of 9 (S2.9) — capstone
**Expert hat:** Business — competitive positioning, monetization, value prop, ICP, distribution
**Budget:** $0 expected (Opus inline per ratified S2.3–S2.8 pattern)
**Primary deliverables:**
1. `data/sprints/audit/findings/findings_business.md` — new business-layer findings
2. `data/sprints/audit/findings/synthesis.md` — prioritized Sprint 3 backlog (primary Sprint 2 deliverable)

---

## Context

S2.1–S2.8 audited Loam as a technical object: DB canonical, DB staging, wine canonical content (sommelier bar), wine reference content, pipeline code, AI voice, UX / frontend, and meta layer (docs + memory + roadmap + sprint infra). 245 findings total. Every session wore a "how does this hold up technically" hat.

S2.9 wears a different hat: **how does this hold up as a business?** Can Loam acquire users? Can it retain them? Can it monetize? What does it do that no competitor does? Who pays for it, and why? What's the wedge? What's the unit economics story? Who's the ICP, and is the current architecture actually aligned with their needs?

This is the first session that reads Loam through a go-to-market lens rather than a correctness lens. It's deliberately last — the technical audit had to establish the product state before the business audit could evaluate it honestly.

S2.9 also does the capstone work for Sprint 2: deduping findings across all 8 prior expert sessions (many findings are manifestations of the same underlying issue seen from different angles), prioritizing against business outcomes rather than raw severity count, and producing the Sprint 3 backlog that Sprint 3 will actually execute from.

## Read first

- `CLAUDE.md` — always
- `data/sprints/audit/status.md` — sprint plan
- `data/sprints/audit/findings/findings_db_canonical.md` — S2.1 (the DB-layer foundation)
- `data/sprints/audit/findings/findings_db_staging.md` — S2.2 (staging depth unlock)
- `data/sprints/audit/findings/findings_wine_canonical.md` — S2.3 (wine-layer sommelier audit)
- `data/sprints/audit/findings/findings_wine_reference.md` — S2.4 (reference-content correctness)
- `data/sprints/audit/findings/findings_code.md` — S2.5 (pipeline code + edge functions)
- `data/sprints/audit/findings/findings_voice.md` — S2.6 (voice + enrichment corpus)
- `data/sprints/audit/findings/findings_ux.md` — S2.7 (frontend + UI)
- `data/sprints/audit/findings/findings_meta.md` — S2.8 (docs + memory + sprint infra)
- `memory/product-architecture.md` — PWA / enrichment tiers / ICP sketches
- `memory/project_quality_before_enrichment.md` — authoritative sprint sequence
- `docs/PRINCIPLES.md` — product philosophy
- `docs/ENRICHMENT.md` — letter-grade enrichment architecture + cost model
- `docs/VOICE.md` — voice / pairing guidance
- `docs/DECISIONS.md` — 2026-04-11 sprint model pivot

## Objectives

### Part 1 — Business audit

1. **Positioning audit.** Build a competitive parity table across Loam / Vivino / Wine-Searcher / CellarTracker / Vinous / Jancis / Decanter / GuildSomm / Wine.com / Perplexity+any-LLM. Which columns does Loam uniquely fill? Which are table-stakes Loam lacks?

2. **ICP audit.** Who is Loam for? Enthusiast, pro sommelier, wine buyer, WSET student, restaurant trade, retail trade, B2B data license? The audit findings imply one thing; the architecture implies another. Reconcile.

3. **Monetization audit.** What's the revenue model? Freemium / subscription / affiliate / API / licensing / ads? Is there a path to revenue, or is Loam pure R&D? What's the unit economics story — marginal cost per enrichment, unit revenue per user?

4. **Distribution audit.** How do users find Loam? SEO, content marketing, partnerships, direct, API embed? Does the audit surface reveal any of these are ready?

5. **Cost-of-goods audit.** Pull live `enrichment_log` totals. Spend to date, per-wine unit cost, projected cost for 100% Grade C + 100% Grade B coverage. Is this a cost problem or a value problem?

6. **Moat audit.** What can Loam do that a well-prompted Claude / Perplexity / Vivino cannot? Is the moat:
   - Structured DB with provenance?
   - AI synthesis quality?
   - Terroir/soil/weather depth?
   - Voice?
   - Backbone IDs (COLA/LWIN/UPC)?
   - Answer honestly. If the moat is thin, that's the finding.

7. **"Why now" thesis.** What makes this buildable in 2026 that wasn't buildable in 2022? Is the thesis still tight after the technical audit?

8. **Legal/risk audit.** Scraping ToS compliance, ADA/a11y (S2.7 F20), data licensing for retailer prices (S2.2 F1 unlocks 82K prices but provenance matters), brand/trademark collisions, enrichment-at-scale ToS ("wine intelligence" is a registered trademark of Wine Intelligence Ltd. since 2002).

9. **User feedback loop audit.** Query `wine_lookups`. Are there logged user sessions? Does the Riddler agent still emit accuracy_audit entries? Is there any signal about what users want vs what Loam is building?

10. **Brand/voice audit.** Is the brand voice (marketing site, human copy, tagline) separate from the enrichment voice? S2.6 F1/F2/F3 showed the enrichment voice is brittle. Does that contaminate the brand voice?

### Part 2 — Cross-session dedupe

1. **Identify compound bugs** — issues that show up in 3+ findings across different experts at different layers. The Chardonnay/Pinot Blanc thread runs through S2.3 F2 → S2.4 F2 → S2.5 F2/F11/F17 → S2.6 F4 → S2.7 F3. These should collapse to 1 compound repair in the backlog.

2. **Identify fix dependencies** — which Sprint 3 items block other Sprint 3 items? Staging relink (S2.2 F1 / S2.5 F3) blocks price/score/vintage unlock. Voice module consolidation (S2.6 F1/F2) must precede any regeneration. Grape repair must precede wine regeneration. Graph these.

3. **Identify "same problem, different layer" findings** that duplicate findings in the raw count but are really one item: doc drift (S2.1 F28 + S2.8 F1/F3/F4/F23), dead column pattern (S2.7 F2 + S2.8 F3), volcanic soil confabulation (S2.3 F14 + S2.4 F18 + S2.6 F5 + S2.7 F6), corpus-wide producer metadata (S2.3 F3 + S2.7 F4), food pairings structured table empty (S2.6 F8 + S2.7 F22), edge function hygiene (S2.5 F1/F4/F31 + S2.6 F2 + S2.8 re-verification).

4. **Compute the net Sprint 3 item count after dedupe.** Raw P0+P1 across 9 sessions ≈ 170+; net should be smaller.

### Part 3 — Sprint 3 backlog synthesis

1. **Prioritize by "unblocks what."**
   - **Tier 1: Enables Sprint 5 regeneration** (contamination blockers — skipping these re-contaminates Sprint 5 output)
   - **Tier 2: Unlocks massive existing data** (staging relink unlocks ~50K prices, ~48K scores)
   - **Tier 3: Credibility-at-first-impression** (UI fixes on pages that break on the marquee wine lookup a sommelier would try)
   - **Tier 4: Hygiene / nice-to-have / deferred to Sprint 4**

2. **Propose Sprint 3 scope: what fits in ~8-12 sessions.** Be honest about what Sprint 3 cannot finish. Punt Sprint 4 work to Sprint 4 explicitly — "reference-layer redesign" is not Sprint 3.

3. **Define Sprint 3 "done."** What exit criteria close Sprint 3? E.g., "100% of the 286K dangling wine_id pointers relinked, Chardonnay/Pinot Blanc bug gone on verification sample, describe-chemical deleted, enrich-wine vendored into git, voice module consolidated, AI disclaimer shipped." Measurable, checkable.

4. **Identify what Sprint 3 deliberately defers** — Sprint 4 / Sprint 5 / post-launch. Be explicit about what's NOT in Sprint 3 so scope-creep gets caught early.

5. **Sequence the Sprint 3 sessions.** Recommend an order (hygiene → relink → grape repair → voice → content regen gate → L3 fact-check gate → producer metadata strategy → exit review).

6. **Business-informed reprioritization.** Where business findings suggest different priority than technical severity — e.g., a P0 empty h1 on a page with 0 views/day is less urgent than a P1 credibility issue on a page the first sommelier demo will hit — reprioritize accordingly in the synthesis doc.

## Method

- Opus 4.6 inline — no Haiku/Sonnet API calls
- `Read` for all prior finding files and doc context
- `Grep` for cross-layer pattern matches (e.g., how many findings mention "volcanic")
- `Bash` + `git log` for velocity/history signals
- `mcp__execute_sql` for the live DB business-signal verification (wine_lookups, enrichment_log totals, deployment state via list_edge_functions)
- `mcp__list_edge_functions` + `mcp__list_scheduled_tasks` for re-verification of S2.5 F1 / S2.8 edge function state at S2.9
- No WebFetch unless specifically needed (competitor landing pages are OK to reference from training knowledge)
- No DB writes, no server runs, no PR-prep

## Severity scale (business findings)

- **P0** — existential to the business (no monetization model, no differentiation vs free LLMs, credibility-at-first-impression fails)
- **P1** — significant strategic risk, must think before Sprint 3 planning (ICP undefined, distribution channel broken, no feedback loop)
- **P2** — valuable to think about now, resolvable later (localization, pricing accuracy, trademark)
- **P3** — nice to have, not Sprint-blocking (press kit, tagline polish)

## Scope boundaries

- **In scope:** business model, positioning, competitive landscape, ICP, distribution, monetization, unit economics, moat, legal/licensing/risk, brand voice vs AI voice, Sprint 3 backlog synthesis, cross-session dedupe
- **Out of scope:** technical findings that belong in prior sessions (the audit is done — don't re-open the technical layers)
- **Partial scope:** Sprint 4 / Sprint 5 scoping — flag the work, don't plan the execution

## Exit criteria

- [ ] `findings_business.md` written with severity-tagged findings
- [ ] Every business finding has at least one concrete piece of evidence (file, DB query, or cross-reference to S2.1-S2.8 findings)
- [ ] `synthesis.md` written with deduped Sprint 3 backlog
- [ ] Sprint 3 backlog sequenced with explicit dependencies and tier assignments
- [ ] Sprint 3 "done" criteria measurable
- [ ] Sprint 4 / Sprint 5 deferrals explicitly listed
- [ ] `sessions.json` S2.9 entry marked `done` with $0 ai_spend
- [ ] `journal.md` S2.9 section completed + Sprint 2 closing summary
- [ ] `budget.json` closed (Sprint 2 total)
- [ ] `current.json` Sprint 2 closed, Sprint 3 ready to start
- [ ] `CLAUDE.md` Current State updated (Sprint 2 closed, Sprint 3 pointer)
- [ ] `memory/project_sprint2_findings.md` updated with S2.9 cross-references + pointer to synthesis.md
- [ ] `data/sessions.md` whiteboard entry added under Done
- [ ] This prompt at `data/sprints/audit/prompts/s2_9_business_synthesis.md`
- [ ] Commit: `S2.9: Business audit + Sprint 3 backlog synthesis — Sprint 2 closed`

## Starting moves

1. Read CLAUDE.md, status.md, all S2.1-S2.8 finding files + memory/project_sprint2_findings.md
2. Query live DB for business signals: wine_lookups, enrichment_log, deployment state
3. TodoWrite the audit + synthesis scope
4. Build findings_business.md with severity-tagged business findings (expect ~25-35 findings)
5. Build dedupe table — compound bugs + "same problem, different layer" groups
6. Build synthesis.md with tiered Sprint 3 backlog + sequence + exit criteria
7. Wrap up per exit criteria checklist
8. Commit

## Notes for the agent running S2.9

- **Be honest about the wedge.** If the audit + live DB query shows there's no user-visible value yet and no monetization plan, say so. A wine-intelligence platform with 0 wine_lookups and $16 spent on enrichment that has 4% Chardonnay/Pinot Blanc factual errors is a pre-product project. That's fine — but Sprint 3 should be prioritized accordingly, not as if it were a production fix list.
- **Sprint 3 should be smaller than the raw P0+P1 count suggests.** Many findings dedupe into single compound fixes. The synthesis value is in the dedupe, not in copying all 170+ items into a Sprint 3 manifest.
- **Every tier-1 item in the backlog is a Sprint 5 unblocker.** If Sprint 3 doesn't need to be done for Sprint 5 to ship, it's tier-2 or tier-3 or deferred.
- **Sprint 3 has a budget.** Sprint 2+3 share a $50 ceiling. Sprint 2 closes at $0. Sprint 3 should assume ~$30-50 of headroom for voice module testing, L3 fact-check gate scaffolding, and spot-check verification on repairs.
- **Sprint 5 is the payoff sprint.** Sprint 3 is prep for Sprint 5. Sprint 4 is prep for Sprint 5. If a Sprint 3 item is scoring itself by its own aesthetics rather than by "what Sprint 5 will be able to do that it couldn't before," reconsider.
- **Business findings should influence technical priorities.** If the first sommelier demo will hit `/wine/DRC-Romanée-Conti` and the demo fails (S2.3 F1), that P0 outranks a P0 on a country page nobody clicks.
- **Cron loops, parallel agents, scripted budget** — NONE. Sequential Opus inline reasoning is the ratified pattern. Any deviation needs pre-justification in journal.md.

Produce a clear, honest, business-grounded findings file and a prioritized Sprint 3 backlog that Sprint 3 can actually execute from.
