# Business Expert Audit — Findings

**Session:** S2.9
**Date:** 2026-04-11
**Expert:** business (PM / founder hat — positioning, monetization, ICP, distribution, moat, unit economics, risk)
**Scope:** Loam as a go-to-market object, not as a technical artifact. Competitive parity vs Vivino / Wine-Searcher / CellarTracker / Vinous / Jancis / Decanter / GuildSomm / Wine.com / Perplexity+LLMs. ICP reconciliation with current architecture. Revenue model. Unit economics. Legal/trademark/ToS risk. Brand voice vs AI voice. User feedback loop via live DB telemetry.
**Method:** Opus 4.6 inline + 8 live DB queries via Supabase MCP (wine_lookups, enrichment_log totals, price/score actual coverage, archive vs public depth, list_edge_functions re-verification). Competitive parity drawn from training knowledge (explicit where specific). Read-only — no DB writes.
**Budget:** **$0.00 actual** vs $0.00 planned (S2.3 pre-auth of $18 not used). Sprint 2 closes at $0.00 / $25.00.

---

## Summary

**Total findings:** 30
- **P0** (existential, must-think before Sprint 3 scope locks): **8**
- **P1** (significant strategic risk, Sprint 3 should reflect): **14**
- **P2** (valuable to surface, not immediately blocking): **6**
- **P3** (nice to have): **2**

**Headline (P0) business risks:**

1. **Loam has no monetization model.** No subscription tier, no affiliate links, no API licensing path, no ads. Zero dollars of revenue anywhere in the architecture. Sprint 2 → Sprint 3 → Sprint 4 → Sprint 5 is ~20+ sessions of work on a cost center. The enrichment budget is real ($16.19 to date, ~$2,000 projected for full Grade B coverage) and the ceiling is undefined.
2. **Loam has had zero user lookups.** Live query: `public.wine_lookups` = 0 rows. The instrumentation table exists but has never captured a single wine lookup. Either the frontend was never wired to write to it, or nobody has ever looked up a wine on Loam. Either way, **there is no user-demand signal to prioritize enrichment against**. The ENRICHMENT.md "on-demand enrichment when users search" architecture is purely theoretical — it's never fired.
3. **Price coverage is 1.81%, not 5.21%.** Live query: only 2,815 of 155,623 active wines have ANY price. The "5.21% / 25,898 distinct wines" number in CLAUDE.md (2026-04-04 depth promotion session) is stale — the 30K rebuild wiped the joins. The 116,717 archive prices + 21,905 archive scores waiting in `archive.wine_vintage_prices` / `archive.wine_vintage_scores` are the real depth — and they're locked behind S2.2 F1 / S2.5 F3. **The single biggest business unlock Loam can ship is the staging+archive relink**, not more enrichment.
4. **Loam's wedge vs Claude/Perplexity/LLMs is inverted right now.** The pitch is "structured + audited + cross-referenced terroir depth." S2.3/S2.4/S2.6 found that Loam's enriched content has factual errors (Chardonnay/Pinot Blanc on 97.6% of Chardonnay-named wines, DRC Corton-Charlemagne falsely claiming "avoids MLF", Joseph Phelps Eisele purchased in 2013 confabulation, volcanic soil confabulation on 49 appellations). A direct Claude/Perplexity query on any marquee wine today returns a more factual answer than Loam's enriched page. **The moat is negative until Sprint 3 repair + Sprint 5 regeneration land.**
5. **ICP is undefined.** CLAUDE.md says "users look up a wine and get the full story." Which users? Enthusiasts (don't pay for terroir — they pay for Vivino price comps). WSET students (pay for GuildSomm, not databases). Trade sommeliers (pay for Vinous / Wine Advocate / Decanter). Retail wine buyers (pay for Wine-Searcher). Restaurant beverage directors (pay for SevenFifty). B2B data consumers (pay for Liv-ex / LWIN directly). The architecture serves "any of the above, partially" which in practice serves none completely.
6. **The "terroir" positioning is 10x more polished than the data supports.** Vineyards: 0 rows in `public.vineyards` (S2.8 F4). Appellation soils: 930 rows but ~60% contain "volcanic" confabulation (S2.6 F5). Producer metadata: 1/10,676 producers has a website (S2.7 F4). A sommelier landing on any marquee wine page today sees NULL vineyard + NULL producer metadata + (in 16,429 pages) confabulated volcanic soil claims. First-impression credibility is actively negative, not neutral.
7. **`describe-chemical` is STILL deployed at S2.9.** Re-verified via `list_edge_functions`: `describe-chemical` version 5, `status=ACTIVE`, `verify_jwt=false`, shares `ANTHROPIC_API_KEY`. Zero wine logic, leftover from an unrelated project. **This is an unauthenticated credit burn risk.** Anyone with the URL can exhaust the Anthropic key budget. S2.5 F1 flagged it; S2.8 re-verified it's still deployed; S2.9 re-verifies again. 5 minutes to delete. Not deleting is an active business risk, not just a hygiene issue.
8. **The "quality before enrichment" sprint sequence delays all user-visible signal by 1-2 quarters.** Sprints 2+3+4+5 before any new user-facing content ships. This is defensible as a correctness strategy but carries a distribution risk: Vivino, Wine-Searcher, Delectable, and LLM-based wine assistants are all shipping during that window. The Sprint-sequence doesn't have a parallel "start collecting user signal" track — no landing page for sign-ups, no "watch us build" newsletter, no demo video, no "preview access for 50 sommeliers." **Quality-first without signal-collection creates a two-quarter window where Loam cannot learn what users want.**

**Biggest business strengths (things the audit confirms are actually working):**

- **The data asset is real.** 155K wines with LWIN+COLA+UPC identifiers, 3.28M TTB COLA records, 10,676 producers, 3,662 appellations with geographic boundaries + weather + soil linkages + grape linkages + rules, 9,693 grapes with synonyms, 346 reference insight rows, 5,108 wine insights, 134,877 appellation-vintage weather rows, 1.6M monthly weather rows. The depth and structure are substantial even if the surface content has errors.
- **The backbone ID strategy is strong.** Storing COLA + LWIN + lwin_7 + UPC + QR in `external_ids` means cross-source dedupe is a straight join, not a fuzzy match. This is a legitimate architectural advantage over Vivino (no backbone IDs — all fuzzy) and CellarTracker (user-entered names — high dupe rate).
- **Price data is closer than it looks.** `archive.wine_vintage_prices` = 139,937 rows vs `public.wine_vintage_prices` = 23,220 live rows. **116,717 prices are waiting in archive for the S2.2 F1 / S2.5 F3 relink to unlock.** This is potentially 2-3 days of work to unlock ~35% price coverage on the active corpus. Price coverage going from 1.81% → ~35% would be the single biggest user-visible improvement Loam has ever shipped.
- **Unit economics are fine.** Haiku Grade C: $0.00293/wine (measured across 5,067 completed calls). Sonnet Grade B: $0.01286/wine. Full Grade C coverage on 155K wines = ~$456. Full Grade B coverage = ~$2,003. **Cost is not the moat constraint.** The moat constraint is correctness, voice, and distribution — not dollars.
- **The TTB COLA data set is genuinely rare.** 3.28M COLA records with 96.8% detail scrape + 99.86% printable scrape + 1.82M label images is not something a competitor would casually replicate. It's the closest thing Loam has to a data moat. The S2.6 F5 volcanic soil issue is a content-quality problem, not a data-asset problem.
- **`enrichment_log` has real cost tracking.** $16.19 total spent, model-tagged, status-tagged, attempt-counted. The cost discipline is ready for a monetization model — but the model itself is missing.

**Scope-breaker check:** **One soft scope-breaker surfaced.** The business audit confirms that the technical audit's "fix everything" framing would take 20+ sessions before any user-visible value lands. Sprint 3 should be scoped around "unblock Sprint 5 + unlock staging depth + restore first-impression credibility on the 100 most-likely-to-be-demoed marquee wines" — not around "close every P0/P1 in the 245-finding list." This is a reframing, not a structural break. Synthesis.md operationalizes it.

---

## Findings

### F1 — No monetization model exists anywhere in the architecture [P0]

**Severity:** P0 — existential
**Effort:** `medium` — Sprint 3 decision, not a code fix
**Related:** Supersedes the implicit assumption in CLAUDE.md Next Steps

**Evidence:**
- `grep -r "subscription\|stripe\|paywall\|billing\|plan" frontend/src/` returns zero results
- No `pricing` route, no `checkout` route, no `account` route in `frontend/src/App.tsx`
- `docs/ENRICHMENT.md` documents cost per wine ($0.018-0.05) but never revenue per wine
- `CLAUDE.md` "Current Focus" / "Strategic Context" / "Next Steps" sections contain zero reference to revenue, subscription, pricing, or monetization
- `docs/DECISIONS.md` (1,559 lines, 271 entries, S2.8 F17) has no entry on monetization strategy
- `docs/PRINCIPLES.md` is entirely about product philosophy, not business model

**Impact:** Sprint 3 + Sprint 4 + Sprint 5 are ~20 sessions of development work on a product with no revenue model. Every $16 of enrichment spend + every hour of engineering is pure R&D. At the current velocity, there's no point at which Loam decides "we have enough to charge for this." The default path is "build forever, launch never" unless this decision lands before Sprint 5.

**Proposed fix:** Sprint 3 adds one decision session (or a Sprint-3-exit gate) that picks a monetization direction from a shortlist. Concrete options:
- **Freemium + Pro subscription** ($9/mo for unlimited deep lookups, free for 10/day). Low infrastructure cost, simple paywall, aligns with quality-first positioning.
- **Affiliate links on prices** (take 4-8% of Wally's / Spec's / LCBO / BC Liquor referral clicks, free consumer tier). Zero-paywall, aligns with "we have the data, you buy wherever." Blocked on S2.2 F1 relink.
- **B2B API license** ($500-2000/mo per client, wine inventory + identifier dedupe + structured facts). Highest-value-per-customer, slowest to close.
- **Pay-per-lookup for verified facts** (the "ChatGPT for wine" sub-economy). Technically feasible, strategically weird.

Not "pick one and never change it" — pick a default, test it at Sprint 5 exit, iterate. The alternative is the current default: "figure it out later" forever.

---

### F2 — Zero user wine lookups ever logged; enrichment pipeline has never fired on-demand [P0]

**Severity:** P0 — no demand signal
**Effort:** `small` — instrumentation is cheap, but the decision about what to learn is harder
**Related:** S2.7 F9 (no .catch() in consumer pages means fetch errors can't be surfaced either)

**Evidence:**
- Live query: `select count(*) from public.wine_lookups` returns **0 rows**
- Live query: `select min(looked_at), max(looked_at) from public.wine_lookups` returns NULL / NULL
- Live query on `enrichment_log`: all 5,108 wine enrichments + 5,293 attempted rows are one-shot batches from 2026-04-10 (same day). Zero on-demand fires.
- `CLAUDE.md` line 367 documents `wine_lookups` table + `anon_insert` RLS policy for anonymous page views — the infrastructure exists, the instrumentation does not
- Frontend `WinePage.tsx` (per S2.7) fetches wine data but never writes to `wine_lookups`
- `docs/ENRICHMENT.md` "User lookup triggers B enrichment" is aspirational — no code path writes it

**Impact:** The entire quality-before-enrichment strategy is being optimized against an imagined user base, not an observed one. Sprint 5 will enrich 155K wines with no information about which wines users actually want. The top 100 wines by hypothetical search volume are an assumption, not a measurement. If Sprint 5 ships Grade B enrichment on the wrong 1,000 wines, there's no way to detect it because there's no user lookup data to check against.

**Proposed fix:** Add a `useWineLookupLog()` hook in `frontend/src/hooks/` that fires on WinePage mount, writing to `wine_lookups` with `(wine_id, wine_vintage_id, source='web')`. Single 10-line effect. Must land before any sommelier demo or external-traffic invitation. Once data flows, **the top 1,000 enrichment targets can be driven by actual lookup frequency instead of guesswork.**

---

### F3 — Loam's moat is inverted: direct LLM queries return more factual answers than Loam's enriched pages today [P0]

**Severity:** P0 — positioning
**Effort:** `large` — Sprint 3+5 work, not a fix
**Related:** S2.3 F5/F7/F8/F10, S2.6 F3/F4/F5, S2.7 F3/F6

**Evidence:**
- S2.3 F2: 2,743 of 2,809 Chardonnay-named wines (97.6%) have Pinot Blanc linked. A direct Claude query on "what grape is Bogle Phantom Chardonnay" returns "Chardonnay"; Loam's page says "Chardonnay + Pinot Blanc."
- S2.3 F5: Joseph Phelps Eisele ai_wine_summary confabulates "Phelps purchased 38-acre vineyard in 2013" (actual: Artemis/Latour). A direct Claude query returns the correct Artemis acquisition.
- S2.3 F14 + S2.6 F5: Hunter Valley "volcanic soils" confabulation (actual: alluvial/clay/sandstone). Direct Claude query returns the correct alluvial characterization.
- S2.6 F3: 5/5 random Grade C hook samples contained invented facts (Schramsberg J Schram "still rosé", DRC Corton-Charlemagne "avoids MLF", Pax Obsidian "likely 14.5%+" fabricated ABV). Direct LLM queries on these wines return factually correct baseline knowledge.
- S2.7 F3: 2,914 live wine pages render the Chardonnay+Pinot Blanc chip combination as UI truth.

**Impact:** A sophisticated user testing Loam on a marquee wine (DRC, Lafite, Chambertin, Barolo, Napa cult cab) will get a worse answer than asking Claude/Perplexity/Gemini directly, for free, with no sign-up. Loam's entire positioning depends on being better than "ask an LLM" — currently it is measurably worse on factual accuracy, matches on voice, and slightly better on structured linkage. **The moat only exists post-Sprint 5 regeneration + L3 fact-check gate**.

**Proposed fix:** This is a Sprint 3/5 fix, not a Sprint 3 line item. But the business implication must land in the Sprint 3 synthesis:
- Sprint 3 pre-reqs MUST include grape repair compound (S2.3 F2 + S2.4 F2 + S2.5 F2/F11/F17) + voice module consolidation (S2.6 F1/F2) + L3 fact-check gate (rescoped $18 S2.3 pre-auth).
- `ENRICHMENT_ENABLED=false` feature flag must stay OFF through Sprint 3 AND Sprint 5 regen until a sample fact-check shows <2% factual errors.
- Post-regen validation should explicitly benchmark against direct Claude/Perplexity answers on a 50-wine sample. If Loam still loses on factual accuracy post-Sprint-5, the positioning needs to change (away from "AI wine story" toward "structured data + store linkage + terroir-specific renderable facts").

---

### F4 — ICP (ideal customer profile) is not defined; architecture implies "any of the above, partially" [P0]

**Severity:** P0 — positioning
**Effort:** `small` — decision session, not implementation
**Related:** CLAUDE.md line 105 ("Users look up a wine and get the full story"), `memory/product-architecture.md`

**Evidence:**
- `CLAUDE.md` line 105 defines the product as "users look up a wine and get the full story" — no qualification of which users
- `memory/product-architecture.md` — contains the phrase "target users" but does not define them with specificity (per S2.8 F20 review, uses "Tier 0-3" nomenclature superseded by F/D/C/B/A anyway)
- `docs/PRINCIPLES.md` is entirely value-driven (#3 real data, #5 no synthetic, #9 structured UI) — no ICP statement
- `docs/ENRICHMENT.md` assumes enrichment fires when "a user searches for a wine" — which user, and with what budget
- `frontend/src/` has no sign-up flow, no persona-specific landing page, no pricing page — the product implicitly serves anonymous browsers
- S2.7 F29: HomePage autoFocus pops mobile keyboard on load — implies mobile-primary, which implies casual/drinking/discovery use, not desktop-deep-reading research use

**Potential ICPs that would shape different Sprint 3 priorities:**
- **Wine enthusiasts / hobbyists** (the Vivino/Delectable market): need price comps + scan-a-label + user ratings. Loam has none of these; the depth/terroir story doesn't matter to them. Sprint 3 priority = price unlock + mobile scan.
- **WSET / sommelier students / trade** (the GuildSomm market): need terroir depth + exam-ready facts + appellation rules + classifications. Loam's Sprint 5 target fits this. Sprint 3 priority = L3 fact-check + appellation rules correctness + reference-layer European coverage.
- **Retail wine buyers** (the Wine-Searcher market): need price search + store links + historical price trends. Loam has 23K prices (F3 evidence). Sprint 3 priority = S2.2 F1 relink + affiliate architecture.
- **Restaurant beverage directors** (the SevenFifty market): need distributor catalogs + production volume + availability + pricing. Loam has ~80K staging prices + importer catalogs (Skurnik, Empson, KL, Winebow) but none linked to "currently available in my state." Sprint 3 priority = staging relink + state-level inventory.
- **B2B data licensees** (the Liv-ex / LWIN / proprietary data market): need clean identifiers + dedupe + structured facts + API. Loam has backbone IDs ready; no API. Sprint 3 priority = REST API + auth + rate limiting.

**Impact:** Sprint 3 prioritization cannot be made rigorously until one of these ICPs is picked. Staging relink (P0 by raw severity) might be lower priority than fact-check gate if the ICP is "trade sommelier," and higher priority than fact-check gate if the ICP is "retail buyer." The "each expert audits their layer" structure of Sprint 2 didn't converge on who the product serves.

**Proposed fix:** Sprint 3 Session 0 = "pick the ICP for Sprint 5 target." Single 30-min decision. Output: one-page "ICP: [X]. Primary use case: [Y]. Secondary use case: [Z]. Sprint 5 success criterion: [measurable]." Every subsequent Sprint 3 prioritization call references this.

**Recommendation:** Default to **WSET / sommelier / trade**. Rationale: (a) this is the only ICP where Loam's "terroir depth + structured + audited + classified" positioning has a clean wedge against competitors; (b) the existing reference-layer investment (appellations, grapes, rules, soils, weather) is already aligned with this audience; (c) this audience will pay for quality ($60-200/year sub-prices are normalized for them); (d) they're the easiest ICP to instrument a feedback loop on (they're reachable via GuildSomm, WSET alumni networks, r/wine, Instagram sommelier accounts); (e) they will tell Loam about factual errors instead of silently churning.

---

### F5 — The "terroir" positioning is 10x more polished than the data supports [P0]

**Severity:** P0 — credibility-at-first-impression
**Effort:** `medium` — Sprint 3 fix scope
**Related:** S2.3 F3, S2.4 F17, S2.6 F5, S2.7 F4, S2.7 F6, S2.8 F4

**Evidence:**
- `CLAUDE.md:3` — product pitch: "Users look up a wine and get the full story — place, vintage weather, soil, grapes, producer choices."
- Live query: `select count(*) from public.vineyards where deleted_at is null` = **0 rows** (S2.8 F4 confirmed, re-verified)
- S2.4 F17: `appellation_soils` has zero provenance columns — the 930 soil links cannot be audited
- S2.6 F5: 49 of 82 appellation_insights contain "volcanic" (60%). Realistically only ~20 are majority-volcanic. Knights Valley / RRV / Sonoma Coast / Hunter Valley / Howell Mountain confabulations all confirmed via primary sources.
- S2.7 F4: 0 of 10,676 producers have `hectares_under_vine` / `total_production_cases` / `address` / `latitude` / `longitude` / `description` / `philosophy` / `year_established` / `parent_producer_id` / `parent_company` / `appellation_id`; 1 has `website_url`
- S2.3 F3: all 15 marquee producers (DRC, Lafite, Latour, Margaux, Haut-Brion, Pétrus, Gaja, etc.) have 0 metadata
- S2.7 F6: 16,429 live wine pages render contaminated volcanic soil claims at the wine level

**Impact:** A sommelier landing on /producer/DRC sees `website_url=null`, `year_established=null`, `latitude=null`, 0 Philosophy, 0 Estates & Labels, near-empty FactGrid. That's worse than what they'd get from a Google search + Wikipedia. A sommelier landing on a Knights Valley wine sees the Section "Soil" render "volcanic soils from ancient Mayacamas eruptions" — a factual confabulation. First-impression credibility is actively damaged, not neutral.

**Proposed fix:** Sprint 3 can partially close this in two steps:
1. **Mark the positioning honest.** Either rewrite `CLAUDE.md` product pitch to reflect actual state ("structured wine identifiers + growing terroir layer"), or add a "beta — depth coverage in progress" disclaimer to the marketing surface before any demo.
2. **Sprint 3 producer metadata strategy** (S2.3 F3 / S2.7 F4 dedupe): not "15 marquee producers manually" but "top 500 by wine count, via Haiku extraction from a curated web source + manual verification for the top 50." Budget $50-100.
3. **Reference-layer regeneration FIRST** (Sprint 5, constrained by this audit): regen 49 contaminated appellation_insights before any wine-layer regen.

---

### F6 — `describe-chemical` edge function is still deployed at S2.9, verified for the third time [P0]

**Severity:** P0 — active credit-burn risk
**Effort:** `trivial` — 1 command
**Related:** S2.5 F1, S2.8 edge function re-verification

**Evidence:**
- S2.9 live `list_edge_functions` MCP call confirms: `describe-chemical` slug, version 5, `status=ACTIVE`, `verify_jwt=false`, `created_at=1774897000667`, `updated_at=1774897000667`
- S2.5 F1 first flagged this 2026-04-11 with full prompt-body evidence ("You are a chemical industry analyst...")
- S2.8 re-verified via same MCP call — still deployed
- S2.9 re-verified a third time — still deployed
- Sharing `ANTHROPIC_API_KEY` with `enrich-wine` means anyone with the describe-chemical URL can exhaust the key budget

**Impact:** 3 sessions of auditing have flagged this. It's still deployed. The fix is `supabase functions delete describe-chemical` — 30 seconds. Not executing it across 3 audit sessions is itself a business finding: **Sprint 2's "read-only, no fixes" discipline is correct in principle but has left a live credit-burn risk deployed for 3 audit cycles.** The fix should happen in the Sprint 3 opening minutes, not wait for the grape-repair compound.

**Proposed fix:** Sprint 3 Session 1 Minute 1: delete `describe-chemical` via `supabase functions delete describe-chemical` (or MCP equivalent). Log in journal. Verify via `list_edge_functions`. This is the smallest Sprint 3 win available and closes a real risk.

---

### F7 — Competitive parity table has Loam losing on every column except "structured backbone IDs" [P0]

**Severity:** P0 — positioning
**Effort:** `medium` — thinking, not code
**Related:** F3 inverted moat; CLAUDE.md strategic pitch

**Evidence:** Explicit competitive comparison, grounded in Loam audit state + training knowledge of competitor feature sets. Treat competitor specifics as point-in-time, not live-queried.

| Column | Loam (today) | Vivino | Wine-Searcher | CellarTracker | Vinous / Jancis / Decanter / WA | GuildSomm | Perplexity / Claude (free) |
|---|---|---|---|---|---|---|---|
| **Wine identity coverage** | 155K active, LWIN+COLA+UPC backbone | ~13M wines, user-crowdsourced | ~15M wines | ~10M wines, cellaring-focused | <10K "rated" wines, editorial-gated | ~5K study-focused wines | N/A (retrieval from training data) |
| **Price data** | **1.81% coverage live** (F3) | price at scan, real-time | industry-standard, near-100% on fine wine | zero (not a price tool) | zero | zero | zero |
| **Scan-a-label UX** | zero | **dominant feature** | zero | zero | zero | zero | zero |
| **User ratings** | zero | 65M+ user ratings | zero | 15M tasting notes | editorial-only, paywalled | small community | zero |
| **Terroir depth (soil/weather/vintage)** | **3,662 appellations + 134K appellation-vintage weather rows + 930 soil links** (30% correct, S2.6 F5) | "Taste Profile" + appellation name | minimal | community tasting notes | editorial prose on top regions | ★ industry standard | rich but unsourced |
| **Producer metadata (website/year/coords)** | **0/10,676 have coords, 1 has website** (S2.7 F4) | producer name + country only | producer name | community-entered | editorial prose | trade-quality | rich but unsourced |
| **Vintage weather / GDD / growing season** | ★ 134,867 rows of real NASA POWER + Open-Meteo data (S2.1, CLAUDE.md) | zero | zero | zero | prose descriptions | prose descriptions | guesswork from training data |
| **Appellation rules / legal structure** | 1,165 `appellation_rules` rows (S2.4 F11 notes schema inconsistency) | zero | zero | zero | editorial prose | ★ best-in-class | guesswork |
| **Grape identity + synonyms** | 9,693 grapes + 34,820 synonyms (S2.4 F2 shows contamination) | limited | name-only | community | prose | ★ best-in-class | rich |
| **AI synthesis / "full story"** | 5,108 enriched rows, **4-6% factual errors** (S2.6 F3/F4), voice issues (S2.6 F1/F2) | zero | zero | zero | ★ **editorial prose is the product** | ★ pedagogical, specific | **cheap, factually OK, no moat** |
| **Mobile PWA / native app** | PWA, PAUSED (S2.7 + CLAUDE.md) | ★ native iOS/Android | mobile-web | mobile-web | mobile-web, paywalled | mobile-web | chat-native |
| **Sign-up / account / feedback loop** | **0 rows in wine_lookups** (F2) | 65M users | accounts-based | community-based | paid accounts | paid accounts | anonymous |
| **Monetization model** | **none** (F1) | ads + affiliate + merchant tools | Pro subscription + API | free + $40/yr CT supporter | $60-200/yr subs | $149/yr sub | N/A |
| **B2B API** | none | none public | **yes ($250+/mo)** | none | none | none | API exists but not wine-specific |
| **Structured facts (varietals, ABV, region)** | 98-100% coverage on identity layer | fuzzy | fuzzy | community | editorial | editorial | chat output |
| **Maps / geographic boundaries** | ★ 2,847 appellations with PostGIS boundaries (CLAUDE.md) | region name only | region name only | region name only | prose | text-based | N/A |

**Loam has ★-class in 4 columns: vintage weather, appellation rules-level structure, geographic boundaries, backbone IDs. Loam is at zero or below on 6 columns: price, scan UX, user ratings, monetization, API, sign-up/feedback.**

**Impact:** Loam's wedge is terroir + weather + geography + structured backbone. That aligns with the WSET/trade/sommelier ICP (F4 recommendation) and not with the Vivino/enthusiast/discovery ICP. Choosing Sprint 3 priorities without choosing an ICP means the 4 wedge columns get underinvested while the 6 gap columns get investment they can't win with.

**Proposed fix:** Sprint 3 priorities should favor the 4 wedge columns (terroir correctness, weather story rendering, appellation rules consistency, backbone ID integrity) over the 6 gap columns (price — although F3 staging relink is a low-effort high-value partial, scan — defer, ratings — defer, monetization — decision only, API — defer to Sprint 6, sign-up — F2's 10-line instrumentation is the ONLY gap-column Sprint 3 should touch).

---

### F8 — Sprint sequence delays user-visible signal by 1-2 quarters without a parallel signal-collection track [P0]

**Severity:** P0 — go-to-market risk
**Effort:** `small` — 1 decision + 3 hours setup
**Related:** Sprint 2+3+4+5 timeline, F2 zero lookups, CLAUDE.md Sprint sequence

**Evidence:**
- `CLAUDE.md:136-142` — Sprint sequence: 1 (30K, done) → 2 (Audit, active) → 3 (Execute fixes) → 4 (Reference Design) → 5 (Reference Enrichment). "Enrichment at scale does NOT start until Sprint 5."
- S2.3 + S2.4 + S2.5 + S2.6 + S2.7 + S2.8 + this S2.9 = 9 sessions in Sprint 2. Rough session velocity: ~1-2 per week for solo work with audit rigor.
- Sprint 3 estimated 8-12 sessions (this synthesis). Sprint 4 (reference redesign) estimated 4-6. Sprint 5 (execution) estimated 6-10.
- Total: ~25-30 sessions between today and first Sprint 5 user-visible content ship.
- At 1-2 sessions/week: **3-6 months before any new user-facing value ships.**
- During that window: Vivino ships updates weekly, Wine-Searcher adds data weekly, LLM models get better monthly, Perplexity ships wine-specific finetunes, Delectable ships mobile updates. Loam's "zero new user-visible shipping for 3-6 months" is a real competitive position.
- Parallel signal-collection: **zero.** No landing page for sign-ups. No "watching us build in public" newsletter. No demo video. No preview access program. No Twitter/X / Instagram / LinkedIn presence. No sommelier outreach.

**Impact:** Sprint 5 launches into a market that has moved significantly. Worse: Sprint 5 launches with zero user feedback to calibrate against, because F2 shows no one has ever looked up a wine. The risk is not "we ship too slow"; the risk is "we ship into a void and learn nothing about whether it worked."

**Proposed fix:**
- **Sprint 3 adds 1 parallel track: "signal collection."** 3 hours of work, not a blocker on any technical Sprint 3 item. Deliverables:
  - Landing page at `loam.onrender.com/` with a clear "what is Loam" sentence + email sign-up (Buttondown/Substack/ConvertKit — free tier).
  - Wire `wine_lookups` instrumentation (F2 fix, 10 lines). Every page view logs.
  - 1 short "building Loam in the open" post with a screenshot of a working wine page + 3 known gaps.
  - Outreach to 10 sommelier contacts via direct message with "preview access, feedback requested."
- **Sprint 3 exit criterion:** at least 50 email subscribers + at least 10 logged wine_lookups + at least 3 qualitative feedback responses. Concrete, measurable, achievable without waiting on Sprint 5.
- **None of this blocks the correctness work.** It runs in parallel, in the same sessions, at the margin. The alternative is Sprint 5 launching to 0 users with 0 feedback.

---

### F9 — No feedback loop instrumentation anywhere except the mostly-idle accuracy_audit table [P1]

**Severity:** P1 — quality signal gap
**Effort:** `small`
**Related:** F2, S2.8 scheduled tasks re-verification

**Evidence:**
- Live query: `select count(*) from public.accuracy_audit` = **34 rows** (across all of Loam's history)
- S2.8 re-verification: 4 of 5 scheduled tasks are `enabled: false`; only `open-meteo-weather-drip` is running. The `data-accuracy-agent` task is disabled.
- `wine_lookups` is 0 rows (F2)
- `enrichment_log.reviewed_by` / `reviewed_at` columns exist but are never populated (no review happens)
- No "report an error" button on any wine page (per S2.7 page grep)
- No "did this answer help?" feedback prompt on any ai_* rendered field
- No Slack/Discord/forum for user feedback referenced anywhere

**Impact:** Loam has no way to learn what's wrong, what's right, or what users want. All 245 audit findings were produced by Opus inline reasoning — not by user-reported bugs. Sprint 5 regeneration will go out with zero real-user validation infrastructure.

**Proposed fix:**
- Sprint 3 wires `wine_lookups` instrumentation (F2 fix).
- Sprint 3 adds a "was this accurate?" thumbs-up/thumbs-down on each ai_* field renderer, writing to a new `content_feedback` table.
- Sprint 3 re-enables the `data-accuracy-agent` scheduled task (S2.8 confirmed it's paused; re-enable with a Sprint-3-refreshed prompt pointing at L3 fact-check gate).
- Sprint 3 exit criterion: at least 10 content_feedback rows from real user sessions (requires F8 outreach to be live).

---

### F10 — No affiliate / retailer-link revenue architecture despite 82K+ prices in staging and 14 retailers [P1]

**Severity:** P1 — monetization gap on lowest-effort revenue model
**Effort:** `medium` — Sprint 4 or Sprint 5 work
**Related:** F1, S2.2 F1 staging relink, 14 retailers in DB

**Evidence:**
- Live query: `select count(*) from public.retailers` = 14 retailers (Wally's, Spec's, LCBO, BC Liquor, Systembolaget, PA PLCB, FirstLeaf, Flatiron, Best Wine Store, Domestique, Last Bottle, Utah DABS, Empson, etc.)
- Archive prices: 139,937 rows waiting to be relinked (S2.2 F1)
- Frontend `WinePage.tsx` (per S2.7): renders prices as plain text, not as clickable affiliate links
- `docs/ENRICHMENT.md` documents cost per wine but no revenue per click
- No UTM tracking, no referral code architecture, no Skimlinks/VigLink/direct-partnership integration
- Wally's, Spec's, LCBO all have existing affiliate programs Loam could sign up for (not audited, but industry-standard)

**Impact:** The S2.2 F1 relink unlocks ~116K prices. Without affiliate links wired, that unlock is pure user-value (they see prices) with zero revenue capture. Wine-Searcher monetizes the same dataset at ~4-8% of referral clicks; at $100 average wine price × 2% take rate × 1,000 clicks/month = $2,000/mo minimum viable affiliate revenue. Loam is one architectural step from having that option.

**Proposed fix:** Sprint 4 (not Sprint 3) — after staging relink lands, add a `retailer_affiliate_links` table mapping retailer_id → base_url + referral_param_template. Render prices as `<a href={template.format(original_url)}>` instead of plain text. Loam does not need to negotiate a partnership to test this — start with generic click-through, measure CTR, then negotiate partnerships at inflection point. Zero Sprint 3 work; but Sprint 3 scope should acknowledge this is next.

---

### F11 — B2B API licensing opportunity is completely unexplored [P1]

**Severity:** P1 — highest-LTV monetization path is not on the roadmap
**Effort:** `large` — Sprint 6+ work
**Related:** F1, strong backbone ID asset, weak API surface

**Evidence:**
- Loam's identifier layer (LWIN 170K + COLA 253K + UPC 13K + lwin_7 50K) is potentially one of the cleaner wine-identifier layers publicly accessible
- 3.28M TTB COLA records with label images is a legitimately rare asset
- No REST API, no GraphQL, no API key system, no rate limiting, no documentation
- Supabase PostgREST is the only "API" and it's not credential-gated for third-party consumption
- `frontend/.env` contains the anon Supabase key — no separation between "consumer frontend auth" and "third-party API auth"
- B2B comparable: Wine-Searcher API starts at $250/mo; Liv-ex feed is enterprise-only; LWIN direct licensing is ~$5K/yr; a Loam API at $200-500/mo/customer only needs 5-10 customers to exceed the Sprint 5 enrichment cost

**Impact:** The highest-margin revenue model available to Loam — B2B data licensing to wine clubs, restaurants, marketplaces, education tools — is not even in the "Open Questions" section of CLAUDE.md. A single $500/mo license covers all of Sprint 5's enrichment budget 20x over. The audit surfaces this for the first time.

**Proposed fix:** Sprint 3 does NOT need to build this. Sprint 3 DOES need to:
- Add this as an Open Question in CLAUDE.md
- Add an "API productization" line item to Sprint 6 scoping (post-Sprint-5)
- Keep the backbone ID layer clean (staging relink in F1 of synthesis.md supports this indirectly — dangling wine_ids are an API-credibility issue)
- Avoid architectural choices in Sprint 3/4 that would make future API productization harder (e.g., don't embed personal data in the public schema)

---

### F12 — Brand voice and AI voice are conflated; audit surfaces that AI voice produces factual errors but the brand voice needs to exist separately [P1]

**Severity:** P1 — credibility
**Effort:** `small` — 4-6 hours of copy + one decision
**Related:** S2.6 F1/F2/F3 voice findings, S2.8 F14 VOICE.md staleness

**Evidence:**
- `docs/VOICE.md` is entirely about AI enrichment prompt voice (S2.8 F14)
- No landing page copy, no tagline, no "about Loam" page (S2.7 F7 confirms the footer `/about` link goes to a blank screen)
- The brand voice is implicitly "whatever Claude writes for Loam" — which is precisely the thing the audit found to be factually unreliable (S2.6 F3)
- A wine product needs a trust signal at first impression: "who is this, why should I believe it, when was this last updated, how do you know" — none of which is rendered today (S2.7 F5)
- Successful wine media operate on brand-voice trust: "Antonio Galloni on Barolo" is worth more than "Vinous's GPT-4 rewrite on Barolo." Loam's positioning cannot survive on AI-generated voice alone.

**Impact:** Even if Sprint 5 ships perfect AI enrichment, the brand voice vacuum means users don't know WHO is making the claim. That's a credibility ceiling, regardless of content quality.

**Proposed fix:** Sprint 3 adds one short writing task:
- One-page `/about` copy answering: Who is Loam (anonymous personal project, explicit), what is the data source (TTB + LWIN + Open-Meteo + Claude synthesis, explicit), what are the known gaps (link to a public "known issues" page — could just be a rendered version of the synthesis.md Sprint 3 backlog), when was it last audited, when is it updated, how can users report errors.
- Adds a "Loam is a personal project / early access" disclaimer to the footer of every page.
- Adds an "AI disclaimer" (S2.7 F5) on every `ai_*` rendered field: "Generated by Loam's AI synthesis — verify for professional use."
- **Separates brand voice from AI voice explicitly:** brand voice is "honest, specific, sourced, known-gaps-out-loud." AI voice is the bounded output of `pipeline/lib/voice.py` (to be built in Sprint 3 per S2.6 F1/F2).

---

### F13 — "Loam" and "wine intelligence" have brand collisions that aren't flagged anywhere [P1]

**Severity:** P1 — trademark and SEO collision
**Effort:** `small` — 30 min research, decision
**Related:** brand strategy, CLAUDE.md "wine intelligence platform" phrasing

**Evidence:**
- "Wine Intelligence" is a registered UK market research firm (wineintelligence.com) since ~2002, now owned by IWSR. First page of Google for "wine intelligence" is wall-to-wall them.
- "Loam" is already used by: loam.bio (agricultural carbon startup, well-funded), loamenergy.com, multiple "Loam" home goods / pottery / architecture firms, and it's a common geological term
- Loam's GitHub repo is `loamv2` (implying there was a loam v1)
- No trademark search referenced anywhere in the docs
- `CLAUDE.md:3` "wine intelligence platform" is the explicit positioning — and the exact phrase that conflicts with Wine Intelligence Ltd.'s 23-year-old brand

**Impact:** SEO competing against Wine Intelligence Ltd. (which has 20+ years of wine-industry SEO) for the phrase "wine intelligence" is structurally unwinnable. Choosing a different primary phrase ("wine terroir platform" / "structured wine intelligence" / "wine data platform" / "wine story engine") opens a clean SEO lane. Trademark collision is a smaller risk but is not zero.

**Proposed fix:** Sprint 3 (or earlier — this is a 30-min decision):
1. Pick a different primary positioning phrase. Concrete options: "wine story engine" (distinctive, fits the "full story" product pitch, no obvious collision), "wine terroir platform" (descriptive, narrower, better SEO fit), "wine data platform" (generic but uncontested). Avoid "wine intelligence" due to the Wine Intelligence Ltd. collision.
2. Run a lightweight trademark search (free USPTO TESS) on the chosen phrase + "Loam" within International Class 42 (Software/SaaS) and 41 (Education).
3. Update `CLAUDE.md:3`, `docs/PRINCIPLES.md`, landing page tagline, README accordingly.

---

### F14 — No SEO / content-indexing strategy; every marketing-distribution channel is structurally broken before Sprint 5 [P1]

**Severity:** P1 — distribution
**Effort:** `small` (tactics), `medium` (content production)
**Related:** S2.7 F1 (empty h1 on 12K pages), S2.7 F2 (country pages 100% broken), S2.7 F21 (h1→h3 skip), S2.8 F9 (empty architecture/pipelines dirs — no docs site either)

**Evidence:**
- S2.7 F1: 12,083 wine pages render empty `<h1></h1>`. Google's 2025 ranking factors weight h1 heavily; empty h1 pages are structurally down-ranked.
- S2.7 F2: 100% of country pages silently fail to render ai content. Country pages are the highest-value SEO surface (long-tail search: "Italian wine regions," "French appellations guide").
- S2.7 F21: h1 → h3 hierarchy skip across all 8 detail pages (WCAG 1.3.1 violation + SEO penalty)
- `frontend/public/` contains no `sitemap.xml`, no `robots.txt`, no `og:` meta tags confirmed (would need specific verification but S2.7 didn't flag their presence)
- No blog, no newsletter, no "wine of the week," no editorial surface
- No X/Twitter account, no Instagram presence, no LinkedIn company page referenced anywhere
- CLAUDE.md Next Steps section mentions "frontend resume" as a future item — no SEO step before or after
- 155K wine pages is a significant structural SEO opportunity: long-tail queries like "2019 Chambertin tasting notes," "Napa Cabernet under $50," "Sauvignon Blanc grape variety" map to specific Loam pages. That opportunity is currently wasted.

**Impact:** Content marketing is the lowest-cost distribution channel available for a content product. Loam has 155K potentially-indexable pages and 0 of them currently rank for their target queries because the structural SEO is broken. Sprint 5 will ship perfect content into a Google black hole.

**Proposed fix:**
- **Sprint 3 UI hygiene bundle (S2.7) fixes the mechanical SEO issues**: empty h1 (F1), country page silent fail (F2), h1→h3 hierarchy (F21). Zero extra work beyond what S2.7 already recommends.
- **Sprint 3 adds 4 SEO hygiene items:** `sitemap.xml` generator, `robots.txt`, og/twitter meta tags on all detail pages, canonical URL tags. ~2 hours.
- **Sprint 3 does NOT start content production.** That's a post-Sprint-5 marketing sprint. But Sprint 3 should NOT close the SEO-hygiene gap so that when content arrives, it's indexable.

---

### F15 — No press kit / media surface / "for reviewers" page [P1]

**Severity:** P1 — distribution
**Effort:** `small` — 2 hours
**Related:** F8 signal-collection, F13 brand positioning

**Evidence:**
- No `/press` route, no `/media` route, no `/for-writers` route
- No screenshot gallery, no high-res logo, no "about the founder" link
- No direct-message channel beyond GitHub issues (and Loam isn't open-source-publicly)
- No "coverage so far" section (zero coverage so far, but needs a placeholder)
- No "get in touch" or contact page
- S2.7 F7: the single /about link goes to a blank screen

**Impact:** If a wine media outlet (Wine Enthusiast, SevenFifty Daily, GuildSomm Podcast, PUNCH, Noble Rot, etc.) wanted to write about Loam or interview the creator, they have no surface to land on. This is a zero-cost distribution channel that's unavailable because the surface doesn't exist.

**Proposed fix:** Sprint 3 adds 3 new static pages:
1. `/about` — one page, "what is Loam, who built it, what is it for" (see F12 copy task)
2. `/press` — screenshots, logo, 3-sentence description, contact email
3. `/known-issues` — rendered version of Sprint 3 backlog, kept current with each audit session

~2 hours combined. Requires F12's brand voice decision but nothing else.

---

### F16 — Unit economics for Sprint 5 coverage are fine; risk is all non-cost [P1]

**Severity:** P1 — de-escalates the cost anxiety, re-escalates the correctness anxiety
**Effort:** `trivial` — decision surfacing only
**Related:** F3 inverted moat, F1 monetization

**Evidence (live-computed):**
- Live `enrichment_log`: 5,067 completed Haiku Grade C wines = $14.85 total → **$0.00293 per wine**
- Live `enrichment_log`: 105 completed Sonnet Grade B wines = $1.35 total → **$0.01286 per wine**
- Sprint 5 projections:
  - Grade C on all 155,623 active wines: ~$456 (one-shot)
  - Grade B on all 155,623 active wines: ~$2,003 (one-shot)
  - Grade B on top 10,000 by search_catalog priority (hypothetical): ~$130
  - L3 fact-check gate + Opus verification on 1,000 spot-check wines: ~$30-80 (depends on Opus pricing at time of run)
  - Full reference-layer regeneration (~500 European appellation_insights): ~$15-20
- Error rate: 229 errors of 5,401 calls = **4.24%** (acceptable for an enrichment pipeline; retriable)
- Combined Sprint 5 coverage (one-shot C + top-1K B + L3 gate + reference regen): **~$620-700 total**
- $50 Sprint 2+3 combined ceiling has been essentially untouched ($0/$25 Sprint 2 actual, $50/$50 Sprint 2+3 headroom)
- Sprint 5 could easily absorb a $500-1K budget if the quality gate demands it

**Impact:** The cost conversation is a red herring. Loam is not cost-constrained on enrichment. Loam is **correctness-constrained, distribution-constrained, and monetization-constrained**. Sprint 3 and Sprint 5 should be scoped around those constraints, not around Haiku vs Sonnet unit prices.

**Proposed fix:** Sprint 3 should not carry "cost anxiety" as a constraint. The budget ceiling should be reframed as "the amount we can spend on correctness verification + voice module testing + L3 fact-check gate + re-runs," not as "the amount we can spend on enrichment itself." Probable Sprint 3 budget: $30-100 actual. Probable Sprint 5 budget: $500-1,500 actual (includes at-scale Haiku C + on-top Sonnet B + L3 gate + reference regen).

---

### F17 — Enrichment cost is currently decoupled from anything that resembles revenue; the unit economics loop is broken on the demand side, not the supply side [P1]

**Severity:** P1 — strategic
**Effort:** `small` — decision + basic instrumentation
**Related:** F1 no monetization, F2 zero lookups, F16 supply-side cost is fine

**Evidence:**
- Supply side: $0.003 per Grade C wine, $0.013 per Grade B wine (F16)
- Demand side: **0 user lookups ever** (F2)
- Demand side: **0 revenue ever** (F1)
- Every $16 spent on enrichment to date has produced zero user-measurable value (because there are no users yet)
- The `enrichment_log` has no "served_at" column — enrichment is pre-computed at batch time, not linked to serving events

**Impact:** Loam is running a supply-side-only pipeline. That's fine for R&D, but it means the unit economics conversation is trapped — you can't optimize for "cost per active user" because active users = 0.

**Proposed fix:**
- F2 instrumentation (wine_lookups) starts building the demand-side denominator.
- Sprint 3 exits with a measurable unit economics statement: "N wine lookups, $M enrichment spend, N / M = $X per lookup value delivered, vs $Y revenue per lookup" (revenue = 0 in Sprint 3, but the structure is in place for Sprint 5+).
- Sprint 5 enrichment budget should be gated by Sprint 3 demand signal. E.g., "if we don't see 500 wine_lookups by Sprint 3 exit, Sprint 5 Grade B coverage is delayed to gather more signal."

---

### F18 — Legal/licensing status of scraped data is undefined across all 32 staging sources [P1]

**Severity:** P1 — legal risk
**Effort:** `medium` — audit + decision per source
**Related:** CLAUDE.md source listings, `docs/SOURCES.md`, S2.8 F3 source storage typo

**Evidence:**
- 32 staging tables, ~4.35M rows, scraped/imported from 32 distinct sources
- TTB COLA = US government data, public-domain, safe
- LWIN = paid trade backbone, licensed (safe)
- PRO Platform / TABC / WV ABCA / Kansas = state regulator public data, safe
- `source_vivino` (archived, xwines_* tables) = crawled from Vivino, **Vivino ToS prohibits scraping** (standard social-data ToS)
- `source_winedeals`, `source_wallys`, `source_specs`, `source_lcbo`, `source_bc_liquor`, `source_systembolaget`, `source_flatiron`, etc. = retailer catalogs, typically scraped via HTTP / sitemap. Most retailer ToS prohibit commercial use without permission.
- `source_berliner`, `source_texsom` = wine competition data (public, safe for wine identity; competition scores may have use restrictions)
- Importer catalogs (Empson, Skurnik, KL, Winebow, European Cellars, Polaner) = sales materials, typically shared without ToS
- No licensing audit is referenced in `docs/SOURCES.md` (S2.8 F3 already flagged this doc has a storage-column typo; F18 adds that it has no licensing column at all)

**Impact:** Public-consumer Loam as currently architected is likely OK (free consumer search tool, fair-use adjacent). B2B API productization (F11) is **legally risky** without per-source licensing clarification. Selling a data feed that includes Vivino-sourced data could be a takedown-worthy issue.

**Proposed fix:**
- Sprint 3 adds a `licensing_status` column to `docs/SOURCES.md` per source: `public_domain` / `licensed` / `scraped_fair_use` / `scraped_needs_review` / `prohibit_commercial_use`.
- Sprint 3 spends ~1 hour classifying each source (no legal review needed — just a self-assessment).
- Sprint 6 (B2B API, if pursued) filters the API data feed to only `public_domain` + `licensed` sources, or negotiates per-source licenses.
- Consumer-side Loam continues to use all sources (fair-use adjacent as a free consumer tool).

---

### F19 — No competitor pricing intelligence informs Loam's (nonexistent) pricing strategy [P1]

**Severity:** P1 — pricing positioning
**Effort:** `trivial` — 30 min of research, no code
**Related:** F1 no monetization, F7 competitive parity

**Evidence (pricing landscape, point-in-time April 2026):**
- Vinous: $169/yr individual
- Wine Advocate (Robert Parker): $129/yr
- Jancis Robinson: £75/yr (~$95/yr)
- Decanter Premium: $60/yr
- GuildSomm: $149/yr individual, $499/yr institutional
- Wine-Searcher: free basic, Pro $99-250/yr (price-alerts + ad-free), API $250+/mo
- CellarTracker: free, $40/yr supporter
- Vivino: free + ads + affiliate + "Premium" cellar tools ~$20/yr
- SevenFifty Daily: free content + SevenFifty ($300+/mo trade-only)

**Impact:** If Loam ever launches a paid tier, it has no reference frame for positioning. A "power user" Loam tier at $9/mo ($108/yr) would be below Vinous, at-Wine-Advocate level, above Jancis/Decanter. A "trade" tier at $149/yr would match GuildSomm. A "free + affiliate" tier matches Wine-Searcher free and Vivino. All of these are plausible; none have been thought through.

**Proposed fix:** Pair with F1. Sprint 3 monetization decision session should reference this pricing landscape. Recommended default: **free consumer tier + $9/mo Pro tier + $149/yr Trade tier**, mirroring the Wine-Searcher free/Pro split and the GuildSomm Trade price point. Institutional/API tier deferred to Sprint 6.

---

### F20 — "Why now" thesis is only partially valid post-audit [P1]

**Severity:** P1 — strategic
**Effort:** `trivial` — thinking only
**Related:** F3 inverted moat, competitive landscape density

**Evidence:**
- The "why now" thesis Loam implicitly assumes: (a) AI costs dropped — TRUE, $0.003 per Grade C wine is cheap; (b) Open-Meteo exists for weather data — TRUE; (c) Supabase + Claude SDK makes solo builds fast — TRUE; (d) LWIN licensing became accessible — TRUE (189K wines in backbone).
- The "why now" objections the audit has surfaced: (a) Perplexity + ChatGPT + Claude can already answer wine questions with factual accuracy (F3); (b) Vivino has 65M users and scan-a-label locked up (F7); (c) Wine-Searcher has 15M wines with real prices (F7); (d) GuildSomm owns the trade-education market (F7); (e) the "AI synthesis" wedge is commoditized — anyone can prompt Claude on a wine and get a reasonable answer (F3); (f) Sprint-5-speed is slower than competitor-ship-speed (F8).
- What IS valid in the "why now": the **structured data + backbone IDs + weather/terroir depth** combination is legitimately rare. No competitor has NASA POWER + Open-Meteo weather data joined to 3,662 appellations joined to 155K wines via LWIN/COLA backbone. That's the Loam-specific wedge. Everything else is commoditized.

**Impact:** "Why now" as a general thesis is not defensible post-audit. "Why now" for the specific niche — **structured terroir + weather + backbone IDs serving the trade/sommelier market** — is defensible. The synthesis needs to narrow the thesis.

**Proposed fix:** Rewrite CLAUDE.md product pitch to reflect the narrow wedge. Current: "Wine intelligence platform... place, vintage weather, soil, grapes, producer choices." Suggested: "Terroir-grade wine data: backbone identifiers, appellation rules, vintage weather, and structured producer facts — the dataset a sommelier can cite." Narrower, more honest, and aligned with the actual data asset.

---

### F21 — The PWA/mobile-first architectural choice is not user-research-backed [P2]

**Severity:** P2 — product choice, revisit at Sprint 5 exit
**Effort:** `small` — thinking + measurement, not code
**Related:** `memory/product-architecture.md`, S2.7 F29 autofocus-pops-keyboard

**Evidence:**
- `memory/product-architecture.md` (per S2.8 F20 content review) references PWA/mobile-first without user research backing
- S2.7 F29: HomePage autoFocus pops mobile keyboard on load — only makes sense if the primary use case is "in-store / restaurant / bar, typing a wine name into the phone"
- The content product Loam is building is deep-terroir content — "I am reading about this wine's soil profile and vintage weather while drinking it." That's more desktop than mobile-first for enthusiasts; more mobile-first for trade-in-situ.
- Wine research behavior split (industry knowledge, not audited): casual/discovery is mobile (Vivino wins), deep research is desktop (Wine-Searcher wins), on-shift reference is mobile+print (GuildSomm ships paper flashcards)

**Impact:** Investing in PWA mobile-first optimization when the ICP (F4) hasn't been chosen means Loam may be investing in the wrong platform polish. Trade users are more likely to read deep terroir content on a laptop during shift-prep than on a phone mid-service.

**Proposed fix:** Don't change anything in Sprint 3. At Sprint 5 exit, when demand data exists (F2 instrumentation), revisit the mobile-vs-desktop usage split and decide whether to invest in native mobile app, responsive-desktop optimization, or PWA polish.

---

### F22 — No "first 100 users" plan anywhere in the roadmap [P2]

**Severity:** P2 — distribution
**Effort:** `trivial` — 1 hour of thinking
**Related:** F8 signal-collection, F15 press kit

**Evidence:**
- No "launch plan" document
- No target-user list
- No "post on Reddit r/wine" or "post on r/sommelier" or "message 20 sommeliers on Instagram" or "submit to ProductHunt"
- No "write 3 blog posts before launch" content calendar
- CLAUDE.md Next Steps has pipeline items but no distribution items

**Impact:** Sprint 5 ships, and the next step is "build a landing page" which is the default of every pre-launch project that never launched. The first 100 users plan needs to exist BEFORE Sprint 5, not AFTER.

**Proposed fix:** Sprint 3 or Sprint 4 adds a single "first 100 users" document listing:
- 20 wine-industry Reddit/X/IG accounts to DM with a preview link
- 10 wine-industry publications/podcasts to pitch a "building in public" interview
- 5 wine-Discord communities to announce in
- 3 "wine of the week" sample posts to seed the /blog route (F14 infrastructure)
- 1 newsletter provider to sign up for (Buttondown free tier, Substack free tier, or ConvertKit)
- Target: 100 email subscribers + 500 wine_lookups before Sprint 5 ships

---

### F23 — Pricing accuracy / staleness is an acute risk that isn't surfaced to the user [P2]

**Severity:** P2 — trust / accuracy
**Effort:** `small` — UI + data hygiene
**Related:** F3 inverted moat, S2.2 F1 staging relink (when it happens), S2.7 F5 no AI disclaimer

**Evidence:**
- `public.wine_vintage_prices` has no `as_of_date` rendered on any consumer page
- Retailer prices change daily; a 6-month-old price is often wrong
- S2.7 confirms there's no "last updated" chip on consumer pages
- Affiliate revenue model (F10) fails if the linked price is wrong at click time — and the click-through is a trust-losing event the user remembers

**Impact:** Shipping stale prices is trust-destructive in a way shipping no prices isn't. Once Sprint 3 F1 staging relink unlocks ~116K prices, the next question is "how do users know which prices to trust?"

**Proposed fix:** Sprint 4 (not Sprint 3) — add `price_fetched_at` + "as of" rendering on price chips. Gate old prices (>30 days) behind a "show stale" toggle. Build a refresh pipeline that re-fetches top-1K wine prices weekly via a lightweight retailer-API drip (same pattern as Open-Meteo weather drip). Not Sprint 3 scope, but Sprint 3 backlog should acknowledge it so it isn't forgotten.

---

### F24 — A11y / ADA compliance is a legal risk in US consumer markets [P2]

**Severity:** P2 — legal / risk
**Effort:** `small` — already scoped in S2.7 F20/F21
**Related:** S2.7 F20 (zero aria attributes), S2.7 F21 (h1→h3 skip)

**Evidence:**
- S2.7 F20: zero aria-current / aria-live / aria-labelledby / htmlFor / role attributes in consumer pages; only 3 aria-label (hamburger menus)
- S2.7 F21: h1 → h3 heading hierarchy skip (WCAG 1.3.1 violation) across all 8 detail pages
- ADA lawsuit landscape: serial plaintiffs actively target small consumer sites with empty h1s, missing aria labels, and keyboard navigation gaps. Settlement-farm lawsuits cost $5-15K each.
- Loam has no sign-in, no account, no commerce — which reduces the exposed attack surface but does not eliminate it (ADA Title III applies to "places of public accommodation" including websites per US DOJ)

**Impact:** Sprint 3's UI hygiene bundle already fixes most of this (S2.7 F20/F21). The business-level framing is that these fixes should be treated as RISK MITIGATION, not POLISH. Do not deprioritize.

**Proposed fix:** Sprint 3 UI hygiene bundle from S2.7 — specifically F20 (a11y baseline — 2 hours per S2.7 estimate) and F21 (h1→h3 fix — 5 min with F27, 8x more without). Do not defer either. Treat S2.7 F20/F21 as a minimum-defensible-standard for public launch.

---

### F25 — No localization strategy; Loam is 100% EN-US and European/Asian/Latin American wine markets are untouched [P2]

**Severity:** P2 — market ceiling
**Effort:** `large` — Sprint 7+ work
**Related:** S2.6 F9 (82/82 US appellations in reference corpus), S2.4 F4 (French diacritics stripped)

**Evidence:**
- S2.6 F9: 82/82 appellation_insights are US AVAs. Zero Chambertin, Barolo, Champagne, Rioja, Chablis.
- S2.4 F4: French AOC names have diacritics stripped (Echezeaux vs Échézeaux, 6 Saint-Emilion variants). This is a data issue per S2.4 but also a localization issue — a French/Italian/Spanish user would instantly distrust "Saint-Emilion" spelled wrong.
- No i18n framework in `frontend/`, no locale switcher, no Spanish/French/Italian/German/Japanese/Mandarin translations
- The TTB COLA dataset is 100% US-registered wines; LWIN is trade-global but the `source_lwin` rows are backbone-only, no per-language content
- Global wine market by value: Europe ~45%, Asia ~25%, North America ~22%, Rest ~8% (training-knowledge rough estimates). EN-US is ~20-25% of the addressable market.

**Impact:** Loam's 4x market ceiling lives behind localization. An EN-US-only product has a 20% ceiling on global wine-buyer reach. For trade/sommelier ICP (F4 recommendation) this matters less (English is the trade lingua franca) but for enthusiast ICP it caps the market at the US+UK+AU slice.

**Proposed fix:** Not Sprint 3. Sprint 7+ work. Sprint 3 should (a) fix F4 diacritics so non-English names render correctly, (b) expand Sprint 5 reference regeneration to include at least top-20 European appellations (S2.6 F9 already scopes this at ~$15-20 incremental cost).

---

### F26 — No "data freshness" communication on consumer pages; everything renders as if perpetually current [P2]

**Severity:** P2 — trust
**Effort:** `small` — UI render + 1-2 schema fields
**Related:** F23 pricing freshness, S2.7 F5 no AI disclaimer

**Evidence:**
- S2.6 verified all 5,108 wine_insights written 2026-04-10 (one-shot batch); all 346 reference insights written 2026-03-06 (one-shot batch)
- No `enriched_at` rendered on any consumer page (S2.7 F5)
- Weather data: drip from Open-Meteo is current within ~days; NASA POWER baseline is 1981-2025
- Prices: 2026-04-04 depth promotion batch is 7 days stale as of S2.9
- Scores: import dates vary by source

**Impact:** A user seeing an insight last written 40 days ago has no way to know. Trust degrades when freshness is implicit. "Last updated" chips are cheap trust signals — failing to render them is a missed trust opportunity on every page.

**Proposed fix:** Sprint 3 UI hygiene pass (combined with S2.7 F5 AI disclaimer) adds a `<LastUpdatedChip>` component rendered under each enriched field. Reads `enriched_at` / `updated_at` from the existing schema. ~1 hour of work if F5's disclaimer work is already scoped.

---

### F27 — Sprint 5 has no measurable "done" criterion; scope could balloon indefinitely [P2]

**Severity:** P2 — scope management
**Effort:** `trivial` — 30 min decision at Sprint 3 exit
**Related:** F1 no monetization, F2 no feedback loop

**Evidence:**
- `data/sprints/current.json` — Sprint 3 is "scope depends on findings; size TBD"
- No Sprint 4 or Sprint 5 "done" criteria defined anywhere
- Without a done criterion, Sprint 5 could expand from "regen existing content" to "regen + new reference content + new producer metadata + new vintage-specific insights + …" indefinitely
- Sprint 2 had a done criterion (produce Sprint 3 backlog) — Sprint 2 is about to close cleanly on that criterion. Sprint 3+ should inherit the pattern.

**Impact:** Scope creep is the default failure mode of solo projects without done criteria. Loam has 2 sprints of track record + 9 sessions of audit discipline — enough to know that "done" criteria work and are enforceable. Not writing them for Sprint 4/5 is a repeat-offender risk.

**Proposed fix:** Sprint 3 synthesis defines Sprint 3 done criteria (see synthesis.md). Sprint 3 exit session defines Sprint 4 done criteria. Sprint 4 exit session defines Sprint 5 done criteria. Concrete, measurable, binary.

---

### F28 — The "loam" name is a soil type; the tagline does not do the explaining the name fails to do [P3]

**Severity:** P3 — branding polish
**Effort:** `trivial` — 1 hour of writing
**Related:** F13 brand collisions

**Evidence:**
- "Loam" is a soil type (sand + silt + clay, often considered the best for viticulture). The name is evocative to viticulture-literate audiences — and meaningless to ~99% of consumer/enthusiast audiences.
- No tagline explains this connection on the homepage, footer, or /about (F15 notes /about doesn't exist)
- A good tagline would answer "loam [what]?" in 5 words. Current: none.

**Proposed fix:** Sprint 3 (with F12 brand voice decision) adds a tagline. Options:
- "Loam — the wine data platform built on terroir." (descriptive, direct)
- "Loam — wine intelligence, rooted in the soil." (metaphor-first, plays on the name)
- "Loam — structured wine data with weather, soil, and story." (functional)

Recommendation: second option ("rooted in the soil") — plays on the name, fits the terroir positioning, and distinguishes from generic "wine data platform."

---

### F29 — "Wine intelligence platform" is generic and collides with Wine Intelligence Ltd. (see F13) [P3]

**Severity:** P3 — duplicates F13, noted separately because it's the specific CLAUDE.md phrasing
**Effort:** `trivial` — 1 CLAUDE.md edit
**Related:** F13 brand collisions

**Evidence:**
- `CLAUDE.md:3` — "Loam is a wine intelligence platform."
- Collides with Wine Intelligence Ltd. (F13)
- Does not describe what makes Loam different from any other "wine intelligence platform"
- Used in 1-2 other internal docs and memory files (per quick grep)

**Proposed fix:** Paired with F13/F20 — rewrite CLAUDE.md:3 to use a tighter, wedge-specific phrase. Suggested: "Loam is a terroir-grade wine data platform — backbone identifiers, appellation rules, vintage weather, structured facts." Update corresponding phrases in memory files to match. ~10 minutes combined.

---

### F30 — Wine.com / Total Wine / K&L data partnerships unexplored despite retailer catalog being the clearest B2B wedge [P3]

**Severity:** P3 — opportunity, not blocking
**Effort:** `medium` — business development time, not code
**Related:** F10 affiliate, F11 API licensing, CLAUDE.md Wine.com DataDome note

**Evidence:**
- CLAUDE.md Open Questions: "Wine.com scraping — BLOCKED: DataDome 403 on all product pages and API endpoints. 262K sitemap URLs in hand for future slug parsing..."
- No follow-up to "partnership" framing — Wine.com has ~262K wines in its catalog; Loam has 155K; a partnership where Loam brings structured facts + Wine.com brings inventory is win-win
- Total Wine and K&L not referenced anywhere
- No "get in touch with partnership" email address
- Same structural issue as F11 B2B API: the commercial path is untested

**Impact:** Loam's most commercially viable B2B partnerships are 3-4 intro emails away. They're not blocked on product quality; they're blocked on nobody having sent an email. Sprint 3 is not the right sprint for this, but the synthesis should acknowledge it.

**Proposed fix:** Add to CLAUDE.md Open Questions. Sprint 6 scope. Do not Sprint 3.

---

## Meta-patterns for synthesis.md

1. **Correctness-constrained, not cost-constrained.** F16 removes the "we can't afford Sprint 5" narrative. The constraint is fact-check gate quality + voice module consolidation + grape repair — not dollars. Sprint 3's job is to unblock correctness, not to save money.

2. **Demand signal comes first; enrichment comes second.** F2 is the single most under-weighted audit finding. Sprint 3 must wire `wine_lookups` instrumentation AND add a minimal signal-collection track (landing page + 10-sommelier outreach). The alternative is Sprint 5 shipping to zero users.

3. **The wedge is narrow — and defensible if narrowed.** F7 + F20 show that "wine intelligence platform" is too broad to win. "Terroir-grade wine data platform for the trade/sommelier audience" is narrow enough to win. Sprint 3 should validate the narrow positioning via the F8 signal-collection.

4. **Three risks re-verified across Sprint 2 sessions.** (a) describe-chemical still deployed (S2.5/S2.8/S2.9); (b) CLAUDE.md stale claims (S2.1/S2.7/S2.8); (c) Chardonnay/Pinot Blanc contamination (S2.3/S2.4/S2.5/S2.6/S2.7). Sprint 3 should execute the obvious fixes in the first session to end the re-verification pattern.

5. **Sprint 3 is 80% dedupe, 20% new work.** The raw P0+P1 count across S2.1-S2.8 is ~170. After dedupe (compound fixes, cross-layer overlaps), net Sprint 3 scope is ~30-40 work items across ~8-12 sessions. Synthesis.md operationalizes.

6. **Business findings reprioritize some technical P0s.** Empty h1 on 12K pages (S2.7 F1) is technically P0 but currently has 0 traffic (F2). The sommelier-demo-wine-lookup path (staging relink + grape repair + marquee producer metadata + describe-chemical delete + voice consolidation) is the actual Sprint 3 critical path, not "close every P0 by count."

7. **Sprint 5 is not "run enrichment";** Sprint 5 is "validate the quality gate by running enrichment on a bounded sample, then gate expansion on measured output quality." Sprint 3 builds the gate. Sprint 5 runs through the gate. Calling it an "enrichment sprint" under-sells the gating logic.

8. **Every sprint should exit with a measurable done criterion.** Sprint 2 did (245 findings → Sprint 3 backlog). Sprint 3+ should inherit this. F27 operationalizes.

---

## Running totals

- **Sprint 2 sessions complete:** 9 of 9 (S2.1 → S2.9) — all $0 actual spend
- **Total findings Sprint 2:** 34 + 31 + 22 + 30 + 32 + 32 + 32 + 32 + **30 (S2.9)** = **275 findings**
- **Sprint 2 budget:** $0.00 / $25.00 ceiling ($0.00 / $50.00 combined Sprint 2+3 ceiling). S2.3 $18 pre-auth never used; rescoped to Sprint 3 L3 fact-check gate build.
- **Sprint 2 scope-breakers surfaced:** 0 hard, 1 soft (S2.9 "Sprint 3 should be scoped around unblocking Sprint 5 + first-impression credibility, not around closing every P0 by raw count" — operationalized in synthesis.md)

**Next:** `synthesis.md` — prioritized, deduped Sprint 3 backlog.
