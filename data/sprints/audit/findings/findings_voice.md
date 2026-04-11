# S2.6 — Voice / Editorial Audit Findings

**Sprint:** 2 (Audit)
**Session:** 6 of ~9 (S2.6)
**Expert hat:** Voice — editorial correctness, prompt discipline, cliché density, confabulation resistance
**Date:** 2026-04-11
**Model:** Opus 4.6 inline (ratified pattern, see `memory/feedback_opus_inline_reasoning.md`)
**Budget:** $0 actual (0 API calls)
**Method:** Read `docs/VOICE.md` as yardstick → read all enrichment prompts (4 reference scripts + `enrich_prompts.py` + live `enrich-wine` edge function via MCP) → query DB for inventory + stratified samples + voice-pattern LIKE scans on the full corpus → triangulate with prior findings (S2.3 F10/F14, S2.4 F18, S2.5 F4/F5).

**Corpus audited:**
- `wine_insights` — 5,108 rows (46 Grade B, 5,062 Grade C)
- `wine_vintage_tasting_insights` — 5,164 rows (sensory grids, not prose; spot-checked)
- `region_insights` — 202 rows
- `appellation_insights` — 82 rows (100% US AVAs)
- `country_insights` — 62 rows
- `grape_insights` — 0 rows (never run)
- `wine_food_pairings` (structured table) — 0 rows (archive has 809)
- Prompt files: `pipeline/enrich/enrich_prompts.py` (new, tightened), `pipeline/enrich/appellation_insights.py`, `region_insights.py`, `country_insights.py`, `grape_insights.py`, `supabase/functions/enrich-wine/index.ts` (live, via MCP `get_edge_function`)

---

## Headline

Voice quality is a **two-prompt problem compounded by a confabulation problem**. Loam has ONE tightened voice-rules block — the `VOICE_RULES_BLOCK` in `pipeline/enrich/enrich_prompts.py` — and it only governs Grade B/C wine enrichment. The four reference-layer enrichment scripts (appellation/region/country/grape) and the deployed `enrich-wine` edge function are all on a strictly weaker voice baseline: an 8-word marketing blocklist, no hedging/sommelier-theater/performative-enthusiasm rules, and stale `claude-sonnet-4-20250514` model IDs. Result: **346 reference-insight rows (202 regions + 82 appellations + 62 countries)** were all written on 2026-03-06 with the weak prompts, and **5,108 wine_insights rows** were all written on 2026-04-10 through a mix of the tightened wine prompts (Grade C via enrich_prompts.py) and the weak edge-function prompt (Grade B). All of it is feeding a **contamination feedback loop**: Knights Valley appellation_insight (2026-03-06) confabulates "volcanic soils from ancient Mayacamas eruptions" → Beringer Alluvium Grade B wine_insight (2026-04-10) inherits the claim via the edge function's `appellationInsight` context slot → extends it with a different wrong volcano ("Mount St. Helena eruptions"). The audit found 46 Grade B wines where **41% are tagged "premium" and 59% contain "likely"** (e.g. DRC Corton-Charlemagne Grade B says *"They avoid malolactic fermentation in most vintages"* — factually wrong) and **487 wine_insights rows** built from S2.3 F2's Chardonnay+Pinot Blanc polluted grape pair, where Claude invents rationales (e.g. Waterbrook Icon: *"100% Chardonnay with 75% Pinot Blanc — an unusual high-proportion blend"*, rationalizing a 175% total into editorial narrative). **No content currently in the DB is safe to carry into Sprint 5 without regeneration.** The Grade B edge function's Session-10 feature flag is still correct.

**Three cross-cutting patterns:**
1. **Prompt drift** — `enrich_prompts.py` is the tightened source of truth; 5 other prompt locations (4 reference scripts + edge function) are behind it. Voice quality is bimodal by prompt origin, not by model.
2. **Voice rules ≠ confabulation rules.** Tightening the hedging blocklist reduced "may" and "appears to" but did not prevent Grade C hooks from inventing 175% blends, wrong grape backstories, or non-existent wines (Schramsberg J Schram described as *"a still rosé from a sparkling house"* — J Schram is Schramsberg's top-tier sparkling).
3. **Reference insights are US-biased and structurally formulaic.** 82/82 appellation_insights are US AVAs (zero Chambertin/Barolo/Champagne/Rioja/Chablis/Burgundy grand crus). Within those 82, **71% of soil profiles say "well-draining"**, **62% say "ancient"**, **60% say "volcanic"** (many incorrectly), and **20% use the "force vines to struggle/dig deep" template**. VOICE.md rule 1 is "Be specific" — the reference corpus uses a template masquerading as specificity.

---

## Severity scale

- **P0** — broken or user-visible correctness issue; must fix before regenerating any content
- **P1** — significant risk; must fix before Sprint 5 runs at scale
- **P2** — improvement; not blocking
- **P3** — nice to have

Effort: `trivial` (< 15 min), `small` (1-2 hours), `medium` (half day), `large` (multi-session).

---

## P0 findings (9)

### F1 — Reference-layer enrichment prompts are materially weaker than wine-layer (346 rows affected)

**Severity:** P0
**Effort:** small (rewrite 4 prompt files against the `enrich_prompts.py` VOICE_RULES_BLOCK shape)
**Evidence:**
- `pipeline/enrich/appellation_insights.py:33-36`, `region_insights.py:32-35`, `country_insights.py:31-34`, `grape_insights.py:35-38` — all 4 use an 8-word `BANNED_WORDS` list: `["prestigious", "world-class", "exceptional", "unparalleled", "legendary", "iconic", "finest", "renowned"]`.
- `pipeline/enrich/enrich_prompts.py:41-59` — the wine-layer `VOICE_RULES_BLOCK` adds 15 hedging words, 20 sommelier-theater phrases, 10 generic-filler words, and a performative-enthusiasm block. None of this is in the reference-layer prompts.
- LIKE scan across the 346 reference-insight rows confirms the gap:
  - `region_insights`: 74/202 (37%) contain "elegant/elegance", 51/202 (25%) contain "typically", 34/202 (17%) contain "remarkable", 26/202 (13%) contain "showcases", 55/202 (27%) contain revival trope ("new generation / revolution / renaissance / reborn")
  - `appellation_insights`: 41/82 (50%) contain "elegant/elegance", 37/82 (45%) contain "typically", 25/82 (30%) contain "remarkable"
  - `country_insights`: 27/62 (44%) contain "elegant/elegance", 14/62 (23%) contain "remarkable"
- Representative hit (Burgundy region `ai_signature_style`): *"The Pinot Noirs capture something ethereal — they're simultaneously delicate and profound, with that distinctive earthy forest floor character layered beneath red fruit. These wines age not by adding weight but by gaining complexity, revealing new facets decade after decade."* — VOICE.md "Don't Do This" banned categories: overly poetic ("ethereal", "simultaneously delicate and profound"), vague ("revealing new facets"), says nothing concrete about Pinot Noir.
- Representative hit (Tuscany region `ai_history`): *"Wine flows through Tuscan history like blood through veins"* — VOICE.md explicitly bans simile/metaphor drift of exactly this shape.

**Proposed fix:** Rewrite the 4 reference prompts against a shared `VOICE_RULES_BLOCK` imported from a new `pipeline/lib/voice.py` module (or extend `enrich_prompts.py`). Include the full hedging + sommelier-theater + generic-filler + performative-enthusiasm blocklists. Add a retrieval-grounded "NEVER INVENT" block (see F2, F5). After the rewrite, ALL 346 reference rows need to be regenerated — this is not a fix-in-place problem, the existing prose is too polluted.

---

### F2 — `enrich-wine` edge function ships a weak voice prompt (wine-layer Grade B regression)

**Severity:** P0
**Effort:** small (vendor the edge function source into git per S2.5 F31, align to `enrich_prompts.py`, redeploy)
**Evidence:**
- Live edge function source (read via MCP `get_edge_function` on `enrich-wine`, version 3, `verify_jwt: true`, currently feature-flagged OFF per Session 10 audit):
  - System prompt preamble (`buildPrompt()`): *"You are a wine expert writing for Loam... Be specific (name soils, climate patterns, geological formations). Connect place to taste. Have a point of view. No generic filler, no sommelier theater, no vague hedging."*
  - **Zero banned-word list, zero NEVER INVENT block, zero retrieval-grounded discipline.**
  - Hardcoded model: `"claude-sonnet-4-20250514"` (stale — S2.5 F5 cross-reference).
- Compare to `pipeline/enrich/enrich_prompts.py:41-59` — the tightened wine block has 15 hedging words, 20 sommelier-theater phrases, and a structured NEVER INVENT section.
- The edge function's Grade B output matches the weak prompt, not the tightened one:
  - 46 Grade B wines, enriched_at 2026-04-10 via enrichment_log `prompt_template: "enrich-wine-v1"`, `model: "claude-sonnet-4-20250514"` (108 calls total incl. 3 errors).
  - LIKE scan hits: 27/46 (59%) "likely", 24/46 (52%) "showcases", 19/46 (41%) "premium", 18/46 (39%) "elegant", 13/46 (28%) "typically", 12/46 (26%) "suggests", 7/46 (15%) "harmonious" — every one of these is in the `enrich_prompts.py` banned list.
  - Representative DRC Corton-Charlemagne Grade B `ai_vinification_summary`: *"DRC ferments in a mix of new and used Vosges oak barrels, with extended lees aging lasting 18-20 months to build texture... They avoid malolactic fermentation in most vintages to preserve the limestone-driven acidity, though the 2009 vintage **likely** saw partial malo due to the year's high ripeness levels."* — MLF claim is factually wrong (DRC does MLF on Corton-Charlemagne), "likely saw partial malo" is pure confabulation.
  - Rosenblum Continente Zinfandel Grade B: *"The blending of Primitivo with other varieties (**likely** Petite Sirah or Carignane, common in Rosenblum's arsenal)"* — inventing a blend Loam has no data for.
- Separately: the edge function reads `grapes.name` (VIVC "CHARDONNAY BLANC") not `grapes.display_name` ("Chardonnay"), per S2.5 F4. Every Grade B prompt also inherits wrong grape labels.

**Proposed fix:** Sprint 3 pre-req. (a) Vendor `supabase/functions/enrich-wine/index.ts` source into git (S2.5 F31). (b) Replace the system prompt preamble with the `enrich_prompts.build_grade_b_prompt()` output assembled client-side from a facts packet (Edge function becomes a thin router; the prompt construction moves to a shared Deno/Python module, or the edge function imports from a shared file). (c) Switch model to `claude-sonnet-4-6` via the F5/S2.5 model registry. (d) Switch grape read to `display_name` (S2.5 F4). (e) Keep the Session-10 feature flag OFF until L3 fact-check gate lands (F4).

---

### F3 — Tightened voice rules do not prevent factual confabulation (Grade C hook regressions)

**Severity:** P0
**Effort:** medium (retrieval-grounded facts packet + L3 fact-check gate — Session 10 audit plan)
**Evidence:** Grade C used the tightened `enrich_prompts.py` prompt (per journal.md S12 reference) which is objectively stronger on voice — hedging dropped from 59% (Grade B) to 2% (Grade C) on "likely", and showcases 1% vs 52%. But factual confabulation DID NOT drop. Receipts from random Grade C sample (SQL via `ORDER BY RANDOM() LIMIT 5`):

| Wine | Field | Confabulation |
|---|---|---|
| Schramsberg J Schram | `ai_hook` | *"Schramsberg's J Schram is a **still rosé** from a sparkling house..."* — **J Schram is Schramsberg's top-tier sparkling wine, not a rosé.** Wine does not exist as described. |
| Pax Obsidian Syrah, Knights Valley | `ai_hook` | *"This is high-alcohol (**likely 14.5%+**)... pairs Syrah with **Durif (a Rhône outlier rarely seen in California)**"* — Durif IS Petite Sirah, extremely common in California, not a "rarely seen" outlier. "likely 14.5%+" is the exact Session-10 confabulation pattern (invented ABV). |
| Vite Colte La Bella, Piemonte Passito | `ai_hook` | *"**Muscat of Alexandria** dried on the vine"* — Piedmont Passito is typically Moscato Bianco, not the Egyptian table grape Muscat of Alexandria. |
| Merry Edwards Meredith Pinot Noir, RRV | `ai_hook` | *"Merry Edwards has spent **40 years** refining Pinot Noir"* — founded 1997, ~30 years as of 2026, not 40. |
| Heitz Cellar Linda Falls Cab, Howell Mtn | `ai_hook` | *"sits at **1,400–2,200 feet** on Howell Mountain's eastern slopes, where **volcanic ash and red clay loam soils drain fast**"* — specific elevation and soil series not in Loam's facts packet. |
| Famille Perrin CdP Les Sinards | `ai_hook` | *"This is a **white wine** from Châteauneuf-du-Pape masquerading under red grape varieties — **Garnacha Tinta, Syrah, and Monastrell are normally fermented dark, but Famille Perrin harvests them early and presses them immediately** to capture the fresh, mineral side of southern Rhône."* — confabulating a winemaking backstory. Root cause: Spanish grape names on a French CdP (S2.4 F14 — `grapes.name` uses Spanish forms), Claude rationalizes instead of flagging. |

**Same Session-10 finding (2026-04-10) the edge function feature flag was raised to address.** Tightened voice rules address surface-level tells (hedging, marketing words) but not the underlying "confidently fill gaps" behavior.

**Proposed fix:** Retrieval-grounded facts packet + mandatory `build_retry_prompt()` L3 fact-check gate (already scaffolded in `enrich_prompts.py:163-192`). L3 pass MUST run before any write, and MUST retry-with-corrections rather than save on first L3 failure. Session 10 audit plan already drafted; Sprint 3 F10 (the rolled-forward $18 S2.3 pre-auth) is the natural landing place — **upgrade the pre-auth to "L3 fact-check gate" rather than "re-fact-check existing prose"**, since the existing prose will be regenerated anyway.

---

### F4 — S2.3 F10 confabulation chain contaminates 487 Chardonnay+Pinot Blanc wines with invented narratives

**Severity:** P0
**Effort:** trivial (the fix is Sprint 3 grape repair 3a-3d + content regeneration — the finding is a scope measurement)
**Evidence:**
- 487 wines with `display_name ILIKE '%chardonnay%'` have BOTH a `wine_insights` row AND a `PINOT BLANC` link in `wine_grapes` (from S2.3 F2 / S2.5 F2 multi-COLA collapse bug).
- Claude does NOT flag the contradiction — it invents rationales. 3 receipts:

| Wine | Grapes DB | Hook (confabulation bolded) |
|---|---|---|
| Waterbrook Icon Chardonnay, Walla Walla | CHARDONNAY BLANC, PINOT BLANC | *"Waterbrook's Icon Chardonnay blends **100% Chardonnay with 75% Pinot Blanc — an unusual high-proportion white blend** that softens oak influence..."* — **175% total rationalized into editorial narrative.** |
| Ceritas Porter-Bass Chardonnay, Sonoma Coast | CHARDONNAY BLANC, PINOT BLANC | *"Ceritas **blends Chardonnay with Pinot Blanc to chase salinity and tension over richness — a deliberate restraint that reads as intelligence rather than timidity**"* — pure invented backstory. Ceritas doesn't blend Pinot Blanc into Porter-Bass Chardonnay. |
| Kiona Vineyards Chardonnay, Columbia Valley | CHARDONNAY BLANC, PINOT BLANC | (hook doesn't mention Pinot Blanc but the wine page will show the polluted grape linkage) |

- Rappahannock Black Label Chardonnay hook from earlier sample: *"The blend with Pinot Blanc — unusual for a Chardonnay bottling — **suggests an experiment in acid preservation and texture**"* — same pattern, different wine.
- This is the exact S2.3 F10 finding manifesting at scale: *"AI content confabulates narratives when data is wrong"*. 487 wines is ~9.5% of the 5,108-row enriched corpus.

**Proposed fix:** Sprint 3 sequence (from S2.3/S2.4/S2.5 synthesis) must run BEFORE content regeneration: (3a) grapes.name cleanup → (3b) synonym collision resolution → (3c) varietal_categories fix → (3c.5) batch_pipeline multi-COLA collapse → (3c.6) ttb_grape_promote DISTINCT ON → (3c.7) consolidate resolvers → (3d) re-run grape resolver against wine_grapes. Only then regenerate. If content is regenerated BEFORE the grape repair, the 487 rows will inherit the polluted grape labels and Claude will invent new rationales.

---

### F5 — Contamination feedback loop: confabulated reference insights become context for wine insights

**Severity:** P0
**Effort:** small (fix is structural — tighten reference prompts + add retrieval grounding + regenerate in Sprint 5 order: reference first, THEN wines)
**Evidence:** The `enrich-wine` edge function's `assembleContext()` reads `appellation_insights.ai_terroir, ai_climate, ai_style` and `region_insights.ai_terroir, ai_climate` and injects up-to-400-character slices into the wine prompt as "Appellation Context" and "Region Context" sections (see live source, `buildPrompt()` lines). **Grade B wine enrichment INHERITS reference-layer prose as authoritative context.**

Traced one concrete chain end-to-end:

1. **Knights Valley `appellation_insights.ai_soil_profile`** (enriched 2026-03-06, confidence 0.8):
   *"**Volcanic soils dominate the valley floor and lower slopes, derived from ancient Mayacamas Mountain eruptions**, providing excellent drainage and mineral complexity. Higher elevations reveal fractured volcanic rock and red clay deposits..."*
   — **Geologically wrong.** Knights Valley floor soils are largely alluvial/sedimentary. "Mayacamas Mountain eruptions" is factually wrong (the Mayacamas range is primarily Franciscan Complex sedimentary with minor volcanic intrusions; not the eruption source).

2. **Beringer Alluvium `wine_insights.ai_wine_summary`** (Grade B, enriched 2026-04-10, confidence 0.85):
   *"Beringer's Alluvium draws from Knights Valley's unique position... The wine's name references the valley's alluvial deposits mixed with **volcanic soils from ancient Mount St. Helena eruptions**."*
   — Inherits the volcanic claim from the appellation insight. Substitutes a DIFFERENT wrong volcano (Mount St. Helena, not Mayacamas). The wine summary even contradicts itself: "alluvial deposits mixed with volcanic soils from ancient eruptions."

3. **Beringer Alluvium `ai_terroir_expression`**: *"Knights Valley's volcanic soils and clay loam base provide excellent drainage..."* — reinforces the claim a third time.

**This is the S2.3 F14 "Hunter Valley volcanic" pattern but confirmed structural, not prose-only.** Also reinforces S2.4 F18 (Hunter Valley → Basalt link) finding — the structured reference layer IS the vehicle for prose confabulation.

**Additional hits found in the same pass:**
- Russian River Valley `ai_soil_profile`: *"hillside sites reveal weathered sandstone and **volcanic ash from ancient eruptions**"* — RRV is Wilson Grove Formation sandstone and Goldridge loam; not volcanic ash.
- Sonoma Coast `ai_soil_profile`: *"marine sediments mixed with **volcanic intrusions from the region's geological past**"* — Sonoma Coast is Franciscan sedimentary/metamorphic; no notable volcanics.
- Howell Mountain `ai_soil_profile`: *"ancient volcanic bedrock, **primarily tufa and obsidian**"* — "tufa" is a sedimentary limestone deposit, not volcanic (Claude meant "tuff"); obsidian is not a dominant Howell Mountain rock type. Howell IS volcanic (tuff/rhyolite/basalt) but the specific minerals named are wrong.
- Across 82 appellation_insights, **49/82 (60%) mention "volcanic"** in `ai_soil_profile`. Realistically ~20 of these AVAs are majority-volcanic.

**Proposed fix:** (a) Reference prompts must gain the same NEVER INVENT block + L3 fact-check gate as the wine prompts. (b) Sprint 5 order is locked: reference-layer regeneration MUST precede wine-layer regeneration. (c) Add a `soil_source_text` provenance field on `appellation_soils` (overlap with S2.4 F17) so reference content can ground in structured data rather than training knowledge.

---

### F6 — `grape_insights` table is empty despite being the best-written reference prompt in the codebase

**Severity:** P0
**Effort:** large (requires Grade-B-quality retrieval + L3 gate; but a v1 could run with existing prompt on top 200 grapes for ~$30)
**Evidence:**
- DB count: `SELECT COUNT(*) FROM grape_insights` = 0 rows.
- `pipeline/enrich/grape_insights.py:63-75` contains the **best food-pairing system prompt in the entire codebase**, matching VOICE.md section "Food Pairings" almost rule-for-rule:
  > *"ai_food_pairing (3-5 sentences): What to eat with wines from this grape. Follow these rules strictly: Start with classic/traditional pairings — they exist for a reason. Name specific dishes and cuisines (Thai, Mexican, Korean, Southern US, Japanese, etc.). Cover the full range — a Tuesday night meal AND a Saturday dinner where it fits. Explain the flavor logic briefly (why the pairing works: acid cuts fat, tannin matches protein, etc.). No sommelier theater — no 'pairs beautifully with a delicate...' Just name the food. No generic cop-outs like 'pairs well with grilled meats and seafood.'"*
- This is the only food pairing prompt in Loam that translates VOICE.md's rules into concrete structural guidance. `enrich_prompts.py` GRADE_B_FIELDS has a ONE-LINE food pairing spec: *"3-5 specific dishes with brief reasoning. Real food, not 'delicate stone fruit and charcuterie'."*
- But grape_insights was never run. Frontend GrapePage has to fall back to fact-grid display only — no narrative, no food pairings, no regional-style comparisons.

**Proposed fix:** (a) Sprint 5 should run grape_insights on top 200-500 grapes (covers ≥95% of wine_grapes link volume). (b) Before running, harmonize grape_insights.py's prompt against the new tightened VOICE_RULES_BLOCK (import from shared voice module per F1 fix). (c) Because grape_insights.py already has the food-pairing discipline, **port its food-pairing field spec upstream into `enrich_prompts.py` GRADE_B_FIELDS** — it's the right template for every food-pairing surface in the product.

---

### F7 — Grade C (5,062 wines, 99% of the corpus) has no food pairing output at all

**Severity:** P0
**Effort:** trivial (extend Grade C field schema; cost minimal since Haiku is cheap)
**Evidence:**
- `pipeline/enrich/enrich_prompts.py:77-81` — `GRADE_C_FIELDS` schema is 3 fields: `hook`, `style_profile`, `comparable_wines`. No `food_pairing`, no `cellar_recommendation`, no `terroir_expression`, no `vinification_summary`.
- DB scan: 105/5108 wine_insights rows have non-empty `ai_food_pairing` (46 Grade B + 59 legacy Grade C). The other **5,003 of 5,062 Grade C rows have NULL food pairing**. 98.8% of enriched wines have no food-pairing prose.
- VOICE.md Food Pairings section: *"These rules apply everywhere food pairing content appears: grape insights, wine insights, any future feature."*
- User-facing consequence: every Grade C wine page has a Pairings slot that renders empty or falls back to fact tiles. Voice guidance is violated by omission.

**Proposed fix:** Add `food_pairing` (3 specific dishes) to GRADE_C_FIELDS. Use the grape_insights-style structured guidance (classics first, name cuisines, flavor logic, banned patterns). Estimated Haiku cost for retrofit on 5,062 Grade C wines: ~$20-30. Or: defer until Sprint 5 content regeneration, where it gets handled in the same regen pass.

---

### F8 — `wine_food_pairings` structured table is empty; CLAUDE.md is stale by 809 rows

**Severity:** P0
**Effort:** trivial (restore from `archive.wine_food_pairings`)
**Evidence:**
- `SELECT COUNT(*) FROM wine_food_pairings` → **0 rows**.
- `SELECT COUNT(*) FROM archive.wine_food_pairings` → **809 rows** (pre-30K rebuild).
- `CLAUDE.md` claims: *"Food pairings: 809 structured links + 203 text descriptions from Empson"* — stale, wiped in 30K rebuild.
- `enrich-wine` edge function `assembleContext()` queries `wine_food_pairings` to pull "existing food pairings" into the Grade B prompt. The query returns 0 rows, so the "Existing Food Pairings" context section is silently empty for every Grade B wine.
- 58-row `food_categories` taxonomy is intact (restaurant-grade categories: "Aperitif / On Its Own", "Asian Cuisine", "Charcuterie & Cured Meats", "Hard Cheese (Parmesan, Pecorino)", etc.) — the schema is ready, just unpopulated.

**Proposed fix:** (a) Sprint 3 staging relink (S2.2 F1) class — restore `wine_food_pairings` from `archive.wine_food_pairings` using the same producer+wine normalized-key match pattern as the staging relink. The 809 rows are the Empson importer food pairings, which should match cleanly. (b) Update CLAUDE.md to reflect live state. (c) Backfill additional pairings from `source_empson.food_pairings_text` (203 rows exist in the staging column).

---

### F9 — Reference-insight coverage is structurally US-biased (82/82 appellation_insights are US AVAs)

**Severity:** P0
**Effort:** large (Sprint 5 content generation scope expansion)
**Evidence:** SQL break-down:
- `appellation_insights`: 82/82 (100%) United States. Zero Chambertin, zero Barolo, zero Champagne, zero Rioja, zero Chablis, zero Margaux — none of the appellations that drive S2.3's marquee-wine sample.
- `region_insights`: 202 rows spanning 15 countries. Top: Italy 21, France 19, Australia 17, Spain 15, Germany 13, US 12, South Africa 9, Portugal 9, NZ 6, Austria 6, Argentina 6, Switzerland 5, Japan 4, Hungary 3, Czech Republic 3. Missing entirely: Chile, Israel, Lebanon, Greece, Romania, Georgia, and large chunks of the ROW market.
- `country_insights`: 62/68 countries, better coverage.

**The imbalance explains why the S2.3 marquee-wine audit found empty terroir context for so many famous wines.** When DRC Corton-Charlemagne was enriched, the `appellation_insights` lookup for Corton-Charlemagne returned NULL — no reference-layer context was injected into the prompt at all, forcing Claude to freestyle the entire terroir_expression from training knowledge (which is how we got the wrong-MLF Corton-Charlemagne result).

**Proposed fix:** Sprint 5 content generation scope MUST cover: all Burgundy grand crus + premiers crus (≈120 appellations), Bordeaux crus classés communes (≈20), Barolo/Barbaresco MGAs (≈200), Chianti Classico UGA (≈11), Champagne + grand cru villages (17), Rioja + Priorat + Ribera del Duero + Rías Baixas, Mosel + Rheingau grosses gewächs. Estimated ~500 new appellation_insights at ~$0.02/row Sonnet cost = ~$10. Trivial cost, large coverage gain.

---

## P1 findings (14)

### F10 — Old reference prompts hardcode stale `claude-sonnet-4-20250514` (overlaps S2.5 F5)

**Severity:** P1
**Effort:** trivial (shared model registry, Sprint 3 pre-req per S2.5 F5)
**Evidence:** `appellation_insights.py:83`, `region_insights.py:82`, `country_insights.py:79`, `grape_insights.py:92` — all four hardcode `model="claude-sonnet-4-20250514"`. `enrich_prompts.py:29` uses `claude-sonnet-4-6`. Edge function uses the stale ID. Three coexisting model IDs across the enrichment surface.

**Proposed fix:** Overlaps S2.5 F5 — one fix at `pipeline/lib/models.py` kills both findings.

---

### F11 — Reference prompts lack retrieval grounding (no NEVER INVENT block)

**Severity:** P1
**Effort:** small (add shared NEVER INVENT block to reference prompts)
**Evidence:** `enrich_prompts.py:92-106` has a 10-item NEVER INVENT list covering soil types, ABV, vineyard aspects, vinification details, comparable wines, producer history, awards, scores. None of the four reference prompts has any equivalent. The reference prompts contain a "HANDLING UNCERTAINTY" paragraph telling Claude to "write shorter entries" — in practice Claude wrote long confident entries anyway (62% confidence distribution bunched in 0.8-0.9).

**Proposed fix:** Extend the shared voice module (F1 fix) with a NEVER INVENT block tailored to reference content: never invent soil types not in `appellation_soils`, never invent established_year, never invent classification level, never invent quality/medal counts, never invent producer-appellation connections, never invent specific yield/ABV/elevation figures.

---

### F12 — "elegant/elegance" is the dominant default adjective across 15% of the enriched corpus

**Severity:** P1
**Effort:** trivial (add to shared banned list)
**Evidence:** LIKE scan counts:

| Corpus | Total | "elegant/elegance" count | Rate |
|---|---|---|---|
| wine_insights Grade B | 46 | 18 | 39% |
| wine_insights Grade C | 5,062 | 797 | 16% |
| appellation_insights | 82 | 41 | 50% |
| region_insights | 202 | 74 | 37% |
| country_insights | 62 | 27 | 44% |

"Elegant" is not on ANY current banned list — not `enrich_prompts.py` VOICE_RULES_BLOCK, not the 8-word reference list, not VOICE.md. But at these rates it's functioning as a sommelier-theater crutch identical to "pairs beautifully" or "harmonious" (both explicitly banned). 37-50% is the rate at which a word becomes filler.

**Proposed fix:** Add "elegant / elegance / elegantly" to the sommelier-theater banned list. Also consider: "refined", "finesse", "grace/graceful" (not scanned but likely parallel).

---

### F13 — "marry/marries/marriage of" banned phrase leaks through (explicit VOICE.md banned category)

**Severity:** P1
**Effort:** trivial (fix once banned lists are unified)
**Evidence:** LIKE scan:
- `wine_insights` Grade B: 3/46 rows use "marry/marries/marriage" — example Alpha Estate Axia `ai_wine_summary`: *"The Axia blend represents winemaker Makis Mavridis's philosophy of **marrying international techniques with Greek heritage varieties**."* — classic sommelier theater.
- Grade C: 5/5062 (low but present). Italy `country_insights.ai_signature_styles`: *"Nebbiolo creates Italy's most age-worthy reds in Barolo and Barbaresco — wines that **marry roses and tar** with decades of evolution ahead."*
- VOICE.md explicitly bans this under "sommelier theater". `enrich_prompts.py:45` explicitly bans "marriage of" in the VOICE_RULES_BLOCK.

**Proposed fix:** The phrase is already banned in `enrich_prompts.py` but still leaks through — meaning either (a) the validator is warning-only (F28 cross-reference), or (b) the Grade B output came from the edge function which doesn't use `enrich_prompts.py`. Fix is the same: unify banned lists under shared module, run validator at write time with REJECT semantics not WARN.

---

### F14 — Grade B wines are 52% "showcases" (vs 1% Grade C) — confirms edge function did not use `enrich_prompts.py`

**Severity:** P1
**Effort:** trivial (derivative of F2 fix — once edge function aligns to shared prompts, this closes)
**Evidence:** LIKE scan:
- Grade B (edge function): 24/46 (52%) contain "showcases"
- Grade C (`enrich_prompts.py`): 53/5062 (1.0%) contain "showcases"
- `enrich_prompts.py:47` explicitly bans "showcases" in the generic-filler block
- The 50x rate delta is the cleanest signal that **the edge function and `enrich_prompts.py` are two different prompt systems** and the edge function is the weaker one

**Same pattern for "premium":**
- Grade B: 19/46 (41%)
- Grade C: 195/5062 (3.9%)
- `enrich_prompts.py:47` explicitly bans "premium"
- 10x rate delta — same root cause

**Proposed fix:** Closes automatically when F2 is fixed.

---

### F15 — 59% of Grade B wines contain "likely" — validates Session-10 feature-flag decision

**Severity:** P1
**Effort:** N/A (finding is a restoration of Session 10 audit evidence in current data)
**Evidence:** LIKE scan, Grade B: 27/46 (59%) of wines contain "likely". `enrich_prompts.py:43` explicitly bans "likely" as the #1 hedging word. The Grade B edge function's prompt does not ban it.

Representative: Penley Estate Phoenix Coonawarra `ai_vinification_summary`: *"The 75% Cabernet Sauvignon base **likely** includes small amounts of Merlot or Cabernet Franc for softening, **typical** of quality Coonawarra blends seeking..."* — two bans in one clause.

**This is the Session 10 audit finding.** The feature flag on the edge function (`ENRICHMENT_ENABLED=false`) is the only thing preventing this content from multiplying. S2.6 confirms: **the flag is still correct**. Do not flip it until F2 (prompt alignment) + F3 (L3 gate) + F4 (grape repair) all land.

---

### F16 — Grade C "suggests" usage is 226/5062 (4.5%) — tightened rules reduced but did not eliminate

**Severity:** P1
**Effort:** small (prompt refinement)
**Evidence:** LIKE scan:
- Grade B: 12/46 (26%) contain "suggests"
- Grade C: 226/5062 (4.5%) contain "suggests"
- `enrich_prompts.py` banned list does NOT include "suggests/suggest" (banned: may/tends to/appears to/seems to/might/could be/likely/possibly/often/typically/generally/usually/somewhat/fairly/rather — 16 hedging words, "suggests" not among them)
- Semantically identical to "likely" in enrichment context: *"The blending of Primitivo with other varieties (likely Petite Sirah or Carignane, common in Rosenblum's arsenal) **adds** backbone"* vs *"The 175% blend **suggests** an unconventional co-fermentation"*

**Proposed fix:** Add "suggests/suggest/suggesting" to the hedging banned list. Same fix extends: "implies", "hints at", "points toward".

---

### F17 — Reference insights are formulaic soil-profile templates ("force vines to struggle", "ancient", "well-draining")

**Severity:** P1
**Effort:** medium (regenerate reference content with retrieval grounding, not just prompt tightening)
**Evidence:** LIKE scan across 82 appellation_insights `ai_soil_profile`:

| Pattern | Hits | Rate |
|---|---|---|
| "well-draining" | 58/82 | 71% |
| "ancient" | 51/82 | 62% |
| "volcanic" | 49/82 | 60% |
| "force [vines/roots] to [struggle/dig deep/go deep]" | 16/82 | 20% |
| "iron-rich" | 12/82 | 15% |
| "decomposed granite" | 12/82 | 15% |
| "mineral backbone" | 10/82 | 12% |

VOICE.md rule 1: "Be specific. Name the soil type. Name the climate pattern. Name the geological formation." The reference content DOES name soil types, but it reuses a formulaic template: **"ancient [something], well-draining, force vines to struggle, mineral backbone"**. When 71% of soil profiles use "well-draining" and 62% use "ancient", the specificity is undifferentiated — it doesn't teach the reader anything about WHY this soil differs from the next soil.

Representative (Atlas Peak): *"The appellation sits on ancient volcanic deposits from the Vaca Mountains, primarily Hambright and Forward series soils derived from weathered volcanic ash and tuff. These **well-draining, iron-rich soils force vines to struggle**, producing small berries..."* — all four template markers in one sentence.

Representative (Fort Ross-Seaview): *"with Franciscan formation creating a complex mix of sandstone, shale, and **volcanic** materials weathered by millennia of coastal storms. **Goldridge sandy loams** dominate many sites, offering exc..."* — fake volcanic again (Fort Ross is largely Franciscan sandstone).

**Proposed fix:** Reference content regeneration must ground in a soil provenance text field (see S2.4 F17 appellation_soils schema extension). Prompt should require: name the PARENT ROCK + dominant soil SERIES + WHY it differs from the next appellation down-slope. Ban the formulaic markers in the shared VOICE_RULES_BLOCK: "well-draining" (always say what it drains; if it doesn't drain, say clay), "ancient" (always say the epoch or age), "force vines to struggle" (explicit ban — it's the reference-layer equivalent of "pairs beautifully").

---

### F18 — Region insights fall into "modern revival" trope (55/202 = 27%)

**Severity:** P1
**Effort:** small (add trope to banned list)
**Evidence:** LIKE scan across 202 region_insights: 55/202 (27%) use "new generation" / "revolution" / "renaissance" / "reborn" in `ai_history`. Examples:
- Italy: *"a handful of producers broke from tradition to prove Italian wine could compete globally, sparking both the Super Tuscan **revolution** and a **renaissance** in ancient varieties"*
- Germany: *"German winemaking underwent a **revolution** in the 1980s and 90s, led by producers who championed dry styles..."*
- Austria: *"**Rather than collapse, this crisis sparked a complete quality revolution**"*

The revival narrative is TRUE in some regions (Austria 1985, German 1970s dry revolution) but using the same trope for 27% of regions dilutes all of them into cliché. VOICE.md: *"Every sentence should teach the reader something or help them make a decision."* When every region's history is "ancient → catastrophe → new generation → renaissance", no region has a distinctive history.

**Proposed fix:** Add to banned tropes. Require reference prompts to name a specific year + specific producer + specific turning point. If no specific turning point exists, state so and move on.

---

### F19 — Formulaic food pairing format: 100% of prose uses "{dish} — {flavor logic}" em-dash template

**Severity:** P1
**Effort:** trivial (improve Grade B food pairing field spec to match grape_insights rules)
**Evidence:** 105/105 wines with `ai_food_pairing` use the exact "em-dash-then-reasoning" structural template. LIKE scan on the food pairing field:
- 75/105 (71%) use "cuts through"
- 52/105 (50%) use "balance"
- 43/105 (41%) use "complements"
- 0/105 use any other rhetorical pattern (no "works because", no "mirrors", no "lifts", no "stands up to" as independent structure)

Every pairing reads like a template. Voice/form follows prompt: `enrich_prompts.py` GRADE_B_FIELDS `food_pairing` spec is **18 words**: *"3-5 specific dishes with brief reasoning. Real food, not 'delicate stone fruit and charcuterie'."* No structural guidance → Sonnet defaults to one pattern and fills in.

Compare to `grape_insights.py:66-72` (the best food-pairing spec in Loam): 7 bullet rules totaling ~130 words covering classics-first, naming cuisines, full table range, flavor logic, banned patterns, vegetarian naturally, no cop-outs.

**Proposed fix:** Port the `grape_insights.py` food-pairing field spec into `enrich_prompts.py` GRADE_B_FIELDS and GRADE_C_FIELDS (per F7). Also adopt it in the shared voice module.

---

### F20 — Confabulated-narrative response to bad facts is not prevented by any prompt rule

**Severity:** P1
**Effort:** small (add to NEVER INVENT block + require "FACTS_PACKET_INCONSISTENT" escape hatch)
**Evidence:** Receipts from F3 + F4 demonstrate Claude's consistent response to contradictory facts-packet data:

| Bad fact in packet | Claude's response |
|---|---|
| Chardonnay wine linked to 175% blend (CHARDONNAY BLANC + PINOT BLANC) | Invents "unusual high-proportion white blend" narrative (Waterbrook Icon) |
| Chardonnay wine with PINOT BLANC link | Invents "deliberate restraint that reads as intelligence rather than timidity" (Ceritas) |
| Spanish grape names on French CdP | Invents winemaker harvests-early-to-press-immediately backstory (Perrin Les Sinards) |
| 185% grape percentages (Ecco Domani) | Flags as "data error or unconventional co-fermentation" — **only one of the sample that handles it honestly** |

The "flag as data error" response happened ONCE out of 5 confabulation hits. VOICE.md tells Claude to be specific and have a point of view, but never tells Claude what to do when the facts packet is internally contradictory. Default behavior is to smooth the contradiction into narrative.

**Proposed fix:** Add to the shared NEVER INVENT block: *"If the facts packet contains a contradiction (e.g. grape percentages > 100%, producer country differs from appellation country, wine_type differs from grape color), write 'The source data for this wine contains a contradiction that Loam has not yet resolved.' Do NOT rationalize the contradiction into narrative."* — add an explicit escape hatch so L3 fact-check gate (F3 fix) can detect and requeue.

---

### F21 — `appellation_insights.ai_soil_profile` applies "volcanic" narrative to ~20 wrong AVAs

**Severity:** P1
**Effort:** trivial (derives from F5 fix — retrieval-ground the soil prompt in `appellation_soils`)
**Evidence:** See F5 receipts. 49/82 appellation soil profiles mention volcanic. Only ~20 of the US AVAs in the corpus are majority-volcanic (Howell Mountain, Diamond Mountain, Atlas Peak, Mount Veeder, parts of Sonoma, some Oregon). The remaining ~29 volcanic claims are either wrong (RRV, Sonoma Coast, Knights Valley valley floor) or misattributed (inventing volcanic components for mostly-alluvial valleys).

This is the PROSE layer of the same pattern S2.4 F18 identified in structured `appellation_soils` data (Hunter Valley → Basalt). Both layers were generated on the same date (2026-03-06) from different sources (prose from `appellation_insights.py` prompts, structured from `haiku_appellation_soils.py`). Both ran on Claude training knowledge without cross-checking against each other or primary sources.

**Proposed fix:** Closes together with F5. Soil prompts must read `appellation_soils` (post S2.4 fix) as authoritative input, not generate soil content freehand.

---

### F22 — Grade C schema drops 5 of 8 narrative fields (terroir, vinification, food, cellar, comparable prose)

**Severity:** P1
**Effort:** small (design decision — either lock in the thin schema or widen it)
**Evidence:**
- `enrich_prompts.py:77-81` GRADE_C_FIELDS = 3 fields: `hook`, `style_profile`, `comparable_wines`
- Edge function / Grade B schema = 8 fields + sensory grid (hook, wine_summary, terroir_expression, vinification_summary, food_pairing, comparable_wines, style_profile, cellar_recommendation)
- 5,062 Grade C wines are missing: terroir_expression, vinification_summary, food_pairing, cellar_recommendation, and the rich wine_summary
- VOICE.md has a whole section on "Confidence and Honesty" — the right move is NOT to cut fields, it's to shorten fields and flag uncertainty. The current Grade C schema says "thin data → thin narrative" but truncates narrative topics that don't need thin data (cellar recommendations, basic food pairings, and vinification at the appellation-rule level are all deriveable without producer-specific facts).

**Proposed fix:** This is a product/voice design decision. Two options:

- **Option A — Widen Grade C** to include shortened versions of food_pairing, cellar_recommendation, terroir_expression. Add retrieval grounding so thin data → shorter narrative, not no narrative. Est cost: ~$15-20 retrofit on 5,062 wines at Haiku, or defer to Sprint 5 regen pass.
- **Option B — Tag Grade C as identity-shell only**, show only fact grids on the frontend for Grade C wines, reserve narrative for Grade B. Frontend change only, zero regen cost. Trades product depth for quality.

**Recommendation:** Option A with retrieval grounding, as part of Sprint 5. Reserves Option B only if L3 fact-check gate proves the facts packet isn't ground-truthed enough for safe Grade C narrative.

---

### F23 — Hyperbolic marketing language is unchecked at country-insight level (France "gold standard", Italy "ancestral homeland")

**Severity:** P1
**Effort:** trivial (extend F1 banned list)
**Evidence:** Representative country_insights `ai_overview` openings:
- France: *"France is **wine's eternal reference point**, the country that **established most of the world's fundamental wine concepts** and whose regions **remain the gold standard** for their respective styles... **Every serious wine conversation eventually circles back to what the French figured out centuries ago**."*
- Italy: *"Italy is winemaking's **ancestral homeland**, where every region tells a different story... This is a country that **thinks in villages rather than brands**, where **the concept of terroir was lived long before it was named**."*
- Germany: *"Germany produces some of the world's **most precise and expressive wines**"* + *"creating wines of **remarkable acidity and mineral clarity**"*

All of these are explicitly banned category "Generic filler" per VOICE.md: *"'producing wines of remarkable quality' — says nothing. Replace with specific information or cut entirely."* But since they're not exact-word matches to any existing banned list, the BANNED_WORDS validator in `country_insights.py:107-129` passed them.

**Proposed fix:** Extend the shared banned list with phrase patterns: "gold standard", "spiritual home", "ancestral homeland", "eternal reference", "wine's [x]", "every serious wine conversation", "thinks in villages", "heart of [wine/winemaking]".

---

## P2 findings (7)

### F24 — `BANNED_WORDS` validators log warnings but don't reject writes (all 4 reference scripts)

**Severity:** P2
**Effort:** small (change validator to raise on banned-word hits + require regeneration)
**Evidence:** `appellation_insights.py:134`, `region_insights.py:132`, `country_insights.py:128`, `grape_insights.py:146` — all 4 scripts call `validate_response()` which returns a warnings list, which prints WARN and **still writes the row** (see `write_insight()` call at line ~311). There is no "reject and retry" path. Grade B edge function has zero voice validation before write.

**Proposed fix:** Change validator semantics: banned-word hits → raise → one retry → if second response still fails → mark row as `enrichment_status='voice_reject'` and skip. Add `voice_violations` JSONB column on insight tables so audits can track which rows got flagged.

---

### F25 — Partial-coverage marquee producers (DRC has 1/3 wines enriched, 2 with NULL)

**Severity:** P2
**Effort:** trivial (finding is a scope observation; fix is Sprint 5 coverage targeting)
**Evidence:** Query for DRC wines with insights:
- Corton-Charlemagne Grand Cru — enriched, but with wrong MLF claim
- Echezeaux Grand Cru — `ai_terroir_expression: NULL`, `ai_vinification_summary: NULL`
- La Tâche Grand Cru — `ai_terroir_expression: NULL`, `ai_vinification_summary: NULL`

Sample included Echezeaux/La Tâche via enrichment but wrote empty narrative fields. This is a S2.3-class finding but specific to DRC: the most famous producer in the world has 2/3 marquee wines with identity-only shells after "enrichment". Combined with S2.3 F1 (Echezeaux + La Tâche not in the marquee hand-pick list) + S2.3 F3 (DRC producer metadata empty), DRC is thoroughly broken at the page level.

**Proposed fix:** Sprint 5 coverage plan must target producer-complete enrichment — if producer X has N wines in the corpus and any are enriched, ALL should be enriched in the same pass.

---

### F26 — `wine_vintage_tasting_insights` sensory grid not audited structurally (5,164 rows)

**Severity:** P2
**Effort:** medium (sensory grid audit requires sommelier sample + side-by-side vs structured ABV/acid data)
**Evidence:** 5,164 rows of sensory data (`sensory_acidity`, `sensory_tannin`, `sensory_body`, `sensory_sweetness`, `sensory_alcohol`, `color_intensity`, `aroma_intensity`, `finish_length`, `complexity`, `quality_level`) generated by the same edge function that wrote `wine_insights`. Not audited in S2.6 (scope limit — this is fact-grid content, not prose). Likely confabulated for the same 175% / MLF-missing / wrong-grape reasons. Should be checked for: (a) alignment with structured ABV on rows where ABV exists, (b) Chardonnay-class tannin scores (should be 1, not 3+), (c) grade-vs-quality correlation.

**Proposed fix:** S2.7 (UX audit) or S2.9 (synthesis) — this is partially UX scope since it's what renders on wine pages. At minimum add a cross-check: `sensory_alcohol` bucket should match actual ABV bucket; `sensory_tannin` on whites should be ≤2.

---

### F27 — Food pairing prose uses only 3 rhetorical verbs ("cuts through", "complements", "balance")

**Severity:** P2
**Effort:** trivial (F19 fix closes this)
**Evidence:** See F19. 75% + 41% + 50% = 166 verb-hits in 105 rows = avg 1.58 banned-adjacent rhetorical verbs per row. Voice palette is narrow.

**Proposed fix:** F19 fix extends the food-pairing field spec with a rhetorical-structure banned list: banned opening verbs ("complements", "balances", "pairs with", "stands up to"), required variety (3 different rhetorical frames per 5 pairings).

---

### F28 — Grade B + reference content enriched_at all compressed to single days (one-shot batch history)

**Severity:** P2
**Effort:** trivial (finding is a trust observation)
**Evidence:**
- Grade B wine_insights: `MIN(enriched_at) = 2026-04-10`, `MAX(enriched_at) = 2026-04-10` — all 46 rows on one day
- Grade C wine_insights: `MIN(enriched_at) = 2026-04-10`, `MAX(enriched_at) = 2026-04-10` — all 5,062 rows on one day
- region_insights: all 202 on 2026-03-06
- appellation_insights: all 82 on 2026-03-06
- country_insights: all 62 on 2026-03-06

**Corpus is two one-shot batches, no ongoing refresh discipline.** `refresh_after` columns exist (90-day or 365-day TTLs) but nothing is enforcing them. Any "refresh" run after Sprint 3 fixes will replace entire rows — not a bug, but means the enrichment system as deployed is batch-first not stream-first, which affects how Sprint 5 is sequenced.

**Proposed fix:** Sprint 5 regeneration plan should explicitly be "wipe and repopulate" rather than "refresh expired rows" — acknowledge the one-shot nature and don't pretend the refresh_after column does anything yet.

---

### F29 — Reference insights have no enrichment_log entries (0 rows in log for `entity_type` reference)

**Severity:** P2
**Effort:** trivial (add logging to reference scripts)
**Evidence:** `SELECT DISTINCT model, entity_type FROM enrichment_log` returns only `{entity_type: 'wine'}` rows. The 4 reference enrichment scripts never write to `enrichment_log`. Zero audit trail for the 346 reference rows — can't track model used (though we infer stale claude-sonnet-4-20250514), cost, token count, prompt template, or failures.

**Proposed fix:** Extend the 4 reference scripts to write `enrichment_log` rows with `entity_type='appellation' | 'region' | 'country' | 'grape'`, model ID, cost_usd, prompt_template, fields_updated. Pre-req for Sprint 5 cost tracking.

---

### F30 — `country_insights.ai_regulatory_overview` is the only reference field that matches VOICE.md baseline (positive finding)

**Severity:** P2
**Effort:** N/A (preserve as template)
**Evidence:** Representative Austria: *"Austria uses a German-inspired system but with important differences — DAC (Districtus Austriae Controllatus) designations focus on regional typicity rather than ripeness levels. The traditional Prädikatswein categories still exist for sweet wines, but dry wines increasingly use the DAC system that emphasizes where grapes are grown and mandates typical regional styles. Labels are generally straightforward, showing grape variety, region, and producer with less complexity than neighboring Germany."*

- Specific: names DAC, Prädikatswein, the 1985 transition implied
- No hedging, no sommelier theater, no generic filler
- Information-dense: every sentence teaches something
- Has a point of view ("less complexity than neighboring Germany")

This is VOICE.md compliance. Why does regulatory_overview clear the bar when `ai_signature_styles` doesn't? **Hypothesis:** regulatory content has narrow, verifiable ground truth (the actual law) and Claude is forced to stay near facts. Signature style content is abstract/sensory and Claude drifts.

**Proposed fix:** Preserve this as the reference voice template. In Sprint 5 prompt rewrites, use this field's outputs as the positive-reference example in the prompt preamble: *"Your target voice matches this example: [Austria ai_regulatory_overview]"*.

---

## P3 findings (2)

### F31 — Grade C style_profile field is the strongest fingerprint of the tightened enrich_prompts.py working

**Severity:** P3
**Effort:** N/A (positive observation, preserve as baseline)
**Evidence:** Representative Bruno Giacosa Falleto Grade C `ai_style_profile`: *"Full-bodied dry red with high acidity, powerful tannins, and mineral-driven complexity — Barolo's traditional archetype rather than the riper, more voluptuous modern expression"* — specific, has a point of view, contrasts against modern style, zero filler. The `GRADE_C_FIELDS` spec for style_profile requires grape + structural descriptor (body/acid/tannin/sweetness/oak) — that structural constraint is why the field clears the bar.

**Proposed fix:** When Sprint 5 redoes all prompts, KEEP the structural constraint pattern: every narrative field must have a minimum structural requirement (e.g. "must name the soil type + the climate influence + the resulting wine trait") rather than just a word-count guideline. Constraints-that-require-specificity are the highest-leverage voice-rule mechanism in the codebase.

---

### F32 — Comparable wines field sometimes invents producers/wines in direct violation of the COMPARABLES CRITICAL RULE

**Severity:** P3
**Effort:** trivial (tighten L1→L3 comparable-wines fact-check)
**Evidence:**
- `enrich_prompts.py:55-58` COMPARABLES CRITICAL RULE: *"DO NOT invent wine names or cuvées. DO NOT attach an appellation to a producer unless the packet says so."*
- Merry Edwards Grade C `ai_comparable_wines`: *"Morey-Saint-Denis Grand Cru **from Domaine Huet**"* — Domaine Huet is a Vouvray producer, not Morey-Saint-Denis. Wrong appellation attached to producer. **Direct violation of rule.**
- Schramsberg J Schram `ai_comparable_wines`: *"**Scharffenberger Brut Rosé — same producer's sparkling version**"* — Scharffenberger is a different winery. **Direct violation.**
- Heitz Cellar Linda Falls `ai_comparable_wines`: *"Stag's Leap Wine Cellars S.L.V. Cabernet Sauvignon... **comparable mid-$80s positioning**"* — fabricated price data not in any facts packet.

**Proposed fix:** (a) Comparable-wines L3 gate must validate each named producer against the DB producers list (deterministic SQL check). (b) If a named producer isn't in the DB comparables list, reject the response and retry. (c) Price claims in comparables must be stripped (comparable wines field should not contain pricing — that's a separate field).

---

## Meta-patterns (for S2.9 synthesis)

**Five patterns to escalate to Sprint 3 backlog and Sprint 5 design:**

1. **Prompt drift is the voice problem.** `enrich_prompts.py` is the only tightened voice-rules source in the codebase. Six other prompt locations are strictly weaker. Consolidating to ONE shared voice module (`pipeline/lib/voice.py` or extending `enrich_prompts.py`) and having every enrichment surface import from it closes **F1, F2, F10, F11, F12, F13, F14, F15, F16, F17, F18, F23, F24, F27** simultaneously. This is the highest-leverage S2.6 cleanup.

2. **Voice rules cannot prevent factual confabulation.** Tightening hedging, sommelier theater, and generic filler reduced surface tells by 10-60x in Grade C vs Grade B. But F3's factual errors (Schramsberg rosé, Perrin white CdP, DRC no-MLF, Merry Edwards 40 years) prove that a stricter voice does not produce a more accurate content. **Retrieval-grounded facts packet + L3 fact-check gate** is the structural fix; Session 10 already diagnosed this and the edge function is feature-flagged as evidence. S2.6's contribution is quantitative: 487 contaminated wines from the Chardonnay/Pinot Blanc bug + 49/82 over-volcanic soil profiles + 5/5 random Grade C hooks with invented facts. The scale demands L3 becomes non-optional in Sprint 5.

3. **Contamination feedback loop is real and bidirectional.** Reference-layer confabulation (Knights Valley volcanic, Hunter Valley basalt, RRV volcanic ash) flows into wine-layer prompts as authoritative context (edge function `assembleContext`). Wine-layer confabulation (Beringer Alluvium's two different wrong volcanoes) stays at the wine layer but reinforces the frontend user's exposure. **Sprint 5 regeneration MUST run reference-first, then wine** — not parallel. The contamination direction is one-way, so the fix direction must also be one-way.

4. **Reference content coverage is US-biased AND thin on the Old World marquee.** 82/82 appellation_insights are US AVAs; zero European grand crus, Chambertin, Barolo, Champagne villages. The sommelier-bar wines in S2.3 cannot get reference-layer context today. Sprint 5 scope must grow to ≥500 additional European appellation_insights (small cost increase; large fidelity gain).

5. **Grade C is voice-audited but food-blind.** 99% of enriched wines (5,062 of 5,108) have no food-pairing prose, because GRADE_C_FIELDS explicitly drops the food_pairing slot. VOICE.md has a whole section on Food Pairings that applies "everywhere food pairing content appears" — but currently food pairing only appears on 1% of the corpus. Sprint 5 F7 + F19 + F23 fix this via schema widening and the grape_insights.py food-pairing field spec porting upstream.

---

## Sprint 3 sequence refined (voice items added)

Previous (post-S2.5): (a) S2.2 F1 staging relink → (b) S2.3 F3 producer seed → (c) refined grape-repair workstream (3a-3e + 3c.5-3c.7) → (d) F6 color+country repair → (e) F10 L3 re-fact-check → (f) content regeneration. Pre-Sprint-3 hygiene: describe-chemical delete + vendor enrich-wine + model IDs.

**S2.6 additions:**

- **Sprint 3 pre-req — voice module consolidation (F1):** create `pipeline/lib/voice.py` with shared VOICE_RULES_BLOCK (upgraded from `enrich_prompts.py:41-59` with F12/F13/F16/F23 additions), NEVER INVENT block (extended from `enrich_prompts.py:92-106` with F11 reference-specific rules + F20 contradiction-escape-hatch), rewrite 4 reference prompts to import it, redeploy edge function reading from the same module. Estimated effort: small (4-6 hours). **This is the highest-leverage pre-req** — it closes 14 of the 32 findings. Must land BEFORE any reference or wine regeneration.

- **Sprint 3 pre-req — vendor edge function source into git (F2, overlaps S2.5 F31):** `supabase/functions/enrich-wine/index.ts` in repo, aligned to shared voice module, model ID via shared registry.

- **Sprint 3 — restore wine_food_pairings from archive (F8):** bulk UPDATE restoring 809 rows from `archive.wine_food_pairings`. Trivial, same pattern as S13 staging relink.

- **Sprint 5 preparation — L3 fact-check gate implementation (F3):** re-scope the $18 S2.3 rolled-forward pre-auth. Original plan: "re-fact-check 5,108 existing rows". New plan: "**build the L3 gate that blocks writes without fact-check**, apply it on the Sprint 5 regeneration pass". Estimated Sonnet cost at Sprint 5 scale: ~$40-80 for L3 on 5,000-8,000 new wine_insights + 500-1000 new reference_insights. Inside combined Sprints 2+3+5 ceiling.

- **Sprint 5 — reference regen first, wine regen second (F5):** sequencing constraint, not additional scope.

- **Sprint 5 — widen GRADE_C_FIELDS (F7, F22):** add food_pairing + cellar_recommendation + a shortened terroir_expression to the Haiku schema. Cost: ~$20-30 at Sprint 5 scale.

- **Sprint 5 — port grape_insights.py food-pairing rules upstream (F19):** 30-min prompt edit.

- **Sprint 5 — expand reference coverage beyond US AVAs (F9):** ~500 new European appellation_insights + 150 new region_insights. Sonnet cost ~$15-20.

**Total S2.6 findings blocking Sprint 3:** **9 P0 + 14 P1 = 23 items**. Overlaps with S2.5 (F2 ↔ S2.5 F31 vendor; F10 ↔ S2.5 F5 models; F2 ↔ S2.5 F4 grape display_name) → net ~20 new items not already in backlog.

---

## Scope-breaker check

None. All findings fit inside the Sprint 3 pre-req + Sprint 5 regen envelope that was already planned. F5 (feedback loop) is a sequencing refinement to Sprint 5; it doesn't expand total scope because reference-first ordering was already implicit in the 30K → Audit → Execute → Reference Design → Reference Enrichment sprint sequence. F9 (European coverage expansion) is the only finding that materially grows Sprint 5 scope and the cost growth is <$30 at Sonnet rates.

The **feature flag on the `enrich-wine` edge function** should stay OFF through Sprint 3 and into Sprint 5. Do not flip it until F1 (shared voice module) + F2 (edge function aligned) + F3 (L3 gate) + F4 (grape repair) all land. S2.6 evidence strongly validates the Session 10 decision to flag it off.

---

## Deliverables

- `data/sprints/audit/findings/findings_voice.md` — this 32-finding report (9 P0, 14 P1, 7 P2, 2 P3)
- `data/sprints/audit/prompts/s2_6_voice.md` — session prompt (written at session start for reproducibility)
- `data/sprints/audit/sessions.json` — S2.6 → done, $0 spend
- `data/sprints/audit/budget.json` — S2.6 entry, running total $0.00 / $25.00
- `data/sprints/audit/journal.md` — S2.6 section (this one)
- `CLAUDE.md` — Current State updated (S2.6 done + finding count)
- `memory/project_sprint2_findings.md` — S2.6 cross-references
- `data/sessions.md` — whiteboard entry moved to Done
