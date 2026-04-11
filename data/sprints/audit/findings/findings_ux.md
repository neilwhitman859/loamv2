# S2.7 — UX / Frontend Audit Findings

**Sprint:** 2 (Audit)
**Session:** 7 of ~9 (S2.7)
**Expert hat:** UX / frontend
**Method:** Opus 4.6 inline read-only static analysis across `frontend/src/` + Supabase MCP `execute_sql` verification queries
**Budget:** $0 actual (ratified Opus inline pattern per S2.3-S2.6)
**Scope:** 9 consumer pages, 9 shared consumer components, 3 shared hooks, 14 dev-explorer pages, `App.tsx` routing, `ConsumerLayout`, `supabase.ts`, empty-state handling, data-to-UI integrity, Principle #9 compliance, a11y, mobile-first behavior

**Frontend status:** paused per `memory/feedback_frontend_pause.md`, but deployed at loam.onrender.com. Audit treats it as code-as-written, not code-as-intended.

---

## Summary

**32 findings. 9 P0, 14 P1, 7 P2, 2 P3.**

**Meta-pattern:** every broken wine in the data layer (S2.1-S2.6) has a specific UI symptom, and most symptoms render silently because there is no `.catch()` anywhere in the consumer pages and no error boundary at the App level. The frontend trusts the data layer completely — which was fine when the data was aspirational and the pages were demo screens, but is now the opposite of what's needed for a read-only audit phase.

**The three most important findings:**

1. **F2 — 100% of country pages are silently broken.** `CountryPage.tsx` line 40 selects a non-existent column (`ai_signature_grapes`; the real column is `ai_signature_styles`). The PostgREST 42703 error is swallowed, so every `/country/:id` visit renders *no* AI overview even though `country_insights` has 62 rows. Traced end-to-end by running the exact PostgREST query and getting an error, then verifying `information_schema.columns` lists `ai_signature_styles` (plural) instead. Single-character fix, universal impact.

2. **F1 — 12,083 wine pages render an empty `<h1></h1>`.** WinePage fetches `wine.name` (not `display_name`), and 7.8% of active wines have `name IS NULL` while `display_name` is populated (samples include Ropiteau Pommard Premier Cru, Mommessin Châteauneuf-du-Pape, Ligeret Chambertin-Clos de Bèze Grand Cru — all marquee Burgundy). The producer link renders below, but the heading is blank.

3. **F4 — ProducerPage is structurally empty across the full corpus.** 0 of 10,676 producers have `hectares_under_vine`, `total_production_cases`, `address`, `latitude`, `description`, `philosophy`, `year_established`, `parent_producer_id`, or `parent_company`. 0 producers have `appellation_id`. Only 1 has `website_url`. Only 1 has `producer_type`. The Section "Details" header renders above an empty FactGrid (which filters null children to `null`), producing a visible empty section on every producer page. The S2.3 F3 claim that "15 famous producers have 0 metadata" undersells it — **every** producer has ~0 metadata.

**Cross-session carry-throughs:** S2.7 extends S2.1 F28 (counts), S2.3 F1 (marquee breakage), S2.3 F2 / S2.5 F2 (Chardonnay/Pinot Blanc), S2.3 F3 (producer metadata), S2.5 F4 (grapes.name in dev explorer), S2.5 F18 (NULL display_name), S2.6 F4 (confabulated wine_insights), S2.6 F5 (volcanic soil profile contamination), S2.6 F7 (empty food pairing), S2.6 F8 (empty wine_food_pairings), S2.6 F9 (US-only appellation_insights).

**Cross-references:** New items total 23 P0+P1 pre-Sprint-3 blockers (9 P0 + 14 P1). Most P0s are trivial-effort (typos, missing column, single-line breadcrumb). P1s are concentrated in "dead fetches" (15 AI fields fetched by consumer pages and never rendered) and "no error handling / no empty states / no a11y."

---

## P0 — Broken or user-visible correctness at the UI layer

### F1 — [P0, trivial] WinePage renders an empty `<h1>` for 12,083 wines (7.8% of active corpus)

**Where:** `frontend/src/pages/consumer/WinePage.tsx:175-187` (SELECT), `WinePage.tsx:352` (render).

**What:** The wine SELECT only fetches `name`, not `display_name`:

```tsx
// WinePage.tsx:175
supabase.from('wines').select(`
  id, name, color, wine_type, ...
```

Then at line 352:

```tsx
<h1 className="font-display text-2xl md:text-3xl font-semibold text-earth-900 leading-tight">
  {wine.name}
</h1>
```

Live DB state:

```sql
SELECT count(*) FILTER (WHERE name IS NULL) AS null_name,
       count(*) FILTER (WHERE name IS NULL AND display_name IS NULL) AS null_both
FROM public.wines WHERE deleted_at IS NULL;
-- null_name = 12,083, null_both = 0
```

Every one of those 12,083 wines has `display_name` populated but `name` NULL. The page renders `<h1></h1>` (React renders NULL as nothing). The breadcrumb at `WinePage.tsx:344` (`<span>{wine.name}</span>`) is also empty.

**Samples (actual DB rows):**
- Ropiteau Pommard Premier Cru — display_name: "Ropiteau, Pommard Premier Cru"
- Mommessin Châteauneuf-du-Pape — display_name: "Mommessin, Châteauneuf-du-Pape"
- Ligeret Chambertin-Clos de Bèze Grand Cru — display_name: "Ligeret, Chambertin-Clos de Bèze Grand Cru"
- Feiler-Artinger Chardonnay Burgenland — display_name: "Feiler-Artinger Chardonnay, Burgenland"
- Palencia Pinot Noir Ancient Lakes — display_name: "Palencia Pinot Noir, Ancient Lakes of Columbia Valley"

Every sample is a real wine with high findability value. They currently render with an invisible title.

**Root cause:** S2.5 F18 identified that `lwin_long_tail.py` inserts wines without populating `display_name` but the inverse is also true for many batches — they populated `display_name` without `name`.

**Fix (Sprint 3 pre-req):** Either (a) add `display_name` to the SELECT and render `wine.name || wine.display_name || '(Unnamed wine)'`, or (b) backfill `name` from `display_name` where NULL. Option (a) is 2 minutes; option (b) is a data fix. Both should land — (a) defends against future recurrence.

---

### F2 — [P0, trivial] CountryPage selects a column that doesn't exist — 100% of country pages silently fail to render ai_overview

**Where:** `frontend/src/pages/consumer/CountryPage.tsx:40`.

**What:**

```tsx
supabase.from('country_insights')
  .select('ai_overview, ai_wine_history, ai_key_regions, ai_signature_grapes')
  .eq('country_id', id).maybeSingle()
```

`ai_signature_grapes` does not exist in `country_insights`. The actual column is `ai_signature_styles` (plural). Verified by running the exact query via MCP:

```
ERROR: 42703: column "ai_signature_grapes" does not exist
LINE 2: SELECT ai_overview, ai_wine_history, ai_key_regions, ai_signature_grapes...
```

And verified by listing columns:

```sql
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'country_insights';
-- ai_key_regions, ai_overview, ai_regulatory_overview, ai_signature_styles, ai_wine_history, confidence, ...
```

Because there's no error handling (F9), the failure is silent. `iRes.data` stays null → `insight` state stays null → the "Overview" section at `CountryPage.tsx:118-122` never renders. **Every country page visit has no AI content rendered**, even though `country_insights` has 62 populated rows with `ai_overview`.

S2.6 F30 flagged `ai_regulatory_overview` as the one VOICE.md-compliant field in the reference corpus. It is fetched by no consumer page at all — worth preserving as a Sprint 5 template but currently invisible.

**Compounding:** `ai_wine_history` and `ai_key_regions` are also fetched but never rendered (see F17). So even if the `ai_signature_grapes` typo is fixed, the CountryPage only surfaces `ai_overview` out of 5 available AI fields.

**Fix:** rename `ai_signature_grapes` → `ai_signature_styles`, render it, and also render `ai_wine_history`, `ai_key_regions`, and `ai_regulatory_overview`. 10-minute fix. Sprint 3 pre-req.

---

### F3 — [P0, medium] 2,914 wine pages render the Chardonnay + Pinot Blanc grape bug as UI chips

**Where:** `frontend/src/pages/consumer/WinePage.tsx:238-241`, `WinePage.tsx:425-438` (Grapes section).

**What:** The UI manifestation of S2.3 F2 / S2.5 F2. Live count:

```sql
WITH chardonnay_wines AS (SELECT DISTINCT wine_id FROM public.wine_grapes
                          WHERE grape_id = (SELECT id FROM grapes WHERE display_name = 'Chardonnay' LIMIT 1))
SELECT count(*) FROM public.wine_grapes wg
JOIN public.wines w ON w.id = wg.wine_id
WHERE wg.grape_id = (SELECT id FROM grapes WHERE display_name = 'Pinot Blanc' LIMIT 1)
  AND wg.wine_id IN (SELECT wine_id FROM chardonnay_wines)
  AND w.deleted_at IS NULL;
-- 2,914 active wine pages
```

Every one of those 2,914 pages renders:

```tsx
<Link to={`/grape/${g.grape.id}`}>
  Chardonnay {g.percentage}%
</Link>
<Link to={`/grape/${g.grape.id}`}>
  Pinot Blanc {g.percentage}%
</Link>
```

The consumer UI correctly uses `grapes.display_name` (verified: 5 of 5 consumer page SELECTs read `display_name`, only dev `WineDetail.tsx:37` uses `name`). But the underlying wine_grapes linkage is wrong, so the UI faithfully shows the wrong data.

**Compounding — F4 voice contamination:** 493 of those 2,914 wines also have `wine_insights` rows that were enriched with the broken grape combo in the facts packet, producing confabulated backstories about a Chardonnay+Pinot Blanc blend. These are rendered as `insight.ai_hook` at `WinePage.tsx:375-377`:

```tsx
{insight?.ai_hook && (
  <p className="text-sm text-earth-500 mt-2 italic">{insight.ai_hook}</p>
)}
```

The Waterbrook Icon "blends 100% Chardonnay with 75% Pinot Blanc — an unusual high-proportion white blend" (quoted in S2.6 F4) renders directly in italic under the producer link with no disclaimer and no indication it's AI-generated (F5).

**Fix:** data fix via Sprint 3 grape-repair workstream (3a-3e + 3c.5-3c.7 from S2.4+S2.5). UI needs no change once data is clean. But the 493 confabulated `wine_insights` rows need to be nulled out in Sprint 3 because they will keep rendering until regeneration in Sprint 5 — at minimum gate the `ai_hook` render on a "stale contamination" flag.

---

### F4 — [P0, medium] ProducerPage is structurally empty — 0 of 10,676 producers have metadata the page is designed to render

**Where:** `frontend/src/pages/consumer/ProducerPage.tsx:160-176` (Details section), `ProducerPage.tsx:198-203` (Philosophy), `ProducerPage.tsx:212-223` (Estates).

**Live DB state** (verified by direct query):

```sql
SELECT count(*) AS total,
  count(*) FILTER (WHERE hectares_under_vine IS NOT NULL) AS with_hectares,
  count(*) FILTER (WHERE total_production_cases IS NOT NULL) AS with_production,
  count(*) FILTER (WHERE address IS NOT NULL) AS with_address,
  count(*) FILTER (WHERE latitude IS NOT NULL) AS with_coords,
  count(*) FILTER (WHERE description IS NOT NULL) AS with_description,
  count(*) FILTER (WHERE parent_producer_id IS NOT NULL) AS with_parent,
  count(*) FILTER (WHERE parent_company IS NOT NULL) AS with_parent_company,
  count(*) FILTER (WHERE year_established IS NOT NULL) AS with_year,
  count(*) FILTER (WHERE producer_type IS NOT NULL) AS with_type,
  count(*) FILTER (WHERE website_url IS NOT NULL) AS with_website,
  count(*) FILTER (WHERE philosophy IS NOT NULL) AS with_philosophy,
  count(*) FILTER (WHERE appellation_id IS NOT NULL) AS with_appellation
FROM public.producers WHERE deleted_at IS NULL;
-- total=10676, hectares=0, production=0, address=0, coords=0, description=0,
-- parent=0, parent_company=0, year=0, type=1, website=1, philosophy=0, appellation=0
```

**What this means at the UI layer:**

1. **"Details" section** (`ProducerPage.tsx:161-176`) fetches `country`, `region`, `appellation`, `hectares`, `production`, `address`, `coords`, `website`. `country` is 100% populated (good), `region` is 91% populated (OK), `appellation` is 0%, everything else is 0%. The FactGrid renders {country, region} — 2 fields. The Section header says "Details" but the content is always country + region, which is already in the breadcrumb 2 lines above. Information is duplicated; the section is redundant.

2. **"Philosophy" section** (`ProducerPage.tsx:198-203`) guards on `producer.philosophy`. 0 producers have philosophy. **This section NEVER renders** anywhere in the app. 6 lines of dead JSX.

3. **"Estates & Labels" section** (`ProducerPage.tsx:212-223`) renders children from `parent_producer_id = this.id`. 0 producers have parent_producer_id. **This section NEVER renders**.

4. **`fetchParentProducer` useEffect branch** (`ProducerPage.tsx:120-125`) — 0 producers have parent_producer_id, so the `if (prod.parent_producer_id)` never fires. Dead code path, still pays the code cost.

5. **Header tags** (`ProducerPage.tsx:150-154`) conditionally show `producer_type`, `year_established`, `parent_company`. All three hit 0/10,676 or 1/10,676. The tag row is always empty on the header.

6. **Aliases subtitle** (`ProducerPage.tsx:155-157`) — needs ≥1 `producer_aliases` row. Let me note this was not verified live; spot check earlier in S2.3 suggested alias data exists for some marquee producers but not uniformly.

7. **`description` field** (`ProducerPage.tsx:13` interface) is declared and fetched but never rendered in JSX. Dead fetch.

**Net user experience on a ProducerPage:** breadcrumb + H1 producer name + certifications (if any) + wines list. Everything else is skeleton. The `<Section title="Details">` header renders above nothing meaningful on every single producer page.

**Why this matters:** S2.3 F3 flagged 15 marquee producers (DRC, Lafite, Latour, Margaux, Haut-Brion, Pétrus, Gaja, etc.) as having 0 metadata. The fix is to seed 15 files. But the actual scope is **every producer** — 10,676 of them. Sprint 3 needs a producer metadata source strategy, not a marquee-producers-only seed file.

**Fix:** Sprint 3 should expand S2.3 F3 from "seed 15 marquee producers" to "close out the dead-section-header problem at the product level." Two parallel tracks:
- (a) Immediate UI fix: delete the dead sections (Philosophy, Estates & Labels) until data exists; conditionally render "Details" only when ≥ 3 non-breadcrumb fields are populated.
- (b) Data fix: producer metadata backfill scope. S2.3 F3 proposed hand-curated seed for 15 producers. This finding suggests we should go broader — scrape producer websites for the next tier (~150-500 producers from top appellations) and accept NULL for the long tail.

---

### F5 — [P0, small] AI-generated content renders as plain text with zero disclaimer, confidence indicator, or source attribution — across all consumer pages

**Where:** every consumer page that renders an `ai_*` field. Sample receipts:

- `WinePage.tsx:376` — `{insight.ai_hook}` in italic, no label, no source, no "AI" badge
- `WinePage.tsx:554` — `{wine.soil_description || appInsight?.ai_soil_profile}` as MiniLabel "Soil" — mixes wine-level structured data with appellation-level AI prose, no attribution
- `WinePage.tsx:608` — `{appInsight.ai_signature_style}` as MiniLabel "Signature style"
- `WinePage.tsx:666` — `{insight.ai_cellar_recommendation}` as plain paragraph
- `WinePage.tsx:674` — `{insight.ai_food_pairing}` as plain paragraph
- `WinePage.tsx:729` — `{insight.ai_comparable_wines}` as plain paragraph
- `AppellationPage.tsx:185-192` — `ai_soil_profile`, `ai_climate_profile`, `ai_signature_style` as plain paragraphs under "Terroir"
- `AppellationPage.tsx:266` — `ai_aging_generalization`
- `RegionPage.tsx:133, 136, 159, 204` — `ai_climate_profile`, `ai_signature_style`, `ai_sub_region_comparison`, `ai_history`
- `CountryPage.tsx:120` — `ai_overview`
- `GrapePage.tsx:150, 153, 218, 223` — `ai_flavor_profile`, `ai_growing_conditions`, `ai_food_pairing`, `ai_aging_characteristics`

**What's missing:**

1. **No confidence indicator.** `ConfidenceBadge.tsx` exists (renders `confidence >= 0.85 ? emerald : amber : orange : red`) but it's only wired into the dev `/data` explorer via `InsightsPanel.tsx`. Consumer pages don't import it. Verified via grep:

```
Grep: InsightsPanel|ConfidenceBadge in frontend/src/pages/consumer
→ No files found
```

The `confidence` column exists on `wine_insights`, `region_insights`, `appellation_insights`, `country_insights`, `grape_insights`. It's fetched by none of the consumer pages.

2. **No "AI-generated" badge or disclaimer.** The `data_grade` pill (`WinePage.tsx:371`) shows "Grade B" or "Grade C" but a user doesn't know what that means. S2.6 F3 proved AI content still confabulates facts even under tightened prompts. The user needs a disclaimer that reads "AI summary — verify key facts independently" at minimum for any content rendered from an `ai_*` field.

3. **No source attribution for cross-level prose.** Biggest symptom: the wine page Soil field. `WinePage.tsx:551-555`:

```tsx
{(wine.soil_description || appInsight?.ai_soil_profile) && (
  <div className="mt-3">
    <MiniLabel>Soil</MiniLabel>
    <p className="text-sm text-earth-700">{wine.soil_description || appInsight?.ai_soil_profile}</p>
  </div>
)}
```

This silently shows the appellation's soil profile under a heading that implies wine-level data. A user reading a Beringer Alluvium wine page sees "Volcanic soils from ancient Mount St. Helena eruptions" as if it's about that vineyard. No label says "from the Knights Valley profile."

4. **No enriched_at shown.** `InsightsPanel.tsx:19` reads and formats `enriched_at`, but again not wired into consumer pages. Users can't tell whether the insight is from 2026-03-06 or 2026-04-10.

**Fix:**
- Add an `<AIBadge confidence={x} enrichedAt={y} />` component and render it above every `ai_*` paragraph in the consumer pages. Can reuse existing `ConfidenceBadge`.
- When rendering appellation-level prose on a wine page, add MiniLabel "From the {appellation.name} profile" or italic footer — make the layer boundary visible.
- Add a small global footer on any page that renders AI content: "Some content on this page is AI-generated. Verify key facts against producer and regulatory sources."

Sprint 3 task. Small effort, large credibility impact.

---

### F6 — [P0, small] 16,429 active wine pages render contaminated volcanic soil claims at the wine level

**Where:** `frontend/src/pages/consumer/WinePage.tsx:551-556`.

**What:** extends S2.6 F5 to the UI layer. `appInsight?.ai_soil_profile` is rendered directly under MiniLabel "Soil" if the wine has no `wine.soil_description` of its own. 49 appellation_insights rows contain the word "volcanic" per S2.6 F5; 14 of those (per S2.6's list of wrong-volcanic AVAs including Knights Valley, RRV, Sonoma Coast, Howell Mountain, Hunter Valley) are false according to primary sources.

Live count:

```sql
SELECT count(*) FROM public.wines w
JOIN public.appellation_insights ai ON ai.appellation_id = w.appellation_id
WHERE w.deleted_at IS NULL AND lower(ai.ai_soil_profile) LIKE '%volcanic%';
-- 16,429 active wine pages
```

16,429 wine pages render a "volcanic" soil claim that came from the appellation insight and is presented as if it's about the wine's own vineyard. For the subset of appellations where the volcanic claim is wrong (per S2.6 F5), users read actively false geology.

**Fix:** part of F5 attribution fix + F3 voice contamination cleanup. Until Sprint 5 regenerates reference insights, either:
- (a) gate wine-level rendering of `appInsight.ai_soil_profile` behind a "clean reference insight" flag (doesn't exist yet; this creates it as a Sprint 3 schema ask), or
- (b) hard-delete confabulated soil profiles from the affected appellation_insights rows (Sprint 3 cleanup action).

Option (b) is faster but lossy. Option (a) lets Sprint 5 regenerate cleanly. Recommend (b) for now and regenerate in Sprint 5.

---

### F7 — [P0, trivial] Footer "About" link is broken

**Where:** `frontend/src/components/consumer/ConsumerLayout.tsx:84`.

```tsx
<Link to="/about" className="hover:text-earth-600 transition-colors">About</Link>
```

`App.tsx` has no `/about` route in the consumer layer. Only `/dev/about` exists (dev-explorer-only, `App.tsx:77`). Clicking the footer About link goes to `/about`, which hits no route and renders a blank screen (see F10 — no 404 catch-all).

**Fix:** two options. Either (a) add a simple `/about` consumer route that tells users what Loam is, or (b) change the link to `/dev/about`. Recommend (a) because `/dev/about` is a dev-explorer link that exposes schema/table browsers to consumer users.

Trivial fix. Sprint 3 pre-req.

---

### F8 — [P0, trivial] `/vineyard/:id` route is dead — 0 vineyards exist and the search doesn't return them

**Where:** `App.tsx:50`, `VineyardPage.tsx`, `HomePage.tsx:56` (search_catalog).

**What:** CLAUDE.md confirmed in Session 14 Phase A that `public.vineyards` is 0 rows post-rebuild (archive_vineyards has 815, but public is empty). Verified live:

```sql
SELECT count(*) FROM public.vineyards;
-- 0
```

The route `/vineyard/:id` is still wired in `App.tsx:50`. `VineyardPage.tsx` still has its full select/render logic (232 LOC). It is unreachable because:
- `search_catalog` RPC entity_types default: `['wine', 'producer', 'region', 'appellation', 'grape']` — no vineyard.
- HomePage `search()` call passes the same entity_types explicitly (`HomePage.tsx:56`).
- SearchPage does the same (`SearchPage.tsx:55`).
- No other page navigates to `/vineyard/:id`.

Every direct URL visit hits the NotFound branch (`VineyardPage.tsx:85`). The entire 232 LOC page is dead weight.

**Fix:** either (a) delete the route, the page, and the unused schema scaffolding, or (b) leave it parked until Sprint 4's reference-layer redesign defines what vineyards should be. Recommend (b) with a `/* S2.7 F8: dead route, parked for Sprint 4 */` comment.

---

### F9 — [P0, small] Zero error handling anywhere in consumer pages — every Supabase failure fails silently and leaves partial state

**Where:** every `supabase.from(...)` / `supabase.rpc(...)` call in the 9 consumer pages.

**What:** grep confirmed no `.catch()` calls in consumer pages. The pattern across all pages is:

```tsx
supabase.from('wines').select(...).eq('id', id).single()
  .then(({ data }) => {
    if (data) { ... }
    else setLoading(false)
  })
```

No error is checked except in SearchPage (`SearchPage.tsx:56` checks `error` but doesn't surface it — only uses it to gate `setResults(data)`). No `.catch()`, no try/catch, no error boundary at App level (`main.tsx:7-13` has no ErrorBoundary wrapping `<App/>`), no retry UI.

**What this causes:**
- F2 (CountryPage bad column) manifests as "no overview ever renders" instead of "database error." A developer would debug this for hours.
- RLS policy failures fail silent. If RLS blocks `wine_insights` for an unauthenticated user (it shouldn't, but if it did), the page renders without insights and no one would know.
- Network flakiness gives a stuck loading state (the parent `setLoading(false)` only fires if the outer wine query succeeds — if the parent wine query times out, the loading skeleton shows forever).
- The `useEntityDetail.ts:13-27` hook actually handles errors correctly (sets `error` state) — but that hook is ONLY used by the dev `/data/*` pages, not the consumer pages. Inconsistent.

**Fix:**
1. Add an error boundary at App level in `main.tsx`.
2. Migrate consumer pages onto `useEntityDetail` + new sibling `useEntityDetailMany` hook, or add `.catch(err => setError(err.message))` to each fetch.
3. Add a generic `<ErrorState />` component and render it when any fetch fails.

Small effort (half day), foundational. Must ship before F2 or any other rendering bug can be trusted to be reportable.

---

## P1 — Significant gaps that must fix before frontend resumes

### F10 — [P1, trivial] No 404 catch-all route — unknown paths render a blank screen

**Where:** `frontend/src/App.tsx` route list.

`App.tsx` defines 22 routes. It has no catch-all `<Route path="*" element={<NotFound />}>` under either `ConsumerLayout` or at the top level. React Router v6 default behavior for an unmatched path is to render nothing, so the header/footer renders with a blank main area.

**Fix:** add `<Route path="*" element={<NotFound />}>` under `ConsumerLayout`, reusing the `NotFound` component pattern that already exists in ProducerPage/AppellationPage/etc. Trivial.

---

### F11 — [P1, trivial] Dashboard.tsx queries `producer_insights` as a core table but it has 0 rows

**Where:** `frontend/src/pages/Dashboard.tsx:44`.

```tsx
{ label: 'Producers', table: 'producers', link: '/data/producers', insightsTable: 'producer_insights' },
```

Verified: `public.producer_insights` table exists but has 0 rows. The dev Dashboard displays "Producer Insights: 0" next to every producer count, implying it's a planned/empty table — which is accurate but noisy.

**Fix:** either remove the insightsTable mapping for producers until the table is populated, or mark it as "aspirational" in the UI. Dev-only, low stakes. Worth catching because it echoes the broader "dead fetches" pattern (F16-F19).

---

### F12 — [P1, small] wineCount displayed across 5 pages is inflated by F-grade empty shells

**Where:** HomePage `wineCount` badge (`HomePage.tsx:41`), AppellationPage/RegionPage/CountryPage header tags (e.g., `AppellationPage.tsx:131`), ProducerPage header count (`ProducerPage.tsx:227`).

**What:** live counts show 96.7% of `appellation_id`-bearing wines are Grade F:

```sql
SELECT count(*) FILTER (WHERE appellation_id IS NOT NULL) AS with_app,
       count(*) FILTER (WHERE appellation_id IS NOT NULL AND data_grade = 'F') AS f_with_app
FROM public.wines WHERE deleted_at IS NULL;
-- 104,788 with appellation, 100,313 are F (95.7%)
```

And for producers, 9,274 of 10,676 (86.9%) have only Grade F wines:

```sql
WITH producer_wine_grades AS (
  SELECT producer_id, count(*) FILTER (WHERE data_grade IN ('B','C','D')) AS enriched
  FROM public.wines WHERE deleted_at IS NULL GROUP BY producer_id)
SELECT count(*) FROM producer_wine_grades WHERE enriched = 0;
-- 9,274
```

The appellation page header shows "1,234 wines" when only ~50 are meaningfully visible. The HomePage footer shows "156K+ wines" (which also doesn't filter `deleted_at IS NULL` — Technical note: `HomePage.tsx:41` is the only `wines` count in the app that misses the `deleted_at` filter, so it reports 156,570 instead of 155,623 — minor, but the bigger issue is the F-grade inflation).

Users see large impressive counts but click through to empty pages. This is a credibility-draining pattern.

**Fix:** two-part.
- (a) Everywhere wineCount is displayed, filter by `data_grade > 'F'` OR render two numbers: "4,975 wines with details · 155K in catalog."
- (b) HomePage footer: fix the missing `is('deleted_at', null)` filter.

Small effort. Works even without Sprint 3 data fixes because it's honest about the current state.

---

### F13 — [P1, small] 5,573 wines in 2,404 duplicate-name groups render on producer pages with no way to distinguish

**Where:** `frontend/src/pages/consumer/ProducerPage.tsx:232-239`.

**Live state:**

```sql
WITH dupes AS (
  SELECT producer_id, name, count(*) AS c
  FROM public.wines WHERE deleted_at IS NULL AND name IS NOT NULL
  GROUP BY producer_id, name HAVING count(*) > 1)
SELECT count(*) AS groups, sum(c) AS rows, count(DISTINCT producer_id) AS producers
FROM dupes;
-- 2,404 groups, 5,573 rows, 1,145 producers
```

10.7% of producers have at least one duplicate-name pair. Example: Vega Sicilia has two "Pintia" entries in its wines list, one Grade C (display_name "Vega Sicilia Pintia, Toro"), one Grade F (display_name "Vega Sicilia Pintia, Ribera del Duero"). The ProducerPage wines list renders:

```tsx
<Link ...>
  <span>{w.name}</span>
  {w.appellation && <span>{w.appellation.name}</span>}
</Link>
```

If both rows have NULL appellation or the same appellation, the two wines are indistinguishable. The user can't tell them apart or choose which to click.

**Root cause:** data-layer duplication that S13 fuzzy merge pass didn't catch. S2.1 F (strict + Haiku fuzzy merged 718). Residual is the 2,404 × (c-1) undeduplicated rows.

**Fix:** two-part.
- (a) Data fix: another fuzzy-merge pass at Sprint 3 targeted at same-producer same-name wines.
- (b) UI fix: show vintage_year when the name collides. Pull vintage count from wine_vintages (n) and render as subtitle. Until (a) lands, this gives users a differentiator.

---

### F14 — [P1, trivial] Producer website URL rendered as non-clickable plain text across consumer pages

**Where:** `WinePage.tsx:622-624`, `ProducerPage.tsx:172-174`.

```tsx
{wine.producer.website_url && (
  <Fact label="Website" value={wine.producer.website_url.replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '')} />
)}
```

`Fact` component only renders link if `link` prop is passed, and this call site doesn't pass one. The URL is shown as text. Users cannot click to visit the producer's website.

Meanwhile, `frontend/src/pages/data/ProducerDetail.tsx:83` does it correctly:

```tsx
<a href={String(producer.website_url)} target="_blank" rel="noopener noreferrer">
  {String(producer.website_url).replace(/^https?:\/\//, '')}
</a>
```

The dev explorer (internal tool) has better link handling than the consumer frontend.

**Fix:** add an `external` prop to `Fact` (or a new `ExternalFact` component) that renders `<a target="_blank" rel="noopener noreferrer">`. Trivial.

---

### F15 — [P1, trivial] Dev WineDetail reads `grapes.name` instead of `display_name`

**Where:** `frontend/src/pages/data/WineDetail.tsx:37`.

```tsx
supabase.from('wine_grapes').select('grape_id, percentage, grape:grapes!grape_id(id, name, color)').eq('wine_id', id),
```

Reads `name` not `display_name`. Extends S2.5 F4 to the dev explorer UI. Users browsing the dev explorer see "CHARDONNAY BLANC" / "MERLOT NOIR" / "TEMPRANILLO TINTO" rendered on wine detail pages. Low-severity because it's a dev tool, but the dev tool is the place we'd audit data from — getting S2.4 F6 VIVC-cépage issues visible here hides them.

**Fix:** change to `display_name`. Trivial.

---

### F16 — [P1, trivial] AppellationPage fetches 3 AI fields that are never rendered

**Where:** `frontend/src/pages/consumer/AppellationPage.tsx:32-39, 77, 182-193`.

The `AppellationInsight` interface declares 7 fields and the SELECT at line 77 fetches all 7:

```tsx
.select('ai_overview, ai_climate_profile, ai_soil_profile, ai_signature_style, ai_key_grapes, ai_aging_generalization, ai_notable_producers_summary')
```

The render block at lines 182-194 only renders `ai_soil_profile`, `ai_climate_profile`, `ai_signature_style` (3 of 7). `ai_aging_generalization` is rendered separately at lines 264-268 (fine). `ai_overview`, `ai_key_grapes`, `ai_notable_producers_summary` are **never rendered**. Dead fetches, wasted bandwidth, and the richest AI field (`ai_overview`) is invisible.

**Why it matters:** `ai_overview` is the one field that might give a Chambertin appellation page meaningful content. S2.6 F9 noted 82/82 appellation_insights are US AVAs — but of the 82 that exist, `ai_overview` is 100% unviewable.

**Fix:** add an "Overview" section rendering `ai_overview` above "Terroir." Render `ai_key_grapes` and `ai_notable_producers_summary` inside the respective sections. 10 minutes.

---

### F17 — [P1, trivial] CountryPage fetches 3 AI fields that are never rendered (and one typo'd as non-existent — see F2)

**Where:** `frontend/src/pages/consumer/CountryPage.tsx:12-17, 40, 118-122`.

The `CountryInsight` interface declares 4 fields. The SELECT fetches 4 fields, but one is misspelled (see F2). The render block only shows `ai_overview`.

Of the 4 actually-existing columns in `country_insights` (`ai_overview`, `ai_wine_history`, `ai_key_regions`, `ai_regulatory_overview`, `ai_signature_styles` — 5 total), CountryPage fetches 3 and renders 1. 4 of 5 available AI fields are invisible. S2.6 F30 highlighted `ai_regulatory_overview` as the only VOICE.md-compliant reference field in the corpus — currently invisible to users.

**Fix:** render all 5 fields. Fix the F2 typo. 10 minutes.

---

### F18 — [P1, trivial] RegionPage fetches `ai_overview` but never renders it

**Where:** `frontend/src/pages/consumer/RegionPage.tsx:16, 59`.

```tsx
interface RegionInsight {
  ai_overview: string | null
  ai_climate_profile: string | null
  ai_sub_region_comparison: string | null
  ai_signature_style: string | null
  ai_history: string | null
}
```

SELECT at line 59 fetches all 5. Render renders `ai_climate_profile`, `ai_signature_style`, `ai_sub_region_comparison`, `ai_history`. `ai_overview` is never rendered. Dead fetch.

202 `region_insights` rows exist; every single one's `ai_overview` is fetched-and-discarded.

**Fix:** add overview render. Trivial.

---

### F19 — [P1, trivial] GrapePage fetches 2 AI fields that are never rendered

**Where:** `frontend/src/pages/consumer/GrapePage.tsx:15-22, 57, 147-156`.

Interface declares 6 fields. SELECT fetches all 6. Render uses `ai_flavor_profile`, `ai_growing_conditions`, `ai_food_pairing`, `ai_aging_characteristics`. Drops `ai_overview` and `ai_regions_of_note`. 2 of 6 dropped (33%).

Compounding note: `grape_insights` has 0 rows per S2.6 F6. So all 6 fields are dead fetches today because the table is empty. When the table is populated in Sprint 5, 4 of 6 will surface and 2 will be invisible.

**Fix:** render both. Trivial. Part of Sprint 3 voice module consolidation should review all page AI field renders.

---

### F20 — [P1, small] a11y: zero aria-current / aria-live / aria-labelledby / role / htmlFor anywhere in consumer pages

**Where:** grep across `frontend/src`:

```
Grep: aria-|role=|htmlFor=
→ 3 matches total. All are aria-label="Menu" on mobile hamburger buttons:
   ConsumerLayout.tsx:46, DataLayout.tsx:72, DevLayout.tsx:71
```

WCAG violations observed:
- **WCAG 2.4.6 Headings and Labels**: inputs on HomePage/ConsumerLayout have no associated `<label htmlFor>` — they rely on placeholder which disappears on focus.
- **WCAG 4.1.3 Status Messages**: HomePage search dropdown + SearchPage loading state have no `aria-live="polite"` on the result count. Screen readers don't announce "8 results" when results come in.
- **WCAG 2.4.4 Link Purpose**: `<Link to="..."><svg>` icons with no accessible name in many places (no `aria-label` on the search button, no `aria-hidden` on decorative svgs).
- **WCAG 1.3.1 Info and Relationships**: heading skip h1 → h3 on every detail page (F21).
- **WCAG 3.3.1 Error Identification**: no error messages rendered for any failed state (F9).

**Fix:** accessibility pass as a Sprint 3 pre-req. Medium effort. Not blocking enrichment but blocking any public launch.

---

### F21 — [P1, trivial] Heading hierarchy skips h1 → h3 in every detail page

**Where:** WinePage/ProducerPage/AppellationPage/RegionPage/CountryPage/GrapePage/VineyardPage. Each has:
- `<h1>` for the entity name (e.g., `WinePage.tsx:352`)
- `<h3>` for the Section component (e.g., `WinePage.tsx:749`)
- No `<h2>`.

WCAG 1.3.1 violation (heading levels must not skip). Screen reader users navigating by heading hierarchy miss the sectioning.

**Fix:** change `<h3>` to `<h2>` in the Section component. Trivial — single file change in the `Section` helper (one per page, or consolidate into a shared `Section` component per F27).

---

### F22 — [P1, trivial] WinePage food pairing section invisible on 99% of Grade C wines

**Where:** `WinePage.tsx:672-676`.

```tsx
{insight?.ai_food_pairing && (
  <Section title="Food pairing">
    <p className="text-sm text-earth-600">{insight.ai_food_pairing}</p>
  </Section>
)}
```

S2.6 F7: `GRADE_C_FIELDS` in `enrich_prompts.py` drops `food_pairing` entirely, so 5,003 of 5,062 Grade C wines have NULL `ai_food_pairing`. The "Food pairing" section never renders on them. Grade B (105 wines) has it 100%.

Further, S2.6 F8: `wine_food_pairings` structured table has 0 rows (archive has 809). The WinePage doesn't render a structured food pairing table either — only the prose field.

**What user sees:** Grade B wines have food pairing. Grade C wines don't. Of 5,108 enriched wines, 105 (2%) show food pairings.

**Fix:** (a) S2.6 F7 proposes widening GRADE_C_FIELDS to include food_pairing — data fix. (b) S2.6 F8 proposes restoring `wine_food_pairings` from archive — data fix. (c) UI fix: WinePage should render the structured `wine_food_pairings` table as a labeled fact grid (aligned with Principle #9) in addition to the prose field. Sprint 3 pre-req.

---

### F23 — [P1, trivial] Classifications loaded with system_name but only level_name rendered — context lost

**Where:** `WinePage.tsx:258-266, 368`.

```tsx
p.push(supabase.from('entity_classifications')
  .select('classification_level:classification_levels!entity_classifications_classification_level_id_fkey(level_name, classification:classifications!classification_levels_classification_id_fkey(name))')
  ...
  .then(({ data: d }) => {
    if (d) setClassifications(d.map((r: any) => ({
      level_name: r.classification_level?.level_name || '',
      system_name: r.classification_level?.classification?.name || '',
    })))
  }))
```

`system_name` is captured into state but never used. Tags at line 368:

```tsx
{classifications.map((c, i) => <Tag key={i} variant="accent">{c.level_name}</Tag>)}
```

User sees "Premier Cru" with no indication of which system. Is it "Saint-Émilion 1855 Premier Grand Cru Classé A" or "Burgundy 1er Cru" or "Médoc 1855 Premier Cru"? Different wines, same tag. Confusing.

**Compounding:** S2.4 F10 noted `classification_level` is dominated by German einzellage (1,179 of 1,743 populated) with only 2 grand_cru and 0 docg/doc/ava/cru_classe. Most tags that do render are in an unfamiliar German vocabulary.

**Fix:** render `{system_name}: {level_name}` as the Tag content. Or add a tooltip. Trivial.

---

## P2 — Improvements, not blocking

### F24 — [P2, trivial] Empty Section headers visible on ProducerPage because FactGrid filters children but Section renders its header unconditionally

**Where:** `ProducerPage.tsx:161-176, 250-255`.

Pattern:
```tsx
<Section title="Details">
  <FactGrid>
    {nullCondition && <Fact ... />}  // all false
  </FactGrid>
</Section>
```

`FactGrid` returns `null` if all children are falsy (`FactGrid.tsx:266`). But `Section` always renders its `<h3>` header. Result on a producer with no metadata: visible header "Details" above empty space. Compounds F4.

**Fix:** make `Section` unconditional only when children have meaningful content. Simpler: check inside `Section`:

```tsx
function Section({ title, children }) {
  const hasContent = React.Children.toArray(children).some(c => c != null && c !== false)
  if (!hasContent) return null
  return <section>...</section>
}
```

Trivial. Applies to all 8 consumer pages (they each define their own copy of `Section`).

---

### F25 — [P2, trivial] EntityMap boundary_source rendered raw — "ldproxy_rlp", "uc_davis_ava"

**Where:** `frontend/src/components/EntityMap.tsx:157`.

```tsx
<span>{data.boundary_source}</span>
```

Shows "ldproxy_rlp" / "uc_davis_ava" / "eurac_eu_pdo" / "nominatim" literally in the map footer. Dev-facing codes. Users don't know what they mean.

**Fix:** add a `SOURCE_LABELS` map in EntityMap with friendly names ("UC Davis AVA shapefile", "Eurac EU PDO dataset", etc.). Trivial.

---

### F26 — [P2, small] EntityMap doesn't use vineyard GPS, falls back to appellation boundary

**Where:** `VineyardPage.tsx:141-145`.

The `Vineyard` interface at line 6 has `latitude`/`longitude` and they're displayed in the FactGrid at line 118-120. But the Map section (line 141-145) calls:

```tsx
<EntityMap entityType="appellation" entityId={vineyard.appellation.id} ... />
```

So the map shows the parent appellation, not the vineyard point. `EntityMap` type signature is `'country' | 'region' | 'appellation'` — doesn't accept vineyards. Users see a large appellation polygon instead of the specific vineyard marker.

Since vineyards is 0 rows (F8), this is theoretical for now. But it's a template problem that would need fixing if Sprint 4 revives vineyards.

**Fix:** extend EntityMap to accept a `point` mode that renders just a lat/lng marker. Or, when `vineyard.latitude` is present, show an appellation map with an additional centroid marker overlaid. Small.

---

### F27 — [P2, small] Section/Tag/Fact/FactGrid/Loading/NotFound/MiniLabel duplicated across 8 consumer pages — ~500 LOC of duplication

**Where:** every file in `frontend/src/pages/consumer/`.

Pattern: each page locally defines helper components at the bottom:
- `Section` (same 7-line impl 8 times)
- `Tag` (variants inconsistent — WinePage/ProducerPage/AppellationPage support variant prop; RegionPage/CountryPage/GrapePage/VineyardPage do not)
- `FactGrid`, `Fact`, `MiniLabel` (identical across pages that have them)
- `Loading` (identical slight variations)
- `NotFound` (identical, different `label` prop)

Estimated duplication: ~60 lines × 8 pages = ~480 LOC.

**Consequences:**
1. Inconsistency — `Tag variant` support is uneven across pages.
2. Any fix (like F20 a11y or F21 heading hierarchy) has to be applied 8 times.
3. Bundle size and maintenance cost.

**Fix:** extract to `frontend/src/components/consumer/primitives.tsx`. Half-day task. Sprint 3 pre-req — reduces effort of every subsequent UI fix by 8x.

---

### F28 — [P2, small] Dashboard stats queries fire N+1 count queries on every page load with no caching

**Where:** `frontend/src/pages/Dashboard.tsx:42-60`.

The Dashboard loads 6 table counts + 6 insight table counts in parallel (12 queries), re-runs them on every mount, no caching. Plus a `dashStats` call and health checks. ~15 queries per dashboard visit.

**Impact:** negligible at current traffic (~0 consumer traffic). Not a production issue; worth noting for when traffic picks up.

**Fix:** Supabase has materialized views — build a `dashboard_stats` view. Or cache in React Query. Small.

---

### F29 — [P2, trivial] HomePage autoFocus causes mobile keyboard pop on load

**Where:** `frontend/src/pages/consumer/HomePage.tsx:118`.

```tsx
<input ... autoFocus ... />
```

On mobile, `autoFocus` triggers the keyboard to pop on page load, pushing content up and causing a layout shift. Users visiting loam.onrender.com on a phone land on a screen with the keyboard already open, which for many is unwanted — it takes over 50% of the viewport.

**Fix:** detect touch device and skip autoFocus. Or remove autoFocus entirely — users who want to search will tap the input. Trivial.

---

### F30 — [P2, small] GrapePage wineCount inflated by Pinot Blanc bug

**Where:** `frontend/src/pages/consumer/GrapePage.tsx:91-94`.

```tsx
p.push(supabase.from('wine_grapes')
  .select('wine_id', { count: 'exact', head: true })
  .eq('grape_id', id)
  ...)
```

Counts all wine_grapes rows for the grape_id. Because 2,914 Chardonnay wines also have Pinot Blanc linked (F3 / S2.3 F2), the Pinot Blanc grape page shows ~2,919 + legitimate Pinot Blanc wines. Pinot Blanc appears artificially popular.

Plus: doesn't deduplicate wine_id, so if a wine has the grape linked twice (data dup), count is inflated by the dup.

**Fix:** count distinct wine_id:

```tsx
.select('wine_id', { count: 'exact', head: true })
```

Actually, `select('wine_id')` with `count:exact` counts rows not distinct. Would need a view or an RPC to count distinct. Small effort. Waits on data fix from Sprint 3 grape-repair workstream.

---

## P3 — Nice to have

### F31 — [P3, trivial] LandingPage.tsx is dead code

**Where:** `frontend/src/pages/LandingPage.tsx`.

```
Grep: LandingPage
→ 1 hit: its own definition. No imports, no routes.
```

19 LOC, not referenced anywhere. Leftover from a pre-HomePage landing iteration.

**Fix:** delete. Trivial.

---

### F32 — [P3, trivial] Footer shows build timestamp only on dev layout, not consumer

**Where:** `frontend/src/components/DataLayout.tsx:56`, `DevLayout.tsx:60` (parallel).

`DataLayout` / `DevLayout` show "Deployed {buildDate}" from `__BUILD_TIMESTAMP__`. `ConsumerLayout` does not. Users can't tell when the deployed consumer site was last built — useful for dogfooding.

**Fix:** add to ConsumerLayout footer. Trivial.

---

## Cross-references to prior sessions

- **S2.1 F28** (hardcoded counts drift) — F12 extends this to consumer UI (wineCount badges on 5 page types).
- **S2.3 F1** (marquee wines broken) — F1 + search_catalog results verified: "romanee-conti" returns obscure cuvees + an "Assortment Case", "vega sicilia" returns "Especial (Blend" with unclosed paren, no Unico variant.
- **S2.3 F2 / S2.5 F2** (Chardonnay+Pinot Blanc) — F3 quantifies UI impact at 2,914 wine pages.
- **S2.3 F3** (marquee producer metadata) — F4 extends to all 10,676 producers; producer page is structurally empty.
- **S2.3 F14 / S2.4 F18 / S2.6 F5** (volcanic contamination) — F6 quantifies UI impact at 16,429 wine pages rendering "volcanic" soil claims unattributed.
- **S2.4 F10** (classification_level German einzellage dominance) — F23 affects Tag rendering.
- **S2.5 F4** (enrich-wine edge function reads grapes.name) — F15 extends to dev WineDetail.tsx.
- **S2.5 F18** (lwin_long_tail without display_name) — F1 shows the inverse: long-tail wines without `name` but with `display_name`. Scope not purely LWIN — 12,083 total across all batches.
- **S2.6 F3** (confabulated facts render despite voice rules) — F5 (no disclaimer) means users have no indication content is AI.
- **S2.6 F4** (487 Chardonnay+Pinot Blanc wines with confabulated wine_insights) — 493 confirmed live, rendered as `ai_hook` italic sub-heading (F3 compounding).
- **S2.6 F5** (contamination feedback loop) — F6 is the UI manifestation of the soil-profile leg.
- **S2.6 F7** (99% Grade C missing food_pairing) — F22 makes this invisible to users.
- **S2.6 F8** (wine_food_pairings structured table empty) — no UI rendering path for structured pairings either.
- **S2.6 F9** (US-only appellation_insights) — F16 compounds: AppellationPage doesn't even render `ai_overview` on the 82 US AVAs that do exist. When Sprint 5 adds European coverage, the render path is still missing.

---

## Meta-patterns for S2.9 synthesis

1. **The consumer frontend has no error path.** F2 is a one-character typo that invalidates 100% of country pages and has been silently shipping. F9 (no .catch anywhere) is the structural reason no one noticed. Sprint 3 pre-req: wire an error boundary + `.catch()` to every fetch. Everything else is downstream of this.

2. **"Dead fetches" are endemic.** F16-F19 find 8 `ai_*` fields fetched by consumer pages and never rendered. Every one of them is a column the enrichment pipeline populates. Sprint 5 could generate the richest Chambertin appellation insight possible, and the user would never see it. Pattern fix: when touching consumer pages in Sprint 3 voice module consolidation, cross-reference the SELECT with the JSX.

3. **Structural empty states are not handled.** F4 (ProducerPage), F24 (empty Section headers), F22 (food pairing invisible on 99% of C-grade wines), F10 (no 404 catch-all) — all point at "the UI was designed for a world where every field was populated." Sprint 3 UI fix: consolidate shared components (F27) and add content-aware section rendering (F24) before Sprint 4/5 regeneration.

4. **AI content rendering has no safety rail.** F5 + F6 + F23 all point at the same hole — once an `ai_*` field is rendered, the user has no way to tell it's AI, no confidence, no source attribution, no cross-layer labeling. This is the product's credibility surface and it's currently invisible. Fix is a single new component (`<AIBadge>` / `<AIContent>` wrapper) applied everywhere.

5. **The dev explorer has more developer attention than the consumer.** F14 (dev has proper external link handling, consumer doesn't), F15 (dev WineDetail has the grapes.name bug that consumer WinePage doesn't), `useEntityDetail` hook has error handling but is only used by the dev explorer. The dev explorer accreted incrementally while the consumer pages were built in one sprint and frozen. Sprint 3 should either decommission the dev explorer or formally mark it as "internal — inconsistencies expected."

6. **Every P0 here is a trivial-effort fix, but they've been silently shipping.** F1 (add display_name fallback: 5 min), F2 (rename column: 1 min), F7 (fix footer link: 1 min), F8 (delete dead route: 2 min). Combined ≤30 minutes of work; impact is "no country page is broken anymore, no wine page has an empty h1, no dead routes." Sprint 3 should triage P0 first-day-batch fixes separately from the data work.

## Sprint 3 impact

**Pre-req additions (fast UI hygiene, ~3-4 hours combined):**
- F2: rename `ai_signature_grapes` → `ai_signature_styles` in CountryPage (1 min)
- F1: add `display_name` to wines SELECT + fallback render chain (5 min)
- F7: fix footer About link (1 min)
- F8: delete or park `/vineyard/:id` route (5 min)
- F9: add error boundary + `.catch()` on all fetches (2 hours)
- F10: add 404 catch-all route (5 min)
- F11: remove `producer_insights` from Dashboard table list (1 min)
- F14: make producer website URL clickable (5 min)
- F15: dev WineDetail use `display_name` (1 min)
- F16-F19: render the 8 dead-fetch AI fields (30 min)
- F20: a11y baseline — aria-current, htmlFor, aria-live (2 hours)
- F21: change `<h3>` to `<h2>` in shared Section (5 min)
- F22: wire `wine_food_pairings` structured rendering path on WinePage (1 hour)
- F23: render `system_name: level_name` on classifications (5 min)
- F24: Section component content-aware conditional render (15 min)
- F27: consolidate shared consumer components (half day, optional — reduces all other effort by 8x)

**Data-dependent (must wait on Sprint 3 data fixes):**
- F3 (Chardonnay+Pinot Blanc): grape-repair workstream 3a-3e + 3c.5-3c.7
- F4 (producer metadata): S2.3 F3 producer seed + broader backfill — needs strategy call
- F6 (volcanic contamination): cleanup confabulated appellation_insights or wait for Sprint 5 regen
- F12 (wineCount inflation): honest display now, data-layer fix via enrichment later
- F13 (duplicate wine names): fuzzy merge pass

**Sprint 5 constraints locked by S2.7:**
- Reference regeneration must happen before wine regeneration (already locked by S2.6 F5; F6 re-confirms at UI layer).
- GRADE_C_FIELDS schema widening (S2.6 F7) must happen BEFORE wine regen — because F22 means only Grade B currently shows food pairing, and Grade B is behind the `ENRICHMENT_ENABLED=false` flag.
- Every new `ai_*` field added in Sprint 5 prompts must have a matching consumer render (F16-F19 lesson: the pipeline generates but the UI doesn't show).
- AI disclaimer + confidence badge (F5) must ship before `ENRICHMENT_ENABLED` flag flips.

**Running Sprint 2 totals:** 213 findings across S2.1+S2.2+S2.3+S2.4+S2.5+S2.6+S2.7, $0 spent / $25 ceiling. **35 P0 + 56 P1 = 91 pre-Sprint-3 blockers** (less after overlap dedup in S2.9).

No scope-breakers. S2.7 is the last pre-Sprint-3 session focused on code and data layers — S2.8 (meta) and S2.9 (business/synthesis) are the remaining audit work.
