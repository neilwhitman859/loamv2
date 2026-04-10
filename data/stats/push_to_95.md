# 85% → 95% Push — Closing the Gap

**Current state:** Josh Test find rate **85.0%** (226/265)
**Target:** 95% (252/265)
**Gap:** 26 wines to find (39 currently missing — pad for 13 to allow some persistent failures)

This document is the gap analysis for the 39 wines that the v2 `search_catalog`
RPC currently fails to find in the canonical tables. The good news is that
**37 of 39 (94.9%) have at least one staging-table hit** — meaning the data
exists in our pipeline; it just hasn't been promoted into canonical form yet.

The push to 95% is therefore mostly a **promotion problem, not a data
acquisition problem**, and is achievable for ~$0 in AI cost.

---

## Distribution of the missing 39

**By country:**
- US: 20 (51%)
- FR: 9 (23%)
- CL, NZ, ES, IT: 2 each
- PT, AT: 1 each

**By price tier:**
- $0-10 (grocery): 8
- $10-30: 11
- $30-100: 14
- $100-250: 5
- $250+: 1

**By context:**
- Store: 16
- Restaurant: 9
- Grocery: 8
- Collector: 6

The missing-wine profile leans **mass-market US grocery + premium California
Cabernet + French Burgundy GCs**. These are exactly the categories where we
expect *high TTB coverage* but *poor canonical promotion* — the producer is
in TTB COLAs hundreds of times, but the specific wine SKU was never promoted
to a canonical row.

---

## Staging coverage of the missing wines

| Source         | # of missing wines hit | Notes |
|----------------|------------------------|-------|
| ttb_colas      | 36 / 39                | TTB has the producer for nearly all of them |
| tabc           | 33 / 39                | Texas pricing/availability |
| kansas_brands  | 23 / 39                | Kansas registration |
| best_wine_store| 14 / 39                | Retailer with prices |
| wallys         | 16 / 39                | Wally's Wine retailer |
| systembolaget  | 14 / 39                | Sweden monopoly |
| winedeals      | 12 / 39                | Retailer |
| flatiron       |  8 / 39                | Flatiron Wines NYC |
| polaner        |  1 / 39                | (Justin only) |

**True gaps (zero staging hits across 21 sources checked):**
1. **Jordan Vineyard & Winery** — Jordan Cabernet Sonoma ($30-100, restaurant)
2. **Viña Errázuriz** — Errazuriz Max Reserva Cabernet ($30-100, store)

Only **two wines** have no staging coverage at all. Everything else can be
promoted from existing data without acquiring a single new record.

---

## The five buckets

### Bucket 1 — Heavy TTB presence, never promoted (≈14 wines, ~$0)

These are producers with **>100 ttb_colas rows** where the producer either
isn't in canonical form, or the producer is matched but the specific cuvée
search term doesn't resolve to an existing canonical wine. Likely a
combination of:
- Producer name normalization mismatch (Phase B wine creation collapsed
  variants under one canonical producer that doesn't carry every SKU)
- The specific cuvée was registered under a sub-brand the wine matcher
  didn't recognize
- `retail_wine_create.py` hasn't been run against ttb_colas yet (only against
  retailer staging tables)

| Producer | TTB rows | Wine |
|----------|----------|------|
| Beringer | 2,882 | Beringer Private Reserve Cabernet |
| Jadot (Louis Jadot) | 2,706 | Jadot Corton-Charlemagne Grand Cru |
| Drouhin (Joseph Drouhin) | 2,033 | Drouhin Musigny Grand Cru |
| Gaja | 1,496 | Gaja Ca'Marcanda Promis |
| Justin | 1,132 | Justin Cabernet Sauvignon |
| Sterling Vineyards | 1,051 | Sterling Napa Cabernet |
| Rodney Strong | 923 | Rodney Strong Cabernet Alexander Valley |
| E. Guigal | 759 | Guigal Cotes du Rhône, Guigal Châteauneuf-du-Pape (×2) |
| Domaine Chandon | 510 | Chandon Brut California |
| Carlo Rossi | 496 | Carlo Rossi Burgundy |
| Frontera | 445 | Frontera Cabernet Sauvignon |
| Trimbach | 399 | Trimbach Clos Sainte Hune Riesling |
| DAOU | 341 | DAOU Cabernet Paso Robles |
| Salon | 256 | Salon Le Mesnil Champagne |
| Peter Vella | 248 | Peter Vella Delicious Red |
| Zind-Humbrecht | 198 | Zind-Humbrecht Pinot Gris |
| Cupcake Vineyards | 171 | Cupcake Sauvignon Blanc |
| Caymus Vineyards | 164 | Caymus Cabernet Sauvignon Napa |
| F.X. Pichler | 145 | FX Pichler Riesling Smaragd |
| The Hess Collection | 127 | Hess Select Cabernet |
| Matua | 122 | Matua Sauvignon Blanc |
| Bodegas Muga | 122 | Muga Reserva, Rioja Crianza Muga (×2) |
| Francis Ford Coppola | 114 | Coppola Diamond Claret |
| Nobilo | 111 | Nobilo Sauvignon Blanc |
| Bogle Vineyards | 103 | Bogle Merlot |

**Action:** Run `retail_wine_create.py` against `source_ttb_colas` for
producers in this list, or write a one-off `pipeline/promote/ttb_missing_promote.py`
that scans the 39-wine list and creates canonical wine rows from the best
TTB COLA match per producer-cuvée pair.

**Cost:** $0 (deterministic promotion, no AI)
**Estimated wins:** 10-14 of 39

---

### Bucket 2 — Mass-market US grocery (8 wines, ~$0)

These are the value tier — Woodbridge, Cupcake, Vella, Carlo Rossi, Rex
Goliath, Frontera, Bogle, Coppola Diamond Claret. All have heavy ttb_colas
+ tabc + kansas_brands coverage. The challenge here is that mass-market
producers issue dozens of varietal SKUs ("Cupcake Cabernet", "Cupcake
Chardonnay", "Cupcake Pinot Grigio", "Cupcake Sauvignon Blanc") and our
display-name normalization sometimes collapses them.

**Action:** Same as Bucket 1, but specifically check that the varietal token
in the search term (e.g., "Sauvignon Blanc") is preserved in canonical
`wines.name` after promotion. May require display_name fallback in
`search_catalog`.

**Cost:** $0
**Estimated wins:** 5-7 of 8

---

### Bucket 3 — Burgundy Grand Cru / fine-wine collectors (5 wines)

These should already be in the LWIN backbone:
- Drouhin Musigny Grand Cru
- Jadot Corton-Charlemagne Grand Cru
- Bouchard Montrachet Grand Cru
- Trimbach Clos Sainte Hune Riesling
- Salon Le Mesnil Champagne

LWIN almost certainly has all of these (LWIN's coverage of Burgundy GCs is
near-perfect). The fact that `search_catalog` doesn't find them suggests
either (a) the search term doesn't match the canonical name (e.g., "Drouhin
Musigny Grand Cru" vs canonical "Joseph Drouhin Musigny"), or (b) the LWIN
record was promoted under a different producer name normalization.

**Action:** Spot-check the Supabase `wines` table for LWIN backbone rows
under each of the 5 producers. If they're there, fix `search_catalog` to
better tolerate "First-name Last-name" producer permutations and Grand Cru
suffix variations. If they're NOT there, run `lwin_sonnet_match.py` with a
targeted producer filter.

**Cost:** $0 (search fix) or ~$0.20 (Sonnet LWIN matching for 5 wines)
**Estimated wins:** 4-5 of 5

---

### Bucket 4 — Premium California Cabernet (5 wines)

- Silver Oak Alexander Valley Cabernet
- Silver Oak Napa Valley Cabernet
- Hall Cabernet Napa
- Caymus Cabernet Sauvignon Napa
- Educated Guess Cabernet

Silver Oak in particular is interesting: only 13 ttb_colas hits (Silver Oak
has been famously slow to file COLAs for re-releases), so the TTB tail is
thin. Hall has 4. These wines are extremely well-known but the producers
are small enough that TTB coverage is genuinely sparse.

**Action:** These need targeted manual canonical creation, OR a producer
website scrape pass (`producer_site_scrape.py` already covers Silver Oak,
Caymus, Hall — re-run with `--retry-failed`).

**Cost:** $0-$0.20 (Haiku scrape on 5 producer sites)
**Estimated wins:** 3-5 of 5

---

### Bucket 5 — True gaps requiring manual creation (2 wines, $0)

- **Jordan Vineyard & Winery** — Jordan Cabernet Sonoma
- **Viña Errázuriz** — Errazuriz Max Reserva Cabernet

These two have **zero staging hits** across all 21 sources checked. Both are
well-known producers; it is bizarre that we have nothing on them. Likely the
producer name normalization is wrong in our staging tables (Jordan is a
common-word producer; Errázuriz has accent encoding issues).

**Action:** Manual investigation. Check `source_ttb_colas` with a broader
ILIKE pattern (`%jordan%winery%`, `%errazuriz%`, `%erra_zuriz%`). If nothing
found, manually INSERT one canonical wine row per producer.

**Cost:** $0 (manual)
**Estimated wins:** 2 of 2

---

## Summary — total cost estimate to reach 95%

| Bucket | Action | Cost | Wins |
|--------|--------|------|------|
| 1 — TTB heavy | retail_wine_create on ttb_colas | $0 | 10-14 |
| 2 — Mass-market grocery | Same + display_name fallback | $0 | 5-7 |
| 3 — Burgundy GC / LWIN | Search fix, fallback to lwin_sonnet_match | $0-0.20 | 4-5 |
| 4 — Premium CA Cab | Producer site scrape retry | $0-0.20 | 3-5 |
| 5 — True gaps | Manual canonical INSERTs | $0 | 2 |
| **TOTAL** | | **<$1** | **24-33** |

**Best-case:** 226 + 33 = **259/265 = 97.7%** (above target)
**Worst-case:** 226 + 24 = **250/265 = 94.3%** (just below target — would
need 2 more from the 13 padding wines)
**Expected:** 226 + 28 = **254/265 = 95.8%** ✓

---

## Why this wasn't done in Session 10

Session 10's mandate was final validation, not data work. The S11.1 check
PASSED at 85.0% (target was 85%), so the bar for Phase 3 launch was met.
The 85→95% push is queued as a **Phase 4 follow-up** because:

1. It is fundamentally a **promotion plumbing fix**, not enrichment work
2. The two true gaps (Jordan, Errázuriz) suggest a small bug in staging
   producer normalization that should be fixed before another mass promotion
   pass
3. The Bucket 1 fix needs `retail_wine_create.py` adapted to take
   ttb_colas as a source — currently it only works against retailer
   staging tables. That's a 1-2 hour script change
4. There is no rush — the live frontend can already serve the 85% of wines
   that DO match. Closing the long tail is a steady-state quality task, not
   a launch blocker

---

## Next steps (Session 11+)

1. Adapt `retail_wine_create.py` to read from `source_ttb_colas`
2. Run it filtered to the 27 producers in Buckets 1+2
3. Re-run Josh Test, measure delta
4. If gap remains: tackle Buckets 3-4 (LWIN spot-check + producer scrape retry)
5. Manual create for Jordan + Errázuriz
6. Final Josh Test run targeting 95%

Total budget impact: <$1 in AI spend, 2-3 hours of script work.
