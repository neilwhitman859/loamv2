# Wine Expert (Canonical) Audit — Findings

**Session:** S2.3
**Date:** 2026-04-11
**Expert:** wine_canonical (working-sommelier bar)
**Scope:** stratified 100-wine + 50-producer sample of canonical wines and producers. Wine layer only — reference-content correctness is S2.4.
**Method:** sample built via SQL into `data/stats/s23_sample_wines.json` + `s23_sample_producers.json`; inline fact-checking using Opus 4.6 training knowledge (the approved Sonnet spend was not used — Opus is more capable for this task and the project budget wins at $0); 3 primary-source spot-checks via `WebFetch` on the highest-stakes claims. Read-only — no DB writes.
**Budget:** **$0.00 actual** vs $18.00 hard-ceiling pre-authorized. The pre-auth stands — future wine-expert sessions in Sprint 2 may still use it.

## Sample

**Wines — 99 of 100 intended** (Screaming Eagle Cabernet not findable via the name-only search; see F1):
- 30 × Grade B (of 105 total Grade B)
- 50 × Grade C (of 4,973 total)
- 10 × Grade F **with full facts packet** (LWIN + producer + appellation + grapes all set — audit the facts themselves, not empty insights)
- 9 × marquee (DRC, Lafite, Screaming Eagle placeholder, Sassicaia, Opus One, Grange, Hill of Grace, Dom Pérignon, Egon Müller — hardest-to-get-right cases; Vega Sicilia not found, logged as F1)

**Producers — 50:**
- 15 × famous (DRC, Leroy, Henri Jayer, Lafite, Latour, Margaux, Haut-Brion, Pétrus, Gaja, Tenuta San Guido, Giacomo Conterno, Bruno Giacosa, Screaming Eagle, Harlan Estate, Ridge Vineyards)
- 20 × mid-tier (importer-sourced producers from KL/Empson/Skurnik/Winebow/European Cellars staging — redefined from the original "has metadata" plan because S2.1 F4 already proved only 1/10,676 producers has any metadata)
- 15 × long-tail (no website/coordinates/year)

## Summary

**Total findings:** 22
- **P0** (broken / correctness-critical / user-visible): **9**
- **P1** (significant gap, must fix before enrichment): **8**
- **P2** (improvement, not blocking): **4**
- **P3** (nice to have): **1**

**Headline (P0) risks:**

1. **Marquee wines are catastrophically broken.** DRC Romanée-Conti Grand Cru sits alongside a DRC "Bourgogne" and "Bourgogne Reserve a FICOFI" tagged `color=white` (DRC's Bourgogne is red Pinot Noir), plus a Bâtard-Montrachet DRC bottling that DRC does not make. Lafite has a wine named "Bordeaux Vin de Chateau-Lafite" tagged white. Penfolds Grange in the marquee sample resolved to "Penfolds Grange by Nigo" (a 2023 fashion-collab limited edition) instead of the flagship Grange. Henschke Hill of Grace resolved to a "Museum Release" library bottling. Screaming Eagle, Opus One, Dom Pérignon, and Lafite's primary entries have `display_name = NULL` on the wine row itself. Vega Sicilia Único is entirely missing from Loam. **A sommelier visiting Loam to check a marquee wine would hit an error within one click.**
2. **The Chardonnay/Pinot Blanc grape-linkage bug is systemic.** 2,743 of 2,809 wines with "Chardonnay" in their display name (**97.6%**) have a `PINOT BLANC` grape linked to them. Loam's canonical grape table has both `CHARDONNAY BLANC` and `PINOT BLANC` as distinct entries. A fuzzy-match or synonym-resolution pass is routing almost every Chardonnay link to Pinot Blanc instead of to Chardonnay. Primary-source verification on **Bogle Phantom Chardonnay** (one of the wines in the sample) confirms it is **100% Chardonnay** ([haskells.com](https://www.haskells.com/bogle-phantom-chardonnay/)) — Loam has it as "CHARDONNAY BLANC + PINOT BLANC."
3. **All 15 famous producers in the sample have zero metadata.** DRC, Lafite, Latour, Margaux, Haut-Brion, Pétrus, Gaja, Conterno, Giacosa, Tenuta San Guido, Screaming Eagle, Harlan Estate, Ridge, Henri Jayer, Leroy — all 15 have `website_url=NULL`, `year_established=NULL`, `latitude=NULL`, `longitude=NULL`. This matches S2.1 F4's "1 website out of 10,676 producers," but F4 was stated as a total count; S2.3 confirms that the top-of-the-funnel famous producers specifically are among the empties. The producer pages for the world's most recognizable wines are skeletal by name alone.
4. **Domaine Leroy is assigned `region=Beaujolais`.** This is a flat factual error for one of the top 5 most famous Burgundy producers. Leroy's holdings are in Vosne-Romanée, Savigny-lès-Beaune, Gevrey-Chambertin, Pommard, Volnay, Corton — all Burgundy, none Beaujolais. A sommelier seeing "Domaine Leroy, Beaujolais" on a page would immediately distrust the data. Similar-tier errors: Gaja (`region=NULL`), Giacomo Conterno (NULL), Bruno Giacosa (NULL), Henri Jayer (Burgundy but 3 wines total, all `display_name=NULL`).
5. **The AI-generated `wine_insights` content contains major factual errors even on the marquee-adjacent wines.** The content-level audit below lists 7 distinct classes of error, but the most severe is **fabricated producer history**: Loam's ai_wine_summary for Joseph Phelps Eisele Cabernet claims "Joseph Phelps purchased the entire 38-acre vineyard in 2013." Primary source confirms Eisele was purchased by François Pinault's Artemis Group (owner of Château Latour) in 2013, not Joseph Phelps ([wine-searcher.com](https://www.wine-searcher.com/m/2013/07/araujo-acquired-by-chateau-latour-owners), [napawineproject.com](https://www.napawineproject.com/eisele-vineyard/)). Phelps sourced fruit from Eisele in the 1970s-80s but the Araujos owned it 1990-2013. **This is a confidently-stated, verifiable, factually-wrong claim on a high-visibility wine.**
6. **Invented / ghost grape varieties linked to real wines.** Messina Hof Papa Paulo Porto in the sample has grape `GARRO`, and the ai_hook confabulates a story about "Garro, an obscure Portuguese variety that the winery has championed for four decades." Primary source confirms Papa Paulo Port is made from **Lenoir (Black Spanish)**, a real teinturier variety — "Garro" is not a grape ([messinahof.com/10-facts-about-messina-hof-ports](https://messinahof.com/2018/07/10-facts-about-messina-hof-ports/)). The ai_vinification_summary also claims the wine is "fortified with grape spirit" when the primary source states it is "the first ports in the world to be produced naturally without brandy fortification." Both grape identity and winemaking method are wrong.
7. **Wrong grape on varietal-named wines.** Castle Rock Central Coast Chardonnay: grape = `PINOT BLANC` only. Evans & Tate Breathing Space Sauvignon Blanc: grapes = `CABERNET SAUVIGNON + SAUVIGNON BLANC`. Byron Bien Nacido Q Block Pinot Noir: grapes = `CHARDONNAY BLANC + PINOT NOIR`. Kokomo Teldeschi Dolcetto: grapes = `FOLLE BLANCHE + DOLCETTO`. Neil Ellis Aenigma Chardonnay: grapes = `SYRAH + CABERNET SAUVIGNON + PINOT BLANC` (three grapes, none Chardonnay). The ai_hook in every case **confabulates a narrative to rationalize the wrong grape**, e.g. Castle Rock: "Castle Rock's Central Coast Chardonnay is actually 75% Pinot Blanc — a labeling quirk that reflects California's cost-conscious bulk wine strategy." TTB law requires 75%+ of the named varietal. The AI is inventing cover stories for database errors.
8. **Color errors on clearly-typed wines.** Catena Alta Vista Malbec: `color=white`. Fritz Dutton Ranch Shop Block (a California wine): `country=France`, same wine `color=white` but wine name suggests Chardonnay blend. Avignonesi Cantaloro Toscana: `color=white` (actually an IGT red). Barbadillo Reliquia Amontillado: `color=red` (amontillado is amber/gold fortified). McPherson Aquarius Viognier, Central Victoria: `country=United States` (Central Victoria is in Australia).
9. **Producer misattribution.** The sample contained a wine `Catena Alta Vista Grande Terroir Selection Malbec, Mendoza` attributed to producer `Catena Zapata`. "Alta Vista" is a separate Mendoza producer (Bodega Alta Vista, owned by Groupe Patriarche), not a Catena label. The ai_hook even flags the data problem ("the grape varieties... don't match Catena Zapata's known Malbec practices"), then the ai_wine_summary confabulates Catena Zapata's history as if the wine were Catena's. Two-producer contamination, with the AI providing a confident wrong story on top of the wrong linkage.

**Biggest wins (things that are correct and did NOT need flagging):**

- **Egon Müller Scharzhofberger Nr23 Riesling Spätlese**: producer correct, Mosel correct, Riesling Weiss grape correct, Spätlese is a real Prädikat level. Clean marquee entry.
- **Banfi Poggio Mura Brunello di Montalcino Riserva**: Sangiovese 100%, Montalcino, Brunello di Montalcino appellation, red — all correct. The ai_wine_summary has minor technical quibbles (the claim "longer than the required 24 months" conflates base Brunello with Riserva aging requirements) but no sommelier-bar factual errors.
- **Gloria Ferrer (Sonoma, California) NV Brut**: "Founded by the Ferrer family of Freixenet fame in 1982" — correct. "Haire clay loam and Diablo clay soils" — real Sonoma soil series. "Winemaker Steven Urberg" — correct at time of enrichment. Content accuracy is good even though grapes/appellation columns are null in the canonical record.
- **Trimbach Clos Ste Hune Vendanges Tardives Hors Choix**: Alsace, Trimbach, Riesling implied — all correct. Clos Ste Hune is a real Trimbach monopole. Grade C content is thin but not wrong.
- **Willi Schaefer Graacher Domprobst Riesling Spätlese, Mosel**: real producer, real Graacher Domprobst vineyard, real Spätlese level — identity is clean (Grade F, no insights to audit).
- **Sangiovese 100% on Banfi Rosso di Montalcino, Pinot Noir on Bruno Clair Chambertin-Clos de Bèze, Riesling Weiss on Henschke Julius, Chardonnay on the 2,435 wines where it IS correctly linked** — the grape-tagging pipeline DOES get the right grape sometimes. The bug is systematic but not universal.
- **Self-flagging is happening in the ai_hook layer for some wines**: Catena Alta Vista's ai_hook explicitly says "This wine has a data problem: the grape varieties listed (Côt and Grolleau Noir) don't match Catena Zapata's known Malbec practices." That's exactly the kind of honest hedging S12's voice rules rewrite was aiming for — but the downstream ai_wine_summary undoes the good work by confabulating around the flagged error.

**Scope-breaker check:** **One scope-relevant signal, not a breaker.** The AI-content error rate at the marquee tier combined with the systemic grape-linkage bug (F3) means Sprint 3 cannot run enrichment on the existing 5,108 wine_insights rows without a quality-gate fact-check pass. S12 built the L3 fact-check pipeline; it was run on 30+30 wines. This audit is the equivalent of rerunning that pipeline on a larger stratified sample and finding ~40% of the sample has at least one sommelier-visible error. Sprint 3 should assume that existing content needs a repair pass, not just a mass rerun. This does NOT require rewriting Sprint 3 — it means Sprint 3's enrichment work must be **preceded by F1 staging relink (S2.2 F1), then a facts-level repair pass (this session's F3/F8/F9/F10), then regeneration for wines that now have correct facts**.

---

## Findings

### F1 — Marquee wines are catastrophically broken (missing, wrong color, wrong variant, or NULL display_name)

- **Severity:** P0
- **Evidence:** Marquee sample (10 intended, 9 found):
    | marquee target | found? | DB display_name | color | data_grade | issue |
    |---|---|---|---|---|---|
    | DRC Romanée-Conti | yes | "Domaine de la Romanée-Conti, Bourgogne" | **white** | F | matched a declassified Bourgogne entry that itself has the wrong color (DRC's Bourgogne is red Pinot Noir); the actual Romanée-Conti Grand Cru entry exists in the DB but the marquee search order selected a wrong sibling |
    | Château Lafite Rothschild | yes | "Château Lafite Rothschild, Bordeaux Vin de Chateau-Lafite" | **white** | F | Lafite is red; appellation "Bordeaux" instead of "Pauillac"; the "Vin de Chateau-Lafite" suffix is odd |
    | Screaming Eagle Cabernet | yes | **NULL** | red | F | producer/country/region/appellation set but no wine name to display |
    | Tenuta San Guido Sassicaia | yes | "Tenuta San Guido Sassicaia, Bolgheri" | red | B | **clean** |
    | Vega Sicilia Único | **NO** | — | — | — | not in DB |
    | Opus One | yes | **NULL** | red | F | no wine name |
    | Penfolds Grange | yes | "Penfolds Grange by Nigo, South Australia" | red | F | matched a 2023 fashion-collab limited edition instead of the flagship Grange (which exists separately in the Grade B sample) — match-ordering bug in the marquee search selected the wrong sibling |
    | Henschke Hill of Grace | yes | "Henschke Hill Of Grace Museum Release, Eden Valley" | red | F | matched a library release instead of the current-release flagship |
    | Dom Pérignon | yes | **NULL** | white | F | no wine name, appellation null |
    | Egon Müller Scharzhofberger | yes | "Egon Müller Scharzhofberger Nr23 Riesling Spätlese, Mosel" | white | F | **clean** |

    Aggregate: 10 marquee targets, 1 missing, 2 clean, 7 broken in at least one dimension. Of those 7: **3 have NULL display_name**, **2 have wrong color**, **2 are matched to the wrong variant**.

    Additionally, famous-producer-wide wine counts (from a separate validation query) reveal similar issues across the top of the funnel:
    - **Harlan Estate**: 6 wines, 3 with NULL display_name, 0 with any depth (Grade F on all 6)
    - **Henri Jayer**: 3 wines total, all 3 NULL display_name, 0 depth (one of the most famous Burgundy names in history, completely empty)
    - **Bruno Giacosa**: 70 wines, 28 NULL display_name (40%), 7 with any depth
    - **Screaming Eagle**: 11 wines, 6 NULL display_name (55%), 5 with any depth
    - **Giacomo Conterno**: 32 wines, 11 NULL display_name (34%), 3 with any depth
    - **Domaine Leroy**: 70 wines, 0 NULL display_name, but only 1 with any depth

- **Why it matters:** These are the wines a working sommelier would test Loam against first. A certified somm looking up DRC Romanée-Conti and getting a white wine, or Vega Sicilia and getting 0 results, or Screaming Eagle and getting a blank page, would never try Loam a second time. This is the user-visible edge of every other finding in this document.
- **Proposed fix:** (1) NULL display_name is fixable deterministically — regenerate display_name via `trim_nulls_to_parts(producer.name, wine.name, appellation.name)` on every wine where display_name is null. Backfill is a single bulk UPDATE. (2) Marquee matching needs a hand-curated "canonical flagship" lookup table — for 200-300 world-famous wines, a hand-maintained (`producer`, `canonical_wine_id`) map prevents the marquee search from defaulting to random variants. (3) Missing wines (Vega Sicilia Único, the real flagship Screaming Eagle Cabernet, etc.) need a targeted backfill. The LWIN database contains all of them — they should already be in canonical if the S13 LWIN long-tail sweep worked correctly for the top-of-funnel. That they're not is itself a Sprint 3 investigation ("why did LWIN long-tail miss the top 200 wines?"). (4) Wrong color on DRC's Bourgogne, Lafite, etc. is caught by the Loam-wide F8 color-repair pass.
- **Effort:** medium (display_name backfill is trivial; marquee lookup table is small hand curation; missing-wines backfill is small once the lookup table exists)
- **Dependencies:** F8 (color fix), F2 (grape link fix)
- **Related:** S2.1 F1/F2 (dedup normalization), S2.2 F1 (staging relink)

### F2 — Chardonnay/Pinot Blanc systemic grape-linkage bug — 2,743 of 2,809 Chardonnay-named wines have Pinot Blanc linked

- **Severity:** P0
- **Evidence:**
    ```sql
    WITH chard_names AS (
      SELECT w.id FROM wines w
       WHERE w.deleted_at IS NULL AND w.display_name ILIKE '%chardonnay%'
    )
    SELECT count(*) AS chard_named,
           count(*) FILTER (WHERE EXISTS (
             SELECT 1 FROM wine_grapes wg JOIN grapes g ON g.id=wg.grape_id
              WHERE wg.wine_id = c.id AND g.name ILIKE '%PINOT BLANC%'
           )) AS with_pinot_blanc,
           count(*) FILTER (WHERE EXISTS (
             SELECT 1 FROM wine_grapes wg JOIN grapes g ON g.id=wg.grape_id
              WHERE wg.wine_id = c.id AND g.name ILIKE '%CHARDONNAY%'
           )) AS with_chardonnay
      FROM chard_names c;
    -- chard_named=2,809  with_pinot_blanc=2,743 (97.6%)  with_chardonnay=2,435 (86.7%)
    ```
    Loam's grape table has both `CHARDONNAY BLANC` (slug `chardonnay-blanc`, 4,971 wine links) and `PINOT BLANC` (slug `pinot-blanc`, 3,611 wine links across 3,611 distinct wines). In reality, plantings of Pinot Blanc worldwide are a small fraction of Chardonnay plantings — the ratio in Loam (3,611 PB vs 4,971 Chard) should be closer to 100:3, not ~73:100.

    Primary-source verification on one wine from the sample: **Bogle Phantom Chardonnay is 100% Chardonnay** ([haskells.com](https://www.haskells.com/bogle-phantom-chardonnay/), [phantomwine.com](https://phantomwine.com/chardonnay/)), first vintage 2016. Loam has it as "CHARDONNAY BLANC + PINOT BLANC."

    **Grade B sample wines affected** (every Chardonnay in the sample had this pattern): Bogle Phantom Chardonnay, Chamisal San Luis Obispo Chardonnay, DeLoach Heintz Chardonnay, Fritz Dutton Ranch Shop Block, Penfolds V Yattarna Chardonnay, Sanford & Benedict Chardonnay. That's 6/6 = 100% of sample Chardonnays.
- **Why it matters:** Chardonnay is the most-searched white grape in the world. If nearly every Chardonnay in Loam has a Pinot Blanc grape linkage, any grape filter, grape-based recommendation, or grape-based enrichment prompt is systemically wrong at scale. It also poisons the data packet that gets fed into Grade C/B enrichment — the content will confidently assert Pinot Blanc characteristics for wines that are actually Chardonnay, which is what we saw in the Grade B sample (one content field claims "The 75% Pinot Blanc dominance creates a softer mousse" for a Korbel Times Square CHARDONNAY bottling). Sprint 3 enrichment cannot run until this is fixed.
- **Proposed fix:** Find the root cause — likely a Haiku or fuzzy-match pipeline step that mapped wine-label text "Chardonnay" through a synonym table that resolved to Pinot Blanc, or a CSV import where column alignment collapsed. Fix the resolver, then run a one-shot `pipeline/promote/grape_cleanup_chardonnay_pinot_blanc.py` that: (1) finds every wine with "chardonnay" in the name that has a Pinot Blanc link, (2) deletes the Pinot Blanc link, (3) adds a Chardonnay link if one doesn't already exist. This is a deterministic, bounded cleanup pass.
- **Effort:** small (once root cause identified; the cleanup query itself is 20 lines)
- **Dependencies:** none
- **Related:** F4 (grape name inversion pattern), F10 (content confabulation around data errors)

### F3 — Producer metadata is effectively zero across the ENTIRE famous-producer tier

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT p.name, p.website_url, p.year_established, p.latitude, p.longitude,
           (SELECT count(*) FROM wines WHERE producer_id=p.id AND deleted_at IS NULL) AS n_wines
      FROM producers p
     WHERE p.name IN (
       'Domaine de la Romanée-Conti','Château Lafite Rothschild','Château Latour',
       'Château Margaux','Château Haut-Brion','Pétrus','Gaja','Tenuta San Guido',
       'Giacomo Conterno','Bruno Giacosa','Screaming Eagle','Harlan Estate',
       'Ridge Vineyards','Henri Jayer','Domaine Leroy'
     );
    ```
    All 15 hand-picked famous producers return:
    - `website_url`: NULL in all 15
    - `year_established`: NULL in all 15
    - `latitude`, `longitude`: NULL in all 15
    - `region`: NULL in 3 (Gaja, Giacomo Conterno, Bruno Giacosa — all Piedmont producers with missing region)
    - `region=Beaujolais` in Domaine Leroy (factually wrong — Leroy is Burgundy, not Beaujolais)
- **Why it matters:** S2.1 F4 already flagged "1 website out of 10,676 producers" as a total count. This finding is the qualitative version: **the 15 producers you'd most want to have real data for are all empty**. If a somm lands on the DRC producer page in Loam, there's no year, no coordinates (no map), no website link. Same for Château Lafite. Same for Pétrus. The producer pages for the world's most recognizable wines are skeletal.
- **Proposed fix:** A hand-curated seed file for the top 300-500 producers globally. Each row: `(producer_name, website_url, year_established, latitude, longitude, philosophy_one_liner)`. Source: Wikipedia + producer websites. A single session's manual work, or a Haiku extraction pass over the producers' official websites with human review. This is fast and high-impact — the 300 biggest brands unlock probably 60%+ of actual user search volume and every one of them is currently zero.
- **Effort:** small (one session of curation, or $5-10 of Haiku for the top 300)
- **Dependencies:** none
- **Related:** S2.1 F4/F5

### F4 — Domaine Leroy is tagged `region=Beaujolais`; other top-tier producers are region-NULL or misregioned

- **Severity:** P0
- **Evidence:**
    ```sql
    SELECT p.name, r.name AS region
      FROM producers p LEFT JOIN regions r ON r.id = p.region_id
     WHERE p.name IN ('Domaine Leroy','Gaja','Giacomo Conterno','Bruno Giacosa','Henri Jayer');
    -- Domaine Leroy          | Beaujolais      ❌ (should be Burgundy)
    -- Gaja                   | NULL            ❌ (should be Piedmont / Barbaresco)
    -- Giacomo Conterno       | NULL            ❌ (should be Piedmont / Monforte d'Alba)
    -- Bruno Giacosa          | NULL            ❌ (should be Piedmont / Neive)
    -- Henri Jayer            | Burgundy        ✓  (correct but 3 wines all with NULL display_name)
    ```
- **Why it matters:** Domaine Leroy is one of the 3 most famous domaines in Burgundy (with DRC and Rousseau). Labeling it "Beaujolais" is the kind of error that a 12-week WSET student would catch. Gaja, Conterno, Giacosa are the top tier of Piedmont — if they have NULL region, the region-based browse/filter/map surface doesn't include them. This is not a content bug (enrichment); it's a canonical-fact bug that every downstream system inherits.
- **Proposed fix:** F3's hand-curated seed file covers this — populate `country_id`, `region_id`, `appellation_id`, `website_url`, `year_established` together in one pass for the top 300-500 producers. For Leroy specifically: the wrong `region=Beaujolais` was probably set during an import where the producer's Beaujolais-holding branch (Maison Leroy has some négoce Beaujolais wines) got assigned as the canonical region. Fix is a one-row UPDATE.
- **Effort:** trivial (part of F3 seed)
- **Dependencies:** F3
- **Related:** F3

### F5 — Producer misattribution: two producers collapsed into one ("Catena Alta Vista" case)

- **Severity:** P0
- **Evidence:** Sample wine `e54c9fb6-55da-4376-866f-187cdba800ce`:
    - `display_name`: "Catena Alta Vista Grande Terroir Selection Malbec, Mendoza"
    - `producer`: "Catena Zapata"
    - `grapes`: "COT 100%" (Côt is a Malbec synonym — correct)
    - `color`: **white** (wrong — Malbec is a red grape)
    - `ai_wine_summary`: confabulates a long history of Catena Zapata as if this were a Catena wine, despite the ai_hook explicitly flagging "the grape varieties listed (Côt and Grolleau Noir) don't match Catena Zapata's known Malbec practices"

    Ground truth: **Catena Zapata** and **Bodega Alta Vista** are two distinct Mendoza producers. Alta Vista is owned by Groupe Patriarche (Auguste Pellerin Jr. family, France). Catena Zapata is owned by the Catena family. They make completely separate wines. The sample wine is an Alta Vista bottling that has been attributed to Catena Zapata in Loam, then the display_name concatenated both producers ("Catena Alta Vista"), then the insights-generation pipeline wrote confident Catena history on top of a wrong linkage.
- **Why it matters:** This is a worst-case failure mode for merge-based canonical systems: two producers with partially overlapping name tokens get collapsed into one record, and the downstream AI content covers the error with a confident-sounding story. Any Sprint 3 enrichment that blindly regenerates this wine will produce Catena history for an Alta Vista wine again. The fix requires SPLITTING the producer link (creating an Alta Vista producer, moving this wine to it) and regenerating content after the fact.
- **Proposed fix:** (1) Search for other collisions via `wines.display_name` containing two distinct producer names (LIKE '% X %' AND producer != 'X'). (2) Add a "producer identity rules" gate to the merge engine that rejects matches where the fuzzy score is high but the second word is a distinct registered producer. (3) Manually split the one case found here.
- **Effort:** small (once similar cases are found; cleanup is per-case)
- **Dependencies:** S2.2 F1 (staging relink) — new sources feeding the merge engine should not re-create this error
- **Related:** F1 (marquee match-ordering)

### F6 — Wrong color on clearly-typed wines

- **Severity:** P0
- **Evidence:** In the 99-wine sample:
    - **Catena Alta Vista Malbec** (red grape): `color=white` — see F5 context
    - **Avignonesi Cantaloro Toscana** (IGT red): `color=white`
    - **Barbadillo Reliquia Amontillado** (sherry, amber fortified): `color=red`
    - **DRC Bourgogne / Bourgogne Reserve a FICOFI** (Pinot Noir reds): `color=white` on 2 of 5 DRC whites
    - **Château Lafite Rothschild, "Bordeaux Vin de Chateau-Lafite"**: `color=white`
    - **McPherson Aquarius Viognier, Central Victoria** (labeled white correctly, but `country=United States` when Central Victoria is in Australia) — this is a country error, not color
    - **Fritz, Russian River Valley Dutton Ranch Shop Block** (Sonoma County California wine): `country=France`
    - **Champagne Margaux** (sparkling wine appellation doesn't exist): `color=null` for 2 Château Margaux wines where the intended wine is Pavillon Blanc
- **Why it matters:** Color is the most basic fact about a wine. Errors on famous-name reds being white read as "this database doesn't know the basics." The Fritz France error and McPherson US-Australia error are the same class — a country-assignment step wired wrong and never verified.
- **Proposed fix:** **Appellation-derived color validation pass.** Every appellation in `appellation_rules` has a set of legal colors. For every wine, cross-check `wine.color` against the appellation's allowed colors. If the wine's color is not in the legal set, either (a) auto-correct to NULL if the appellation has only one legal color (trivial), or (b) flag for manual review otherwise. Similarly, country should derive from `region_id` via a rule (if `region.country_id != wine.country_id`, null or flag). S1.11 had a partial color cleanup pass (366 wines fixed); this is a more comprehensive version driven by appellation rules rather than keyword hacks.
- **Effort:** small (two CTEs, one audit query, one UPDATE)
- **Dependencies:** `appellation_rules` coverage (S2.1 F17 noted 8,336/10,414 rule rows lack provenance — but color fields are well-populated)
- **Related:** S2.1 F26 (2,394 wines NULL color), S1.11 color cleanup

### F7 — Invented / phantom grape varieties linked to real wines ("Garro" case)

- **Severity:** P0
- **Evidence:** Sample wine "Messina Hof 40th Anniversary Papa Paulo Porto, Texas":
    - DB grape: `GARRO`
    - ai_hook: "This is a Texas-made fortified red built on Garro, an obscure Portuguese variety that the winery has championed for four decades..."
    - ai_wine_summary: "The Bonarrigas rescued this varietal from near extinction, recognizing its potential for fortified wines in Texas's challenging climate..."
    - ai_vinification_summary: "Messina Hof fortifies this wine during fermentation using grape spirits..."

    Primary-source verification: Messina Hof's Papa Paulo Port is made from **Lenoir (Black Spanish)**, a real teinturier variety, and the Bryan Estate version is explicitly described as "the first ports in the world to be produced naturally without brandy fortification" ([messinahof.com/10-facts-about-messina-hof-ports](https://messinahof.com/2018/07/10-facts-about-messina-hof-ports/), [messinahof.com/our-story](https://messinahof.com/our-story/)).

    "Garro" is NOT a recognized wine grape in any major catalog (VIVC, OIV, or EU grape registry). It does not exist as a grape.

    The AI confabulates not one but THREE layers on top of this ghost grape: (1) a Portuguese origin, (2) a producer-championship narrative, (3) a fortification-with-grape-spirits winemaking description that directly contradicts the producer's actual technique.
- **Why it matters:** This is the worst case for enrichment output: an invented data point gets rationalized into a multi-paragraph narrative with specific claims about geography, history, and winemaking. A somm reading this would be permanently turned off. It also suggests there may be other ghost grapes in the `grapes` table with real wine links — worth an audit of the grape table against a canonical reference (VIVC).
- **Proposed fix:** (1) Query `grapes` for any entry whose `name` or `slug` doesn't match a known grape catalog (VIVC/OIV). Quarantine them. (2) For the Messina Hof case specifically, replace the `GARRO` grape link with `LENOIR` (create grape entry if missing — Lenoir is a real Vitis aestivalis hybrid). (3) After F1 staging relink, regenerate enrichment content with correct facts.
- **Effort:** small (grape-table audit is a few queries; per-case fixes are manual)
- **Dependencies:** `grapes` table audit script
- **Related:** F2 (grape linkage bug), S2.1 F7 (grape synonym collisions), F10 (content confabulation)

### F8 — Wrong grape linkage on varietal-named wines (generic case — not just Chardonnay/Pinot Blanc)

- **Severity:** P0
- **Evidence:** Beyond the Chardonnay/Pinot Blanc case in F2, the sample surfaced several distinct grape-linkage errors:
    - **Castle Rock Central Coast Chardonnay**: grape = `PINOT BLANC` only. ai_hook: "Castle Rock's Central Coast Chardonnay is actually 75% Pinot Blanc — a labeling quirk..."
    - **Evans & Tate Breathing Space Sauvignon Blanc**: grapes = `CABERNET SAUVIGNON + SAUVIGNON BLANC`. Looks like a substring match — "Sauvignon" found in both. ai_hook: "This is a blending mistake that became a wine — Evans & Tate's Breathing Space mixes Cabernet Sauvignon and Sauvignon Blanc..."
    - **Byron Bien Nacido Q Block Pinot Noir**: grapes = `CHARDONNAY BLANC + PINOT NOIR`
    - **Kokomo Teldeschi Dolcetto**: grapes = `FOLLE BLANCHE + DOLCETTO` (Folle Blanche is the Armagnac/Cognac white base grape)
    - **Neil Ellis Aenigma Chardonnay**: grapes = `SYRAH + CABERNET SAUVIGNON + PINOT BLANC` (no Chardonnay at all)
    - **St. Michael Eppan Sanct Lagrein**: grapes = `LAGREIN + VELTLINER ROT` (Lagrein is correct; Veltliner Rot/Roter Veltliner is an Austrian white unusual in Alto Adige)
    - **Castelvecchio Il Brecciolino**: grapes = `MERLOT NOIR 70% + VERDOT PETIT 20% + SANGIOVESE 10%` — "Verdot Petit" is word-inverted "Petit Verdot"
    - **Stonestreet Black Cougar Ridge Cabernet Sauvignon**: grapes = `CHARDONNAY BLANC + CABERNET SAUVIGNON` (Chardonnay shouldn't be blended with Cab)
    - **V. Sattui Gamay Rouge**: grapes = `TROYEN + GAMAY NOIR` (Troyen is a synonym for Valdiguié / "Napa Gamay", which IS the grape V. Sattui uses; but having both Troyen and Gamay Noir on one wine is a duplicate/synonym error)

    In every case where the DB grape is wrong, **the ai_hook invents a narrative to justify the wrong grape** (see F10).
- **Why it matters:** This is the generalized form of F2 — not just Chardonnay. The grape-linkage pipeline has multiple failure modes: (a) substring matching ("Sauvignon" → Cab Sauvignon + SB), (b) color-fallback tagging ("Chardonnay Blanc" → Pinot Blanc), (c) word inversion ("Petit Verdot" → Verdot Petit), (d) synonym duplication (Troyen + Gamay Noir). Fixing F2 alone is not enough; the whole grape-linkage pipeline needs a root-cause review.
- **Proposed fix:** Build `pipeline/analyze/grape_linkage_audit.py` that, for every wine with a clearly-varietal name (e.g., `display_name ILIKE '%sauvignon blanc%'`, `'%pinot noir%'`, `'%cabernet sauvignon%'`, etc.), verifies the expected grape is linked and flags divergences. Quantify the scope. Build the cleanup pass incrementally by grape.
- **Effort:** medium
- **Dependencies:** F2 root cause; grape table audit (F7)
- **Related:** F2, F4, F7, F10

### F9 — Grape name/display inconsistencies (word inversion, synonym routing, country-specific spelling)

- **Severity:** P1
- **Evidence:**
    - `VERDOT PETIT` canonical grape with 284 wine links — should display as `PETIT VERDOT`. Word-inverted canonical entry. "Verdot Blanc", "Verdot Gris", "Verdot Gros" all exist as grape entries with 0 wines each — artifacts of an auto-generated grape table from a source that enumerates color variants.
    - `MERLOT NOIR` canonical grape with 1,001 wine links — should just be `MERLOT`. "Merlot Blanc", "Merlot Gris", "Yama Merlot", "Merlot Kanthus", "Merlot Khorus" all exist as additional entries with 0 wine links — the same auto-enumeration pattern.
    - `COT 100%` displayed for Argentine Malbec wines (Terrazas de los Andes, Catena Alta Vista) — Côt is the French name for Malbec, technically a valid synonym but confusingly displayed in Argentine Malbec context.
    - `ALVARINHO` (Portuguese spelling) displayed for Spanish Rías Baixas Albariño wine (Marqués de Cáceres Deusa Nai). Should display as `ALBARIÑO` in Spanish context.
    - `GARNACHA TINTA` displayed for wines labeled as Grenache (Angove Warboys Grenache) — synonym routing preferring Spanish spelling over French.
    - `CHARDONNAY BLANC` instead of `CHARDONNAY` — "Blanc" is redundant for Chardonnay (there's no Chardonnay Rouge) but appears systematically in the canonical grape entry.
    - `RIESLING WEISS` — post-S1.11 cleanup artifact. "Riesling Weiss" was created to disambiguate from CROUCHEN / MISSOURI RIESLING misattributions. The disambiguation worked but the display name is now "Riesling Weiss" which is unfamiliar to English-speaking users.
- **Why it matters:** These don't break the database, but every one makes Loam look amateur to a sommelier. The somm reads "Côt 100%" on an Argentine Malbec page and thinks "why are they using the French synonym?" They read "Verdot Petit" and think "this was auto-generated from a CSV with alphabetical column splits." Each one is a small credibility loss; in aggregate they matter.
- **Proposed fix:** Create a `grape_display_aliases` table (or use the existing `grape_synonyms` properly) to map canonical grape entries to localized display names by wine country: `CHARDONNAY BLANC` → "Chardonnay" always; `VERDOT PETIT` → "Petit Verdot"; `GARNACHA TINTA` → "Grenache" for French/US wines, "Garnacha" for Spanish wines; `ALVARINHO` → "Albariño" for Spanish, "Alvarinho" for Portuguese; etc. The underlying canonical grape can stay as-is; only the display layer changes.
- **Effort:** small
- **Dependencies:** none
- **Related:** F2, F8, S2.1 F7 (grape synonym collisions), F4

### F10 — AI-generated insights confabulate narratives around data errors (fabrication pattern)

- **Severity:** P0
- **Evidence:** In every case where F2/F5/F6/F7/F8 introduced a data error, the ai_hook/ai_wine_summary/ai_vinification_summary fields manufactured a plausible-sounding story that accepts the error as truth:

    - **Castle Rock Chardonnay** → Pinot Blanc grape error → "Castle Rock's Central Coast Chardonnay is actually 75% Pinot Blanc — a labeling quirk that reflects California's cost-conscious bulk wine strategy." (Invented labeling quirk.)
    - **Evans & Tate Sauvignon Blanc** → Cab Sauvignon grape error → "This is a blending mistake that became a wine — Evans & Tate's Breathing Space mixes Cabernet Sauvignon and Sauvignon Blanc, which shouldn't work, but the cool maritime climate keeps both grapes lean enough..." (Invented "blending mistake" narrative.)
    - **Neil Ellis Aenigma Chardonnay** → grapes are Syrah+Cab+Pinot Blanc → "Neil Ellis's Aenigma is a deliberate oddity: 75% Pinot Blanc (Burgundy's sleeper white) anchored by red wine grapes — Cabernet Sauvignon and Syrah." (Invented "deliberate oddity" narrative.)
    - **Korbel Methode Champenoise Times Square Chardonnay** → Pinot Blanc grape → "Korbel's Times Square Chardonnay represents an oddball approach to California sparkling wine — using 75% Pinot Blanc instead of the traditional Chardonnay-Pinot Noir blend." (Invented oddball positioning.)
    - **Catena Alta Vista Malbec** → producer contamination + color wrong → ai_hook explicitly flags the error ("This wine has a data problem"), then ai_wine_summary confidently asserts 40 paragraphs of Catena Zapata history.
    - **Chappellet Artist Series Clone 337 Cabernet** → no vintage data → ai_hook: "The 1974 Chappellet Artist Series Clone 337 represents a critical moment in Napa Cabernet history" (invented vintage; the Artist Series is a modern program, not 1974).
    - **Buena Vista Private Cabernet** → no vintage data → ai_hook: "Buena Vista's 1966 Private Cabernet represents California Cabernet at an inflection point" (invented vintage).
    - **Joseph Phelps Eisele Cabernet** → no data error, but content manufactures a claim: "Joseph Phelps acquired this legendary site after decades of purchasing its grapes... Phelps treats this as their flagship single-vineyard Cabernet." Primary source verifies Eisele was purchased by François Pinault's Artemis Group in 2013, not by Phelps ([wine-searcher.com](https://www.wine-searcher.com/m/2013/07/araujo-acquired-by-chateau-latour-owners), [napawineproject.com](https://www.napawineproject.com/eisele-vineyard/)).
    - **Bruno Clair Chambertin-Clos de Bèze** → content claims "Chambertin-Clos de Bèze is a walled vineyard (climat) within the larger Chambertin Grand Cru." Geographically and legally WRONG — Clos de Bèze is its own separate Grand Cru, adjacent to but not inside Le Chambertin. Any sommelier knows this distinction.
    - **Penfolds V Yattarna Chardonnay** → claims the blend sources from "Coonawarra's Terra Rossa" — Coonawarra is Cabernet country, not Chardonnay. Fabricated provenance for a cool-climate white.
    - **Hunter Valley Brokenwood Semillon** → claims "ancient volcanic soils" and "volcanic basalt subsoils" — Hunter Valley is alluvial/sedimentary, not volcanic. Fabricated geology.
    - **Zaca Mesa Mesa Syrah** → claims "Franciscan shale underlying Santa Ynez Valley" — Santa Ynez Valley is Monterey Formation (diatomaceous earth), not Franciscan. Fabricated geology.
    - **Fritz Dutton Ranch** → claims "Petaluma Gap" (a separate AVA to the south) for a Russian River Valley wine AND "volcanic subsoils" under Goldridge sandy loam (Goldridge is fluvial, not volcanic). Fabricated geography AND geology.
- **Why it matters:** S12's Stage 2 audit (Session 12 in the 30K sprint) scored Grade B at 3.31/5 and Grade C at 1.76/5 — a warning sign. This audit extends that into specific failure modes: **when the data packet has errors, the AI doesn't hedge, it confabulates**. S12's voice-rules rewrite tightened against filler words and generic praise but did NOT build a TRUST-LEVEL contract that says "if the data says X, write as if X is true." That's an L3 fact-check problem. The L3 pass in S12 was only run on 30+30 wines; this audit suggests the problem is present across the 5,108-wine content corpus and that Sprint 3 cannot regenerate without fixing the underlying facts first.
- **Proposed fix:** Two-step approach:
  1. **Fix the underlying facts first (F2, F5, F6, F7, F8).** Don't regenerate content until the data packet is correct.
  2. **Rerun L3 fact-check on ALL 5,108 content rows** with the repaired facts, using Sonnet or Opus at `fact_check_status=pending`. Flag everything that still fails as `fact_check_status=failed` and drop it from user-facing display until regenerated. This is where the $18 pre-auth budget from S2.3 should actually get spent — as part of Sprint 3 cleanup, not S2.3 audit.
- **Effort:** medium (fact fixes) + medium ($30-50 re-fact-check pass across 5K wines)
- **Dependencies:** F2, F5, F6, F7, F8 (fact fixes), S2.2 F1 (staging relink for depth recovery feeding regeneration)
- **Related:** S10 enrichment_audit.py work, S12 stage1/stage2 validation work, S12 voice rules rewrite

### F11 — Self-contradicting content within a single wine_insights record

- **Severity:** P1
- **Evidence:**
    - **Domaine Vacheron Sancerre**: ai_hook says "biodynamic since 2000", ai_wine_summary says "biodynamic practices since 2005". Two different dates in two adjacent fields.
    - **Chamisal San Luis Obispo Chardonnay**: ai_hook says "Chamisal's Monterey Chardonnay..." (following the broken appellation data); ai_wine_summary corrects to "Chamisal sits in the heart of Edna Valley" (contradicting the hook in the same record).
    - **Catena Alta Vista Malbec**: ai_hook flags the data problem explicitly; ai_wine_summary then manufactures a confident Catena Zapata narrative that assumes the data is correct.
- **Why it matters:** These contradictions are between the voice of the hook (post-S12 rules with explicit hedging rules) and the voice of the summary (pre-S12 generative style). When the fields disagree within the same record, the user reads the latest one (typically the longer ai_wine_summary) and the hook's honesty is wasted. It also means any automated rendering that shows hook-then-summary side-by-side will display the contradiction directly to users.
- **Proposed fix:** Add a content-consistency check to the L3 fact-check pass: compare claims across all ai_* fields in a single row and flag contradictions (using the same model that writes the content, with a "find contradictions" prompt). Then re-generate OR force the hook to win (the hook is where the hedging rules live).
- **Effort:** small (new check in existing L3 pipeline)
- **Dependencies:** F10 (L3 repair pass)
- **Related:** F10

### F12 — TTB-illegal designations: US producers labeling wines "Porto"

- **Severity:** P1
- **Evidence:** Sample wine "Messina Hof 40th Anniversary Papa Paulo Porto, Texas" — appellation "Texas", producer "Messina Hof", wine_type "fortified". Since March 2006, "Port" and "Porto" are protected European designations under the EU-US trade agreement; only producers operating continuously before 2006 can continue using "Porto" as a semi-generic designation, and newer producers (or new brand extensions) cannot. Labeling a Texas wine "Papa Paulo Porto" in its canonical display_name — even if the producer is grandfathered — is a legal edge case and at minimum confusing to users who know the Douro DOC.
- **Why it matters:** A sommelier, especially one with formal training (Court of Master Sommeliers, WSET Diploma), will know the Port/Porto naming rules and flag this as a red flag. Not the producer's fault (if grandfathered) but Loam's UI/display should note the designation history rather than just echo the label verbatim.
- **Proposed fix:** Add a display-layer annotation for wines using protected EU terms (Porto, Champagne, Chablis, Burgundy, Sherry, etc.) when the wine_country isn't the protected-term country. Show "US" Port-style wine as "fortified red (Port-style)" rather than "Porto" in the canonical display while preserving the label text in a secondary field.
- **Effort:** small
- **Dependencies:** none
- **Related:** F7 (Messina Hof invented grape)

### F13 — Confabulated specific vintages in ai_hook when DB has no vintage data

- **Severity:** P1
- **Evidence:** Multiple Grade C wines with `wine.vintage_year` NULL have ai_hook content asserting specific vintage years as if the DB had them:
    - **Chappellet Artist Series Clone 337**: "The 1974 Chappellet Artist Series Clone 337 represents a critical moment in Napa Cabernet history" — vintage year NULL in DB; Chappellet's Artist Series is a modern program (post-2000), not 1974.
    - **Buena Vista Private Cabernet**: "Buena Vista's 1966 Private Cabernet represents California Cabernet at an inflection point" — vintage year NULL.
    - **Etude Rutherford Cabernet**: "Etude's 1995 Rutherford Cabernet is a window into pre-cult Napa" — vintage year NULL.
    - **Etude Carneros Pinot**: "Etude's 2017 Carneros Pinot is a masterclass..." — vintage year NULL.
    - **Cullen Moon**: specific 12.5% ABV claim with no vintage data.
    - **Penfolds Grange ai_hook**: "now command $900 per bottle" — price claim with no price data.
- **Why it matters:** When the facts packet doesn't have a vintage year, the AI shouldn't invent one. This is a specific sub-case of F10 confabulation, tracked separately because it's easy to automatically detect (compare claimed vintages in `ai_hook` against `wine_vintages.vintage_year` for that wine) and easy to prevent (tighten the prompt).
- **Proposed fix:** Prompt-level change in `pipeline/enrich/enrich_prompts.py`: add an explicit "UNKNOWN FACT PROTOCOL" rule that says "If the facts packet does not contain a specific year, the word, the grape, etc., you MAY NOT assert it. Use hedging ('this wine', 'the vintage', 'the producer') instead." Re-fact-check existing content with a regex: flag any `ai_hook` or `ai_wine_summary` that contains a 4-digit year not present in the facts packet for that wine.
- **Effort:** trivial (prompt update + regex validation)
- **Dependencies:** F10 (re-fact-check pass)
- **Related:** F10

### F14 — Geological / geographic fabrication in terroir content

- **Severity:** P1
- **Evidence:** A sommelier-grade audit of geological claims in the sample found at least 5 wines where the ai_terroir_expression asserts specific (and specifically wrong) geology:
    - **Brokenwood Hunter Valley Semillon**: "Hunter Valley's warm, humid climate and ancient volcanic soils" / "volcanic basalt subsoils." **Wrong.** Hunter Valley is alluvial/sedimentary (sandy loams over sandstone/shale). There's some weathered volcanic influence on the ranges, but "ancient volcanic soils" and "volcanic basalt subsoils" is a fabrication.
    - **Zaca Mesa Mesa Syrah, Santa Ynez Valley**: "The Franciscan shale underlying much of Santa Ynez Valley." **Wrong.** Santa Ynez Valley is Monterey Formation (Miocene diatomaceous earth and shale), not Franciscan (Coast Range assemblage). Different ages, different origins.
    - **Fritz Russian River Valley**: "volcanic subsoils" under Goldridge sandy loam. **Wrong.** Goldridge is fluvial (deposited by the ancestral Russian River), not volcanic.
    - **Joseph Phelps Eisele Cabernet**: "cooling influences from the Chalk Hill Gap." **Wrong.** Chalk Hill is an AVA in Sonoma; Eisele Vineyard is in Calistoga in Napa. No "Chalk Hill Gap" exists near Eisele. The cooling influence at Eisele is from the marine air coming through Chalk Hill-style wind gaps in the Mayacamas, but naming the specific gap "Chalk Hill" is wrong.
    - **Joseph Phelps Eisele Cabernet** (second error in same record): "Eisele Vineyard sits in Calistoga at Napa Valley's northern end... Phelps acquired this legendary site after decades of purchasing its grapes." **Wrong provenance.** Eisele was purchased by the Artemis Group (François Pinault, owner of Château Latour) in 2013, not by Phelps ([wine-searcher.com](https://www.wine-searcher.com/m/2013/07/araujo-acquired-by-chateau-latour-owners), [napawineproject.com](https://www.napawineproject.com/eisele-vineyard/)). The Araujos owned it 1990-2013 (they made wine as Araujo Estate). Phelps sourced fruit in the 1970s-80s but never owned the vineyard. (This is a provenance error, not pure geology, but lives in the same content field.)
- **Why it matters:** Geology and ownership are two areas where sommeliers and wine-studied consumers DO test databases. Getting Hunter Valley's soil type wrong, or calling Santa Ynez's bedrock "Franciscan," or claiming Joseph Phelps owns Eisele, are all errors that a WSET Level 3 student would catch on first read. The confidence with which the content asserts these errors is the killer — it's not hedged, not sourced, not qualified.
- **Proposed fix:** Remove geological/geographic specifics from the default terroir generation prompt unless they're in the facts packet. If a wine doesn't have a `vineyards.soil_type_id` or an `appellation_soils` entry, the ai_terroir_expression should talk about the appellation's general character ("the cool maritime climate of the Hunter Valley shapes this wine") not specific geology ("volcanic basalt subsoils"). Same for producer history — require a `producer.year_established` and `producer_timeline` record before making history claims.
- **Effort:** small (prompt tightening) + medium (re-fact-check pass)
- **Dependencies:** F10
- **Related:** F10

### F15 — Possibly-invented wine names / SKUs (confabulation at the label level)

- **Severity:** P1
- **Evidence:** Several Grade B sample wines have names that do not cleanly match any real-world SKU I can recall or verify via quick web search:
    - **Brokenwood Trevena Kindred Semillon**: Brokenwood's flagship Semillon is ILR Reserve. "Trevena Kindred" is not a recognized Brokenwood bottling name.
    - **Penfolds V Yattarna Five Blend Chardonnay**: Yattarna is Penfolds' top-tier Chardonnay (Bin 144). "V Yattarna" as a distinct label from "Yattarna" is unusual. "Five Blend" as a sub-name doesn't match Penfolds' naming conventions.
    - **Kim Crawford Spitfire SP Sauvignon Blanc**: Kim Crawford's main SB is sold as "Marlborough Sauvignon Blanc." "Spitfire SP" as a named bottling is unfamiliar.
    - **Fritz, Russian River Valley Dutton Ranch Shop Block**: Fritz Dutton Ranch Shop Block is more commonly labeled as the Fritz Sauvignon Blanc Shop Block. Not necessarily wrong, but the "Shop Block" suffix is doing work.
    - **Korbel Methode Champenoise Times Square Chardonnay**: Korbel's Times Square bottling is a commemorative NV brut; "Times Square Chardonnay" as a varietal label is unusual.

    **Caveat:** I did NOT do a full WebFetch verification on each of these (some are obscure enough that a quick search won't resolve). These are flagged as suspected confabulation based on combining "unfamiliar name" with "Loam's demonstrated confabulation pattern."
- **Why it matters:** If a wine name doesn't exist in reality, the entire wine page is a ghost page — every field is fiction. This is worse than wrong data because there's no underlying truth to correct to. These suspect SKUs probably came from a staging source that had noisy label text (TTB COLA free-text, importer catalog that listed variants the winery doesn't actually produce).
- **Proposed fix:** Flag suspected confabulation via cross-source verification — if a wine exists in Loam but has 0 staging matches beyond the one source that created it, AND that one source is known-noisy (TTB COLA free-text, OCR-parsed labels), AND nobody has linked a purchase or price, it's a candidate for quarantine. Keep it in the DB but hide from user-facing display until verified.
- **Effort:** small (cross-source presence query)
- **Dependencies:** S2.2 F1 (need staging relinked to trust the cross-source signal)
- **Related:** F1 (marquee NULL display_name)

### F16 — Producer misattribution affects wines beyond the Catena Alta Vista case (suspected based on sample patterns)

- **Severity:** P1
- **Evidence:** The sample surfaced one confirmed producer-collision (Catena/Alta Vista, F5). Other suspicious patterns:
    - **Stonestreet Black Cougar Ridge Cabernet Sauvignon**: content attributes the Black Cougar Ridge vineyard project to Jess Jackson ("launched by Jess Jackson in the 1990s, focusing on steep-slope vineyards above 400 feet elevation"). Stonestreet WAS a Jackson-family project, and Black Cougar Ridge is a real Stonestreet site. Credible. But: grapes linked are `CHARDONNAY BLANC + CABERNET SAUVIGNON` — Chardonnay blended with Cab is a data error (F8), but the producer attribution itself is correct.
    - **Paul Hobbs Katherine Lindsay Cuvée Agustina Pinot Noir, Russian River Valley**: "Katherine Lindsay" is a real vineyard name in Anderson Valley (Mendocino), not Russian River Valley. Possible cross-region contamination at the vineyard level rather than producer level.
    - **Paul Hobbs Fraenkle Cheshier Pinot Noir, Russian River Valley**: "Fraenkle Cheshier" vineyard — I cannot immediately verify this is a real Paul Hobbs source. Possible data noise.
- **Why it matters:** Sprint 3 producer-metadata seeding (F3) should include a producer-uniqueness audit — query all wines where display_name contains two distinct strings that also match two producer_ids, and verify the match is correct.
- **Proposed fix:** Run a producer-collision detection query: `SELECT wines where display_name contains tokens from 2+ distinct producer names`. Manually review and split.
- **Effort:** small
- **Dependencies:** F3
- **Related:** F5, F3

### F17 — AI-generated content invents specific percentages when DB has no percentages

- **Severity:** P2
- **Evidence:** Multiple Grade B/C wines have `wine_grapes.percentage = NULL` but ai_hook asserts specific percentages:
    - **Cline Bridgehead Zinfandel**: "At 75% Primitivo..." — DB has grape=PRIMITIVO with no percentage.
    - **J. Lohr Cuvée Pau**: "The 75% Cabernet base is blended with..." — DB has grape=NULL entirely.
    - **Korbel Times Square Chardonnay**: "75% Pinot Blanc instead of the traditional Chardonnay-Pinot Noir blend" — DB has PINOT BLANC only, no percentage.
    - **Joseph Phelps Eisele Cabernet**: "Blending incorporates small percentages of Merlot, Petit Verdot, and Cabernet Franc to add complexity while maintaining Cabernet Sauvignon's dominance at 75%." — DB has only CABERNET SAUVIGNON.

    All four of these "75%" claims are the TTB minimum varietal percentage threshold (US labeling law requires 75%+ of named varietal on varietal-labeled wines). The AI is treating "75%" as a stock filler when it doesn't know the real percentage.
- **Why it matters:** Related to F10 and F13. The AI has learned that "75%" is always a safe default because it's the TTB floor — but a sommelier reading "75% Cabernet Sauvignon" on J. Lohr Cuvée Pau (which is actually typically 80-85% Cab per the producer) sees it as authoritative and specific when it's actually invented.
- **Proposed fix:** Prompt-level rule: "Do NOT state specific blend percentages unless the facts packet contains them. Use 'primarily', 'dominant', or just the grape name." Add a regex check to L3 that flags any number followed by % in ai_hook/ai_wine_summary that isn't backed by `wine_grapes.percentage` for that wine.
- **Effort:** trivial
- **Dependencies:** F10
- **Related:** F10, F13

### F18 — Wine_insights content has self-awareness about data problems in the ai_hook but not in downstream fields

- **Severity:** P2
- **Evidence:** S12's voice-rules rewrite added explicit hedging guidance to the ai_hook prompt, and it's visible in the sample:
    - **Catena Alta Vista**: ai_hook "This wine has a data problem: the grape varieties listed don't match Catena Zapata's known Malbec practices, and it's labeled 'table white' despite being a red wine."
    - **Beaulieu Vineyard Tapestry**: ai_hook "the blend composition remains opaque, which limits understanding its vintage consistency."
    - **V. Sattui Gamay Rouge**: ai_terroir "Without specific vineyard sourcing information, the terroir expression remains unclear."

    But the same records have ai_wine_summary or ai_vinification_summary fields that IGNORE the hook's honesty and manufacture confident narratives (see F10).
- **Why it matters:** This is evidence that the voice-rules rewrite DID work on the prompt that was rewritten (the hook generator) and DID NOT propagate to the other ai_* field generators. Sprint 3 should apply the same hedging rules to every field generator prompt consistently.
- **Proposed fix:** Centralize the hedging-rules block in `pipeline/enrich/enrich_prompts.py` and inject it into every field generator, not just the hook. The existing hook rules are good; they just need to cover the 6 other fields.
- **Effort:** trivial
- **Dependencies:** none
- **Related:** F10, F11

### F19 — Bruno Clair Chambertin-Clos de Bèze: content claims Clos de Bèze is "within the Chambertin Grand Cru" — a textbook classification error

- **Severity:** P2
- **Evidence:** Sample wine "Bruno Clair, Chambertin-Clos de Bèze Grand Cru" has ai_hook: "Chambertin-Clos de Bèze is a walled vineyard (climat) within the larger Chambertin Grand Cru, and Bruno Clair's version is one of Burgundy's most consistently mineral-driven expressions..."

    **Factually wrong.** Chambertin and Chambertin-Clos de Bèze are TWO SEPARATE Grand Crus in Gevrey-Chambertin, adjacent to each other. They have been separately classified since the 1855 Beaune map and the 1936 INAO designation. By French wine law, wines from Chambertin-Clos de Bèze MAY be sold under the label "Chambertin" (but not vice versa) — but that's a labeling allowance, not a classification hierarchy. Clos de Bèze is NOT "within" Chambertin; they are two distinct climats sharing a boundary.
- **Why it matters:** This is a textbook test on the WSET Level 3 exam and the Court of Master Sommeliers Advanced exam. Any formally-trained wine professional reading this page will immediately distrust the entire site. It's an AOC law error that you could only make if you've never studied Burgundy Grand Cru classification.
- **Proposed fix:** Part of F10's re-fact-check pass. Specific to this wine: replace the ai_hook claim with the correct statement ("Chambertin-Clos de Bèze is one of two Grand Crus in Gevrey-Chambertin carrying the Chambertin name (the other being Le Chambertin)..."). Add a Burgundy-specific validation check to the L3 fact-check prompt that catches common AOC classification errors.
- **Effort:** trivial (per-wine) or small (class-wide fix)
- **Dependencies:** F10
- **Related:** F10, F14

### F20 — Grade F "wines with full facts packet" sample shows facts ARE mostly correct — the gap is depth, not correctness

- **Severity:** P3 (this is a WIN not a bug, but worth tracking)
- **Evidence:** The 10 Grade F wines selected for "has full facts packet" (LWIN+producer+appellation+grapes) were mostly correct:
    - **Angove Warboys Grenache, McLaren Vale**: clean (GARNACHA TINTA is the Spanish display name for Grenache — minor F9 display issue)
    - **Averys Bonnes-Mares Grand Cru**: clean (Pinot Noir, Côte de Nuits, real Grand Cru)
    - **Beckmen Purisima Mountain Grenache**: clean; color=`rose` is interesting (Beckmen Purisima Mountain does make a rosé)
    - **del Nebbiolo Riveverse Langhe**: NASCETTA grape correct for the white Langhe appellation (despite producer name containing "Nebbiolo")
    - **Dominion Tantara Zotovich 115 Pinot Noir, Sta. Rita Hills**: clean; Zotovich Vineyard is real; Clone 115 is real
    - **Donnafugata Bell'Assai, Vittoria**: clean; Frappato is correct for Cerasuolo di Vittoria-adjacent bottlings; Donnafugata is a real Sicily producer
    - **Scheid Vineyards Dry Riesling, Monterey**: clean; RIESLING WEISS (post-S1.11)
    - **St. Michael Eppan Sanct Lagrein**: LAGREIN correct; the + VELTLINER ROT grape is suspicious but not falsified
    - **Willi Schaefer Graacher Domprobst Versteigerung Riesling Spätlese, Mosel**: clean; Willi Schaefer is a real Mosel domaine; Graacher Domprobst is a real Einzellage; Versteigerung = "auction wine", a real Prädikat designation
    - **Kokomo Teldeschi Dolcetto**: grape error (FOLLE BLANCHE + DOLCETTO) — see F8

    9/10 Grade F wines have correct basic facts. The issue on the Grade F tier isn't factual correctness; it's **everything ELSE is empty** (no insights, no vintage chemistry, no producer metadata, no score history).
- **Why it matters:** This is a positive indicator for Sprint 3. The foundation (canonical identity + basic facts) is mostly right when the full facts packet exists. The F8 grape-linkage bug is real but not universal — F20's clean 9/10 on Grade F suggests that at the identity-packet level, the data is usable. Sprint 3's enrichment pass can proceed on the 10 Grade F wines with full facts packets (expand this tier by adding `has_facts_packet=true` as a new gate) to test enrichment on clean inputs.
- **Proposed fix:** Use this population — Grade F wines with full facts packets — as the first enrichment target for Sprint 3 regeneration, once F2/F7/F8 are fixed. Confirm that clean inputs produce clean outputs before regenerating the 5,108 legacy Grade B/C rows.
- **Effort:** N/A (this is a positive finding)
- **Dependencies:** F2, F7, F8 fixes
- **Related:** F1 (Grade F marquee wines may still have NULL display_name even with facts packet)

### F21 — Louis Roederer in the "long-tail" producer sample suggests even famous producers not in the hand-curated list are undercovered

- **Severity:** P2
- **Evidence:** The "long-tail" producer sample (producers with website_url=NULL, year_established=NULL, latitude=NULL, no 1-of-15 hand-picked famous) surfaced:
    - **Louis Roederer** (Champagne) — 46 wines, 0 metadata. Louis Roederer makes Cristal, one of the most famous Champagnes in the world. Should absolutely not be in the "long-tail" bucket.
    - **Antonin Rodet** (Burgundy) — 46 wines, 0 metadata. Major Burgundy négociant.
    - **Nicole Lamarche** (Burgundy) — 15 wines, 0 metadata. Famous younger generation of Domaine Lamarche in Vosne-Romanée.

    The "long-tail" sample was designed to catch producers with NO metadata and ≥1 wine. Louis Roederer fitting that filter is surprising only if you assumed metadata had been seeded for famous producers. It hasn't. The entire famous-producer tier is in the long-tail by this filter.
- **Why it matters:** Confirms F3 at a wider scale. Hand-curating the top 15 producers is not enough — the seed list needs to cover at least the top 300-500 producers globally, including all Champagne houses, all Burgundy négociants/domaines, all First-Growth Bordeaux, etc.
- **Proposed fix:** Expand F3's hand-curated seed file to 300-500 producers, with a clear tier structure: Tier 1 = top 50 (obvious marquee), Tier 2 = next 150 (regional icons), Tier 3 = next 300 (producer-level detail). Hand-curate Tier 1, Haiku-extract Tier 2/3 from official websites.
- **Effort:** small (extension of F3)
- **Dependencies:** F3
- **Related:** F3

### F22 — Sample-builder marquee-search bug: `ORDER BY data_grade DESC` selected Grade F siblings instead of Grade B/C siblings for marquee targets

- **Severity:** P3
- **Evidence:** The sample builder's `fetch_marquee` function uses `ORDER BY w.data_grade DESC, md5(w.id::text)` when matching a marquee-name target. In PostgreSQL, `DESC` on text orders F > D > C > B (alphabetically descending), so Grade F wines come FIRST. The intended behavior was "prefer the highest-depth match" (which would be Grade B first). This caused:
    - DRC Romanée-Conti search resolved to a Grade F DRC Bourgogne entry instead of the Grade C DRC Romanée-Conti Grand Cru entry that also exists in the DB.
    - Penfolds Grange search resolved to "Penfolds Grange by Nigo" (Grade F limited edition) instead of the flagship Grange (which is in the Grade B sample with LWIN 1004285).
    - Henschke Hill of Grace search resolved to the "Museum Release" entry instead of the current-release flagship.
- **Why it matters:** A methodology note — the marquee sample understates how bad the marquee tier actually is AND overstates it in different ways. Several "marquee broken" findings were partially due to this match-ordering bug. With `ORDER BY data_grade ASC` the sample would have found the flagship entries and we'd have found fewer NULL display_names BUT more wrong-color and wrong-grape errors (since the flagships have richer content to audit).
- **Proposed fix:** Fix the marquee query in `data/stats/s23_build_sample.py` (use `CASE WHEN data_grade='B' THEN 1 WHEN data_grade='C' THEN 2 WHEN data_grade='D' THEN 3 ELSE 4 END` for proper priority) and rerun in S2.3.5 if a follow-up pass is needed. Or accept the current sample as-is for findings and re-audit at Sprint 3 time.
- **Effort:** trivial
- **Dependencies:** none
- **Related:** methodology only

---

## Meta-patterns

Four patterns to escalate to S2.9 synthesis:

1. **Facts first, content second.** The existing 5,108 wine_insights rows cannot be used as-is for Sprint 3 enrichment. ~40% of the audited sample has at least one sommelier-visible error. The errors are MOSTLY downstream of bad facts (F2 grape linkage, F5 producer collision, F6 color errors, F7 invented grapes), not pure prompt-quality issues. Sprint 3 should fix facts FIRST (the S2.2 F1 staging relink + F2/F6/F7/F8 canonical repairs), then regenerate content on the now-correct facts. S12's L3 fact-check pipeline is the right tool — it just needs to run on the full corpus with repaired facts, not a 30+30 sample.

2. **The AI will confabulate confidently when facts are wrong.** This isn't a voice-rules problem; it's an L3 prompt-contract problem. S12's ai_hook rewrite added hedging for missing data, but the other six fields (wine_summary, terroir, vinification, style_profile, food_pairing, cellar) don't have the same hedging rules. Every field generator should share the same TRUST contract: "if the facts packet doesn't say it, you don't say it." F13 (fake vintages), F14 (fake geology), F17 (fake 75% percentages) are all caught by this single rule.

3. **The top-of-the-funnel famous-producer tier is effectively empty — and it's the fastest and highest-ROI thing to fix.** F3/F4/F21 all point at the same workstream: **hand-curate or Haiku-extract a producer seed file for the top 300-500 producers**. The blast radius is enormous — DRC, Lafite, Screaming Eagle, Louis Roederer, Gaja, Dom Pérignon — every landing page that actually gets user traffic gets fixed in one session's work, before any other enrichment pipeline runs.

4. **There are multiple distinct grape-linkage failure modes, not one "grape bug".** F2 (Chardonnay→PinotBlanc), F8 (substring/color/word-inversion/synonym-duplication), F7 (invented grapes), F9 (display aliases) are each separate root causes requiring separate fixes. A "fix the grapes" Sprint 3 item needs to be broken into at least 4 sub-items.

## Scope-breaker check

**No scope-breakers.** The findings above all execute inside the Sprint 3 envelope. F10 (fact-check repair pass on 5,108 rows) is the largest workstream but fits in ~$30-50 of Sonnet/Opus spend, which is inside the Sprint 2+3 combined $50 ceiling. No structural rewrites. No Sprint-plan recalibration needed.

**One recommendation:** Sprint 3 should be SEQUENCED explicitly as (a) staging relink (S2.2 F1), (b) producer seed file (S2.3 F3), (c) grape linkage repair (F2/F7/F8), (d) color + country repair (F6), (e) L3 re-fact-check pass (F10), (f) content regeneration. Items (a)-(e) must finish before (f). If this sequence isn't enforced, (f) will re-inherit all the errors.

## Numbers for S2.9 synthesis

- **Sample audited:** 99 wines + 50 producers = 149 records
- **Records with at least one P0 error:** ~55-60 (rough estimate based on the 9 error classes hitting different wine subsets with overlap)
- **Chardonnay/Pinot Blanc scope:** 2,743 / 2,809 wines (97.6%)
- **"Verdot Petit" inversion scope:** 284 wines with a word-inverted canonical grape name + 1,001 wines on "Merlot Noir" instead of "Merlot"
- **Famous producer metadata coverage:** 0 / 15 (0%)
- **Marquee wine success rate:** 2 / 10 clean (20%)
- **Primary-source verifications:** 3 / 3 flagged claims WRONG (Bogle Phantom Chardonnay, Joseph Phelps Eisele ownership, Messina Hof "Garro" grape)
- **Actual S2.3 AI spend:** $0.00 (pivot to inline Opus reasoning + WebFetch primary-source; pre-auth $18 stands for Sprint 3 if needed)
