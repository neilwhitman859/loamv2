# Cron Loop Journal

Append-only log of automated loop runs. Each entry records what was attempted,
what worked, what was wasted, and what to skip next time. Read this BEFORE
designing a new cron loop — it prevents repeating dead-end work.

See `data/session_prompts/cron_loop_template.md` for the structural template.

---

## Run 1: 2026-04-06 Overnight — Prices + Vineyards + Data Quality

**Duration:** ~27 cycles over ~4.5 hours (*/10 cron)
**Prompt:** 3-track loop (Track A prices, Track B vineyards, Track C data quality sweeps)

### Track A: Price Coverage — LOW YIELD
- **Phase 1** (retail_promote for 5 sources): best_wine_store yielded some, rest were 0
- **Phase 2** (batch_matcher + retail_promote for 10 sources): Near-zero new matches across all sources. Prior sessions had already matched and promoted everything reachable.
- **Phase 3** (re-promote wallys/specs/systembolaget/lcbo): 0 new prices — all wines already had prices from other merchants.
- **Lesson:** Should have run a gap analysis query first. All price sources were already promoted. The entire track was wasted cycles.
- **Skip next time:** Don't re-run batch_matcher/retail_promote for sources that prior sessions already covered. Query `NOT EXISTS` counts first.

### Track B: Classified Vineyards — HIGH VALUE (0 → 815)
- **Phase 1** (26 Burgundy villages): 585 Premier Cru climats from INAO CDCs. Genuine new data creation. Required careful PDF parsing, slug conflict resolution (9 conflicts), format variation handling per village. **This was the real win.**
- **Phase 2** (Barolo MGAs): 170 from MASAF disciplinare. Clean extraction from comma-separated list in legal text. PDF had OCR artifacts (missing commas, split words) requiring judgment.
- **Phase 3** (51 Alsace Grand Crus): Straightforward — appellations already existed in DB, just needed vineyard rows linked to them.
- **Lesson:** Legal source seeding is the ideal cron loop use case. Large backlog, self-contained units, zero inference risk.

### Track C: Data Quality Sweeps — ALREADY DONE
- All 6 sweeps (C1-C6) returned 0 rows from cycle 1 onward.
- Prior sessions had already executed these exact queries.
- **Lesson:** Run all sweeps once as a verification pass. If they return 0, drop the track entirely. Don't rotate through them for 27 cycles.

### Structural Issues
- **No self-termination:** Cron kept firing after all tracks completed. Had to cancel manually.
- **Multi-track waste:** 2 of 3 tracks were exhausted from the start. Gap analysis would have caught this.
- **What to do differently:** Single-track focus on genuinely large backlog. Self-termination check at cycle start. Pre-flight gap analysis before cron creation.

### Final Numbers
| Metric | Start | End | Delta |
|---|---|---|---|
| Vineyards | 9 | 815 | +806 |
| Wines with prices | 27,264 | 27,264 | +0 |
| Colors | 282,146 | 282,146 | +0 |
| Varietal categories | 104,504 | 104,504 | +0 |

### Cycle Log (abbreviated — no per-cycle logging existed for this run)
| Cycles | Track | Item | Result |
|--------|-------|------|--------|
| 1-8 | A | Phase 1+2 price promotion (8 sources) | ~0 new prices (already promoted) |
| 9-10 | A | Phase 3 re-promote wallys/specs/syst/lcbo | 0 new prices |
| 1-19 | B | Burgundy 1er Crus (20 villages) | +446 vineyards, 9 slug conflicts resolved |
| 20-25 | B | Burgundy continued (Rully→Chablis) | +149 vineyards |
| 26 | B | Barolo MGAs | +170 vineyards |
| 27 | B | Alsace Grand Crus | +51 vineyards |
| 1-27 | C | All 6 sweeps (rotated) | 0 rows affected (all exhausted from prior sessions) |

### Remaining Backlog for Future Loops
- **Barbaresco MGAs** — MASAF disciplinare in hand, ~66 MGAs
- **Brunello/Chianti Classico/other Italian DOCG vineyards** — from MASAF sweep PDFs
- **German Einzellagen** — if VDP classification list can be sourced legally
- **More appellation_rules** — 549 done, ~3,100 appellations remain
- **Appellation_vintages weather data** — Open-Meteo integration (empty table, 0 rows)
- **Score coverage** — 2.24%, needs fuzzy matching (interactive, not loop material)

---

## Run 2: 2026-04-06 Overnight — US AVA Rules + Barbaresco Vineyards

**Duration:** */30 cron, target ~30 cycles over 15 hours
**Prompt:** 2-phase loop (Phase 1: Barbaresco MGAs, Phase 2: US AVA rules from eCFR)
**Prompt file:** `data/session_prompts/loop_ava_rules_vineyards.md`

### Phase 1: Barbaresco MGAs — COMPLETE (cycle 1)
- +66 Barbaresco MGAs inserted as vineyards (0 → 66)
- 2 slug conflicts resolved (Canova, Roncaglie — disambiguated with `-barbaresco` suffix)
- Source: MASAF disciplinare Barbaresco, Art. 7 (DPR 03.10.1980, last modified DM 17.04.2015)
- Total vineyards: 815 → 881

### Phase 2: US AVA Rules — IN PROGRESS
- Target: 238 US AVAs without rules (78,737 wines)
- 6 AVAs per cycle from eCFR (27 CFR Part 9)
- Captures: established_year, area_ha, elevation, boundary summary, geographic features

### Cycle Log
| Cycle | Phase | Items | Result | Notes |
|-------|-------|-------|--------|-------|
| 1 | Vineyards | Barbaresco 66 MGAs | +66 vineyards | 2 slug conflicts resolved |
| 2 | AVA Rules | Napa Valley, Paso Robles, Columbia Valley, Russian River Valley, Willamette Valley, Sonoma Coast | +6 rules, +6 established_year, +5 area_ha, +2 elevation | eCFR blocked (302 redirect), used WebSearch fallback. 29,081 wines covered. |
| 3 | AVA Rules | Finger Lakes, Sta. Rita Hills, Central Coast, Yakima Valley, Alexander Valley, Dry Creek Valley, Santa Ynez Valley, Sonoma Valley, Lodi, Los Carneros, Walla Walla Valley, Mendocino, Red Mountain, Santa Maria Valley, Texas High Plains, Eola-Amity Hills, North Coast, Dundee Hills, Anderson Valley, Monterey | +20 rules, +20 established_year, +17 area_ha, +1 elevation (Texas HP) | 212 remaining. Rules total: 575. |
| 4 | AVA Rules | Santa Lucia Highlands, Horse Heaven Hills, Yamhill-Carlton, Oakville, Santa Cruz Mountains, Rogue Valley, North Fork of Long Island, Chehalem Mountains, Edna Valley, Rutherford, Monticello (VA), Sierra Foothills, Columbia Gorge, Wahluke Slope, Willcox (AZ), Howell Mountain, St. Helena, Livermore Valley, Paso Robles Willow Creek District, Applegate Valley | +20 rules, +20 established_year, +19 area_ha, +7 elevation ranges | 192 remaining. Rules total: 595. |
| 5 | AVA Rules | Temecula Valley, Texas Hill Country, El Dorado, Adelaida District, Seneca Lake, Arroyo Seco, Stags Leap District, Ribbon Ridge, Knights Valley, Shenandoah Valley (VA/WV), Calistoga, Coombsville, Mount Veeder, Clarksburg, Sonoma Mountain, Petaluma Gap, Spring Mountain District, Oak Knoll District of Napa Valley, Happy Canyon of Santa Barbara | +19 rules, +19 established_year, +19 area_ha, +5 elevation ranges | 173 remaining. Rules total: 614. San Luis Obispo skipped — no standalone federal AVA (SLO Coast §9.275 est. 2022 is distinct). |
| 6 | AVA Rules | San Luis Obispo (county), McMinnville, Yountville, Lake Michigan Shore, Rattlesnake Hills, Green Valley of Russian River Valley, Bennett Valley, Snake River Valley, Arroyo Grande Valley, Atlas Peak, Royal Slope, Leelanau Peninsula, Ballard Canyon, Grand Valley, Santa Clara Valley, Mendocino Ridge, Chalk Hill, Lake Chelan, Van Duzer Corridor, Lake Erie | +20 rules, +19 established_year, +20 area_ha, +12 elevation ranges | 153 remaining. Rules total: 634. San Luis Obispo inserted as county-level (75% rule, 27 CFR 4.25a(e)(2)). Mendocino Ridge: only non-contiguous AVA in US. |
| 7 | AVA Rules | Chalone, Old Mission Peninsula, Redwood Valley, Los Olivos District, Carmel Valley, Red Hills Lake County, Contra Costa, Southern Oregon, Rockpile, Diamond Mountain District, Yorkville Highlands, Verde Valley, Templeton Gap District, Fort Ross-Seaview, Suisun Valley, Outer Coastal Plain, Umpqua Valley, Cienega Valley, Potter Valley, Yadkin Valley | +20 rules, +20 established_year, +20 area_ha, +17 elevation ranges | 133 remaining. Rules total: 654. Notable: Contra Costa §9.291 est. 2024 (newest AVA in batch); Verde Valley AZ; Yadkin Valley NC (NC's first AVA); Outer Coastal Plain NJ. |
| 8 | AVA Rules | Ancient Lakes of Columbia Valley, Sonoita, West Sonoma Coast, California Shenandoah Valley, High Valley, Mokelumne River, Dunnigan Hills, El Pomar District, Moon Mountain District, Snipes Mountain, Long Island, Elkton Oregon, Fountaingrove District, Tualatin Hills, Ramona Valley, Paicines, Pine Mountain-Cloverdale Peak, Grand River Valley, Fair Play, Laurelwood District | +20 rules, +20 established_year, +20 area_ha, +18 elevation ranges | 113 remaining. Rules total: 674. Cycle split across 2 sessions — batch B delayed overnight. West Sonoma Coast est. 2022 (newest in batch). Sonoita AZ at 4,500-5,000 ft. |
| 9 | AVA Rules | Indiana Uplands, The Rocks District of Milton-Freewater, Lehigh Valley, San Francisco Bay, Paso Robles Geneseo District, Niagara Escarpment, York Mountain, Lime Kiln Valley, Alisos Canyon, San Benito County, Shawnee Hills, Northern Neck George Washington Birthplace, Goose Gap, Cucamonga Valley, Paso Robles Highlands District, Upper Mississippi River Valley, San Antonio Valley, Clements Hills, Cayuga Lake, Augusta | +20 rules, +20 established_year, +20 area_ha, +15 elevation ranges | 93 remaining. Rules total: 694. Session restart required — old agents' output files were empty after context compaction; data re-sourced from task notifications. Notable: Augusta §9.22 est. 1980-06-20 — first federally recognized AVA in the US. Upper Mississippi River Valley §9.216 — largest multi-state AVA (~19.1M acres, driftless area). Goose Gap §9.277 est. 2021 (newest in cycle). Backfilled 5 appellations with missing established_year/area_ha from already-inserted cycle 9 rules. |
| 10 | AVA Rules | Santa Margarita Ranch, Puget Sound, Middleburg Virginia, Ozark Mountain, Naches Heights, Lewis-Clark Valley, Mt. Harlan, Madera, Solano County Green Valley, White Bluffs, Fiddletown, Clear Lake, Saddle Rock-Malibu, Tracy Hills, Guenoc Valley, Wisconsin Ledge, Ozark Highlands, The Hamptons Long Island, McDowell Valley, Linganore | +20 rules, +20 established_year, +20 area_ha, +20 elevation ranges | 73 remaining. Rules total: 714. Notable: Guenoc Valley §9.26 est. 1981 — first single-estate AVA in the US. McDowell Valley §9.36 — one of the smallest AVAs in CA (540 acres). Naches Heights — only Columbia Valley sub-AVA on Andesite bedrock (Goat Rocks volcano). Ozark Mountain §9.108 — 35M acres across AR/MO/OK. Mt. Harlan — 1,800–2,200 ft elevation, limestone-rich Sheridan soils, petitioned by Josh Jensen of Calera. |
| 11 | AVA Rules | Alta Mesa, North Yuba, Chiles Valley, Swan Creek, San Pasqual Valley, Hudson River Region, Altus, Sierra Pelona Valley, Hermann, Cape May Peninsula, Upper Hiwassee Highlands, Kelsey Bench-Lake County, Cole Ranch, Malibu Coast, South Coast, Warren Hills, Capay Valley, Ohio River Valley, Paso Robles Estrella District, Hames Valley | +20 rules, +20 established_year, +20 area_ha, +17 elevation ranges | 53 remaining. Rules total: 734. Notable: Cole Ranch §9.42 — smallest AVA in the US (60 acres, single estate, Mendocino County). Ohio River Valley §9.78 est. 1983 — 15.66M acres across IN/OH/WV/KY (3rd largest). Hudson River Region §9.47 est. 1982 — oldest continuously operating wine region in the US. Cape May Peninsula §9.262 est. 2018 — entirely defined by 10-ft elevation contour, max 10 ft ASL. San Pasqual Valley §9.25 est. 1981 — nation's 5th AVA. |
| 12 | AVA Rules | Ben Lomond Mountain, San Miguel District, Manton Valley, Catoctin, Borden Ranch, Fennville, San Bernabe, Dahlonega Plateau, San Lucas, Gabilan Mountains, River Junction, Bell Mountain, Northern Sonoma, Rocky Reach, Escondido Valley, Texas Davis Mountains, Eagle Peak Mendocino County, Creston District, Lancaster Valley, Candy Mountain | +20 rules, +20 established_year, +20 area_ha, +18 elevation ranges | 33 remaining. Rules total: 754. Batches B+C agents lost in context compaction — re-launched fresh agents, data re-sourced. Notable: Fennville §9.33 est. 1981-09-18 — Michigan's first and the nation's 4th AVA. Bell Mountain §9.55 est. 1986-10-16 — first Texas AVA. Lancaster Valley §9.41 est. 1982-05-11 — Pennsylvania's first AVA, nation's 12th overall. Gabilan Mountains §9.288 est. 2022-09-14 + Rocky Reach §9.287 est. 2022-07-05 — two newest AVAs in batch. River Junction §9.164 — unusually low elevation (18–25 ft), river confluence alluvial bowl. Candy Mountain §9.272 — smallest AVA in Washington (815 acres). Texas Davis Mountains §9.155 — mountain island at 4,500–8,300 ft rising from Chihuahuan Desert. Dahlonega Plateau §9.263 — Georgia's 2nd AVA, gold-belt schist/gneiss geology. |
| 13 | AVA Rules | Winters Highlands, San Luis Rey, Upper Lake Valley, Willow Creek, Mimbres Valley, Virginia Peninsula, Covelo, Southeastern New England, Wild Horse Valley, Comptche, Fredericksburg in the Texas Hill Country, Lamorinda, Appalachian High Country, Merritt Island, Texoma, Cosumnes River, Lake Wisconsin, Ulupalakua, Jahant, Eagle Foothills | +20 rules, +20 established_year, +20 area_ha, +18 elevation ranges | 13 remaining. Rules total: 774. Notable: San Luis Rey §9.295 est. 2024-08-30 + Comptche §9.292 est. 2024-05-08 — two 2024 AVAs, newest in dataset. Winters Highlands §9.290 est. 2023-09-28. Ulupalakua §9.278 — Hawaii's first and only AVA, just 70 acres total (~16 under vine) on Maui's Haleakala slopes. Southeastern New England §9.72 — 1,880,000 acres across CT/RI/MA, no elevation range (coastal/geographic boundary). Texoma §9.185 — 2,300,000 acres north TX along Red River, largest in this cycle. Appalachian High Country §9.260 — spans NC/TN/VA at 1,338–6,000 ft, 139-day growing season, cold-hardy hybrids only. Merritt Island §9.68 — flat Sacramento Delta island with portions below sea level (no elevation range in CFR), sub-AVA of Clarksburg. Eagle Foothills §9.252 — first Idaho-only AVA (sub-AVA of Snake River Valley). |

| 14 | AVA Rules | Middle Rio Grande Valley, Crystal Springs of Napa Valley, Malibu-Newton Canyon, Red Hill Douglas County OR, Champlain Valley of New York, Dos Rios, Sloughhouse, Haw River Valley, Eastern Connecticut Highlands, Alexandria Lakes, West Elks, Western Connecticut Highlands, Lower Long Tom | +13 rules, US phase COMPLETE | +13 rules. Rules total: 787. US Phase done: 0 remaining AVAs with wines. Phase 3 begins next cycle (France 129, Australia 84, South Africa 73, Spain 48, Greece 44). Notable: Crystal Springs of Napa Valley §9.296 est. 2024-11-15 — newest AVA in entire dataset. Ulupalakua already logged in Cycle 13. |
| 15 | France IGP | Vin de France, Pays d'Oc, Côtes de Gascogne, Val de Loire, Collines Rhodaniennes, Pays d'Hérault, Côtes Catalanes, Méditerranée, Ardèche, Côtes de Thongue | +10 rules, France 129→119 | Phase 3 cycle 1. All 10 are French IGPs (not AOCs) — permissive multi-color designations with 20-119 authorized varieties, no strict grape mandates. Sources: INAO CDCs from extranet.inao.gouv.fr. Notable: Vin de France is a VSIG (not an IGP) created 2009 to replace Vin de Table. Méditerranée spans 10 departments including all of Corsica. Pays d'Hérault and Ardèche both trace to the same foundational decree n° 68-807 of 13 September 1968. Pays d'Oc est. 1987-10-15 is France's largest IGP by volume (88,000 ha). No appellation_grapes inserted (all highly permissive umbrellas). Rules total: 797. Batch size upgraded to 15/cycle (3 agents × 5) based on mix analysis showing ~73% of remaining French appellations are IGPs. |
| 16 | AU+SA GIs | Stellenbosch, Barossa Valley, McLaren Vale, Margaret River, Adelaide Hills, Yarra Valley, Western Cape, Barossa (zone), Swartland, Clare Valley, Hunter Valley, Paarl, Coonawarra, Eden Valley, Robertson | +15 rules, AU 84→74, SA 73→68 | Phase 3 cycle 2. First AU/SA cycle — queue jumped from France (all <73 wines) to major AU/SA GIs (up to 2,205 wines). All 15 are purely geographic (no grape mandates). Sources: Wine Australia Register of Protected GIs (AU) + WOSA/SAWIS (SA). Exact WO district gazette dates not published by SA authorities — established_date NULL for Stellenbosch, Swartland, Robertson (year-only for Paarl, Robertson = 1973 WO system year). Coonawarra registered 2003-01-06 after 7-year Federal Court boundary dispute. area_ha = vineyard ha for AU (total GI land ha not broken out by Wine Australia). Rules total: 812. |
| 17 | AU+SA+ES | Franschhoek, Elgin, Mornington Peninsula, Coastal Region, Langhorne Creek, Walker Bay, Heathcote, Frankland River, Rutherglen, Castilla (ES IGP), Pyrenees (AU), Granite Belt, Riverina, Constantia, Wellington | +15 rules, AU 74→66, SA 68→62, ES 48→47 | Phase 3 cycle 3. First Spain entry (Castilla IGP — covers all of Castilla-La Mancha, 59 authorized varieties, est. 1999 per MAPA pliego). Notable: Granite Belt QLD is highest wine region in Australia (600–1,100 m). Riverina produces ~25% of Australian wine by volume (20,113 ha irrigated). Wellington SA officially demarcated 2012-09-21. Franschhoek elevated to WO district 2010 (previously ward within Paarl). SA WO exact gazette dates still mostly unavailable from public WOSA/SAWIS sources. Rules total: 827. |
| 18 | AU+SA+GR+ES+FR | Darling (SA), Tasmania (AU), Great Southern (AU), Peloponnese (GR PGI), Limestone Coast (AU), Castilla y León (ES IGP), Beechworth (AU), Gippsland (AU), Durbanville (SA), Crete (GR PGI), Piekenierskloof (SA), Orange (AU), Simonsberg-Paarl (SA), Geelong (AU), Île de Beauté (FR IGP) | +15 rules, AU 66→59, SA 62→58, ES 47→46, GR 44→42, FR 119→118 | Phase 3 cycle 4. First Greece entries (Peloponnese PGI + Crete PGI — both broad regional umbrellas est. 1989 with extensive authorized variety lists). First Corsica entry (Île de Beauté IGP est. 1982-01-25, 3,150 ha, 46 authorized varieties from INAO CDC). Castilla y León IGP (49 varieties, covers 94,000 km2, est. 2000, ITACyL pliego Rev.2 2020). Orange AU: unique altitude-defined GI boundary at 600 m (one of only two in Australia, alongside Adelaide Hills). Tasmania: 15.6 degrees C mean growing season temperature = coolest in Australia. Piekenierskloof elevation 650-750 m confirmed. Geelong registered 1996-06-11, 467 ha, three sub-regions. No cascades (all multi-color or geographic-only). Rules total: 842. Remaining: FR 118, AU 59, SA 58, ES 46, GR 42 = 323. |
| 19 | AU+GR+SA+FR | King Valley (AU), Cyclades (GR PGI), Mount Barker (AU), Swan Valley (AU), Riverland (AU), Adelaide (AU super zone), Tulbagh (SA), Breedekloof (SA), Var (FR IGP), Tumbarumba (AU), Bot River (SA), Alpilles (FR IGP), Pemberton (AU), Central Victoria (AU zone), Vin des Allobroges (FR IGP) | +15 rules, AU 59→51, SA 58→55, FR 118→115, GR 42→41 | Phase 3 cycle 5. Notable: Cyclades PGI (2008, full authorized variety list from Wines of Greece — 11 white + 9 red varieties including Mavrotragano); Riverland AU: 22,032 ha = one of the largest AU winegrape regions by tonnage; Swan Valley: hottest AU wine region (24.15 degrees C Jan mean); Breedekloof WO: 2006-06-02 from Government Gazette, 13,029 ha (SAWIS), 80-250 m elevation; King Valley: 1,535 ha, 155-860 m, Italian variety pioneer; Tumbarumba: 214 ha under vine in 3,007 km2 GI — sub-alpine, coolest NSW region; Var IGP est. 1981-03-05, 226 authorized varieties (key primary varieties noted); Bot River: minimal data (SAWIS SSL blocked); Alpilles/Vin des Allobroges: CDC locked, grape_rules null. No cascades (all geographic-only or multi-color). Rules total: 857. Remaining: FR 115, AU 51, SA 55, ES 46, GR 41 = 308. |
| 20 | FR+AU+SA+GR | Côte Roannaise (FR AOC), Vaucluse (FR IGP), Canberra District (AU), Bendigo (AU), Wrattonbully (AU), Grampians (AU), Simonsberg-Stellenbosch (SA), Padthaway (AU), Murray Darling (AU), Upper Hemel-en-Aarde Valley (SA), Hilltops (AU), Fleurieu zone (AU), Gard (FR IGP), Macedon Ranges (AU), Florina (GR PGI) | +15 rules, +1 appellation_grapes, FR 115→112, AU 51→42, SA 55→53, ES 46→46, GR 41→40 | Phase 3 cycle 6. Notable: Côte Roannaise AOC — full treatment: appellation_rules + appellation_grapes (GAMAY NOIR 100% required, association_type='required') for red+rosé only; whites sold as IGP Urfé; 215 ha, 350-550m granite hillsides. No color cascade (dual-color red+rosé). Vaucluse IGP: broad southern Rhône + international palette, 9,000 ha, CDC from INAO extranet. Gard IGP: 90+ authorized varieties (Article 5, CDC homologued 2019-04-04), 3 terroir zones. Florina GR PGI: ~700m elevation, Xinomavro appears in both white and red lists (blanc de noirs style possible). Upper Hemel-en-Aarde Valley SA WO: est. August 2006, Table Mountain Sandstone soils on north-facing slopes + decomposed granite south-facing, largest of 3 Hemel-en-Aarde wards. Murray Darling AU: 16,068 ha vineyard area (one of largest in dataset), 25,578 km2. Padthaway AU: 4,052 ha, flat limestone coast, irrigation-dependent, max 50m. Batch B agent died silently mid-run — re-launched, added ~30 min delay. Rules total: 872. Remaining: FR 112, SA 53, ES 46, AU 42, GR 40 = 293. |
| 22 | SA+GR+AU+ES+FR | Cape South Coast (SA), Evia (GR), Elim (SA), Calitzdorp (SA), Malgas (SA), Goulburn Valley (AU), Piccadilly Valley (AU), Cádiz (ES), Côtes de Thau (FR), Uclés (ES), South Coast (AU), Atlantique (FR), Pokolbin (AU), Nagambie Lakes (AU), Voor-Paardeberg (SA) | +15 rules, +0 appellation_grapes, FR 106→104, SA 50→45, ES 46→44, AU 38→33, GR 38→37 | Phase 3 cycle 8. Agents recovered after context compaction wiped prior session results — Batch C data reconstructed from session summary, A+B re-researched. Evia PGI: full variety list from EUR-Lex CELEX:52022XC0413(03) (EU OJ 2022 amendment) — 10 white + 10 red varieties, Savatiano ~4/5 of plantings. Uclés DO: highest-altitude DO in Castilla-La Mancha (500–1,200m), indigenous Tinto Velasco, old-vine gobelet fundamental characteristic. Piccadilly Valley AU: tiny 24.2 km² sub-GI within Adelaide Hills, 450–650m, podsolised duplex soils derived from 3 distinct geological formations. Côtes de Thau IGP: one of original vin de pays zones (1981 decree), 15 communes around Étang de Thau lagoon, ~75% white production, Terret Blanc emblematic. SA WO established dates unavailable for 4 of 5 SA wards (SAWIS SSL unreachable) — established_year null except Malgas (2010 vintage). No cascades (all geographic-only or multi-color). Rules total: 902. Remaining: FR 104, SA 45, ES 44, GR 37, AU 33 = 263. |
| 21 | GR+FR+AU+SA | Macedonia (GR PGI), Cévennes (FR IGP), Strathbogie Ranges (AU), Hemel-en-Aarde Ridge (SA), Comté Tolosan (FR IGP), Aude (FR IGP), Saint-Guilhem-le-Désert (FR IGP), Kangaroo Island (AU), Overberg (SA), Côtes du Lot (FR IGP), Cape Town (SA), Lenswood (AU), Geographe (AU), Coteaux de Béziers (FR IGP), Drama (GR PGI) | +15 rules, FR 112→106, SA 53→50, AU 42→38, GR 40→38, ES 46→46 | Phase 3 cycle 7. Notable: Macedonia GR PGI (1989) — largest Greek regional umbrella spanning 13 prefectures, 20+ indigenous + international varieties. Comté Tolosan IGP: 12 SW France departments from Pyrenees to Massif Central, explicitly enumerated principal varieties from CDC (11 rouge + 11 blanc). Aude + Côtes du Lot both trace to same foundational decree of 13 September 1968 (one of the original vin de pays decrees). Saint-Guilhem-le-Désert has two sub-zones (Cité d'Aniane and Val de Montferrand) with different variety lists and yields per 2022 CDC. Hemel-en-Aarde Ridge WO (2009) — highest elevation of the three Hemel-en-Aarde wards, Bokkeveld Shale soils. Cape Town WO (2017) — newest SA district in this dataset, 6,800 ha. Cévennes/Aude/Coteaux de Béziers CDCs all accessed from INAO extranet. No cascades (all multi-color or geographic-only). Rules total: 887. Remaining: FR 106, SA 50, ES 46, AU 38, GR 38 = 278. | Phase 3 cycle 6. Notable: Côte Roannaise AOC — full treatment: appellation_rules + appellation_grapes (GAMAY NOIR 100% required, association_type='required') for red+rosé only; whites sold as IGP Urfé; 215 ha, 350-550m granite hillsides. No color cascade (dual-color red+rosé). Vaucluse IGP: broad southern Rhône + international palette, 9,000 ha, CDC from INAO extranet. Gard IGP: 90+ authorized varieties (Article 5, CDC homologued 2019-04-04), 3 terroir zones. Florina GR PGI: ~700m elevation, Xinomavro appears in both white and red lists (blanc de noirs style possible). Upper Hemel-en-Aarde Valley SA WO: est. August 2006, Table Mountain Sandstone soils on north-facing slopes + decomposed granite south-facing, largest of 3 Hemel-en-Aarde wards. Murray Darling AU: 16,068 ha vineyard area (one of largest in dataset), 25,578 km2. Padthaway AU: 4,052 ha, flat limestone coast, irrigation-dependent, max 50m. Batch B agent died silently mid-run — re-launched, added ~30 min delay. Rules total: 872. Remaining: FR 112, SA 53, ES 46, AU 42, GR 40 = 293. |

---

## Run 3: 2026-04-07 Nightly — Open-Meteo Weather Drip (Scheduled Task)

**Duration:** ~44 min (single run, self-terminating on daily limit)
**Script:** `pipeline/fetch/open_meteo_weather.py --by-wines --delay 5`

### Summary
- **Start state:** 11 Open-Meteo appellations, 2,986 NASA POWER only
- **End state:** 20 Open-Meteo appellations (+9 upgraded tonight)
- **Data written:** 505 yearly rows + 6,060 monthly rows
- **API calls:** 19 calls, 0 cache hits, 8 errors (429 exhausted retries)
- **Stopped:** Hit daily limit after Sancerre (#20)

### Appellations Upgraded
| # | Appellation | Years |
|---|-------------|-------|
| 12 | Paso Robles | 46 |
| 13 | Columbia Valley | 46 |
| 14 | Bourgogne | 46 |
| 15 | Russian River Valley | 46 |
| 16 | Willamette Valley | 46 |
| 17 | Barolo | 46 |
| 18 | McLaren Vale | 45 |
| 19 | Chassagne-Montrachet | 46 |
| 20 | Sta. Rita Hills | 46 |
| 21 | Nuits-Saint-Georges | 46 |
| 22 | Ribera del Duero | 46 |
| 23 | Sancerre | 46 (daily limit hit after) |

### Errors (429 exhausted after 5 retries — skipped)
- Sonoma Coast, Rioja, Meursault, Stellenbosch, Alsace, Gevrey-Chambertin, Langhe, Finger Lakes
- These will be retried automatically tomorrow (not marked as fetched — resume mode will pick them up)

### Notes
- Script self-terminated cleanly (exit code 0)
- 429 rate-limit backpressure was heavy mid-run — most appellations needed 2+ retries
- 8 appellations exhausted all retries and were skipped (will retry tomorrow)
- Tomorrow's run resumes at ~appellation #21 in wine-count order
