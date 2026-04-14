# Bake-off Design

The model bake-off tests candidates across three task tiers to find the best model
for each. Model lists come from `bakeoff_brief.md` (finalized externally). Everything
else — test sets, prompts, evaluation methodology, scoring rubrics — is designed here.

All tasks are treated with equal rigor. Tasks 1 and 2 are arguably more important
to Loam's product than Task 3: structured data correctness IS the product. Task 3
is where the money goes, but a wrong number in a fact grid hurts more than mediocre
prose.

---

## Principles

1. **No anchoring.** Every model starts fresh. We don't compare outputs to existing
   Sonnet enrichments — we evaluate on absolute quality against a rubric.
2. **Quantitative scoring, not pass/fail.** Every model gets continuous scores on
   every metric. We look at distributions, tier-level breakdowns, and cost-quality
   tradeoffs with the data in hand — not pre-committed thresholds.
3. **Ground truth is built, not assumed.** For Tasks 1 and 2, we construct labeled
   datasets as part of the bake-off. This is real work and part of the deliverable.
4. **Cost estimation per model.** Each task reports actual tokens consumed per call,
   projected to production volume. The final recommendation includes full-corpus cost
   at each tier.
5. **Blind evaluation.** The Opus judge never sees model names. Outputs are identified
   by opaque IDs only.
6. **Opus inline judging.** Task 3 prose is scored by Opus 4.6 in-conversation ($0
   marginal cost). Same pattern as the S2.3 audit. Tasks 1 and 2 are scored by
   automated comparison against ground truth.
7. **Judge calibration before scoring.** The Opus judge is calibrated on a sample
   before scoring the full set. See calibration protocol below.

---

## Task 1: Dedup / Classification (17 models)

### What we're testing

Can the model correctly classify wine-name pairs as SAME wine or DIFFERENT wine,
given structured metadata? This is a safety-critical task — a false merge destroys
data permanently.

### Test set: 200 labeled pairs

Built from real Loam data. Four strata of 50 pairs each:

**Stratum A — Clear same (50 pairs)**
Varietal suffix pattern: same wine, one record has the grape appended.
Source: the demo set duplicates already identified (Ridge "Blasi" vs "Blasi Zinfandel",
Stag's Leap "Cask 23" vs "Cask 23 Cabernet Sauvignon", etc). Pad with corpus examples.
Ground truth: SAME.

**Stratum B — Clear different (50 pairs)**
Same producer, similar names, but demonstrably different wines.
Types: different grape (York Creek Zinfandel vs York Creek Cabernet Sauvignon),
different color (Rodney's White vs Rodney's Red), different vineyard designation
(Fay vs Fay Block 2 at Stag's Leap), different wine type (sparkling vs still).
Ground truth: DIFFERENT.

**Stratum C — Classification/designation differences (50 pairs)**
Same base name, different quality tiers that make them distinct bottlings.
Types: Rioja Reserva vs Gran Reserva (López de Heredia Bosconia), German Prädikats
(Spätlese vs Auslese vs TBA with lot numbers), Champagne cuvées (Krug Collection
vs vintage), Alsace Grand Cru vs lieu-dit. These require wine domain knowledge.
Ground truth: DIFFERENT (classification tier = different wine in every case).

**Stratum D — Ambiguous / hard cases (50 pairs)**
Genuine judgment calls that require reasoning. Spelling variants (Cerreta vs Cerretta
at Conterno), NV vs named (Huet "Bourg" vs "Bourg Sec" vs "Bourg Moelleux"), style
suffixes (Brut vs Extra Brut), name-in-name containment ("La Vieille Ferme" vs
"La Vieille Ferme Rouge"). Mix of SAME and DIFFERENT ground truth.
Ground truth: determined by Opus analysis + LWIN cross-reference where available.

### Prompt template

```
You are classifying whether two wine records represent the same wine or different wines.
Two records are the SAME wine if they would share a single page in a wine encyclopedia
— same producer, same vineyard/cuvée, same grape variety, same style. They may differ
only in name formatting (abbreviation, varietal suffix appended/omitted).

Two records are DIFFERENT wines if they represent distinct bottlings that a consumer
would encounter separately: different grapes, different classification tiers (Reserva
vs Gran Reserva), different vineyard designations, different colors, different wine
types (still vs sparkling), or different sweetness levels (Sec vs Moelleux).

Key rules:
- Varietal suffix difference ALONE (e.g., "Armillary" vs "Armillary Cabernet Sauvignon")
  usually means SAME — the shorter name is just an abbreviation
- UNLESS the producer makes multiple wines from that vineyard with different grapes
- Different grapes = DIFFERENT wine, always, even if vineyard name matches
- Different Rioja classification (Reserva/Gran Reserva/Crianza) = DIFFERENT
- Different German Prädikat level (Kabinett/Spätlese/Auslese/BA/TBA) = DIFFERENT
- Different lot/cask numbers at the same Prädikat level = DIFFERENT
- Rosé vs Red from the same vineyard = DIFFERENT
- NV (non-vintage) vs named cuvée = requires analysis of whether they're the same bottling

## Wine Pair

Wine A: {name_a}
Wine B: {name_b}
Producer: {producer_name}

{metadata_block}

## Response Format (JSON only)

{
  "verdict": "SAME" or "DIFFERENT",
  "confidence": 0.0-1.0,
  "reasoning": "2-3 sentences explaining the key evidence"
}
```

The metadata_block includes grape data, color, appellation, wine_type, LWIN if
available — everything we have in the DB for both records.

### Scoring: automated against ground truth

Every model gets a full quantitative profile. No binary pass/fail.

**Metrics reported per model:**
- **Overall accuracy** (% of 200 pairs correct)
- **False positive rate (FPR):** predicted SAME when truth is DIFFERENT. The most
  dangerous error — a bad merge destroys data permanently.
- **False negative rate (FNR):** predicted DIFFERENT when truth is SAME. Less dangerous
  — a missed merge just leaves a dupe.
- **Per-stratum accuracy:** A (clear same), B (clear different), C (classification),
  D (ambiguous). Reveals whether a model aces easy cases but fails hard ones.
- **Confidence calibration:** correlation between reported confidence and actual accuracy.
  A model that says 0.95 confidence and is right 95% of the time is well-calibrated.
- **JSON parse rate:** % of responses that parse as valid JSON on first attempt.
- **Average tokens consumed** (input + output) for cost projection.
- **Projected cost @ 4,000 pairs** (production volume).

### Output format

```
bakeoff/scores/task1_scores.csv
```

| model | accuracy | fpr | fnr | stratum_a | stratum_b | stratum_c | stratum_d | confidence_corr | parse_rate | avg_tokens_in | avg_tokens_out | cost_per_call | cost_at_4k |
|-------|----------|-----|-----|-----------|-----------|-----------|-----------|-----------------|------------|---------------|----------------|---------------|------------|

---

## Task 2: Structured Data Extraction (17 models)

### What we're testing

Can the model extract structured wine data from raw HTML accurately? Three subtasks:

**2A. Tech sheet extraction** — Given a producer website page or tech sheet HTML,
extract: blend percentages, ABV, oak program (% new, months, vessel), cases produced,
chemistry (pH, TA, RS, SO2), harvest dates, winemaker name, farming practices.

**2B. Vintage discovery** — Given a producer's portfolio/archive page, extract a
complete list of wines and vintage years available.

**2C. Null discipline** — When a data field is not mentioned on the page, the model
must output null, not a plausible guess.

### Test set: 50 wine pages with ground truth

**Building ground truth (the hard part):**

1. Select 50 wine pages from producers with detailed tech sheets. Prioritize
   producers we haven't enriched (no contamination) but who publish rich data:
   - US producers with tech sheets: Shafer, Cakebread, Joseph Phelps, Paul Hobbs,
     Wind Gap, Bonterra, Knights Bridge
   - European producers with detail: Kir Yianni (Greece), Terroir Al Limit (Spain),
     Terre del Barolo (Italy), des Bosquets (Gigondas)
   - Australian producers: d'Arenberg, Howard Park, Tyrrell's, Penfolds
   - Budget producers (thin data, tests null handling): Black Box, Barefoot, Yellow Tail

2. Scrape each page's HTML (actual raw HTML, not cleaned).

3. Build ground truth JSON per page using Opus inline:
   - Opus reads the HTML and extracts all structured data
   - A human (you) verifies 15 of the 50 manually
   - Cross-reference ABV values against TTB COLA data where we have it (we have
     170K ABVs from TTB — strong external validation)
   - Fields genuinely absent from the page are labeled `null` in ground truth

4. Store as `bakeoff/data/task2/page_{n}.html` + `bakeoff/data/task2/truth_{n}.json`.

### Prompt template

```
Extract structured wine data from this webpage HTML. Return ONLY valid JSON.

For every field: if the information is not explicitly stated on the page, return null.
Do NOT guess, estimate, or infer values that are not clearly present in the text.
A null is always preferable to a wrong number.

## HTML Content
{raw_html}

## Output Format (JSON only)
{
  "wines_found": [
    {
      "name": "wine name as it appears on the page",
      "vintage_year": integer or null,
      "abv_pct": number or null,
      "blend": [{"grape": "name", "pct": number}] or null,
      "oak": {
        "vessel": "barrel type" or null,
        "months": integer or null,
        "new_pct": number or null,
        "origin": "French/American/etc" or null
      },
      "cases_produced": integer or null,
      "chemistry": {
        "ph": number or null,
        "ta_g_l": number or null,
        "rs_g_l": number or null,
        "so2_total_mg_l": number or null
      },
      "harvest_date": "YYYY-MM-DD" or null,
      "winemaker": "name" or null,
      "farming": "organic/biodynamic/sustainable/conventional" or null,
      "winemaker_notes": "verbatim text if present" or null
    }
  ]
}
```

### Scoring: automated against ground truth

Every model gets a full quantitative profile across multiple dimensions.

**Metrics reported per model:**

- **Numerical accuracy:** % of numerical fields (ABV, pH, TA, cases, blend %, oak %,
  months, vintage year) that match ground truth exactly (integers) or within ±0.1
  (decimals).
- **Hallucination rate:** % of fields where model outputs a specific value but ground
  truth is null. The most dangerous error — puts wrong numbers in user-facing fact grids.
- **Null recall:** When a field is genuinely absent from the page, how often does the
  model correctly output null? (1 - hallucination rate on absent fields.)
- **Null precision:** When the model outputs null, is the field genuinely absent? Low
  precision = model is overly cautious, missing data that exists on the page.
- **Text accuracy:** winemaker names (exact or close variant), farming practices
  (correct classification), grape names (matched against Loam grape table, synonym-aware).
- **Per-field breakdown:** Accuracy broken out by field type (ABV, pH, blend, oak, cases,
  chemistry, vintage, winemaker, farming). Reveals whether a model nails ABV but
  hallucinates pH.
- **Vintage discovery completeness:** % of ground-truth vintages found (subtask 2B).
- **Vintage discovery precision:** % of extracted vintages that are real (no hallucinated years).
- **JSON parse rate:** % of responses that parse as valid JSON on first attempt.
- **Average tokens consumed** for cost projection.
- **Projected cost @ 50,000 calls** (production volume: 15K extraction + 35K vintage discovery).

### Output format

```
bakeoff/scores/task2_scores.csv
```

| model | num_accuracy | halluc_rate | null_recall | null_precision | text_accuracy | vintage_completeness | vintage_precision | parse_rate | avg_tokens_in | avg_tokens_out | cost_per_call | cost_at_50k |
|-------|-------------|-------------|-------------|----------------|---------------|---------------------|-------------------|------------|---------------|----------------|---------------|-------------|

Also: `bakeoff/scores/task2_per_field.csv` with per-model per-field accuracy breakdown.

---

## Task 3: Customer-Facing Prose (21 models)

### What we're testing

Can the model write wine enrichment prose that is specific, direct, factually grounded,
structurally varied, and voice-matched — at a level a knowledgeable wine person would
call genuinely good writing?

### Test set: 30 wines, stratified

Selected from the 150K unenriched wines to avoid contamination. Stratified across
three difficulty tiers:

**Tier A — Famous wines with rich data (10 wines)**
High structured data coverage (grapes, scores, prices, vintages). Well-known producers
and appellations where the model's wine knowledge should be deep. Tests whether the
model uses prompt data vs fabricates from training data.
Candidates: Shafer One Point Five, Penfolds Bin 2, Catena Zapata High Mountain Vines,
Fritz Haag Brauneberger Juffer Sonnenuhr, Cakebread Cuttings Wharf, Tyrrell's Vat 1,
Paul Hobbs Agustina Chardonnay, d'Arenberg The High Trellis, Oyster Bay Chardonnay,
Vasse Felix Filius.

**Tier B — Mid-obscurity with moderate data (10 wines)**
Some scores, some vintages, but less famous. The model needs to work harder — can't
coast on general knowledge. Tests specificity under uncertainty.
Candidates: Kir Yianni Ramnista (Naoussa, Greece), Terroir Al Limit Les Tosses (Priorat),
Comando G El Reventon (Cebreros), Terre del Barolo Arnaldo Rivera, Leo Hillinger
Blaufrankisch (Burgenland), des Bosquets Les Routes (Gigondas), Montes M Alpha (Chile),
Lafage Tessellae (Côtes Catalanes), Howard Park Leston (Margaret River), Atamisque
Chardonnay (Mendoza).

**Tier C — Deep cuts with thin data (10 wines)**
Minimal structured data. Tests null discipline in prose (does the model make things up
to fill the fields?) and honest uncertainty handling.
Candidates: Wind Gap Woodruff Pinot Noir, Knights Bridge West Block Chardonnay,
Bonterra The Roost, Layer Cake Chardonnay, Black Box Deep & Dark, Barefoot Buttery
Chardonnay, Bota Box Nighthawk, Yellow Tail Pure Bright, Tolosa 1772 Grenache,
Plantagenet Omrah Cabernet.

### Prompt

Use the existing Grade A prompt from `batch_enrich.py` (VOICE_PREAMBLE + identity
block + data blocks + cascade context + output format). This is the production prompt.
The bake-off tests whether other models can execute this same prompt well.

The prompt is identical for every model — same context data, same instructions, same
output schema. Only the model changes.

### Scoring: 5-Dimension Rubric (Opus inline)

Every output is scored by Opus inline on five dimensions, each 1.0–5.0 (continuous,
half-points allowed). The judge receives:
- The input context (what data was available)
- The model output (blinded — no model name)
- The full VOICE.md
- The rubric below with concrete examples at each score level

#### Dimension 1: Factual Grounding (weight: 30%)

Does the output reference the data in the prompt? Does it fabricate?

| Score | Definition | Example |
|-------|-----------|---------|
| 5 | Every factual claim traceable to prompt data. Chemistry values match. Grape percentages match. No unsourced specifics. | "75% Zinfandel, 20% Petite Sirah, 5% Carignane — aged 14 months in American oak" (all from prompt) |
| 4 | One minor unsourced claim that's plausibly true but not in the prompt. All numbers match. | Adds "volcanic soils" for an Etna wine — likely true but not stated in prompt data |
| 3 | Mixes prompt data with plausible additions. No contradictions but some claims can't be verified. | Mentions "3,000 cases" when prompt says 3,200 |
| 2 | Makes specific numerical claims not in the prompt. Invents percentages or chemistry. | States "pH 3.45" when no pH data was provided |
| 1 | Largely fabricated. Multiple invented specifics. Contradicts prompt data. | Wrong grape variety, wrong appellation, invented scores |

**Scoring note:** Any output that states a specific number (pH, ABV, blend %, cases)
not present in the prompt data gets max 1.5 on this dimension. This is the hardest
penalty in the rubric because fabricated numbers in fact grids directly undermine the product.

#### Dimension 2: Specificity (weight: 20%)

Does it name actual things, or describe in generalities?

| Score | Definition | Example |
|-------|-----------|---------|
| 5 | Names specific soils, climate patterns, geological formations, vineyard features. Every descriptor is concrete. | "The Willakenzie series soils — uplifted marine sedimentary deposits — give the wine a chalky mineral spine" |
| 4 | Mostly specific with one generic phrase that could be cut. | Good throughout but includes "producing wines of notable quality" once |
| 3 | Mix of specific and generic. Some good details surrounded by filler. | Names the appellation and grape correctly but describes terroir as "favorable conditions" |
| 2 | Mostly generic. Uses "well-drained soils", "favorable climate", "careful winemaking". | Could describe almost any wine in the same region |
| 1 | Entirely generic. No concrete details. Pure atmosphere. | "An exceptional wine showcasing the best of the region" |

#### Dimension 3: Voice Match (weight: 25%)

Does it sound like Loam? Reference: VOICE.md + the Cain Five / Christopher Howell
touchstone — knowledgeable, unpretentious, place-centered, comfortable being negative.

| Score | Definition | Example |
|-------|-----------|---------|
| 5 | Indistinguishable from the best Loam content. Direct, opinionated, explains the "why," names names. Comfortable saying when something isn't great. | "Most Côtes du Rhône is forgettable. This isn't — des Bosquets farms old-vine Grenache on the limestone slopes east of Gigondas proper, and you can taste the altitude." |
| 4 | Right voice with minor lapses. One hedge too many, or one moment of generic praise. | Good throughout but includes "seems to be gaining recognition" instead of stating it directly |
| 3 | Recognizably trying for the Loam voice but with template patterns, banned words, or sommelier theater creeping in. | Uses "elegant" or "showcases" despite the banned list. Hedges with "appears to" |
| 2 | Generic wine writing. Correct but sounds like any wine website. | "A beautiful expression of the terroir with silky tannins and a long finish" |
| 1 | Sommelier theater or chatbot wine copy. | "An exquisite accompaniment to fine dining, this wine elevates the dining experience" |

**Scoring note:** Any output containing a BANNED WORD from VOICE_PREAMBLE gets max 2.0
on this dimension. Banned words listed in `batch_enrich.py`.

#### Dimension 4: Structural Diversity (weight: 10%, batch metric)

Do consecutive outputs from the same model read differently?

| Score | Definition |
|-------|-----------|
| 5 | Every wine feels like its own piece. Varied openers, varied structures. No detectable template. |
| 4 | 1-2 recognizable patterns across 10 wines but most feel distinct. |
| 3 | A template is visible — similar openers, similar field structures. Content varies but scaffolding repeats. |
| 2 | Most wines follow the same structure. Feels like a form with blanks filled in. |
| 1 | Identical structure every time. Mad libs with a wine skin. |

**Measurement method:**
- Opus reads all 10 Tier A outputs from each model in sequence and scores diversity.
- Also compute automated metrics as secondary signal:
  - Opening sentence embedding cosine similarity (average pairwise across 10 wines)
  - Trigram overlap in hook fields
  - Count of repeated opener patterns (e.g., how many start with the producer name?)

#### Dimension 5: Usefulness (weight: 15%)

Would a wine enthusiast learn something they didn't know?

| Score | Definition | Example |
|-------|-----------|---------|
| 5 | Genuinely informative. Reader comes away understanding something new about this wine, this place, or this producer. Makes the data come alive. | Explains WHY this vineyard's elevation matters for this grape — connects a specific weather pattern to a flavor outcome |
| 4 | Good information density. One or two sentences could be cut without losing insight, but overall teaches well. | Solid on terroir and winemaking, food pairing is generic ("grilled meats") |
| 3 | Correct but not particularly enlightening. Reorganizes the prompt data into prose without adding understanding. | Restates "75% Zinfandel" as "predominantly Zinfandel" — true but adds nothing |
| 2 | Mostly atmosphere, little substance. Could remove the prose and the fact grid alone would be more useful. | Two paragraphs of mood-setting before any actual information |
| 1 | Adds nothing. Pure filler. | Describes the wine in terms so vague they could apply to any wine |

### Composite Score

**Weighted average:**
- Factual Grounding: **30%**
- Voice Match: **25%**
- Specificity: **20%**
- Usefulness: **15%**
- Structural Diversity: **10%**

Reported per model both as composite and per-dimension. Also broken out by tier
(A/B/C) — a model that scores 4.5 on famous wines but 3.0 on thin-data wines has
a grounding problem visible in the tier split. This matters because most of the 156K
corpus looks like Tier C.

### Opus Judge Calibration Protocol

Before scoring the full set, the Opus judge (me) is calibrated against human evaluation.

1. **Pick 3 calibration wines** — one from each tier (A/B/C). Run all 21 models on
   these during the model run phase.

2. **Score the 3 × 21 = 63 outputs openly (not blinded).** Show full reasoning for
   each score: "I gave this a 3 on Voice because it uses 'elegant' twice and hedges
   with 'seems to' — here's the exact text."

3. **Human reviews ~10-15 scores** — focusing on the 4/5 boundary (the scores that
   matter most for the production decision). Human says: "That's a 3, not a 4" or
   "Yep, that's right."

4. **If disagreement on >20% of the sample,** revise rubric language to sharpen the
   boundary and re-score the calibration set.

5. **Once aligned, score the remaining 27 wines blinded.** Same rubric, now calibrated.

6. **Self-consistency check.** After scoring all 30 wines, re-score the 3 calibration
   wines blind (without seeing earlier scores). If re-scores differ by >0.5 on
   average from the calibration round, flag the drift and review.

### Output format

**Per-wine per-model scores (the raw data):**
```
bakeoff/scores/task3_scores.csv
```

| wine_id | wine_name | tier | model_id | grounding | specificity | voice | diversity | usefulness | composite | tokens_in | tokens_out | cost |
|---------|-----------|------|----------|-----------|-------------|-------|-----------|------------|-----------|-----------|------------|------|

630 rows (21 models × 30 wines), plus 21 diversity scores.

**Model summary (the decision table):**
```
bakeoff/scores/task3_summary.csv
```

| model | avg_grounding | avg_specificity | avg_voice | diversity | avg_usefulness | composite | tier_a_composite | tier_b_composite | tier_c_composite | avg_tokens_in | avg_tokens_out | cost_per_call | cost_at_170k |
|-------|-------------|-----------------|-----------|-----------|----------------|-----------|-----------------|-----------------|-----------------|---------------|----------------|---------------|--------------|

21 rows. This is what the production decision comes from.

**Cost-quality data (the chart):**
For each model: composite score (y-axis) vs cost per 170K wines (x-axis). The winner
is the point closest to top-left: highest quality at lowest cost.

---

## Execution Plan

### B5.2: Build test data

1. **Task 1:** Generate 200 labeled pairs from DB data.
   - Query same-producer wines with high name similarity
   - Label ground truth using LWIN cross-reference, grape/color data, Opus analysis
   - Save to `bakeoff/data/task1/pairs.json`

2. **Task 2:** Scrape 50 producer website pages, build ground truth.
   - Fetch HTML from the 15-20 producers with good tech sheets
   - Build ground truth JSON per page (Opus extraction + human verification sample)
   - Cross-validate ABV against TTB COLA data
   - Save to `bakeoff/data/task2/`

3. **Task 3:** Assemble context packages for 30 test wines.
   - Pull structured data from DB for each wine
   - Build the same context dict that `batch_enrich.py` would build
   - Save to `bakeoff/data/task3/contexts.json`

### B5.3: Run + score Task 1 (dedup)

Run 17 models × 200 pairs = 3,400 API calls via OpenRouter.
Score automatically against ground truth. Generate `task1_scores.csv`.
Fast — mostly automated, one block.

### B5.4: Run + score Task 2 (extraction)

Run 17 models × 50 pages = 850 API calls via OpenRouter.
Score automatically against ground truth. Generate `task2_scores.csv` and
`task2_per_field.csv`. Fast — mostly automated, one block.

### B5.5: Run Task 3 (prose)

Run 21 models × 30 wines = 630 API calls via OpenRouter.
Save raw outputs. No scoring yet. This block just collects the data.

### B5.6: Score Task 3 (prose) — Opus inline judging

1. Calibrate on 3 wines × 21 models (63 outputs, open scoring, human review).
2. Score remaining 27 wines × 21 models (567 outputs, blinded).
3. Self-consistency check on calibration wines.
4. Generate `task3_scores.csv` and `task3_summary.csv`.
5. Share interesting comparisons along the way.

May need to split across sessions if context fills. Each wine-pass reads ~21 outputs
(~25K tokens) + rubric context.

### B5.7: Final report

1. Rank models per task with scores, cost projections, and tier breakdowns.
2. Identify best models per tier (may surface multi-model strategies).
3. Flag surprises — cheap models beating expensive, or vice versa.
4. Final recommendation: one model per task tier + projected full-corpus cost.
5. Cost-quality scatter data for the decision chart.

---

## Deliverables

```
bakeoff/
  DESIGN.md                  ← this document
  data/
    task1/pairs.json         ← 200 labeled dedup pairs with ground truth
    task2/                   ← 50 HTML pages + ground truth JSONs
    task3/contexts.json      ← 30 wine context packages
  results/
    task1/{model}/           ← raw dedup verdicts per pair
    task2/{model}/           ← raw extraction outputs per page
    task3/{model}/           ← raw prose outputs per wine
  scores/
    task1_scores.csv         ← per-model: accuracy, FPR, FNR, per-stratum, cost
    task2_scores.csv         ← per-model: num accuracy, halluc rate, null handling, cost
    task2_per_field.csv      ← per-model per-field accuracy breakdown
    task3_scores.csv         ← per-wine per-model: 5 dimensions + composite + cost
    task3_summary.csv        ← per-model: averages, tier splits, composite, cost
  REPORT.md                  ← final ranking, recommendations, cost projections
```

---

## Budget

Estimated:
- B5.3 Task 1 API calls: ~$2-5 (3,400 calls, small token budget)
- B5.4 Task 2 API calls: ~$8-12 (850 calls, larger token budget from HTML)
- B5.5 Task 3 API calls: ~$8-12 (630 calls, moderate token budget)
- B5.6 Opus judging: **$0** (inline)
- Task 1+2 automated scoring: **$0**
- **Total: ~$20-25**

This is ~$20 to avoid a bad $1,700+ production decision.

---

## Additional Metrics (collected during model runs)

These are captured per-call during execution, not scored separately:

1. **JSON parse rate:** % of responses that parse as valid JSON on first attempt.
   Collected for all three tasks. A model that produces great content but breaks JSON
   10% of the time adds engineering complexity to the production pipeline.

2. **Speed (tokens/sec):** Wall-clock time per call via OpenRouter. Lightweight metric
   but matters for batch-processing 170K wines. A model that's 5x slower turns a
   2-day job into a 10-day job.

3. **Token consumption:** Actual input + output tokens per call. Some models are
   wordier than others on the same prompt. Affects production cost directly.
