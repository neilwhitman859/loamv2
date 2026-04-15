# Task 3 Judge Calibration Notes

Preserved from B5.6 calibration session, 2026-04-15. These are the human
calibration scores that shaped the judge rubric and ground truth approach.

---

## The Setup

The user (wine enthusiast and owner of the Shafer wine being scored) was shown
8 blinded outputs (labels A-H) from different models on the same wine: Shafer
Vineyards One Point Five, Napa Valley. Same prompt, same data, different models.
The user scored each on 4 fields (hook, insider take, value assessment, food
pairing) on a 0-10 scale.

### Model Mapping (was blinded during scoring)

| Label | Model |
|-------|-------|
| A | qwen/qwen3.5-plus-02-15 |
| B | x-ai/grok-4.1-fast |
| C | minimax/minimax-m2.7 |
| D | xiaomi/mimo-v2-flash |
| E | deepseek/deepseek-v3.2 |
| F | mistralai/mistral-large |
| G | anthropic/claude-sonnet-4.6 |
| H | openai/gpt-5.4 |

---

## Human Scores (0-10 scale)

| Label | Model | Hook | Insider | Value | Food | Avg |
|-------|-------|------|---------|-------|------|-----|
| A | Qwen 3.5+ | 0 | 6 | 7 | 7 | 5.00 |
| B | Grok 4.1 Fast | 5 | 7 | 7 | 7 | 6.50 |
| C | Minimax M2.7 | 0 | 7 | 6 | 8 | 5.25 |
| D | MIMO v2 Flash | 0 | 4 | 7 | 7 | 4.50 |
| E | DeepSeek v3.2 | 8 | 6 | 8 | 8 | 7.50 |
| F | Mistral Large | 7 | 9 | 8 | 8 | 8.00 |
| G | Sonnet 4.6 | 0 | 8 | 8 | 9 | 6.25 |
| H | GPT-5.4 | 5 | 9 | 9 | 7 | 7.50 |

### Key Human Insights

- **Wrong facts in hooks = 0/10** regardless of quality of other writing. 4 of 8
  outputs (A, C, D, G) got 0 on hook because of fabricated naming origins.
- **Only E (DeepSeek) got the hook right**: "1.5 generations bridging father and
  son" — earned 8/10.
- **F (Mistral Large) highest overall at 8.0** — strong insider take: "Most people
  buy One Point Five because it's a Shafer wine that's not $300."
- **H (GPT-5.4) tied with E at 7.5** — peak Loam voice: "people who actually drink
  their Cabernet rather than collect labels."
- **G (Sonnet 4.6) — the production baseline — got 6.25** because its hook was wrong
  despite excellent insider/value/food.

---

## Judge Calibration Evolution

### Pass 1: Ungrounded Opus Judge (no ground truth)

Judge composite scores (converted to 0-10 for comparison):

| Label | Model | Human | Ungrounded Judge | Gap |
|-------|-------|-------|------------------|-----|
| H | GPT-5.4 | 7.5 | 7.9 | +0.4 ✓ |
| E | DeepSeek | 7.5 | 7.0 | -0.5 ✓ |
| D | MIMO Flash | 4.5 | 4.5 | 0.0 ✓ |
| F | Mistral Large | 8.0 | 7.0 | -1.0 |
| G | Sonnet 4.6 | 6.25 | **7.4** | **+1.2** ❌ |
| A | Qwen 3.5+ | 5.0 | **6.6** | **+1.6** ❌ |
| C | Minimax | 5.25 | **6.8** | **+1.5** ❌ |
| B | Grok 4.1 | 6.5 | 4.8 | -1.8 (harsh) |

**Problem identified:** The judge (Opus 4.6) had the SAME wrong training data as
the models. It believed "1.5 miles" and "Bay Area joke about drive time" were
correct naming origins because they're in its training data. It couldn't catch
fabrications that looked plausible.

### Pass 2: Grounded Judge (with ground truth injected)

Added `bakeoff/data/task3/ground_truth.json` with web-verified facts for all 30
wines. Judge prompt now includes the verified facts per wine.

| Label | Model | Human | Grounded Judge | Gap |
|-------|-------|-------|----------------|-----|
| H | GPT-5.4 | 7.5 | 7.45 | **-0.05 PERFECT** ✓ |
| G | Sonnet 4.6 | 6.25 | 5.88 | **-0.37** ✓ (was 7.4, now fixed) |
| E | DeepSeek | 7.5 | 6.75 | -0.75 ✓ |
| C | Minimax | 5.25 | 5.05 | **-0.20 PERFECT** ✓ |
| A | Qwen 3.5+ | 5.0 | 4.75 | **-0.25 PERFECT** ✓ |
| B | Grok | 6.5 | 4.50 | -2.0 (harsh on fab. oak) |
| F | Mistral Large | 8.0 | 5.63 | **-2.37** (still undervalues insider) |
| D | MIMO Flash | 4.5 | 2.38 | -2.12 (harsh — multiple errors) |

**Result:** Grounded judge aligns well with human on 5 of 8 outputs. Remaining
gaps are consistent (judge tends to be harsher than human on fabricated specifics,
and undervalues strong insider takes). These are acceptable biases given the
production use case — we want a judge that's strict on facts.

---

## Key Findings

### 1. Fabricated naming origins are invisible to the ungrounded judge
Models confidently state wrong naming stories ("1.5 miles", "15-year commitment",
"Bay Area drive time joke") that are in LLM training data. The judge believes them
because it has the same training data. **Web grounding is essential.**

### 2. The production pipeline needs web-grounded fact-checking
The bake-off judge limitation is the same limitation the production enrichment
will have. Any serious enrichment pipeline needs:
- Tech sheet lookup per wine (producer website)
- Claim extraction + web verification after generation
- Instruction: "do not invent naming origins" when producer data is thin

See `docs/WEB_GROUNDING_PATTERNS.md` for the pattern library.

### 3. Hook is the highest-stakes field for correctness
Users read the hook first. A wrong fact there destroys trust in everything else.
Human rubric: wrong fact in hook → 0 on hook regardless of writing quality.
Judge rubric: wrong hook fact → max 1.5 on correctness (hard cap).

### 4. The Opus judge undervalues strong insider takes
Consistent pattern: human scored F (Mistral Large) at 9/10 on insider take
("it's a Shafer wine that's not $300"), but judge scored 4.0-4.5 on voice.
The judge is conservative about rewarding opinionated takes. Not fatal — the
ranking order is still roughly right — but worth noting.

### 5. GPT-5-mini parse failure was OUR bug
87% parse failures turned out to be `max_tokens=3000` truncating the model
mid-JSON. Raised to 8000 before tournament. Moral: investigate parse failures
before eliminating models.

---

## For Future Calibration Rounds

- Ask the user to calibrate 4-8 blinded outputs per new wine category (not 63)
- Focus calibration on borderline cases at the 4/5 boundary
- Always use web-grounded judge, never ungrounded
- If the judge can't tell you WHY a fact is wrong (cite ground truth), its
  correctness score is unreliable
- Preserve the human scores in files like this one so calibration knowledge
  carries across sessions
