# Session 12 Prompt — L3 Fact-Check Pass + Stage 1 Validation

Paste the below into a new session chat to start Session 12.

---

## Starting prompt

Session 12 of the Loam 30K Plan — **Build L3 fact-check pass and run Stage 1 enrichment validation.**

**Before starting, read:**
- `docs/BACKLOG.md` — the new append-only follow-up tracker. Look especially at the "Enrichment quality fix (three-layer redesign)" P0 entry. Read the LWIN long-tail sweep and grape synonym cleanup P1/P2 entries too so you know what ELSE is pending (do not start on those this session).
- `memory/30k_status.md` — current phase + next session's goal (Stage 1). This is your primary briefing file.
- `data/sessions.md` — Session 11's summary has all the L1 validation numbers.
- `data/stats/l1_audit.md` — the Session 11 L1 validation results table (per-wine deltas + remaining issue tags).
- `data/stats/enrichment_audit.md` — the original Session 10 audit that identified the problem.
- `pipeline/enrich/l1_test.py` — the working L1 prototype. The facts packet logic + prompt template live here. You'll refactor it this session.
- `pipeline/enrich/l1_audit.py` — the audit runner. Use this (or its `audit_grade_b` / `audit_grade_c` imports) to measure L1+L3 results against the same scale.

**Context from Session 11 (what's already proven):**
- Session 10 found the enrichment pipeline was writing factually unreliable copy: Grade C 2.48/5, Grade B 2.65/5, 111+91 factual_error tags across 70 sampled wines. Sonnet and Haiku both confabulate wine facts when the prompt doesn't constrain them.
- Session 11 designed a three-layer fix: **L1** retrieval-grounded prompts with a structured facts packet + explicit do-not-invent rules; **L2** per-field constraints (DB-only comparable wines, appellation_rules-backed terroir, real winemaker notes for vinification); **L3** post-generation fact-check pass (Haiku) that validates claims against ground truth, retries once if flagged.
- Session 11 validated L1 alone on the 10 worst audit samples. Result: **+1.2 avg overall, +1.57 on Grade B, 91 → 9 factual_error tags (−90%)**. Grade B jumped from 2.0 to 3.57, moving 7 fails to 0 fails (6 warns + 1 pass).
- Remaining Grade B issues after L1: vague_hedging (20), generic_filler (16), residual factual_error (9), sommelier_theater (6), voice_drift (5). L3 is confirmed necessary to close the final gap.
- Grade C only moved 2.0 → 2.33 on 3 samples (too small to draw conclusions). Needs a proper retest in production conditions (Haiku model, larger sample).
- `enrich-wine` Edge Function was deployed as v3 with `ENRICHMENT_ENABLED=false` feature flag. It returns 503 until L1+L3 ships.

**Session 12 goal:** Execute Stage 1 of the three-stage enrichment rollout plan. Specifically: refactor the L1 prototype into reusable modules, build the L3 fact-check pass, re-enrich the 34 wines that scored "fail" in the Session 10 audit, re-audit them, and decide whether to proceed to Stage 2.

**Budget:** ~$1.50 expected for Stage 1. Absolute limit: $5. If you blow past that without completing the stage, STOP and report.

---

## Tasks (in order)

### 1. Session briefing (5 minutes)

Query the database for live state:
```bash
python -m pipeline.analyze.thirty_k_dashboard
```

Confirm these numbers match expectations from `memory/30k_status.md`:
- Active wines ≈ 51,614 (51,790 from S10, minus 191 Session 11 dupe merges, plus 15 Fort Ross)
- Producers ≈ 2,530 (includes Fort Ross)
- Provenance coverage = 100% (was 98.5% pre-S11)
- Data grade breakdown should be unchanged from S10: B=105, C=4,857, D≈155, F=46,688

Report any surprises.

### 2. Build `pipeline/enrich/build_facts_packet.py` (30-45 min)

The facts packet builder is currently inline in `l1_test.py` as `fetch_hard_facts()` + `fetch_comparable_wines()` + the prompt builder. Refactor these into a proper reusable module so L3 and the eventual full re-enrichment script can both use it.

**Module structure:**
```python
# pipeline/enrich/build_facts_packet.py

def build_facts_packet(cur, wine_id: str) -> dict:
    """Returns a structured facts dict for a wine, ready to be serialized
    into a prompt. Contains hard facts, appellation law, vintage chemistry,
    explicit unknowns, and pre-fetched comparable wines."""
    facts = _fetch_hard_facts(cur, wine_id)
    facts["comparables"] = _fetch_comparable_wines(cur, wine_id, limit=3)
    facts["unknowns"] = _identify_unknowns(facts)
    return facts


def render_facts_packet(facts: dict) -> str:
    """Renders a facts dict into the markdown-style block used in prompts.
    This is the "## WINE IDENTITY / ## VINTAGE DATA / ## APPELLATION LAW /
    ## UNKNOWN FIELDS / ## COMPARABLE WINES" section from l1_test.py."""
    ...
```

Port the logic from `l1_test.py:fetch_hard_facts`, `l1_test.py:fetch_comparable_wines`, and the facts-packet sections of `l1_test.py:build_prompt`. The prompt *instructions* (the do-not-invent rules, the grade-specific field schema) stay in a separate module — see step 3.

**Test it standalone:** add a `__main__` block that takes a wine_id as an arg and prints the rendered packet. Test on `29d0e262-bbcb-432d-9276-f6a310e1e628` (Frei Brothers — had the L1 +1 improvement) and verify the output matches what `l1_test.py` produced.

### 3. Build `pipeline/enrich/enrich_prompts.py` (15 min)

Extract the prompt templates + generation logic from `l1_test.py`. Module structure:

```python
# pipeline/enrich/enrich_prompts.py

def build_grade_b_prompt(facts_packet_markdown: str) -> str:
    """L1 prompt for Grade B (8 narrative fields)."""
    ...

def build_grade_c_prompt(facts_packet_markdown: str) -> str:
    """L1 prompt for Grade C (3 fields: hook, style, comparable)."""
    ...

def call_model(client, prompt: str, model: str, max_tokens: int = 2000):
    """Shared Claude call + JSON parsing + markdown fence stripping."""
    ...
```

This unblocks L3 (which needs to call a second model on the same facts) and the eventual full re-enrichment script (which needs the same prompts).

**Important change from the L1 prototype:** tighten the voice language in both prompts. Add explicit rules the Session 11 audit flagged:
- "DO NOT use hedging words: may, tends to, appears to, seems to, might, could be, often, typically, generally"
- "DO NOT use sommelier theater: exquisite, elegant, harmonious, pairs beautifully, sophisticated, refined, dance of flavors, symphony of, complex interplay"
- "DO NOT use performative enthusiasm: exclamation marks, 'Where to begin?', 'What a wine!'"
- "Write in declarative statements. If you do not know, write 'Not documented' or omit the claim."

Keep the "safe inferences allowed" list from the L1 prototype.

### 4. Build `pipeline/enrich/fact_check_pass.py` (1 hour — the real work)

The L3 fact-check pass. Takes the enrichment JSON + the facts packet, asks Haiku to identify specific claims in the enrichment and verify each against the facts packet.

**Module structure:**
```python
# pipeline/enrich/fact_check_pass.py

HAIKU_MODEL = "claude-haiku-4-5-20251001"
PRICING = {"input": 1.0, "output": 5.0}  # per 1M

def fact_check(client, enrichment: dict, facts_packet: dict, grade: str) -> dict:
    """Runs Haiku to extract and verify factual claims. Returns a dict:
    {
      "verified": bool,  # true if no flagged claims
      "flagged": [
        {"field": "ai_wine_summary", "claim": "X", "issue": "not in facts",
         "severity": "high|medium|low"},
        ...
      ],
      "usage": {...},
      "cost_usd": ...
    }
    """
    ...


def retry_with_flags(client, prompt_builder, facts_packet: dict,
                     flagged_claims: list, grade: str) -> dict:
    """If L3 flags anything, re-call the generation model with the flags
    as corrections in the prompt ('remove or change these specific claims').
    Returns the corrected enrichment."""
    ...
```

**The L3 prompt (Haiku):**
```
You are a fact-checker for wine catalog entries. Your job: identify every
specific factual claim in the enrichment below, then check whether each
claim is supported by the ground-truth facts packet.

## ENRICHMENT TO CHECK
{enrichment_json}

## GROUND TRUTH (only source of verifiable facts for this wine)
{facts_packet_markdown}

## TASK

For each factual claim in the enrichment, determine:
1. Is it a SPECIFIC claim? (number, soil type, grape %, vinification method,
   named comparable wine, specific producer history date/year)
2. Is it supported by the ground truth?
   - "supported": the exact fact is in the ground truth
   - "not_in_facts": the fact is not in the ground truth AND could only be
     known from a specific source (NOT a safe inference like "Nebbiolo brings
     tar")
   - "contradicted": the ground truth says something different
3. Safe inferences are OK: grape character, broad appellation style,
   acid-cuts-fat food logic, oak-means-round-mouthfeel.

Return ONLY a JSON object:
{
  "verified": true/false,
  "flagged": [
    {
      "field": "ai_hook|ai_wine_summary|ai_style_profile|ai_terroir_expression|
                ai_food_pairing|ai_cellar_recommendation|ai_comparable_wines|
                ai_vinification_summary",
      "claim": "the specific sentence or phrase",
      "issue": "not_in_facts|contradicted",
      "severity": "high|medium|low",
      "suggested_fix": "remove|mark_unknown|replace_with"
    }
  ],
  "notes": "one sentence on overall quality"
}
```

**Retry logic:**
- If `flagged` is empty → mark verified, return as-is
- If `flagged` has 1-3 items (severity < high) → accept as warn, keep
- If `flagged` has any high-severity → retry generation ONCE with flagged claims in the prompt as corrections
- If retry still fails → drop the problem fields, return the rest

### 5. Add `fact_check_status` + `fact_check_flags` columns to `wine_insights` (5 min)

```sql
ALTER TABLE wine_insights
  ADD COLUMN fact_check_status TEXT
    CHECK (fact_check_status IN ('pending','passed','retried_passed','partial','failed'))
    DEFAULT 'pending',
  ADD COLUMN fact_check_flags JSONB;
```

Use `mcp__c4a52b5c-...__apply_migration` for this. Migration name: `add_wine_insights_fact_check_columns`.

### 6. Build `pipeline/enrich/stage1_revalidate.py` — the Stage 1 runner (30 min)

Reads the original `data/stats/enrichment_audit.json`, filters to wines with `overall.verdict == 'fail'`, re-enriches each one through the full L1+L3 pipeline (in-memory — **do NOT write to wine_insights**), then audits the results.

**Key design decisions:**
- **Do NOT update wine_insights.** This is a test. Results go to `data/stats/stage1_results.json` only.
- **Use Sonnet for Grade B, Haiku for Grade C.** Mirror the production model choice so the test is realistic. The Session 11 L1 prototype used Sonnet for everything — don't repeat that.
- **Call L3 on every re-enrichment** (not just Grade B). We need to know if Haiku-produced Grade C benefits from fact-checking too.
- **Re-audit** with `pipeline/enrich/l1_audit.py` helpers (import `audit_grade_b`, `audit_grade_c`, `summarize` from `enrichment_audit.py`).

**Output structure (`data/stats/stage1_results.json`):**
```json
{
  "run_date": "...",
  "models": {"grade_b": "sonnet", "grade_c": "haiku"},
  "n_wines": 34,
  "original_avg": 2.X,
  "stage1_avg": Y.Y,
  "delta": +Z.Z,
  "by_grade": {
    "B": {"n": 7, "orig_avg": 2.0, "new_avg": ..., "fact_check_passed": N},
    "C": {"n": 27, "orig_avg": 2.X, "new_avg": ..., "fact_check_passed": N}
  },
  "fact_check_impact": {
    "passed_first_try": N,
    "retried_once": N,
    "final_partial": N,
    "avg_flags_per_wine": X.Y
  },
  "cost_usd": X.XX,
  "results": [
    {
      "wine_id": "...",
      "grade": "B",
      "original_score": 2,
      "stage1_score": 4,
      "fact_check_status": "passed",
      "fact_check_flags": [],
      "retries": 0,
      "enrichment": {...}
    },
    ...
  ]
}
```

**Also write a markdown summary** to `data/stats/stage1_results.md` with before/after table.

### 7. Run Stage 1 and analyze (15 min + cost)

```bash
python -m pipeline.enrich.stage1_revalidate
```

Cost expectation: ~$1.50 total
- 27 Grade C enrichments with Haiku: ~$0.05
- 7 Grade B enrichments with Sonnet: ~$0.20
- 34 L3 fact-check calls with Haiku: ~$0.15
- ~5 retries (estimated): ~$0.05
- 34 audit calls with Sonnet (L1 audit): ~$1.00

If cost exceeds $5 without completing, STOP.

### 8. Decide: does Stage 1 pass? (15 min)

**Stage 1 passes if:**
- Grade B avg ≥ 4.0/5 (was 2.0 in original, 3.57 with L1-only)
- Grade C avg ≥ 3.5/5 (was 2.0 in original, 2.33 with L1-only but on 3 samples)
- 0 fails on Grade B
- ≤5 fails on Grade C (out of 27)
- Factual errors per grade dropped by >90% vs original
- Average L3 flags per wine < 2

**If all pass:** write `data/stats/stage2_plan.md` describing the vertical slice (500 California Cab + Burgundy wines), with estimated cost and a selection query. Do NOT execute Stage 2 — that's for a future session.

**If Grade B passes but Grade C stays weak:** document the Grade C gap and options (upgrade to Sonnet? Different format?). Ask user.

**If Grade B also stays weak:** diagnose. Is L3 prompt failing to catch errors? Are specific error types slipping through? Do we need L3 calls on specific high-risk fields (ai_vinification_summary) vs skipping others (ai_hook)? Report findings, stop, wait for user direction.

### 9. Wrap-up (15 min)

- Append Session 12 entry to `data/stats/30k_journal.md` — what was built, what Stage 1 found, decision for Stage 2
- Update `data/sessions.md` — move entry to Done
- Update `memory/30k_status.md` — reflect Stage 1 outcome and what Session 13 should do
- Update `docs/BACKLOG.md` — mark "Enrichment quality fix" status (in-progress or gated on Stage 2)
- Update `data/stats/30k_budget.json` — add Session 12 spend
- Regenerate dashboard: `python -m pipeline.analyze.thirty_k_dashboard`
- Commit with a clear message summarizing Stage 1 results and the Stage 2 decision

---

## Exit criteria

- `pipeline/enrich/build_facts_packet.py` built and tested
- `pipeline/enrich/enrich_prompts.py` built (tighter voice rules included)
- `pipeline/enrich/fact_check_pass.py` built with Haiku L3 + retry logic
- `wine_insights` has `fact_check_status` + `fact_check_flags` columns
- `pipeline/enrich/stage1_revalidate.py` built
- Stage 1 run complete, results in `data/stats/stage1_results.{json,md}`
- Decision made: proceed to Stage 2, fix and retry, or escalate
- All memory files + BACKLOG + sessions.md + journal updated
- Committed and pushed
- Total spend ≤ $5

---

## Don't do

- **Don't re-enrich the full corpus.** This is Stage 1 only — 34 wines, in-memory, nothing writes to `wine_insights`.
- **Don't start Stage 2 or Stage 3.** Those are separate sessions.
- **Don't touch the Edge Function feature flag.** `ENRICHMENT_ENABLED=false` stays until the full corpus re-enrichment lands.
- **Don't work on the LWIN long-tail sweep or broader grape synonym cleanup** — they're in the backlog for future sessions.
- **Don't rebuild `pipeline/analyze/enrichment_audit.py`.** Import from it instead.
- **Don't use `claude-sonnet-4-5` or `claude-haiku-3`.** Use `claude-sonnet-4-6` and `claude-haiku-4-5-20251001` (the latest models). Check `pipeline/enrich/batch_api.py` if unsure of the exact model IDs.
- **Don't skip the voice rule tightening in step 3.** The Session 11 audit found vague_hedging (20 tags) and generic_filler (16 tags) were the biggest remaining issues after L1. The tighter rules are cheap to add and should close most of that gap.
- **Don't create cron loops or background tasks.** Everything runs in the foreground, user-visible.

---

## Reference data (from Session 11)

**The 10 worst audit samples (used in Session 11 L1 test):**
```
B 29d0e262 Frei Brothers Chardonnay (original 2/5 → L1 3/5)
B 5de74b56 Henschke Julius (2/5 → 4/5 PASS)
B 777d8193 Fess Parker American Tradition (2/5 → 3/5)
B 8e03d24a Cuvaison Durrell (2/5 → 4/5)
B c2fb7430 Landmark Overlook (2/5 → 4/5)
B d4a3e184 Taylor's Fine Ruby (2/5 → 3/5)
B de8f4f9e Frank Family Chiles Valley (2/5 → 4/5)
C 069a7c0f des Bosquets La Font (2/5 → 3/5)
C 08120386 Louis Latour Les Quatre Journaux (2/5 → 2/5 no change)
C 12b6479f Felton Road Block 2 (2/5 → 2/5 no change)
```

**Stage 1 set (34 wines) = all "fail" verdicts from `data/stats/enrichment_audit.json`:**
- 7 Grade B fails (should match the 7 above)
- 27 Grade C fails (includes the 3 above plus 24 more; see `data/stats/enrichment_audit.json` results array, filter `audit.overall.verdict == 'fail'`)

**The original Session 10 audit scores to beat:**
- Grade C overall: 2.48/5
- Grade B overall: 2.65/5
- Grade C fails: 27/50
- Grade B fails: 7/20
- Top factual_error counts: 111 (C), 91 (B)

**Session 11 L1-only results (to beat):**
- Grade B avg: 3.57/5 (7 wines, 1 pass / 6 warn / 0 fail)
- Grade C avg: 2.33/5 (3 wines, 0 pass / 1 warn / 2 fail — small sample, retest in Stage 1)
- Factual errors on Grade B: 91 → 9 (−90%)

---

## Good luck

The design is validated, the infrastructure is in place. Stage 1 is mostly a matter of plumbing + running + measuring. If Grade B hits 4+/5 and Grade C hits 3.5+/5, we have a real enrichment pipeline. If not, the flagged issues will tell us exactly what to fix next.

Focus. Be surgical. Don't rebuild what already works. Commit at natural milestones so we don't lose progress.
