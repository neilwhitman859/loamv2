# Session 4 - Broad Bakeoff Design

**Date:** 2026-04-20  
**Goal:** lock the adjudication bakeoff before any packet-generator or queue-building work starts  
**Primary deliverable:** frozen v1 bakeoff spec for the merge-only producer-dedup rebuild  
**Not doing:** packet-generator implementation, adjudication queues, merge execution SQL, `PARENT_CHILD` expansion, or benchmark edits beyond factual corrections  
**Stop when:** one durable bakeoff spec exists, the contender list is frozen for v1, the scorecard math is explicit, and the next session can build the packet generator without changing the rules halfway through

---

## 1. What This Bakeoff Is Actually Testing

The first bakeoff is a test of **adjudication quality**, not a test of candidate generation and not a test of alternate packet schemas.

What stays fixed in v1:

- `benchmark_v1.json` is the frozen answer key.
- `evidence_packet_v1.md` is the frozen visible-packet schema.
- Retrieval is part of packet generation, not part of adjudication.
- `PARENT_CHILD` stays out of scope.
- Every contender must emit the same adjudication output contract.

What varies in v1:

- adjudication method class
- adjudication model
- whether the final verdict comes from one model or a consensus rule

Reason: we already decided that search is retrieval, not truth by itself. The packet generator should gather evidence once; the bakeoff should then measure how well contenders judge the same evidence.

---

## 2. Frozen Contender Set For v1

This list is frozen for the first bakeoff. Do not add or swap contenders mid-run.

| contender_id | method class | model(s) | role | production-eligible |
|---|---|---|---|---|
| `deterministic_control_v1` | deterministic control | none | shadow baseline only | no |
| `haiku_single_v1` | single-model adjudicator | Claude Haiku 4.5 | cheap primary candidate | yes |
| `gemini_single_v1` | single-model adjudicator | Gemini 2.5 Flash | cheap cross-family candidate | yes |
| `gpt5mini_single_v1` | single-model adjudicator | GPT-5.4-mini | strong non-Claude candidate | yes |
| `haiku_gemini_consensus_v1` | consensus adjudicator | Claude Haiku 4.5 + Gemini 2.5 Flash | safety-first low-cost ensemble | yes |
| `sonnet_single_v1` | single-model adjudicator | Claude Sonnet 4.5 | expensive upper-bound / fallback candidate | yes |

### Why these six

- `deterministic_control_v1` tells us what the packet-free rule floor looks like, but it is not allowed to "win" because v1 is explicitly testing whether grounded adjudication beats pure deterministic heuristics.
- `haiku_single_v1`, `gemini_single_v1`, and `gpt5mini_single_v1` give three distinct cheap-to-mid-cost single-model families.
- `haiku_gemini_consensus_v1` explicitly tests the strongest lesson from prior producer-dedup calibration: cross-family agreement is often safer than trusting one model's confidence alone.
- `sonnet_single_v1` is the expensive quality ceiling and the most plausible fallback if the cheaper paths are too lossy or too flag-heavy.

### Operating conditions frozen across all non-control contenders

- Same `benchmark_v1` cases.
- Same `evidence_packet_v1` visible packet.
- Same adjudicator instructions version: `bakeoff_adjudicator_v1`.
- Same JSON-only output schema.
- Same `temperature = 0`.
- No tool use and no live web search during adjudication.
- No access to `benchmark_overlay`.

If a model is unavailable at runtime, stop and amend the spec in a later session. Do not silently substitute another model.

---

## 3. Deterministic Control Policy

Decision: the deterministic-only control is **scored in the same bakeoff report but ranked separately and excluded from winner selection**.

That means:

- It gets the same packet id list and the same normalized output shape.
- It appears in score tables for context.
- It does **not** count as a valid production path or fallback path.

Reason: we want one baseline row in the report, but we do not want a control designed for easy conservative cases to crowd out the adjudication paths we actually intend to use.

---

## 4. Exact Input Contract

The packet generator and evaluation harness must work against the following frozen three-layer contract.

### 4.1 Hidden benchmark case

Source: `data/sprints/dedup/benchmark_v1.json`

Required benchmark fields:

- `case_id`
- `pair_id`
- `producer_name_a`
- `producer_name_b`
- `country_a`
- `country_b`
- `stratum`
- `source_pair_tier`
- `expected_verdict`
- `pattern_cluster`
- `historical_failure_mode`
- `source_of_truth`
- `source_artifact`
- `rationale`

These fields exist so the harness can:

- choose the frozen pair list
- validate packet resolution against the live DB
- score outputs against the hidden answer key
- break out results by `core` vs `tail` and by benchmark stratum

Only `pair_id` and the derived `pair_tier` may influence the visible packet. The answer-bearing benchmark fields must stay hidden.

### 4.2 Visible evidence packet

Source: packet generator output following `data/sprints/dedup/evidence_packet_v1.md`

Visible packet contract:

```json
{
  "packet_version": "v1",
  "packet_id": "producer_pair_<pair_id>_v1",
  "envelope": {
    "pair_id": 4067,
    "producer_id_a": "uuid",
    "producer_id_b": "uuid",
    "pair_tier": "core | tail | unknown",
    "candidate_family": "same_country_lexical_alias | cross_country_same_brand | rare_wine_anchor | catalog_coherence | mixed",
    "source_methods": ["blocking:s2_trigram", "blocking:s9_substring"],
    "generated_at": "2026-04-20T21:30:00-04:00",
    "data_cutoff_at": "2026-04-20T21:25:00-04:00",
    "completeness": {
      "local_catalog": "complete | partial | missing",
      "retrieval": "complete | partial | missing",
      "survivor_calc": "complete | partial | missing"
    }
  },
  "evidence": {}
}
```

Mandatory contamination rule:

- `envelope.benchmark_overlay` may exist in the stored packet row for scoring joins, but it must be removed before the model sees the packet.

### 4.3 Adjudicator request wrapper

Every non-control contender receives the same request wrapper:

```json
{
  "benchmark_id": "producer_dedup_benchmark_v1",
  "case_id": "blind_core_audit_001",
  "contender_id": "haiku_single_v1",
  "instructions_version": "bakeoff_adjudicator_v1",
  "packet_visible": {},
  "output_contract_version": "adjudication_output_v1",
  "allow_tools": false,
  "temperature": 0
}
```

### 4.4 Packet-build validation rules

Before a packet is accepted into the bakeoff corpus, the generator must verify:

1. `pair_id` resolves in the current DB.
2. The resolved producer ids match the expected pair members for that benchmark case.
3. Current producer names and countries are still recognizably aligned with the benchmark row.
4. Missing retrieval or survivor data is marked in `completeness`; it is never hidden.

If validation fails, the pair is a `packet_build_error` and the bakeoff stops until the case is repaired or formally corrected as a benchmark factual fix.

---

## 5. Frozen Adjudicator Output Contract

Model-visible adjudication output stays exactly aligned with `evidence_packet_v1`.

```json
{
  "packet_id": "producer_pair_4067_v1",
  "verdict": "MERGE | SKIP | FLAGGED",
  "confidence": 0.94,
  "rule_ids": ["11.4.h"],
  "reason": "Short/full-form alias for the same Chablis estate; local catalog and external evidence align.",
  "key_support_refs": ["official_1", "lexical_short_full_form", "catalog_subset_match"],
  "key_contradiction_refs": ["catalog_asymmetry"],
  "survivor_producer_id": "uuid_or_null",
  "follow_up": null
}
```

Output rules frozen for v1:

- `verdict` must be one of `MERGE`, `SKIP`, `FLAGGED`.
- `PARENT_CHILD` is invalid in v1.
- `confidence` must be a numeric value from `0` to `1`.
- `rule_ids` must cite valid `docs/IDENTITY_RULES.md` paths.
- `MERGE` must include `survivor_producer_id` when the packet includes a non-null deterministic survivor recommendation.
- `SKIP` must set `survivor_producer_id = null`.
- `FLAGGED` may set `survivor_producer_id = null` and should use `follow_up` to say what evidence is missing or contradictory.

### 5.1 Harness-normalized result row

The scorer needs more than the model's raw JSON. The evaluation harness should persist one normalized row per contender per case:

```json
{
  "benchmark_id": "producer_dedup_benchmark_v1",
  "case_id": "blind_core_audit_001",
  "packet_id": "producer_pair_645_v1",
  "contender_id": "haiku_single_v1",
  "normalized_output": {},
  "schema_valid": true,
  "citation_integrity": true,
  "rule_trace_valid": true,
  "timing_ms": 1832,
  "usage": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "search_calls": 0,
    "cost_usd": 0.0
  }
}
```

### 5.2 Fail-closed normalization

If a contender emits invalid JSON, an illegal verdict, missing required fields, broken refs, or a `MERGE` with no required survivor, the harness must:

- mark `schema_valid = false`
- coerce the normalized verdict to `FLAGGED`
- set `follow_up = "schema_error"` or a more specific failure code

This keeps bad outputs from disappearing while still preventing them from being mistaken for executable decisions.

---

## 6. How We Score The Bakeoff

We do **not** use one blended magic score to decide whether a contender is safe. We use:

1. raw outcome counts
2. explicit rates
3. hard gates
4. deterministic winner-selection ordering among contenders that already passed the gates

### 6.1 Outcome buckets

For each case:

- `true_merge` = expected `MERGE`, predicted `MERGE`
- `true_skip` = expected `SKIP`, predicted `SKIP`
- `false_merge` = expected `SKIP`, predicted `MERGE`
- `hard_missed_merge` = expected `MERGE`, predicted `SKIP`
- `soft_missed_merge` = expected `MERGE`, predicted `FLAGGED`
- `safe_flag` = expected `SKIP`, predicted `FLAGGED`
- `survivor_error` = `true_merge` but wrong `survivor_producer_id` on a survivor-scorable case

### 6.2 Primary rates

Let:

- `M` = number of benchmark cases whose expected verdict is `MERGE`
- `S` = number of benchmark cases whose expected verdict is `SKIP`
- `SM` = number of `MERGE` cases with deterministic survivor recommendation present and `completeness.survivor_calc = complete`

Then:

- `false_merge_rate = false_merge / S`
- `hard_missed_merge_rate = hard_missed_merge / M`
- `soft_missed_merge_rate = soft_missed_merge / M`
- `merge_capture_rate = true_merge / M`
- `safe_flag_rate = safe_flag / S`
- `flag_rate_total = (soft_missed_merge + safe_flag) / (M + S)`
- `survivor_accuracy = (SM - survivor_error) / SM`
- `exact_verdict_accuracy = (true_merge + true_skip) / (M + S)`

### 6.3 Cost metrics

Report all three:

- `cost_per_pair = total_cost_usd / total_cases`
- `median_latency_ms`
- `tokens_per_pair = (prompt_tokens + completion_tokens) / total_cases`

For consensus contenders, cost is the sum of both child runs plus consensus glue logic.

### 6.4 Auditability score

Auditability is a scored dimension, not a vibe check.

Let:

- `schema_valid_rate = valid_schema_cases / total_cases`
- `citation_integrity_rate = citation_integrity_cases / total_cases`
- `rule_trace_rate = valid_rule_trace_cases / total_cases`

Then:

`auditability_score = 0.40 * schema_valid_rate + 0.40 * citation_integrity_rate + 0.20 * rule_trace_rate`

Where:

- `schema_valid_rate` means the normalized output fully matched the contract before any fail-closed coercion.
- `citation_integrity_rate` means every cited support/contradiction ref resolves to a packet item and the minimum evidence requirements were met.
- `rule_trace_rate` means at least one valid rule id was provided for every non-schema-error case.

All three component rates are reported separately alongside the weighted score.

---

## 7. How `FLAGGED` Is Treated

Decision: `FLAGGED` is **reported separately and penalized by queue-burden metrics, but it is not treated as a false merge**.

That means:

- `FLAGGED` on a true `MERGE` case becomes a `soft_missed_merge`.
- `FLAGGED` on a true `SKIP` case becomes a `safe_flag`.
- `FLAGGED` never counts as a `false_merge`.

Reason:

- treating `FLAGGED` as neutral would let a contender dodge hard cases without cost
- treating `FLAGGED` as equivalent to `MERGE`/`SKIP` error would ignore the real difference between a reversible escalation and an irreversible bad merge

So v1 treats `FLAGGED` as a third operational outcome: safer than a wrong merge, but still costly because it inflates review queues and blocks automation.

---

## 8. Mandatory Report Shape

The first bakeoff readout must publish these tables for every contender, including the control:

1. **Overall summary**
   - `true_merge`, `true_skip`, `false_merge`, `hard_missed_merge`, `soft_missed_merge`, `safe_flag`, `survivor_error`
   - all primary rates
   - cost metrics
   - auditability metrics

2. **Core vs tail breakdown**
   - same metrics grouped by `source_pair_tier`
   - report `core` and `tail` separately even if the overall total looks fine

3. **Stratum breakdown**
   - `blind_core_audit`
   - `known_false_merge_patterns`
   - `known_missed_merge_patterns`
   - `tail_random_sample`

4. **Error ledger**
   - one row per `false_merge`, `hard_missed_merge`, `soft_missed_merge`, or `survivor_error`
   - includes `case_id`, `pair_id`, contender id, predicted output, expected verdict, and packet refs used

5. **Winner-selection table**
   - eligible / ineligible
   - gate failures if any
   - ranking tuple values

---

## 9. Hard Gates Before Queue-Building Is Allowed

Queue-building is blocked until there is:

- one production-eligible winner, and
- one fallback contender that also passes its gate set

The deterministic control can satisfy neither requirement.

### 9.1 Production-eligibility gates

A contender is production-eligible only if all of the following are true:

1. `false_merge = 0` across the full benchmark
2. `blind_core_audit` has:
   - `false_merge = 0`
   - `hard_missed_merge = 0`
   - `soft_missed_merge <= 1`
3. `known_false_merge_patterns` has:
   - `false_merge = 0`
   - `safe_flag <= 4`
4. `known_missed_merge_patterns` has:
   - `hard_missed_merge <= 2`
   - `soft_missed_merge <= 4`
5. `tail_random_sample` has:
   - `false_merge = 0`
   - `hard_missed_merge <= 2`
   - `flag_rate_total <= 0.25`
6. `survivor_accuracy >= 0.95`
7. `auditability_score >= 0.95`
8. `schema_valid_rate = 1.00`

Interpretation:

- We are intentionally harsh on false merges because the benchmark is compact and pattern-led.
- Core may tolerate at most one flagged merge case, but it may not silently skip a real core merge.
- Tail may be more conservative than core, but not so conservative that it becomes mostly a flag machine.

### 9.2 Fallback-eligibility gates

The fallback contender is allowed to be more conservative, but not unsafe.

A contender is fallback-eligible only if all of the following are true:

1. `false_merge = 0`
2. `blind_core_audit false_merge = 0`
3. `survivor_accuracy >= 0.90`
4. `auditability_score >= 0.95`
5. `flag_rate_total <= 0.35`

If no second contender passes this fallback gate set, queue-building stays blocked even if one production winner exists.

Reason: we want one backup path for outages, disagreement review, or cases where the production path is too aggressive or unavailable.

---

## 10. Winner Selection Once Gates Pass

Among contenders that pass the production gate set, choose the production winner by this exact ranking tuple, in order:

1. lowest `soft_missed_merge` on `blind_core_audit`
2. lowest `hard_missed_merge` overall
3. highest `survivor_accuracy`
4. highest `auditability_score`
5. lowest `flag_rate_total`
6. lowest `cost_per_pair`

Tie-breaking continues down the list until one contender wins.

The fallback contender is then:

- the highest-ranked remaining contender that passes the fallback gates, and
- if possible, from a different method class than the production winner

Method classes for this rule:

- deterministic control
- single-model adjudicator
- consensus adjudicator

If no contender passes the production gate set, the bakeoff ends with **no production path chosen** and the next session must open a v2 design for either:

- packet/retrieval changes, or
- a new contender lineup

No queue-building work starts in that failure state.

---

## 11. Production Path vs Fallback Path

### Production path

The production path is the contender that:

- passes the production gates,
- wins the ranking tuple above,
- becomes the default adjudicator for queue-building in the next phase

### Fallback path

The fallback path is the contender that:

- passes the fallback gates,
- remains available for disagreement review, outage handling, or spot validation,
- is not automatically fused into the production verdict rules unless a later session explicitly says so

This is important: the fallback path is a **backup adjudicator**, not a hidden second vote inside the production pipeline.

---

## 12. Implications For The Next Session

The next session should build only:

1. packet generation from `benchmark_v1` into `evidence_packet_v1`
2. the hidden-overlay strip that produces `packet_visible`
3. the adjudication harness that runs the frozen contender set and normalizes outputs into `run_result_row_v1`

The next session should **not** revisit:

- contender list
- `FLAGGED` semantics
- score formulas
- winner-selection rules
- production/fallback gate thresholds

Those are locked here unless the bakeoff fails and we deliberately open a `v2` design session.
