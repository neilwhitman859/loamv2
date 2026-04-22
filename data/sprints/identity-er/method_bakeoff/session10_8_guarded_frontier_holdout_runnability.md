# Session 10.8 - guarded frontier holdout runnability

- Date: 2026-04-21
- Candidate under confirmation: `hybrid_guarded_frontier_v1`
- Fresh confirmation slice: `session10_8_guarded_frontier_holdout_manifest_v1.json`
- Offline packet bundle: `session10_8_guarded_frontier_holdout_offline_bundle_v1.json`

## What Was Checked

The fresh 24-case holdout was re-checked against frozen local artifacts only.
No DB access, no network calls, and no new model calls were used.

The join looked for two levels of frozen evidence:

- verdict-level evidence snippets from `core_verdicts.jsonl`, `mid_verdicts.jsonl`, and `tail_verdicts.jsonl`
- structured side context (producer ids, wine counts, wine lists) from the local rechrome context files

## Coverage Result

- Holdout cases: `24`
- Cases with at least one frozen verdict-evidence snippet: `24 / 24`
- Cases with structured side context: `10 / 24`
- MERGE cases with structured side context: `10 / 10`
- SKIP cases with structured side context: `0 / 14`

## Honest Runnability Split

- Zero-cost offline audit runnable: `true`
- Independent fresh confirmation runnable: `false`
- Faithful fresh-holdout method rerun runnable: `false`

## Why The Split Matters

All 24 cases preserve enough frozen evidence to build blinded offline packets for a manual or inline audit.
That is useful, but it is not the same thing as an independent or faithful rerun of the guarded-frontier method.

The frozen evidence snippets are already post-adjudicated summaries taken from the Chrome-validation verdict files rather than raw packet-build retrieval traces.
So an offline audit can still test whether the current candidate remains logically aligned with the frozen source-of-truth, but it cannot honestly promote the method to "fresh-holdout independently confirmed."

The blocker is asymmetry in the preserved packet structure:

- all 10 positive holdout cases still have structured side context
- none of the 14 skip holdout cases still have structured side context

Because the current live risk is false merges on adjacent or shared-surname skip traps, losing the structured side context exactly on the negative set means a fully comparable rerun would over-credit the candidate if we treated this offline bundle as equivalent to the benchmark packets.

## Thin-Coverage Cases

`shared_surname_skip_core_3324`, `shared_surname_skip_core_18759`, `shared_surname_skip_core_20583`, `shared_surname_skip_core_62419`, `shared_surname_skip_mid_2546`, `shared_surname_skip_mid_2655`, `shared_surname_skip_mid_3323`, `shared_surname_skip_mid_14708`, `related_holdco_skip_core_14624`, `related_holdco_skip_core_94073`, `related_holdco_skip_core_122651`, `related_holdco_skip_mid_2507`, `related_holdco_skip_mid_30822`, `related_holdco_skip_mid_31289`

## Recommendation

Recommended wording now:

> `hybrid_guarded_frontier_v1` is benchmark-pass with repeated rerun confirmation; a zero-cost offline holdout audit is now runnable from frozen local artifacts, but that audit would be consistency-only, not independent confirmation, and a faithful fresh-holdout rerun is still blocked because the preserved offline packet structure is incomplete on all 14 negative holdout cases.

Next bounded step:

- if we want maximum rigor without unfreezing infrastructure, run a clearly labeled zero-cost offline audit on the blinded packets
- if we want a true fresh-holdout rerun claim, restore a working local packet-build/runtime path first
