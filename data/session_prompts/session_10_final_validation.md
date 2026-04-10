# Session 10 Prompt — Final Validation

Paste the below into a new session chat to start Session 10.

---

## Starting prompt

Session 10 of the 30K Plan — Final Validation.

Read `docs/30K_PLAN.md` (Session 11 / S11 validation checks), `data/stats/30k_dashboard.md`, `data/stats/loam_roadmap.md`, and `data/sessions.md` for context before starting.

**Session 9 just completed**: built the enrichment pipeline (`pipeline/enrich/batch_enrich.py`, `batch_api.py`, `batch_runner.py`), calibrated voice on 7 wines, then ran Anthropic Batch API sweeps. Final state: 105 Grade B, 4,857 Grade C, 140 D, 46,688 F. 4,962 wine_insights rows. $15.76 spent ($159 remaining in budget). Josh Test still 85%.

**Session 10 goal**: prove the 30K foundation is ready for Phase 4 (Frontend). Execute the S11 validation checks from the plan, plus verify enrichment quality at scale.

### Tasks (in order)

1. **Session briefing** — run `python -m pipeline.analyze.loam_roadmap` and `python -m pipeline.analyze.thirty_k_dashboard` to confirm current state. Report the numbers to me.

2. **Retry failed Grade C wines** (~$0.50, ~10 min)
   - ~168 wines errored during Session 9 Grade C sweep (mostly JSON parse failures from Haiku)
   - Run: `python -m pipeline.enrich.batch_runner --mode retry --grade C`
   - This submits a batch, polls for completion, processes results end-to-end
   - Verify error count dropped

3. **Save Josh Test results natively**
   - `pipeline/analyze/josh_test.py` currently prints but doesn't save to file
   - Add a `--save` flag that writes `data/stats/josh_test_latest.json` with the schema already expected by the dashboards (see existing file for format)
   - Run: `python -m pipeline.analyze.josh_test --save`

4. **WineTest with Story dimension** (~$0.60, ~30 min)
   - Run: `python -m pipeline.analyze.winetest`
   - Key metric: Story dimension. Was ~0/5 pre-enrichment. Should now be 3+/5 for enriched wines.
   - Save results, compare to the last WineTest run.

5. **Systematic enrichment quality audit** (~$3, ~45 min)
   - Build `pipeline/analyze/enrichment_audit.py` that:
     - Samples 50 random Grade C wines + 20 random Grade B wines
     - Sends each to Sonnet with the voice guide as a meta-prompt: "Rate this enrichment against these voice rules (1-5) and flag any issues"
     - Stores results in `data/stats/enrichment_audit.json`
   - Report any systematic issues found (generic filler, sommelier theater, factual errors, voice drift)

6. **S11 validation checks** (from `docs/30K_PLAN.md`)
   - S11.1 Josh Test find rate ≥ 85% ← likely passing
   - S11.2 Josh Test avg confirmation ≥ B
   - S11.3 Josh Test avg completeness ≥ 6/11
   - S11.4 Barcode spot-check: 100 random wines with UPCs, verify UPC lookup via `external_ids` returns correct wine (≥95%)
   - S11.5 Display names correct: 50 per major country (US, FR, IT, DE, AU), spot-check for artifacts
   - S11.6 No remaining duplicates: query `match_decisions` for unprocessed true_duplicate entries
   - S11.7 Provenance coverage: verify `data_provenance` entries for key fields
   - S11.8 Run `python -m pipeline.analyze.thirty_k_validate` one final time (should be 21/21 or better)
   - Document pass/fail in `data/stats/30k_s11_checks.md`

7. **Document the 85→95% push** (~30 min)
   - Which 39 wines from the Josh Test sample are missing?
   - Which sources would cover them (grocery brands, regional specialties, etc.)?
   - Estimated cost to close the gap
   - Save to `data/stats/push_to_95.md`

8. **Wrap-up**
   - Update `data/stats/30k_sessions.json` — mark Session 10 as done, record ai_spend
   - Update `data/stats/30k_budget.json` — add session 10 spend
   - Update `data/stats/loam_roadmap.json` — mark Phase 3 sub-tasks complete, flip Phase 3 status to "done" if appropriate, flag Phase 4 as "next"
   - Append Session 10 entry to `data/stats/30k_journal.md`
   - Update `data/sessions.md` (move to Done)
   - Regenerate dashboards: `python -m pipeline.analyze.thirty_k_dashboard` and `python -m pipeline.analyze.loam_roadmap --save`
   - Commit with a clear message summarizing S11 check results

### Exit criteria

- All S11 checks have a pass/fail status documented
- WineTest Story dimension measurably improved vs pre-enrichment baseline
- Enrichment audit run on 70 sample wines, any systematic issues flagged
- Dashboards and docs updated
- Committed and pushed

### Don't do

- Don't enrich more wines (save that for the post-Session 10 Grade C F-sweep)
- Don't touch the frontend (Phase 4 is its own session)
- Don't re-run the Grade C sweep — just the retry pool
- Don't cross into "should we launch?" — that's Phase 9

### Context budget

Expect: ~$5 AI ($0.50 retry + $0.60 WineTest + $3 audit + buffer)

### When you're done, start a new session to plan Phase 4 or the F-grade Grade C sweep. I'll be doing that separately.
