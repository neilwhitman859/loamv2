# Session 14 Phase B — kickoff prompt

Paste this into the fresh session to pick up where Phase A ended.

---

Start Session 14 Phase B. Phase A is already committed as `a422e13`. Read the plan file first, then the Phase A handoff notes below, then start W5.

**Plan file:** `C:\Users\neilw\.claude\plans\wondrous-conjuring-whisper.md` — Phase B starts at W5 (line ~174). Read the "SESSION 14 — PHASE B" section end-to-end before doing anything.

**Dashboard:** Run `python -m pipeline.analyze.sprint_dashboard` first thing to confirm live state. Should show Session 14 in_progress, ~155,623 wines, $23.33/$175 budget.

## Phase A handoff notes

Phase A landed clean. Four workstreams done, all committed:

- **W1 dashboards** — `thirty_k_dashboard.py` renamed to `sprint_dashboard.py`, sprint-aware. `loam_roadmap.py` has a Current Sprint banner and live `METRIC_DISPATCH` instead of hardcoded `key_metrics`. Sprint state moved to `data/sprints/30k/{meta.json, sessions.json, budget.json, journal.md, status.md, prompts/}` with `data/sprints/current.json` as the pointer. `scripts/dash.ps1` launcher created. Budget file now at $23.33 (was $22.99 — added S13 $0.34). Sessions.json extended to include S11/S12/S13/S14.
- **W2 repo cleanup** — `archive_raw/` (gitignored) holds 2026-03 scrape dumps. `_gen_files.py`, `_gen_geo.py`, empty `lib/`, empty `tools/`, `docs/ROADMAP.md` all deleted. Readiness snapshots moved to `data/stats/archive/`. `.gitignore` extended. `git rm --cached` swept 17 tracked scratch files.
- **W3 backlog + tiny fixes** — 4 migrations applied (see below). BACKLOG.md rewritten. DECISIONS.md got 3 new entries. HISTORY.md got a "Closed: architecture changed" section.
- **W4 CLAUDE.md** — Cut 82 lines (525→443). Current State is now ~30 lines of live numbers. Pre-30K bullets moved to `docs/HISTORY.md` under "Pre-30K rebuild history".

**Tiny fixes that shipped (all four in S14 migration history):**
- #1 `SAUVIGNON GRIS` color red→white
- #2 6 phantom `appellation_grapes` rows deleted (Bardolino/Prosecco GARGANEGA, Pomerol CARMENERE, Bucelas/Colares/Carcavelos TOURIGA NACIONAL)
- #3 Duplicate MALVASIA row (VIVC 22968) merged into canonical (VIVC 15674). **Soft-deleted** — the `archive_wine_grapes` FK blocks hard delete. This is the pattern for any similar grape dedup work going forward.
- #4 `validate_post_dedup()` added to `pipeline/analyze/thirty_k_validate.py`. Run `python -m pipeline.analyze.thirty_k_validate --session s11`. Strict U2 count: **0** real dupes. The old `GROUP BY name_normalized` path kept as informational-only warn line.

**Known things Phase B should carry into its work:**
- `source_lwin.canonical_*` FKs still point at `archive_*` (the one staging table S13 didn't relink). Phase B W5 fixes this.
- `wine_detail_view` / `wine_vintage_detail_view` don't JOIN `wine_insights`. Phase B W5 adds the JOINs.
- Phase B W5 moves 45 `public.archive_*` tables to a new `archive` schema. The FK from `archive_wine_grapes` → `grapes` showed up during W3 fix #3 — expect similar cross-references during the schema move. Use `DROP CONSTRAINT IF EXISTS` where needed; don't touch archive table data.
- **P0 grape percentage repair (W6) has a USER REVIEW GATE.** Audit-first: build `pipeline/analyze/audit_grape_percentages.py` (read-only), output per-pattern breakdown (275% / 200% / 150% / etc.), **STOP and wait for the user to pick a strategy per pattern** before writing anything. Then dry-run, then wet run. Do NOT skip the gate.

**Budget:** $23.33 / $175 spent. Phase B should be near-zero AI cost (mostly SQL migrations). The grape percentage fix might use Haiku for LWIN re-derivation if the user picks that strategy — budget accordingly.

**Files to read before starting:**
1. `C:\Users\neilw\.claude\plans\wondrous-conjuring-whisper.md` — Phase B workstreams (W5-W8)
2. `data/sprints/30k/status.md` — current sprint state (pre-commit)
3. `docs/BACKLOG.md` — the active items list (updated in Phase A W3)
4. `docs/DECISIONS.md` — read the last 3 entries (sprint model, Grade C deprecation, one-session-two-phases)
5. `CLAUDE.md` "Current State" — the new concise snapshot, so you know what's live

**Do NOT:**
- Touch `archive_*` table DATA (only schema moves are in scope for W5)
- Skip the user review gate on W6 grape percentage repair
- Delete anything from `docs/BACKLOG.md` until the item is actually closed
- Rehash Phase A work — read the Phase A commit (`git show a422e13 --stat`) if you need the detail

**W8 — 30K sprint closure at the end of Phase B:**
- Re-run Josh Test: `python -m pipeline.analyze.josh_test` (target: still ≥85%)
- Update `data/sprints/30k/budget.json`: set `closed: true`, freeze total
- Update `data/sprints/30k/meta.json`: `status: closed`, `ended: 2026-04-11`
- Move `data/sprints/30k/` → `data/sprints/_archive/30k/`
- Update `data/sprints/current.json`: clear or point at the new sprint
- Append the final closure entry to the archived `journal.md`
- Also update the memory file: `C:\Users\neilw\.claude\projects\C--Users-neilw-Documents-GitHub-loamv2\memory\30k_status.md` should now say "CLOSED" and point at `data/sprints/_archive/30k/` for the archive, or be replaced entirely with a sprint-neutral pointer
- Final commit: `"30K sprint closed — X wines, Y producers, $Z spent. Session 14 interregnum complete, handing off to Reference-First sprint."`

Good luck. Work the plan; use the review gate at W6.
