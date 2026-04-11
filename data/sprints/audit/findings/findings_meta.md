# S2.8 — Meta Audit Findings

**Sprint:** 2 (Audit)
**Session:** 8 of ~9 (S2.8)
**Expert hat:** Meta — docs / CLAUDE.md / memory / roadmap / sprint infra / scheduled tasks
**Method:** Opus 4.6 inline read-only static analysis + Supabase MCP `execute_sql` drift verification + `list_scheduled_tasks` + `list_edge_functions`
**Budget:** $0 actual (ratified Opus inline pattern per S2.3–S2.7)
**Scope:** All of `docs/*.md` (14 files + 2 in `docs/reference/`), empty `docs/architecture/` and `docs/pipelines/` dirs, `CLAUDE.md` (611 lines), every file under `memory/` (19 files), `data/stats/loam_roadmap.json` + `pipeline/analyze/loam_roadmap.py`, `data/sprints/current.json` + `data/sprints/audit/`, `data/sprints/_archive/30k/`, `data/sessions.md`, `data/stats/` contents, `data/session_prompts/` legacy dir, `scripts/dash.ps1`, scheduled tasks via MCP, edge function deployment state, and `docs/BACKLOG.md`.

---

## Summary

**32 findings. 9 P0, 14 P1, 7 P2, 2 P3.**

**The meta layer is the foundation the other seven audits stand on.** Every session starts by reading CLAUDE.md and memory, every sprint dashboard reads `loam_roadmap.json`, and every finding this sprint wrote is categorized against these same files. When they drift, the whole next-session loop inherits the drift. S2.8 finds the drift is already significant: CLAUDE.md's `## Current Focus` section is 5+ sessions behind, still asserting "Session 14 housekeeping interregnum" with "Sprint 2 = Reference-First Enrichment (planning session 15, execution sessions 16+)." That frame was superseded on 2026-04-11 the same day it was written. Every `SESSION BRIEFING` generated from CLAUDE.md pulls the wrong frame first.

**The three most important findings:**

1. **F1 — CLAUDE.md `## Current Focus` is 5+ sessions stale and internally contradicts `## Current State`.** Line 520: "Session 14 housekeeping interregnum … Sprint 2 = Reference-First Enrichment (planning session 15, execution sessions 16+)." Line 109–245 (Current State) correctly tracks Sprint 2 as the Audit with S2.1–S2.7 done. A session reading top-down gets Reference-First as the forward frame; a session reading the Current State section gets Audit. The two sections were written in different sessions and never reconciled. Same file, opposite guidance.

2. **F2 — `memory/30k_status.md` ships the stale Reference-First frame into every conversation via `MEMORY.md`.** Line 29: "Sprint 2: Reference-First Enrichment. Planning session = Session 15." MEMORY.md imports it as a live pointer. Every new conversation gets the briefing with this misdirection loaded before the user has typed anything. The S14 Phase A prompt (`s2_1_db_canonical.md` A9) instructed the *other* misleading memory file (`project_sprint_model_and_rf_direction.md`) to be rewritten — that one was fixed. `30k_status.md` was missed.

3. **F3 — `docs/SOURCES.md` mis-documents the external_ids Backbone ID storage** — line 33: "All three are stored in the `external_ids` table with `id_type` = `'ttb_cola'`, `'lwin'`, or `'upc'`." Live `information_schema.columns` query confirms (a) the column is `system`, not `id_type`; (b) the COLA value is `cola`, not `ttb_cola`; (c) `lwin_7` is a valid `system` value (50,908 rows) not mentioned. Any pipeline author writing a query from this doc as documented will get zero rows and silently ship a bug. The doc is the canonical primary source for Backbone IDs (CLAUDE.md:523 points at it explicitly).

**Cross-session pattern with S2.1 F28 and S2.7 F2:** Every hardcoded number in docs/memory drifts. S2.1 found ≥6 places where CLAUDE.md counts drifted from live DB. S2.7 found a column that didn't exist. S2.8 finds the internal contradictions AND one table's row count is wrong in CLAUDE.md's own `Major Gaps` block (`vineyards has 815 rows` vs live `0` rows in `public.vineyards`; archive has 881, not 815). S2.1/S2.7/S2.8 together argue for a **drift check** as a standard session wrap-up step.

**Cross-references:** Extends S2.1 F28 (CLAUDE.md hardcoded counts), S2.5 F1 (describe-chemical still deployed — verified via MCP this session), S2.6 F9 (VOICE.md older than the problem it tries to solve). Validates `memory/feedback_opus_inline_reasoning.md` (S2.8 at $0 budget).

**Net add to Sprint 3 backlog:** 23 pre-req items (9 P0 + 14 P1), most trivial-effort (doc patches, memory edits, dir cleanups). The full bundle is ~2–3 hours of focused doc hygiene. Notably, **4 of the 9 P0s are single-line fixes** (F1 Current Focus header, F2 30k_status.md pointer, F3 SOURCES.md backbone row, F4 CLAUDE.md vineyards contradiction). **No scope-breakers.**

**Running Sprint 2 totals: 245 findings** across S2.1–S2.8. Still **$0 spent / $25 ceiling**.

---

## P0 — Broken or incorrect, user-visible or correctness-critical

### F1 — [P0, trivial] CLAUDE.md `## Current Focus` is 5+ sessions stale and contradicts `## Current State`

**Where:** `CLAUDE.md:518–550` (`## Current Focus`) vs `CLAUDE.md:109–247` (`## Current State`).

**What:** Line 520 reads:

> **Session 14 housekeeping interregnum** — 30K Plan (Sprint 1) closes at end of Session 14 Phase B. Sprint 2 = Reference-First Enrichment (planning session 15, execution sessions 16+). See `data/stats/loam_roadmap.json` + `python -m pipeline.analyze.loam_roadmap` for the full phased plan.

Three independent inaccuracies in this one paragraph:

1. **"Session 14 housekeeping interregnum" is the current activity** — false. S1.14 closed 2026-04-11. S2.1–S2.7 have since run. This session is S2.8.
2. **"Sprint 2 = Reference-First Enrichment"** — false. Sprint 2 is the multi-expert Audit. Reference enrichment moved to Sprint 5. DECISIONS.md 2026-04-11 "Sprint model + Reference-First pivot" entry is what made this rename. `memory/project_quality_before_enrichment.md` is the authoritative sequence; `memory/project_sprint_model_and_rf_direction.md` was rewritten to say so; CLAUDE.md was missed.
3. **"planning session 15, execution sessions 16+"** — false. Sprint 2 sessions are named S2.1..S2.9, not 15..16+. The old 30K-style numbered session model was superseded by the sprint/session model (memory/workflow_sprint_session_naming.md).

**Cross-read:** The same file's `## Current State` section at lines 109–247 correctly tracks the Sprint 2 audit: "Sprint 2 (Audit) is active as of 2026-04-11. Multi-expert read-only audit … S2.1 … S2.2 … S2.7 … 213 findings". Internal contradiction. Two sections of one file shipping mutually exclusive forward frames.

**Why it matters:** CLAUDE.md auto-loads into every session's system prompt. The `SESSION BRIEFING` behavioral instruction at line 32 tells the model to give a "Last session … current state … next step" summary. A model reading top-down stops at `## Current Focus` line 520 and concludes next-step = plan Reference-First Session 15. A model reading the long `## Current State` block concludes next-step = continue Sprint 2 audit (S2.8 is what we're doing now). Both are grounded in the same file. The only reason this session isn't starting on the wrong foot is that the user typed "execute s2.8" explicitly. A less specific "/briefing" would have surfaced the wrong frame.

**Proposed fix:** Rewrite `## Current Focus` (~30 lines) to reflect Sprint 2 Audit status, listing S2.1–S2.7 findings totals, current session (S2.8), and pointing at `data/sprints/audit/status.md` as the live sprint plan. Collapse the pre-30K "Next Steps (cleaned 2026-04-03)" block (F10, below) into git history; it's referenced elsewhere here as a stale-block anchor. **Estimated effort: 30 minutes combined with F4, F10, F12.**

**Severity:** P0 because every session start depends on this being right. S2.1 F28 flagged hardcoded count drift; this is a worse class of drift — whole-paragraph forward-frame drift.

**Effort:** trivial (single-paragraph rewrite).

**Dependencies:** none. Strictly a doc patch. Sprint 3 pre-req.

---

### F2 — [P0, trivial] `memory/30k_status.md` tells future sessions to plan Sprint 2 as Reference-First

**Where:** `~/.claude/projects/C--Users-neilw-Documents-GitHub-loamv2/memory/30k_status.md:29`, referenced from `memory/MEMORY.md:8`.

**What:** The full block (lines 27–35):

> ## Next
>
> **Sprint 2: Reference-First Enrichment.** Planning session = Session 15. See `docs/DECISIONS.md` 2026-04-11 entry "Sprint model + Reference-First pivot" for rationale. See `data/sprints/_archive/30k/journal.md` Session 14 closure for the handoff summary.

Same staleness class as F1 but lands in a worse place — memory files auto-load into every conversation via the bolted-in `MEMORY.md` index. The S14 Phase A prompt (`s2_1_db_canonical.md` Phase A9) correctly instructed the *other* misleading memory file (`project_sprint_model_and_rf_direction.md`) to be rewritten and updated; that file was fixed (journal:15 confirms). `30k_status.md` slipped through because it was framed as a "historical pointer" — but its "Next" section still ships forward guidance.

**Cross-verify:** Read `memory/project_sprint_model_and_rf_direction.md:32`: "An earlier version of this memory claimed Sprint 2 = Reference-First Enrichment with Session 15 as a planning kickoff. That plan was superseded on 2026-04-11. Sprint 2 is now the multi-expert audit; reference redesign moved to Sprint 4; reference enrichment moved to Sprint 5." Good — that one was corrected. `30k_status.md` ships the exact inverse claim.

**Why it matters:** MEMORY.md is what the model reads *first*, before CLAUDE.md even. It is the canonical source-of-truth layer. An incorrect pointer here is a worse bug than an incorrect sentence in CLAUDE.md.

**Proposed fix:** Rewrite the `## Next` section to say Sprint 2 = Audit (active, S2.1–S2.8 done, 213 + this session's findings). Point at `data/sprints/audit/status.md` as live state. Or stop at "Sprint 2 is the next sprint, see `memory/project_quality_before_enrichment.md` for the authoritative sequence" — less content, same utility.

**Effort:** trivial.

**Dependencies:** none. Sprint 3 pre-req; before any session that reads memory.

---

### F3 — [P0, trivial] `docs/SOURCES.md` mis-documents the external_ids Backbone ID storage

**Where:** `docs/SOURCES.md:33`.

**What:** The Backbone IDs section says:

> All three are stored in the `external_ids` table with `id_type` = `'ttb_cola'`, `'lwin'`, or `'upc'`.

Verified via MCP `execute_sql`:

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='public' AND table_name='external_ids' ORDER BY ordinal_position;
```

Result columns include `system TEXT`, `external_id TEXT`, `entity_type TEXT`, `entity_id UUID` — **no `id_type` column**. And the live distribution:

```sql
SELECT system, count(*) FROM external_ids GROUP BY system ORDER BY count(*) DESC;
```

```
cola     253,301
lwin     119,889
lwin_7    50,908
upc       13,162
qr_url     1,400
qr           163
```

Three errors in one sentence:
1. Column name is `system`, not `id_type`.
2. COLA value is `'cola'`, not `'ttb_cola'`.
3. LWIN fine-wine + long-tail split (`lwin` and `lwin_7` as two distinct systems with 170,797 combined) is not documented.

**Why it matters:** `docs/SOURCES.md` is CLAUDE.md's Docs-consultation source-of-truth for external sources (line 17). A pipeline author reading SOURCES.md and writing `WHERE id_type = 'ttb_cola'` gets a column-does-not-exist error. A more charitable author writing `WHERE system = 'ttb_cola'` gets zero rows, silently. This is a **code footgun** at the documentation layer. The S2.7 F2 pattern in miniature (dead fetch from wrong column name, silent failure).

**Proposed fix:** Update line 33 to:
> All are stored in the `external_ids` table under `system = 'cola'`, `'lwin'`, `'lwin_7'`, or `'upc'`. (QR codes and QR URLs also live there as `'qr'` and `'qr_url'`.)

And add a one-line snippet showing the correct query: `SELECT * FROM external_ids WHERE system = 'cola' AND external_id = '22312001000001';`

**Effort:** trivial. 2-minute patch.

**Dependencies:** none.

---

### F4 — [P0, trivial] CLAUDE.md internal contradiction — vineyards count is 0, 815, or 881 depending on paragraph

**Where:** `CLAUDE.md:334` vs `CLAUDE.md:487`.

**What:** Line 334 (Aspirational section):
> `vineyards`, `water_bodies`, `wine_vintage_descriptors`, `wine_vintage_nv_components`, `producer_timeline`, etc. — canonical scaffolding preserved. The pre-30K "815 vineyards" lives in `archive_vineyards`; current public.vineyards is empty post-rebuild.

Line 487 (Major Gaps):
> ~38 canonical tables still at 0 rows (descriptors, wine_relationships, etc.) — **vineyards has 815 rows**, weather tables now fully populated.

Same file, opposite claims. Verified live:

```sql
SELECT count(*) FROM public.vineyards;     -- 0
SELECT count(*) FROM archive.vineyards;    -- 881  ← not 815
```

**Three nested errors here:**
1. Line 487 says vineyards has 815 rows in the canonical public table. False — 0 rows.
2. Both lines say "815 vineyards" in archive. False — 881 rows.
3. Line 487's "vineyards has 815 rows" blocks its own co-sentence about "38 canonical tables at 0 rows" — if vineyards is one of those 38, it should be at 0 in the same sentence.

The 815 number likely came from the original pre-30K vineyard build logged in memory; the archive accumulated 66 additional rows somewhere along the way (possibly the vineyard cron loop run 1 mentioned in `data/stats/cron_loop_journal.md`).

**Why it matters:** Vineyards is one of the empty-canonical tables frequently mentioned as a Sprint-4 redesign target. Inconsistent numbers on the same entity lose reader trust in the doc as a whole.

**Proposed fix:** Delete the "vineyards has 815 rows" clause from line 487 entirely (it's the wrong section). Update line 334 to "881 vineyards in `archive.vineyards`" or just "hundreds of vineyards in archive" and stop citing a specific number — or, even better, just delete the count and add a pointer `SELECT count(*) FROM archive.vineyards`. Sprint 3 pre-req.

**Effort:** trivial.

**Dependencies:** none.

---

### F5 — [P0, trivial] `loam_roadmap.json` Sprint 2 sub-tasks are all 7 sessions behind live state

**Where:** `data/stats/loam_roadmap.json:28–42`.

**What:** Phase 3 (Sprint 2 — Audit) in the roadmap JSON lists 9 sub-tasks. All marked `"status": "planned"` except S2.1 which is marked `"status": "in_progress"`. Live state per `data/sprints/audit/sessions.json`:

```
S2.1 → done (2026-04-11)
S2.2 → done (2026-04-11)
S2.3 → done (2026-04-11)
S2.4 → done (2026-04-11)
S2.5 → done (2026-04-11)
S2.6 → done (2026-04-11)
S2.7 → done (2026-04-11)
S2.8 → in_progress (this session)
S2.9 → not_started
```

The JSON is 7 session states behind. `pipeline/analyze/loam_roadmap.py` reads from this JSON at line 210 and renders Phase 3 sub-tasks with the stale statuses — so `python -m pipeline.analyze.loam_roadmap` currently shows "S2.1 in_progress" with S2.2–S2.9 as planned, even though the sprint dashboard (`scripts/dash.ps1`) correctly reads `data/sprints/audit/sessions.json` and shows S2.1–S2.7 done.

**Two dashboards, two different answers.** A user running `dash project` vs `dash` gets divergent pictures of Sprint 2 progress.

**Why it matters:** `loam_roadmap.json` is listed in CLAUDE.md's Docs section (line 19) as "Read at session start to know what phase we're in." A session briefing that reads the JSON concludes S2.1 is active and S2.2 is the next session. That's 7 sessions wrong.

**Proposed fix:** Write a simple sync script at session wrap-up that reads `data/sprints/<current>/sessions.json` and updates `loam_roadmap.json` Phase 3 sub-tasks. Or — simpler — update the sub_tasks list in `loam_roadmap.json` manually as part of every wrap. Or — simplest — delete the sub_tasks list from `loam_roadmap.json` Phase 3 and let the `dash.ps1` sprint view handle Sprint 2 session state (since it reads the live sessions.json directly). This session will fix it.

**Effort:** trivial.

**Dependencies:** none. Sprint 3 should add a `loam_roadmap.py` dispatcher that pulls sub-task state from the sprint JSONs instead of the roadmap JSON — eliminates the class of drift.

---

### F6 — [P0, trivial] `docs/30K_PLAN.md` is alive in docs/ root with "Session 4 DONE — GO for Batch 1" as its live status and 10 broken `data/sprints/30k/` path references

**Where:** `docs/30K_PLAN.md:9` (header status), lines 161, 164, 167, 183, 185–186, 191–193 (path refs).

**What:** Header:
> **Status:** Session 4 DONE — GO for Batch 1. Next: Session 5 (500 producers, 50-cap)

The 30K sprint is **closed** (2026-04-11, archived at `data/sprints/_archive/30k/`). "Session 4 DONE" is 10 sessions out of date. Also the file references `data/sprints/30k/` 10 times:

```
docs/30K_PLAN.md:161:All sprint-tracked state lives under `data/sprints/30k/`:
docs/30K_PLAN.md:164:`data/sprints/30k/status.md`
docs/30K_PLAN.md:167:`data/sprints/30k/prompts/30k_phase_N.md`
docs/30K_PLAN.md:183:1. Read the plan doc, status file (`data/sprints/30k/status.md`), and journal (`data/sprints/30k/journal.md`)
docs/30K_PLAN.md:185:... Check `data/sprints/30k/sessions.json`
docs/30K_PLAN.md:186:4. Mark current session as `"in_progress"` in `data/sprints/30k/sessions.json`
docs/30K_PLAN.md:191:3. Update `data/sprints/30k/sessions.json`
docs/30K_PLAN.md:192:4. Append entry to `data/sprints/30k/journal.md`
docs/30K_PLAN.md:193:5. Update `data/sprints/30k/status.md`
```

**All 10 paths are broken** — the 30K directory moved to `data/sprints/_archive/30k/` on S14 close. Anyone following the workflow instructions in 30K_PLAN.md tries to write to a non-existent dir.

**Also:** `CLAUDE.md:306` references `data/sprints/30k/journal.md` once too, with the same broken path.

**Why it matters:** 30K_PLAN.md is 815 lines of historical plan content that's still "live" in docs/ root. A session reading it for context gets a workflow that writes to the wrong paths. Same class as F1 and F2 — wrong forward frame in a top-level file.

**Proposed fix:** Two options:
- **(a) Archive it:** Move to `docs/reference/30K_PLAN.md` like `LWIN_STRATEGY.md` and `SCHEMA_ASSESSMENT.md`. Add a one-line header: "Archived. The 30K plan closed on 2026-04-11; see `data/sprints/_archive/30k/journal.md` for the session log." (recommended)
- **(b) Rewrite header + purge paths:** Change the header to "Closed 2026-04-11" and rewrite 10 paths to `data/sprints/_archive/30k/`. More work, same effect.

Also patch CLAUDE.md:306.

**Effort:** trivial (option a) or small (option b).

**Dependencies:** none.

---

### F7 — [P0, trivial] `docs/BACKLOG.md` is orphan from CLAUDE.md's Docs index while `sprint_dashboard.py:49` hardcodes it as a live input

**Where:** `CLAUDE.md:12–26` (Docs index — no BACKLOG.md entry), `pipeline/analyze/sprint_dashboard.py:49` (`BACKLOG_PATH = Path("docs/BACKLOG.md")`), `docs/BACKLOG.md` 134 lines.

**What:** Three issues in one file:

1. **Orphan from doc index.** CLAUDE.md's `## Docs — When to Consult Each` lists SCHEMA, PRINCIPLES, DECISIONS, VOICE, ENRICHMENT, SOURCES, loam_roadmap.json, current.json, MERGE_STRATEGY, WORKFLOW, reference/. BACKLOG.md is not mentioned. A session reading CLAUDE.md's doc index has no reason to consult BACKLOG.md and doesn't know it exists as a live file.

2. **Actively read by the sprint dashboard.** `sprint_dashboard.py:49` hardcodes `BACKLOG_PATH = Path("docs/BACKLOG.md")` as an input. The dashboard parses this file and renders top P0/P1 items next to the sprint summary. So it's a live input to an active tool, not a legacy file.

3. **Self-inconsistent workflow.** BACKLOG.md:129 says:
   > When completing an item, move its entry to `data/sessions.md` Done under the session that closed it, and delete from this file

   Current content shows **6 items** with "~~P0~~ **CLOSED in Session 14 Phase B W6/W7**" headers — they should have been deleted per the file's own rule, but weren't. Also has items like `[2026-04-11] Session 14 Phase A` and `[2026-04-11] Session 14 Phase B` that are closed sessions.

**Why it matters:** Any reader using the dashboard sees partially-stale backlog content next to live sprint state. And any reader using only CLAUDE.md has no idea BACKLOG.md exists. Split-brain state on the same set of issues.

**Proposed fix:**
- Add BACKLOG.md to CLAUDE.md's doc index with a one-line description (e.g., "Append-only list of discovered issues and deferred work. Actively read by `sprint_dashboard.py`. Prune closed items at session wrap-up.").
- Prune the 6 CLOSED items now. Delete the "Session 14 Phase A" and "Session 14 Phase B" entries.
- Long term: Sprint 3 should evaluate whether BACKLOG.md serves any purpose that `findings_*.md` + sprint sessions don't already cover, and possibly retire it. Parallel finding in the Sprint 3 pre-req bundle.

**Effort:** trivial.

**Dependencies:** none.

---

### F8 — [P0, trivial] `docs/AUDIT_2026-04-01.md` is a superseded audit still in docs/ root competing with Sprint 2 findings

**Where:** `docs/AUDIT_2026-04-01.md` (180 lines, 2026-04-01 dated).

**What:** A one-off canonical DB audit from 2026-04-01 with "The canonical database is clean but thin" as its headline. Since then, S2.1 (2026-04-11) produced 34 DB canonical findings contradicting this conclusion (≥3,534 wines in 1,686 true-dup groups, 919 grape synonym collisions, 0 producer metadata, etc.). The April 1 audit concluded "Data integrity: PERFECT — zero referential integrity violations across the entire canonical layer." S2.1 audit found true dup clusters, dangling pointers, and staging-FK violations that contradict that conclusion.

Same class as 30K_PLAN.md (F6) — superseded content that hasn't been archived.

**Why it matters:** A reader stumbling into `docs/AUDIT_2026-04-01.md` may conclude the canonical layer is "perfect" and skip the Sprint 2 audit findings. And it sits alphabetically above BACKLOG.md in any `ls docs/`, increasing visibility.

**Proposed fix:** Move to `docs/reference/AUDIT_2026-04-01.md` alongside `LWIN_STRATEGY.md`. Add a one-line header: "Superseded by Sprint 2 audit findings at `data/sprints/audit/findings/`."

**Effort:** trivial.

**Dependencies:** none.

---

### F9 — [P0, trivial] `docs/architecture/` and `docs/pipelines/` are empty directories scaffolded 2026-03-05 and never populated

**Where:** `docs/architecture/`, `docs/pipelines/`.

**What:** Confirmed via `ls -la`:

```
docs/architecture/:
total 8
drwxr-xr-x 1 neilw 197610 0 Mar  5 19:56 .

docs/pipelines/:
total 4
drwxr-xr-x 1 neilw 197610 0 Mar  5 19:56 .
```

Both are empty. Created 2026-03-05 (36 days ago). Likely a scaffold from an intended doc-restructure that never landed — around the time of the 2026-03-12 "Consolidate docs into 7-file workflow system" commit (docs/VOICE.md / WORKFLOW.md last-modified). The actual consolidation landed in the file-based 7-file system (PRINCIPLES/VOICE/WORKFLOW/SCHEMA/DECISIONS/ENRICHMENT + CLAUDE.md), not a per-dir breakdown.

**Why it matters:** Empty dirs in a docs tree read as "work in progress / something coming here" and create maintenance cognitive load. Also they get checked into git.

**Proposed fix:** `git rm -r docs/architecture/ docs/pipelines/` (empty dirs can be rm'd directly).

**Effort:** trivial.

**Dependencies:** none.

---

## P1 — Significant gap or risk, must fix

### F10 — [P1, small] CLAUDE.md "Next Steps (cleaned 2026-04-03)" block is pre-30K and references 8 dead artifacts

**Where:** `CLAUDE.md:530–550`.

**What:** The `### Next Steps (cleaned 2026-04-03)` block was written pre-30K-rebuild. Contents:

- "Active: Data merge paused — see 'Next steps (resume here)' in merge infrastructure section"
- "Upcoming: Link Phase B wines back to TTB (6,767 new wines need canonical_wine_id backlinks)"
- "Upcoming: Enrichment pipeline MVP (Edge Function + prompts) — next major phase"
- "Upcoming: COLA-keyed deterministic merge (PRO/TABC/WV/Kansas/barcode → shared COLA numbers, pure SQL)"
- "Upcoming: Importer catalog merge (10K wines against TTB+LWIN backbone)"
- "Upcoming: TTB COLA Phase 3 AI parse (Haiku on 1.35M non-001 fanciful names, ~$10) — lower priority"
- "Upcoming: Remaining importer scrapers (Kysela, Louis/Dressner, Broadbent)"
- "Upcoming: Frontend resume — after canonical tables have real depth"

Every "Phase B wine" reference points at a sub-stage of the pre-30K pipeline that no longer exists (30K rebuild archived those wines). The "6,767 new wines" line is meaningless in the current corpus. "Enrichment pipeline MVP" already shipped and has a feature flag. "Frontend resume" is paused per a separate memory file. This section is a point-in-time snapshot that never got updated when the 30K sprint ran.

**Why it matters:** Same class as F1 — wrong forward frame at the top level. A session reading "Next Steps" gets pre-30K directives.

**Proposed fix:** Delete the entire `### Next Steps (cleaned 2026-04-03)` section. Point at `data/sprints/audit/status.md` and `data/sprints/audit/sessions.json` as the live next-step source. Sprint state is the forward frame now, not a text block in CLAUDE.md.

**Effort:** small (requires deciding what (if anything) to preserve from the block).

**Dependencies:** F1 (same section rewrite).

---

### F11 — [P1, small] `docs/MERGE_STRATEGY.md` still frames the project as "Python migration decision TBD" with Ollama local matching and references 3 non-existent files

**Where:** `docs/MERGE_STRATEGY.md` (258 lines, 2026-03-19 dated).

**What:** Four stale claims:

1. **Lines 5, 156:** Frames the Python migration as a pending decision:
   > **Decision:** All new data pipeline work is written in Python.

   This was a 2026-03-19 decision. The migration happened. 265 Python files now under `pipeline/` (per S2.5 F1 finding). MERGE_STRATEGY.md still reads as a design doc proposing the migration.

2. **Lines 116–131:** The "AI-Assisted Matching" section says:
   > **Decision:** Use Ollama with a small local model (Llama 3.1 8B or Mistral 7B) for bulk matching to eliminate API costs.

   Ollama local matching never materialized. The project standardized on cloud Haiku/Sonnet/Opus for AI work (confirmed by `memory/feedback_opus_inline_reasoning.md` — default is Opus inline, not local).

3. **Lines 17, 150–154:** References three files as "to build":
   - "Merge engine (build fresh — JS version untested)" — still untested; no Python merge engine lives in `pipeline/lib/merge.py` per S2.5 review
   - "`sync_project_context.py` — to build" — does not exist (`Glob **/sync_project_context.py` returns nothing)
   - Also "`lib/merge.mjs`" — Node archive, not in `pipeline/`

4. **Line 246:** "Pattern: 20 min in Claude.ai before starting new Claude Code work" — obsolete workflow; CLAUDE.md current behavioral instruction is "do the work inline in the Claude Code session, not in a separate Claude.ai conversation."

**Why it matters:** MERGE_STRATEGY.md is in CLAUDE.md's doc index at line 21: "Read before building merge/matching infrastructure." Any session actually doing that would inherit obsolete framing and chase non-existent files. Sprint 3 will touch the merge layer; Sprint 3 planning must not read this doc as gospel.

**Proposed fix:** Rewrite MERGE_STRATEGY.md as a retrospective — "What we decided and what actually happened." Keep the wine identity definition and the "more data when it helps vs hurts" section (those are still valid). Drop the Ollama section entirely. Mark the sync_project_context.py and merge engine bullets as "abandoned — see `pipeline/lib/` for current merge code." Or move to `docs/reference/` with a header saying it's historical.

**Effort:** small (1–2 hours, or 15 min if we just move it to reference/).

**Dependencies:** none.

---

### F12 — [P1, small] `docs/ENRICHMENT.md` is from 2026-03-16 and contradicts S2.6/S2.7 findings (flag-off status, VOICE.md compliance, Grade C deprecation)

**Where:** `docs/ENRICHMENT.md` (290 lines, 2026-03-16 base, 2026-04-03 last-modified for a wrap-up update).

**What:** Four contradictions with current reality:

1. **Line 152 "Deployment Status":** "MVP deployed 2026-04-04. Edge Function `enrich-wine` is live on Supabase. On-demand B-grade enrichment tested on 2 wines. Voice/prompt refinement needed before batch spend >$16."

   Reality: `enrich-wine` is deployed but **behind the `ENRICHMENT_ENABLED=false` feature flag** (confirmed CLAUDE.md:325 and memory/feedback_opus_inline_reasoning.md via S2.6 F1–F3). The "Voice/prompt refinement needed" caveat is still unresolved (S2.6 F1, F2, F3 all flag this).

2. **Lines 45–84 (Grade C and Grade B sections):** Describes an active batch pre-warming path ("30K Grade C wines ~$120 total"). Per `DECISIONS.md` 2026-04-11 ("Grade C enrichment deprecated under Reference-First architecture") and `memory/project_sprint_model_and_rf_direction.md:18` ("Grade C voice regression DEPRECATED — do not fix"), Grade C wine-level enrichment is now deprecated. ENRICHMENT.md still documents it as a forward path.

3. **Line 222–230 "Prompt Design Principles":** "All enrichment prompts must follow `docs/VOICE.md`." S2.6 F1/F2 proved this is aspirational — only `pipeline/enrich/enrich_prompts.py` enforces the strict rules; the 4 reference-layer scripts and the live `enrich-wine` edge function ship weaker prompts. The doc says "must follow" as if it's enforced at the code layer; it isn't.

4. **Line 110–120 Cost Model table:** "B count grows with user demand. At early scale (hundreds of users), cost is negligible. Budget cap exists as safety net but not expected to be a constraint at launch." This framing is stale relative to Sprint 5 scope (S2.6 F5 found contamination in reference tables that must regen first; Sprint 5 cost re-estimation pending).

**Why it matters:** ENRICHMENT.md is in CLAUDE.md's doc index at line 18: "Read before building or modifying the enrichment pipeline." Sprint 3 will touch this directly (pre-req voice module consolidation). A Sprint 3 session reading this doc plans against a frame that no longer holds.

**Proposed fix:** Add a "Status (2026-04-11)" section at the top documenting: (1) the feature flag is off; (2) Grade C is deprecated; (3) Sprint 5 scope is TBD after Sprint 4 design. Either keep the rest of the doc as a reference for the original intent with a top banner, or rewrite the Cost Model and Grade C sections. **My recommendation:** banner + minimal delete.

**Effort:** small (1 hour).

**Dependencies:** none.

---

### F13 — [P1, trivial] `docs/SOURCES.md` "Last updated: 2026-03-25" is 17 days stale and misses 3 post-date events

**Where:** `docs/SOURCES.md:5`.

**What:** Header says "Last updated: 2026-03-25". Post-date events not reflected:

1. **TABC refresh 2026-04-03** — CLAUDE.md line 347 notes "Refreshed 2026-04-03 (201K API records → 183K unique TTB after dedup, no net new)." SOURCES.md doesn't mention this.
2. **Knowledge seed pipeline 2026-04-07** — 920 wines generated, 200 promoted (DECISIONS.md 2026-04-07). Not in SOURCES.md.
3. **Barcode scan completion 2026-04-06** — 106K UPCs promoted (CLAUDE.md line 571). SOURCES.md only shows partial barcode scrape progress.

Also references `docs/SOURCES.md` as the "master reference" in CLAUDE.md:17 — but point-in-time snapshot, not live state.

**Why it matters:** SOURCES.md is cross-referenced by S2.5 (code audit), S2.6 (voice audit), and every future merge session. Staleness here compounds.

**Proposed fix:** Update header to 2026-04-11, add the 3 missed events, and add a "See `CLAUDE.md` `## Current State` for live numbers; this file is a narrative reference, not a live source count" disclaimer.

**Effort:** trivial.

**Dependencies:** none.

---

### F14 — [P1, small] `docs/VOICE.md` (2026-03-12) is older than the problem S2.6 found — the doc ships voice rules it doesn't enforce

**Where:** `docs/VOICE.md` (130 lines, 2026-03-12).

**What:** VOICE.md is the source-of-truth for voice rules in Loam. It's been essentially unchanged since 2026-03-12. Since then, S2.6 discovered:

- **Reference-layer prompts don't implement the rules** — 4 scripts + the live edge function ship weaker preambles (S2.6 F1/F2).
- **Factual confabulation isn't prevented by voice rules alone** — tightened prompts still produce invented grapes, invented history, invented geology (S2.6 F3).
- **5 invented-fact hooks on Grade C samples** all followed VOICE.md style but were factually wrong.

VOICE.md as-written treats "state what you know directly" as a style rule, without addressing what happens when the model *doesn't* know. There's no factual-accuracy gate specified; S2.6's L3 fact-check recommendation is absent from VOICE.md.

**Why it matters:** VOICE.md is in CLAUDE.md's doc index at line 16 as the source-of-truth for voice content. A Sprint 5 author reading VOICE.md as the contract for enrichment content has no awareness of the L3 fact-check gate requirement. The doc itself needs to grow a "Fact-checking is upstream of voice" section.

**Proposed fix:** Add a `## Never Invent` section that codifies S2.6 F3's finding: when the facts packet doesn't contain a datum, the prompt must return `null` for that field rather than prose from training knowledge. Cross-reference the L3 fact-check gate Sprint 3 pre-req. Keep the Be Specific / Connect Place To Taste sections but frame them as "how to voice facts you have" not "what to say about wines."

**Effort:** small (needs care — VOICE.md is foundational).

**Dependencies:** S2.6 F1–F3 findings, but S2.8 flags the doc-level gap.

---

### F15 — [P1, trivial] `docs/PATH_A_ROLLBACK.md` is 719 lines of unused rollback SQL in docs/ root

**Where:** `docs/PATH_A_ROLLBACK.md`.

**What:** Per-migration rollback SQL for Path A appellation-rules seeding (2026-04-05 era). Never needed — no Path A rollback has been executed. Sits in docs/ root as the largest undiscussed file.

**Why it matters:** docs/ root is meant for currently-useful reference material. 719 lines of rollback SQL is operational archive — belongs in `docs/reference/` alongside LWIN_STRATEGY.md and SCHEMA_ASSESSMENT.md, or in a dedicated `docs/rollbacks/` dir, or simply deleted (if a Path A rollback becomes necessary, the SQL can be regenerated from `list_migrations`).

**Proposed fix:** Move to `docs/reference/PATH_A_ROLLBACK.md`. Add a one-line header "Archived. No Path A rollback has been executed. Regenerate from `list_migrations` if needed."

**Effort:** trivial.

**Dependencies:** none.

---

### F16 — [P1, trivial] `docs/IDENTITY_RULES.md` is Session 2 design spec in active docs/ tree; header presumes the 30K-session model

**Where:** `docs/IDENTITY_RULES.md:3`, 777 lines total.

**What:** Header:
> Design spec for wine identity, display names, cuvée extraction, and data quality rules. Created Session 2 (2026-04-08). Every subsequent session follows this document.

The content is mostly still valid — the wine-identity tuple model, NV convention, display name computation rules — and some of it is actively enforced by `batch_pipeline.py` and the resolvers. But the header presumes the 30K-session-numbering world. "Session 2" maps to S1.2 (Sprint 1 Session 2) in the current sprint model, which is confusing.

Also: CLAUDE.md's doc index does **not** list `docs/IDENTITY_RULES.md`. So it's simultaneously (a) in-scope for any session doing wine identity work and (b) invisible to CLAUDE.md's doc-consultation index.

**Why it matters:** Wine identity is core to Sprint 3's grape-repair compound (pre-req F2 / F7 in S2.4). The design spec for how identity is supposed to work is not in the doc index the session will read.

**Proposed fix:** (1) Add IDENTITY_RULES.md to CLAUDE.md's Docs index with a one-line description. (2) Rewrite the header to drop "Session 2" framing — replace with a simple "Authored 2026-04-08 during 30K Plan." (3) Consider moving to `docs/reference/IDENTITY_RULES.md` if the rules are enforced at the code layer and the doc is purely narrative — but verify first.

**Effort:** trivial (add to index) or small (full review + move).

**Dependencies:** none.

---

### F17 — [P1, small] `docs/DECISIONS.md` is 1,559 lines / 271 dated entries with no archive strategy; superseded decisions live alongside current guidance

**Where:** `docs/DECISIONS.md` (1,559 lines).

**What:** 271 dated decision entries, append-only, no pruning. File contains:

- Josh Test v1 (2026-04-08) → v2 (2026-04-09) → "directional metric, not product quality" (2026-04-09) — three sequential decisions on the same topic, none marked SUPERSEDED.
- Grade C enrichment deprecation (2026-04-11) alongside a 2026-04-09 entry saying "Defer Grade C Haiku enrichment until after Batch 2/3".
- Multiple 2026-03-03 schema decisions that were later reversed in schema hardening rounds.
- The 2026-03-19 merge strategy decision alongside the 2026-04-11 Opus inline reasoning decision (which reverses the earlier "use Haiku/Sonnet for audits" pattern).

CLAUDE.md says (line 15): "Read when you need to understand why something was done a certain way. Never re-litigate settled decisions without the user raising it." But "settled" is impossible to determine from the file itself — there's no SUPERSEDED tag, no archive section, no index.

**Why it matters:** A reader looking for "how does Josh Test work" gets three contradictory decision entries and no indication which is current. Readers who are skeptical (S2.1 principle 12 Claude taking agency) will want to re-verify before acting; readers who defer to the first hit get stale guidance.

**Proposed fix:** Add a `## Current Guidance` section at the top listing the *current* decisions on each recurring topic (Josh Test = directional, Grade C = deprecated, audit AI = Opus inline, etc.). Or: add a `SUPERSEDED BY [date]` line to entries that were reversed by later ones. Don't delete anything; preserve the append-only nature. Just add forward pointers.

Alternatively: split DECISIONS.md into `DECISIONS_CURRENT.md` (~200 lines) and `DECISIONS_ARCHIVE.md` (1,300+ lines). Current stays in docs/ root, archive lives in `docs/reference/`.

**Effort:** small (1–2 hours, judgment-heavy).

**Dependencies:** none.

---

### F18 — [P1, small] `data/sessions.md` is a 69-line file with individual session entries of 8K+ characters per line — read-cost is high, search is impossible

**Where:** `data/sessions.md`.

**What:** Session entries are single-paragraph blobs, each containing the full finding summary inline. Measured via `awk '{print length, NR}'`:

```
8,830 chars  line 13  (S2.7 entry)
7,917 chars  line 11  (S2.7 intro — wait, reread)
6,364 chars  line 15  (S2.6 entry)
6,087 chars  line 19  (S2.5)
5,807 chars  line 17  (S2.4/S2.5)
```

The top 10 lines are all 2,800+ characters. A single file read pulls the full S2.1–S2.7 content (~45K chars / ~11K tokens) into context every time the whiteboard is consulted.

**Why it matters:** The file's stated purpose (per its header: "Active and recent sessions. Read at session start, append when starting/finishing work") is defeated by its current structure. The TodoWrite system has replaced most of its in-session utility. It's ballooning without pruning.

**Proposed fix:** Two options:
- **(a) Prune on sprint close.** Move all Sprint 1 session entries into `data/sprints/_archive/30k/journal.md` (they're already there). Delete from sessions.md. Repeat for Sprint 2 when it closes.
- **(b) One-line format.** Change the sessions.md convention to one line per session: `- S2.7 2026-04-11 ux audit · 32 findings · $0 → findings_ux.md`. Let the findings file carry the detail. Saves ~95% of the current bytes.

Recommend (a)+(b) together: prune closed sprints on close, keep only current-sprint entries, use the one-line format going forward.

**Effort:** small (cleanup) + trivial (convention change).

**Dependencies:** none.

---

### F19 — [P1, trivial] `memory/vivino-pipeline.md` has no frontmatter, breaks the memory-file format convention

**Where:** `memory/vivino-pipeline.md` (59 lines).

**What:** Every other memory file starts with:

```markdown
---
name: ...
description: ...
type: project | feedback | reference | user
---
```

`vivino-pipeline.md` starts with:

```markdown
# Vivino Pipeline Results

## Phase 1: Mega Fetch ...
```

No frontmatter. No type tag. 32 days old per the auto-reminder (age shown in the system-reminder when read).

**Why it matters:** CLAUDE.md's "How to save memories" section codifies the frontmatter format. One file out of 18 breaking the convention is the kind of small inconsistency that degrades search and categorization.

**Also:** the content describes the pre-30K Vivino scraping pipeline. It's valuable historical reference (describes `xwines_*` tables' origin), but the content pre-dates the 30K rebuild and is essentially frozen archival data.

**Proposed fix:** Add frontmatter:

```markdown
---
name: Vivino pipeline results (archived)
description: Pre-30K Vivino scraping pipeline. Source of xwines_* tables. Archival reference — data never reintegrated.
type: reference
---
```

Alternatively, move the content into `memory/` if it's actively useful or delete if not. Content is valuable enough to keep as reference.

**Effort:** trivial.

**Dependencies:** none.

---

### F20 — [P1, trivial] `memory/product-architecture.md` uses "Tier 0-3" nomenclature that was replaced by F/D/C/B/A

**Where:** `memory/product-architecture.md:17`.

**What:**
> **Enrichment:** Four-tier model (Tier 0-3) with on-demand lazy enrichment. See `docs/ENRICHMENT.md`. Key insight: enrichment pipeline architecture should be designed before scaling imports.

The current letter-grade model (F / D / C / B / A) is documented in `docs/ENRICHMENT.md` and used everywhere in `CLAUDE.md`, pipeline code, schema (`wines.data_grade` text field with F/D/C/B/A values), and the S2.6 and S2.7 audits. The "Tier 0-3" framing is from 2026-03-16 (26 days old per auto-reminder) and was superseded before the 30K Plan even began.

**Why it matters:** Memory files auto-load. A reader doing product work and seeing "Tier 0-3" in their system prompt gets vocabulary that doesn't match any code or doc. "Wine not found: Option D — Claude Vision reads label, creates Tier 0 + immediate Tier 1 enrichment" (line 19) is confusing when nothing in the pipeline uses Tier 0/1.

**Proposed fix:** Update line 17 to match docs/ENRICHMENT.md: "Letter-grade model (F/D/C/B/A) with on-demand lazy enrichment." Update line 19's "Option D" line similarly. Optionally rename the whole file or age-flag it.

**Effort:** trivial.

**Dependencies:** none.

---

### F21 — [P1, small] `memory/workflow_session_tips.md` is 8 days old with a "CLAUDE.md Hygiene (flagged 2026-04-03)" section whose cleanup already happened, and gives pre-sprint-model workflow advice

**Where:** `memory/workflow_session_tips.md` (27 lines).

**What:** Three stale pieces of guidance in one file:

1. **Line 14:**
   > CLAUDE.md Hygiene (flagged 2026-04-03)
   > CLAUDE.md is ~800 lines and growing. It's doing too many jobs: context doc, changelog, task list, reference manual. This slows every session start.
   >
   > Pending cleanup (do between sessions):
   > - Trim to ~200 lines of essential context
   > - Move historical detail (TTB scraping saga, 50-state survey, import play-by-play) to `docs/HISTORY.md`
   > - Keep Next Steps to 5-10 active items, not 37 with 30 struck through

   The cleanup landed in S14 Phase A. Current CLAUDE.md is 611 lines (not 800, not 200), and historical detail did move to `docs/HISTORY.md`. The "pending" framing is wrong.

2. **Lines 23–24:**
   > No Dedicated Planning Session Needed
   > CLAUDE.md "Next Steps" section IS the task queue. Don't maintain a separate planning session. Just keep Next Steps clean and actionable. Any session can read it and know what to do.

   This is from the 30K-Plan-session-based world. The sprint model (`data/sprints/<name>/sessions.json`) replaced "Next Steps" as the task queue. Per F10 above, "Next Steps (cleaned 2026-04-03)" in CLAUDE.md is pre-30K stale anyway.

3. **Lines 25–28:**
   > Planning Pass Instead
   > Before a work session, spend 5 minutes updating Next Steps (or ask Claude to triage at session start via "briefing"). The behavioral instruction already exists — make it do more work.

   Same issue — pre-sprint-model workflow advice being loaded as guidance into new conversations.

**Why it matters:** Auto-loads into every conversation. A model that reads this memory gets instructions that contradict the actual sprint model.

**Proposed fix:** Rewrite the file to remove the CLAUDE.md Hygiene section (outdated) and the Next-Steps-is-the-queue sections. Keep only the "One Session, One Goal" advice (evergreen, still valid). Or delete outright — "one session one goal" is already in project principles.

**Effort:** trivial.

**Dependencies:** F10 (same narrative).

---

### F22 — [P1, trivial] `memory/project_sprint_model_and_rf_direction.md` — filename still contains "rf_direction" even after the content was rewritten to drop Reference-First-as-Sprint-2

**Where:** `memory/project_sprint_model_and_rf_direction.md`.

**What:** Per the S2.1 prompt Phase A9 and journal line 15, the content of this file was rewritten during S2.1 to remove the Reference-First-as-Sprint-2 claim. The content now correctly points at `project_quality_before_enrichment.md` as the authoritative sprint sequence. But the **filename** still contains "rf_direction" (an abbreviation for Reference-First direction). A reader searching memory for sprint-model guidance sees a filename that suggests it's about RF — contradicts content.

`MEMORY.md:5` links to it via relative path: `[Sprint model + dashboards](project_sprint_model_and_rf_direction.md)`. Link text says "Sprint model + dashboards" (which matches content) but the filename still reads RF-aligned.

**Why it matters:** Minor, but a search like `grep -l "reference" memory/` matches this file by name even though content is about sprint structure. Confusing.

**Proposed fix:** Rename to `project_sprint_model_and_dashboards.md` (matches the link text in MEMORY.md). Update MEMORY.md link target.

**Effort:** trivial (`git mv` + 1 line MEMORY.md edit).

**Dependencies:** none.

---

### F23 — [P1, trivial] `CLAUDE.md` drift in hardcoded numbers — 4 entries contradict live DB, 1 contradicts itself

**Where:** Multiple — primarily `CLAUDE.md:309`, `311`, `314`, `318`, `334`, `487`.

**What:** Cross-checked via live MCP `execute_sql`:

| CLAUDE.md claim | Live DB | Delta |
|---|---|---|
| Wine_grapes: **47,035** (line 309) | 46,028 | −1,007 (2.1%) |
| Color: 153,**311** (line 314) | 153,229 | −82 |
| LWIN: 170,**797** (line 318) | 170,797 | ✓ match |
| COLA: 253,**301** (line 319) | 253,301 | ✓ match |
| public.vineyards **0** rows (line 334) | 0 | ✓ match |
| **vineyards has 815 rows** (line 487) | 0 (public) / 881 (archive) | **contradicts itself + live** |
| archive vineyards **815** (line 334) | 881 | −66 |
| wine_food_pairings 0 (line 174 S2.6 F8) | 0 | ✓ match |
| grape_insights 0 (line 175) | 0 | ✓ match |
| appellation_insights 82 (line 331) | 82 | ✓ match |

Most numbers are correct (S14 Phase A cleanup helped); the residual drift is 3 items (wine_grapes, color, archive vineyards) plus the internal contradiction (public.vineyards 815 vs 0) from F4.

**Why it matters:** S2.1 F28 explicitly flagged "Trust the DB, not the docs" and noted ≥6 places where hardcoded counts had drifted. S2.8 finds the drift is down to ≤4 places after S14 cleanup, but it hasn't been eliminated. The fix is to delete the specific numbers entirely and substitute queries.

**Proposed fix:** Replace all hardcoded counts in CLAUDE.md with:
- Either a single `**Live numbers:** run \`python -m pipeline.analyze.sprint_dashboard\` or query DB.`
- Or a section header saying `**Corpus scale (query DB for live):**` followed by field names without numbers.

The line 305 disclaimer already exists: "Run `python -m pipeline.analyze.sprint_dashboard` or query the DB for live numbers. These are point-in-time snapshots." But the numbers below it still drift. Strip them.

**Effort:** trivial.

**Dependencies:** F4, F10.

---

## P2 — Improvement, not blocking

### F24 — [P2, trivial] `loam_roadmap.json` has no `metrics_query` dispatcher for Sprint 2 (Audit phase)

**Where:** `data/stats/loam_roadmap.json:28–42` (Phase 3), `pipeline/analyze/loam_roadmap.py:188` (METRIC_DISPATCH).

**What:** Phase 3 has no `metrics_query` field. METRIC_DISPATCH only defines `foundation`, `data_population`, `enrichment`. A Sprint 2 dispatcher doesn't exist. So the Phase 3 header in the roadmap output shows sub-tasks (stale per F5) and phase notes, but no live metrics (finding counts, sessions done).

**Why it matters:** Sprint 2 is an audit sprint — its metrics are "findings so far" and "sessions complete," not data-population or enrichment counters. A dispatcher that reads `data/sprints/audit/sessions.json` and sums findings from each `findings_*.md` file would give a live "213 findings across 7 sessions done, $0 spent / $25 ceiling" line under Phase 3. ~20 lines of Python.

**Proposed fix:** Add `_metrics_audit(m: dict)` to METRIC_DISPATCH and set `"metrics_query": "audit"` on Phase 3 in the JSON. Function reads the sprint dir, counts sessions done, grepss `P[0-3]` in finding files, returns two short strings.

**Effort:** small.

**Dependencies:** none.

---

### F25 — [P2, trivial] `loam_roadmap.json` Phase 10 "Quality & Maintenance" only lists 1 of 4 paused scheduled tasks

**Where:** `data/stats/loam_roadmap.json:130–134`, `data-accuracy-agent` mentioned; 3 other paused tasks missing.

**What:** Phase 10 lists:
```
{"name": "Data accuracy agent re-enable", "status": "paused", "metric": "Built, currently disabled"}
```

Scheduled tasks live state per `list_scheduled_tasks`:

| taskId | enabled | lastRunAt |
|---|---|---|
| data-accuracy-agent | false | 2026-04-05 |
| loam-stats | false | 2026-04-06 |
| loam-data-quality | false | 2026-04-03 |
| open-meteo-weather-drip | **true** | 2026-04-11 (yesterday) |
| nightly-schema-audit | false | 2026-04-08 |

Four scheduled tasks are currently disabled. Only `open-meteo-weather-drip` is running. The roadmap represents only 1 of the 4 paused tasks.

**Why it matters:** Future sprint planning can't easily see what paused infrastructure exists. "Re-enable data-accuracy-agent" is in the roadmap but "re-enable loam-stats / loam-data-quality / nightly-schema-audit" is not, creating an implicit "these are deprecated" signal that may not match intent.

**Proposed fix:** Add the other 3 tasks to Phase 10 sub-tasks with `"status": "paused"` and a metric line showing last run date. Or consolidate into a single sub-task "Scheduled task health check (4 paused, 1 active)".

**Effort:** trivial.

**Dependencies:** none.

---

### F26 — [P2, trivial] `data/stats/` contains ~55 files including ad-hoc Python scripts, stdout dumps, and per-session pass1/pass2 snapshots

**Where:** `data/stats/`.

**What:** Listing via `ls`:
- Python script in a stats dir: `s23_build_sample.py`
- stdout/stderr dumps from session runs: `lwin_long_tail_stdout.txt`, `producer_scrape_intl_log.txt`, `batch2_output.txt`, `execute_output.txt`, `execute_stderr.txt`, `ttb_link_batch_log.txt`, `winetest_session_10_run.log`, `wine_dupe_classify_session13.log`, `wine_merge_session13.log`, `producer_scrape_retry_log.txt`
- Multi-version pass snapshots: `stage1_results.json` + `stage1_results_pass1.json` + `stage1_results.md` + `stage1_results_pass1.md`, `stage2_results.json` + `stage2_results.md`
- Per-date schema snapshots that grew stale: `schema_audit_2026-04-06.json` / `...07.json` / `...08.json` + `schema_audit_latest.json`
- Non-stats scratch: `_heredia_detail.json` (with underscore prefix suggesting temp), `_josh_test_missing.json`

**Why it matters:** The intent of `data/stats/` is a location for analysis artifacts (the `analyze/` pipeline outputs). It's currently a mixed-use dumping ground — some files are canonical sprint state (`sprint_dashboard.md`, `loam_roadmap.md`), some are journals, and a lot are one-off run outputs that were never cleaned up. Reading `ls data/stats/` is noisy.

**Proposed fix:** Sprint 3 cleanup pass. Three buckets:
- **Keep in data/stats/:** journals (`agent_journal.md`, `cron_loop_journal.md`, `schema_audit_journal.md`, `producer_scrape_journal.md`), live dashboards (`sprint_dashboard.md`, `loam_roadmap.md`), winetest dir.
- **Move to data/stats/archive/:** schema_audit daily dumps, stage1/stage2 multi-version snapshots, finished session logs, lwin_long_tail_progress files, ocr_bakeoff_results.
- **Delete:** `s23_build_sample.py` (script belongs in `pipeline/`), underscore-prefix temp files, stdout/stderr dumps from specific sessions.

**Effort:** small (30 min of `git mv` / `git rm`).

**Dependencies:** none.

---

### F27 — [P2, trivial] `data/session_prompts/` has 8 legacy files alongside the new sprint-specific `prompts/` dirs

**Where:** `data/session_prompts/`.

**What:** Directory contents:
```
cron_loop_template.md          (REFERENCED by CLAUDE.md line 148)
loop_ava_rules_vineyards.md
next_human_session.md
next_session.md
option_b_data_gaps.md
seed_appellation_rules.md
seed_appellation_rules_continue.md
winetest_session.md
```

Mixed state:
- `cron_loop_template.md` is actively referenced by CLAUDE.md as "the full structural template" for cron loops. Live.
- `next_session.md` and `next_human_session.md` read as one-off session-handoff files, now obsolete since sprint-state lives in `data/sprints/<name>/`.
- `seed_appellation_rules*.md` are Path A era (2026-04-05). Archive material.
- `option_b_data_gaps.md`, `loop_ava_rules_vineyards.md`, `winetest_session.md` — one-off session specs.

**Why it matters:** The sprint model deprecates the dir but 1 file (cron_loop_template.md) is still live. Mixed state makes it unclear whether the dir is kept or archived.

**Proposed fix:** Promote `cron_loop_template.md` to `data/sprints/<current>/prompts/` when the next cron loop is designed, or keep it in `data/session_prompts/` as a shared template. Archive the other 7 files to `data/session_prompts/_archive/` or just delete (git history preserves them).

**Effort:** trivial.

**Dependencies:** none.

---

### F28 — [P2, trivial] `scripts/` contains 9 ad-hoc batch files (`fetch_legal_sources_batch{2,3,4,5,8,10}.py`) with gaps suggesting abandoned iteration

**Where:** `scripts/`.

**What:**
```
fetch_legal_sources.py
fetch_legal_sources_batch2.py
fetch_legal_sources_batch3.py
fetch_legal_sources_batch4.py
fetch_legal_sources_batch5.py
fetch_legal_sources_batch8.py
fetch_legal_sources_batch10.py
```

No batch 1, 6, 7, 9. Sequential iteration that never converged. These are clearly archival from a specific ad-hoc loop session. Also sweep_masaf_catalogoviti.py reads as single-use. None of these match the `pipeline/` organizational model (`fetch/` would be the right home).

**Why it matters:** `scripts/` is a catch-all that should have been mostly cleared out when `pipeline/` became the canonical location. Small.

**Proposed fix:** Move fetch_legal_sources* and sweep_masaf_* to `scripts_archive/` (same sibling dir where the Node.js archives live) or `pipeline/fetch/` if they're still useful. Sprint 3 cleanup pass.

**Effort:** trivial.

**Dependencies:** none.

---

### F29 — [P2, trivial] `docs/HISTORY.md` has no TOC — 366 lines of historical reference requires sequential scrolling

**Where:** `docs/HISTORY.md`.

**What:** 366 lines containing Schema Hardening history, Reference Data Progress, ~15 session recap entries, Pre-30K rebuild history. No heading index. `grep "^## "` returns headings but from inside the file a reader scrolls.

**Proposed fix:** Add a TOC at the top. 5-minute patch. Not urgent since this is reference material and CLAUDE.md's doc index doesn't currently point readers at HISTORY.md for anything specific.

**Effort:** trivial.

**Dependencies:** none.

---

### F30 — [P2, small] `CLAUDE.md` "Reference Tables (complete)" heading is misleading given S2.4 findings

**Where:** `CLAUDE.md:288`.

**What:**
> ### Reference Tables (complete)
> Countries (68), regions (389), appellations (3,662), grapes (9,693 + 34,820 synonyms), varietal categories (161), publications (78), attribute definitions (73), tasting descriptors (304), farming certifications (21), biodiversity certifications (7), soil types (39). All seeded, audited, and cross-validated. See `docs/HISTORY.md` for detail.

S2.4 found 30 issues in reference content, 8 P0, including:
- **F1** varietal_categories has 5+ P0 wrong-grape links (Merlot→Grolleau Noir, Riesling→Crouchen, Verdejo→Trousseau Noir, Greco→Albana Bianca, St. Laurent→Muscat St. Laurent).
- **F2** PINOT BLANC has 4 polluting VIVC synonyms causing the Chardonnay/Pinot Blanc systemic bug.
- **F3** 121 famous appellations stored with slash-concatenated alias names.
- **F5** Pauillac 1855 classification has 3 of 5 tier counts wrong.
- **F6** grapes.name uses VIVC cépage+suffix form causing cascade issues.

"Complete" with "cross-validated" is actively misleading given the S2.4 findings.

**Proposed fix:** Rename heading to `### Reference Tables (seeded, known issues flagged in S2.4)`. Add a one-line pointer: "See `data/sprints/audit/findings/findings_wine_reference.md` for 30 known issues including 8 P0 wrong-grape links in varietal_categories."

**Effort:** trivial.

**Dependencies:** none.

---

## P3 — Nice to have

### F31 — [P3, trivial] `data/stats/loam_roadmap.md` is a git-tracked auto-generated text dump that drifts when `--save` isn't run

**Where:** `data/stats/loam_roadmap.md`.

**What:** `loam_roadmap.py:313` defines `save_to_file()` which writes `data/stats/loam_roadmap.md` when `--save` is passed. So this file:
- Is generated from `loam_roadmap.json` + live DB metrics.
- Drifts whenever `loam_roadmap.json` changes but `--save` isn't re-run.
- Is checked into git (per `ls data/stats/`).

Same category as a compiled artifact in source control.

**Why it matters:** Either it's authoritative (then every JSON update must rerun `--save` or it drifts) or it's ephemeral (then gitignore it). Current state is neither.

**Proposed fix:** Either (a) add `data/stats/loam_roadmap.md` to `.gitignore` and document that it's ephemeral, or (b) add a pre-commit hook / make target to regenerate it on JSON change. Low priority.

**Effort:** trivial.

**Dependencies:** none.

---

### F32 — [P3, trivial] `memory/MEMORY.md` line length — some entries approach the 150-character limit stated in CLAUDE.md

**Where:** `memory/MEMORY.md:9`, line 12 (Sprint 2 findings entry).

**What:** CLAUDE.md memory instructions say:
> `MEMORY.md` is an index, not a memory — each entry should be one line, under ~150 characters

The longest entry in the current MEMORY.md is:

```
- [Sprint 2 findings so far](project_sprint2_findings.md) — 213 findings across S2.1-S2.7. Sprint 3 pre-reqs: voice module consolidation (S2.6 F1/F2) + UI hygiene bundle (S2.7 F1/F2/F9 + dead-fetch renders, ~3-4 hrs) + staging relink (S2.2 F1 / S2.5 F3) + grape repair compound (data + code). Feature flag on enrich-wine stays OFF until voice + L3 + grape repair + AI-disclaimer UI (S2.7 F5) all land.
```

— approximately **530 characters**. Also the Quality-before-enrichment line is ~320 characters. Both exceed the 150-char limit.

**Why it matters:** MEMORY.md loads into every conversation. Keeping it lean is explicitly called out in CLAUDE.md's memory instructions. The current longest lines add ~8× the intended per-line budget. Not urgent because they're still short relative to the full memory files they point to — but the index convention is slipping.

**Proposed fix:** Tighten the 2 longest entries to ~150 chars each. "[Sprint 2 findings so far](project_sprint2_findings.md) — 213 findings across S2.1–S2.7. Sprint 3 pre-reqs: voice consolidation + UI hygiene + staging relink + grape repair." The detail lives in `project_sprint2_findings.md`, which is where it should.

**Effort:** trivial.

**Dependencies:** none.

---

## Cross-session observations for S2.9 synthesis

Five patterns worth escalating to S2.9 synthesis:

1. **Doc staleness is systematic, not ad-hoc.** S2.1 F28 flagged ≥6 places; S2.7 F2 found a dead column reference in a page fetch; S2.8 finds CLAUDE.md sections that contradict each other (F1, F4), memory that contradicts itself across files (F2 vs F1 vs `project_sprint_model_and_rf_direction.md`), documentation with wrong column names (F3), and multiple "current" focus blocks that point at defunct paths (F6, F10). The common thread: **nothing enforces doc freshness except manual discipline at session wrap-up.** Sprint 3 should add a "drift check" step to the wrap-up checklist that runs `python -m pipeline.analyze.db_counts` and compares against hardcoded numbers in CLAUDE.md.

2. **The sprint-model pivot (DECISIONS.md 2026-04-11) was incompletely executed.** Three files documented the change correctly (CLAUDE.md `## Current State`, `memory/project_quality_before_enrichment.md`, `memory/project_sprint_model_and_rf_direction.md` content). Three files still carry the pre-pivot frame (CLAUDE.md `## Current Focus`, `memory/30k_status.md`, `docs/ENRICHMENT.md` via Grade C path). One file carries it ambiguously (`docs/30K_PLAN.md` header + broken paths). The fix was one-session-wide and missed half the surface area.

3. **docs/ root has 6 files that should be in `docs/reference/`.** AUDIT_2026-04-01.md, 30K_PLAN.md, PATH_A_ROLLBACK.md, IDENTITY_RULES.md (maybe), BACKLOG.md (maybe), MERGE_STRATEGY.md (maybe). Current `docs/reference/` has 2 files (LWIN_STRATEGY, SCHEMA_ASSESSMENT) — both moved there as retired docs. The move pattern is established; several other candidates haven't been moved yet. Sprint 3 gets a 30-min archive pass.

4. **Empty dirs and orphaned scripts indicate scaffold intent that never landed.** `docs/architecture/`, `docs/pipelines/` (F9), `scripts/fetch_legal_sources_batch*.py` gaps (F28), `data/stats/s23_build_sample.py` in a stats directory (F26). Pattern: someone planned a reorg, started it, got interrupted, the half-done state stuck. Sprint 3 should consolidate these into one "structural cleanup" pass.

5. **Two dashboards = two answers.** `loam_roadmap.py` reads `loam_roadmap.json` sub_tasks (stale per F5); `dash.ps1` reads sprint dir directly (live). For phases that map to sprints, the roadmap should delegate phase-level state to the sprint dir instead of duplicating it in JSON. F24 would add a Sprint 2 dispatcher; a bigger fix would remove `sub_tasks` from `loam_roadmap.json` entirely for phases that have a sprint dir.

---

## Sprint 3 pre-req UI hygiene carryover

Not a new finding, but worth noting: the S2.7 UI hygiene bundle (~3-4 hours) + this session's doc hygiene bundle (~2-3 hours) together total ~6 hours of prep work before Sprint 3 execution can start. Both bundles are highly parallel (doc fixes don't block UI fixes and vice versa). A single pre-Sprint-3 "hygiene" session could close both.

---

## Scope-breaker check

None of the findings require a Sprint 3 rewrite or scope change. All execute inside the existing audit → fix → redesign → enrich sequence. No escalation to the user needed under the recalibration clause.

**Budget check:** $0 actual spend (Opus inline pattern). $25.00 / $25.00 remaining on Sprint 2 ceiling. S2.9 (synthesis + Sprint 3 backlog) remains at $0 expected under the same pattern.

---

## Deliverables

- `data/sprints/audit/findings/findings_meta.md` (this file) — 32 findings
- `data/sprints/audit/sessions.json` — S2.8 marked `done` with $0 ai_spend
- `data/sprints/audit/journal.md` — S2.8 section appended
- `data/sprints/audit/budget.json` — S2.8 entry at $0
- `data/sprints/audit/prompts/s2_8_meta.md` — session prompt moved into sprint
- `CLAUDE.md` — Current State updated with S2.8 done + running totals
- `memory/project_sprint2_findings.md` — updated with S2.8 cross-references
- `data/sessions.md` — whiteboard entry moved to Done (short-form per F18 recommendation)

No DB writes. No pipeline runs. No DDL/DML. Read-only audit.
