# B6.3 — Schema + IDENTITY_RULES Section 11 + Blocking Dry-Run + L1 Haiku

You are opening B6.3 of Sprint 6 (Producer Dedup). B6.1 locked the plan; B6.2
completed the LWIN long-tail import. The producers table now holds **33,281
rows (33,225 active)** up from 10,683 — the complete dedup universe.

Full plan: `data/sprints/dedup/plan.md`. Journal: `data/sprints/dedup/journal.md`.

---

## Scope of B6.3 (in order)

### Part A: Schema migration (Supabase branch)

1. Create a Supabase development branch for S6 work (one-time, reused through
   B6.6 execution). This is Gate 2 in the plan — schema migration lands on the
   branch, not production, until user approves the dry-run merge execution.
2. Extend `producer_dedup_pairs`: add `producer_id_a uuid`, `producer_id_b
   uuid`, `method_name text`, `confidence numeric`, `reasoning text`,
   `cost_cents integer`, `signals jsonb`, `ttb_evidence jsonb`, `web_evidence
   jsonb`, `flag_reason text`. Keep existing columns if any; migration should
   be additive. Index (producer_id_a, producer_id_b, method_name).
3. Create `producer_merge_history`: full JSON snapshot of each merged pair +
   repointed-rows audit (wines moved, external_ids moved, source_* pointers
   updated). Purpose = programmatic rollback. Columns: `id uuid pk`,
   `merged_at timestamptz`, `survivor_id uuid`, `absorbed_id uuid`,
   `absorbed_snapshot jsonb` (full row including all child tables),
   `repointed_counts jsonb`, `method_name text`, `reasoning text`,
   `reviewed_by text`.
4. DO NOT create a `producer_relationships` table — parent-child uses the
   existing `producers.parent_producer_id` column (see plan).
5. Write the migration file to `supabase/migrations/` with a dated name
   (e.g. `2026-04-16_b6_3_producer_dedup_schema.sql`). Test it against the
   development branch, confirm it's a clean apply.

### Part B: IDENTITY_RULES.md Section 11 (Producer Identity Rules)

Extend `docs/IDENTITY_RULES.md` with a new Section 11: "Producer Identity
Rules" based on the plan's Producer identity decisions. Core rule:
**brand-on-label**. Draft sections:

- 11.1 MERGE criterion: same brand identity on label
- 11.2 PARENT-CHILD criterion: distinct brands with ownership
- 11.3 SKIP criterion: unrelated producers
- 11.4 Edge cases (one subsection each):
  - Renames → MERGE + alias row
  - Dissolved + reopened → one continuous producer
  - Private labels (Charles Shaw, Kirkland) → producer row, per-wine
    `actual_vintner` in `wines.metadata`
  - Retailers (Trader Joe's, Costco) → never producers
  - Second wines (Les Forts de Latour, Carruades) → wine row, not producer
  - Négociant + estate (if same entity bottles both) → PARENT-CHILD
  - Accent variants → MERGE, survivor matches actual label form
  - Importer prefixes → strip when matching (verify examples in B6.3)
  - US producers sharing a TTB permittee_basic_permit → very strong MERGE signal
- 11.5 The rules above are **embedded verbatim** in every LLM prompt (L1, L2, L3).

Gate 1 (plan): user reviews Section 11 before L1 runs. Pause for user
review — show the draft, wait for approval, edit if asked, then proceed.

### Part C: Blocking dry-run (Gate 3 — no LLM spend yet)

Build `pipeline/identity/producer_blocking.py` that implements the 9 blocking
strategies from the plan as union'd queries, writes the resulting
`producer_dedup_pairs` rows with `method_name='blocking'` and evidence
in `signals`, but **does NOT call any LLM**. Returns the pair count per
strategy + the union total.

Expected output: a count table like
```
Strategy                                          pairs    cumulative
1. Same country + exact normalized name             ?          ?
2. Same country + trigram >= 0.3                    ?          ?
3. Same country + embedding cosine >= 0.5           ?          ?
4. Same country + first-3-char + token overlap      ?          ?
5. Shared external_id (LWIN_7, website host)        ?          ?
6. Shared TTB permittee_basic_permit                ?          ?
7. Cross-country + strong signal                    ?          ?
8. Wine-catalog overlap >= 30%                      ?          ?
9. Producer-name substring containment              ?          ?
UNION TOTAL                                         ?          ?
```

Show this to the user. **Do not proceed to L1 until the user sees the
actuals.** This is cost lever 4 — if the union is 500K instead of 150K, we
decide what to drop before committing L1 spend.

### Part D: TTB fingerprint spot-check (Gate 4)

For 10-20 US producers in the pairs list, compute the TTB fingerprint
`{permittee_basic_permit, address, brand_name_list, cola_count}` by joining
`source_ttb_colas`. Show the user the raw fingerprints for eyeballing. Goal
= confirm the TTB signal looks good before embedding it in L1 prompts.

### Part E: L1 pilot (Gate 5) — 200 pairs

Build `pipeline/identity/producer_dedup_l1.py`:

- Model: `claude-haiku-4-5` via Anthropic SDK direct (not OpenRouter) with
  `pipeline/lib/models.py`. Reuse prompt caching if the prefix > 4K tokens.
- Prompt: IDENTITY_RULES Section 11 verbatim + per-pair producer context
  (name, country, region, wine count, top 5 wines, TTB fingerprint if US,
  LWIN presence, website host). **Batched 10 pairs per call** (plan Lever 3).
- Output JSON per pair: `{verdict: MERGE|PARENT_CHILD|SKIP|UNCERTAIN,
  confidence: 0.0-1.0, reasoning: text}`.
- Write to `producer_dedup_pairs` with `method_name='l1_haiku_batch'`.

Run L1 on a 200-pair pilot first (mix of strategies 1, 2, 6, 7 for a
representative spread). Compute agreement with the S4.1 re-verification
anchors + accuracy vs. obvious-SAME pairs. Show user pilot accuracy + cost
(~$0.50-1). **Pause for user approval before full L1 run.**

### Part F: Full L1 run (post-approval)

After user approves pilot, run L1 on the full definitive pair list. Expected
cost per plan: $60-130 for 100K-250K pairs post-LWIN.

---

## Do NOT do in B6.3

- L2, L3, L4 (B6.4-B6.5)
- Merge execution (B6.6)
- Anchor set construction (B6.4)
- Ablation tests (B6.4)
- Production schema writes — everything lands on the Supabase branch until
  B6.6 merges

---

## Acceptance gate for B6.3

1. Supabase branch created, schema migration applied cleanly on branch
2. `docs/IDENTITY_RULES.md` Section 11 drafted AND user-approved
3. Blocking dry-run completed, pair counts shown to user AND user
   acknowledges before L1 spend
4. TTB fingerprint spot-check done, user comfortable with the signal
5. L1 pilot (200 pairs) completed, pilot accuracy meets user bar
6. Full L1 run complete, all pairs have an `l1_haiku_batch` row in
   `producer_dedup_pairs`
7. B6.3 spend tracked and logged to budget.json

Budget for B6.3: $60-130 (L1). Ceiling still $250 across the full sprint.

---

## Close-out

1. Update `data/sprints/dedup/journal.md` with B6.3 entry (schema, rules,
   blocking counts, pilot accuracy, L1 run stats, cost)
2. Update `data/sprints/dedup/sessions.json` + `budget.json`
3. Update `data/dashboard.html` (B6.3 checkbox, vitals update, L1 stats)
4. Update `CLAUDE.md` Current Focus (B6.3 done, B6.4 next)
5. Update `data/sessions.md`
6. Write `data/session_prompts/b6_4_l2_l3_anchor.md` (prompt for B6.4)
7. Commit + push: "B6.3: schema + IDENTITY_RULES §11 + blocking + L1 Haiku"

---

## Key context files

- `data/sprints/dedup/plan.md` — full sprint plan (the authority)
- `data/sprints/dedup/journal.md` — B6.1 + B6.2 history
- `docs/IDENTITY_RULES.md` — existing wine identity rules (extend, don't rewrite)
- `pipeline/lib/models.py` — model ID constants
- `pipeline/lib/db.py` — direct psycopg2 session pooler via `get_conn()`
- `pipeline/lib/normalize.py` — `normalize()` + `normalize_producer()` helpers
- `producers` table — canonical, 33,281 rows
- `source_lwin` table — 189,359 rows, 0 unlinked, LWIN backbone
- `source_ttb_colas` table — TTB fingerprint source for US producers
- `external_ids` table — LWIN_7 IDs for 157,346 wines, website hosts for some producers
