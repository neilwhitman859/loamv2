# Loam — Spend Ledger

Running record of every dollar spent on LLM calls and external services. Auto-updated on each dashboard tick via `pipeline/analyze/update_dashboard.py`. Historical entries stay fixed; only the current in-progress block at the top gets refreshed.

<!--ledger-header-->
**Last updated:** 2026-04-17 22:15
**Project total:** ~$245.15
**Current sprint:** Sprint 6 — Producer Dedup (active, $152.71 of $250 ceiling, $97.29 remaining)
<!--/ledger-header-->

---

## Sprint 6 — Producer Dedup (active)

<!--ledger-b6-5a-->
### B6.5a — Production ladder (in progress, ~$49.29 so far)

Production run of the tiered classifier ladder on all 151K blocking pairs. Outputs merge into `producer_dedup_pairs` under distinct `method_name` per tier. Will deliver `review_queue.json` for B6.5b interactive user review.

| Timestamp | Item | Model / Tier | Pairs | Cost |
|---|---|---|---|---:|
| 2026-04-17 16:00 | L1.5 Gemini basic pre-pause | Gemini 3 Flash Preview (OpenRouter) | 48,675 | $10.13 |
| 2026-04-17 17:05 | L1.5 Gemini basic resumed | Gemini 3 Flash Preview (OpenRouter) | 102,445 | $22.01 |
| 2026-04-17 20:12 | SKIP audit L2 | Haiku 4.5 rich (Anthropic) | 600 | $0.51 |
| 2026-04-17 20:12 | SKIP audit L3 | Sonnet 4.6 no-web (Anthropic) | 600 | $6.06 |
| 2026-04-17 21:45 | L2 production (in progress) | Haiku 4.5 rich (Anthropic) | 8,025 so far | $5.77 |
| 2026-04-17 21:45 | L2.5 production (in progress) | Gemini 3 Flash Preview rich (OpenRouter) | 17,335 so far | $4.81 |
| | | | **Subtotal** | **$49.29** |

**Key events:** OpenRouter key hit $100 cumulative spending cap during L1.5 run; paused at pair 48,675; user raised cap to $200; resumed at pair 48,676. Stage 1 threshold adjusted from committed 0.97/0.97 to 0.95/0.95 after SKIP audit showed 0 auto-apply FNs at lower threshold across 600-pair band-stratified sample.
<!--/ledger-b6-5a-->

### B6.4 — Calibration + thresholds ($24.51)

Built 600-pair stratified calibration set, gold-labeled 367 pairs, ran all 4 classifier tiers against it, committed thresholds.

| Item | Model / Tier | Pairs | Cost |
|---|---|---|---:|
| Calibration oracle (gold labels) | Sonnet 4.6 + web_search_20250305 | 167 hard-case pairs | $22.93 |
| L1.5 Gemini basic calibration | Gemini 3 Flash Preview | 595 | $0.13 |
| L2 Haiku rich calibration | Haiku 4.5 | 600 | $0.59 |
| L2.5 Gemini rich calibration | Gemini 3 Flash Preview | 600 | $0.19 |
| L3 no-web ablation | Sonnet 4.6 no-web | 50 | $0.65 |
| Safety Net A (unblocked spot-check) | Haiku 4.5 + Gemini 3 Flash | 100 | $0.05 |
| | | **Subtotal** | **$24.54** |

### B6.3 — Schema + IDENTITY_RULES §11 + blocking + L1 ($78.44)

Applied migrations, drafted §11, ran 8 blocking strategies, classified all 151K pairs with L1 Haiku batched 10/call with prompt caching.

| Item | Model / Tier | Pairs | Cost |
|---|---|---|---:|
| L1 pilot | Haiku 4.5 batched cached | 200 | $0.12 |
| L1 full corpus run | Haiku 4.5 batched cached | 151,120 | $78.32 |
| | | **Subtotal** | **$78.44** |

### B6.2.2 — LWIN wine recovery via display_name ($0)
### B6.2.1 — Pre-dedup hygiene (search_vector trigger fix) ($0)
### B6.2 — LWIN long-tail producer import ($0)
### B6.1 — Sprint 6 planning & design ($0)

No LLM spend on these blocks — pure Python + SQL work.

**Sprint 6 total so far: ~$152.71**

---

## Sprint 5 — AI Bakeoff (closed, ~$55.00)

3-task bake-off across 29 models via OpenRouter. Task 1 dedup (200 pairs), Task 2 extraction (50 HTML pages), Task 3 prose (30 wines × 3 rounds + repechage + field-specialization + search-grounded test).

| Block | Scope | Cost |
|---|---|---:|
| B5.1-B5.5 (design + data build + runs) | Test data + 29 model runs × 3 tasks | ~$3 |
| B5.5 prose generation | 29 models × 30 wines × multiple rounds | ~$11.86 |
| B5.6 tournament judge | Opus 4.6 judge calls across 3 rounds + repechage | ~$22.48 |
| B5.6 R4 repechage | gemini-3-flash + gpt-5-mini | ~$2 |
| B5.6 R5 field specialization | 3 wines × 3 cheap models | ~$1 |
| B5.6 R6 search grounding test | 8 models × 12 wines | ~$14 |
| B5.7 prompt caching test | Opus + Sonnet via OpenRouter | $0.82 |
| | **Subtotal** | **~$55** |

Outcome: current-prompt leader gpt-5.4-mini (3.960 / $452 per 170K wines). Cheaper models (gemini-3-flash, DeepSeek v3.2) close enough that prompt v2 could flip rankings.

---

## Sprint 4 — Demo (closed, $37.44)

515 wines enriched across 14 producers via Opus-inline voice calibration + parallel Grade A worker. Cascade: 4 countries → 20 regions → 51 appellations → 41 grapes → 14 producers → 515 wines.

| Block | Scope | Cost |
|---|---|---:|
| S4.1 Track 0+1 | wire wine_lookups, Ridge/Lopez/CIRQ merges, grape display names | $0 |
| S4.2 Track 2 | Reference cascade — countries/regions/appellations/grapes via Opus inline | $0 |
| S4.3 Track 2+3 | Producer enrichment + wine voice calibration (5 wines) | $0 |
| S4.4 Track 3-5 serial | 209 wines Grade A single-worker | $6.59 |
| S4.5 Track 3-5 parallel | 306 wines Grade A × 8 workers | $11.33 |
| S4.6 Voice re-enrichment | All 515 with revised prompt (banned words, variation rules) | $19.52 |
| | **Subtotal** | **$37.44** |

---

## Sprints 1-3 (closed, $0)

All three sprints used Opus-inline reasoning + Python/SQL only. No LLM-API spend tracked against project budget.

- **Sprint 3 Fix** — 158 findings fixed, 101 deferred. Infrastructure + SQL. $0 of $25 budget.
- **Sprint 2 Audit** — 9 expert audits, 275 findings, all Opus inline. $0 of $25 budget.
- **Sprint 1 Build** — 156K wines + 32 sources + schema + frontend. Pre-budget-tracking era. $0.

---

## Project-wide totals

| Category | Amount |
|---|---:|
| Sprint 6 Producer Dedup (in progress) | ~$152.71 |
| Sprint 5 AI Bakeoff | ~$55.00 |
| Sprint 4 Demo | $37.44 |
| Sprints 1-3 | $0 |
| **Project total to date** | **~$245.15** |

---

## Categorization

| Category | Description | Running |
|---|---|---:|
| LLM via direct Anthropic SDK | Haiku, Sonnet, Opus inline | ~$135 |
| LLM via OpenRouter | Gemini, DeepSeek, GPT-5.x, others (bake-off + Gemini tiers in Sprint 6) | ~$110 |
| External APIs | None (NASA POWER, Open-Meteo are free) | $0 |
| Hosting / infrastructure | Supabase Small $10/mo × N months, Render static site free | (not counted against sprint budgets) |

---

*This ledger is the source of truth for per-run cost. `budget.json` tracks block totals; `dashboard.html` shows live status. For any discrepancy between them, trust the DB's `producer_dedup_pairs.cost_cents` column.*
