"""Export all Phase 2 (pipeline auto-decided) dedup decisions into the bundle.

Phase 2 = `producer_dedup_routing_stage3` — 151,150 decisions the AI ladder made
without escalating to Chrome-per-pair validation.

Actionable decisions (MERGE + PC + user-review queues): dumped in full.
SKIP decisions (146,435): dumped as counts + a 500-row random sample.

Outputs:
    data/sprints/dedup/execution_bundle/phase2_actionable_decisions.jsonl
    data/sprints/dedup/execution_bundle/phase2_skip_sample.jsonl
    data/sprints/dedup/execution_bundle/phase2_summary.md
"""
from __future__ import annotations

import json
import random
from collections import Counter
from pathlib import Path

from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "data/sprints/dedup/execution_bundle"
BUNDLE.mkdir(parents=True, exist_ok=True)

ACTIONABLE_OUT = BUNDLE / "phase2_actionable_decisions.jsonl"
SKIP_SAMPLE_OUT = BUNDLE / "phase2_skip_sample.jsonl"
SUMMARY_OUT = BUNDLE / "phase2_summary.md"

ACTIONABLE_ACTIONS = (
    "auto_apply_merge",
    "auto_apply_pc",
    "user_review_pc",
    "user_review_merge_lowconf",
    "user_review_merge_unvalidated",
    "user_review_missing",
)


def main():
    conn = get_conn()
    conn.autocommit = True

    # Pull all action counts
    with conn.cursor() as cur:
        cur.execute("SELECT stage3_action, COUNT(*) FROM producer_dedup_routing_stage3 GROUP BY stage3_action ORDER BY 2 DESC")
        action_counts = dict(cur.fetchall())

    # Export actionable decisions (MERGE + PC + review queues)
    print(f"Exporting actionable decisions to {ACTIONABLE_OUT}...")
    actionable_n = 0
    with ACTIONABLE_OUT.open("w", encoding="utf-8") as f, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pair_id, producer_id_a::text, producer_id_b::text,
                   name_a, name_b, country, core_a, core_b, is_core,
                   stage1_action, stage2_action,
                   l1_verdict, l1_conf, l1_5_verdict, l1_5_conf,
                   l2_verdict, l2_conf, l2_5_verdict, l2_5_conf,
                   web_verdict, web_conf, web_reasoning,
                   stage3_action, stage3_reason, created_at
            FROM producer_dedup_routing_stage3
            WHERE stage3_action = ANY(%s)
            ORDER BY stage3_action, pair_id
            """,
            (list(ACTIONABLE_ACTIONS),),
        )
        cols = [d[0] for d in cur.description]
        for row in cur:
            rec = dict(zip(cols, row))
            # Normalize timestamps
            if rec.get("created_at") is not None:
                rec["created_at"] = rec["created_at"].isoformat()
            # Convert numerics to float for JSON
            for k in ("l1_conf", "l1_5_conf", "l2_conf", "l2_5_conf", "web_conf"):
                if rec.get(k) is not None:
                    rec[k] = float(rec[k])
            # Truncate long reasoning to keep file under control
            if rec.get("web_reasoning") and len(rec["web_reasoning"]) > 800:
                rec["web_reasoning"] = rec["web_reasoning"][:800] + "... [truncated]"
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            actionable_n += 1
    print(f"  wrote {actionable_n} actionable decisions")

    # Export SKIP sample (500 random rows)
    print(f"Exporting SKIP sample to {SKIP_SAMPLE_OUT}...")
    skip_n = 0
    with SKIP_SAMPLE_OUT.open("w", encoding="utf-8") as f, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pair_id, producer_id_a::text, producer_id_b::text,
                   name_a, name_b, country, is_core,
                   stage1_action, stage2_action,
                   l1_verdict, l1_conf, l1_5_verdict, l1_5_conf,
                   l2_verdict, l2_conf, l2_5_verdict, l2_5_conf,
                   stage3_action, stage3_reason
            FROM producer_dedup_routing_stage3
            WHERE stage3_action LIKE 'auto_apply_skip%%'
            ORDER BY random()
            LIMIT 500
            """,
        )
        cols = [d[0] for d in cur.description]
        for row in cur:
            rec = dict(zip(cols, row))
            for k in ("l1_conf", "l1_5_conf", "l2_conf", "l2_5_conf"):
                if rec.get(k) is not None:
                    rec[k] = float(rec[k])
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            skip_n += 1
    print(f"  wrote {skip_n} SKIP samples")

    # Per-action summary
    print(f"Writing {SUMMARY_OUT}...")
    with conn.cursor() as cur:
        # Cluster breakdown within actionable
        cur.execute(
            """
            SELECT stage3_action, l1_verdict, l2_verdict, web_verdict, COUNT(*)
            FROM producer_dedup_routing_stage3
            WHERE stage3_action = ANY(%s)
            GROUP BY stage3_action, l1_verdict, l2_verdict, web_verdict
            ORDER BY stage3_action, COUNT(*) DESC
            """,
            (list(ACTIONABLE_ACTIONS),),
        )
        verdict_breakdown = cur.fetchall()

        # Is-core breakdown (top-US-producers subset)
        cur.execute(
            """
            SELECT stage3_action, is_core, COUNT(*)
            FROM producer_dedup_routing_stage3
            GROUP BY stage3_action, is_core
            ORDER BY stage3_action, is_core
            """
        )
        core_breakdown = cur.fetchall()

        # Countries in actionable
        cur.execute(
            """
            SELECT stage3_action, country, COUNT(*) FROM producer_dedup_routing_stage3
            WHERE stage3_action = ANY(%s)
            GROUP BY stage3_action, country
            ORDER BY stage3_action, COUNT(*) DESC
            """,
            (list(ACTIONABLE_ACTIONS),),
        )
        country_breakdown = cur.fetchall()

        # Top-20 by web_conf (highest and lowest) among user_review and auto_apply
        cur.execute(
            """
            SELECT pair_id, name_a, name_b, country,
                   stage3_action, l1_verdict, l1_conf, l2_verdict, l2_conf,
                   web_verdict, web_conf
            FROM producer_dedup_routing_stage3
            WHERE stage3_action IN ('auto_apply_merge', 'auto_apply_pc')
            ORDER BY COALESCE(web_conf, l2_conf, l1_conf) ASC
            LIMIT 20
            """
        )
        weakest_auto = cur.fetchall()

    L = []
    L.append("# Phase 2 — Pipeline Auto-Applied Decisions (routing_stage3)")
    L.append("")
    L.append("These are decisions the AI ladder (L1→L1.5→L2→L2.5→L3) made without")
    L.append("needing Chrome-per-pair validation. They live in the DB table")
    L.append("`producer_dedup_routing_stage3` and are UNEXECUTED — nothing has been")
    L.append("written to the producers table yet (`producer_merge_history` is empty).")
    L.append("")
    L.append("**Phase 1 (the 493-pair Chrome-validated ledger at `verdict_ledger.jsonl`)")
    L.append("is a SUBSET of this Phase 2 queue** — specifically, the pairs that were")
    L.append("escalated to Chrome validation. Phase 1 refines those 493 decisions with")
    L.append("per-pair web evidence.")
    L.append("")
    L.append("## Action distribution")
    L.append("")
    L.append("| stage3_action | Count | Type |")
    L.append("|---|---|---|")
    total = 0
    for action, count in sorted(action_counts.items(), key=lambda kv: -kv[1]):
        total += count
        kind = "no-op" if action.startswith("auto_apply_skip") else ("mutation" if action.startswith("auto_apply") else "needs review")
        L.append(f"| `{action}` | {count:,} | {kind} |")
    L.append(f"| **Total** | **{total:,}** | |")
    L.append("")
    L.append(f"Actionable decisions (mutations + review queues): **{sum(action_counts.get(a, 0) for a in ACTIONABLE_ACTIONS):,}**")
    L.append(f"No-op decisions (SKIPs): **{sum(v for k, v in action_counts.items() if 'skip' in k):,}**")
    L.append("")

    L.append("## Core vs Tail (by `is_core`)")
    L.append("")
    L.append("`is_core = TRUE` when max(wines_a, wines_b) ≥ 10 — i.e., the pair involves")
    L.append("at least one US-market-relevant producer.")
    L.append("")
    L.append("| stage3_action | core_pairs | tail_pairs |")
    L.append("|---|---|---|")
    core_data: dict[str, dict] = {}
    for action, is_core, count in core_breakdown:
        core_data.setdefault(action, {})[bool(is_core)] = count
    for action in sorted(core_data.keys()):
        c = core_data[action]
        L.append(f"| `{action}` | {c.get(True, 0):,} | {c.get(False, 0):,} |")
    L.append("")

    L.append("## Verdict agreement within actionable decisions")
    L.append("")
    L.append("How the AI ladder classified each pair. `web_verdict` is present only when")
    L.append("the pair escalated to L3 Sonnet (w/o Chrome).")
    L.append("")
    L.append("| stage3_action | L1 | L2 | web | count |")
    L.append("|---|---|---|---|---|")
    for action, l1, l2, web, count in verdict_breakdown[:50]:
        L.append(f"| `{action}` | {l1 or '-'} | {l2 or '-'} | {web or '-'} | {count:,} |")
    if len(verdict_breakdown) > 50:
        L.append(f"| _(+{len(verdict_breakdown)-50} more combinations)_ | | | | |")
    L.append("")

    L.append("## Country distribution of actionable decisions")
    L.append("")
    L.append("| stage3_action | country | count |")
    L.append("|---|---|---|")
    by_action_country: dict[str, list] = {}
    for action, country, count in country_breakdown:
        by_action_country.setdefault(action, []).append((country, count))
    for action in ACTIONABLE_ACTIONS:
        if action not in by_action_country:
            continue
        rows = by_action_country[action]
        for country, count in rows[:10]:
            L.append(f"| `{action}` | {country or '(null)'} | {count:,} |")
        if len(rows) > 10:
            L.append(f"| `{action}` | _({len(rows)-10} more countries)_ | |")
    L.append("")

    L.append("## Lowest-confidence auto-applies (potentially risky — should be reviewed)")
    L.append("")
    L.append("The 20 `auto_apply_merge` / `auto_apply_pc` decisions with the lowest")
    L.append("confidence signals in the routing pipeline. These are the most likely to")
    L.append("be wrong and should be spot-checked before execution.")
    L.append("")
    L.append("| pair_id | country | action | names | L1 (conf) | L2 (conf) | web (conf) |")
    L.append("|---|---|---|---|---|---|---|")
    for row in weakest_auto:
        pid, na, nb, ctry, action, l1v, l1c, l2v, l2c, wv, wc = row
        L.append(f"| {pid} | {ctry} | `{action}` | {na} ⇔ {nb} | {l1v or '-'} ({l1c or '-'}) | {l2v or '-'} ({l2c or '-'}) | {wv or '-'} ({wc or '-'}) |")
    L.append("")

    L.append("## Files in this directory")
    L.append("")
    L.append(f"- `phase2_actionable_decisions.jsonl` — **{actionable_n:,} rows.** Full data for")
    L.append("  every MERGE + PC + review-queue decision in the pipeline. Suitable for AI")
    L.append("  review at scale.")
    L.append(f"- `phase2_skip_sample.jsonl` — **{skip_n} rows.** Random sample of")
    L.append("  `auto_apply_skip` decisions to spot-check the SKIP classification.")
    L.append(f"- `phase2_summary.md` — this file.")
    L.append(f"- `phase2_risk_analysis.md` — what could go wrong if Phase 2 is executed.")
    L.append("")
    L.append("## Relationship to Phase 1")
    L.append("")
    L.append("The 493-pair Phase 1 ledger (`verdict_ledger.jsonl` at bundle root) was")
    L.append("produced by Chrome-per-pair validation of pairs that the pipeline flagged")
    L.append("for review (`user_review_*` actions in stage3). Phase 1 is the fully")
    L.append("human+Chrome-reviewed slice. Phase 2 covers everything else the pipeline")
    L.append("decided without Chrome.")
    L.append("")
    L.append("If Phase 1 is executed as-is, the Phase 2 auto-apply decisions (3,154")
    L.append("MERGEs + 785 PCs) would still be unexecuted. They need either a separate")
    L.append("sampled audit before execution OR trust in the L1+L2 consensus agreement")
    L.append("that produced them.")
    L.append("")

    SUMMARY_OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"Summary written: {SUMMARY_OUT}")
    conn.close()


if __name__ == "__main__":
    main()
