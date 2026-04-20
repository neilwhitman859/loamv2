"""Build execution_plan.md — human-readable narrative of what the execute script will do.

Reads: data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
Writes: data/sprints/dedup/execution_bundle/execution_plan.md

For reviewers who don't want to read Python, this is a plain-English walkthrough
of every action the execute script will take, grouped by action type.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
LEDGER = REPO / "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl"
OUT = REPO / "data/sprints/dedup/execution_bundle/execution_plan.md"


def fetch_producer_detail(conn, pids):
    if not pids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id::text, name, (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) "
            "FROM producers p WHERE p.id = ANY(%s::uuid[])",
            (list(pids),),
        )
        return {r[0]: {"id": r[0], "name": r[1], "wines": r[2]} for r in cur.fetchall()}


def pick_survivor_heuristic(a, b, hint):
    """Mirror the execute script's §11.6 heuristic for report purposes."""
    if hint:
        h = hint.strip().lower()
        if (a.get("name") or "").strip().lower() == h:
            return a, b
        if (b.get("name") or "").strip().lower() == h:
            return b, a
    if any(ord(c) > 127 for c in (a.get("name") or "")) != any(ord(c) > 127 for c in (b.get("name") or "")):
        return (a, b) if any(ord(c) > 127 for c in (a.get("name") or "")) else (b, a)
    if (a.get("wines", 0)) >= (b.get("wines", 0)):
        return a, b
    return b, a


def main():
    with LEDGER.open(encoding="utf-8") as f:
        ledger = [json.loads(line) for line in f if line.strip()]

    merges = [e for e in ledger if e.get("final_verdict") == "MERGE"]
    pcs = [e for e in ledger if e.get("final_verdict") == "PARENT_CHILD"]
    skips = [e for e in ledger if e.get("final_verdict") in ("SKIP", "KEEP_AS_IS", "DEFERRED_SPRINT_7")]

    all_pids = set()
    for e in ledger:
        for k in ("producer_id_a", "producer_id_b", "canonical_redirect_id"):
            v = e.get(k)
            if v:
                all_pids.add(v)

    conn = get_conn()
    try:
        details = fetch_producer_detail(conn, all_pids)
    finally:
        conn.close()

    L = []
    L.append("# Execution Plan (Narrative)")
    L.append("")
    L.append("This is a human-readable walkthrough of what the execute script will do.")
    L.append("The source of truth is `scripts/sprint6_step10_execute.py`; this document is")
    L.append("a review aid.")
    L.append("")
    L.append("## High-level")
    L.append("")
    L.append(f"- **{len(merges)} MERGEs** — absorb one producer row into another")
    L.append(f"- **{len(pcs)} PARENT_CHILDs** — set `parent_producer_id` on child row")
    L.append(f"- **{len(skips)} SKIPs / KEEP_AS_IS / DEFERREDs** — no action this sprint")
    L.append("")
    L.append("## Per-merge protocol")
    L.append("")
    L.append("For each MERGE, the execute script runs this transaction:")
    L.append("")
    L.append("```sql")
    L.append("BEGIN;")
    L.append("-- 1. Snapshot the loser row for reversibility")
    L.append("-- (recorded in producer_merge_history.merged_producer_json)")
    L.append("")
    L.append("-- 2. Re-point FKs for every table referencing producers.id")
    L.append("UPDATE wines SET producer_id = :survivor WHERE producer_id = :loser;")
    L.append("UPDATE source_ttb_colas SET canonical_producer_id = :survivor WHERE canonical_producer_id = :loser;")
    L.append("-- ... (every FK-referencing table; row IDs captured in repointed_rows JSONB)")
    L.append("")
    L.append("-- 3. Move alias (loser's name becomes an alias of survivor)")
    L.append("INSERT INTO producer_aliases (producer_id, name, name_normalized, source, alias_type)")
    L.append("VALUES (:survivor, :loser_name, lower(:loser_name), 'b6_6_merge', 'merged_from');")
    L.append("")
    L.append("-- 4. Record the merge event (reversible)")
    L.append("INSERT INTO producer_merge_history")
    L.append("  (merged_producer_id, survivor_producer_id, merged_producer_json,")
    L.append("   repointed_rows, method_name, reasoning, reviewed_by)")
    L.append("VALUES (:loser, :survivor, :snapshot, :repointed, 'B6.6 Chrome-validated',")
    L.append("        :reasoning, 'b6_6_chrome');")
    L.append("")
    L.append("-- 5. Soft-delete the loser")
    L.append("UPDATE producers SET deleted_at = NOW() WHERE id = :loser;")
    L.append("COMMIT;")
    L.append("```")
    L.append("")
    L.append("Each pair runs independently — one failure does not roll back the rest.")
    L.append("Chain merges are pre-resolved via union-find so the terminal survivor")
    L.append("receives all absorbed rows.")
    L.append("")
    L.append("## Per-PC protocol")
    L.append("")
    L.append("```sql")
    L.append("BEGIN;")
    L.append("UPDATE producers SET parent_producer_id = :parent WHERE id = :child;")
    L.append("INSERT INTO producer_merge_history -- records the PC as a non-merge history event")
    L.append("  (merged_producer_id, survivor_producer_id, merged_producer_json,")
    L.append("   repointed_rows, method_name, reasoning)")
    L.append("VALUES (:child, :parent, :child_snapshot, '{\"parent_child_link\": true}',")
    L.append("        'B6.6 Chrome-validated', :reasoning);")
    L.append("COMMIT;")
    L.append("```")
    L.append("")
    L.append("## MERGEs (detailed)")
    L.append("")
    L.append("| ledger_key | cluster | loser → survivor | loser wines | survivor wines | canonical redirect? |")
    L.append("|---|---|---|---|---|---|")
    for e in merges:
        pid_a = e.get("producer_id_a")
        pid_b = e.get("producer_id_b")
        canon = e.get("canonical_redirect_id")
        a = details.get(pid_a, {"name": e.get("name_a"), "wines": 0})
        b = details.get(pid_b, {"name": e.get("name_b"), "wines": 0})
        hint = e.get("final_survivor_name")

        if canon and canon not in (pid_a, pid_b):
            c = details.get(canon, {})
            L.append(f"| {e.get('ledger_key')} | {e.get('pattern_cluster')} | "
                     f"`{a.get('name', '?')}` + `{b.get('name', '?')}` → `{c.get('name', '?')}` | "
                     f"{a.get('wines', 0)} + {b.get('wines', 0)} | {c.get('wines', 0)} | "
                     f"**yes** (→{e.get('canonical_redirect_name')}) |")
        else:
            surv, loser = pick_survivor_heuristic(a, b, hint)
            redirect_note = "no"
            L.append(f"| {e.get('ledger_key')} | {e.get('pattern_cluster')} | "
                     f"`{loser.get('name', '?')}` → `{surv.get('name', '?')}` | "
                     f"{loser.get('wines', 0)} | {surv.get('wines', 0)} | {redirect_note} |")
    L.append("")

    L.append("## PARENT_CHILDs (detailed)")
    L.append("")
    L.append("| ledger_key | cluster | child → parent | child wines | parent wines |")
    L.append("|---|---|---|---|---|")
    for e in pcs:
        pid_a = e.get("producer_id_a")
        pid_b = e.get("producer_id_b")
        a = details.get(pid_a, {"name": e.get("name_a"), "wines": 0})
        b = details.get(pid_b, {"name": e.get("name_b"), "wines": 0})
        hint = e.get("final_parent_name")
        # Parent = name matching hint, else larger wines
        if hint and (a.get("name") or "").strip().lower() == hint.strip().lower():
            parent, child = a, b
        elif hint and (b.get("name") or "").strip().lower() == hint.strip().lower():
            parent, child = b, a
        elif a.get("wines", 0) >= b.get("wines", 0):
            parent, child = a, b
        else:
            parent, child = b, a
        L.append(f"| {e.get('ledger_key')} | {e.get('pattern_cluster')} | "
                 f"`{child.get('name', '?')}` → `{parent.get('name', '?')}` | "
                 f"{child.get('wines', 0)} | {parent.get('wines', 0)} |")
    L.append("")

    OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"Execution plan written: {OUT}")


if __name__ == "__main__":
    main()
