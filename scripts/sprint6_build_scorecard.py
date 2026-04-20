"""Build the execution_bundle scorecard from the final verdict ledger.

Reads: data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
Writes: data/sprints/dedup/execution_bundle/scorecard.md

The scorecard is the human-facing pre-execution sanity check:
- Aggregate counts per final_verdict, tier, pattern_cluster
- FK surface impact (how many rows re-pointed per staging table)
- Top 20 largest MERGEs by loser wine count
- Top 20 largest PCs by parent wine count
- Blocking / soft flags
- Chain-merge warnings
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
LEDGER = REPO / "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl"
OUT = REPO / "data/sprints/dedup/execution_bundle/scorecard.md"


def load_ledger():
    with LEDGER.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def fetch_producer_detail(conn, pids: set[str]) -> dict:
    if not pids:
        return {}
    pids_l = list(pids)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, name,
                   (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) AS wines
            FROM producers p WHERE p.id = ANY(%s::uuid[])
            """,
            (pids_l,),
        )
        return {r[0]: {"id": r[0], "name": r[1], "wines": r[2]} for r in cur.fetchall()}


def fk_surface_counts(conn, loser_ids: set[str]) -> Counter:
    if not loser_ids:
        return Counter()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND ccu.table_name = 'producers'
              AND ccu.column_name = 'id'
              AND tc.table_schema = 'public'
            """
        )
        fks = cur.fetchall()
    skip = {("producer_merge_history", "survivor_producer_id"),
            ("producer_merge_history", "merged_producer_id"),
            ("producer_dedup_pairs", "producer_id_a"),
            ("producer_dedup_pairs", "producer_id_b")}
    fks = [(t, c) for t, c in fks if (t, c) not in skip]
    counts = Counter()
    pids_l = list(loser_ids)
    with conn.cursor() as cur:
        for table, col in fks:
            cur.execute(
                f"SELECT COUNT(*) FROM public.{table} WHERE {col} = ANY(%s::uuid[])",
                (pids_l,),
            )
            n = cur.fetchone()[0]
            if n:
                counts[f"{table}.{col}"] = n
    return counts


def main():
    ledger = load_ledger()

    # Aggregate counts
    by_verdict = Counter(e.get("final_verdict") for e in ledger)
    by_tier = defaultdict(Counter)
    for e in ledger:
        by_tier[e.get("tier")][e.get("final_verdict")] += 1
    by_cluster = defaultdict(Counter)
    for e in ledger:
        by_cluster[e.get("pattern_cluster")][e.get("final_verdict")] += 1

    overrides = sum(1 for e in ledger if e.get("override_source"))
    flipped = Counter(e.get("override_action") for e in ledger if e.get("override_action") and e.get("override_action") != "KEEP")
    canonical_redirects = sum(1 for e in ledger if e.get("canonical_redirect_id"))

    merges = [e for e in ledger if e.get("final_verdict") == "MERGE"]
    pcs = [e for e in ledger if e.get("final_verdict") == "PARENT_CHILD"]

    # Collect producer IDs for DB lookups
    all_pids = set()
    for e in ledger:
        if e.get("producer_id_a"):
            all_pids.add(e["producer_id_a"])
        if e.get("producer_id_b"):
            all_pids.add(e["producer_id_b"])
        if e.get("canonical_redirect_id"):
            all_pids.add(e["canonical_redirect_id"])

    conn = get_conn()
    try:
        details = fetch_producer_detail(conn, all_pids)

        # Determine per-merge loser/survivor (heuristic: smaller wine count = loser, unless canonical redirect)
        merge_details = []
        for e in merges:
            pid_a = e.get("producer_id_a")
            pid_b = e.get("producer_id_b")
            canon = e.get("canonical_redirect_id")
            if canon and canon not in (pid_a, pid_b):
                # Both sides become losers
                for loser in (pid_a, pid_b):
                    if loser:
                        l = details.get(loser, {})
                        s = details.get(canon, {})
                        merge_details.append({
                            "entry": e, "loser_id": loser, "survivor_id": canon,
                            "loser_name": l.get("name"), "survivor_name": s.get("name"),
                            "loser_wines": l.get("wines", 0), "survivor_wines": s.get("wines", 0),
                            "via_canonical": True,
                        })
                continue
            a = details.get(pid_a, {})
            b = details.get(pid_b, {})
            if not a or not b:
                continue
            if a.get("wines", 0) >= b.get("wines", 0):
                surv, loser = a, b
            else:
                surv, loser = b, a
            merge_details.append({
                "entry": e, "loser_id": loser["id"], "survivor_id": surv["id"],
                "loser_name": loser["name"], "survivor_name": surv["name"],
                "loser_wines": loser.get("wines", 0), "survivor_wines": surv.get("wines", 0),
                "via_canonical": False,
            })

        loser_ids = {m["loser_id"] for m in merge_details}
        fk_counts = fk_surface_counts(conn, loser_ids)

        pc_details = []
        for e in pcs:
            pid_a = e.get("producer_id_a")
            pid_b = e.get("producer_id_b")
            a = details.get(pid_a, {})
            b = details.get(pid_b, {})
            if not a or not b:
                continue
            # Parent = higher wine count (heuristic for scorecard display only)
            if a.get("wines", 0) >= b.get("wines", 0):
                parent, child = a, b
            else:
                parent, child = b, a
            pc_details.append({
                "entry": e,
                "parent_id": parent["id"], "child_id": child["id"],
                "parent_name": parent["name"], "child_name": child["name"],
                "parent_wines": parent.get("wines", 0), "child_wines": child.get("wines", 0),
            })

    finally:
        conn.close()

    # Top 20 MERGEs
    top_merges = sorted(merge_details, key=lambda m: -m["loser_wines"])[:20]
    # Top 20 PCs
    top_pcs = sorted(pc_details, key=lambda p: -p["parent_wines"])[:20]

    # Total wines affected
    total_wines_repointed = sum(m["loser_wines"] for m in merge_details)
    # Deduplicate loser producers (some are loser in multiple pairs via canonical redirect)
    unique_losers = {m["loser_id"] for m in merge_details}

    # Build markdown
    L = []
    L.append("# Sprint 6 B6.6 — Pre-execution Scorecard")
    L.append("")
    L.append(f"Ledger entries: **{len(ledger)}**")
    L.append("")
    L.append("## Final verdict distribution")
    L.append("")
    L.append("| Verdict | Count |")
    L.append("|---|---|")
    for v in ("MERGE", "PARENT_CHILD", "SKIP", "KEEP_AS_IS", "DEFERRED_SPRINT_7"):
        L.append(f"| {v} | {by_verdict.get(v, 0)} |")
    L.append("")
    L.append(f"- Unique producers soft-deleted: **{len(unique_losers)}**")
    L.append(f"- Producers with parent_producer_id set: **{len(pc_details)}**")
    L.append(f"- Total wines re-pointed: **{total_wines_repointed:,}**")
    L.append(f"- B6.6 overrides applied: **{overrides}**")
    L.append(f"  - Breakdown: {dict(flipped)}")
    L.append(f"- Canonical-row redirects (existing DB row used as merge target): **{canonical_redirects}**")
    L.append("")

    L.append("## Per-tier breakdown")
    L.append("")
    L.append("| Tier | MERGE | PC | SKIP | KEEP_AS_IS | DEFERRED | Total |")
    L.append("|---|---|---|---|---|---|---|")
    for tier in ("yellow", "core", "mid", "tail"):
        c = by_tier.get(tier, Counter())
        total = sum(c.values())
        L.append(f"| {tier} | {c.get('MERGE', 0)} | {c.get('PARENT_CHILD', 0)} | {c.get('SKIP', 0)} | {c.get('KEEP_AS_IS', 0)} | {c.get('DEFERRED_SPRINT_7', 0)} | {total} |")
    L.append("")

    L.append("## Pattern-cluster breakdown")
    L.append("")
    L.append("| Cluster | MERGE | PC | SKIP | Total |")
    L.append("|---|---|---|---|---|")
    for cluster in sorted(by_cluster.keys(), key=lambda x: (x is None, str(x))):
        c = by_cluster[cluster]
        total = sum(c.values())
        L.append(f"| {cluster or '(none)'} | {c.get('MERGE', 0)} | {c.get('PARENT_CHILD', 0)} | {c.get('SKIP', 0)} | {total} |")
    L.append("")

    L.append("## FK surface impact (rows to re-point)")
    L.append("")
    L.append("| Table.Column | Rows |")
    L.append("|---|---|")
    for k, v in sorted(fk_counts.items(), key=lambda kv: -kv[1]):
        L.append(f"| {k} | {v:,} |")
    L.append("")

    L.append("## Top 20 largest MERGEs (by loser wine count)")
    L.append("")
    L.append("| ledger_key | cluster | loser → survivor | loser wines | survivor wines | via canonical? |")
    L.append("|---|---|---|---|---|---|")
    for m in top_merges:
        e = m["entry"]
        L.append(f"| {e.get('ledger_key')} | {e.get('pattern_cluster')} | "
                 f"{m['loser_name']} → {m['survivor_name']} | "
                 f"{m['loser_wines']} | {m['survivor_wines']} | "
                 f"{'yes' if m['via_canonical'] else 'no'} |")
    L.append("")

    L.append("## Top 20 largest PARENT_CHILDs (by parent wine count)")
    L.append("")
    L.append("| ledger_key | cluster | child → parent | child wines | parent wines |")
    L.append("|---|---|---|---|---|")
    for p in top_pcs:
        e = p["entry"]
        L.append(f"| {e.get('ledger_key')} | {e.get('pattern_cluster')} | "
                 f"{p['child_name']} → {p['parent_name']} | "
                 f"{p['child_wines']} | {p['parent_wines']} |")
    L.append("")

    L.append("## Flags")
    L.append("")
    sprint7_entries = [e for e in ledger if e.get("sprint7_flag") or e.get("final_verdict") == "DEFERRED_SPRINT_7"]
    L.append(f"- Entries flagged for Sprint 7 follow-up: **{len(sprint7_entries)}**")
    if sprint7_entries:
        for e in sprint7_entries[:20]:
            flag = e.get("sprint7_flag") or "deferred"
            L.append(f"  - {e.get('ledger_key')}: {flag}")
    L.append("")

    OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"Scorecard written: {OUT}")
    print(f"  MERGE: {len(merge_details)}  PC: {len(pc_details)}")
    print(f"  Unique losers: {len(unique_losers)}   Wines re-pointed: {total_wines_repointed:,}")
    print(f"  Overrides: {overrides}   Canonical redirects: {canonical_redirects}")


if __name__ == "__main__":
    main()
