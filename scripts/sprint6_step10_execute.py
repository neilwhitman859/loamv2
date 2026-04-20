"""Sprint 6 Step 10 — apply frozen verdict ledger to the DB.

DRY-RUN BY DEFAULT. Pass --execute to actually mutate the DB.

Reads: data/sprints/dedup/execution_bundle/verdict_ledger.jsonl

For each ledger entry:
  final_verdict = MERGE           → re-point FKs, write merge_history, soft-delete loser
  final_verdict = PARENT_CHILD    → set parent_producer_id on child, write merge_history
  final_verdict = SKIP            → no-op
  final_verdict = KEEP_AS_IS      → no-op
  final_verdict = DEFERRED_SPRINT_7 → no-op (flagged for Sprint 7)

Chain merges are resolved via union-find: if A→B and B→C both MERGE, A ends up
pointing at C.

Canonical-row redirect: if an entry has `canonical_redirect_id`, BOTH sides of
the pair become losers merged into the canonical. Creates two merge rows.

Usage:
    python scripts/sprint6_step10_execute.py
    python scripts/sprint6_step10_execute.py --execute
    python scripts/sprint6_step10_execute.py --limit 5 --execute
    python scripts/sprint6_step10_execute.py --only-tier core --execute
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
LEDGER = REPO / "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl"
METHOD_NAME = "B6.6 Chrome-validated"


def load_ledger():
    with LEDGER.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def enumerate_fk_columns(conn) -> list[tuple[str, str, bool]]:
    """Return [(table, column, has_id_pk)] for tables FK-referencing producers.id."""
    with conn.cursor() as cur:
        cur.execute("""
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
        """)
        fks = cur.fetchall()
        # Check which tables have an `id` column
        cur.execute("""
            SELECT table_name FROM information_schema.columns
            WHERE table_schema = 'public' AND column_name = 'id'
        """)
        has_id = {r[0] for r in cur.fetchall()}
    skip = {
        ("producer_merge_history", "survivor_producer_id"),
        ("producer_merge_history", "merged_producer_id"),
        ("producers", "parent_producer_id"),
        ("producer_dedup_pairs", "producer_id_a"),
        ("producer_dedup_pairs", "producer_id_b"),
    }
    return [(t, c, t in has_id) for (t, c) in fks if (t, c) not in skip]


def resolve_merge_chains(ledger: list[dict]) -> dict[str, str]:
    """Union-find for MERGE verdicts. Returns loser_id → terminal_survivor_id."""
    direct: dict[str, str] = {}
    for e in ledger:
        if e.get("final_verdict") != "MERGE":
            continue
        loser, surv = _pick_merge_loser_survivor(e)
        if loser and surv and loser != surv:
            direct[loser] = surv

    def terminal(pid, seen):
        if pid not in direct or pid in seen:
            return pid
        seen.add(pid)
        return terminal(direct[pid], seen)

    return {l: terminal(l, set()) for l in direct}


def _pick_merge_loser_survivor(e: dict) -> tuple[str | None, str | None]:
    """Return (loser_id, survivor_id) for a MERGE entry, honoring canonical_redirect."""
    pid_a = e.get("producer_id_a")
    pid_b = e.get("producer_id_b")
    canon = e.get("canonical_redirect_id")
    if canon and pid_a and pid_b and canon not in (pid_a, pid_b):
        # Both sides become losers merged into canonical
        return pid_a, canon  # Caller must additionally emit pid_b → canon
    # §11.6 survivor selection: prefer row with higher wine count (proxy used here)
    if not pid_a or not pid_b:
        return None, None
    # Actual §11.6 logic requires row details; we use a DB lookup in execute path
    # For chain resolution, use a simple heuristic: the survivor_name's row wins
    return pid_b, pid_a  # placeholder; actual logic in execute path uses DB detail


def fetch_row_snapshot(conn, producer_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_jsonb(p) FROM producers p WHERE p.id = %s::uuid
            """,
            (producer_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def pick_survivor_detailed(conn, pid_a: str, pid_b: str, survivor_name_hint: str | None) -> tuple[str, str]:
    """§11.6 survivor-selection against live DB data. Returns (survivor_id, loser_id)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, name, created_at,
                   (CASE WHEN website_url IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN year_established IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN hectares_under_vine IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN region_id IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) AS metadata_count,
                   (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) AS wine_count
            FROM producers p WHERE p.id = ANY(%s::uuid[])
            """,
            ([pid_a, pid_b],),
        )
        rows = {r[0]: {"id": r[0], "name": r[1], "created_at": r[2],
                        "metadata_count": r[3], "wine_count": r[4]} for r in cur.fetchall()}
    a, b = rows.get(pid_a), rows.get(pid_b)
    if not a or not b:
        return (pid_a, pid_b) if a else (pid_b, pid_a)

    # Survivor-name hint: if the hint matches one row, that row wins
    if survivor_name_hint:
        hint = survivor_name_hint.strip().lower()
        if a["name"].strip().lower() == hint:
            return a["id"], b["id"]
        if b["name"].strip().lower() == hint:
            return b["id"], a["id"]

    # §11.6 tie-breakers: non-ASCII > metadata > wine_count > created_at
    a_non_ascii = any(ord(c) > 127 for c in a["name"])
    b_non_ascii = any(ord(c) > 127 for c in b["name"])
    if a_non_ascii != b_non_ascii:
        return (a["id"], b["id"]) if a_non_ascii else (b["id"], a["id"])
    if a["metadata_count"] != b["metadata_count"]:
        return (a["id"], b["id"]) if a["metadata_count"] > b["metadata_count"] else (b["id"], a["id"])
    if a["wine_count"] != b["wine_count"]:
        return (a["id"], b["id"]) if a["wine_count"] > b["wine_count"] else (b["id"], a["id"])
    return (a["id"], b["id"]) if (a["created_at"] or 0) <= (b["created_at"] or 0) else (b["id"], a["id"])


def apply_merge(conn, loser_id: str, survivor_id: str, fk_cols: list[tuple[str, str]],
                ledger_entry: dict, execute: bool = False) -> dict:
    """Apply a single MERGE. Returns a report dict."""
    report = {"ledger_key": ledger_entry.get("ledger_key"), "loser": loser_id,
              "survivor": survivor_id, "repointed": {}, "applied": False, "error": None}
    snapshot = fetch_row_snapshot(conn, loser_id)
    if not snapshot:
        report["error"] = "loser_not_found"
        return report

    try:
        with conn.cursor() as cur:
            if execute:
                cur.execute("BEGIN")
            # For each FK table+column, collect row IDs (if possible) and re-point
            repointed_ids = defaultdict(list)
            repointed_counts = {}
            for item in fk_cols:
                table, col, has_id = item if len(item) == 3 else (*item, False)
                if has_id:
                    cur.execute(f"SELECT id::text FROM public.{table} WHERE {col} = %s::uuid", (loser_id,))
                    ids = [r[0] for r in cur.fetchall()]
                    if ids:
                        repointed_ids[f"{table}.{col}"] = ids
                        if execute:
                            cur.execute(
                                f"UPDATE public.{table} SET {col} = %s::uuid WHERE {col} = %s::uuid",
                                (survivor_id, loser_id),
                            )
                else:
                    # Table without id column — track count only
                    cur.execute(f"SELECT COUNT(*) FROM public.{table} WHERE {col} = %s::uuid", (loser_id,))
                    n = cur.fetchone()[0]
                    if n:
                        repointed_counts[f"{table}.{col}"] = n
                        if execute:
                            cur.execute(
                                f"UPDATE public.{table} SET {col} = %s::uuid WHERE {col} = %s::uuid",
                                (survivor_id, loser_id),
                            )
            # Producer-level parent_producer_id self-ref: any row pointing parent_producer_id at loser
            cur.execute("SELECT id::text FROM producers WHERE parent_producer_id = %s::uuid", (loser_id,))
            pp_ids = [r[0] for r in cur.fetchall()]
            if pp_ids:
                repointed_ids["producers.parent_producer_id"] = pp_ids
                if execute:
                    cur.execute(
                        "UPDATE producers SET parent_producer_id = %s::uuid WHERE parent_producer_id = %s::uuid",
                        (survivor_id, loser_id),
                    )

            if execute:
                # Insert producer_aliases row for the loser's name
                loser_name = snapshot.get("name")
                if loser_name:
                    cur.execute(
                        """
                        INSERT INTO producer_aliases (producer_id, name, name_normalized, source, alias_type)
                        VALUES (%s::uuid, %s, lower(%s), 'b6_6_merge', 'merged_from')
                        ON CONFLICT DO NOTHING
                        """,
                        (survivor_id, loser_name, loser_name),
                    )
                # Insert producer_merge_history row
                repointed_summary = {"with_ids": dict(repointed_ids), "counts_only": dict(repointed_counts)}
                cur.execute(
                    """
                    INSERT INTO producer_merge_history
                        (merged_producer_id, survivor_producer_id, merged_producer_json,
                         repointed_rows, method_name, reasoning, reviewed_by)
                    VALUES (%s::uuid, %s::uuid, %s::jsonb, %s::jsonb, %s, %s, 'b6_6_chrome')
                    """,
                    (loser_id, survivor_id, json.dumps(snapshot),
                     json.dumps(repointed_summary), METHOD_NAME,
                     (ledger_entry.get("override_reasoning") or ledger_entry.get("original_reasoning") or "")[:1000])
                )
                # Soft-delete loser
                cur.execute("UPDATE producers SET deleted_at = NOW() WHERE id = %s::uuid", (loser_id,))
                cur.execute("COMMIT")
            report["applied"] = execute
            report["repointed"] = {**{k: len(v) for k, v in repointed_ids.items()},
                                    **repointed_counts}
    except Exception as e:
        if execute:
            conn.rollback()
        report["error"] = str(e)
    return report


def apply_parent_child(conn, parent_id: str, child_id: str, ledger_entry: dict,
                       execute: bool = False) -> dict:
    """Apply a single PARENT_CHILD. Returns a report dict."""
    report = {"ledger_key": ledger_entry.get("ledger_key"), "parent": parent_id,
              "child": child_id, "applied": False, "error": None}
    snapshot = fetch_row_snapshot(conn, child_id)
    if not snapshot:
        report["error"] = "child_not_found"
        return report
    try:
        with conn.cursor() as cur:
            if execute:
                cur.execute("BEGIN")
                cur.execute(
                    "UPDATE producers SET parent_producer_id = %s::uuid WHERE id = %s::uuid",
                    (parent_id, child_id),
                )
                cur.execute(
                    """
                    INSERT INTO producer_merge_history
                        (merged_producer_id, survivor_producer_id, merged_producer_json,
                         repointed_rows, method_name, reasoning, reviewed_by)
                    VALUES (%s::uuid, %s::uuid, %s::jsonb, %s::jsonb, %s, %s, 'b6_6_chrome')
                    """,
                    (child_id, parent_id, json.dumps(snapshot),
                     json.dumps({"parent_child_link": True}), METHOD_NAME,
                     ("[PC] " + (ledger_entry.get("override_reasoning") or
                                 ledger_entry.get("original_reasoning") or ""))[:1000])
                )
                cur.execute("COMMIT")
            report["applied"] = execute
    except Exception as e:
        if execute:
            conn.rollback()
        report["error"] = str(e)
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true",
                    help="Actually mutate the DB. Default is dry-run.")
    ap.add_argument("--ledger", type=Path, default=LEDGER,
                    help="Path to verdict_ledger.jsonl")
    ap.add_argument("--limit", type=int, help="Limit to first N MERGE+PC entries (for testing)")
    ap.add_argument("--only-tier", choices=("yellow", "core", "mid", "tail"),
                    help="Restrict to one tier")
    args = ap.parse_args()

    if not args.ledger.exists():
        print(f"ERROR: ledger not found at {args.ledger}")
        return 1

    with args.ledger.open(encoding="utf-8") as f:
        ledger = [json.loads(line) for line in f if line.strip()]
    print(f"Loaded {len(ledger)} ledger entries from {args.ledger}")

    if args.only_tier:
        ledger = [e for e in ledger if e.get("tier") == args.only_tier]
        print(f"  Filtered to tier={args.only_tier}: {len(ledger)}")

    merges = [e for e in ledger if e.get("final_verdict") == "MERGE"]
    pcs = [e for e in ledger if e.get("final_verdict") == "PARENT_CHILD"]
    skips = [e for e in ledger if e.get("final_verdict") in ("SKIP", "KEEP_AS_IS", "DEFERRED_SPRINT_7")]
    print(f"  MERGE: {len(merges)}  PC: {len(pcs)}  SKIP/KEEP/DEFERRED: {len(skips)}")

    if args.limit:
        merges = merges[:args.limit]
        pcs = pcs[:args.limit]
        print(f"  --limit={args.limit}: trimmed")

    if not args.execute:
        print("\n[DRY-RUN MODE — no DB writes. Pass --execute to apply.]")

    conn = get_conn()
    conn.autocommit = True  # Use explicit BEGIN/COMMIT only for mutations
    fk_cols = enumerate_fk_columns(conn)
    print(f"  FK columns to re-point: {len(fk_cols)}")

    # Pre-resolve survivor/loser per merge entry using §11.6 logic + canonical redirect
    merge_actions: list[dict] = []
    for e in merges:
        pid_a = e.get("producer_id_a")
        pid_b = e.get("producer_id_b")
        if not pid_a or not pid_b:
            continue
        canon = e.get("canonical_redirect_id")
        survivor_name = e.get("final_survivor_name") or e.get("canonical_redirect_name")
        if canon and canon not in (pid_a, pid_b):
            # Both pair sides become losers merged into canonical
            merge_actions.append({"entry": e, "loser": pid_a, "survivor": canon, "reason": "canonical_redirect"})
            merge_actions.append({"entry": e, "loser": pid_b, "survivor": canon, "reason": "canonical_redirect"})
        else:
            surv, loser = pick_survivor_detailed(conn, pid_a, pid_b, survivor_name)
            merge_actions.append({"entry": e, "loser": loser, "survivor": surv, "reason": "§11.6_pick"})

    # Resolve chain merges
    direct = {a["loser"]: a["survivor"] for a in merge_actions if a["loser"] != a["survivor"]}
    def terminal(pid, seen=None):
        if seen is None:
            seen = set()
        if pid not in direct or pid in seen:
            return pid
        seen.add(pid)
        return terminal(direct[pid], seen)
    # Rewrite actions to use terminal survivors
    for a in merge_actions:
        a["survivor_terminal"] = terminal(a["survivor"])

    # Execute MERGEs
    merge_reports = []
    for a in merge_actions:
        r = apply_merge(conn, a["loser"], a["survivor_terminal"], fk_cols, a["entry"], execute=args.execute)
        r["chain_intermediate"] = a["survivor"] if a["survivor"] != a["survivor_terminal"] else None
        r["reason"] = a["reason"]
        merge_reports.append(r)

    # Execute PCs
    pc_reports = []
    for e in pcs:
        pid_a = e.get("producer_id_a")
        pid_b = e.get("producer_id_b")
        parent_hint = (e.get("final_parent_name") or "").strip().lower()
        if not pid_a or not pid_b:
            continue
        # Determine parent_id by matching parent_hint against both names
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id::text, name FROM producers WHERE id = ANY(%s::uuid[])",
                ([pid_a, pid_b],),
            )
            rows = {r[0]: r[1] for r in cur.fetchall()}
        parent_id = None
        if parent_hint:
            for pid, n in rows.items():
                if n.strip().lower() == parent_hint:
                    parent_id = pid
                    break
        if not parent_id and e.get("canonical_redirect_id"):
            parent_id = e["canonical_redirect_id"]
        if not parent_id:
            # Fallback: larger wine count is parent
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id::text FROM producers p
                    WHERE p.id = ANY(%s::uuid[])
                    ORDER BY (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) DESC
                    LIMIT 1
                    """,
                    ([pid_a, pid_b],),
                )
                row = cur.fetchone()
                parent_id = row[0] if row else None
        child_id = pid_b if parent_id == pid_a else pid_a
        if not parent_id or not child_id:
            continue
        r = apply_parent_child(conn, parent_id, child_id, e, execute=args.execute)
        pc_reports.append(r)

    # Print summary
    print()
    print(f"=== Summary ({'EXECUTED' if args.execute else 'DRY-RUN'}) ===")
    print(f"  MERGE actions prepared: {len(merge_actions)}")
    applied_merges = sum(1 for r in merge_reports if r.get("applied"))
    merge_errors = sum(1 for r in merge_reports if r.get("error"))
    print(f"    applied: {applied_merges}   errors: {merge_errors}")
    applied_pcs = sum(1 for r in pc_reports if r.get("applied"))
    pc_errors = sum(1 for r in pc_reports if r.get("error"))
    print(f"  PC actions:  {len(pc_reports)}  applied: {applied_pcs}  errors: {pc_errors}")

    if merge_errors:
        print("\nMERGE errors:")
        for r in merge_reports:
            if r.get("error"):
                print(f"  {r['ledger_key']}: {r['error']}")
    if pc_errors:
        print("\nPC errors:")
        for r in pc_reports:
            if r.get("error"):
                print(f"  {r['ledger_key']}: {r['error']}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
