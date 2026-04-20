"""B6.6 Mid+Tail+Yellow re-Chrome: prep pair context bundles for subagent.

Reads mid/tail/yellow verdict JSONL files, filters to MERGE+PC, joins DB wines,
and writes a single context file.

Output: data/sprints/dedup/chrome_validation/_rechrome_rest_context.jsonl
"""
import json
from pathlib import Path
from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
CHROME = REPO / "data/sprints/dedup/chrome_validation"
OUT = CHROME / "_rechrome_rest_context.jsonl"


def load_nonyellow_targets():
    targets = []
    for fname, tier in (("mid_verdicts.jsonl", "mid"), ("tail_verdicts.jsonl", "tail")):
        with (CHROME / fname).open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    v = json.loads(line)
                    if v.get("verdict") in ("MERGE", "PARENT_CHILD"):
                        targets.append({
                            "pair_id": v["pair_id"],
                            "tier": tier,
                            "verdict_original": v["verdict"],
                            "pattern_cluster": v.get("pattern_cluster"),
                            "reasoning_original": v.get("reasoning"),
                            "survivor_name": v.get("survivor_name"),
                            "parent_name": v.get("parent_name"),
                            "name_a": v.get("name_a"),
                            "name_b": v.get("name_b"),
                        })
    return targets


def load_yellow_targets():
    targets = []
    with (CHROME / "yellow_verdicts.jsonl").open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                v = json.loads(line)
                if v.get("verdict") in ("MERGE", "PARENT_CHILD"):
                    targets.append({
                        "tier": "yellow",
                        "yellow_idx": v.get("idx"),
                        "verdict_original": v["verdict"],
                        "pattern_cluster": v.get("pattern_cluster"),
                        "reasoning_original": v.get("reasoning"),
                        "survivor_name": v.get("survivor_name"),
                        "parent_side": v.get("parent_side"),
                        "name": v.get("name"),
                        "sibling_name": v.get("sibling_name"),
                        "producer_id": v.get("producer_id"),
                        "merge_target_id": v.get("merge_target_id"),
                        "merge_target_name": v.get("merge_target_name"),
                        "merge_source_id": v.get("merge_source_id"),
                        "merge_source_name": v.get("merge_source_name"),
                        "parent_id": v.get("parent_id"),
                        "sibling_id": v.get("sibling_id"),
                        "unflagged_additional_merges": v.get("unflagged_additional_merges"),
                    })
    return targets


def main():
    mid_tail = load_nonyellow_targets()
    yellow = load_yellow_targets()
    pair_ids = [t["pair_id"] for t in mid_tail]

    # Collect all producer UUIDs referenced
    conn = get_conn()
    with conn.cursor() as cur:
        # Pair-based rows
        cur.execute(
            """
            SELECT d.id, d.name_a, d.name_b, d.country,
                   d.producer_id_a::text, d.producer_id_b::text
            FROM producer_dedup_pairs d
            WHERE d.id = ANY(%s)
            """,
            (pair_ids,),
        )
        pair_rows = {r[0]: {"pair_id": r[0], "name_a": r[1], "name_b": r[2],
                             "country": r[3], "pid_a": r[4], "pid_b": r[5]}
                      for r in cur.fetchall()}

        # Build union of all UUIDs
        all_pids = set()
        for pr in pair_rows.values():
            if pr["pid_a"]:
                all_pids.add(pr["pid_a"])
            if pr["pid_b"]:
                all_pids.add(pr["pid_b"])
        # Yellow UUIDs
        import re
        UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
        for y in yellow:
            for k in ("producer_id", "merge_target_id", "merge_source_id", "parent_id", "sibling_id"):
                v = y.get(k)
                if isinstance(v, str) and UUID_RE.match(v):
                    all_pids.add(v)
        pids_list = list(all_pids)

        # Wines
        cur.execute(
            """
            SELECT producer_id::text, COALESCE(display_name, name)
            FROM wines WHERE producer_id = ANY(%s::uuid[])
            ORDER BY producer_id, COALESCE(display_name, name)
            """,
            (pids_list,),
        )
        wines_by_pid = {}
        for pid, wn in cur.fetchall():
            wines_by_pid.setdefault(pid, []).append(wn)

        # Producer metadata
        cur.execute(
            "SELECT id::text, name, website_url FROM producers WHERE id = ANY(%s::uuid[])",
            (pids_list,),
        )
        prod_meta = {r[0]: {"name": r[1], "website": r[2]} for r in cur.fetchall()}

    conn.close()

    written = 0
    with OUT.open("w", encoding="utf-8") as f:
        # Non-yellow
        for t in mid_tail:
            pr = pair_rows.get(t["pair_id"])
            if not pr:
                continue
            ctx = {
                "ledger_key": f"{t['tier']}#{t['pair_id']}",
                "pair_id": t["pair_id"],
                "tier": t["tier"],
                "verdict_original": t["verdict_original"],
                "pattern_cluster": t["pattern_cluster"],
                "reasoning_original": t["reasoning_original"],
                "survivor_name": t.get("survivor_name"),
                "parent_name": t.get("parent_name"),
                "country": pr["country"],
                "side_a": {
                    "producer_id": pr["pid_a"], "name": pr["name_a"],
                    "website": (prod_meta.get(pr["pid_a"]) or {}).get("website"),
                    "wine_count": len(wines_by_pid.get(pr["pid_a"], [])),
                    "wines": wines_by_pid.get(pr["pid_a"], [])[:30],
                },
                "side_b": {
                    "producer_id": pr["pid_b"], "name": pr["name_b"],
                    "website": (prod_meta.get(pr["pid_b"]) or {}).get("website"),
                    "wine_count": len(wines_by_pid.get(pr["pid_b"], [])),
                    "wines": wines_by_pid.get(pr["pid_b"], [])[:30],
                },
            }
            f.write(json.dumps(ctx, ensure_ascii=False) + "\n")
            written += 1

        # Yellow
        for y in yellow:
            pid = y.get("producer_id") if UUID_RE.match(str(y.get("producer_id") or "")) else None
            side = {"yellow_idx": y.get("yellow_idx"), "name": y.get("name"),
                    "sibling_name": y.get("sibling_name"),
                    "producer_id": pid,
                    "wines": wines_by_pid.get(pid, [])[:30] if pid else [],
                    "wine_count": len(wines_by_pid.get(pid, [])) if pid else 0,
                    "merge_target_id": y.get("merge_target_id"),
                    "merge_target_name": y.get("merge_target_name"),
                    "merge_source_id": y.get("merge_source_id"),
                    "merge_source_name": y.get("merge_source_name"),
                    "parent_id": y.get("parent_id"),
                    "sibling_id": y.get("sibling_id"),
                    "unflagged_additional_merges": y.get("unflagged_additional_merges"),
                    }
            ctx = {
                "ledger_key": f"yellow#{y['yellow_idx']}",
                "tier": "yellow",
                "yellow_idx": y.get("yellow_idx"),
                "verdict_original": y["verdict_original"],
                "pattern_cluster": y.get("pattern_cluster"),
                "reasoning_original": y.get("reasoning_original"),
                "survivor_name": y.get("survivor_name"),
                "yellow_payload": side,
            }
            f.write(json.dumps(ctx, ensure_ascii=False) + "\n")
            written += 1

    print(f"Wrote {written} records to {OUT}")
    print(f"  Mid+Tail MERGE+PC: {len(mid_tail)}")
    print(f"  Yellow MERGE+PC:   {len(yellow)}")


if __name__ == "__main__":
    main()
