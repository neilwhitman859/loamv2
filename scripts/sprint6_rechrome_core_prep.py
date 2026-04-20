"""B6.6 Core MERGE+PC re-Chrome: prep pair context bundles.

Reads _rechrome_core_targets.json (66 pairs) and joins DB producer/wine data
so each pair has a compact context bundle for Chrome verification.

Output: data/sprints/dedup/chrome_validation/_rechrome_core_context.jsonl
"""
import json
from pathlib import Path
from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
TARGETS = REPO / "data/sprints/dedup/chrome_validation/_rechrome_core_targets.json"
OUT = REPO / "data/sprints/dedup/chrome_validation/_rechrome_core_context.jsonl"


def main():
    with TARGETS.open(encoding="utf-8") as f:
        targets = json.load(f)
    pair_ids = [t["pair_id"] for t in targets]

    conn = get_conn()
    with conn.cursor() as cur:
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

        # Wine lists
        all_pids = set()
        for pr in pair_rows.values():
            if pr["pid_a"]:
                all_pids.add(pr["pid_a"])
            if pr["pid_b"]:
                all_pids.add(pr["pid_b"])
        pids_list = list(all_pids)

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
            """
            SELECT id::text, name, website_url, slug
            FROM producers WHERE id = ANY(%s::uuid[])
            """,
            (pids_list,),
        )
        producer_meta = {r[0]: {"name": r[1], "website": r[2], "slug": r[3]}
                          for r in cur.fetchall()}

    conn.close()

    by_id = {t["pair_id"]: t for t in targets}

    with OUT.open("w", encoding="utf-8") as f:
        for pid in pair_ids:
            if pid not in pair_rows:
                continue
            pr = pair_rows[pid]
            t = by_id[pid]
            ctx = {
                "pair_id": pid,
                "verdict_original": t["verdict"],
                "pattern_cluster": t["cluster"],
                "reasoning_original": t["reasoning"],
                "survivor_name": t.get("survivor_name"),
                "parent_name": t.get("parent_name"),
                "country": pr["country"],
                "side_a": {
                    "producer_id": pr["pid_a"],
                    "name": pr["name_a"],
                    "website": (producer_meta.get(pr["pid_a"]) or {}).get("website"),
                    "wine_count": len(wines_by_pid.get(pr["pid_a"], [])),
                    "wines": wines_by_pid.get(pr["pid_a"], [])[:40],
                },
                "side_b": {
                    "producer_id": pr["pid_b"],
                    "name": pr["name_b"],
                    "website": (producer_meta.get(pr["pid_b"]) or {}).get("website"),
                    "wine_count": len(wines_by_pid.get(pr["pid_b"], [])),
                    "wines": wines_by_pid.get(pr["pid_b"], [])[:40],
                },
            }
            f.write(json.dumps(ctx, ensure_ascii=False) + "\n")

    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
