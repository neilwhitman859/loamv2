"""
B6.4 — Sync tier verdicts from producer_dedup_pairs → calibration_set.json.

The calibration_set.json is built once (Phase A) then enriched by oracle.
As subsequent tiers (L1.5 Gemini basic, L2 Haiku rich, L2.5 Gemini rich, L3
Sonnet+web) run to producer_dedup_pairs, this script mirrors their verdicts
into the JSON under tier-specific keys so `calibration_analysis.py` can score
each tier against gold.

Key mapping:
  DB method_name                     JSON keys
  -----------------                  ---------
  l1_haiku_batch                     l1_verdict / l1_confidence / l1_reasoning
  l1_gemini_basic_calibration        l1_gemini_basic_verdict / _confidence / _reasoning
  l2_haiku_rich                      l2_haiku_rich_verdict / _confidence / _reasoning
  l2_gemini_rich_calibration         l2_gemini_rich_verdict / _confidence / _reasoning
  l3_sonnet_web_calibration          l3_sonnet_web_verdict / _confidence / _reasoning

Run:
    python -m pipeline.identity.sync_calibration
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pipeline.lib.db import get_conn


CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"

METHOD_TO_PREFIX = {
    "l1_haiku_batch":                "l1",
    "l1_gemini_basic_calibration":   "l1_gemini_basic",
    "l1_gemini_basic":               "l1_gemini_basic",
    "l2_haiku_rich":                 "l2_haiku_rich",
    "l2_gemini_rich_calibration":    "l2_gemini_rich",
    "l2_gemini_rich":                "l2_gemini_rich",
    "l3_sonnet_web_calibration":     "l3_sonnet_web",
    "l3_sonnet_web":                 "l3_sonnet_web",
}


def main() -> int:
    if not CALIBRATION_PATH.exists():
        print(f"Missing {CALIBRATION_PATH}", file=sys.stderr)
        return 1

    doc = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))

    # Build pair_id -> pair map
    pair_by_id: dict[int, dict] = {}
    for tier, pairs in doc["tiers"].items():
        for p in pairs:
            pair_by_id[p["pair_id"]] = p

    conn = get_conn()
    cur = conn.cursor()

    all_pair_ids = list(pair_by_id.keys())
    placeholders = ",".join(["%s"] * len(all_pair_ids))

    # First look up blocking rows matching our pair ids, to get their producer ids
    cur.execute(f"""
        SELECT id, producer_id_a, producer_id_b
        FROM producer_dedup_pairs
        WHERE method_name='blocking' AND id IN ({placeholders})
    """, all_pair_ids)
    pair_id_to_producers = {}
    for pid, pa, pb in cur.fetchall():
        pair_id_to_producers[pid] = (str(pa), str(pb))

    producer_pair_to_id = {v: k for k, v in pair_id_to_producers.items()}

    # For each known method_name, pull the verdicts
    updated = 0
    for method_name, prefix in METHOD_TO_PREFIX.items():
        cur.execute(f"""
            SELECT producer_id_a, producer_id_b, verdict, confidence, reasoning
            FROM producer_dedup_pairs
            WHERE method_name=%s
        """, (method_name,))
        rows = cur.fetchall()
        for pa, pb, v, conf, rsn in rows:
            key = (str(pa), str(pb))
            pid = producer_pair_to_id.get(key)
            if not pid:
                continue
            pair = pair_by_id.get(pid)
            if not pair:
                continue
            vkey = f"{prefix}_verdict"
            ckey = f"{prefix}_confidence"
            rkey = f"{prefix}_reasoning"
            # Prefer calibration-specific method_name if multiple exist; rich/basic
            # already distinguished by prefix, no shadowing
            if pair.get(vkey) != v or pair.get(ckey) != (float(conf) if conf is not None else None):
                pair[vkey] = v
                pair[ckey] = float(conf) if conf is not None else None
                pair[rkey] = rsn
                updated += 1

    CALIBRATION_PATH.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    print(f"Updated {updated} fields in {CALIBRATION_PATH}")

    # Quick report
    for prefix in sorted(set(METHOD_TO_PREFIX.values())):
        vkey = f"{prefix}_verdict"
        n = sum(1 for p in pair_by_id.values() if p.get(vkey))
        if n:
            print(f"  {prefix}: {n}/{len(pair_by_id)} pairs have verdicts")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
