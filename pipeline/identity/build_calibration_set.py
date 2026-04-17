"""
B6.4 Phase A — Build calibration set for tier-threshold tuning.

Pulls 600 pairs from producer_dedup_pairs stratified into 5 tiers:

  T1 (100): Near-certain MERGE — blocking signals >= 3 OR shared >=5 LWIN_7.
            Proxy gold = MERGE (based on blocking structure, not L1).
  T2 (100): Near-certain SKIP — s2_trigram only, 0.35-0.45, L1 SKIP >= 0.97,
            no other signals. Proxy gold = SKIP.
  T3 (100): Borderline MERGE/PC — s6_ttb_permit shared + L1 MERGE/PC 0.80-0.92.
            Needs oracle.
  T4 (100): Borderline SKIP — similarity >= 0.5 + L1 SKIP 0.88-0.95. Needs oracle.
  T5 (200): L3-oracle random sample — stratified by L1 verdict x confidence band.
            Needs oracle.

Output: data/sprints/dedup/calibration_set.json

Run:
    python -m pipeline.identity.build_calibration_set --execute
    python -m pipeline.identity.build_calibration_set --execute --force  # overwrite
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline.lib.db import get_conn


OUTPUT_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"

PLAN = {
    "T1": 100,
    "T2": 100,
    "T3": 100,
    "T4": 100,
    "T5": 200,
}


def _pair_row_to_dict(row: tuple) -> dict:
    """Convert SELECT b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
                    b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
                    l.verdict, l.confidence, l.reasoning
       into a dict."""
    return {
        "pair_id": row[0],
        "producer_id_a": str(row[1]),
        "producer_id_b": str(row[2]),
        "name_a": row[3],
        "name_b": row[4],
        "country": row[5],
        "similarity": float(row[6]) if row[6] is not None else None,
        "wines_a": row[7],
        "wines_b": row[8],
        "signals": row[9],
        "l1_verdict": row[10],
        "l1_confidence": float(row[11]) if row[11] is not None else None,
        "l1_reasoning": row[12],
    }


SELECT_COLS = """
  b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
  b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
  l.verdict, l.confidence, l.reasoning
"""

JOIN_CLAUSE = """
  FROM producer_dedup_pairs b
  LEFT JOIN producer_dedup_pairs l
    ON l.producer_id_a = b.producer_id_a
   AND l.producer_id_b = b.producer_id_b
   AND l.method_name = 'l1_haiku_batch'
  WHERE b.method_name = 'blocking'
"""


def _make_pair(cur, sql_filter: str, n: int, params=(), exclude_ids=None) -> list[dict]:
    """Select up to n pairs matching the filter, with a stable random order."""
    exclude_ids = exclude_ids or set()
    cur.execute(f"""
      SELECT {SELECT_COLS}
      {JOIN_CLAUSE}
        AND {sql_filter}
      ORDER BY random()
      LIMIT {n * 3}
    """, params)
    out = []
    for row in cur.fetchall():
        pair_id = row[0]
        if pair_id in exclude_ids:
            continue
        out.append(_pair_row_to_dict(row))
        if len(out) >= n:
            break
    return out


def sample_tier1(cur, n: int = 100, seen: set | None = None) -> list[dict]:
    """Near-certain MERGE: blocking signals >= 3, or shared >= 5 LWIN_7."""
    seen = seen or set()
    pairs = _make_pair(cur, """
        (SELECT COUNT(*) FROM jsonb_object_keys(b.signals)) >= 3
    """, n, exclude_ids=seen)
    for p in pairs:
        p["tier"] = "T1"
        p["proxy_gold"] = "MERGE"
        p["proxy_reason"] = f"multi-strategy agreement >=3 signals ({len(p['signals'])} keys)"
    return pairs


def sample_tier2(cur, n: int = 100, seen: set | None = None) -> list[dict]:
    """Near-certain SKIP: s2_trigram only, low similarity, L1 SKIP >= 0.97."""
    seen = seen or set()
    pairs = _make_pair(cur, """
        l.verdict = 'SKIP'
        AND l.confidence >= 0.97
        AND b.similarity BETWEEN 0.35 AND 0.45
        AND (SELECT COUNT(*) FROM jsonb_object_keys(b.signals)) = 1
        AND b.signals ? 's2_trigram'
    """, n, exclude_ids=seen)
    for p in pairs:
        p["tier"] = "T2"
        p["proxy_gold"] = "SKIP"
        p["proxy_reason"] = f"low trigram ({p['similarity']:.2f}) + L1 SKIP {p['l1_confidence']:.2f} + no other signals"
    return pairs


def sample_tier3(cur, n: int = 100, seen: set | None = None) -> list[dict]:
    """Borderline MERGE/PC: s6_ttb_permit + L1 MERGE/PC 0.80-0.92.

    Needs oracle: S6 shared TTB permit suggests same entity, but brand-on-label
    rule may distinguish PARENT_CHILD vs MERGE; confidence band is borderline.
    """
    seen = seen or set()
    pairs = _make_pair(cur, """
        b.signals ? 's6_ttb_permit'
        AND l.verdict IN ('MERGE', 'PARENT_CHILD')
        AND l.confidence BETWEEN 0.80 AND 0.92
    """, n, exclude_ids=seen)
    for p in pairs:
        p["tier"] = "T3"
        p["proxy_gold"] = None
        p["proxy_reason"] = f"S6 BW-permit shared + L1 {p['l1_verdict']} {p['l1_confidence']:.2f}"
    return pairs


def sample_tier4(cur, n: int = 100, seen: set | None = None) -> list[dict]:
    """Borderline SKIP: similarity >= 0.5 + L1 SKIP 0.88-0.95.

    Needs oracle: high textual similarity but L1 SKIPped; commune-name collision
    or family-name split are typical patterns — could be wrong.
    """
    seen = seen or set()
    pairs = _make_pair(cur, """
        l.verdict = 'SKIP'
        AND l.confidence BETWEEN 0.88 AND 0.95
        AND b.similarity >= 0.5
    """, n, exclude_ids=seen)
    for p in pairs:
        p["tier"] = "T4"
        p["proxy_gold"] = None
        p["proxy_reason"] = f"trigram {p['similarity']:.2f} + L1 SKIP {p['l1_confidence']:.2f}"
    return pairs


def sample_tier5(cur, n: int = 200, seen: set | None = None) -> list[dict]:
    """L3-oracle random sample — stratified across L1 verdict x confidence band.

    Allocates quotas:
      MERGE: 50  (across conf bands 0.80-0.92, 0.92-0.97, 0.97+, 0.75-0.80)
      PARENT_CHILD: 40 (across the same bands)
      SKIP: 80 (across conf bands 0.85-0.92, 0.92-0.97, 0.97+, 0.75-0.85)
      UNCERTAIN: 30 (all)
    """
    seen = seen or set()
    quotas = [
        ("MERGE",        "l.confidence BETWEEN 0.75 AND 0.80",  5),
        ("MERGE",        "l.confidence BETWEEN 0.80 AND 0.92", 20),
        ("MERGE",        "l.confidence BETWEEN 0.92 AND 0.97", 15),
        ("MERGE",        "l.confidence >= 0.97",               10),
        ("PARENT_CHILD", "l.confidence BETWEEN 0.75 AND 0.80",  5),
        ("PARENT_CHILD", "l.confidence BETWEEN 0.80 AND 0.92", 20),
        ("PARENT_CHILD", "l.confidence BETWEEN 0.92 AND 0.97", 15),
        ("SKIP",         "l.confidence BETWEEN 0.75 AND 0.85", 10),
        ("SKIP",         "l.confidence BETWEEN 0.85 AND 0.92", 20),
        ("SKIP",         "l.confidence BETWEEN 0.92 AND 0.97", 30),
        ("SKIP",         "l.confidence >= 0.97",               20),
        ("UNCERTAIN",    "1=1",                                30),
    ]
    out = []
    for verdict, conf_clause, quota in quotas:
        pairs = _make_pair(cur, f"""
            l.verdict = %s
            AND {conf_clause}
        """, quota, params=(verdict,), exclude_ids=seen | {p['pair_id'] for p in out})
        for p in pairs:
            p["tier"] = "T5"
            p["proxy_gold"] = None
            p["proxy_reason"] = f"random stratified {verdict} conf {p['l1_confidence']:.2f}"
        out.extend(pairs)
    return out[:n]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="write output (required)")
    ap.add_argument("--force", action="store_true", help="overwrite existing file")
    args = ap.parse_args()

    if not args.execute:
        print("Specify --execute to write calibration_set.json", file=sys.stderr)
        return 2

    if OUTPUT_PATH.exists() and not args.force:
        print(f"Already exists: {OUTPUT_PATH}. Use --force to overwrite.", file=sys.stderr)
        return 1

    conn = get_conn()
    cur = conn.cursor()

    seen: set[int] = set()
    tiers: dict[str, list[dict]] = {}

    for tier_name, n in PLAN.items():
        sampler = {
            "T1": sample_tier1,
            "T2": sample_tier2,
            "T3": sample_tier3,
            "T4": sample_tier4,
            "T5": sample_tier5,
        }[tier_name]
        print(f"Sampling {tier_name} (target {n})...", flush=True)
        pairs = sampler(cur, n=n, seen=seen)
        for p in pairs:
            seen.add(p["pair_id"])
            p["gold_verdict"] = None
            p["gold_confidence"] = None
            p["gold_reasoning"] = None
            p["gold_web_evidence"] = None
            p["gold_source"] = None
        tiers[tier_name] = pairs
        print(f"  {tier_name}: {len(pairs)} pairs")

    n_total = sum(len(v) for v in tiers.values())
    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_total": n_total,
        "plan": PLAN,
        "tiers": tiers,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    print(f"\nWrote {n_total} pairs to {OUTPUT_PATH}")

    # Summary
    print("\n-- Calibration set composition --")
    for tier_name, pairs in tiers.items():
        verdicts = {}
        for p in pairs:
            v = p.get("l1_verdict") or "NONE"
            verdicts[v] = verdicts.get(v, 0) + 1
        print(f"  {tier_name} ({len(pairs)} pairs): L1 verdicts = {verdicts}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
