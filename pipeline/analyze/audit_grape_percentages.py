"""
Audit wine_grapes.percentage values that sum to more than 100% per wine.

Read-only. Produces a per-pattern breakdown the user can review before deciding
on a repair strategy. Outputs two files:

    data/stats/grape_percentage_audit.json   — structured data for downstream fix script
    data/stats/grape_percentage_audit.md     — human-readable summary for the review gate

Usage:
    python -m pipeline.analyze.audit_grape_percentages

Context (Session 14 Phase B, W6):
    Stage 1 pass 2 of the Session 12 L3 enrichment validation found Kumeu River
    Hunting Hill carries `wine_grapes` = Chardonnay 100% + Pinot Noir 75% = 175%.
    Population query showed ~6,300-6,600 wines affected (12-13% of the pre-30K
    corpus with grape links). This is a ground-truth data bug — no prompt
    engineering can fix enrichment on these wines. Fix is P0 for any future
    wine-level or reference-level enrichment pass.

    This script does NOT write. It surfaces patterns so the user can pick a repair
    strategy per pattern (NULL-out, keep-highest, re-derive from LWIN, etc.).
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pipeline.lib.db import get_conn


STATS_DIR = Path("data/stats")
JSON_OUT = STATS_DIR / "grape_percentage_audit.json"
MD_OUT = STATS_DIR / "grape_percentage_audit.md"


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall() if cur.description else []


def q_dict(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def bucketize(total_pct: float) -> str:
    """Turn a raw sum into a named pattern bucket."""
    if total_pct is None:
        return "null"
    if total_pct <= 100:
        return "ok"
    if total_pct <= 110:
        return "110 (edge)"
    if total_pct <= 150:
        return "101-150"
    if total_pct <= 175:
        return "151-175"
    if total_pct <= 200:
        return "176-200"
    if total_pct <= 250:
        return "201-250"
    if total_pct <= 299:
        return "251-299"
    if total_pct <= 300:
        return "300"
    if total_pct <= 400:
        return "301-400"
    return ">400"


def main():
    conn = get_conn()
    conn.autocommit = True

    STATS_DIR.mkdir(parents=True, exist_ok=True)

    # Pull the per-wine sums + link counts (active wines only)
    print("[1/5] Aggregating wine_grapes sums per active wine...")
    rows = q_dict(conn, """
        SELECT
          wg.wine_id,
          SUM(wg.percentage) AS total_pct,
          count(*) AS link_count,
          count(*) FILTER (WHERE wg.percentage = 100) AS links_at_100,
          count(*) FILTER (WHERE wg.percentage IS NOT NULL AND wg.percentage < 100) AS links_below_100,
          count(*) FILTER (WHERE wg.percentage IS NULL) AS links_null_pct,
          array_agg(wg.grape_id ORDER BY wg.percentage DESC NULLS LAST) AS grape_ids
        FROM wine_grapes wg
        JOIN wines w ON w.id = wg.wine_id
        WHERE w.deleted_at IS NULL
        GROUP BY wg.wine_id
    """)

    # Split into ok / over-100
    over_100 = [r for r in rows if r["total_pct"] is not None and r["total_pct"] > 100]
    print(f"      {len(rows):,} wines with grape links, {len(over_100):,} over 100% (bad)")

    # Bucket
    buckets = {}
    for r in over_100:
        b = bucketize(float(r["total_pct"]))
        buckets.setdefault(b, []).append(r)

    bucket_order = [
        "110 (edge)", "101-150", "151-175", "176-200",
        "201-250", "251-299", "300", "301-400", ">400",
    ]

    # For each bucket, compute stats
    print("[2/5] Computing per-bucket stats...")
    bucket_stats = []
    for b in bucket_order:
        if b not in buckets:
            continue
        group = buckets[b]
        total = len(group)
        n_single_link_100 = sum(1 for g in group if g["link_count"] == 1 and g["links_at_100"] == 1)
        n_multi_100_some_partial = sum(
            1 for g in group if g["links_at_100"] >= 1 and g["links_below_100"] >= 1
        )
        n_multi_100_only = sum(
            1 for g in group if g["links_at_100"] >= 2 and g["links_below_100"] == 0 and g["links_null_pct"] == 0
        )
        n_has_null = sum(1 for g in group if g["links_null_pct"] > 0)
        avg_sum = sum(float(g["total_pct"]) for g in group) / total if total else 0
        avg_links = sum(g["link_count"] for g in group) / total if total else 0
        bucket_stats.append({
            "bucket": b,
            "wines": total,
            "avg_sum": round(avg_sum, 1),
            "avg_link_count": round(avg_links, 1),
            "multi_100_only": n_multi_100_only,
            "mix_100_and_partial": n_multi_100_some_partial,
            "has_null_percentage_link": n_has_null,
            "single_link_100": n_single_link_100,
        })

    # LWIN availability for over-100 wines (can we re-derive from LWIN?)
    print("[3/5] Checking LWIN coverage for broken wines...")
    broken_ids = [str(r["wine_id"]) for r in over_100]
    lwin_cover = {"with_lwin": 0, "without_lwin": 0}
    if broken_ids:
        # Cast the text[] parameter to uuid[] on the server side
        rows_l = q(conn, """
            SELECT count(DISTINCT entity_id)
            FROM external_ids
            WHERE entity_type = 'wine'
              AND system IN ('lwin', 'lwin_7')
              AND entity_id = ANY(%s::uuid[])
        """, (broken_ids,))
        with_l = rows_l[0][0] if rows_l else 0
        lwin_cover["with_lwin"] = with_l
        lwin_cover["without_lwin"] = len(broken_ids) - with_l

    # Sample wines per bucket (10 of each, with producer/wine name)
    print("[4/5] Collecting samples per bucket...")
    samples = {}
    for b in bucket_order:
        if b not in buckets:
            continue
        # Take up to 10 from this bucket
        chosen = buckets[b][:10]
        sample_ids = [str(g["wine_id"]) for g in chosen]
        if not sample_ids:
            continue
        names = q_dict(conn, """
            SELECT
              w.id AS wine_id,
              w.display_name,
              p.name AS producer_name,
              (
                SELECT array_agg(g.name || ' ' || COALESCE(wg.percentage::text, 'NULL') || '%%'
                                 ORDER BY wg.percentage DESC NULLS LAST)
                FROM wine_grapes wg
                JOIN grapes g ON g.id = wg.grape_id
                WHERE wg.wine_id = w.id
              ) AS grape_detail
            FROM wines w
            JOIN producers p ON p.id = w.producer_id
            WHERE w.id = ANY(%s::uuid[])
        """, (sample_ids,))
        samples[b] = names

    # Overall totals
    total_broken = len(over_100)
    total_wines_with_links = len(rows)
    pct_broken = (total_broken / total_wines_with_links * 100) if total_wines_with_links else 0

    # Write JSON
    print("[5/5] Writing outputs...")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "totals": {
            "wines_with_any_grape_link": total_wines_with_links,
            "wines_over_100": total_broken,
            "pct_over_100": round(pct_broken, 2),
            "lwin_available_for_broken": lwin_cover["with_lwin"],
            "lwin_unavailable_for_broken": lwin_cover["without_lwin"],
        },
        "buckets": bucket_stats,
    }
    JSON_OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"      {JSON_OUT}")

    # Write Markdown
    md = []
    md.append("# Grape percentage audit")
    md.append("")
    md.append(f"**Generated:** {payload['generated_at']}")
    md.append(f"**Script:** `pipeline/analyze/audit_grape_percentages.py` (read-only)")
    md.append("")
    md.append("## Summary")
    md.append("")
    md.append(f"- Active wines with at least one `wine_grapes` row: **{total_wines_with_links:,}**")
    md.append(f"- Wines whose percentages sum to **> 100%**: **{total_broken:,}** ({pct_broken:.1f}%)")
    md.append(f"- Of those, have a LWIN match (re-derivable): **{lwin_cover['with_lwin']:,}**")
    md.append(f"- Without LWIN: **{lwin_cover['without_lwin']:,}**")
    md.append("")
    md.append("## Per-pattern breakdown")
    md.append("")
    md.append("| Bucket | Wines | Avg sum | Avg links | multi-100 only | mix 100+partial | has NULL link | single 100 |")
    md.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for b in bucket_stats:
        md.append(
            f"| **{b['bucket']}** | {b['wines']:,} | {b['avg_sum']:.1f} | {b['avg_link_count']:.1f} | "
            f"{b['multi_100_only']:,} | {b['mix_100_and_partial']:,} | "
            f"{b['has_null_percentage_link']:,} | {b['single_link_100']:,} |"
        )
    md.append("")
    md.append("**Column key:**")
    md.append("- `multi_100 only` — all grape links are 100% (e.g. 3 × 100% = 300%). Classic duplicate-promotion pattern.")
    md.append("- `mix 100+partial` — some links at 100%, at least one with a <100% number (e.g. 100% + 75% = 175%).")
    md.append("- `has NULL link` — at least one row has `percentage IS NULL` alongside non-null totals.")
    md.append("- `single 100` — exactly one row, at 100%. Should never be here (sum = 100 = ok). If non-zero, data quality bug.")
    md.append("")
    md.append("## Sample offenders per bucket")
    md.append("")
    for b in bucket_order:
        if b not in samples:
            continue
        md.append(f"### {b} — sample of 10 wines")
        md.append("")
        for s in samples[b]:
            name = s["display_name"] or s["producer_name"] or s["wine_id"]
            grapes = ", ".join(s["grape_detail"] or [])
            md.append(f"- **{name}** — {grapes}")
        md.append("")
    md.append("## Repair strategy — decide per bucket")
    md.append("")
    md.append("This is the review gate. Pick a strategy per bucket, then run `fix_grape_percentages.py --dry-run`.")
    md.append("")
    md.append("Candidate strategies:")
    md.append("")
    md.append("1. **NULL-out percentages** — keep the grape links, clear all `percentage` values on wines in this bucket. Conservative; loses blend proportion but keeps grape identity.")
    md.append("2. **Keep single highest-confidence row, drop others** — if one row has a meaningful percentage and the rest are redundant 100s, keep the one row and delete the 100s.")
    md.append("3. **Re-derive from LWIN** — for wines with a LWIN match, pull percentages from `source_lwin` (if LWIN carries clean values for that wine).")
    md.append("4. **Normalize to sum 100** — scale all non-null values proportionally so the sum becomes 100. Only valid if the current values encode real relative proportions and just need scaling.")
    md.append("5. **Leave alone** — if the pattern is edge-case and small (e.g. 110 bucket from rounding), may not be worth touching.")
    md.append("")
    md.append("Log the chosen strategy per bucket in `docs/DECISIONS.md` before executing the fix script.")
    MD_OUT.write_text("\n".join(md), encoding="utf-8")
    print(f"      {MD_OUT}")

    print()
    print(f"DONE. {total_broken:,} broken wines across {len(bucket_stats)} pattern buckets.")
    print(f"Review {MD_OUT} before writing any fix.")

    conn.close()


if __name__ == "__main__":
    main()
