"""Sprint 6 Step 7 audit analysis: measure Core auto-SKIP False-Negative rate.

Runs Opus-style inline analysis: for each of the 2,020 audit-sampled Core auto-SKIP
pairs, the Haiku+Serper run already wrote a verdict. This script joins the audit
sample to those verdicts and computes:
  - Distribution of web verdicts (MERGE/SKIP/PC/UNCERTAIN) x confidence band
  - Estimated FN rate extrapolated to the full Core auto-SKIP universe (86,354)
  - Breakdown by stratum (stage x country bucket)
  - Sample of flipped pairs (where web says MERGE/PC but auto-SKIP said SKIP)
"""
import sys
import json
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn

conn = get_conn()
cur = conn.cursor()

# Load audit manifest pair IDs
with open('data/sprints/dedup/step7_audit_manifest.json') as f:
    audit_ids = json.loads(f.read())

print(f"Audit sample: {len(audit_ids):,} pairs")

# Pull web verdicts for audit pairs (joining via producer_id tuple)
cur.execute("""
    WITH audit_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b, b.country,
               COALESCE(s2.stage2_action, s1.stage1_action) AS skip_stage
        FROM producer_dedup_pairs b
        JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = b.id
        LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = b.id
        WHERE b.id = ANY(%s)
    )
    SELECT ap.pair_id, ap.name_a, ap.name_b, ap.country, ap.skip_stage,
           w.verdict AS web_verdict, w.confidence AS web_conf, w.reasoning AS web_reasoning
    FROM audit_pairs ap
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name = 'l2_haiku_rich_web'
      AND w.producer_id_a = ap.producer_id_a
      AND w.producer_id_b = ap.producer_id_b
""", (audit_ids,))
rows = cur.fetchall()

total = len(rows)
with_web = sum(1 for r in rows if r[5] is not None)
print(f"With web verdict: {with_web:,} / {total:,} ({100*with_web/total:.1f}%)")

print()
print("=== Web verdict distribution on audit sample ===")
cur.execute("""
    WITH audit_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b
        FROM producer_dedup_pairs b WHERE b.id = ANY(%s)
    )
    SELECT w.verdict,
           CASE
             WHEN w.confidence >= 0.95 THEN 'conf >=0.95'
             WHEN w.confidence >= 0.90 THEN '0.90-0.95'
             WHEN w.confidence >= 0.85 THEN '0.85-0.90'
             ELSE '<0.85'
           END AS band,
           COUNT(*) AS n
    FROM audit_pairs ap
    JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = ap.producer_id_a
      AND w.producer_id_b = ap.producer_id_b
    GROUP BY 1, 2 ORDER BY 1, 2
""", (audit_ids,))
for row in cur.fetchall():
    print(f"  {row[0]:<15} {row[1]:<15} {row[2]:,}")

print()
print("=== FN breakdown: web says MERGE/PC on pairs auto-SKIP'd by cheap AIs ===")
# MERGE at conf >=0.90 is a real Missed Merge (would have been applied if we'd web-validated)
# MERGE at conf 0.85-0.90 is a borderline Missed Merge
# PC is a Missed PC (lower priority)
cur.execute("""
    WITH audit_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b
        FROM producer_dedup_pairs b WHERE b.id = ANY(%s)
    )
    SELECT
      COUNT(*) FILTER (WHERE w.verdict='MERGE' AND w.confidence >= 0.90) AS fn_merge_strong,
      COUNT(*) FILTER (WHERE w.verdict='MERGE' AND w.confidence BETWEEN 0.85 AND 0.90) AS fn_merge_borderline,
      COUNT(*) FILTER (WHERE w.verdict='MERGE' AND w.confidence < 0.85) AS fn_merge_weak,
      COUNT(*) FILTER (WHERE w.verdict='PARENT_CHILD') AS fn_pc,
      COUNT(*) FILTER (WHERE w.verdict='SKIP') AS confirmed_skip,
      COUNT(*) FILTER (WHERE w.verdict='UNCERTAIN') AS uncertain,
      COUNT(*) AS total_with_web
    FROM audit_pairs ap
    JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = ap.producer_id_a
      AND w.producer_id_b = ap.producer_id_b
""", (audit_ids,))
r = cur.fetchone()
fn_m_strong, fn_m_border, fn_m_weak, fn_pc, conf_skip, unc, total = r
print(f"  Total with web verdict:        {total:,}")
print(f"  Confirmed SKIP (correct):      {conf_skip:,} ({100*conf_skip/total:.2f}%)")
print(f"  FN-Merge strong (conf >=0.90): {fn_m_strong:,} ({100*fn_m_strong/total:.2f}%)  <- real Missed Merges")
print(f"  FN-Merge borderline (0.85-0.90): {fn_m_border:,} ({100*fn_m_border/total:.2f}%)")
print(f"  FN-Merge weak (<0.85):         {fn_m_weak:,} ({100*fn_m_weak/total:.2f}%)")
print(f"  PC verdict (Missed PC):        {fn_pc:,} ({100*fn_pc/total:.2f}%)")
print(f"  UNCERTAIN:                     {unc:,} ({100*unc/total:.2f}%)")

# Extrapolate to full Core auto-SKIP universe (86,354)
CORE_AUTO_SKIP_UNIVERSE = 86354
if total > 0:
    fn_rate = fn_m_strong / total
    proj_fn = fn_rate * CORE_AUTO_SKIP_UNIVERSE
    print()
    print(f"=== Extrapolation to full Core auto-SKIP universe ({CORE_AUTO_SKIP_UNIVERSE:,}) ===")
    print(f"  FN-strong rate on sample:          {100*fn_rate:.2f}%")
    print(f"  Projected Missed Merges in Core:   ~{int(proj_fn):,} pairs")
    border_rate = fn_m_border / total
    proj_border = border_rate * CORE_AUTO_SKIP_UNIVERSE
    print(f"  FN-borderline rate:                {100*border_rate:.2f}%")
    print(f"  Projected borderline Missed Merges: ~{int(proj_border):,} pairs")
    print(f"  Combined projected (strong+border): ~{int(proj_fn + proj_border):,} pairs")

print()
print("=== Sample of 20 FN-strong flipped pairs (real Missed Merges) ===")
cur.execute("""
    WITH audit_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b, b.country
        FROM producer_dedup_pairs b WHERE b.id = ANY(%s)
    )
    SELECT ap.name_a, ap.name_b, ap.country, w.confidence, w.reasoning
    FROM audit_pairs ap
    JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = ap.producer_id_a
      AND w.producer_id_b = ap.producer_id_b
    WHERE w.verdict = 'MERGE' AND w.confidence >= 0.90
    ORDER BY w.confidence DESC
    LIMIT 20
""", (audit_ids,))
for i, row in enumerate(cur.fetchall(), 1):
    name_a, name_b, country, conf, reasoning = row
    print(f"  {i}. [{country}] '{name_a}' / '{name_b}' (conf {conf:.2f})")
    print(f"     {reasoning[:180]}...")

cur.close(); conn.close()
