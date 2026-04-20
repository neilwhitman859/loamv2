"""Sprint 6 Step 7: build 3 pair-ID manifests for Haiku+Serper web validation.

Writes:
  data/sprints/dedup/step7_core_manifest.json    (Core escalations + PC + missing)
  data/sprints/dedup/step7_tail_manifest.json    (Tail escalate_to_l3 only)
  data/sprints/dedup/step7_audit_manifest.json   (2,000-pair stratified Core auto-SKIP)
"""
import sys
import json
import random
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn

OUTDIR = Path(__file__).resolve().parents[1] / "data" / "sprints" / "dedup"
OUTDIR.mkdir(parents=True, exist_ok=True)

conn = get_conn()
cur = conn.cursor()

# ---- Manifest 1: Core pairs that need web validation ----
# All Core-involved pairs NOT yet web-validated AND NOT in auto-skip buckets
cur.execute("""
    WITH core_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b
        FROM producer_dedup_pairs b
        WHERE b.method_name = 'blocking'
          AND (b.producer_id_a IN (SELECT producer_id FROM sprint6_core_producers)
               OR b.producer_id_b IN (SELECT producer_id FROM sprint6_core_producers))
    )
    SELECT cp.pair_id
    FROM core_pairs cp
    LEFT JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = cp.pair_id
    LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = cp.pair_id
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = cp.producer_id_a
      AND w.producer_id_b = cp.producer_id_b
    WHERE w.id IS NULL
      AND COALESCE(s2.stage2_action, s1.stage1_action, 'missing') NOT IN ('stage1_auto_skip', 'stage2_auto_skip')
    ORDER BY cp.pair_id
""")
core_ids = [row[0] for row in cur.fetchall()]
(OUTDIR / "step7_core_manifest.json").write_text(json.dumps(core_ids))
print(f"Core manifest: {len(core_ids):,} pair IDs → step7_core_manifest.json")
print(f"  Est. cost: ${len(core_ids)*0.006:.2f}")

# ---- Manifest 2: Tail escalate_to_l3 pairs ----
cur.execute("""
    WITH tail_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b
        FROM producer_dedup_pairs b
        WHERE b.method_name = 'blocking'
          AND b.producer_id_a NOT IN (SELECT producer_id FROM sprint6_core_producers)
          AND b.producer_id_b NOT IN (SELECT producer_id FROM sprint6_core_producers)
    )
    SELECT tp.pair_id
    FROM tail_pairs tp
    LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = tp.pair_id
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = tp.producer_id_a
      AND w.producer_id_b = tp.producer_id_b
    WHERE s2.stage2_action = 'escalate_to_l3' AND w.id IS NULL
    ORDER BY tp.pair_id
""")
tail_ids = [row[0] for row in cur.fetchall()]
(OUTDIR / "step7_tail_manifest.json").write_text(json.dumps(tail_ids))
print(f"Tail manifest: {len(tail_ids):,} pair IDs → step7_tail_manifest.json")
print(f"  Est. cost: ${len(tail_ids)*0.006:.2f}")

# ---- Manifest 3: Stratified Core auto-SKIP audit sample ----
# Sample 2,000 from the Core auto-skip pool, stratified by stage (1 vs 2) and country bucket
# to get representative coverage
cur.execute("""
    WITH core_skips AS (
        SELECT b.id AS pair_id, b.country,
          COALESCE(s2.stage2_action, s1.stage1_action) AS skip_stage
        FROM producer_dedup_pairs b
        JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = b.id
        LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = b.id
        WHERE b.method_name = 'blocking'
          AND (b.producer_id_a IN (SELECT producer_id FROM sprint6_core_producers)
               OR b.producer_id_b IN (SELECT producer_id FROM sprint6_core_producers))
          AND COALESCE(s2.stage2_action, s1.stage1_action) IN ('stage1_auto_skip', 'stage2_auto_skip')
    )
    SELECT pair_id, country, skip_stage FROM core_skips
""")
rows = cur.fetchall()
print(f"\nCore auto-SKIP universe: {len(rows):,} pairs")

# Stratify by (skip_stage, country-bucket)
# Country buckets: US, FR, IT, other-single, cross-country
def bucket(country):
    if country is None:
        return "null"
    if "/" in country:
        return "cross_country"
    if country in ("US", "FR", "IT"):
        return country
    return "other"

strata = {}
for pid, country, stage in rows:
    k = (stage, bucket(country))
    strata.setdefault(k, []).append(pid)

# Allocate ~2000 across strata proportionally but with min 50 per non-empty stratum
target = 2000
total = sum(len(v) for v in strata.values())
random.seed(42)
sample = []
for k, ids in strata.items():
    alloc = max(50, int(round(target * len(ids) / total))) if ids else 0
    alloc = min(alloc, len(ids))
    sample.extend(random.sample(ids, alloc))

print(f"Audit sample: {len(sample):,} pairs across {len(strata)} strata")
for k in sorted(strata):
    alloc = sum(1 for pid in sample if pid in strata[k])
    print(f"  {k[0]:<20} {k[1]:<15} universe={len(strata[k]):>6,}  sampled={alloc}")

(OUTDIR / "step7_audit_manifest.json").write_text(json.dumps(sorted(sample)))
print(f"\nAudit manifest: {len(sample):,} pair IDs → step7_audit_manifest.json")
print(f"  Est. cost: ${len(sample)*0.006:.2f}")

# ---- Summary ----
total_pairs = len(core_ids) + len(tail_ids) + len(sample)
total_cost = total_pairs * 0.006
print(f"\n=== Step 7 totals ===")
print(f"Total pairs: {total_pairs:,}")
print(f"Total cost:  ${total_cost:.2f}")
print(f"Runtime at 6 workers, ~1 req/sec: ~{total_pairs/(6*3600):.1f} hrs")

cur.close(); conn.close()
