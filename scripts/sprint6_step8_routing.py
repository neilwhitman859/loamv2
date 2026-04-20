"""Sprint 6 Step 8: build producer_dedup_routing_stage3 table with final decisions.

Decision tree priority (top-down, first match wins):

=== Tier A: web verdict present (Phase 3a + Step 7) ===
  web=MERGE  conf >= 0.90         -> auto_apply_merge                (web_merge_high)
  web=MERGE  conf in [0.85, 0.90) -> user_review_merge_lowconf      (web_merge_low)
  web=MERGE  conf <  0.85         -> auto_apply_skip                (web_merge_veryweak)
  web=SKIP   conf >= 0.85         -> auto_apply_skip                (web_skip_high)
  web=SKIP   conf <  0.85         -> auto_apply_skip                (web_skip_low - asymmetric safe)
  web=PARENT_CHILD conf >= 0.90   -> auto_apply_pc                  (web_pc_high)
  web=PARENT_CHILD conf [0.85,0.90)-> user_review_pc                (web_pc_low)
  web=PARENT_CHILD conf <  0.85   -> auto_apply_skip                (web_pc_weak -> noise)
  web=UNCERTAIN                    -> auto_apply_skip                (web_uncertain_default)

=== Tier B: no web verdict, trust earlier tiers ===
  stage1_action = stage1_auto_skip              -> auto_apply_skip           (stage1_consensus_skip)
  stage2_action = stage2_auto_skip              -> auto_apply_skip           (stage2_consensus_skip)
  stage1_action = stage1_auto_merge (no web)    -> user_review_merge_unvalidated (should be ~3 edge pairs)
  stage2_action = stage2_user_review_pc no web  -> auto_apply_skip_tail_pc   (Tail PC deferred; Core all covered)
  stage2_action = escalate_to_l3 no web         -> auto_apply_skip_residual  (Safety Net B target)
  stage2_action = stage2_missing_tier           -> auto_apply_skip_missing
  missing routing                                -> user_review_missing

Action vocabulary:
  auto_apply_merge       - apply MERGE during Step 10
  auto_apply_skip        - no action needed (already two rows)
  auto_apply_pc          - create parent_producer_id link during Step 10
  auto_apply_skip_*      - SKIP with a specific reason (audit-only subtypes)
  user_review_*          - surface to user in Step 9 pattern curation
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn

conn = get_conn()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS producer_dedup_routing_stage3")

cur.execute("""
CREATE TABLE producer_dedup_routing_stage3 (
    pair_id INTEGER PRIMARY KEY,
    producer_id_a UUID NOT NULL,
    producer_id_b UUID NOT NULL,
    name_a TEXT, name_b TEXT, country TEXT,
    core_a BOOLEAN NOT NULL, core_b BOOLEAN NOT NULL,
    is_core BOOLEAN NOT NULL,
    stage1_action TEXT, stage2_action TEXT,
    l1_verdict TEXT, l1_conf NUMERIC,
    l1_5_verdict TEXT, l1_5_conf NUMERIC,
    l2_verdict TEXT, l2_conf NUMERIC,
    l2_5_verdict TEXT, l2_5_conf NUMERIC,
    web_verdict TEXT, web_conf NUMERIC, web_reasoning TEXT,
    stage3_action TEXT NOT NULL,
    stage3_reason TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
)
""")
cur.execute("CREATE INDEX ON producer_dedup_routing_stage3 (stage3_action)")
cur.execute("CREATE INDEX ON producer_dedup_routing_stage3 (is_core, stage3_action)")

cur.execute("""
INSERT INTO producer_dedup_routing_stage3 (
    pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
    core_a, core_b, is_core,
    stage1_action, stage2_action,
    l1_verdict, l1_conf, l1_5_verdict, l1_5_conf,
    l2_verdict, l2_conf, l2_5_verdict, l2_5_conf,
    web_verdict, web_conf, web_reasoning,
    stage3_action, stage3_reason
)
WITH all_verdicts AS (
    SELECT
        b.id AS pair_id, b.producer_id_a, b.producer_id_b,
        b.name_a, b.name_b, b.country,
        (b.producer_id_a IN (SELECT producer_id FROM sprint6_core_producers)) AS core_a,
        (b.producer_id_b IN (SELECT producer_id FROM sprint6_core_producers)) AS core_b,
        s1.stage1_action, s2.stage2_action,
        s1.l1_verdict, s1.l1_conf,
        s1.l1_5_verdict, s1.l1_5_conf,
        s2.l2_verdict, s2.l2_conf,
        s2.l2_5_verdict, s2.l2_5_conf,
        w.verdict AS web_verdict,
        w.confidence AS web_conf,
        w.reasoning AS web_reasoning
    FROM producer_dedup_pairs b
    LEFT JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = b.id
    LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = b.id
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name = 'l2_haiku_rich_web'
      AND w.producer_id_a = b.producer_id_a
      AND w.producer_id_b = b.producer_id_b
    WHERE b.method_name = 'blocking'
)
SELECT
    pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
    core_a, core_b, (core_a OR core_b) AS is_core,
    stage1_action, stage2_action,
    l1_verdict, l1_conf, l1_5_verdict, l1_5_conf,
    l2_verdict, l2_conf, l2_5_verdict, l2_5_conf,
    web_verdict, web_conf, web_reasoning,
    CASE
        -- Tier A: web verdict present
        WHEN web_verdict = 'MERGE' AND web_conf >= 0.90 THEN 'auto_apply_merge'
        WHEN web_verdict = 'MERGE' AND web_conf >= 0.85 THEN 'user_review_merge_lowconf'
        WHEN web_verdict = 'MERGE' AND web_conf <  0.85 THEN 'auto_apply_skip'
        WHEN web_verdict = 'SKIP'                       THEN 'auto_apply_skip'
        WHEN web_verdict = 'PARENT_CHILD' AND web_conf >= 0.90 THEN 'auto_apply_pc'
        WHEN web_verdict = 'PARENT_CHILD' AND web_conf >= 0.85 THEN 'user_review_pc'
        WHEN web_verdict = 'PARENT_CHILD' AND web_conf <  0.85 THEN 'auto_apply_skip'
        WHEN web_verdict = 'UNCERTAIN'                  THEN 'auto_apply_skip'
        -- Tier B: no web verdict
        WHEN stage1_action = 'stage1_auto_skip'         THEN 'auto_apply_skip'
        WHEN stage2_action = 'stage2_auto_skip'         THEN 'auto_apply_skip'
        WHEN stage1_action = 'stage1_auto_merge'        THEN 'user_review_merge_unvalidated'
        WHEN stage2_action = 'stage2_auto_merge'        THEN 'user_review_merge_unvalidated'
        WHEN stage2_action = 'stage2_user_review_pc'    THEN 'auto_apply_skip'
        WHEN stage2_action = 'escalate_to_l3'           THEN 'auto_apply_skip_residual'
        WHEN stage2_action = 'stage2_missing_tier'      THEN 'auto_apply_skip_missing'
        ELSE 'user_review_missing'
    END AS stage3_action,
    CASE
        WHEN web_verdict = 'MERGE' AND web_conf >= 0.90 THEN 'web_merge_high'
        WHEN web_verdict = 'MERGE' AND web_conf >= 0.85 THEN 'web_merge_low'
        WHEN web_verdict = 'MERGE' AND web_conf <  0.85 THEN 'web_merge_veryweak'
        WHEN web_verdict = 'SKIP'  AND web_conf >= 0.85 THEN 'web_skip_high'
        WHEN web_verdict = 'SKIP'  AND web_conf <  0.85 THEN 'web_skip_low'
        WHEN web_verdict = 'PARENT_CHILD' AND web_conf >= 0.90 THEN 'web_pc_high'
        WHEN web_verdict = 'PARENT_CHILD' AND web_conf >= 0.85 THEN 'web_pc_low'
        WHEN web_verdict = 'PARENT_CHILD' AND web_conf <  0.85 THEN 'web_pc_weak'
        WHEN web_verdict = 'UNCERTAIN'                  THEN 'web_uncertain_default'
        WHEN stage1_action = 'stage1_auto_skip'         THEN 'stage1_consensus_skip'
        WHEN stage2_action = 'stage2_auto_skip'         THEN 'stage2_consensus_skip'
        WHEN stage1_action = 'stage1_auto_merge'        THEN 'stage1_auto_merge_unvalidated'
        WHEN stage2_action = 'stage2_auto_merge'        THEN 'stage2_auto_merge_unvalidated'
        WHEN stage2_action = 'stage2_user_review_pc'    THEN 'stage2_pc_no_web'
        WHEN stage2_action = 'escalate_to_l3'           THEN 'escalate_to_l3_no_web'
        WHEN stage2_action = 'stage2_missing_tier'      THEN 'stage2_missing_tier'
        ELSE 'routing_missing'
    END AS stage3_reason
FROM all_verdicts
""")
conn.commit()

cur.execute("SELECT COUNT(*) FROM producer_dedup_routing_stage3")
print(f"Rows inserted: {cur.fetchone()[0]:,}")

print("\n=== Stage 3 action counts (all pairs) ===")
cur.execute("""
    SELECT stage3_action, COUNT(*) FROM producer_dedup_routing_stage3
    GROUP BY stage3_action ORDER BY 2 DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]:<38} {row[1]:>8,}")

print("\n=== Core breakdown by action ===")
cur.execute("""
    SELECT stage3_action, COUNT(*) FROM producer_dedup_routing_stage3
    WHERE is_core GROUP BY stage3_action ORDER BY 2 DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]:<38} {row[1]:>8,}")

print("\n=== Tail breakdown by action ===")
cur.execute("""
    SELECT stage3_action, COUNT(*) FROM producer_dedup_routing_stage3
    WHERE NOT is_core GROUP BY stage3_action ORDER BY 2 DESC
""")
for row in cur.fetchall():
    print(f"  {row[0]:<38} {row[1]:>8,}")

print("\n=== Stage 3 reason audit (top 15) ===")
cur.execute("""
    SELECT stage3_reason, COUNT(*) FROM producer_dedup_routing_stage3
    GROUP BY stage3_reason ORDER BY 2 DESC LIMIT 15
""")
for row in cur.fetchall():
    print(f"  {row[0]:<38} {row[1]:>8,}")

cur.close(); conn.close()
