# B6.5a routing SQL reference

SQL kept here for clarity & reproducibility. All tables populated via MCP execute_sql during B6.5a execution. They are working tables — safe to drop after B6.5b.

## Stage 1 routing (post-L1 + L1.5)

Rules (from `final_thresholds.json`):

- `stage1_auto_merge` — L1 MERGE ≥0.88 AND L1.5 MERGE ≥0.88
- `stage1_auto_skip` — L1 SKIP ≥0.97 AND L1.5 SKIP ≥0.97
- everything else → escalate to Stage 2

```sql
DROP TABLE IF EXISTS producer_dedup_routing_stage1;
CREATE TABLE producer_dedup_routing_stage1 AS
SELECT
  l1.id AS pair_id,
  l1.producer_id_a, l1.producer_id_b,
  l1.name_a, l1.name_b, l1.country,
  l1.verdict AS l1_verdict, l1.confidence AS l1_conf,
  g.verdict AS l1_5_verdict, g.confidence AS l1_5_conf,
  CASE
    WHEN l1.verdict='MERGE' AND l1.confidence>=0.88
         AND g.verdict='MERGE' AND g.confidence>=0.88
      THEN 'stage1_auto_merge'
    WHEN l1.verdict='SKIP' AND l1.confidence>=0.97
         AND g.verdict='SKIP' AND g.confidence>=0.97
      THEN 'stage1_auto_skip'
    WHEN l1.verdict='PARENT_CHILD' AND g.verdict='PARENT_CHILD'
      THEN 'escalate_pc_consensus'
    WHEN l1.verdict='PARENT_CHILD' OR g.verdict='PARENT_CHILD'
      THEN 'escalate_pc_single'
    WHEN l1.verdict='UNCERTAIN' OR g.verdict='UNCERTAIN'
      THEN 'escalate_uncertain'
    WHEN l1.verdict <> g.verdict
      THEN 'escalate_disagreement'
    ELSE 'escalate_lowconf'
  END AS stage1_action
FROM producer_dedup_pairs l1
JOIN producer_dedup_pairs g
  ON g.producer_id_a = l1.producer_id_a
 AND g.producer_id_b = l1.producer_id_b
 AND g.method_name = 'l1_gemini_basic'
WHERE l1.method_name = 'l1_haiku_batch';

CREATE INDEX ON producer_dedup_routing_stage1 (stage1_action);
CREATE INDEX ON producer_dedup_routing_stage1 (pair_id);
```

Bucket check:

```sql
SELECT stage1_action, COUNT(*) FROM producer_dedup_routing_stage1 GROUP BY stage1_action ORDER BY 2 DESC;
```

Projections: auto_merge 1,500-2,200 · auto_skip 100,000-125,000 · escalate 24,000-49,000.

## Stage 2 routing (post-L2 + L2.5 on escalations)

Rules:

- `stage2_auto_merge` — L2 MERGE ≥0.90 AND L2.5 MERGE ≥0.90
- `stage2_auto_skip` — L2 SKIP ≥0.95 AND L2.5 SKIP ≥0.95
- refined PC routing (user review): 2+ tiers PC anywhere; OR L2/L2.5 rich PC ≥0.90; OR L3 PC; else PC is noise and follow cross-tier non-PC consensus
- else → escalate to L3

```sql
DROP TABLE IF EXISTS producer_dedup_routing_stage2;
CREATE TABLE producer_dedup_routing_stage2 AS
WITH base AS (
  SELECT
    r1.pair_id,
    r1.producer_id_a, r1.producer_id_b,
    r1.name_a, r1.name_b, r1.country,
    r1.stage1_action,
    r1.l1_verdict, r1.l1_conf,
    r1.l1_5_verdict, r1.l1_5_conf,
    l2.verdict AS l2_verdict, l2.confidence AS l2_conf,
    l25.verdict AS l2_5_verdict, l25.confidence AS l2_5_conf
  FROM producer_dedup_routing_stage1 r1
  LEFT JOIN producer_dedup_pairs l2
    ON l2.producer_id_a = r1.producer_id_a
   AND l2.producer_id_b = r1.producer_id_b
   AND l2.method_name = 'l2_haiku_rich'
  LEFT JOIN producer_dedup_pairs l25
    ON l25.producer_id_a = r1.producer_id_a
   AND l25.producer_id_b = r1.producer_id_b
   AND l25.method_name = 'l2_gemini_rich'
  WHERE r1.stage1_action LIKE 'escalate_%'
)
SELECT
  *,
  CASE
    WHEN l2_verdict IS NULL OR l2_5_verdict IS NULL
      THEN 'stage2_missing_tier'
    WHEN l2_verdict='MERGE' AND l2_conf>=0.90
         AND l2_5_verdict='MERGE' AND l2_5_conf>=0.90
      THEN 'stage2_auto_merge'
    WHEN l2_verdict='SKIP' AND l2_conf>=0.95
         AND l2_5_verdict='SKIP' AND l2_5_conf>=0.95
      THEN 'stage2_auto_skip'
    -- refined PC review rules (two flavors)
    WHEN (
      ( (l1_verdict='PARENT_CHILD')::int +
        (l1_5_verdict='PARENT_CHILD')::int +
        (l2_verdict='PARENT_CHILD')::int +
        (l2_5_verdict='PARENT_CHILD')::int ) >= 2
    )
      THEN 'stage2_user_review_pc'
    WHEN (l2_verdict='PARENT_CHILD' AND l2_conf>=0.90)
      OR (l2_5_verdict='PARENT_CHILD' AND l2_5_conf>=0.90)
      THEN 'stage2_user_review_pc'
    -- PC noise: L1/L1.5 said PC but L2 tiers don't confirm; follow non-PC cross-tier consensus
    WHEN l2_verdict='MERGE' AND l2_conf>=0.90
         AND l2_5_verdict='MERGE' AND l2_5_conf>=0.90
      THEN 'stage2_auto_merge'
    WHEN l2_verdict='SKIP' AND l2_conf>=0.95
         AND l2_5_verdict='SKIP' AND l2_5_conf>=0.95
      THEN 'stage2_auto_skip'
    ELSE 'escalate_to_l3'
  END AS stage2_action
FROM base;

CREATE INDEX ON producer_dedup_routing_stage2 (stage2_action);
CREATE INDEX ON producer_dedup_routing_stage2 (pair_id);
```

## Stage 3 final routing (post-L3)

```sql
DROP TABLE IF EXISTS producer_dedup_routing_final;
CREATE TABLE producer_dedup_routing_final AS
WITH all_pairs AS (
  -- Auto-applied at Stage 1
  SELECT pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
         'auto_merge' AS final_action, 'stage1' AS decided_at,
         NULL::text AS l3_verdict, NULL::numeric AS l3_conf
  FROM producer_dedup_routing_stage1 WHERE stage1_action='stage1_auto_merge'
  UNION ALL
  SELECT pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
         'auto_skip' AS final_action, 'stage1' AS decided_at,
         NULL, NULL
  FROM producer_dedup_routing_stage1 WHERE stage1_action='stage1_auto_skip'
  UNION ALL
  -- Auto-applied at Stage 2
  SELECT pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
         'auto_merge', 'stage2', NULL, NULL
  FROM producer_dedup_routing_stage2 WHERE stage2_action='stage2_auto_merge'
  UNION ALL
  SELECT pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
         'auto_skip', 'stage2', NULL, NULL
  FROM producer_dedup_routing_stage2 WHERE stage2_action='stage2_auto_skip'
  UNION ALL
  -- PC user review at Stage 2
  SELECT pair_id, producer_id_a, producer_id_b, name_a, name_b, country,
         'user_review', 'stage2_pc', NULL, NULL
  FROM producer_dedup_routing_stage2 WHERE stage2_action='stage2_user_review_pc'
  UNION ALL
  -- Decided at L3 (Stage 3)
  SELECT r2.pair_id, r2.producer_id_a, r2.producer_id_b,
         r2.name_a, r2.name_b, r2.country,
         CASE
           WHEN l3.verdict='MERGE' AND l3.confidence>=0.92 THEN 'auto_merge'
           WHEN l3.verdict='SKIP'  AND l3.confidence>=0.90 THEN 'auto_skip'
           ELSE 'user_review'
         END AS final_action,
         'stage3' AS decided_at,
         l3.verdict AS l3_verdict,
         l3.confidence AS l3_conf
  FROM producer_dedup_routing_stage2 r2
  LEFT JOIN producer_dedup_pairs l3
    ON l3.producer_id_a = r2.producer_id_a
   AND l3.producer_id_b = r2.producer_id_b
   AND l3.method_name IN ('l3_sonnet_web', 'l3_sonnet_noweb')
  WHERE r2.stage2_action='escalate_to_l3'
)
SELECT * FROM all_pairs;

CREATE INDEX ON producer_dedup_routing_final (final_action);
CREATE INDEX ON producer_dedup_routing_final (pair_id);
```

## Sampling helpers

Pair IDs of Stage 1 escalations:

```sql
SELECT array_agg(pair_id) FROM producer_dedup_routing_stage1
WHERE stage1_action LIKE 'escalate_%';
```

Random 200 auto-SKIPs for SKIP audit:

```sql
SELECT array_agg(pair_id) FROM (
  SELECT pair_id FROM producer_dedup_routing_stage1
  WHERE stage1_action='stage1_auto_skip'
  ORDER BY random() LIMIT 200
) t;
```
