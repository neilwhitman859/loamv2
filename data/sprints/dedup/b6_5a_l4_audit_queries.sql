-- B6.5a Step 10 — L4 Opus-inline audit queries.
-- Run these once Stages 1-3 are populated; results pulled into Opus context window.

-- A) All auto_merge rows with per-tier reasoning
SELECT
  rf.pair_id, rf.decided_at,
  rf.name_a, rf.name_b, rf.country,
  l1.verdict AS l1_v,  l1.confidence AS l1_c,  l1.reasoning AS l1_r,
  g1.verdict AS l15_v, g1.confidence AS l15_c, g1.reasoning AS l15_r,
  l2.verdict AS l2_v,  l2.confidence AS l2_c,  l2.reasoning AS l2_r,
  g2.verdict AS l25_v, g2.confidence AS l25_c, g2.reasoning AS l25_r,
  l3.verdict AS l3_v,  l3.confidence AS l3_c,  l3.reasoning AS l3_r,
  rf.l3_verdict, rf.l3_conf
FROM producer_dedup_routing_final rf
LEFT JOIN producer_dedup_pairs l1  ON l1.producer_id_a=rf.producer_id_a AND l1.producer_id_b=rf.producer_id_b AND l1.method_name='l1_haiku_batch'
LEFT JOIN producer_dedup_pairs g1  ON g1.producer_id_a=rf.producer_id_a AND g1.producer_id_b=rf.producer_id_b AND g1.method_name='l1_gemini_basic'
LEFT JOIN producer_dedup_pairs l2  ON l2.producer_id_a=rf.producer_id_a AND l2.producer_id_b=rf.producer_id_b AND l2.method_name='l2_haiku_rich'
LEFT JOIN producer_dedup_pairs g2  ON g2.producer_id_a=rf.producer_id_a AND g2.producer_id_b=rf.producer_id_b AND g2.method_name='l2_gemini_rich'
LEFT JOIN producer_dedup_pairs l3  ON l3.producer_id_a=rf.producer_id_a AND l3.producer_id_b=rf.producer_id_b AND l3.method_name IN ('l3_sonnet_web','l3_sonnet_noweb')
WHERE rf.final_action = 'auto_merge';

-- B) Similar-shape detection: pairs where both sides share a name prefix
--    (to spot inconsistent handling)
-- Returns groups of 3+ pairs by shared name-prefix signature
SELECT
  LOWER(SUBSTRING(rf.name_a FROM 1 FOR 4)) ||'/'||
  LOWER(SUBSTRING(rf.name_b FROM 1 FOR 4)) AS shape,
  COUNT(*) AS n,
  COUNT(DISTINCT rf.final_action) AS n_actions,
  ARRAY_AGG(DISTINCT rf.final_action) AS actions,
  ARRAY_AGG(rf.pair_id ORDER BY rf.pair_id) AS pair_ids,
  ARRAY_AGG(DISTINCT rf.country) AS countries
FROM producer_dedup_routing_final rf
GROUP BY 1
HAVING COUNT(*) >= 3 AND COUNT(DISTINCT rf.final_action) > 1
ORDER BY n DESC;

-- C) PC patterns — all pairs with at least one PC verdict
SELECT
  rf.pair_id, rf.final_action, rf.decided_at,
  rf.name_a, rf.name_b, rf.country,
  l1.verdict AS l1_v,  l1.confidence AS l1_c,
  g1.verdict AS l15_v, g1.confidence AS l15_c,
  l2.verdict AS l2_v,  l2.confidence AS l2_c,
  g2.verdict AS l25_v, g2.confidence AS l25_c,
  l3.verdict AS l3_v,  l3.confidence AS l3_c,
  l2.reasoning AS l2_reasoning, l3.reasoning AS l3_reasoning
FROM producer_dedup_routing_final rf
LEFT JOIN producer_dedup_pairs l1  ON l1.producer_id_a=rf.producer_id_a AND l1.producer_id_b=rf.producer_id_b AND l1.method_name='l1_haiku_batch'
LEFT JOIN producer_dedup_pairs g1  ON g1.producer_id_a=rf.producer_id_a AND g1.producer_id_b=rf.producer_id_b AND g1.method_name='l1_gemini_basic'
LEFT JOIN producer_dedup_pairs l2  ON l2.producer_id_a=rf.producer_id_a AND l2.producer_id_b=rf.producer_id_b AND l2.method_name='l2_haiku_rich'
LEFT JOIN producer_dedup_pairs g2  ON g2.producer_id_a=rf.producer_id_a AND g2.producer_id_b=rf.producer_id_b AND g2.method_name='l2_gemini_rich'
LEFT JOIN producer_dedup_pairs l3  ON l3.producer_id_a=rf.producer_id_a AND l3.producer_id_b=rf.producer_id_b AND l3.method_name IN ('l3_sonnet_web','l3_sonnet_noweb')
WHERE 'PARENT_CHILD' IN (
  COALESCE(l1.verdict,''), COALESCE(g1.verdict,''),
  COALESCE(l2.verdict,''), COALESCE(g2.verdict,''),
  COALESCE(l3.verdict,'')
);

-- D) Suspicious auto_skip: pairs with any MERGE verdict but final_action auto_skip
SELECT
  rf.pair_id, rf.final_action, rf.decided_at,
  rf.name_a, rf.name_b, rf.country,
  l1.verdict AS l1_v,  l1.confidence AS l1_c,
  g1.verdict AS l15_v, g1.confidence AS l15_c,
  l2.verdict AS l2_v,  l2.confidence AS l2_c,
  g2.verdict AS l25_v, g2.confidence AS l25_c
FROM producer_dedup_routing_final rf
LEFT JOIN producer_dedup_pairs l1  ON l1.producer_id_a=rf.producer_id_a AND l1.producer_id_b=rf.producer_id_b AND l1.method_name='l1_haiku_batch'
LEFT JOIN producer_dedup_pairs g1  ON g1.producer_id_a=rf.producer_id_a AND g1.producer_id_b=rf.producer_id_b AND g1.method_name='l1_gemini_basic'
LEFT JOIN producer_dedup_pairs l2  ON l2.producer_id_a=rf.producer_id_a AND l2.producer_id_b=rf.producer_id_b AND l2.method_name='l2_haiku_rich'
LEFT JOIN producer_dedup_pairs g2  ON g2.producer_id_a=rf.producer_id_a AND g2.producer_id_b=rf.producer_id_b AND g2.method_name='l2_gemini_rich'
WHERE rf.final_action='auto_skip'
  AND 'MERGE' IN (
    COALESCE(l1.verdict,''), COALESCE(g1.verdict,''),
    COALESCE(l2.verdict,''), COALESCE(g2.verdict,'')
  );

-- E) High wine-count auto_merge (higher-stakes checks)
SELECT
  rf.pair_id, rf.final_action, rf.decided_at,
  rf.name_a, rf.name_b, rf.country,
  b.wines_a, b.wines_b,
  l1.verdict AS l1_v,  l1.confidence AS l1_c,
  g1.verdict AS l15_v, g1.confidence AS l15_c,
  l2.verdict AS l2_v,  l2.confidence AS l2_c,
  g2.verdict AS l25_v, g2.confidence AS l25_c
FROM producer_dedup_routing_final rf
JOIN producer_dedup_pairs b
  ON b.id = rf.pair_id AND b.method_name='blocking'
LEFT JOIN producer_dedup_pairs l1  ON l1.producer_id_a=rf.producer_id_a AND l1.producer_id_b=rf.producer_id_b AND l1.method_name='l1_haiku_batch'
LEFT JOIN producer_dedup_pairs g1  ON g1.producer_id_a=rf.producer_id_a AND g1.producer_id_b=rf.producer_id_b AND g1.method_name='l1_gemini_basic'
LEFT JOIN producer_dedup_pairs l2  ON l2.producer_id_a=rf.producer_id_a AND l2.producer_id_b=rf.producer_id_b AND l2.method_name='l2_haiku_rich'
LEFT JOIN producer_dedup_pairs g2  ON g2.producer_id_a=rf.producer_id_a AND g2.producer_id_b=rf.producer_id_b AND g2.method_name='l2_gemini_rich'
WHERE rf.final_action='auto_merge'
  AND (b.wines_a > 20 OR b.wines_b > 20)
ORDER BY GREATEST(b.wines_a, b.wines_b) DESC;
