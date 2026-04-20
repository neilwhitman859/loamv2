"""Sprint 6 Step 7 setup: create sprint6_core_producers table + count Step 7 work."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn

conn = get_conn()
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS sprint6_core_producers")
cur.execute("""
    CREATE TABLE sprint6_core_producers (
      producer_id UUID PRIMARY KEY,
      reasons TEXT[] NOT NULL,
      created_at TIMESTAMPTZ DEFAULT now()
    )
""")

cur.execute("""
    WITH
    us_country AS (
      SELECT p.id FROM producers p JOIN countries c ON c.id=p.country_id WHERE c.iso_code='US'
    ),
    us_staging AS (
      SELECT DISTINCT w.producer_id AS id FROM wines w WHERE w.producer_id IS NOT NULL AND w.id IN (
        SELECT canonical_wine_id FROM source_specs WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_wallys WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_flatiron WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_skurnik WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_kermit_lynch WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_empson WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_winebow WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_european_cellars WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_polaner WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_domestique WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_best_wine_store WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_firstleaf WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_last_bottle WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_winedeals WHERE canonical_wine_id IS NOT NULL
      )
    ),
    ttb_linked AS (
      SELECT DISTINCT canonical_producer_id AS id FROM source_ttb_colas WHERE canonical_producer_id IS NOT NULL
    ),
    wine_counts AS (
      SELECT p.id, COUNT(w.id) AS wc FROM producers p LEFT JOIN wines w ON w.producer_id=p.id GROUP BY p.id
    ),
    has_scores AS (
      SELECT DISTINCT w.producer_id AS id FROM wines w
      WHERE w.producer_id IS NOT NULL
        AND EXISTS(SELECT 1 FROM wine_vintage_scores s WHERE s.wine_id=w.id)
    ),
    has_prices AS (
      SELECT DISTINCT w.producer_id AS id FROM wines w
      WHERE w.producer_id IS NOT NULL
        AND EXISTS(SELECT 1 FROM wine_vintage_prices pr WHERE pr.wine_id=w.id)
    ),
    intl_retail AS (
      SELECT DISTINCT w.producer_id AS id FROM wines w WHERE w.producer_id IS NOT NULL AND w.id IN (
        SELECT canonical_wine_id FROM source_lcbo WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_systembolaget WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_bc_liquor WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_pa WHERE canonical_wine_id IS NOT NULL
      )
    ),
    competitions AS (
      SELECT DISTINCT w.producer_id AS id FROM wines w WHERE w.producer_id IS NOT NULL AND w.id IN (
        SELECT canonical_wine_id FROM source_berliner WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_texsom WHERE canonical_wine_id IS NOT NULL
        UNION SELECT canonical_wine_id FROM source_enofile WHERE canonical_wine_id IS NOT NULL
      )
    )
    INSERT INTO sprint6_core_producers (producer_id, reasons)
    SELECT
      p.id,
      ARRAY_REMOVE(ARRAY[
        CASE WHEN p.id IN (SELECT id FROM us_country) THEN 'us_country' END,
        CASE WHEN p.id IN (SELECT id FROM us_staging) THEN 'us_staging' END,
        CASE WHEN p.id IN (SELECT id FROM ttb_linked) THEN 'ttb_cola' END,
        CASE WHEN wc.wc >= 5 THEN 'wines_5plus' END,
        CASE WHEN p.id IN (SELECT id FROM has_scores) THEN 'has_scores' END,
        CASE WHEN p.id IN (SELECT id FROM has_prices) THEN 'has_prices' END,
        CASE WHEN p.id IN (SELECT id FROM intl_retail) THEN 'intl_retail' END,
        CASE WHEN p.id IN (SELECT id FROM competitions) THEN 'competitions' END
      ], NULL) AS reasons
    FROM producers p
    LEFT JOIN wine_counts wc ON wc.id = p.id
    WHERE p.id IN (SELECT id FROM us_country)
       OR p.id IN (SELECT id FROM us_staging)
       OR p.id IN (SELECT id FROM ttb_linked)
       OR wc.wc >= 5
       OR p.id IN (SELECT id FROM has_scores)
       OR p.id IN (SELECT id FROM has_prices)
       OR p.id IN (SELECT id FROM intl_retail)
       OR p.id IN (SELECT id FROM competitions)
""")
conn.commit()

cur.execute("SELECT COUNT(*) FROM sprint6_core_producers")
n = cur.fetchone()[0]
print(f"Expanded Core locked: {n:,} producers")

cur.execute("SELECT COUNT(*) FROM wines WHERE producer_id IN (SELECT producer_id FROM sprint6_core_producers)")
wines = cur.fetchone()[0]
print(f"Wines under Expanded Core: {wines:,}")

cur.execute("""
    WITH core_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b, b.country
        FROM producer_dedup_pairs b
        WHERE b.method_name = 'blocking'
          AND (b.producer_id_a IN (SELECT producer_id FROM sprint6_core_producers)
               OR b.producer_id_b IN (SELECT producer_id FROM sprint6_core_producers))
    )
    SELECT
      CASE
        WHEN w.id IS NOT NULL THEN 'already_web_validated'
        WHEN s2.stage2_action IS NOT NULL THEN 's2:' || s2.stage2_action
        WHEN s1.stage1_action IS NOT NULL THEN 's1:' || s1.stage1_action
        ELSE 'missing_routing'
      END AS bucket,
      COUNT(*)
    FROM core_pairs cp
    LEFT JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = cp.pair_id
    LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = cp.pair_id
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = cp.producer_id_a
      AND w.producer_id_b = cp.producer_id_b
    GROUP BY 1 ORDER BY 2 DESC
""")
print()
print("Expanded Core pairs by current state:")
total = 0
for row in cur.fetchall():
    print(f"  {row[0]:<35} {row[1]:,}")
    total += row[1]
print(f"  TOTAL: {total:,}")

cur.execute("""
    WITH core_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b
        FROM producer_dedup_pairs b
        WHERE b.method_name = 'blocking'
          AND (b.producer_id_a IN (SELECT producer_id FROM sprint6_core_producers)
               OR b.producer_id_b IN (SELECT producer_id FROM sprint6_core_producers))
    )
    SELECT COUNT(*)
    FROM core_pairs cp
    LEFT JOIN producer_dedup_routing_stage1 s1 ON s1.pair_id = cp.pair_id
    LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = cp.pair_id
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = cp.producer_id_a
      AND w.producer_id_b = cp.producer_id_b
    WHERE w.id IS NULL
      AND COALESCE(s2.stage2_action, s1.stage1_action, 'missing') NOT IN ('stage1_auto_skip', 'stage2_auto_skip')
""")
core_step7 = cur.fetchone()[0]
print()
print(f"Core pairs needing Step 7 web validation (escalations + PC + missing): {core_step7:,}")
print(f"  Cost at $0.006/pair: ${core_step7*0.006:.2f}")

cur.execute("""
    WITH tail_pairs AS (
        SELECT b.id AS pair_id, b.producer_id_a, b.producer_id_b
        FROM producer_dedup_pairs b
        WHERE b.method_name = 'blocking'
          AND b.producer_id_a NOT IN (SELECT producer_id FROM sprint6_core_producers)
          AND b.producer_id_b NOT IN (SELECT producer_id FROM sprint6_core_producers)
    )
    SELECT COUNT(*)
    FROM tail_pairs tp
    LEFT JOIN producer_dedup_routing_stage2 s2 ON s2.pair_id = tp.pair_id
    LEFT JOIN producer_dedup_pairs w
      ON w.method_name='l2_haiku_rich_web'
      AND w.producer_id_a = tp.producer_id_a
      AND w.producer_id_b = tp.producer_id_b
    WHERE s2.stage2_action = 'escalate_to_l3' AND w.id IS NULL
""")
tail_esc = cur.fetchone()[0]
print(f"Tail escalate_to_l3 pairs needing Step 7 web validation: {tail_esc:,}")
print(f"  Cost at $0.006/pair: ${tail_esc*0.006:.2f}")

print()
print(f"Step 7 total pair count: {core_step7 + tail_esc:,}")
print(f"Step 7 total cost: ${(core_step7 + tail_esc)*0.006:.2f}")

cur.close(); conn.close()
