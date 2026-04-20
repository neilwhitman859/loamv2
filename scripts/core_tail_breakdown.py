"""Ad-hoc: Core/Tail breakdown analysis for Sprint 6 Step 7 planning."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn

conn = get_conn()
cur = conn.cursor()

cur.execute("""
    CREATE TEMP TABLE core_producers AS
    WITH us_country AS (
      SELECT p.id FROM producers p
      JOIN countries c ON c.id = p.country_id WHERE c.iso_code = 'US'
    ),
    us_staging AS (
      SELECT DISTINCT w.producer_id AS id FROM wines w
      WHERE w.producer_id IS NOT NULL AND w.id IN (
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
      SELECT DISTINCT canonical_producer_id AS id FROM source_ttb_colas
      WHERE canonical_producer_id IS NOT NULL
    )
    SELECT id FROM us_country UNION SELECT id FROM us_staging UNION SELECT id FROM ttb_linked
""")

def q1(sql, *args):
    cur.execute(sql, args)
    return cur.fetchone()[0]

print("=== PRODUCER SPLIT ===")
total_p = q1("SELECT COUNT(*) FROM producers")
core_p = q1("SELECT COUNT(*) FROM core_producers")
print(f"Total producers: {total_p:,}")
print(f"  Core: {core_p:,} ({100*core_p/total_p:.1f}%)")
print(f"  Tail: {total_p - core_p:,} ({100*(total_p-core_p)/total_p:.1f}%)")

print()
print("=== WINE SPLIT ===")
total_w = q1("SELECT COUNT(*) FROM wines")
core_w = q1("SELECT COUNT(*) FROM wines WHERE producer_id IN (SELECT id FROM core_producers)")
print(f"Total wines: {total_w:,}")
print(f"  Core-producer wines: {core_w:,} ({100*core_w/total_w:.1f}%)")
print(f"  Tail-producer wines: {total_w - core_w:,} ({100*(total_w-core_w)/total_w:.1f}%)")

print()
print("=== WHY EACH CORE PRODUCER IS CORE (overlap counts) ===")
us_country = q1("""SELECT COUNT(*) FROM producers p JOIN countries c ON c.id = p.country_id WHERE c.iso_code='US'""")
us_stag = q1("""SELECT COUNT(DISTINCT w.producer_id) FROM wines w WHERE w.producer_id IN (SELECT id FROM core_producers)
               AND w.id IN (
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
               )""")
ttb = q1("""SELECT COUNT(DISTINCT canonical_producer_id) FROM source_ttb_colas WHERE canonical_producer_id IS NOT NULL""")
print(f"  US country code:         {us_country:,}")
print(f"  In US retail/importer:   {us_stag:,}")
print(f"  Linked to TTB COLA:      {ttb:,}")

print()
print("=== CORE: by country (top 15) ===")
cur.execute("""
    SELECT COALESCE(c.iso_code, 'UNKNOWN') AS cc, COUNT(*) AS n
    FROM core_producers cp
    JOIN producers p ON p.id = cp.id
    LEFT JOIN countries c ON c.id = p.country_id
    GROUP BY cc ORDER BY n DESC LIMIT 15
""")
for row in cur.fetchall():
    print(f"  {row[0]:<6} {row[1]:,}")

print()
print("=== TAIL: by country (top 15) ===")
cur.execute("""
    SELECT COALESCE(c.iso_code, 'UNKNOWN') AS cc, COUNT(*) AS n
    FROM producers p
    LEFT JOIN countries c ON c.id = p.country_id
    WHERE p.id NOT IN (SELECT id FROM core_producers)
    GROUP BY cc ORDER BY n DESC LIMIT 15
""")
for row in cur.fetchall():
    print(f"  {row[0]:<6} {row[1]:,}")

print()
print("=== TAIL: wines per producer distribution ===")
cur.execute("""
    WITH tail_stats AS (
      SELECT p.id, COUNT(w.id) AS wc
      FROM producers p LEFT JOIN wines w ON w.producer_id = p.id
      WHERE p.id NOT IN (SELECT id FROM core_producers)
      GROUP BY p.id
    )
    SELECT
      CASE
        WHEN wc = 0 THEN '0 (no wines)'
        WHEN wc = 1 THEN '1'
        WHEN wc = 2 THEN '2'
        WHEN wc BETWEEN 3 AND 5 THEN '3-5'
        WHEN wc BETWEEN 6 AND 10 THEN '6-10'
        WHEN wc BETWEEN 11 AND 25 THEN '11-25'
        ELSE '26+'
      END AS bucket, COUNT(*) AS n
    FROM tail_stats GROUP BY 1 ORDER BY MIN(wc)
""")
for row in cur.fetchall():
    print(f"  {row[0]:<15} {row[1]:,}")

print()
print("=== HYPOTHETICAL MORE-INCLUSIVE CORE (adding each criterion) ===")
print("Non-Core producers who would become Core if we add...")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND EXISTS(SELECT 1 FROM external_ids e JOIN wines w ON w.id = e.entity_id
                     WHERE w.producer_id = p.id AND e.entity_type='wine' AND e.system='LWIN')""")
print(f"  +any LWIN-backed wine:                          +{n:,}")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND EXISTS(SELECT 1 FROM wine_vintage_scores s JOIN wines w ON w.id = s.wine_id
                     WHERE w.producer_id = p.id)""")
print(f"  +producers with any score:                      +{n:,}")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND EXISTS(SELECT 1 FROM wine_vintage_prices pr JOIN wines w ON w.id = pr.wine_id
                     WHERE w.producer_id = p.id)""")
print(f"  +producers with any price:                      +{n:,}")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) >= 3""")
print(f"  +producers with 3+ wines:                       +{n:,}")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) >= 5""")
print(f"  +producers with 5+ wines:                       +{n:,}")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND EXISTS(SELECT 1 FROM wines w WHERE w.producer_id = p.id AND (
            w.id IN (SELECT canonical_wine_id FROM source_lcbo WHERE canonical_wine_id IS NOT NULL)
            OR w.id IN (SELECT canonical_wine_id FROM source_systembolaget WHERE canonical_wine_id IS NOT NULL)
            OR w.id IN (SELECT canonical_wine_id FROM source_bc_liquor WHERE canonical_wine_id IS NOT NULL)
            OR w.id IN (SELECT canonical_wine_id FROM source_pa WHERE canonical_wine_id IS NOT NULL)
          ))""")
print(f"  +international retailer (LCBO/Systemb/BC/PA):   +{n:,}")

n = q1("""SELECT COUNT(*) FROM producers p WHERE p.id NOT IN (SELECT id FROM core_producers)
          AND EXISTS(SELECT 1 FROM wines w WHERE w.producer_id = p.id AND (
            w.id IN (SELECT canonical_wine_id FROM source_berliner WHERE canonical_wine_id IS NOT NULL)
            OR w.id IN (SELECT canonical_wine_id FROM source_texsom WHERE canonical_wine_id IS NOT NULL)
            OR w.id IN (SELECT canonical_wine_id FROM source_enofile WHERE canonical_wine_id IS NOT NULL)
          ))""")
print(f"  +competition data (Berliner/TEXSOM/Enofile):    +{n:,}")

# Union of multiple "soft" criteria = a reasonable expanded Core
cur.execute("""
    CREATE TEMP TABLE expanded_core AS
    SELECT id FROM core_producers
    UNION
    SELECT p.id FROM producers p WHERE
      EXISTS(SELECT 1 FROM external_ids e JOIN wines w ON w.id = e.entity_id
             WHERE w.producer_id = p.id AND e.entity_type='wine' AND e.system='LWIN')
      OR EXISTS(SELECT 1 FROM wine_vintage_scores s JOIN wines w ON w.id = s.wine_id WHERE w.producer_id = p.id)
      OR EXISTS(SELECT 1 FROM wine_vintage_prices pr JOIN wines w ON w.id = pr.wine_id WHERE w.producer_id = p.id)
""")
exp_n = q1("SELECT COUNT(*) FROM expanded_core")
print()
print(f"=== EXPANDED CORE = original + (any LWIN OR score OR price): {exp_n:,} of {total_p:,} ({100*exp_n/total_p:.1f}%) ===")
exp_w = q1("SELECT COUNT(*) FROM wines WHERE producer_id IN (SELECT id FROM expanded_core)")
print(f"  Wines under Expanded Core: {exp_w:,} of {total_w:,} ({100*exp_w/total_w:.1f}%)")

# Pairs
cur.execute("""
    SELECT COUNT(*) FROM producer_dedup_pairs b
    WHERE b.method_name = 'blocking'
      AND (b.producer_id_a IN (SELECT id FROM expanded_core)
           OR b.producer_id_b IN (SELECT id FROM expanded_core))
""")
exp_pairs = cur.fetchone()[0]
print(f"  Dedup pairs involving Expanded Core: {exp_pairs:,} of 151,150 ({100*exp_pairs/151150:.1f}%)")

# Cost delta to validate
delta_pairs = exp_pairs - 84043
print(f"  DELTA pairs vs original Core: +{delta_pairs:,}")
print(f"  Cost to web-validate the delta at $0.006/pair: ${delta_pairs*0.006:.0f}")

cur.close(); conn.close()
