"""
B6.3 Part C — Producer dedup blocking (no LLM calls).

Emits candidate pairs into producer_dedup_pairs with method_name='blocking'.
Each row merges per-strategy evidence into the signals jsonb. Reports
per-strategy counts, unique catches, and the union total.

Strategies (per data/sprints/dedup/plan.md):
  1 exact_normalized     Same country + exact normalized name
  2 trigram              Same country + pg_trgm similarity >= 0.3
  3 embeddings           SKIPPED — no producer embedding column yet
  4 first3_tokens        Same country + first-3-char match + >=1 shared token
  5 shared_wine_lwin     Two producers share >=1 wine carrying same LWIN_7
  6 shared_ttb_permit    Two producers share a TTB permit_no (US only)
  7 cross_country_strong Cross-country pair with shared LWIN OR >=5 shared wines
  8 catalog_overlap      Shared-wine-name overlap >= 30% of the smaller catalog
  9 substring_contain    Same country + token or char substring containment

Run:
    python -m pipeline.identity.producer_blocking               # full run, writes rows
    python -m pipeline.identity.producer_blocking --analyze     # count only, no writes
    python -m pipeline.identity.producer_blocking --strategies 1,2,6,9
    python -m pipeline.identity.producer_blocking --clear       # delete prior blocking rows first
    python -m pipeline.identity.producer_blocking --report-only # report existing counts, no new run

Writes to `producer_dedup_pairs` with method_name='blocking'. ON CONFLICT
merges signals so a pair caught by multiple strategies gets one row with
all signal keys populated.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pipeline.lib.db import get_conn


STRATEGIES = {
    1:  ("exact_normalized",     "Same country + exact normalized name"),
    2:  ("trigram",              "Same country + pg_trgm similarity >= 0.35"),
    4:  ("first3_tokens",        "(dropped) First-3-char too permissive at 33K scale"),
    5:  ("shared_wine_lwin",     "Producers share >=1 wine carrying same LWIN_7"),
    6:  ("shared_ttb_permit",    "Producers share a TTB BW permit (capped 10/permit)"),
    7:  ("cross_country_strong", "Cross-country + exact name OR trigram >=0.5 OR shared LWIN"),
    8:  ("catalog_overlap",      "Shared distinguishing wine names >=30% overlap"),
    9:  ("substring_contain",    "Same country + substring containment of normalized name"),
    10: ("shared_rare_wine",     "Producers share a wine name appearing in <=5 producers"),
    11: ("cross_word_subset",    "Cross-country + one full name is a token in the other"),
}


UPSERT_PREFIX = """
INSERT INTO producer_dedup_pairs
  (producer_id_a, producer_id_b, name_a, name_b, country, similarity,
   wines_a, wines_b, method_name, signals, created_at, updated_at)
"""
UPSERT_SUFFIX = """
ON CONFLICT (producer_id_a, producer_id_b, method_name)
DO UPDATE SET
  signals = COALESCE(producer_dedup_pairs.signals, '{}'::jsonb) || EXCLUDED.signals,
  similarity = GREATEST(COALESCE(producer_dedup_pairs.similarity, 0), EXCLUDED.similarity),
  wines_a = GREATEST(COALESCE(producer_dedup_pairs.wines_a, 0), EXCLUDED.wines_a),
  wines_b = GREATEST(COALESCE(producer_dedup_pairs.wines_b, 0), EXCLUDED.wines_b),
  name_a = COALESCE(producer_dedup_pairs.name_a, EXCLUDED.name_a),
  name_b = COALESCE(producer_dedup_pairs.name_b, EXCLUDED.name_b),
  country = COALESCE(producer_dedup_pairs.country, EXCLUDED.country),
  updated_at = now();
"""


def _wine_counts_cte() -> str:
    """CTE that gives wine count per active producer."""
    return """
    WITH wine_counts AS (
      SELECT producer_id, COUNT(*)::int AS cnt
      FROM wines
      WHERE deleted_at IS NULL AND producer_id IS NOT NULL
      GROUP BY producer_id
    )
    """


def run_strategy_1(cur, write: bool) -> int:
    """Same country + exact normalized name."""
    sql = _wine_counts_cte() + """,
    pairs AS (
      SELECT p1.id AS id_a, p2.id AS id_b,
             p1.name AS name_a, p2.name AS name_b,
             COALESCE(c.iso_code, 'NULL') AS country,
             1.0::numeric AS similarity,
             COALESCE(wc1.cnt, 0) AS wines_a,
             COALESCE(wc2.cnt, 0) AS wines_b
      FROM producers p1
      JOIN producers p2
        ON p1.name_normalized = p2.name_normalized
       AND p1.country_id IS NOT DISTINCT FROM p2.country_id
       AND p1.id < p2.id
      LEFT JOIN countries c ON c.id = p1.country_id
      LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
      LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
      WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
        AND length(p1.name_normalized) >= 2
    )
    SELECT COUNT(*) FROM pairs;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + _wine_counts_cte() + """,
          pairs AS (
            SELECT p1.id AS id_a, p2.id AS id_b,
                   p1.name AS name_a, p2.name AS name_b,
                   COALESCE(c.iso_code, 'NULL') AS country,
                   1.0::numeric AS similarity,
                   COALESCE(wc1.cnt, 0) AS wines_a,
                   COALESCE(wc2.cnt, 0) AS wines_b
            FROM producers p1
            JOIN producers p2
              ON p1.name_normalized = p2.name_normalized
             AND p1.country_id IS NOT DISTINCT FROM p2.country_id
             AND p1.id < p2.id
            LEFT JOIN countries c ON c.id = p1.country_id
            LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
            LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
            WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
              AND length(p1.name_normalized) >= 2
          )
          SELECT id_a, id_b, name_a, name_b, country, similarity,
                 wines_a, wines_b, 'blocking',
                 jsonb_build_object('s1_exact', true),
                 now(), now()
          FROM pairs
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_2(cur, write: bool, threshold: float = 0.35) -> int:
    """Same country + pg_trgm similarity >= threshold (uses gin_trgm_ops index).

    We set pg_trgm.similarity_threshold to `threshold` inside the session so
    the `%` operator aligns with our filter. This uses the idx_producers_name_trgm
    GIN index for pair pre-filter.
    """
    cur.execute(f"SET pg_trgm.similarity_threshold = {threshold};")
    count_sql = _wine_counts_cte() + f""",
    pairs AS (
      SELECT p1.id AS id_a, p2.id AS id_b,
             p1.name AS name_a, p2.name AS name_b,
             COALESCE(c.iso_code, 'NULL') AS country,
             similarity(p1.name_normalized, p2.name_normalized) AS sim,
             COALESCE(wc1.cnt, 0) AS wines_a,
             COALESCE(wc2.cnt, 0) AS wines_b
      FROM producers p1
      JOIN producers p2
        ON p1.country_id IS NOT DISTINCT FROM p2.country_id
       AND p1.id < p2.id
       AND p1.name_normalized % p2.name_normalized
      LEFT JOIN countries c ON c.id = p1.country_id
      LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
      LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
      WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
        AND length(p1.name_normalized) >= 3
        AND length(p2.name_normalized) >= 3
        AND similarity(p1.name_normalized, p2.name_normalized) >= {threshold}
        AND p1.name_normalized <> p2.name_normalized  -- already covered by S1
    )
    SELECT COUNT(*) FROM pairs;
    """
    cur.execute(count_sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + _wine_counts_cte() + f""",
          pairs AS (
            SELECT p1.id AS id_a, p2.id AS id_b,
                   p1.name AS name_a, p2.name AS name_b,
                   COALESCE(c.iso_code, 'NULL') AS country,
                   similarity(p1.name_normalized, p2.name_normalized) AS sim,
                   COALESCE(wc1.cnt, 0) AS wines_a,
                   COALESCE(wc2.cnt, 0) AS wines_b
            FROM producers p1
            JOIN producers p2
              ON p1.country_id IS NOT DISTINCT FROM p2.country_id
             AND p1.id < p2.id
             AND p1.name_normalized % p2.name_normalized
            LEFT JOIN countries c ON c.id = p1.country_id
            LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
            LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
            WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
              AND length(p1.name_normalized) >= 3
              AND length(p2.name_normalized) >= 3
              AND similarity(p1.name_normalized, p2.name_normalized) >= {threshold}
              AND p1.name_normalized <> p2.name_normalized
          )
          SELECT id_a, id_b, name_a, name_b, country, sim AS similarity,
                 wines_a, wines_b, 'blocking',
                 jsonb_build_object('s2_trigram', round(sim::numeric, 4)),
                 now(), now()
          FROM pairs
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_4(cur, write: bool, max_token_frequency: int = 50) -> int:
    """Same country + first-3-char match + shared distinguishing token.

    A "distinguishing" token is a long token (>= 4 chars) that appears in
    at most `max_token_frequency` producers globally. Filters out common
    connective words like "domaine" (thousands of French producers),
    "chateau", "winery", "vineyards", "bodegas" which would otherwise make
    every pair of producers with the same first-3-char a candidate.

    Catches word-reordering variants that trigram misses when the
    distinguishing tokens are in different positions.
    """
    sql = _wine_counts_cte() + f""",
    token_freq AS (
      SELECT t, COUNT(*) AS n
      FROM (
        SELECT unnest(string_to_array(name_normalized, ' ')) AS t
        FROM producers WHERE deleted_at IS NULL
      ) tt
      WHERE length(t) >= 4
      GROUP BY t
    ),
    tok AS (
      SELECT p.id, p.name, p.name_normalized, p.country_id,
             ARRAY(
               SELECT tt FROM unnest(string_to_array(p.name_normalized, ' ')) AS tt
               WHERE length(tt) >= 4
                 AND (SELECT n FROM token_freq WHERE t = tt) <= {max_token_frequency}
             ) AS dist_toks,
             substr(p.name_normalized, 1, 3) AS first3
      FROM producers p
      WHERE p.deleted_at IS NULL AND length(p.name_normalized) >= 3
    ),
    pairs AS (
      SELECT p1.id AS id_a, p2.id AS id_b,
             (SELECT name FROM producers WHERE id = p1.id) AS name_a,
             (SELECT name FROM producers WHERE id = p2.id) AS name_b,
             COALESCE(c.iso_code, 'NULL') AS country,
             similarity(p1.name_normalized, p2.name_normalized) AS sim,
             COALESCE(wc1.cnt, 0) AS wines_a,
             COALESCE(wc2.cnt, 0) AS wines_b,
             (SELECT array_length(ARRAY(
                SELECT unnest(p1.dist_toks) INTERSECT SELECT unnest(p2.dist_toks)
              ), 1)) AS shared_dist_tokens
      FROM tok p1
      JOIN tok p2
        ON p1.country_id IS NOT DISTINCT FROM p2.country_id
       AND p1.first3 = p2.first3
       AND p1.id < p2.id
      LEFT JOIN countries c ON c.id = p1.country_id
      LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
      LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
      WHERE array_length(p1.dist_toks, 1) >= 1
        AND array_length(p2.dist_toks, 1) >= 1
    )
    SELECT COUNT(*) FROM pairs WHERE shared_dist_tokens >= 1;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + _wine_counts_cte() + f""",
          token_freq AS (
            SELECT t, COUNT(*) AS n
            FROM (
              SELECT unnest(string_to_array(name_normalized, ' ')) AS t
              FROM producers WHERE deleted_at IS NULL
            ) tt
            WHERE length(t) >= 4
            GROUP BY t
          ),
          tok AS (
            SELECT p.id, p.name, p.name_normalized, p.country_id,
                   ARRAY(
                     SELECT tt FROM unnest(string_to_array(p.name_normalized, ' ')) AS tt
                     WHERE length(tt) >= 4
                       AND (SELECT n FROM token_freq WHERE t = tt) <= {max_token_frequency}
                   ) AS dist_toks,
                   substr(p.name_normalized, 1, 3) AS first3
            FROM producers p
            WHERE p.deleted_at IS NULL AND length(p.name_normalized) >= 3
          ),
          pairs AS (
            SELECT p1.id AS id_a, p2.id AS id_b,
                   (SELECT name FROM producers WHERE id = p1.id) AS name_a,
                   (SELECT name FROM producers WHERE id = p2.id) AS name_b,
                   COALESCE(c.iso_code, 'NULL') AS country,
                   similarity(p1.name_normalized, p2.name_normalized) AS sim,
                   COALESCE(wc1.cnt, 0) AS wines_a,
                   COALESCE(wc2.cnt, 0) AS wines_b,
                   (SELECT array_length(ARRAY(
                      SELECT unnest(p1.long_toks) INTERSECT SELECT unnest(p2.long_toks)
                    ), 1)) AS shared_long_tokens
            FROM tok p1
            JOIN tok p2
              ON p1.country_id IS NOT DISTINCT FROM p2.country_id
             AND p1.first3 = p2.first3
             AND p1.id < p2.id
            LEFT JOIN countries c ON c.id = p1.country_id
            LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
            LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
            WHERE array_length(p1.long_toks, 1) >= 1
              AND array_length(p2.long_toks, 1) >= 1
          )
          SELECT id_a, id_b, name_a, name_b, country, sim AS similarity,
                 wines_a, wines_b, 'blocking',
                 jsonb_build_object('s4_first3_tokens', jsonb_build_object(
                   'shared_dist_tokens', shared_dist_tokens,
                   'trigram', round(sim::numeric, 4)
                 )),
                 now(), now()
          FROM pairs WHERE shared_dist_tokens >= 1
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_5(cur, write: bool) -> int:
    """Two producers share >=1 wine that carries the same LWIN_7.

    Interpretation: producer-level external_ids are empty, but wine-level
    LWIN_7 codes are widespread (157K wines). If two distinct producer rows
    both own a wine with the same LWIN_7, that's very strong identity
    evidence — LWIN assigns one wine per (producer, wine) combination.
    """
    sql = """
    WITH producer_lwins AS (
      SELECT DISTINCT w.producer_id, ei.external_id AS lwin
      FROM external_ids ei
      JOIN wines w ON w.id = ei.entity_id
      WHERE ei.entity_type = 'wine' AND ei.system = 'lwin_7'
        AND w.deleted_at IS NULL AND w.producer_id IS NOT NULL
    ),
    shared AS (
      SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
             jsonb_agg(DISTINCT p1.lwin) AS shared_lwins,
             COUNT(DISTINCT p1.lwin) AS n
      FROM producer_lwins p1
      JOIN producer_lwins p2 ON p1.lwin = p2.lwin AND p1.producer_id < p2.producer_id
      GROUP BY p1.producer_id, p2.producer_id
    )
    SELECT COUNT(*) FROM shared;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + """
          WITH producer_lwins AS (
            SELECT DISTINCT w.producer_id, ei.external_id AS lwin
            FROM external_ids ei
            JOIN wines w ON w.id = ei.entity_id
            WHERE ei.entity_type = 'wine' AND ei.system = 'lwin_7'
              AND w.deleted_at IS NULL AND w.producer_id IS NOT NULL
          ),
          shared AS (
            SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
                   jsonb_agg(DISTINCT p1.lwin) AS shared_lwins,
                   COUNT(DISTINCT p1.lwin)::int AS n
            FROM producer_lwins p1
            JOIN producer_lwins p2 ON p1.lwin = p2.lwin AND p1.producer_id < p2.producer_id
            GROUP BY p1.producer_id, p2.producer_id
          ),
          wc AS (
            SELECT producer_id, COUNT(*)::int AS cnt FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
            GROUP BY producer_id
          )
          SELECT s.id_a, s.id_b,
                 pa.name, pb.name,
                 COALESCE(c.iso_code, 'NULL'),
                 1.0::numeric,
                 COALESCE(wca.cnt, 0), COALESCE(wcb.cnt, 0),
                 'blocking',
                 jsonb_build_object('s5_shared_wine_lwin', jsonb_build_object(
                   'n', s.n, 'lwins', s.shared_lwins
                 )),
                 now(), now()
          FROM shared s
          JOIN producers pa ON pa.id = s.id_a
          JOIN producers pb ON pb.id = s.id_b
          LEFT JOIN countries c ON c.id = pa.country_id
          LEFT JOIN wc wca ON wca.producer_id = s.id_a
          LEFT JOIN wc wcb ON wcb.producer_id = s.id_b
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_6(cur, write: bool, max_producers_per_permit: int = 10) -> int:
    """Two producers share a TTB Bonded Winery (BW) permit_no.

    Only `BW-*` and `<ST>-BW-*` permit formats identify an actual producer
    entity. `I-*` (Importer) and `W-*` (Wholesaler) permits identify the
    filer of the COLA, not the producer — an importer submits COLAs for
    hundreds of distinct foreign producers, so shared importer permit is
    NOT a MERGE signal. Strategy 6 filters to BW permits only.

    Also caps at max_producers_per_permit distinct producers per permit:
    large custom-crush facilities (e.g., Bronco at BW-CA-9 with 137 producers)
    bottle for dozens of independent brands, and the pairs generated are
    mostly SKIP. We keep the 2-to-N cluster size where a shared BW permit
    is a strong MERGE signal (small estate with second label, owner/négociant
    with estate line, etc.).
    """
    sql = f"""
    WITH producer_permits AS (
      SELECT DISTINCT canonical_producer_id AS producer_id,
             COALESCE(permit_no, permit_number) AS permit
      FROM source_ttb_colas
      WHERE canonical_producer_id IS NOT NULL
        AND (permit_no IS NOT NULL OR permit_number IS NOT NULL)
        AND (
          COALESCE(permit_no, permit_number) ~ '^BW-'
          OR COALESCE(permit_no, permit_number) ~ '^[A-Z]{{2,3}}-BW-'
          OR COALESCE(permit_no, permit_number) ~ '^[A-Z]{{2,3}}-BWC-'
          OR COALESCE(permit_no, permit_number) ~ '^[A-Z]{{2,3}}-BWN-'
        )
    ),
    permit_sizes AS (
      SELECT permit, COUNT(DISTINCT producer_id) AS n_producers
      FROM producer_permits GROUP BY permit
      HAVING COUNT(DISTINCT producer_id) BETWEEN 2 AND {max_producers_per_permit}
    ),
    filtered_permits AS (
      SELECT pp.producer_id, pp.permit
      FROM producer_permits pp
      JOIN permit_sizes ps ON ps.permit = pp.permit
    ),
    shared AS (
      SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
             jsonb_agg(DISTINCT p1.permit) AS shared_permits,
             COUNT(DISTINCT p1.permit)::int AS n
      FROM filtered_permits p1
      JOIN filtered_permits p2 ON p1.permit = p2.permit AND p1.producer_id < p2.producer_id
      GROUP BY p1.producer_id, p2.producer_id
    )
    SELECT COUNT(*) FROM shared;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + f"""
          WITH producer_permits AS (
            SELECT DISTINCT canonical_producer_id AS producer_id,
                   COALESCE(permit_no, permit_number) AS permit
            FROM source_ttb_colas
            WHERE canonical_producer_id IS NOT NULL
              AND (permit_no IS NOT NULL OR permit_number IS NOT NULL)
              AND (
                COALESCE(permit_no, permit_number) ~ '^BW-'
                OR COALESCE(permit_no, permit_number) ~ '^[A-Z]{{2,3}}-BW-'
                OR COALESCE(permit_no, permit_number) ~ '^[A-Z]{{2,3}}-BWC-'
                OR COALESCE(permit_no, permit_number) ~ '^[A-Z]{{2,3}}-BWN-'
              )
          ),
          permit_sizes AS (
            SELECT permit, COUNT(DISTINCT producer_id) AS n_producers
            FROM producer_permits GROUP BY permit
            HAVING COUNT(DISTINCT producer_id) BETWEEN 2 AND {max_producers_per_permit}
          ),
          filtered_permits AS (
            SELECT pp.producer_id, pp.permit
            FROM producer_permits pp
            JOIN permit_sizes ps ON ps.permit = pp.permit
          ),
          shared AS (
            SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
                   jsonb_agg(DISTINCT p1.permit) AS shared_permits,
                   COUNT(DISTINCT p1.permit)::int AS n
            FROM filtered_permits p1
            JOIN filtered_permits p2 ON p1.permit = p2.permit AND p1.producer_id < p2.producer_id
            GROUP BY p1.producer_id, p2.producer_id
          ),
          wc AS (
            SELECT producer_id, COUNT(*)::int AS cnt FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
            GROUP BY producer_id
          )
          SELECT s.id_a, s.id_b,
                 pa.name, pb.name,
                 COALESCE(c.iso_code, 'NULL'),
                 1.0::numeric,
                 COALESCE(wca.cnt, 0), COALESCE(wcb.cnt, 0),
                 'blocking',
                 jsonb_build_object('s6_ttb_permit', jsonb_build_object(
                   'n', s.n, 'permits', s.shared_permits
                 )),
                 now(), now()
          FROM shared s
          JOIN producers pa ON pa.id = s.id_a
          JOIN producers pb ON pb.id = s.id_b
          LEFT JOIN countries c ON c.id = pa.country_id
          LEFT JOIN wc wca ON wca.producer_id = s.id_a
          LEFT JOIN wc wcb ON wcb.producer_id = s.id_b
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_7(cur, write: bool, trigram_threshold: float = 0.5) -> int:
    """Cross-country pair with strong signal: exact normalized name OR high trigram.

    Catches dupes that B6.2's country-aware matching split into separate
    rows: Cupcake Vineyards [US/IT/NZ], Josh [US/IT], etc. Also catches
    shared LWIN_7 across countries.

    Signal ladder:
      7a. Cross-country + identical normalized name (highest confidence)
      7b. Cross-country + trigram >= trigram_threshold
      7c. Cross-country + shared wine-LWIN_7
    """
    cur.execute(f"SET pg_trgm.similarity_threshold = {trigram_threshold};")
    sql = f"""
    WITH producer_lwins AS (
      SELECT DISTINCT w.producer_id, ei.external_id AS lwin
      FROM external_ids ei
      JOIN wines w ON w.id = ei.entity_id
      WHERE ei.entity_type = 'wine' AND ei.system = 'lwin_7'
        AND w.deleted_at IS NULL AND w.producer_id IS NOT NULL
    ),
    lwin_matches AS (
      SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
             jsonb_agg(DISTINCT p1.lwin) AS shared_lwins,
             COUNT(DISTINCT p1.lwin)::int AS n
      FROM producer_lwins p1
      JOIN producer_lwins p2 ON p1.lwin = p2.lwin AND p1.producer_id < p2.producer_id
      JOIN producers pa ON pa.id = p1.producer_id
      JOIN producers pb ON pb.id = p2.producer_id
      WHERE pa.country_id IS DISTINCT FROM pb.country_id
      GROUP BY p1.producer_id, p2.producer_id
    ),
    name_matches AS (
      SELECT p1.id AS id_a, p2.id AS id_b,
             similarity(p1.name_normalized, p2.name_normalized) AS sim
      FROM producers p1
      JOIN producers p2
        ON p1.id < p2.id
       AND p1.country_id IS DISTINCT FROM p2.country_id
       AND p1.name_normalized % p2.name_normalized
      WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
        AND length(p1.name_normalized) >= 3
        AND length(p2.name_normalized) >= 3
        AND similarity(p1.name_normalized, p2.name_normalized) >= {trigram_threshold}
    ),
    unioned AS (
      SELECT id_a, id_b FROM lwin_matches
      UNION
      SELECT id_a, id_b FROM name_matches
    )
    SELECT COUNT(*) FROM unioned;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + f"""
          WITH producer_lwins AS (
            SELECT DISTINCT w.producer_id, ei.external_id AS lwin
            FROM external_ids ei
            JOIN wines w ON w.id = ei.entity_id
            WHERE ei.entity_type = 'wine' AND ei.system = 'lwin_7'
              AND w.deleted_at IS NULL AND w.producer_id IS NOT NULL
          ),
          lwin_matches AS (
            SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
                   jsonb_agg(DISTINCT p1.lwin) AS shared_lwins,
                   COUNT(DISTINCT p1.lwin)::int AS n
            FROM producer_lwins p1
            JOIN producer_lwins p2 ON p1.lwin = p2.lwin AND p1.producer_id < p2.producer_id
            JOIN producers pa ON pa.id = p1.producer_id
            JOIN producers pb ON pb.id = p2.producer_id
            WHERE pa.country_id IS DISTINCT FROM pb.country_id
            GROUP BY p1.producer_id, p2.producer_id
          ),
          name_matches AS (
            SELECT p1.id AS id_a, p2.id AS id_b,
                   similarity(p1.name_normalized, p2.name_normalized) AS sim
            FROM producers p1
            JOIN producers p2
              ON p1.id < p2.id
             AND p1.country_id IS DISTINCT FROM p2.country_id
             AND p1.name_normalized % p2.name_normalized
            WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
              AND length(p1.name_normalized) >= 3
              AND length(p2.name_normalized) >= 3
              AND similarity(p1.name_normalized, p2.name_normalized) >= {trigram_threshold}
          ),
          unioned AS (
            SELECT id_a, id_b,
                   to_jsonb(shared_lwins) AS shared_lwins_j,
                   n AS n_shared_lwins,
                   NULL::numeric AS sim
            FROM lwin_matches
            UNION ALL
            SELECT nm.id_a, nm.id_b,
                   NULL::jsonb, NULL::int,
                   nm.sim
            FROM name_matches nm
            WHERE NOT EXISTS (
              SELECT 1 FROM lwin_matches lm2
              WHERE lm2.id_a = nm.id_a AND lm2.id_b = nm.id_b
            )
          ),
          wc AS (
            SELECT producer_id, COUNT(*)::int AS cnt FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
            GROUP BY producer_id
          )
          SELECT u.id_a, u.id_b,
                 pa.name, pb.name,
                 COALESCE(ca.iso_code, 'NULL') || '/' || COALESCE(cb.iso_code, 'NULL'),
                 COALESCE(u.sim, 1.0)::numeric,
                 COALESCE(wca.cnt, 0), COALESCE(wcb.cnt, 0),
                 'blocking',
                 jsonb_build_object('s7_cross_country', jsonb_build_object(
                   'n_shared_lwins', u.n_shared_lwins,
                   'lwins', u.shared_lwins_j,
                   'trigram', CASE WHEN u.sim IS NOT NULL THEN round(u.sim::numeric, 4) ELSE NULL END,
                   'country_a', ca.iso_code, 'country_b', cb.iso_code,
                   'name_a_nn', pa.name_normalized,
                   'name_b_nn', pb.name_normalized
                 )),
                 now(), now()
          FROM unioned u
          JOIN producers pa ON pa.id = u.id_a
          JOIN producers pb ON pb.id = u.id_b
          LEFT JOIN countries ca ON ca.id = pa.country_id
          LEFT JOIN countries cb ON cb.id = pb.country_id
          LEFT JOIN wc wca ON wca.producer_id = u.id_a
          LEFT JOIN wc wcb ON wcb.producer_id = u.id_b
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_8(cur, write: bool, min_overlap: float = 0.3,
                    min_shared: int = 2, min_name_len: int = 10,
                    max_name_frequency: int = 20) -> int:
    """Shared distinguishing wine-name overlap >= min_overlap of smaller catalog.

    A "distinguishing" shared wine name is one that appears in at most
    max_name_frequency producers globally. This filters out generic grape
    names ("chardonnay" in 2,710 producers) and appellation-only names
    ("saint emilion grand cru" in 461 producers) so the signal is driven
    by producer-specific named wines (e.g., Ridge's "Monte Bello", Chateau
    Margaux's "Pavillon Rouge").
    """
    sql = f"""
    WITH name_freq AS (
      SELECT name_normalized, COUNT(DISTINCT producer_id) AS n_producers
      FROM wines
      WHERE deleted_at IS NULL AND producer_id IS NOT NULL
        AND name_normalized IS NOT NULL AND length(name_normalized) >= {min_name_len}
      GROUP BY name_normalized
    ),
    distinguishing_wines AS (
      SELECT w.producer_id, w.name_normalized
      FROM wines w
      JOIN name_freq nf ON nf.name_normalized = w.name_normalized
      WHERE w.deleted_at IS NULL AND w.producer_id IS NOT NULL
        AND length(w.name_normalized) >= {min_name_len}
        AND nf.n_producers <= {max_name_frequency}
    ),
    producer_wines AS (
      SELECT producer_id, name_normalized
      FROM distinguishing_wines
      GROUP BY producer_id, name_normalized
    ),
    producer_sizes AS (
      SELECT producer_id, COUNT(DISTINCT name_normalized) AS n
      FROM producer_wines GROUP BY producer_id
    ),
    shared AS (
      SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
             COUNT(DISTINCT p1.name_normalized) AS n_shared
      FROM producer_wines p1
      JOIN producer_wines p2
        ON p1.name_normalized = p2.name_normalized
       AND p1.producer_id < p2.producer_id
      GROUP BY p1.producer_id, p2.producer_id
      HAVING COUNT(DISTINCT p1.name_normalized) >= {min_shared}
    ),
    scored AS (
      SELECT s.id_a, s.id_b, s.n_shared,
             sa.n AS size_a, sb.n AS size_b,
             s.n_shared::numeric / LEAST(sa.n, sb.n) AS overlap
      FROM shared s
      JOIN producer_sizes sa ON sa.producer_id = s.id_a
      JOIN producer_sizes sb ON sb.producer_id = s.id_b
    )
    SELECT COUNT(*) FROM scored WHERE overlap >= {min_overlap};
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + f"""
          WITH name_freq AS (
            SELECT name_normalized, COUNT(DISTINCT producer_id) AS n_producers
            FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
              AND name_normalized IS NOT NULL AND length(name_normalized) >= {min_name_len}
            GROUP BY name_normalized
          ),
          distinguishing_wines AS (
            SELECT w.producer_id, w.name_normalized
            FROM wines w
            JOIN name_freq nf ON nf.name_normalized = w.name_normalized
            WHERE w.deleted_at IS NULL AND w.producer_id IS NOT NULL
              AND length(w.name_normalized) >= {min_name_len}
              AND nf.n_producers <= {max_name_frequency}
          ),
          producer_wines AS (
            SELECT producer_id, name_normalized
            FROM distinguishing_wines
            GROUP BY producer_id, name_normalized
          ),
          producer_sizes AS (
            SELECT producer_id, COUNT(DISTINCT name_normalized) AS n
            FROM producer_wines GROUP BY producer_id
          ),
          shared AS (
            SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
                   COUNT(DISTINCT p1.name_normalized)::int AS n_shared,
                   jsonb_agg(DISTINCT p1.name_normalized) FILTER (
                     WHERE p1.name_normalized IS NOT NULL
                   ) AS shared_wine_names
            FROM producer_wines p1
            JOIN producer_wines p2
              ON p1.name_normalized = p2.name_normalized
             AND p1.producer_id < p2.producer_id
            GROUP BY p1.producer_id, p2.producer_id
            HAVING COUNT(DISTINCT p1.name_normalized) >= {min_shared}
          ),
          scored AS (
            SELECT s.id_a, s.id_b, s.n_shared, s.shared_wine_names,
                   sa.n AS size_a, sb.n AS size_b,
                   round((s.n_shared::numeric / LEAST(sa.n, sb.n))::numeric, 4) AS overlap
            FROM shared s
            JOIN producer_sizes sa ON sa.producer_id = s.id_a
            JOIN producer_sizes sb ON sb.producer_id = s.id_b
          )
          SELECT sc.id_a, sc.id_b,
                 pa.name, pb.name,
                 COALESCE(c.iso_code, 'NULL'),
                 sc.overlap AS similarity,
                 sc.size_a, sc.size_b,
                 'blocking',
                 jsonb_build_object('s8_catalog_overlap', jsonb_build_object(
                   'n_shared', sc.n_shared,
                   'size_a', sc.size_a, 'size_b', sc.size_b,
                   'overlap', sc.overlap,
                   'sample_names', (
                     SELECT jsonb_agg(x) FROM (
                       SELECT jsonb_array_elements_text(sc.shared_wine_names) AS x LIMIT 5
                     ) t
                   )
                 )),
                 now(), now()
          FROM scored sc
          JOIN producers pa ON pa.id = sc.id_a
          JOIN producers pb ON pb.id = sc.id_b
          LEFT JOIN countries c ON c.id = pa.country_id
          WHERE sc.overlap >= {min_overlap}
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_9(cur, write: bool) -> int:
    """Same country + normalized-name substring containment OR token-subset.

    Catches 'Ridge' ⊂ 'Ridge Vineyards' and 'Louis Latour' ⊂ 'Maison Louis
    Latour'. Prefilter via trigram (uses gin index), then exact check.
    """
    cur.execute("SET pg_trgm.similarity_threshold = 0.25;")
    sql = _wine_counts_cte() + """,
    cand AS (
      SELECT p1.id AS id_a, p2.id AS id_b,
             p1.name AS name_a, p2.name AS name_b,
             p1.name_normalized AS nn_a, p2.name_normalized AS nn_b,
             COALESCE(c.iso_code, 'NULL') AS country,
             similarity(p1.name_normalized, p2.name_normalized) AS sim,
             COALESCE(wc1.cnt, 0) AS wines_a,
             COALESCE(wc2.cnt, 0) AS wines_b
      FROM producers p1
      JOIN producers p2
        ON p1.country_id IS NOT DISTINCT FROM p2.country_id
       AND p1.id < p2.id
       AND p1.name_normalized % p2.name_normalized
      LEFT JOIN countries c ON c.id = p1.country_id
      LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
      LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
      WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
        AND length(p1.name_normalized) >= 4
        AND length(p2.name_normalized) >= 4
        AND p1.name_normalized <> p2.name_normalized
    ),
    filtered AS (
      SELECT *,
        CASE
          WHEN nn_a LIKE '% ' || nn_b || ' %' OR nn_a LIKE nn_b || ' %' OR nn_a LIKE '% ' || nn_b THEN 'word_contain'
          WHEN nn_b LIKE '% ' || nn_a || ' %' OR nn_b LIKE nn_a || ' %' OR nn_b LIKE '% ' || nn_a THEN 'word_contain'
          WHEN position(nn_b in nn_a) > 0 OR position(nn_a in nn_b) > 0 THEN 'substring'
          ELSE NULL
        END AS kind
      FROM cand
    )
    SELECT COUNT(*) FROM filtered WHERE kind IS NOT NULL;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + _wine_counts_cte() + """,
          cand AS (
            SELECT p1.id AS id_a, p2.id AS id_b,
                   p1.name AS name_a, p2.name AS name_b,
                   p1.name_normalized AS nn_a, p2.name_normalized AS nn_b,
                   COALESCE(c.iso_code, 'NULL') AS country,
                   similarity(p1.name_normalized, p2.name_normalized) AS sim,
                   COALESCE(wc1.cnt, 0) AS wines_a,
                   COALESCE(wc2.cnt, 0) AS wines_b
            FROM producers p1
            JOIN producers p2
              ON p1.country_id IS NOT DISTINCT FROM p2.country_id
             AND p1.id < p2.id
             AND p1.name_normalized % p2.name_normalized
            LEFT JOIN countries c ON c.id = p1.country_id
            LEFT JOIN wine_counts wc1 ON wc1.producer_id = p1.id
            LEFT JOIN wine_counts wc2 ON wc2.producer_id = p2.id
            WHERE p1.deleted_at IS NULL AND p2.deleted_at IS NULL
              AND length(p1.name_normalized) >= 4
              AND length(p2.name_normalized) >= 4
              AND p1.name_normalized <> p2.name_normalized
          ),
          filtered AS (
            SELECT *,
              CASE
                WHEN nn_a LIKE '% ' || nn_b || ' %' OR nn_a LIKE nn_b || ' %' OR nn_a LIKE '% ' || nn_b THEN 'word_contain'
                WHEN nn_b LIKE '% ' || nn_a || ' %' OR nn_b LIKE nn_a || ' %' OR nn_b LIKE '% ' || nn_a THEN 'word_contain'
                WHEN position(nn_b in nn_a) > 0 OR position(nn_a in nn_b) > 0 THEN 'substring'
                ELSE NULL
              END AS kind
            FROM cand
          )
          SELECT id_a, id_b, name_a, name_b, country,
                 sim AS similarity, wines_a, wines_b, 'blocking',
                 jsonb_build_object('s9_substring', jsonb_build_object(
                   'kind', kind, 'trigram', round(sim::numeric, 4),
                   'nn_a', nn_a, 'nn_b', nn_b
                 )),
                 now(), now()
          FROM filtered WHERE kind IS NOT NULL
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_10(cur, write: bool, max_name_frequency: int = 5,
                     min_name_len: int = 8) -> int:
    """Shared rare wine name — any pair of producers sharing a wine name
    that appears in at most max_name_frequency producers globally.

    Catches abbreviation/initialism cases like DRC ↔ de la Romanée-Conti
    which share 'marey monge' (specific lieu-dit appearing only in DRC
    rows) but not enough common wines for S8's 30% overlap threshold, and
    have trigram < 0.05 (S2 fails).

    This is a lower-bar version of S8 — min_shared=1, but with a strict
    name-rarity filter. Volume at max_freq=5 is ~15K; at max_freq=3 is ~8K.
    """
    sql = f"""
    WITH rare_names AS (
      SELECT name_normalized
      FROM wines
      WHERE deleted_at IS NULL AND producer_id IS NOT NULL
        AND name_normalized IS NOT NULL AND length(name_normalized) >= {min_name_len}
      GROUP BY name_normalized
      HAVING COUNT(DISTINCT producer_id) BETWEEN 2 AND {max_name_frequency}
    ),
    producer_rare AS (
      SELECT DISTINCT w.producer_id, w.name_normalized
      FROM wines w
      JOIN rare_names r ON r.name_normalized = w.name_normalized
      WHERE w.deleted_at IS NULL AND w.producer_id IS NOT NULL
    ),
    shared AS (
      SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
             jsonb_agg(DISTINCT p1.name_normalized) AS shared_rare_wines,
             COUNT(DISTINCT p1.name_normalized)::int AS n
      FROM producer_rare p1
      JOIN producer_rare p2
        ON p1.name_normalized = p2.name_normalized
       AND p1.producer_id < p2.producer_id
      GROUP BY p1.producer_id, p2.producer_id
    )
    SELECT COUNT(*) FROM shared;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + f"""
          WITH rare_names AS (
            SELECT name_normalized
            FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
              AND name_normalized IS NOT NULL AND length(name_normalized) >= {min_name_len}
            GROUP BY name_normalized
            HAVING COUNT(DISTINCT producer_id) BETWEEN 2 AND {max_name_frequency}
          ),
          producer_rare AS (
            SELECT DISTINCT w.producer_id, w.name_normalized
            FROM wines w
            JOIN rare_names r ON r.name_normalized = w.name_normalized
            WHERE w.deleted_at IS NULL AND w.producer_id IS NOT NULL
          ),
          shared AS (
            SELECT p1.producer_id AS id_a, p2.producer_id AS id_b,
                   jsonb_agg(DISTINCT p1.name_normalized) AS shared_rare_wines,
                   COUNT(DISTINCT p1.name_normalized)::int AS n
            FROM producer_rare p1
            JOIN producer_rare p2
              ON p1.name_normalized = p2.name_normalized
             AND p1.producer_id < p2.producer_id
            GROUP BY p1.producer_id, p2.producer_id
          ),
          wc AS (
            SELECT producer_id, COUNT(*)::int AS cnt FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
            GROUP BY producer_id
          )
          SELECT s.id_a, s.id_b,
                 pa.name, pb.name,
                 COALESCE(c.iso_code, 'NULL'),
                 1.0::numeric,
                 COALESCE(wca.cnt, 0), COALESCE(wcb.cnt, 0),
                 'blocking',
                 jsonb_build_object('s10_shared_rare_wine', jsonb_build_object(
                   'n', s.n,
                   'shared_wines', s.shared_rare_wines
                 )),
                 now(), now()
          FROM shared s
          JOIN producers pa ON pa.id = s.id_a
          JOIN producers pb ON pb.id = s.id_b
          LEFT JOIN countries c ON c.id = pa.country_id
          LEFT JOIN wc wca ON wca.producer_id = s.id_a
          LEFT JOIN wc wcb ON wcb.producer_id = s.id_b
        """ + UPSERT_SUFFIX)
    return count


def run_strategy_11(cur, write: bool) -> int:
    """Cross-country word-subset containment.

    Catches cross-country pairs where one producer's full normalized name
    appears as a token in the other's. Example: Mondavi (US) vs Mondavi &
    Frescobaldi (IT) — "mondavi" is a token in the latter. S2 is
    same-country; S7 requires exact name or trigram >= 0.5 cross-country
    (both fail here). S9 is same-country substring.
    """
    sql = """
    WITH tok AS (
      SELECT id, country_id, name, name_normalized AS nn,
             string_to_array(name_normalized, ' ') AS toks
      FROM producers
      WHERE deleted_at IS NULL AND length(name_normalized) >= 5
    )
    SELECT COUNT(*) FROM (
      SELECT DISTINCT p1.id AS id_a, p2.id AS id_b
      FROM tok p1
      JOIN tok p2
        ON p1.id < p2.id
       AND p1.country_id IS DISTINCT FROM p2.country_id
       AND (p1.nn = ANY(p2.toks) OR p2.nn = ANY(p1.toks))
    ) x;
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    if write and count > 0:
        cur.execute(UPSERT_PREFIX + """
          WITH tok AS (
            SELECT id, country_id, name, name_normalized AS nn,
                   string_to_array(name_normalized, ' ') AS toks
            FROM producers
            WHERE deleted_at IS NULL AND length(name_normalized) >= 5
          ),
          wc AS (
            SELECT producer_id, COUNT(*)::int AS cnt FROM wines
            WHERE deleted_at IS NULL AND producer_id IS NOT NULL
            GROUP BY producer_id
          ),
          pairs AS (
            SELECT DISTINCT
              LEAST(p1.id, p2.id) AS id_a,
              GREATEST(p1.id, p2.id) AS id_b,
              p1.id AS p1_id, p2.id AS p2_id,
              p1.name AS p1_name, p2.name AS p2_name,
              p1.nn AS nn_a, p2.nn AS nn_b,
              p1.country_id AS ca_id, p2.country_id AS cb_id,
              CASE WHEN p1.nn = ANY(p2.toks) THEN p1.nn ELSE p2.nn END AS shared_tok
            FROM tok p1
            JOIN tok p2
              ON p1.id < p2.id
             AND p1.country_id IS DISTINCT FROM p2.country_id
             AND (p1.nn = ANY(p2.toks) OR p2.nn = ANY(p1.toks))
          )
          SELECT pr.id_a, pr.id_b,
                 pr.p1_name, pr.p2_name,
                 COALESCE(ca.iso_code, 'NULL') || '/' || COALESCE(cb.iso_code, 'NULL'),
                 1.0::numeric,
                 COALESCE(wca.cnt, 0), COALESCE(wcb.cnt, 0),
                 'blocking',
                 jsonb_build_object('s11_cross_word_subset', jsonb_build_object(
                   'shared_token', pr.shared_tok,
                   'country_a', ca.iso_code,
                   'country_b', cb.iso_code
                 )),
                 now(), now()
          FROM pairs pr
          LEFT JOIN countries ca ON ca.id = pr.ca_id
          LEFT JOIN countries cb ON cb.id = pr.cb_id
          LEFT JOIN wc wca ON wca.producer_id = pr.id_a
          LEFT JOIN wc wcb ON wcb.producer_id = pr.id_b
        """ + UPSERT_SUFFIX)
    return count


STRATEGY_FNS = {
    1: run_strategy_1,
    2: run_strategy_2,
    # 4 omitted — first-3-char blocking too permissive at 33K producers.
    #   Trigram (S2) + substring (S9) cover word-reorder variants.
    5: run_strategy_5,
    6: run_strategy_6,
    7: run_strategy_7,
    8: run_strategy_8,
    9: run_strategy_9,
    10: run_strategy_10,
    11: run_strategy_11,
}


def report(cur) -> None:
    """Print per-strategy counts and union total from the current state."""
    signal_keys = [
        ('s1_exact',            1),
        ('s2_trigram',          2),
        ('s4_first3_tokens',    4),
        ('s5_shared_wine_lwin', 5),
        ('s6_ttb_permit',       6),
        ('s7_cross_country',    7),
        ('s8_catalog_overlap',  8),
        ('s9_substring',        9),
        ('s10_shared_rare_wine', 10),
        ('s11_cross_word_subset', 11),
    ]
    filter_clauses = ',\n        '.join(
        f"COUNT(*) FILTER (WHERE signals ? '{k}') AS n_{k}"
        for k, _ in signal_keys
    )
    cur.execute(f"""
      SELECT {filter_clauses},
             COUNT(*) AS union_total
      FROM producer_dedup_pairs WHERE method_name = 'blocking';
    """)
    r = cur.fetchone()
    print(f"\n{'Strategy':<44} {'pairs':>10} {'unique':>10}")
    print('-' * 66)
    for i, (key, strat_num) in enumerate(signal_keys):
        label = STRATEGIES.get(strat_num, ('', key))[1]
        other_keys = [k for k, _ in signal_keys if k != key]
        cur.execute(f"""
          SELECT COUNT(*) FROM producer_dedup_pairs
          WHERE method_name = 'blocking'
            AND signals ? '{key}'
            AND NOT (signals ?| ARRAY[{','.join(repr(k) for k in other_keys)}])
        """)
        unique_count = cur.fetchone()[0]
        total = r[i]
        print(f"{strat_num}. {label[:40]:<40} {total:>10,} {unique_count:>10,}")
    print('-' * 66)
    print(f"{'UNION TOTAL':<44} {r[-1]:>10,}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--analyze', action='store_true',
                    help='count only, no writes')
    ap.add_argument('--clear', action='store_true',
                    help='delete prior method_name=blocking rows first')
    ap.add_argument('--strategies', type=str,
                    help='comma list, e.g. "1,2,6"')
    ap.add_argument('--report-only', action='store_true',
                    help='print current counts from DB without running')
    args = ap.parse_args()

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        if args.report_only:
            report(cur)
            return 0

        if args.clear:
            print("Clearing prior method_name='blocking' rows...")
            cur.execute("DELETE FROM producer_dedup_pairs WHERE method_name = 'blocking';")
            conn.commit()

        chosen = list(STRATEGY_FNS.keys())
        if args.strategies:
            chosen = [int(s) for s in args.strategies.split(',')]

        write = not args.analyze
        for s in chosen:
            if s not in STRATEGY_FNS:
                print(f"Strategy {s} not implemented, skipping.")
                continue
            label = STRATEGIES[s][1]
            print(f"\n[S{s}] {label}")
            t0 = time.time()
            count = STRATEGY_FNS[s](cur, write=write)
            dt = time.time() - t0
            print(f"       -> {count:,} pairs in {dt:,.1f}s")
            if write:
                conn.commit()

        print("\n=== FINAL REPORT ===")
        report(cur)

    finally:
        cur.close()
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
