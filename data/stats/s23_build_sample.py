"""S2.3 sample builder.

Pulls a stratified 100-wine + 50-producer sample for the wine-expert canonical
audit (S2.3) and saves to JSON for reproducibility.

Stratification:
- 30 Grade B  (of 105)
- 50 Grade C  (of 4,973)
- 10 Grade F with full facts packet (LWIN + producer + appellation + grapes)
- 10 marquee  (hand-picked, high visibility)

Producer sample:
- 15 famous   (hand-picked)
- 20 mid-tier (has website OR has winemaker OR has year_established — rare)
- 15 long-tail (LWIN-only, no metadata)

Deterministic ordering via md5(id) for reproducibility.
One-shot script — not reusable. Save output, do not re-run.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_conn

OUT_DIR = Path(__file__).resolve().parent
WINES_OUT = OUT_DIR / "s23_sample_wines.json"
PRODUCERS_OUT = OUT_DIR / "s23_sample_producers.json"

MARQUEE_WINE_SEARCH = [
    # (search_producer, search_name_fragment, display_label)
    ("Romanée-Conti", "Romanée-Conti", "DRC Romanée-Conti"),
    ("Lafite", "Lafite", "Château Lafite Rothschild"),
    ("Screaming Eagle", None, "Screaming Eagle Cabernet"),
    ("Sassicaia", "Sassicaia", "Tenuta San Guido Sassicaia"),
    ("Vega Sicilia", "Único", "Vega Sicilia Único"),
    ("Opus One", None, "Opus One"),
    ("Penfolds", "Grange", "Penfolds Grange"),
    ("Henschke", "Hill of Grace", "Henschke Hill of Grace"),
    ("Dom Pérignon", None, "Dom Pérignon"),
    ("Egon Müller", "Scharzhofberger", "Egon Müller Scharzhofberger"),
]

MARQUEE_PRODUCER_NAMES = [
    "Domaine de la Romanée-Conti", "Domaine Leroy", "Henri Jayer", "Château Lafite Rothschild",
    "Château Latour", "Château Margaux", "Château Haut-Brion", "Pétrus", "Gaja",
    "Tenuta San Guido", "Giacomo Conterno", "Bruno Giacosa",
    "Screaming Eagle", "Harlan Estate", "Ridge Vineyards",
]


def fetch_wines(cur, where_clause, limit):
    cur.execute(f"""
        WITH sample AS (
          SELECT w.id
            FROM wines w
           WHERE w.deleted_at IS NULL AND {where_clause}
           ORDER BY md5(w.id::text)
           LIMIT {int(limit)}
        )
        SELECT
          w.id::text, w.display_name, w.color, w.wine_type, w.data_grade,
          p.name AS producer,
          c.name AS country, r.name AS region, a.name AS appellation,
          (SELECT string_agg(g.name || COALESCE(' '||round(wg.percentage)::text||'%',''), ' + '
                             ORDER BY wg.percentage DESC NULLS LAST)
             FROM wine_grapes wg JOIN grapes g ON g.id=wg.grape_id
            WHERE wg.wine_id = w.id) AS grapes,
          (SELECT json_build_object(
                    'lwin', max(external_id) FILTER (WHERE system='lwin'),
                    'lwin_7', max(external_id) FILTER (WHERE system='lwin_7'),
                    'cola_count', count(*) FILTER (WHERE system='cola'),
                    'upc_count', count(*) FILTER (WHERE system='upc'))
             FROM external_ids WHERE entity_id = w.id AND entity_type='wine') AS ids,
          wi.ai_hook, wi.ai_wine_summary, wi.ai_style_profile,
          wi.ai_terroir_expression, wi.ai_food_pairing,
          wi.ai_vinification_summary, wi.enrichment_tier
          FROM sample s JOIN wines w ON w.id = s.id
          LEFT JOIN producers p ON p.id = w.producer_id
          LEFT JOIN countries c ON c.id = w.country_id
          LEFT JOIN regions r ON r.id = w.region_id
          LEFT JOIN appellations a ON a.id = w.appellation_id
          LEFT JOIN wine_insights wi ON wi.wine_id = w.id
         ORDER BY w.display_name
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_f_with_facts(cur, limit):
    # Grade F wines with full canonical facts packet: LWIN or COLA ID + producer + appellation + at least one grape
    cur.execute(f"""
        WITH pool AS (
          SELECT w.id
            FROM wines w
           WHERE w.deleted_at IS NULL AND w.data_grade='F'
             AND w.producer_id IS NOT NULL
             AND w.appellation_id IS NOT NULL
             AND w.color IS NOT NULL
             AND EXISTS (SELECT 1 FROM wine_grapes wg WHERE wg.wine_id = w.id)
             AND EXISTS (SELECT 1 FROM external_ids e WHERE e.entity_id=w.id AND e.system IN ('lwin','lwin_7','cola'))
           ORDER BY md5(w.id::text)
           LIMIT {int(limit)}
        )
        SELECT
          w.id::text, w.display_name, w.color, w.wine_type, w.data_grade,
          p.name AS producer,
          c.name AS country, r.name AS region, a.name AS appellation,
          (SELECT string_agg(g.name || COALESCE(' '||round(wg.percentage)::text||'%',''), ' + '
                             ORDER BY wg.percentage DESC NULLS LAST)
             FROM wine_grapes wg JOIN grapes g ON g.id=wg.grape_id
            WHERE wg.wine_id = w.id) AS grapes,
          (SELECT json_build_object(
                    'lwin', max(external_id) FILTER (WHERE system='lwin'),
                    'lwin_7', max(external_id) FILTER (WHERE system='lwin_7'),
                    'cola_count', count(*) FILTER (WHERE system='cola'))
             FROM external_ids WHERE entity_id = w.id AND entity_type='wine') AS ids
          FROM pool s JOIN wines w ON w.id = s.id
          LEFT JOIN producers p ON p.id = w.producer_id
          LEFT JOIN countries c ON c.id = w.country_id
          LEFT JOIN regions r ON r.id = w.region_id
          LEFT JOIN appellations a ON a.id = w.appellation_id
         ORDER BY w.display_name
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_marquee(cur):
    """Find marquee wines by fuzzy producer + name match."""
    found = []
    for prod, name_frag, label in MARQUEE_WINE_SEARCH:
        if name_frag:
            cur.execute("""
                SELECT w.id::text, w.display_name, w.color, w.wine_type, w.data_grade,
                       p.name AS producer, c.name AS country, r.name AS region, a.name AS appellation,
                       (SELECT string_agg(g.name, ' + ') FROM wine_grapes wg JOIN grapes g ON g.id=wg.grape_id
                          WHERE wg.wine_id = w.id) AS grapes,
                       (SELECT json_build_object('lwin', max(external_id) FILTER (WHERE system='lwin'))
                          FROM external_ids WHERE entity_id = w.id) AS ids
                  FROM wines w LEFT JOIN producers p ON p.id=w.producer_id
                  LEFT JOIN countries c ON c.id=w.country_id
                  LEFT JOIN regions r ON r.id=w.region_id
                  LEFT JOIN appellations a ON a.id=w.appellation_id
                 WHERE w.deleted_at IS NULL
                   AND (p.name ILIKE %s OR w.display_name ILIKE %s)
                   AND w.display_name ILIKE %s
                 ORDER BY w.data_grade DESC, md5(w.id::text)
                 LIMIT 1
            """, (f"%{prod}%", f"%{prod}%", f"%{name_frag}%"))
        else:
            cur.execute("""
                SELECT w.id::text, w.display_name, w.color, w.wine_type, w.data_grade,
                       p.name AS producer, c.name AS country, r.name AS region, a.name AS appellation,
                       (SELECT string_agg(g.name, ' + ') FROM wine_grapes wg JOIN grapes g ON g.id=wg.grape_id
                          WHERE wg.wine_id = w.id) AS grapes,
                       (SELECT json_build_object('lwin', max(external_id) FILTER (WHERE system='lwin'))
                          FROM external_ids WHERE entity_id = w.id) AS ids
                  FROM wines w LEFT JOIN producers p ON p.id=w.producer_id
                  LEFT JOIN countries c ON c.id=w.country_id
                  LEFT JOIN regions r ON r.id=w.region_id
                  LEFT JOIN appellations a ON a.id=w.appellation_id
                 WHERE w.deleted_at IS NULL
                   AND (p.name ILIKE %s OR w.display_name ILIKE %s)
                 ORDER BY w.data_grade DESC, md5(w.id::text)
                 LIMIT 1
            """, (f"%{prod}%", f"%{prod}%"))
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            d = dict(zip(cols, row))
            d['_marquee_label'] = label
            found.append(d)
        else:
            found.append({'_marquee_label': label, 'not_found': True})
    return found


def fetch_producers(cur):
    result = {'famous': [], 'mid_tier': [], 'long_tail': []}

    # 15 famous — hand-picked names, best fuzzy match per name
    for name in MARQUEE_PRODUCER_NAMES:
        cur.execute("""
            SELECT p.id::text, p.name, p.country_id, c.name AS country, r.name AS region,
                   p.website_url, p.year_established, p.latitude, p.longitude,
                   (SELECT count(*) FROM wines w WHERE w.producer_id=p.id AND w.deleted_at IS NULL) AS n_wines
              FROM producers p
              LEFT JOIN countries c ON c.id=p.country_id
              LEFT JOIN regions r ON r.id=p.region_id
             WHERE p.deleted_at IS NULL AND p.name ILIKE %s
             ORDER BY length(p.name), md5(p.id::text)
             LIMIT 1
        """, (f"%{name}%",))
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            d = dict(zip(cols, row))
            d['_marquee_label'] = name
            result['famous'].append(d)
        else:
            result['famous'].append({'_marquee_label': name, 'not_found': True})

    # 20 mid-tier: producers matched from rich importer sources (KL, Empson, Skurnik, Winebow, EC).
    # S2.1 F4 found only 1 producer has any metadata (website_url/year_established/latitude)
    # so "mid-tier by metadata" doesn't exist. Redefine as "matched from importer staging".
    cur.execute("""
        SELECT p.id::text, p.name, c.name AS country, r.name AS region,
               p.website_url, p.year_established, p.latitude, p.longitude,
               (SELECT count(*) FROM wines w WHERE w.producer_id=p.id AND w.deleted_at IS NULL) AS n_wines,
               (SELECT string_agg(DISTINCT src, ',') FROM (
                  SELECT 'kl' AS src FROM source_kermit_lynch WHERE canonical_producer_id=p.id
                  UNION SELECT 'klg' FROM source_kermit_lynch_growers WHERE canonical_producer_id=p.id
                  UNION SELECT 'empson' FROM source_empson WHERE canonical_producer_id=p.id
                  UNION SELECT 'skurnik' FROM source_skurnik WHERE canonical_producer_id=p.id
                  UNION SELECT 'winebow' FROM source_winebow WHERE canonical_producer_id=p.id
                  UNION SELECT 'ec' FROM source_european_cellars WHERE canonical_producer_id=p.id
               ) s) AS importer_sources
          FROM producers p
          LEFT JOIN countries c ON c.id=p.country_id
          LEFT JOIN regions r ON r.id=p.region_id
         WHERE p.deleted_at IS NULL
           AND EXISTS (
             SELECT 1 FROM source_kermit_lynch WHERE canonical_producer_id=p.id
             UNION ALL
             SELECT 1 FROM source_empson WHERE canonical_producer_id=p.id
             UNION ALL
             SELECT 1 FROM source_skurnik WHERE canonical_producer_id=p.id
             UNION ALL
             SELECT 1 FROM source_winebow WHERE canonical_producer_id=p.id
             UNION ALL
             SELECT 1 FROM source_european_cellars WHERE canonical_producer_id=p.id
             UNION ALL
             SELECT 1 FROM source_kermit_lynch_growers WHERE canonical_producer_id=p.id
           )
         ORDER BY md5(p.id::text)
         LIMIT 20
    """)
    cols = [d[0] for d in cur.description]
    result['mid_tier'] = [dict(zip(cols, row)) for row in cur.fetchall()]

    # 15 long-tail: LWIN-only producers, no metadata, low wine count
    cur.execute("""
        SELECT p.id::text, p.name, c.name AS country, r.name AS region,
               p.website_url, p.year_established,
               (SELECT count(*) FROM wines w WHERE w.producer_id=p.id AND w.deleted_at IS NULL) AS n_wines
          FROM producers p
          LEFT JOIN countries c ON c.id=p.country_id
          LEFT JOIN regions r ON r.id=p.region_id
         WHERE p.deleted_at IS NULL
           AND p.website_url IS NULL
           AND p.year_established IS NULL
           AND p.latitude IS NULL
           AND EXISTS (SELECT 1 FROM wines w WHERE w.producer_id=p.id AND w.deleted_at IS NULL)
         ORDER BY md5(p.id::text)
         LIMIT 15
    """)
    cols = [d[0] for d in cur.description]
    result['long_tail'] = [dict(zip(cols, row)) for row in cur.fetchall()]

    return result


def main():
    conn = get_conn()
    cur = conn.cursor()

    print("Fetching 30 Grade B wines...", flush=True)
    grade_b = fetch_wines(cur, "w.data_grade='B'", 30)

    print("Fetching 50 Grade C wines...", flush=True)
    grade_c = fetch_wines(cur, "w.data_grade='C'", 50)

    print("Fetching 10 Grade F wines with full facts packet...", flush=True)
    grade_f = fetch_f_with_facts(cur, 10)

    print("Fetching 10 marquee wines...", flush=True)
    marquee = fetch_marquee(cur)

    print("Fetching producer samples (15 famous + 20 mid-tier + 15 long-tail)...", flush=True)
    producers = fetch_producers(cur)

    wines_out = {
        "grade_b": grade_b,
        "grade_c": grade_c,
        "grade_f_with_facts": grade_f,
        "marquee": marquee,
        "meta": {
            "sprint": 2, "session": "2.3", "date": "2026-04-11",
            "counts": {
                "grade_b": len(grade_b),
                "grade_c": len(grade_c),
                "grade_f_with_facts": len(grade_f),
                "marquee": len([m for m in marquee if not m.get('not_found')]),
            }
        }
    }
    producers_out = {
        **producers,
        "meta": {
            "sprint": 2, "session": "2.3", "date": "2026-04-11",
            "counts": {k: len([x for x in v if not x.get('not_found')]) for k, v in producers.items()},
        }
    }

    WINES_OUT.write_text(json.dumps(wines_out, indent=2, default=str), encoding='utf-8')
    PRODUCERS_OUT.write_text(json.dumps(producers_out, indent=2, default=str), encoding='utf-8')
    print(f"Wrote {WINES_OUT} and {PRODUCERS_OUT}", flush=True)
    print(f"Wine counts: {wines_out['meta']['counts']}", flush=True)
    print(f"Producer counts: {producers_out['meta']['counts']}", flush=True)


if __name__ == '__main__':
    main()
