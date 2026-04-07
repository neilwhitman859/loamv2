"""
Label designation backfill with unaccent matching.

Matches label_designations.canonical_name against wines.name_normalized
(already lowercase + unaccented) using word-boundary regex.

Usage:
    python -m pipeline.promote.label_designation_fill [--dry-run] [--execute]
"""

import argparse
import unicodedata
import re

from pipeline.lib.db import get_conn


def strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', default=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    dry_run = not args.execute

    conn = get_conn()
    cur = conn.cursor()

    # Load deduplicated designations (one ID per canonical_name, prefer the one with existing links)
    cur.execute("""
        SELECT DISTINCT ON (ld.canonical_name)
            ld.id, ld.canonical_name
        FROM label_designations ld
        LEFT JOIN wine_label_designations wld ON wld.label_designation_id = ld.id
        WHERE char_length(ld.canonical_name) >= 4
        GROUP BY ld.id, ld.canonical_name
        ORDER BY ld.canonical_name, COUNT(wld.wine_id) DESC
    """)
    designations = cur.fetchall()
    print(f"Loaded {len(designations)} designations")

    total = 0
    results = []

    for desig_id, name in designations:
        # Build unaccented lowercase pattern for name_normalized matching
        pattern = strip_accents(name).lower()
        # Escape regex special chars
        pattern = re.sub(r'([.*+?^${}()|[\]\\])', r'\\\1', pattern)

        if dry_run:
            cur.execute("""
                SELECT COUNT(DISTINCT w.id)
                FROM wines w
                WHERE w.name_normalized ~* (%s)
                AND NOT EXISTS (
                    SELECT 1 FROM wine_label_designations wld
                    WHERE wld.wine_id = w.id AND wld.label_designation_id = %s
                )
            """, (r'\m' + pattern + r'\M', desig_id))
            count = cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO wine_label_designations (wine_id, label_designation_id)
                SELECT w.id, %s
                FROM wines w
                WHERE w.name_normalized ~* (%s)
                AND NOT EXISTS (
                    SELECT 1 FROM wine_label_designations wld
                    WHERE wld.wine_id = w.id AND wld.label_designation_id = %s
                )
                ON CONFLICT DO NOTHING
            """, (desig_id, r'\m' + pattern + r'\M', desig_id))
            count = cur.rowcount
            conn.commit()

        if count > 0:
            print(f"  {name}: {count}")
            results.append((name, count))
            total += count

    print(f"\n{'DRY RUN — ' if dry_run else ''}Total: {total}")
    if results:
        print("Top designations:")
        for name, count in sorted(results, key=lambda x: -x[1])[:15]:
            print(f"  {name}: {count}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
