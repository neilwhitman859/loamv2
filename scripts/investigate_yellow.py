"""Quick investigation of yellow-flag producers — are they actionable?"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn
conn = get_conn()
cur = conn.cursor()

def investigate(label: str, name_pattern: str, flagship_keywords: list):
    print(f"\n=== {label} ===")
    cur.execute("SELECT id, name FROM producers WHERE name ILIKE %s AND deleted_at IS NULL", (name_pattern,))
    rows = cur.fetchall()
    for pid, name in rows:
        cur.execute("SELECT COUNT(*) FROM wines WHERE producer_id=%s::uuid", (pid,))
        wc = cur.fetchone()[0]
        flag_hits = 0
        for kw in flagship_keywords:
            cur.execute("""
                SELECT COUNT(*) FROM wines WHERE producer_id=%s::uuid
                AND (name ILIKE %s OR display_name ILIKE %s)
            """, (pid, f'%{kw}%', f'%{kw}%'))
            flag_hits += cur.fetchone()[0]
        cur.execute("SELECT name FROM wines WHERE producer_id=%s::uuid ORDER BY name LIMIT 3", (pid,))
        sample = [r[0] for r in cur.fetchall() if r[0]]
        print(f"  {str(name)[:45]:<47} wines={wc:<4} flag_hits={flag_hits}  sample={sample[:2]}")

    cur.execute("""
        SELECT s3.name_a, s3.name_b, s3.stage3_action, s3.web_conf
        FROM producer_dedup_routing_stage3 s3
        WHERE (s3.name_a ILIKE %s OR s3.name_b ILIKE %s)
        AND s3.stage3_action NOT IN ('auto_apply_skip','auto_apply_skip_residual','auto_apply_skip_missing')
        LIMIT 8
    """, (name_pattern, name_pattern))
    rows = cur.fetchall()
    if rows:
        print(f"  NON-SKIP routing decisions:")
        for r in rows:
            print(f"    '{str(r[0])[:35]}' vs '{str(r[1])[:35]}' -> {r[2]} @{r[3]}")


investigate("Trimbach", "%trimbach%", ["clos sainte hune", "frederic emile", "hune"])
investigate("Penfolds", "%penfolds%", ["grange", "bin 389", "bin 707", "bin 28"])
investigate("Chateau Pavie", "%pavie%", ["pavie"])
investigate("Ridge Vineyards", "%ridge%", ["monte bello", "lytton springs"])
investigate("Joseph Phelps", "%joseph phelps%", ["insignia"])
investigate("Dr. Loosen", "%loosen%", ["wehlener", "erdener", "urziger", "bernkasteler"])

cur.close(); conn.close()
