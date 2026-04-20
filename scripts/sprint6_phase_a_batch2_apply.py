"""Apply Phase A calibration batch 2 verdicts (30 pairs) with refined criteria."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn
conn = get_conn()
cur = conn.cursor()

VERDICTS = [
    (2109,   "MERGE", "auto_decide", "Schumacher-Knepper / Schumacher: Domaine Viticole Schumacher-Knepper (Luxembourg Moselle). Small single estate. Cross-tier disagreement but web explicitly identifies single domaine.", "11.1", None),
    (47799,  "ESCALATE_TO_CHROME", "escalate", "Mazzi vs Private Mazzi [IT] — 'Private' prefix is unusual (could be US-importer private label). L2 SKIP, L2.5 UNCERTAIN. Check robertomazzi.it for 'Private' label.", "11.4.p", None),
    (49410,  "ESCALATE_TO_CHROME", "escalate", "La Capra Danzante vs Danzante [IT] — 3-tier disagreement (L2+L2.5 SKIP@0.94-0.95, web MERGE@0.88). Corporate structure (Tenute di Toscana). Verify whether sub-brand or distinct.", "11.4.s", None),
    (54219,  "ESCALATE_TO_CHROME", "escalate", "Pierre Meurgey vs Meurgey Croses [FR Burgundy] — 4th-gen family + possible generational label split. L2 SKIP, L2.5 PC, web MERGE. Check pierremeurgey.com.", "11.4.m / 11.4.s", None),
    (59536,  "ESCALATE_TO_CHROME", "escalate", "Pascal Jolivet vs Jolivet [FR Loire] — 'Jolivet' alone could be another family. L2 PC, L2.5 SKIP. Web says same house but needs verification since Jolivet is a common Loire surname.", "11.4.m / 11.1", None),
    (59943,  "MERGE", "auto_decide", "Hospices de Beaune + (Cecile Tremblay) — HdB négociant variant. L2+L2.5+web all MERGE. 3-tier consensus.", "11.4.q", None),
    (71217,  "ESCALATE_TO_CHROME", "escalate", "Marie et Vincent Tricot vs No Control (Vincent Marie) [FR] — two projects by possibly same person. L2 SKIP, L2.5 PC. §11.4.o JV/collab territory. Verify project structure.", "11.4.o / 11.4.m", None),
    (102991, "ESCALATE_TO_CHROME", "escalate", "Terre Piano vs Terre al Piano [IT] — L2+L2.5 both SKIP@0.94+, web MERGE@0.89. Cross-tier strong disagreement. Check whether 'Terre Piano' typo of 'Terre al Piano' or distinct Tuscan estate.", "11.1", None),
    (9979,   "MERGE", "auto_decide", "Manuel Marinacci / Manuel Marinacci Paja [IT] — 'Paja' is a Barbaresco vineyard/cuvée under Manuel Marinacci. 3-tier MERGE consensus.", "11.4.s", None),
    (30003,  "ESCALATE_TO_CHROME", "escalate", "Barons Edmond Benjamin de Rothschild vs Baron Benjamin de Rothschild [FR] — Edmond de Rothschild Heritage corporate portfolio (7 domains, 500 ha). Same owner-of-many-labels risk as BPDR. Needs Chrome verification.", "11.4.g", None),
    (41670,  "MERGE", "auto_decide", "Coulerette / De La Coulerette [FR Provence] — Château de la Coulerette, Côtes de Provence. 3-tier MERGE consensus. Missing 'La'.", "11.1", None),
    (42491,  "MERGE", "auto_decide", "Moulin / Le Moulin Basileus [FR Pomerol] — Château Le Moulin. 'Basileus' is a cuvée name appended. Web specific, L2.5 MERGE.", "11.4.s", None),
    (62726,  "MERGE", "auto_decide", "Atrevida / Altrevida [AR Mendoza] — typo variant. All merchant platforms converge on single winery. Small single producer, no corporate risk.", "11.1", None),
    (71585,  "MERGE", "auto_decide", "Behere Courtin / Behere [FR] — Château Béhèré Courtin. Accent + hyphenation variants.", "11.4.h", None),
    (80365,  "ESCALATE_TO_CHROME", "escalate", "Tulbagh Mountain vs Tulbagh [ZA] — 'Tulbagh' alone is a region name, might be a generic catchall or separate producer. L2+L2.5 SKIP. Needs Chrome to disambiguate.", "data_state / 11.4.k", None),
    (134857, "MERGE", "auto_decide", "Mary Taylor (Jean Marc Barthez) / Barthez [FR] — Mary Taylor is importer/curator, Barthez is actual producer. §11.4.p merchant bundling (reversed format: Merchant (Producer)).", "11.4.p", None),
    # PC cases
    (3550,   "PARENT_CHILD", "auto_decide", "Niepoort Projectos / Niepoort [PT] — Dirk Niepoort's experimental/collab sub-brand. 3-tier PC consensus. Niepoort is parent.", "11.4.s", "b_is_parent"),
    (13568,  "ESCALATE_TO_CHROME", "escalate", "Baron de Rothschild vs Barons de Rothschild [FR] — possibly BPDR vs Lafite-Rothschild (DBR) or Edmond branch. Corporate-owner-of-many-labels. L2 SKIP, L2.5 MERGE. Needs Chrome.", "11.4.g", None),
    (28994,  "PARENT_CHILD", "auto_decide", "Marchand-Tawse vs Pascal Marchand [FR Burgundy] — Marchand-Tawse is collab (Pascal Marchand + Moray Tawse). §11.4.o collaboration label. Marchand is parent.", "11.4.o", "b_is_parent"),
    (28997,  "PARENT_CHILD", "auto_decide", "Marchand & Burch vs Pascal Marchand [FR/AU] — Marchand & Burch is Burgundy + Western Australia JV (Pascal Marchand + Jeff Burch). §11.4.o. Marchand is parent.", "11.4.o", "b_is_parent"),
    (84071,  "ESCALATE_TO_CHROME", "escalate", "Michel Gaunoux vs Jean-Michel Gaunoux [FR Burgundy] — known distinct Gaunoux family estates (Michel is famous Pommard producer, Jean-Michel is son's separate estate 1990). L2+L2.5 both SKIP. §11.4.m shared-surname split.", "11.4.m", None),
    (118425, "ESCALATE_TO_CHROME", "escalate", "Sandlands vs Couloir [US] — shared TTB permit (custom crush). Distinct brands, different winemakers. L2 MERGE, L2.5 SKIP. §11.4.j permit-shares-but-distinct-brands case.", "11.4.j", None),
    (119507, "ESCALATE_TO_CHROME", "escalate", "Substance vs CasaSmith [US] — both House of Smith brands (Charles Smith). PC candidate but holdco 'House of Smith' not in DB as producer. §11.4.g. Unclear whether to SKIP or PC.", "11.4.g", None),
    (146966, "ESCALATE_TO_CHROME", "escalate", "Guillemot Michel vs Emilian Gillet [FR Burgundy] — Jean Thévenet's complex Macônnais structure (1998 new brand for ancestral winemaker). L2 SKIP, L2.5 PC. Burgundy family estate relationship.", "11.4.m", None),
    (9950,   "FLAG_FOR_NEXT_SPRINT", "flag_next", "Esprit Destieu vs Grand Destieu [FR] — both are CUVÉES of Château Grand Destieu (not separate producers). §11.4.e data-state issue: Esprit should be a wine row under Grand Destieu, not its own producer.", "11.4.e data_state", None),
    (24233,  "FLAG_FOR_NEXT_SPRINT", "flag_next", "Bois de Plince vs La Fleur de Plince [FR] — La Fleur de Plince is second wine of Château de Plince. §11.4.e data-state issue: should be a wine under Plince, not its own producer row.", "11.4.e data_state", None),
    (54827,  "ESCALATE_TO_CHROME", "escalate", "Dutraive vs Jean-Louis Dutraive [FR Fleurie] — father + son Dutraive. L2+L2.5 MERGE, web PC. Cross-tier disagreement. Verify at dutraive.com whether they operate as one estate or two.", "11.4.m", None),
    (70096,  "PARENT_CHILD", "auto_decide", "Fonseca & Van Zeller vs Van Zeller [PT] — JV of JM da Fonseca + Cristiano van Zeller. Van Zellers & Co is Cristiano's own. §11.4.l JV label. Van Zeller is parent.", "11.4.o / 11.4.l", "b_is_parent"),
    (72739,  "PARENT_CHILD", "auto_decide", "Rolf Binder (Binder Mitchell) vs Binder Mitchell [AU] — Rolf Binder primary winery, Binder Mitchell is Rolf's partnership sub-brand. §11.4.o. Rolf Binder is parent.", "11.4.o / 11.4.s", "a_is_parent"),
    (98052,  "ESCALATE_TO_CHROME", "escalate", "Larose vs Larose Perganson [FR] — 'Les Vignobles de Larose' umbrella holds Trintaudon, Perganson, possibly others. Corporate structure question. L2+L2.5 both SKIP. Needs Chrome to verify umbrella relationship.", "11.4.g / 11.4.k", None),
]

for pid, verdict, bucket, reasoning, pattern_cluster, pc_dir in VERDICTS:
    cur.execute("""
        INSERT INTO step9_opus_verdicts (pair_id, verdict, bucket, reasoning, pattern_cluster, pc_parent_direction)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (pair_id) DO UPDATE SET
          verdict = EXCLUDED.verdict,
          bucket = EXCLUDED.bucket,
          reasoning = EXCLUDED.reasoning,
          pattern_cluster = EXCLUDED.pattern_cluster,
          pc_parent_direction = EXCLUDED.pc_parent_direction
    """, (pid, verdict, bucket, reasoning, pattern_cluster, pc_dir))

conn.commit()
print(f"Inserted {len(VERDICTS)} batch-2 verdicts")

from collections import Counter
bucket_counts = Counter(v[2] for v in VERDICTS)
verdict_counts = Counter(v[1] for v in VERDICTS)
pattern_counts = Counter(v[4] for v in VERDICTS)
print(f"\nBatch 2 buckets: {dict(bucket_counts)}")
print(f"Batch 2 verdicts: {dict(verdict_counts)}")

# Combined across both batches
cur.execute("SELECT bucket, COUNT(*) FROM step9_opus_verdicts GROUP BY bucket ORDER BY 2 DESC")
print(f"\n=== Combined (60 pairs across 2 batches) ===")
for b, n in cur.fetchall():
    print(f"  {b:<15} {n}")
cur.execute("SELECT verdict, COUNT(*) FROM step9_opus_verdicts GROUP BY verdict ORDER BY 2 DESC")
print(f"\nCombined verdicts:")
for v, n in cur.fetchall():
    print(f"  {v:<25} {n}")

cur.close(); conn.close()
