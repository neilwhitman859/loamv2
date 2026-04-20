"""Apply my Phase A calibration verdicts (30 pairs) to the step9_opus_verdicts table."""
import sys
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn
conn = get_conn()
cur = conn.cursor()

# Verdicts for the 30-pair calibration sample.
# Format: (pair_id, verdict, bucket, reasoning, pattern_cluster, pc_parent_direction)
VERDICTS = [
    # AR/AR cluster — sub-brand / spelling / global-brand MERGEs
    (53289,  "MERGE", "auto_decide", "La Riojana Cooperativa: both brands of single coop (lariojana.com.ar). Wine catalog overlap (Torrontes, Malbec) + web convergence.", "11.4.s", None),
    (40076,  "MERGE", "auto_decide", "Same Michelini family operation, Tupungato Mendoza. Jancis Robinson confirms single op. 'Michelini Wine' is short-form.", "11.1", None),
    (143941, "MERGE", "auto_decide", "Bernard Magrez global wine empire (42 vineyards, bernard-magrez.com). Identical brand on label across Argentina + Morocco.", "11.4.n", None),
    (143651, "MERGE", "auto_decide", "Layer Cake = single brand portfolio, Vintage Wine Estates acquisition. Single website layercakewines.com.", "11.4.n", None),
    (142249, "MERGE", "auto_decide", "Peter Wetzer = single Hungarian winemaker (Sopron), works cross-border Burgenland/Sopron. Identical name + Burgenland-Sopron wine zone.", "11.4.b", None),
    (105057, "MERGE", "auto_decide", "The Black Chook / The Chook = same McLaren Vale producer (theblackchook.com.au uses both names). Shared Shiraz Viognier catalog.", "11.1", None),
    (79706,  "ESCALATE_TO_CHROME", "escalate", "Koerner vs Brothers Koerner — ambiguous: could be MERGE (single family op) or PARENT_CHILD (distinct Adelaide Hills line). L2 said SKIP, L2.5 said PC, web said MERGE at 0.89. Check koernerwine.com.au 'Our Wines' page.", "11.4.s", None),
    (141222, "MERGE", "auto_decide", "Tussock Jumper = global curated brand across 11 countries (tussockjumperwines.com). §11.4.n cross-country global brand.", "11.4.n", None),
    (136865, "MERGE", "auto_decide", "Layer Cake global brand, multi-region. Single website + brand portfolio confirms.", "11.4.n", None),
    (136363, "MERGE", "auto_decide", "Wine Spots = single brand (Daniel LeFrancois since 2003), winespotswines.com. Multi-country sourcing (CA, FR, AU).", "11.4.n", None),
    (138965, "MERGE", "auto_decide", "Tussock Jumper AU/US same brand, single website.", "11.4.n", None),
    (138507, "MERGE", "auto_decide", "Tussock Jumper AU/ZA same brand, single website.", "11.4.n", None),
    (83076,  "MERGE", "auto_decide", "Elqui Wines (Steffan Jorgensen/Pamela Nunez, Elqui Valley Chile). Same Instagram @elquiwines + Elixir Wine Group listing for both.", "11.4.s", None),
    (139478, "MERGE", "auto_decide", "Baron Philippe de Rothschild = corporate brand bpdr.com. Escudo Rojo + other lines confirmed. L2.5 SKIP overridden by explicit brand identity.", "11.4.n", None),
    (138283, "MERGE", "auto_decide", "Beefsteak Club = single brand (beefsteakclubwines.com) sourcing multi-region.", "11.4.n", None),
    (138966, "MERGE", "auto_decide", "Tussock Jumper CL/US same brand.", "11.4.n", None),
    (67948,  "ESCALATE_TO_CHROME", "escalate", "Walter vs Walter Gerlach (DE) — substring relationship + shared Mosel/Riesling. Web conf 0.87 (low). L2 MERGE, L2.5 SKIP (cross-tier disagreement). Check weingut-gerlach.de or wein.plus to confirm same estate.", "11.1", None),
    (109,    "MERGE", "auto_decide", "'Wine of The Sea (Dr. H. Thanisch)' = importer/curator prefix + producer in parens. §11.4.i strip prefix, merge to Dr. H. Thanisch. Shared Berncasteler Doctor Riesling GG wine confirms.", "11.4.p", None),
    (141225, "MERGE", "auto_decide", "Tussock Jumper DE/AR same brand.", "11.4.n", None),
    (143326, "MERGE", "auto_decide", "Sea Change (seachangewine.com) = unified eco-brand, multi-region portfolio (Pfalz + Languedoc/Provence).", "11.4.n", None),
    (138969, "MERGE", "auto_decide", "Tussock Jumper DE/US same brand.", "11.4.n", None),
    (108518, "ESCALATE_TO_CHROME", "escalate", "Manzanos vs Dinastia Manzanos [ES] — classic sub-brand ambiguity. Web says MERGE (0.87), L2.5 says PARENT_CHILD (0.85). Per §11.4.s tiebreaker, PC is safer default unless bodegasmanzanos.com shows integrated Manzanos/Dinastia lineup vs separate commercial identities.", "11.4.s", None),
    (51068,  "MERGE", "auto_decide", "Jorge Navascues = single winemaker. 'Navascues Enologia' is oenology consulting entity, 'J. Navascues' is commercial brand. Same Cutio line.", "11.1", None),
    (143328, "MERGE", "auto_decide", "Sea Change ES/FR same eco-brand.", "11.4.n", None),
    (137982, "MERGE", "auto_decide", "Sea Change ES/NZ same eco-brand.", "11.4.n", None),
    (142899, "MERGE", "auto_decide", "Osborne Spanish house since 1722. Single entity with Sherry + Port portfolio. L2+L2.5 both MERGE.", "11.4.n", None),
    (143472, "MERGE", "auto_decide", "Niepoort Portuguese Port house. A's single Ribeira Sacra wine is a legit Dirk Niepoort side project (Ladredo). Main catalog is B (124 wines). L2+L2.5 both MERGE.", "11.4.n", None),
    (140520, "ESCALATE_TO_CHROME", "escalate", "Vermillon vs Vermillion [ES/US] — web claims ES row is data artifact (should be US brand Helen Keplinger). L2+L2.5 both SKIP. Need to verify: is there a real Spanish producer named 'Vermillon' in Ribera del Duero, or is this a mislabel?", "11.4.h / data_state", None),
    (138970, "MERGE", "auto_decide", "Tussock Jumper ES/US same brand.", "11.4.n", None),
    (32540,  "ESCALATE_TO_CHROME", "escalate", "R. Belland-Rombi vs Joseph Belland [FR Burgundy]. Multiple Bellands in Santenay (generational estate splits common). Web conf 0.87, L2+L2.5 both SKIP. Need to verify at domaine-belland.com whether 'Roger-Joseph Belland' is one person (MERGE) or father-son split (SKIP).", "11.4.m", None),
]

# Insert verdicts
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
print(f"Inserted {len(VERDICTS)} calibration verdicts")

# Summary
from collections import Counter
bucket_counts = Counter(v[2] for v in VERDICTS)
verdict_counts = Counter(v[1] for v in VERDICTS)
pattern_counts = Counter(v[4] for v in VERDICTS)
print(f"\nBucket distribution: {dict(bucket_counts)}")
print(f"Verdict distribution: {dict(verdict_counts)}")
print(f"Pattern clusters: {dict(pattern_counts)}")

cur.close(); conn.close()
