"""Sprint 6 Step 7b — Producer sanity scorecard (Part B Deep + enhancements).

For each of the 664 US-encountered matched producers:
  1. DB state: matching rows, wine count, sample wines, external IDs
  2. Post-Step-10 projection via producer_dedup_routing_stage3 actions
  3. Serper web context (cached) — knowledge graph, website, top search results
  4. Flagship wine presence check (for the ~50 producers where flagships are codified)
  5. Sibling producer collision detection (same-country name-neighbors not planned for merge)
  6. LWIN consistency check (does producer have LWIN-backed wines? Is variety reasonable?)
  7. Green / yellow / red status with flags + notes

Output: data/sprints/dedup/producer_sanity_scorecard.json

Cost: ~$1-2 (Serper queries, many cached).
Runtime: 6-10 min.
"""
import sys
import json
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8')

from pipeline.lib.db import get_conn
from pipeline.lib.serper import producer_context


OUTDIR = Path(__file__).resolve().parents[1] / "data" / "sprints" / "dedup"


# ---- Flagship wine registry (50 most iconic producers) ----
# keyed by DB-side name substring (lowercase, normalized); values = list of flagship cuvee/wine name substrings
FLAGSHIPS = {
    # American cult / flagship
    "ridge vineyards": ["monte bello", "lytton springs", "geyserville", "pagani", "three valleys"],
    "ridge ": ["monte bello", "lytton springs", "geyserville"],
    "caymus": ["special selection", "cabernet sauvignon"],
    "stag's leap wine cellars": ["cask 23", "s.l.v.", "slv", "fay", "artemis"],
    "stags leap wine cellars": ["cask 23", "s.l.v.", "slv", "fay", "artemis"],
    "silver oak": ["alexander valley", "napa valley"],
    "opus one": ["opus one"],
    "screaming eagle": ["cabernet"],
    "harlan estate": ["harlan"],
    "dominus estate": ["dominus"],
    "shafer": ["hillside select", "one point five"],
    "joseph phelps": ["insignia"],
    "duckhorn": ["three palms", "cabernet sauvignon"],
    "caymus vineyards": ["special selection", "cabernet sauvignon"],
    "williams selyem": ["pinot noir"],
    "kistler": ["les noisetiers", "mccrea", "durell"],
    "marcassin": ["pinot noir", "chardonnay"],
    "quilceda creek": ["cabernet sauvignon", "red"],
    "continuum": ["continuum"],
    "fort ross": ["pinot noir"],
    "cirq": ["pinot noir"],
    # Bordeaux
    "chateau margaux": ["margaux", "pavillon rouge"],
    "chateau latour": ["latour", "les forts de latour", "pauillac"],
    "chateau lafite": ["lafite", "carruades"],
    "chateau mouton": ["mouton", "le petit mouton"],
    "chateau haut-brion": ["haut-brion"],
    "petrus": ["petrus"],
    "chateau cheval blanc": ["cheval blanc"],
    "chateau d'yquem": ["yquem"],
    "chateau lynch-bages": ["lynch-bages"],
    "chateau palmer": ["palmer", "alter ego"],
    "chateau pichon": ["pichon"],
    "chateau cos d'estournel": ["cos d'estournel"],
    "chateau ducru-beaucaillou": ["ducru"],
    # Burgundy
    "domaine de la romanee-conti": ["romanee-conti", "la tache", "echezeaux", "grands echezeaux", "richebourg", "montrachet"],
    "domaine leflaive": ["montrachet", "puligny", "batard"],
    "domaine leroy": ["musigny", "richebourg", "chambertin", "nuits-saint-georges"],
    "domaine georges roumier": ["musigny", "bonnes mares", "chambolle"],
    "domaine armand rousseau": ["chambertin", "clos de beze", "ruchottes"],
    "domaine dujac": ["clos de la roche", "clos saint-denis", "echezeaux"],
    "domaine coche-dury": ["meursault", "corton-charlemagne"],
    "domaine lafon": ["meursault", "le montrachet"],
    "domaine raveneau": ["chablis", "les clos", "valmur"],
    "domaine rene et vincent dauvissat": ["chablis", "les clos", "preuses"],
    # Champagne
    "krug": ["grande cuvee", "rose", "vintage", "clos du mesnil", "clos d'ambonnay"],
    "dom perignon": ["vintage", "rose", "p2", "p3"],
    "louis roederer": ["cristal", "vintage", "brut premier"],
    "bollinger": ["grande annee", "special cuvee", "r.d."],
    "veuve clicquot": ["brut yellow label", "vintage", "la grande dame"],
    "salon": ["cuvee s", "le mesnil"],
    "pierre peters": ["cuvee de reserve", "les chetillons", "l'esprit"],
    "jacques selosse": ["substance", "initial", "v.o."],
    # Italy
    "giacomo conterno": ["monfortino", "cascina francia", "barolo"],
    "bruno giacosa": ["asili", "rabaja", "falletto", "barolo"],
    "bartolo mascarello": ["barolo"],
    "gaja": ["sperss", "conteisa", "sori tildin", "sori san lorenzo", "costa russi", "barbaresco"],
    "marchesi antinori": ["tignanello", "solaia", "guado al tasso"],
    "antinori": ["tignanello", "solaia", "guado al tasso"],
    "tenuta san guido": ["sassicaia", "guidalberto", "le difese"],
    "ornellaia": ["ornellaia", "masseto", "le serre nuove"],
    "masseto": ["masseto"],
    "fontodi": ["flaccianello", "chianti classico", "vigna del sorbo"],
    "isole e olena": ["cepparello", "chianti classico"],
    "biondi-santi": ["brunello", "riserva"],
    "quintarelli": ["amarone", "alzero", "recioto"],
    # Spain
    "lopez de heredia": ["tondonia", "bosconia", "cubillo", "gravonia"],
    "la rioja alta": ["gran reserva 890", "gran reserva 904", "vina arana"],
    "cvne": ["vina real", "imperial", "real de asua"],
    "bodegas muga": ["prado enea", "torre muga", "aro"],
    "vega sicilia": ["unico", "valbuena", "reserva especial"],
    "dominio de pingus": ["pingus", "flor de pingus"],
    "pingus": ["pingus", "flor de pingus"],
    "alvaro palacios": ["l'ermita", "finca dofi", "les terrasses"],
    # Port / Douro
    "taylor": ["vintage", "single quinta vargellas", "20 year", "10 year"],
    "fonseca": ["vintage", "guimaraens", "20 year"],
    "dow": ["vintage", "quinta do bomfim"],
    "graham": ["vintage", "six grapes", "20 year"],
    "niepoort": ["batuta", "charme", "redoma"],
    "quinta do noval": ["nacional", "vintage", "20 year"],
    # Germany / Austria
    "dr. loosen": ["erdener pralat", "urziger wurzgarten", "wehlener sonnenuhr", "bernkasteler"],
    "j.j. prum": ["wehlener sonnenuhr", "graacher himmelreich", "bernkasteler badstube"],
    "egon muller": ["scharzhofberger"],
    "donnhoff": ["hermannshohle", "dellchen", "felsenberg", "brucke"],
    "f.x. pichler": ["kellerberg", "loibenberg", "smaragd"],
    # Aussie / NZ
    "penfolds": ["grange", "bin 389", "bin 707", "bin 28", "rwt", "bin 150"],
    "henschke": ["hill of grace", "mount edelstone", "cyril"],
    "torbreck": ["runrig", "the laird", "the factor"],
    "cloudy bay": ["sauvignon blanc", "te koko", "pelorus"],
    "felton road": ["block 3", "block 5", "bannockburn"],
    # Rhone
    "m. chapoutier": ["le pavillon", "l'ermite", "le meal"],
    "e. guigal": ["la mouline", "la turque", "la landonne", "ex-voto", "chateau d'ampuis"],
    "guigal": ["la mouline", "la turque", "la landonne"],
    "jean-louis chave": ["hermitage", "saint-joseph", "cathelin"],
    "chateau de beaucastel": ["hommage a jacques perrin", "blanc", "chateauneuf"],
    "chateau rayas": ["chateauneuf", "pignan"],
    # Alsace / Loire
    "trimbach": ["clos sainte hune", "cuvee frederic emile"],
    "zind-humbrecht": ["clos saint urbain", "rangen", "goldert"],
    "domaine huet": ["le mont", "le haut-lieu", "clos du bourg"],
    "domaine tempier": ["la tourtine", "la migoua", "cabassaou", "bandol"],
    # Grocery (flagship single wines)
    "barefoot": ["moscato", "pinot grigio", "merlot", "cabernet"],
    "cupcake": ["cabernet", "chardonnay"],
    "josh cellars": ["cabernet sauvignon", "reserve"],
    "yellow tail": ["shiraz", "chardonnay", "cabernet"],
    "19 crimes": ["red blend", "snoop"],
    "meiomi": ["pinot noir", "cabernet"],
    "apothic": ["red", "crush", "dark"],
    "kendall-jackson": ["vintner's reserve", "grand reserve"],
    "la crema": ["sonoma coast", "russian river"],
    "bogle": ["phantom", "essential red"],
}


def flagship_match_db(cur, producer_id, producer_name: str) -> tuple:
    """Direct DB check for flagship wines (not limited to alphabetical sample).

    Returns (has_flagship_data, matched_flagships, expected_flagships).
    """
    if not producer_name:
        return False, [], []
    pn = producer_name.lower().strip()
    expected = None
    for key, flagships in FLAGSHIPS.items():
        if key in pn or pn.startswith(key.rstrip()):
            expected = flagships
            break
    if not expected:
        return False, [], []
    matched = []
    for flag in expected:
        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM wines w WHERE w.producer_id = %s::uuid
                AND (w.name ILIKE %s OR w.display_name ILIKE %s)
            )
        """, (producer_id, f'%{flag}%', f'%{flag}%'))
        if cur.fetchone()[0]:
            matched.append(flag)
    return True, matched, expected


def main():
    conn = get_conn()
    cur = conn.cursor()

    pids_file = OUTDIR / "us_encountered_producer_ids.json"
    pids = json.loads(pids_file.read_text())
    print(f"Scorecard target: {len(pids):,} producers")

    cur.execute("""
        SELECT EXISTS(
          SELECT 1 FROM information_schema.tables
          WHERE table_name = 'producer_dedup_routing_stage3'
        )
    """)
    has_routing = cur.fetchone()[0]
    if not has_routing:
        print("NOTE: producer_dedup_routing_stage3 not yet built. Projections disabled.")

    scorecard = []
    for i, pid in enumerate(pids, 1):
        if i % 50 == 0:
            print(f"  progress: {i}/{len(pids)}")

        # 1. Current DB state
        cur.execute("""
            SELECT p.id, p.name, c.iso_code,
              (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) AS wine_count,
              (SELECT array_agg(DISTINCT e.system) FROM external_ids e
               WHERE e.entity_type='producer' AND e.entity_id = p.id) AS ext_systems,
              ARRAY(
                SELECT DISTINCT COALESCE(w.display_name, w.name)
                FROM wines w WHERE w.producer_id = p.id
                  AND COALESCE(w.display_name, w.name) IS NOT NULL
                ORDER BY 1 LIMIT 15
              ) AS sample_wines,
              (SELECT COUNT(DISTINCT e.external_id) FROM external_ids e
               JOIN wines w ON w.id = e.entity_id
               WHERE e.entity_type='wine' AND w.producer_id = p.id AND e.system='LWIN') AS distinct_lwin_ids
            FROM producers p
            LEFT JOIN countries c ON c.id = p.country_id
            WHERE p.id = %s::uuid AND p.deleted_at IS NULL
        """, (pid,))
        row = cur.fetchone()
        if not row:
            scorecard.append({
                "producer_id": pid,
                "status": "red",
                "flags": ["producer_deleted_or_missing"],
            })
            continue
        pid_u, name, country, wine_count, ext_systems, sample_wines, distinct_lwin = row

        # 2. Post-Step-10 projection
        projection = {}
        if has_routing:
            cur.execute("""
                SELECT stage3_action, COUNT(*) FROM producer_dedup_routing_stage3
                WHERE (producer_id_a = %s::uuid OR producer_id_b = %s::uuid)
                GROUP BY 1
            """, (pid, pid))
            for act, n in cur.fetchall():
                projection[act] = n

        # 3. Serper web context (cached)
        try:
            ser = producer_context(name, country=country)
            web = {
                "kg_title": (ser.knowledge_graph or {}).get("title") if ser.knowledge_graph else None,
                "kg_type": (ser.knowledge_graph or {}).get("type") if ser.knowledge_graph else None,
                "kg_website": (ser.knowledge_graph or {}).get("website") if ser.knowledge_graph else None,
                "top_links": [o.get("link", "")[:100] for o in (ser.organic or [])[:3]],
                "error": ser.error,
            }
        except Exception as e:
            web = {"error": str(e)[:200]}

        # 4. Flagship check (DB-direct, not limited to the 15-wine sample)
        has_flagship, matched_flagships, expected_flagships = flagship_match_db(cur, pid, name)
        flagship_info = None
        if has_flagship:
            flagship_info = {
                "expected": expected_flagships,
                "matched": matched_flagships,
                "coverage_pct": round(100 * len(matched_flagships) / max(1, len(expected_flagships)), 1),
            }

        # 5. Sibling producer check — STRICT: same country + first-normalized-word matches
        # (first word after stripping "domaine", "chateau", "bodega", etc.)
        import re
        NOISE = {"domaine", "chateau", "ch\u00e2teau", "bodega", "bodegas", "tenuta", "casa", "cantina",
                 "weingut", "clos", "maison", "the", "le", "la", "de", "du", "di"}
        def first_word(s):
            toks = re.findall(r"\w+", (s or "").lower())
            toks = [t for t in toks if t not in NOISE]
            return toks[0] if toks else ""
        pname_fw = first_word(name)
        cur.execute("""
            SELECT p2.id, p2.name, similarity(p2.name, %s) AS sim
            FROM producers p2
            WHERE p2.id != %s::uuid
              AND p2.deleted_at IS NULL
              AND p2.country_id = (SELECT country_id FROM producers WHERE id = %s::uuid)
              AND similarity(p2.name, %s) > 0.55
            ORDER BY sim DESC LIMIT 15
        """, (name, pid, pid, name))
        all_siblings = cur.fetchall()
        # Keep only ones with first-word match (stricter)
        siblings = [s for s in all_siblings if first_word(s[1]) == pname_fw and pname_fw]
        unmerged_siblings = []
        if siblings and has_routing:
            sibling_ids = [s[0] for s in siblings]
            cur.execute("""
                SELECT producer_id_a, producer_id_b, stage3_action
                FROM producer_dedup_routing_stage3
                WHERE ((producer_id_a = %s::uuid AND producer_id_b = ANY(%s::uuid[]))
                    OR (producer_id_b = %s::uuid AND producer_id_a = ANY(%s::uuid[])))
            """, (pid, sibling_ids, pid, sibling_ids))
            planned = {(str(r[0]), str(r[1])): r[2] for r in cur.fetchall()}
            for sid, sname, sim in siblings:
                key1 = (str(pid), str(sid))
                key2 = (str(sid), str(pid))
                act = planned.get(key1) or planned.get(key2)
                if not act or act.startswith("auto_apply_skip"):
                    unmerged_siblings.append({"name": sname, "similarity": float(sim), "action": act or "no_pair"})

        # 6. Automated status determination
        flags = []
        status = "green"

        if wine_count is None or wine_count == 0:
            flags.append("no_wines_linked")
            status = "red"
        elif wine_count < 3 and country != "US":
            flags.append(f"low_wine_count_{wine_count}")
            if status == "green":
                status = "yellow"

        if has_flagship and flagship_info["coverage_pct"] < 40:
            flags.append(f"flagship_coverage_low_{flagship_info['coverage_pct']}%")
            if status == "green":
                status = "yellow"

        if len(unmerged_siblings) >= 1:
            # Stricter: any true same-first-word sibling that we're not merging
            flags.append(f"unmerged_same_firstword_sibling_{len(unmerged_siblings)}")
            if status in ("green", "yellow"):
                status = "yellow"

        if web.get("error"):
            flags.append("serper_error")
        elif not web.get("kg_title") and not web.get("top_links"):
            flags.append("no_web_presence")
            if status == "green":
                status = "yellow"

        # LWIN consistency: if producer has wines but very few distinct LWINs, may indicate variants unmerged
        if wine_count and wine_count > 20 and distinct_lwin is not None:
            lwin_coverage = distinct_lwin / wine_count if wine_count else 0
            if 0 < lwin_coverage < 0.3:
                flags.append(f"low_lwin_coverage_{round(100*lwin_coverage)}%")

        scorecard.append({
            "producer_id": str(pid_u),
            "name": name,
            "country": country,
            "wine_count": wine_count,
            "distinct_lwin_ids": distinct_lwin,
            "external_id_systems": ext_systems,
            "sample_wines": sample_wines,
            "projected_post_step10": projection,
            "web": web,
            "flagship": flagship_info,
            "unmerged_siblings": unmerged_siblings,
            "status": status,
            "flags": flags,
        })

    outp = OUTDIR / "producer_sanity_scorecard.json"
    outp.write_text(json.dumps(scorecard, default=str, indent=2))
    print()
    print(f"Scorecard written: {outp}")

    from collections import Counter
    status_counts = Counter(s.get("status") for s in scorecard)
    print()
    print("=== Status summary ===")
    for s, n in sorted(status_counts.items()):
        print(f"  {s:<10} {n}")

    # Flag frequency
    flag_counter = Counter()
    for s in scorecard:
        for f in s.get("flags", []):
            flag_counter[f.split("_")[0] + "_" + (f.split("_")[1] if "_" in f else "")] += 1
    print()
    print("=== Top flag categories ===")
    for flag, n in flag_counter.most_common(15):
        print(f"  {flag:<40} {n}")

    print()
    print("=== First 25 red + yellow producers (for Step 9 review) ===")
    shown = 0
    for row in scorecard:
        if row.get("status") in ("red", "yellow") and shown < 25:
            print(f"  [{row['status']}] {row.get('name', '')[:50]:<52} "
                  f"country={row.get('country', '??'):<4} "
                  f"wines={str(row.get('wine_count', '-')):<4} "
                  f"flags={row.get('flags', [])}")
            shown += 1

    cur.close(); conn.close()


if __name__ == "__main__":
    main()
