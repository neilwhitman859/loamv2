"""
Build the Batch 1 producer roster (~500 producers).

Sources:
1. Josh Test sample — every producer should be in Batch 1
2. LWIN staging — top producers by wine count
3. Wine knowledge — fill gaps for well-known producers not in LWIN

Output: data/batch1_roster.json
"""
import json
import re
import unicodedata
from pipeline.lib.db import get_conn


def strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def normalize_for_match(name: str) -> str:
    """Normalize producer name for fuzzy matching."""
    n = strip_accents(name).lower().strip()
    # Strip common prefixes/suffixes
    for prefix in ["chateau ", "château ", "domaine ", "domaines ", "maison ",
                    "bodegas ", "cave ", "caves ", "cantina ", "tenuta ",
                    "marchesi ", "casa ", "quinta ", "bodega "]:
        if n.startswith(prefix):
            n = n[len(prefix):]
    for suffix in [" vineyards", " vineyard", " winery", " estate",
                   " cellars", " wines", " wine company", " wine co",
                   " & fils", " et fils", " pere & fils", " pere et fils",
                   " freres", " & son", " & sons"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)]
    return n.strip()


# Batch 0 canonical names (skip these)
BATCH_0 = {
    "Château Margaux", "Château Lafite Rothschild", "Château Mouton Rothschild",
    "Château Haut-Brion", "Château Latour", "Ridge Vineyards", "Caymus Vineyards",
    "Stag's Leap Wine Cellars", "Opus One", "Silver Oak Cellars",
    "Giacomo Conterno", "Marchesi Antinori", "Gaja", "Masseto", "Tenuta San Guido",
    "López de Heredia", "La Rioja Alta", "CVNE", "Bodegas Muga", "Marqués de Riscal",
    "J.J. Prüm", "Dr. Loosen", "Dönnhoff", "Egon Müller", "Fritz Haag",
    "Penfolds", "Henschke", "Torbreck", "Tyrrell's", "Yalumba",
    "Barefoot", "Josh Cellars", "Meiomi", "19 Crimes", "Yellow Tail",
    "Louis Jadot", "Louis Latour", "Joseph Drouhin", "Bouchard Père et Fils",
    "Maison Champy", "Dominus Estate", "Screaming Eagle", "Harlan Estate",
    "Scarecrow", "Kendall-Jackson", "Duckhorn Vineyards",
}
BATCH_0_NORM = {normalize_for_match(n) for n in BATCH_0}

# Country name → ISO code mapping
COUNTRY_MAP = {
    "france": "FR", "united states": "US", "italy": "IT", "spain": "ES",
    "germany": "DE", "portugal": "PT", "australia": "AU", "new zealand": "NZ",
    "argentina": "AR", "chile": "CL", "south africa": "ZA", "austria": "AT",
    "greece": "GR", "hungary": "HU", "georgia": "GE", "lebanon": "LB",
    "israel": "IL", "canada": "CA", "united kingdom": "GB", "romania": "RO",
    "slovenia": "SI", "croatia": "HR", "luxembourg": "LU", "switzerland": "CH",
    "uruguay": "UY", "mexico": "MX", "japan": "JP", "china": "CN",
    "brazil": "BR", "turkey": "TR", "morocco": "MA", "tunisia": "TN",
    "bulgaria": "BG", "czech republic": "CZ", "slovakia": "SK",
    "north macedonia": "MK", "moldova": "MD", "serbia": "RS",
}


def main():
    conn = get_conn()
    cur = conn.cursor()

    # =========================================================================
    # Step 1: Get all LWIN producers with wine counts
    # =========================================================================
    print("Loading LWIN producers...")
    cur.execute("""
        SELECT producer_name, country, COUNT(*) as wine_count
        FROM source_lwin
        WHERE producer_name IS NOT NULL
        GROUP BY producer_name, country
        ORDER BY wine_count DESC
    """)
    lwin_producers = {}
    for row in cur.fetchall():
        name, country, count = row
        norm = normalize_for_match(name)
        cc = COUNTRY_MAP.get(country.lower().strip(), "??") if country else "??"
        lwin_producers[norm] = {
            "lwin_name": name,
            "country_full": country,
            "country_code": cc,
            "wine_count": count,
        }
    print(f"  {len(lwin_producers)} distinct LWIN producers")

    # =========================================================================
    # Step 2: Load Josh Test producers
    # =========================================================================
    print("Loading Josh Test producers...")
    with open("data/josh_test_sample.json", encoding="utf-8") as f:
        jt = json.load(f)

    josh_producers = {}
    for w in jt["wines"]:
        p = w["producer"]
        norm = normalize_for_match(p)
        if norm not in josh_producers and norm not in BATCH_0_NORM:
            josh_producers[norm] = {
                "canonical_name": p,
                "country_code": w["country"],
                "price_tier": w["price_tier"],
                "source": "josh_test",
            }
    print(f"  {len(josh_producers)} Josh Test producers (excluding Batch 0)")

    # =========================================================================
    # Step 3: Match Josh Test producers to LWIN
    # =========================================================================
    print("Matching Josh Test to LWIN...")
    roster = []
    unmatched_josh = []

    for norm, jp in josh_producers.items():
        lwin = lwin_producers.get(norm)
        if lwin:
            roster.append({
                "canonical_name": jp["canonical_name"],
                "lwin_name": lwin["lwin_name"],
                "country_code": jp["country_code"],
                "price_tier": jp["price_tier"],
                "source": "josh_test",
                "lwin_wines": lwin["wine_count"],
            })
        else:
            unmatched_josh.append(jp)

    print(f"  Matched: {len(roster)}, Unmatched: {len(unmatched_josh)}")

    # Manual LWIN name mappings for Josh Test producers stored differently in LWIN
    MANUAL_LWIN = {
        "Duval-Leroy": "Duval Leroy",
        "F.X. Pichler": "Franz Xaver Pichler",
        "Jordan Vineyard & Winery": "Jordan",
        "L'Ecole No 41": "Ecole No 41",
        "Marqués de Cáceres": "de Caceres",
        "Perrier-Jouët": "Perrier Jouet",
        "Piper-Heidsieck": "Piper Heidsieck",
        "Viña Errázuriz": "Errazuriz",
        "Castello di Ama": "di Ama",
        "Lindeman's": "Lindemans",
        "Gallo Family Vineyards": "Gallo",
        "Murphy-Goode": "Murphy Goode",
        "A to Z Wineworks": "A to Z",
    }

    for jp in unmatched_josh:
        name = jp["canonical_name"]
        # Check manual mapping first
        if name in MANUAL_LWIN:
            manual = MANUAL_LWIN[name]
            cur.execute("""
                SELECT producer_name, country, COUNT(*) as cnt
                FROM source_lwin
                WHERE UPPER(producer_name) = UPPER(%s)
                GROUP BY producer_name, country
                ORDER BY cnt DESC LIMIT 1
            """, (manual,))
            row = cur.fetchone()
            if row:
                roster.append({
                    "canonical_name": name,
                    "lwin_name": row[0],
                    "country_code": jp["country_code"],
                    "price_tier": jp["price_tier"],
                    "source": "josh_test_manual",
                    "lwin_wines": row[2],
                })
                continue

        # Try multiple query variants
        found = None
        for query_name in [name, strip_accents(name)]:
            cur.execute("""
                SELECT producer_name, country, COUNT(*) as cnt
                FROM source_lwin
                WHERE UPPER(producer_name) = UPPER(%s)
                GROUP BY producer_name, country
                ORDER BY cnt DESC LIMIT 1
            """, (query_name,))
            row = cur.fetchone()
            if row:
                found = row
                break
            # Try without common prefixes/suffixes
            clean = query_name
            for pfx in ["Château ", "Chateau ", "Domaine ", "Bodegas ", "Maison "]:
                if clean.startswith(pfx):
                    clean = clean[len(pfx):]
            for sfx in [" Vineyards", " Vineyard", " Winery", " Estate",
                        " Cellars", " Wines", " Wine Company", " & Winery"]:
                if clean.endswith(sfx):
                    clean = clean[:-len(sfx)]
            if clean != query_name:
                cur.execute("""
                    SELECT producer_name, country, COUNT(*) as cnt
                    FROM source_lwin
                    WHERE UPPER(producer_name) = UPPER(%s)
                    GROUP BY producer_name, country
                    ORDER BY cnt DESC LIMIT 1
                """, (clean,))
                row = cur.fetchone()
                if row:
                    found = row
                    break

        if found:
            cc_found = COUNTRY_MAP.get(found[1].lower().strip(), "??") if found[1] else "??"
            roster.append({
                "canonical_name": jp["canonical_name"],
                "lwin_name": found[0],
                "country_code": jp["country_code"],
                "price_tier": jp["price_tier"],
                "source": "josh_test_db_match",
                "lwin_wines": found[2],
            })
        else:
            # No LWIN match — add with lwin_name=None (TTB-only or skipped)
            roster.append({
                "canonical_name": jp["canonical_name"],
                "lwin_name": None,
                "country_code": jp["country_code"],
                "price_tier": jp["price_tier"],
                "source": "josh_test_no_lwin",
                "lwin_wines": 0,
            })

    print(f"  After fuzzy: {len(roster)} total Josh Test producers in roster")

    # =========================================================================
    # Step 4: Add top LWIN producers to fill to ~500
    # =========================================================================
    print("Adding top LWIN producers...")
    roster_norms = {normalize_for_match(r["canonical_name"]) for r in roster}

    # Sort LWIN producers by wine count descending
    sorted_lwin = sorted(lwin_producers.items(), key=lambda x: x[1]["wine_count"], reverse=True)

    target = 500
    added_lwin = 0
    for norm, ldata in sorted_lwin:
        if len(roster) >= target:
            break
        if norm in roster_norms or norm in BATCH_0_NORM:
            continue
        # Skip junk/aggregator producers
        skip = False
        for junk in ["various", "mixed", "assorted", "selection", "unknown",
                     "anonymous", "n/a", "test", "sample"]:
            if junk in norm:
                skip = True
        if skip:
            continue

        roster.append({
            "canonical_name": ldata["lwin_name"],  # Use LWIN name as canonical for now
            "lwin_name": ldata["lwin_name"],
            "country_code": ldata["country_code"],
            "price_tier": "$30-100",  # Default — most LWIN producers are in this tier
            "source": "lwin_top",
            "lwin_wines": ldata["wine_count"],
        })
        roster_norms.add(norm)
        added_lwin += 1

    print(f"  Added {added_lwin} LWIN producers. Total: {len(roster)}")

    # =========================================================================
    # Step 5: Summary
    # =========================================================================
    print(f"\n{'='*60}")
    print(f"ROSTER SUMMARY: {len(roster)} producers")
    print(f"{'='*60}")

    # By source
    by_source = {}
    for r in roster:
        s = r["source"]
        by_source[s] = by_source.get(s, 0) + 1
    for s, c in sorted(by_source.items()):
        print(f"  {s:25s} {c:4d}")

    # By country
    print("\nBy country:")
    by_country = {}
    for r in roster:
        cc = r["country_code"]
        by_country[cc] = by_country.get(cc, 0) + 1
    for cc, c in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {cc:5s} {c:4d}")

    # By price tier
    print("\nBy price tier:")
    by_tier = {}
    for r in roster:
        t = r["price_tier"]
        by_tier[t] = by_tier.get(t, 0) + 1
    for t in ["$0-10", "$10-30", "$30-100", "$100-250", "$250+"]:
        print(f"  {t:10s} {by_tier.get(t, 0):4d}")

    # LWIN coverage
    with_lwin = sum(1 for r in roster if r["lwin_name"])
    print(f"\nLWIN coverage: {with_lwin}/{len(roster)} ({100*with_lwin/len(roster):.0f}%)")

    # Total LWIN wines
    total_wines = sum(r["lwin_wines"] for r in roster)
    print(f"Total LWIN wines: {total_wines}")

    # =========================================================================
    # Step 6: Save
    # =========================================================================
    output = {
        "_meta": {
            "description": "Batch 1 producer roster for 30K rebuild",
            "total": len(roster),
            "session": "30K Session 5",
        },
        "producers": sorted(roster, key=lambda r: (-r["lwin_wines"], r["canonical_name"])),
    }
    with open("data/batch1_roster.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to data/batch1_roster.json")

    # Show unmatched Josh Test producers
    no_lwin = [r for r in roster if not r["lwin_name"]]
    if no_lwin:
        print(f"\n[!] {len(no_lwin)} producers without LWIN match:")
        for r in no_lwin:
            print(f"  {r['canonical_name']:35s} {r['country_code']}")

    conn.close()


if __name__ == "__main__":
    main()
