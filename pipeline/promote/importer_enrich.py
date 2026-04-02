"""
Promote enrichment data from linked importer staging to canonical tables.

Step 1: Grape → wine_grapes (parse blend strings, resolve to grape IDs)
Step 2: Scores → wine_vintage_scores (parse JSONB arrays from Winebow/EC)
Step 3: Soil/farming/vinification → wines table (KL, Empson, EC text fields)

Usage:
    python -m pipeline.promote.importer_enrich [--dry-run] [--step 1|2|3|all]
"""

import argparse
import re
import unicodedata
from collections import defaultdict

from pipeline.lib.db import get_supabase


def normalize(s):
    """Strip accents and uppercase."""
    n = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in n if not unicodedata.combining(c)).upper().strip()


def load_grape_map(sb):
    """Load grape name → ID mapping from grapes table + synonyms."""
    grape_map = {}  # UPPER name → grape_id

    # Primary names (VIVC stores as uppercase)
    offset = 0
    while True:
        resp = sb.table("grapes").select("id, name, display_name").range(offset, offset + 999).execute()
        if not resp.data:
            break
        for g in resp.data:
            grape_map[g["name"].upper()] = g["id"]
            if g.get("display_name"):
                grape_map[g["display_name"].upper()] = g["id"]
        if len(resp.data) < 1000:
            break
        offset += 1000

    # Manual overrides for common names that VIVC stores differently
    overrides = {
        "CHARDONNAY": "CHARDONNAY BLANC",
        "MERLOT": "MERLOT NOIR",
        "CABERNET SAUVIGNON": "CABERNET SAUVIGNON",
        "SAUVIGNON BLANC": "SAUVIGNON BLANC",
        "PINOT GRIGIO": "PINOT GRIS",
        "SHIRAZ": "SYRAH",
        "MALBEC": "MALBEC",
        "GRENACHE": "GRENACHE NOIR",
        "MOURVEDRE": "MOURVEDRE NOIR",
        "CARIGNAN": "CARIGNAN NOIR",
        "CINSAULT": "CINSAULT",
        "VIOGNIER": "VIOGNIER",
        "MARSANNE": "MARSANNE BLANCHE",
        "ROUSSANNE": "ROUSSANNE",
        "CHENIN BLANC": "CHENIN BLANC",
        "SEMILLON": "SEMILLON",
        "MUSCAT": "MUSCAT BLANC A PETITS GRAINS",
        "MUSCADET": "MELON",
        "MEUNIER": "MEUNIER",
        "PINOT MEUNIER": "MEUNIER",
        "TEMPRANILLO": "TEMPRANILLO TINTO",
        "GARNACHA": "GRENACHE NOIR",
        "MONASTRELL": "MOURVEDRE NOIR",
        "VERDEJO": "VERDEJO BLANCO",
        "ALBARINO": "ALVARINHO",
        "NERO D'AVOLA": "CALABRESE",
        "PRIMITIVO": "PRIMITIVO",
        "PROSECCO": "GLERA",
        "PINOT NERO": "PINOT NOIR",
        "SANGIOVESE GROSSO": "SANGIOVESE",
        "VERDICCHIO": "VERDICCHIO BIANCO",
        "TREBBIANO": "TREBBIANO TOSCANO",
        "MALVASIA": "MALVASIA BIANCA LUNGA",
    }
    for alias, canonical in overrides.items():
        if canonical in grape_map and alias not in grape_map:
            grape_map[alias] = grape_map[canonical]

    print(f"  Loaded {len(grape_map)} grape name mappings")
    return grape_map


def parse_grape_string(grape_str):
    """Parse a grape string into list of (grape_name, percentage).

    Handles:
    - "100% Sangiovese" → [("Sangiovese", 100)]
    - "85% Glera, 15% Pinot Nero" → [("Glera", 85), ("Pinot Nero", 15)]
    - "60% Alicante, 30% Nocera and 10% Nero d'Avola" → [("Alicante", 60), ("Nocera", 30), ("Nero d'Avola", 10)]
    - "Chardonnay" → [("Chardonnay", None)]
    - "Blend - Rhone" → [] (skip blend categories)
    - "Mencia-based blend" → [("Mencia", None)]
    """
    if not grape_str or not grape_str.strip():
        return []

    s = grape_str.strip()

    # Skip blend category labels
    if s.startswith("Blend -") or s.startswith("Blend–") or s == "Blend":
        return []

    # Handle "X-based blend"
    m = re.match(r'^(\w[\w\s\']+?)\s*-based\s+blend', s, re.IGNORECASE)
    if m:
        return [(m.group(1).strip(), None)]

    # Split on comma, semicolon, "and"
    parts = re.split(r'[,;]\s*|\s+and\s+', s)
    results = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # Try to extract percentage
        m = re.match(r'^(\d+(?:\.\d+)?)\s*%?\s+(.+)$', part)
        if m:
            pct = float(m.group(1))
            name = m.group(2).strip()
            results.append((name, pct))
        else:
            # Clean up common patterns
            name = re.sub(r'\s*\(.*?\)\s*', '', part)  # strip parentheticals
            name = name.strip()
            if name and len(name) > 1:
                results.append((name, None))

    return results


def resolve_grape(name, grape_map):
    """Resolve a grape name to a grape ID."""
    upper = normalize(name)
    if upper in grape_map:
        return grape_map[upper]
    # Try without color suffix
    for suffix in [" BLANC", " NOIR", " ROSE", " GRIS", " BIANCO", " ROSSO", " NERO"]:
        if upper + suffix in grape_map:
            return grape_map[upper + suffix]
    return None


def load_publication_map(sb):
    """Load publication name → ID mapping."""
    pub_map = {}
    resp = sb.table("publications").select("id, name").execute()
    for p in resp.data:
        pub_map[p["name"].upper()] = p["id"]
        # Add common short forms
        pub_map[p["name"].lower()] = p["id"]

    # Aliases
    aliases = {
        "JS": "James Suckling", "JR": "Jancis Robinson",
        "WS": "Wine Spectator", "WA": "Wine Advocate", "WE": "Wine Enthusiast",
        "RP": "Wine Advocate",  # Robert Parker
    }
    for alias, full in aliases.items():
        if full.upper() in pub_map:
            pub_map[alias.upper()] = pub_map[full.upper()]

    print(f"  Loaded {len(pub_map)} publication mappings")
    return pub_map


def step_1_grapes(sb, dry_run=False):
    """Promote grape data from importers to wine_grapes."""
    print("\n=== STEP 1: Promote grapes ===\n")

    grape_map = load_grape_map(sb)

    # Load existing wine_grapes to avoid duplicates
    existing = set()
    offset = 0
    while True:
        resp = sb.table("wine_grapes").select("wine_id, grape_id").range(offset, offset + 4999).execute()
        if not resp.data:
            break
        for r in resp.data:
            existing.add((r["wine_id"], r["grape_id"]))
        if len(resp.data) < 5000:
            break
        offset += 5000
    print(f"  Existing wine_grapes: {len(existing)}")

    # Source configs: table, grape column
    sources = [
        ("skurnik", "source_skurnik", "grape"),
        ("kl", "source_kermit_lynch", "blend"),
        ("empson", "source_empson", "grape"),
        ("ec", "source_european_cellars", "grape"),
        ("winebow", "source_winebow", "grape"),
    ]

    total_created = 0
    total_resolved = 0
    total_unresolved = 0
    unresolved_names = defaultdict(int)

    for src_name, table, col in sources:
        # Fetch linked wines with grape data — deduplicate by wine_id
        offset = 0
        rows = []
        while True:
            resp = (
                sb.table(table)
                .select(f"canonical_wine_id, {col}")
                .not_.is_("canonical_wine_id", "null")
                .not_.is_(col, "null")
                .range(offset, offset + 999)
                .execute()
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            if len(resp.data) < 1000:
                break
            offset += 1000

        # Deduplicate by wine_id (take first occurrence)
        by_wine = {}
        for row in rows:
            wid = row["canonical_wine_id"]
            if wid not in by_wine:
                by_wine[wid] = row[col]

        created = 0
        resolved = 0
        unresolved = 0

        for wine_id, grape_str in by_wine.items():
            components = parse_grape_string(grape_str)
            for grape_name, pct in components:
                grape_id = resolve_grape(grape_name, grape_map)
                if grape_id:
                    resolved += 1
                    if (wine_id, grape_id) in existing:
                        continue

                    record = {"wine_id": wine_id, "grape_id": grape_id}
                    if pct is not None:
                        record["percentage"] = pct

                    if not dry_run:
                        try:
                            sb.table("wine_grapes").insert(record).execute()
                            existing.add((wine_id, grape_id))
                            created += 1
                        except Exception as e:
                            if "duplicate" not in str(e).lower():
                                pass  # silently skip
                    else:
                        created += 1
                        existing.add((wine_id, grape_id))
                else:
                    unresolved += 1
                    unresolved_names[grape_name] += 1

        total_created += created
        total_resolved += resolved
        total_unresolved += unresolved
        print(f"  {src_name}: {created} wine_grapes created, {resolved} resolved, {unresolved} unresolved")

    if unresolved_names:
        top_unresolved = sorted(unresolved_names.items(), key=lambda x: -x[1])[:15]
        print(f"\n  Top unresolved grape names:")
        for name, count in top_unresolved:
            print(f"    {name}: {count}")

    print(f"\nStep 1 complete: {total_created} wine_grapes created, {total_unresolved} unresolved")
    return total_created


def step_2_scores(sb, dry_run=False):
    """Promote scores from Winebow and EC to wine_vintage_scores."""
    print("\n=== STEP 2: Promote scores ===\n")

    pub_map = load_publication_map(sb)

    # Load existing scores to avoid duplicates
    existing = set()
    offset = 0
    while True:
        resp = sb.table("wine_vintage_scores").select("wine_id, vintage_year, score, publication_id").range(offset, offset + 4999).execute()
        if not resp.data:
            break
        for r in resp.data:
            existing.add((r["wine_id"], r["vintage_year"], float(r["score"]) if r["score"] else None, r.get("publication_id")))
        if len(resp.data) < 5000:
            break
        offset += 5000
    print(f"  Existing scores: {len(existing)}")

    total_created = 0

    # Winebow: scores is JSONB array with {score, note, publication, vintage}
    for src_name, table in [("winebow", "source_winebow"), ("ec", "source_european_cellars")]:
        offset = 0
        rows = []
        while True:
            resp = (
                sb.table(table)
                .select("canonical_wine_id, scores")
                .not_.is_("canonical_wine_id", "null")
                .not_.is_("scores", "null")
                .range(offset, offset + 999)
                .execute()
            )
            if not resp.data:
                break
            rows.extend(resp.data)
            if len(resp.data) < 1000:
                break
            offset += 1000

        created = 0
        for row in rows:
            wine_id = row["canonical_wine_id"]
            scores = row.get("scores")
            if not scores or not isinstance(scores, list):
                continue

            for s in scores:
                if not isinstance(s, dict) or "score" not in s:
                    continue

                score_val = s.get("score")
                if not score_val or not isinstance(score_val, (int, float)):
                    continue

                vintage_str = s.get("vintage")
                vintage_year = None
                if vintage_str and re.match(r'^\d{4}$', str(vintage_str)):
                    vintage_year = int(vintage_str)

                # Resolve publication
                pub_name = s.get("publication", "")
                pub_id = None
                if pub_name:
                    pub_id = pub_map.get(pub_name.upper()) or pub_map.get(pub_name.lower())

                # Determine score scale
                if score_val <= 20:
                    score_scale = "20"
                else:
                    score_scale = "100"

                # Check duplicate
                key = (wine_id, vintage_year, float(score_val), pub_id)
                if key in existing:
                    continue

                record = {
                    "wine_id": wine_id,
                    "score": score_val,
                    "score_scale": score_scale,
                    "score_provenance": "retailer_quote",
                }
                if vintage_year:
                    record["vintage_year"] = vintage_year
                if pub_id:
                    record["publication_id"] = pub_id
                if s.get("note"):
                    record["tasting_note"] = s["note"][:2000]  # truncate long notes

                if not dry_run:
                    try:
                        sb.table("wine_vintage_scores").insert(record).execute()
                        existing.add(key)
                        created += 1
                    except Exception as e:
                        if "duplicate" not in str(e).lower():
                            pass
                else:
                    created += 1
                    existing.add(key)

        total_created += created
        print(f"  {src_name}: {created} scores created")

    print(f"\nStep 2 complete: {total_created} scores created")
    return total_created


def step_3_winemaking(sb, dry_run=False):
    """Promote soil/vinification/farming data to wines table."""
    print("\n=== STEP 3: Promote winemaking data ===\n")

    total_updated = 0

    # KL: soil, vine_age, vinification → wines
    offset = 0
    kl_rows = []
    while True:
        resp = (
            sb.table("source_kermit_lynch")
            .select("canonical_wine_id, soil, vine_age, vineyard_area, vinification")
            .not_.is_("canonical_wine_id", "null")
            .range(offset, offset + 999)
            .execute()
        )
        if not resp.data:
            break
        kl_rows.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000

    # Deduplicate by wine_id
    kl_by_wine = {}
    for r in kl_rows:
        wid = r["canonical_wine_id"]
        if wid not in kl_by_wine:
            kl_by_wine[wid] = r

    kl_updated = 0
    for wine_id, data in kl_by_wine.items():
        updates = {}
        if data.get("soil"):
            updates["soil_description"] = data["soil"][:500]
        if data.get("vine_age"):
            updates["vine_age_description"] = data["vine_age"][:500]
        if data.get("vinification"):
            # Clean HTML entities
            vin = data["vinification"]
            vin = vin.replace("&nbsp;", " ").replace("&amp;", "&")
            vin = re.sub(r'&[a-z]+;', '', vin)  # strip remaining HTML entities
            vin = re.sub(r'&#\d+;', '', vin)
            updates["vinification_notes"] = vin[:2000]
        if data.get("vineyard_area"):
            # Try to parse hectares
            area = data["vineyard_area"]
            m = re.match(r'^([\d.]+)\s*ha', area, re.IGNORECASE)
            if m:
                try:
                    updates["vineyard_area_ha"] = float(m.group(1))
                except ValueError:
                    pass

        if updates and not dry_run:
            try:
                sb.table("wines").update(updates).eq("id", wine_id).execute()
                kl_updated += 1
            except Exception:
                pass
        elif updates:
            kl_updated += 1

    print(f"  kl: {kl_updated} wines updated with soil/vinification/vine_age")
    total_updated += kl_updated

    # Empson: soil, altitude, vine_age, winemaker → wines
    offset = 0
    emp_rows = []
    while True:
        resp = (
            sb.table("source_empson")
            .select("canonical_wine_id, soil, altitude, vine_age, vineyard_size")
            .not_.is_("canonical_wine_id", "null")
            .range(offset, offset + 999)
            .execute()
        )
        if not resp.data:
            break
        emp_rows.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000

    emp_by_wine = {}
    for r in emp_rows:
        wid = r["canonical_wine_id"]
        if wid not in emp_by_wine:
            emp_by_wine[wid] = r

    emp_updated = 0
    for wine_id, data in emp_by_wine.items():
        updates = {}
        if data.get("soil"):
            updates["soil_description"] = data["soil"][:500]
        if data.get("vine_age"):
            updates["vine_age_description"] = data["vine_age"][:500]
        if data.get("altitude"):
            # Try to parse altitude
            alt = data["altitude"]
            m = re.search(r'(\d+)\s*-\s*(\d+)\s*(?:meters|m)', alt, re.IGNORECASE)
            if m:
                updates["altitude_m_low"] = int(m.group(1))
                updates["altitude_m_high"] = int(m.group(2))
            else:
                m = re.search(r'(\d+)\s*(?:meters|m)', alt, re.IGNORECASE)
                if m:
                    updates["altitude_m_low"] = int(m.group(1))

        if updates and not dry_run:
            try:
                sb.table("wines").update(updates).eq("id", wine_id).execute()
                emp_updated += 1
            except Exception:
                pass
        elif updates:
            emp_updated += 1

    print(f"  empson: {emp_updated} wines updated with soil/altitude/vine_age")
    total_updated += emp_updated

    # EC: soil, altitude, vinification, aging → wines
    offset = 0
    ec_rows = []
    while True:
        resp = (
            sb.table("source_european_cellars")
            .select("canonical_wine_id, soil, altitude, vinification, aging")
            .not_.is_("canonical_wine_id", "null")
            .range(offset, offset + 999)
            .execute()
        )
        if not resp.data:
            break
        ec_rows.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000

    ec_by_wine = {}
    for r in ec_rows:
        wid = r["canonical_wine_id"]
        if wid not in ec_by_wine:
            ec_by_wine[wid] = r

    ec_updated = 0
    for wine_id, data in ec_by_wine.items():
        updates = {}
        if data.get("soil"):
            updates["soil_description"] = data["soil"][:500]
        if data.get("vinification"):
            updates["vinification_notes"] = data["vinification"][:2000]

        if updates and not dry_run:
            try:
                sb.table("wines").update(updates).eq("id", wine_id).execute()
                ec_updated += 1
            except Exception:
                pass
        elif updates:
            ec_updated += 1

    print(f"  ec: {ec_updated} wines updated with soil/vinification")
    total_updated += ec_updated

    print(f"\nStep 3 complete: {total_updated} wines updated with winemaking data")
    return total_updated


def main():
    parser = argparse.ArgumentParser(description="Promote importer enrichment data")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--step", default="all", choices=["1", "2", "3", "all"])
    args = parser.parse_args()

    sb = get_supabase()

    if args.step in ("1", "all"):
        step_1_grapes(sb, dry_run=args.dry_run)

    if args.step in ("2", "all"):
        step_2_scores(sb, dry_run=args.dry_run)

    if args.step in ("3", "all"):
        step_3_winemaking(sb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
