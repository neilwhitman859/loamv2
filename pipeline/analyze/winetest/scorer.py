"""
WineTest scoring engine.

Scores the Loam database against a benchmark wine list across two dimensions:
1. Findability — can we find each benchmark wine?
2. Depth — for found wines, how complete is the data?

Also includes blind spot detection (pattern analysis on misses).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipeline.lib.normalize import normalize, normalize_producer, normalize_wine_name


# Depth category weights (user-agreed: identity/price/quality = high,
# story/visual = medium, vintage/winemaking = low)
DEPTH_WEIGHTS = {
    "identity": 3,
    "price": 3,
    "quality": 3,
    "story": 2,
    "visual": 2,
    "vintage": 1,
    "winemaking": 1,
}


def score_findability(benchmark_wines: list[dict], conn) -> list[dict]:
    """Search for each benchmark wine in our DB.

    Matching stages:
    1. Exact normalized producer → exact/fuzzy/partial wine name
    2. Fuzzy producer (verified) → exact/fuzzy/partial wine name
    3. Fallback: grape-as-name (search by grape variety within producer)
    4. Fallback: eponymous wine (producer name = wine name)
    5. Fallback: keyword match (individual significant words)

    Returns list of result dicts with status, wine_id, confidence.
    """
    cur = conn.cursor()

    # Preload producer lookup (name_normalized → id, name)
    cur.execute("SELECT id, name, name_normalized FROM producers WHERE deleted_at IS NULL")
    producer_index = {}
    for row in cur.fetchall():
        pid, pname, pnorm = row
        if pnorm:
            producer_index[pnorm] = {"id": str(pid), "name": pname}

    # Preload grape names for grape-as-name fallback
    cur.execute("SELECT display_name FROM grapes")
    grape_names = set()
    for row in cur.fetchall():
        grape_names.add(normalize(row[0]))

    results = []
    for i, wine in enumerate(benchmark_wines):
        result = {
            "benchmark": wine,
            "status": "not_found",
            "wine_id": None,
            "producer_id": None,
            "match_wine_name": None,
            "match_producer_name": None,
            "confidence": 0.0,
        }

        producer_norm = normalize_producer(wine.get("producer", ""))
        wine_name_norm = normalize_wine_name(wine.get("name", ""))

        if not producer_norm:
            results.append(result)
            continue

        # --- Stage 1: Find producer ---
        producer_match = None

        # Exact match
        if producer_norm in producer_index:
            producer_match = producer_index[producer_norm]
            producer_match["confidence"] = 1.0

        # Fuzzy match via pg_trgm — with verification to prevent false positives
        # (e.g., "Pichon Longueville" → "Assortment Case")
        if not producer_match:
            try:
                cur.execute(
                    """SELECT id, name, name_normalized,
                              similarity(name_normalized, %s) as sim
                    FROM producers
                    WHERE name_normalized %% %s AND deleted_at IS NULL
                    ORDER BY sim DESC LIMIT 1""",
                    (producer_norm, producer_norm),
                )
                row = cur.fetchone()
                if row and row[3] >= 0.35:
                    match_norm = row[2] or ""
                    sim = float(row[3])
                    # Verify: either high similarity OR one name contains the other
                    is_substring = (
                        producer_norm in match_norm or match_norm in producer_norm
                    )
                    if sim >= 0.55 or is_substring:
                        producer_match = {
                            "id": str(row[0]),
                            "name": row[1],
                            "confidence": sim,
                        }
            except Exception:
                conn.rollback()

        if producer_match:
            result["producer_id"] = producer_match["id"]
            result["match_producer_name"] = producer_match["name"]
            result["status"] = "producer_only"
            pid = producer_match["id"]

            # --- Stage 2: Find wine within producer ---
            wine_match = _match_wine(cur, conn, pid, wine_name_norm, producer_norm, grape_names)

            if wine_match:
                result["status"] = "found"
                result["wine_id"] = wine_match["id"]
                result["match_wine_name"] = wine_match["name"]
                result["confidence"] = min(
                    producer_match["confidence"], wine_match["confidence"]
                )

        # --- Stage 3: Swap-and-retry ---
        # Catches "Chianti Classico - Antinori" where Haiku put the appellation
        # as producer and the real producer as the wine name.
        # Try treating the "name" field as producer and "producer" as wine name.
        if result["status"] in ("not_found", "producer_only") and wine_name_norm:
            swap_prod_norm = normalize_producer(wine.get("name", ""))
            swap_wine_norm = normalize_wine_name(wine.get("producer", ""))

            if swap_prod_norm and swap_prod_norm != producer_norm:
                swap_match = None
                if swap_prod_norm in producer_index:
                    swap_match = producer_index[swap_prod_norm].copy()
                    swap_match["confidence"] = 0.9  # slight penalty for swap

                if swap_match:
                    swap_wine = _match_wine(
                        cur, conn, swap_match["id"],
                        swap_wine_norm, swap_prod_norm, grape_names,
                    )
                    if swap_wine:
                        result["status"] = "found"
                        result["producer_id"] = swap_match["id"]
                        result["match_producer_name"] = swap_match["name"]
                        result["wine_id"] = swap_wine["id"]
                        result["match_wine_name"] = swap_wine["name"]
                        result["confidence"] = min(
                            swap_match["confidence"], swap_wine["confidence"]
                        )

        results.append(result)

        if (i + 1) % 50 == 0:
            print(f"    Searched {i + 1}/{len(benchmark_wines)}...")

    cur.close()
    return results


def _match_wine(cur, conn, producer_id, wine_name_norm, producer_norm, grape_names):
    """Try multiple strategies to match a wine within a producer.

    Returns dict with id, name, confidence or None.
    """
    if wine_name_norm:
        # --- Try 1: Exact normalized name ---
        cur.execute(
            """SELECT id, name FROM wines
            WHERE producer_id = %s AND name_normalized = %s
            AND deleted_at IS NULL LIMIT 1""",
            (producer_id, wine_name_norm),
        )
        row = cur.fetchone()
        if row:
            return {"id": str(row[0]), "name": row[1], "confidence": 1.0}

        # --- Try 2: Fuzzy name ---
        try:
            cur.execute(
                """SELECT id, name, similarity(name_normalized, %s) as sim
                FROM wines
                WHERE producer_id = %s AND name_normalized %% %s
                AND deleted_at IS NULL
                ORDER BY sim DESC LIMIT 1""",
                (wine_name_norm, producer_id, wine_name_norm),
            )
            row = cur.fetchone()
            if row and row[2] >= 0.3:
                return {"id": str(row[0]), "name": row[1], "confidence": float(row[2])}
        except Exception:
            conn.rollback()

        # --- Try 3: Partial/substring match ---
        cur.execute(
            """SELECT id, name FROM wines
            WHERE producer_id = %s AND deleted_at IS NULL
            AND (name_normalized LIKE %s OR %s LIKE '%%' || name_normalized || '%%')
            ORDER BY length(name) LIMIT 1""",
            (producer_id, f"%{wine_name_norm}%", wine_name_norm),
        )
        row = cur.fetchone()
        if row:
            return {"id": str(row[0]), "name": row[1], "confidence": 0.6}

        # --- Try 4: Grape-as-name fallback ---
        # If the wine name IS a grape variety (e.g., "Cabernet Sauvignon"),
        # find any wine by this producer that has that grape in its name
        # or linked via wine_grapes
        if wine_name_norm in grape_names:
            # First try: wine name contains the grape
            cur.execute(
                """SELECT id, name FROM wines
                WHERE producer_id = %s AND deleted_at IS NULL
                AND name_normalized LIKE %s
                ORDER BY data_grade ASC, name LIMIT 1""",
                (producer_id, f"%{wine_name_norm}%"),
            )
            row = cur.fetchone()
            if row:
                return {"id": str(row[0]), "name": row[1], "confidence": 0.55}

            # Second try: wine has this grape linked via wine_grapes
            try:
                cur.execute(
                    """SELECT DISTINCT w.id, w.name FROM wines w
                    JOIN wine_grapes wg ON w.id = wg.wine_id
                    JOIN grapes g ON wg.grape_id = g.id
                    WHERE w.producer_id = %s AND w.deleted_at IS NULL
                    AND lower(g.display_name) = %s
                    ORDER BY w.data_grade ASC, w.name LIMIT 1""",
                    (producer_id, wine_name_norm),
                )
                row = cur.fetchone()
                if row:
                    return {"id": str(row[0]), "name": row[1], "confidence": 0.5}
            except Exception:
                conn.rollback()

        # --- Try 5: Keyword match ---
        # Try each significant word from the wine name individually
        # e.g., "Riesling Semi-Dry" → try "riesling", then "semi dry"
        words = [w for w in wine_name_norm.split() if len(w) >= 4]
        for word in words:
            if word in grape_names:
                # This word is a grape — try it as name substring
                cur.execute(
                    """SELECT id, name FROM wines
                    WHERE producer_id = %s AND deleted_at IS NULL
                    AND name_normalized LIKE %s
                    ORDER BY data_grade ASC, name LIMIT 1""",
                    (producer_id, f"%{word}%"),
                )
                row = cur.fetchone()
                if row:
                    return {"id": str(row[0]), "name": row[1], "confidence": 0.45}

        # --- Try 6: Eponymous wine ---
        # Producer name = wine name (e.g., "Chateau Margaux" producer has wine "Chateau Margaux")
        cur.execute(
            """SELECT id, name FROM wines
            WHERE producer_id = %s AND deleted_at IS NULL
            AND (name_normalized LIKE %s OR %s LIKE '%%' || name_normalized || '%%')
            ORDER BY data_grade ASC, name LIMIT 1""",
            (producer_id, f"%{producer_norm}%", producer_norm),
        )
        row = cur.fetchone()
        if row:
            return {"id": str(row[0]), "name": row[1], "confidence": 0.5}

    else:
        # No wine name — try eponymous or most prominent wine
        cur.execute(
            """SELECT w.id, w.name FROM wines w
            WHERE w.producer_id = %s AND w.deleted_at IS NULL
            ORDER BY w.data_grade ASC, w.created_at ASC
            LIMIT 1""",
            (producer_id,),
        )
        row = cur.fetchone()
        if row:
            return {"id": str(row[0]), "name": row[1], "confidence": 0.7}

    return None


def score_depth(findability_results: list[dict], conn) -> list[dict]:
    """Score data completeness for found wines.

    Uses bulk SQL queries for efficiency — runs ~8 queries total
    regardless of benchmark size.
    """
    found = [r for r in findability_results if r["status"] == "found"]
    if not found:
        return []

    wine_ids = [r["wine_id"] for r in found]
    cur = conn.cursor()

    # --- Bulk fetch: wine base data ---
    cur.execute(
        """SELECT w.id, w.producer_id, w.region_id, w.appellation_id, w.country_id,
                  w.color, w.wine_type, w.data_grade, w.varietal_category_id,
                  p.name as producer_name,
                  CASE WHEN w.region_id IS NOT NULL THEN 1 ELSE 0 END as has_region,
                  CASE WHEN w.appellation_id IS NOT NULL THEN 1 ELSE 0 END as has_appellation,
                  CASE WHEN w.country_id IS NOT NULL THEN 1 ELSE 0 END as has_country
           FROM wines w
           JOIN producers p ON w.producer_id = p.id
           WHERE w.id = ANY(%s::uuid[])""",
        (wine_ids,),
    )
    wine_data = {}
    for row in cur.fetchall():
        wine_data[str(row[0])] = {
            "producer_id": str(row[1]),
            "has_region": bool(row[10]),
            "has_appellation": bool(row[11]),
            "has_country": bool(row[12]),
            "has_color": row[5] is not None,
            "has_type": row[6] is not None,
            "has_varietal_category": row[8] is not None,
            "data_grade": row[7],
            "producer_name": row[9],
        }

    # --- Bulk fetch: grape counts ---
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_grapes
           WHERE wine_id = ANY(%s::uuid[]) GROUP BY wine_id""",
        (wine_ids,),
    )
    grape_counts = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Bulk fetch: vintage counts + ABV ---
    cur.execute(
        """SELECT wine_id, COUNT(*), COUNT(abv) FROM wine_vintages
           WHERE wine_id = ANY(%s::uuid[]) GROUP BY wine_id""",
        (wine_ids,),
    )
    vintage_data = {str(r[0]): {"count": r[1], "abv_count": r[2]} for r in cur.fetchall()}

    # --- Bulk fetch: score counts ---
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_vintage_scores
           WHERE wine_id = ANY(%s::uuid[]) GROUP BY wine_id""",
        (wine_ids,),
    )
    score_counts = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Bulk fetch: price counts ---
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_vintage_prices
           WHERE wine_id = ANY(%s::uuid[]) GROUP BY wine_id""",
        (wine_ids,),
    )
    price_counts = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Bulk fetch: label images ---
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_vintages
           WHERE wine_id = ANY(%s::uuid[]) AND label_image_url IS NOT NULL
           GROUP BY wine_id""",
        (wine_ids,),
    )
    image_counts = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Bulk fetch: farming certs ---
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_farming_certifications
           WHERE wine_id = ANY(%s::uuid[]) GROUP BY wine_id""",
        (wine_ids,),
    )
    cert_counts = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Bulk fetch: enrichment (wine_insights) ---
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_insights
           WHERE wine_id = ANY(%s::uuid[]) GROUP BY wine_id""",
        (wine_ids,),
    )
    insight_counts = {str(r[0]): r[1] for r in cur.fetchall()}

    # --- Score each wine ---
    depth_results = []
    for r in found:
        wid = r["wine_id"]
        wd = wine_data.get(wid, {})

        # Identity: producer(always), region, appellation, country, grapes, color, type (7 fields)
        identity_fields = [
            True,  # producer always present
            wd.get("has_region", False),
            wd.get("has_appellation", False),
            wd.get("has_country", False),
            grape_counts.get(wid, 0) > 0,
            wd.get("has_color", False),
            wd.get("has_type", False),
        ]
        identity = sum(identity_fields) / len(identity_fields)

        # Price: has any price, has 2+ prices
        pc = price_counts.get(wid, 0)
        price_fields = [pc > 0, pc >= 2]
        price = sum(price_fields) / len(price_fields)

        # Quality: has any score, has 2+ scores, has data_grade <= C
        sc = score_counts.get(wid, 0)
        grade = wd.get("data_grade", "F")
        quality_fields = [
            sc > 0,
            sc >= 2,
            grade in ("A", "B", "C"),
        ]
        quality = sum(quality_fields) / len(quality_fields)

        # Story: has enrichment insights, data_grade is B or better
        ic = insight_counts.get(wid, 0)
        story_fields = [
            ic > 0,
            grade in ("A", "B"),
        ]
        story = sum(story_fields) / len(story_fields)

        # Visual: has label image
        im = image_counts.get(wid, 0)
        visual = 1.0 if im > 0 else 0.0

        # Vintage: has vintages, has ABV
        vd = vintage_data.get(wid, {"count": 0, "abv_count": 0})
        vintage_fields = [vd["count"] > 0, vd["abv_count"] > 0, vd["count"] >= 3]
        vintage = sum(vintage_fields) / len(vintage_fields)

        # Winemaking: has farming certs, has varietal category
        winemaking_fields = [
            cert_counts.get(wid, 0) > 0,
            wd.get("has_varietal_category", False),
        ]
        winemaking = sum(winemaking_fields) / len(winemaking_fields)

        # Weighted overall
        categories = {
            "identity": identity,
            "price": price,
            "quality": quality,
            "story": story,
            "visual": visual,
            "vintage": vintage,
            "winemaking": winemaking,
        }
        total_weight = sum(DEPTH_WEIGHTS.values())
        overall = sum(
            categories[k] * DEPTH_WEIGHTS[k] for k in categories
        ) / total_weight

        depth_results.append(
            {
                "wine_id": wid,
                "benchmark": r["benchmark"],
                "match_wine_name": r.get("match_wine_name"),
                "categories": categories,
                "overall": overall,
            }
        )

    cur.close()
    return depth_results


def detect_blind_spots(findability_results: list[dict]) -> list[dict]:
    """Analyze patterns in missed wines to find systematic gaps.

    Groups misses by country, color, price tier, and category,
    and flags any group with >50% miss rate.
    """
    dimensions = ["country", "color", "price_tier", "category"]
    blind_spots = []

    for dim in dimensions:
        groups: dict[str, dict] = {}
        for r in findability_results:
            val = r["benchmark"].get(dim, "unknown")
            if not val:
                val = "unknown"
            val = val.strip().lower()
            if val not in groups:
                groups[val] = {"total": 0, "missed": 0}
            groups[val]["total"] += 1
            if r["status"] == "not_found":
                groups[val]["missed"] += 1

        for val, counts in groups.items():
            if counts["total"] < 3:
                continue  # too few to be meaningful
            miss_rate = counts["missed"] / counts["total"]
            if miss_rate > 0.5:
                blind_spots.append(
                    {
                        "dimension": dim,
                        "value": val,
                        "miss_rate": round(miss_rate, 2),
                        "missed": counts["missed"],
                        "total": counts["total"],
                    }
                )

    blind_spots.sort(key=lambda x: x["miss_rate"], reverse=True)
    return blind_spots
