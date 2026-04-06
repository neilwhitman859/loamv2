"""
WineTest accuracy and story quality verification.

Uses Haiku to verify:
1. Accuracy — are the key facts in our DB correct?
2. Story — would an enthusiast learn something useful from our data?

Cost: ~$0.01/wine for accuracy, ~$0.01/wine for story. ~$0.60 total for
a 30-wine sample with both checks.
"""

import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipeline.lib.db import get_env  # loads .env

import anthropic

ACCURACY_PROMPT = """\
You are verifying the accuracy of a wine database entry.

The wine being looked up: **{producer} {wine_name}**

Our database contains this data:
- Producer: {db_producer}
- Country: {db_country}
- Region: {db_region}
- Appellation: {db_appellation}
- Color: {db_color}
- Wine type: {db_type}
- Grapes: {db_grapes}

For each field, respond with one of:
- CORRECT — you are confident this is right
- INCORRECT — you are confident this is wrong (explain briefly)
- UNCERTAIN — you don't have enough knowledge to verify
- MISSING — the field was empty/null in our database

Return ONLY a JSON object:
{{
  "producer": {{"verdict": "CORRECT|INCORRECT|UNCERTAIN|MISSING", "note": "..."}},
  "country": {{"verdict": "...", "note": "..."}},
  "region": {{"verdict": "...", "note": "..."}},
  "appellation": {{"verdict": "...", "note": "..."}},
  "color": {{"verdict": "...", "note": "..."}},
  "type": {{"verdict": "...", "note": "..."}},
  "grapes": {{"verdict": "...", "note": "..."}}
}}

Only mark INCORRECT if you are highly confident. When in doubt, use UNCERTAIN.
"""

STORY_PROMPT = """\
You are evaluating whether a wine database entry would be useful to an \
educated wine enthusiast who just looked up this wine.

The wine: **{producer} {wine_name}**

Here is everything our database knows about this wine:

IDENTITY:
- Producer: {db_producer}
- Country: {db_country} | Region: {db_region} | Appellation: {db_appellation}
- Color: {db_color} | Type: {db_type}
- Grapes: {db_grapes}
- Varietal category: {db_varietal_category}

COMMERCE:
- Prices: {db_price_count} price records
- Scores: {db_score_count} score records (avg: {db_score_avg})

DEPTH:
- Vintages: {db_vintage_count}
- Label image: {db_has_image}
- Farming certifications: {db_cert_count}
- Enrichment insights: {db_insight_count}
- Data grade: {db_data_grade}

Rate this entry on a 1-5 scale:
1 = Useless — just a name, no real information
2 = Bare minimum — identity only, nothing an enthusiast wouldn't already know
3 = Decent — has some useful context (scores, prices, region) but gaps remain
4 = Good — an enthusiast would learn something and find it helpful
5 = Excellent — comprehensive, tells the wine's story, would impress a sommelier

Return ONLY a JSON object:
{{"score": N, "reasoning": "One sentence explaining the rating"}}
"""


def score_accuracy(
    findability_results: list[dict],
    conn,
    sample_size: int = 30,
    seed: int | None = None,
) -> dict:
    """Verify accuracy of a random sample of found wines using Haiku.

    Returns dict with per-wine verdicts and aggregate accuracy percentage.
    """
    rng = random.Random(seed)
    found = [r for r in findability_results if r["status"] == "found"]
    if not found:
        return {"sample_size": 0, "accuracy_pct": None, "results": []}

    sample = rng.sample(found, min(sample_size, len(found)))
    wine_ids = [r["wine_id"] for r in sample]

    # Fetch detailed data for sample wines
    wine_details = _fetch_wine_details(conn, wine_ids)

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    results = []
    errors = []

    for r in sample:
        wid = r["wine_id"]
        details = wine_details.get(wid, {})
        bm = r["benchmark"]

        prompt = ACCURACY_PROMPT.format(
            producer=bm.get("producer", ""),
            wine_name=bm.get("name", ""),
            db_producer=details.get("producer_name", "N/A"),
            db_country=details.get("country_name") or "N/A",
            db_region=details.get("region_name") or "N/A",
            db_appellation=details.get("appellation_name") or "N/A",
            db_color=details.get("color") or "N/A",
            db_type=details.get("wine_type") or "N/A",
            db_grapes=details.get("grapes") or "N/A",
        )

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                if text.endswith("```"):
                    text = text[: text.rfind("```")]
                text = text.strip()

            verdicts = json.loads(text)
            fields_checked = 0
            fields_correct = 0
            field_errors = []

            for field, v in verdicts.items():
                verdict = v.get("verdict", "UNCERTAIN") if isinstance(v, dict) else v
                if verdict in ("CORRECT", "MISSING"):
                    fields_correct += 1
                    fields_checked += 1
                elif verdict == "INCORRECT":
                    fields_checked += 1
                    field_errors.append(
                        {
                            "field": field,
                            "note": v.get("note", "") if isinstance(v, dict) else "",
                        }
                    )
                # UNCERTAIN doesn't count toward accuracy

            results.append(
                {
                    "wine_id": wid,
                    "benchmark_producer": bm.get("producer"),
                    "benchmark_name": bm.get("name"),
                    "match_name": r.get("match_wine_name"),
                    "fields_checked": fields_checked,
                    "fields_correct": fields_correct,
                    "accuracy": fields_correct / fields_checked if fields_checked > 0 else None,
                    "errors": field_errors,
                    "verdicts": verdicts,
                }
            )
        except Exception as e:
            errors.append(f"{bm.get('producer')} {bm.get('name')}: {e}")

    # Aggregate
    scored = [r for r in results if r["accuracy"] is not None]
    avg_accuracy = (
        sum(r["accuracy"] for r in scored) / len(scored) if scored else None
    )
    all_errors = [e for r in results for e in r.get("errors", [])]

    return {
        "sample_size": len(sample),
        "wines_checked": len(results),
        "accuracy_pct": round(avg_accuracy * 100, 1) if avg_accuracy is not None else None,
        "total_errors": len(all_errors),
        "errors": all_errors,
        "api_errors": errors,
        "results": results,
    }


def score_story(
    findability_results: list[dict],
    conn,
    sample_size: int = 30,
    seed: int | None = None,
) -> dict:
    """Rate whether our data tells a useful story for an enthusiast.

    Uses the same sample as accuracy (if seed matches) for efficiency.
    Returns dict with per-wine scores and aggregate average.
    """
    rng = random.Random(seed)
    found = [r for r in findability_results if r["status"] == "found"]
    if not found:
        return {"sample_size": 0, "avg_score": None, "results": []}

    sample = rng.sample(found, min(sample_size, len(found)))
    wine_ids = [r["wine_id"] for r in sample]

    wine_details = _fetch_wine_details(conn, wine_ids)
    # Also fetch counts for the story prompt
    wine_counts = _fetch_wine_counts(conn, wine_ids)

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    results = []

    for r in sample:
        wid = r["wine_id"]
        details = wine_details.get(wid, {})
        counts = wine_counts.get(wid, {})
        bm = r["benchmark"]

        prompt = STORY_PROMPT.format(
            producer=bm.get("producer", ""),
            wine_name=bm.get("name", ""),
            db_producer=details.get("producer_name", "N/A"),
            db_country=details.get("country_name") or "N/A",
            db_region=details.get("region_name") or "N/A",
            db_appellation=details.get("appellation_name") or "N/A",
            db_color=details.get("color") or "N/A",
            db_type=details.get("wine_type") or "N/A",
            db_grapes=details.get("grapes") or "N/A",
            db_varietal_category=details.get("varietal_category") or "N/A",
            db_price_count=counts.get("prices", 0),
            db_score_count=counts.get("scores", 0),
            db_score_avg=details.get("critic_score_avg") or "N/A",
            db_vintage_count=counts.get("vintages", 0),
            db_has_image="Yes" if counts.get("images", 0) > 0 else "No",
            db_cert_count=counts.get("certs", 0),
            db_insight_count=counts.get("insights", 0),
            db_data_grade=details.get("data_grade", "F"),
        )

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                if text.endswith("```"):
                    text = text[: text.rfind("```")]
                text = text.strip()

            parsed = json.loads(text)
            results.append(
                {
                    "wine_id": wid,
                    "benchmark_producer": bm.get("producer"),
                    "benchmark_name": bm.get("name"),
                    "score": parsed.get("score", 1),
                    "reasoning": parsed.get("reasoning", ""),
                }
            )
        except Exception as e:
            results.append(
                {
                    "wine_id": wid,
                    "benchmark_producer": bm.get("producer"),
                    "benchmark_name": bm.get("name"),
                    "score": None,
                    "reasoning": f"Error: {e}",
                }
            )

    scored = [r for r in results if r["score"] is not None]
    avg_score = sum(r["score"] for r in scored) / len(scored) if scored else None

    return {
        "sample_size": len(sample),
        "avg_score": round(avg_score, 2) if avg_score is not None else None,
        "avg_score_normalized": round((avg_score - 1) / 4 * 100, 1) if avg_score else None,
        "distribution": {
            i: len([r for r in scored if r["score"] == i]) for i in range(1, 6)
        },
        "results": results,
    }


def _fetch_wine_details(conn, wine_ids: list[str]) -> dict:
    """Fetch detailed wine data for a list of wine IDs."""
    cur = conn.cursor()
    cur.execute(
        """SELECT w.id, w.name, w.color, w.wine_type, w.data_grade,
                  w.critic_score_avg, w.varietal_category_id,
                  p.name as producer_name,
                  r.name as region_name,
                  a.name as appellation_name,
                  c.name as country_name,
                  vc.name as varietal_category
           FROM wines w
           JOIN producers p ON w.producer_id = p.id
           LEFT JOIN regions r ON w.region_id = r.id
           LEFT JOIN appellations a ON w.appellation_id = a.id
           LEFT JOIN countries c ON w.country_id = c.id
           LEFT JOIN varietal_categories vc ON w.varietal_category_id = vc.id
           WHERE w.id = ANY(%s::uuid[])""",
        (wine_ids,),
    )
    details = {}
    for row in cur.fetchall():
        wid = str(row[0])
        details[wid] = {
            "name": row[1],
            "color": row[2],
            "wine_type": row[3],
            "data_grade": row[4],
            "critic_score_avg": float(row[5]) if row[5] else None,
            "varietal_category": row[11],
            "producer_name": row[7],
            "region_name": row[8],
            "appellation_name": row[9],
            "country_name": row[10],
        }

    # Fetch grapes for each wine
    cur.execute(
        """SELECT wg.wine_id, g.display_name
           FROM wine_grapes wg
           JOIN grapes g ON wg.grape_id = g.id
           WHERE wg.wine_id = ANY(%s::uuid[])
           ORDER BY wg.percentage DESC NULLS LAST""",
        (wine_ids,),
    )
    for row in cur.fetchall():
        wid = str(row[0])
        if wid in details:
            if "grapes" not in details[wid] or details[wid]["grapes"] is None:
                details[wid]["grapes"] = row[1]
            else:
                details[wid]["grapes"] += f", {row[1]}"

    cur.close()
    return details


def _fetch_wine_counts(conn, wine_ids: list[str]) -> dict:
    """Fetch aggregate counts for story scoring."""
    cur = conn.cursor()
    counts: dict[str, dict] = {wid: {} for wid in wine_ids}

    for table, col, key in [
        ("wine_vintage_prices", "wine_id", "prices"),
        ("wine_vintage_scores", "wine_id", "scores"),
        ("wine_vintages", "wine_id", "vintages"),
        ("wine_farming_certifications", "wine_id", "certs"),
        ("wine_insights", "wine_id", "insights"),
    ]:
        cur.execute(
            f"""SELECT {col}, COUNT(*) FROM {table}
               WHERE {col} = ANY(%s::uuid[]) GROUP BY {col}""",
            (wine_ids,),
        )
        for row in cur.fetchall():
            wid = str(row[0])
            if wid in counts:
                counts[wid][key] = row[1]

    # Images
    cur.execute(
        """SELECT wine_id, COUNT(*) FROM wine_vintages
           WHERE wine_id = ANY(%s::uuid[]) AND label_image_url IS NOT NULL
           GROUP BY wine_id""",
        (wine_ids,),
    )
    for row in cur.fetchall():
        wid = str(row[0])
        if wid in counts:
            counts[wid]["images"] = row[1]

    cur.close()
    return counts
