"""
Wine Expert Sonnet Review of all region boundaries.
Uses Claude Sonnet to review every region as a wine expert would,
checking for attribution issues, missing areas, and boundary sanity.

Batches by country for efficiency. Outputs a detailed report and
can automatically apply corrections (with logging).

Usage:
  python -m pipeline.geo.review_region_boundaries --dry-run
  python -m pipeline.geo.review_region_boundaries --apply
  python -m pipeline.geo.review_region_boundaries --country FR
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase, get_env
from pipeline.geo.helpers import fetch_all_paginated

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SYSTEM_PROMPT = """You are a Master of Wine reviewing a wine database's region boundaries and appellation attributions. Your role is to ensure every region is correctly structured and bounded as a wine professional would expect.

Context: This is a two-level region hierarchy (L1 parent, L2 child). Each region has appellations attributed to it. Region boundaries were derived from child appellation polygon unions, copied from matching appellations, or fetched from administrative boundary databases.

Key principle: Appellations should be on the LOWEST-LEVEL region they can accurately be attributed to. They roll up naturally from L2 → L1.

Key distinction: Appellations are legally defined. Regions are qualitative approximations of wine-producing areas. It's OK for region boundaries to be somewhat approximate.

For each country, review:
1. ATTRIBUTION: Are appellations on the right regions? Should any catch-all appellations move to a named region?
2. MISSING REGIONS: Are there important wine regions missing from the hierarchy?
3. BOUNDARY SANITY: Do the boundary sources make sense? Any obviously wrong boundaries?
4. NAMING: Are regions named as a WSET L3 student or MW would expect?

Respond with JSON only. Format:
{
  "country": "Country Name",
  "overall_assessment": "pass" | "minor_issues" | "major_issues",
  "corrections": [
    {
      "type": "move_appellation" | "rename_region" | "flag_review" | "smooth_boundary",
      "description": "What and why",
      "region_slug": "affected-region",
      "appellation_name": "if applicable",
      "severity": "auto_fix" | "needs_review"
    }
  ],
  "notes": "Brief overall assessment"
}

Rules:
- Only suggest corrections you're confident about as a wine expert
- "move_appellation" = move from catch-all to a named region (must specify which)
- "smooth_boundary" = suggest using ST_ConvexHull to fill gaps in derived boundaries
- "flag_review" = something looks off but you're not sure enough to auto-fix
- Keep corrections actionable and specific
- If everything looks good, return empty corrections array"""


def call_sonnet(system_prompt: str, user_message: str, max_tokens: int = 4096) -> str:
    """Call Anthropic Sonnet API with retries."""
    api_key = get_env("ANTHROPIC_API_KEY")
    for attempt in range(3):
        try:
            resp = httpx.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "content-type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                },
                timeout=120,
            )
            if resp.status_code in (429, 529):
                wait = min(30, 5 * (attempt + 1))
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data["content"][0]["text"]
        except Exception as e:
            if attempt == 2:
                raise
            print(f"    Retry {attempt + 1}: {e}")
            time.sleep(3)
    raise Exception("Failed after retries")


def load_data(sb):
    """Load all data needed for review."""
    all_regions = fetch_all_paginated(
        sb, "regions", "id, slug, name, parent_id, country_id, is_catch_all"
    )
    all_regions = [r for r in all_regions if not r.get("is_catch_all") and r.get("deleted_at") is None]

    countries = sb.table("countries").select("id, name, iso_code").is_("deleted_at", "null").execute().data
    country_by_id = {c["id"]: c for c in countries}

    all_bounds = fetch_all_paginated(
        sb, "geographic_boundaries", "region_id, boundary_confidence, boundary_source, centroid, boundary"
    )
    bound_by_region = {b["region_id"]: b for b in all_bounds if b.get("region_id")}

    all_apps = fetch_all_paginated(
        sb, "appellations", "id, slug, name, region_id, country_id, classification_level"
    )

    catch_alls = sb.table("regions").select("id, country_id").is_("deleted_at", "null").eq("is_catch_all", True).execute().data
    catch_all_by_country = {ca["country_id"]: ca["id"] for ca in catch_alls}

    return {
        "all_regions": all_regions,
        "country_by_id": country_by_id,
        "bound_by_region": bound_by_region,
        "all_apps": all_apps,
        "catch_all_by_country": catch_all_by_country,
    }


def build_country_review(country_id: str, data: dict) -> dict:
    """Build review data for a single country."""
    country = data["country_by_id"][country_id]
    regions = [r for r in data["all_regions"] if r["country_id"] == country_id]
    region_by_id = {r["id"]: r for r in regions}

    l2_regions = [r for r in regions if r.get("parent_id")]

    apps_by_region = {}
    catch_all_apps = []
    catch_all_id = data["catch_all_by_country"].get(country_id)

    for a in data["all_apps"]:
        if a["country_id"] != country_id:
            continue
        if a["region_id"] == catch_all_id:
            catch_all_apps.append(a)
        elif a["region_id"] and region_by_id.get(a["region_id"]):
            apps_by_region.setdefault(a["region_id"], []).append(a)

    region_summaries = []
    for r in regions:
        bound = data["bound_by_region"].get(r["id"])
        apps = apps_by_region.get(r["id"], [])
        level = "L2" if r.get("parent_id") else "L1"
        parent = region_by_id.get(r.get("parent_id"))
        children = [c["name"] for c in l2_regions if c.get("parent_id") == r["id"]]

        region_summaries.append({
            "slug": r["slug"],
            "name": r["name"],
            "level": level,
            "parent": parent["name"] if parent else None,
            "children": children,
            "boundary": {
                "confidence": bound["boundary_confidence"],
                "source": bound["boundary_source"],
                "hasPolygon": bool(bound.get("boundary")),
            } if bound else None,
            "appellations": [{"name": a["name"], "level": a.get("classification_level")} for a in apps],
        })

    return {
        "country": country["name"],
        "iso": country["iso_code"],
        "regionCount": len(regions),
        "catchAllApps": [{"name": a["name"], "level": a.get("classification_level")} for a in catch_all_apps],
        "regions": region_summaries,
    }


def apply_corrections(results, data, sb):
    """Apply auto-fix corrections."""
    region_by_slug = {r["slug"]: r for r in data["all_regions"]}

    for result in results:
        auto_fixes = [c for c in (result.get("corrections") or []) if c.get("severity") == "auto_fix"]
        if not auto_fixes:
            continue

        for fix in auto_fixes:
            if fix["type"] == "move_appellation" and fix.get("region_slug") and fix.get("appellation_name"):
                region = region_by_slug.get(fix["region_slug"])
                if not region:
                    print(f"  WARNING Region not found: {fix['region_slug']}")
                    continue

                app = next(
                    (a for a in data["all_apps"]
                     if a["name"] == fix["appellation_name"] and a["country_id"] == region["country_id"]),
                    None,
                )
                if not app:
                    print(f"  WARNING Appellation not found: {fix['appellation_name']}")
                    continue

                sb.table("appellations").update({"region_id": region["id"]}).eq("id", app["id"]).execute()
                print(f"  ok Moved {fix['appellation_name']} -> {region['name']}")

            elif fix["type"] == "smooth_boundary" and fix.get("region_slug"):
                print(f"  NOTE Smooth boundary noted for {fix['region_slug']} (applied in batch)")


def main():
    parser = argparse.ArgumentParser(description="Wine Expert Region Review")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--country", type=str, default=None)
    args = parser.parse_args()

    apply = args.apply
    sb = get_supabase()
    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n=== Wine Expert Region Review -- Phase 4 ({mode}) ===\n")

    data = load_data(sb)
    all_regions = data["all_regions"]
    country_by_id = data["country_by_id"]

    # Group regions by country
    countries_with_regions = {}
    for r in all_regions:
        countries_with_regions.setdefault(r["country_id"], []).append(r)

    country_ids = list(countries_with_regions.keys())
    if args.country:
        country_ids = [cid for cid in country_ids
                       if country_by_id[cid]["iso_code"] == args.country.upper()]
        if not country_ids:
            print(f"No regions found for country: {args.country}")
            return

    # Sort by region count descending
    country_ids.sort(key=lambda cid: len(countries_with_regions.get(cid, [])), reverse=True)

    print(f"Reviewing {len(all_regions)} regions across {len(country_ids)} countries\n")

    all_results = []
    total_corrections = 0
    auto_fix_count = 0
    review_count = 0

    for i, country_id in enumerate(country_ids):
        country = country_by_id[country_id]
        region_count = len(countries_with_regions.get(country_id, []))
        print(f"[{i + 1}/{len(country_ids)}] {country['name']} ({region_count} regions)...")

        review_data = build_country_review(country_id, data)
        user_message = json.dumps(review_data, indent=2)

        try:
            response = call_sonnet(SYSTEM_PROMPT, user_message)

            # Parse JSON from response
            json_str = response
            import re as _re
            json_match = _re.search(r"```(?:json)?\s*([\s\S]*?)```", response)
            if json_match:
                json_str = json_match.group(1)
            json_str = json_str.strip()

            result = json.loads(json_str)
            all_results.append(result)

            corrections = result.get("corrections") or []
            total_corrections += len(corrections)

            auto_fixes = [c for c in corrections if c.get("severity") == "auto_fix"]
            reviews = [c for c in corrections if c.get("severity") == "needs_review"]
            auto_fix_count += len(auto_fixes)
            review_count += len(reviews)

            if not corrections:
                print(f"  PASS {result.get('overall_assessment', '')}")
            else:
                label = "MAJOR" if result.get("overall_assessment") == "major_issues" else "WARN"
                print(f"  {label} {result.get('overall_assessment', '')}: {len(corrections)} items")
                for c in corrections:
                    icon = "FIX" if c.get("severity") == "auto_fix" else "REVIEW"
                    print(f"    [{icon}] [{c['type']}] {c['description']}")

            if result.get("notes"):
                print(f"  NOTE {result['notes']}")
        except Exception as e:
            print(f"  ERROR: {e}")
            all_results.append({
                "country": country["name"],
                "overall_assessment": "error",
                "corrections": [],
                "notes": str(e),
            })

    # Write full report
    report_path = PROJECT_ROOT / "data" / "region_review_report.json"
    report_path.write_text(json.dumps(all_results, indent=2), encoding="utf-8")

    print(f"\n{'=' * 60}")
    print("REVIEW COMPLETE")
    print(f"{'=' * 60}")
    print(f"Countries reviewed: {len(country_ids)}")
    print(f"Total corrections found: {total_corrections}")
    print(f"  Auto-fixable: {auto_fix_count}")
    print(f"  Needs review: {review_count}")
    print(f"\nFull report: data/region_review_report.json")

    if apply and auto_fix_count > 0:
        print(f"\nApplying {auto_fix_count} auto-fixes...")
        apply_corrections(all_results, data, sb)

    print("\nDone!")


if __name__ == "__main__":
    main()
