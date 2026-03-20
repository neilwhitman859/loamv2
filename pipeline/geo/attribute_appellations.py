"""
Appellation -> Region Attribution Script

Attributes appellations from catch-all regions to their proper named regions
using a three-pass strategy:
  Pass 1: Containment hierarchy trace -> bridge table lookup
  Pass 2: Name-pattern matching (regex)
  Pass 3: Direct slug-to-region lookup

Usage:
  python -m pipeline.geo.attribute_appellations --pass 1
  python -m pipeline.geo.attribute_appellations --pass 1 --apply
  python -m pipeline.geo.attribute_appellations --pass 2
  python -m pipeline.geo.attribute_appellations --pass 3
"""

import sys
import re
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import fetch_all_paginated

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_FILE = PROJECT_ROOT / "data" / "appellation_region_attributions.json"


def load_catch_all_appellations(sb):
    """Load all appellations currently on catch-all regions."""
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        result = (
            sb.table("appellations")
            .select("id, name, slug, country_id, region_id, regions!inner(is_catch_all, slug), countries!inner(slug, name)")
            .eq("regions.is_catch_all", True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_rows.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size

    return [
        {
            "id": a["id"],
            "name": a["name"],
            "slug": a["slug"],
            "country_id": a["country_id"],
            "country_slug": a["countries"]["slug"],
            "country_name": a["countries"]["name"],
            "region_id": a["region_id"],
        }
        for a in all_rows
    ]


def load_regions(sb):
    """Load all regions, return slug->info map for non-catch-all regions."""
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        result = (
            sb.table("regions")
            .select("id, slug, name, country_id, is_catch_all, countries!inner(slug)")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_rows.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size

    region_by_slug = {}
    for r in all_rows:
        if not r["is_catch_all"]:
            region_by_slug[r["slug"]] = {
                "id": r["id"],
                "name": r["name"],
                "country_slug": r["countries"]["slug"],
            }
    return region_by_slug


def trace_catch_all_ancestors(sb):
    """Trace containment hierarchy for all catch-all appellations via BFS."""
    all_containment = fetch_all_paginated(sb, "appellation_containment", "child_id, parent_id")
    print(f"  Loaded {len(all_containment)} containment rows")

    app_lookup = {}
    all_apps = fetch_all_paginated(sb, "appellations", "id, name, slug")
    for a in all_apps:
        app_lookup[a["id"]] = {"name": a["name"], "slug": a["slug"]}
    print(f"  Loaded {len(app_lookup)} appellation lookups")

    # Build parent map: child_id -> [parent_id, ...]
    parent_map = {}
    for row in all_containment:
        parent_map.setdefault(row["child_id"], []).append(row["parent_id"])

    # BFS for each appellation to find all ancestors
    result = {}
    for child_id, parent_ids in parent_map.items():
        ancestors = []
        visited = {child_id}
        frontier = [{"id": pid, "depth": 1} for pid in parent_ids]

        while frontier:
            next_frontier = []
            for item in frontier:
                anc_id = item["id"]
                depth = item["depth"]
                if anc_id in visited:
                    continue
                visited.add(anc_id)
                info = app_lookup.get(anc_id)
                if info:
                    ancestors.append({"slug": info["slug"], "name": info["name"], "depth": depth})
                for gp in parent_map.get(anc_id, []):
                    if gp not in visited:
                        next_frontier.append({"id": gp, "depth": depth + 1})
            frontier = next_frontier

        ancestors.sort(key=lambda a: a["depth"])
        result[child_id] = ancestors

    print(f"  Built ancestor chains for {len(result)} appellations\n")
    return result


def run_pass1(catch_all_apps, region_by_slug, data_config, sb):
    """Pass 1: Containment Trace -> Bridge Lookup."""
    print("Pass 1: Containment Trace -> Bridge Lookup\n")

    bridge_map = {}
    for entry in data_config["containment_bridges"]["entries"]:
        bridge_map[entry["appellation_slug"]] = entry["region_slug"]

    ancestors = trace_catch_all_ancestors(sb)

    attributions = []
    skipped = []

    for app in catch_all_apps:
        # Step 1: Check if this appellation's own slug matches a bridge entry
        if app["slug"] in bridge_map:
            region_slug = bridge_map[app["slug"]]
            if region_slug is None:
                skipped.append({
                    "name": app["name"], "slug": app["slug"],
                    "country": app["country_name"],
                    "reason": "Bridge entry explicitly null (stays catch-all)",
                })
                continue
            region = region_by_slug.get(region_slug)
            if region:
                attributions.append({
                    "appellation_id": app["id"],
                    "appellation_name": app["name"],
                    "appellation_slug": app["slug"],
                    "country": app["country_name"],
                    "region_slug": region_slug,
                    "region_name": region["name"],
                    "region_id": region["id"],
                    "method": "bridge-self",
                })
                continue

        # Step 2: Check ancestors (closest first)
        app_ancestors = ancestors.get(app["id"], [])
        matched = False
        for anc in app_ancestors:
            if anc["slug"] in bridge_map:
                region_slug = bridge_map[anc["slug"]]
                if region_slug is None:
                    skipped.append({
                        "name": app["name"], "slug": app["slug"],
                        "country": app["country_name"],
                        "reason": f"Ancestor '{anc['name']}' bridge is null (stays catch-all)",
                    })
                    matched = True
                    break
                region = region_by_slug.get(region_slug)
                if region:
                    attributions.append({
                        "appellation_id": app["id"],
                        "appellation_name": app["name"],
                        "appellation_slug": app["slug"],
                        "country": app["country_name"],
                        "region_slug": region_slug,
                        "region_name": region["name"],
                        "region_id": region["id"],
                        "method": f"bridge-ancestor({anc['name']})",
                    })
                    matched = True
                    break

        if not matched and app_ancestors:
            root_anc = app_ancestors[-1]
            skipped.append({
                "name": app["name"], "slug": app["slug"],
                "country": app["country_name"],
                "reason": f"Has containment (root: '{root_anc['name']}' / {root_anc['slug']}) but no bridge match",
            })

    return {"attributions": attributions, "skipped": skipped}


def run_pass2(catch_all_apps, region_by_slug, data_config, already_attributed_ids):
    """Pass 2: Name-Pattern Matching."""
    print("Pass 2: Name-Pattern Matching\n")

    patterns = []
    for e in data_config.get("name_patterns", {}).get("entries", []):
        patterns.append({
            "country": e["country"],
            "regex": re.compile(e["pattern"], re.IGNORECASE),
            "region_slug": e["region_slug"],
            "source": e.get("source", ""),
        })

    if not patterns:
        print("  No patterns defined yet.\n")
        return {"attributions": [], "skipped": []}

    remaining = [a for a in catch_all_apps if a["id"] not in already_attributed_ids]
    attributions = []

    for app in remaining:
        for pat in patterns:
            if pat["country"] != app["country_name"]:
                continue
            if pat["regex"].search(app["name"]):
                region = region_by_slug.get(pat["region_slug"])
                if region:
                    attributions.append({
                        "appellation_id": app["id"],
                        "appellation_name": app["name"],
                        "appellation_slug": app["slug"],
                        "country": app["country_name"],
                        "region_slug": pat["region_slug"],
                        "region_name": region["name"],
                        "region_id": region["id"],
                        "method": f"pattern({pat['regex'].pattern})",
                    })
                    break

    return {"attributions": attributions, "skipped": []}


def run_pass3(catch_all_apps, region_by_slug, data_config, already_attributed_ids):
    """Pass 3: Direct Slug-to-Region Lookup."""
    print("Pass 3: Direct Slug-to-Region Lookup\n")

    direct_map = {}
    for entry in data_config.get("direct_attributions", {}).get("entries", []):
        direct_map[entry["appellation_slug"]] = entry["region_slug"]

    if not direct_map:
        print("  No direct attributions defined yet.\n")
        return {"attributions": [], "skipped": []}

    remaining = [a for a in catch_all_apps if a["id"] not in already_attributed_ids]
    attributions = []

    for app in remaining:
        if app["slug"] in direct_map:
            region_slug = direct_map[app["slug"]]
            if region_slug is None:
                continue
            region = region_by_slug.get(region_slug)
            if region:
                attributions.append({
                    "appellation_id": app["id"],
                    "appellation_name": app["name"],
                    "appellation_slug": app["slug"],
                    "country": app["country_name"],
                    "region_slug": region_slug,
                    "region_name": region["name"],
                    "region_id": region["id"],
                    "method": "direct-lookup",
                })

    return {"attributions": attributions, "skipped": []}


def apply_attributions(attributions, sb):
    """Apply attributions to DB."""
    print(f"\nApplying {len(attributions)} attributions...")
    applied = 0
    for attr in attributions:
        try:
            sb.table("appellations").update({"region_id": attr["region_id"]}).eq("id", attr["appellation_id"]).execute()
            applied += 1
        except Exception as e:
            print(f"  ERROR updating {attr['appellation_name']}: {e}")
    print(f"  ok Applied {applied}/{len(attributions)} attributions")


def print_report(attributions, skipped, total_catch_all):
    """Print attribution summary report."""
    by_country = {}
    for attr in attributions:
        by_country.setdefault(attr["country"], []).append(attr)

    print("\n" + "-" * 60)
    print("ATTRIBUTION SUMMARY")
    print("-" * 60)

    for country, attrs in sorted(by_country.items(), key=lambda x: -len(x[1])):
        print(f"\n  {country}: {len(attrs)} attributions")
        by_region = {}
        for a in attrs:
            key = f"{a['region_name']} ({a['region_slug']})"
            by_region[key] = by_region.get(key, 0) + 1
        for region, count in sorted(by_region.items(), key=lambda x: -x[1]):
            print(f"    -> {region}: {count}")

    if skipped:
        print("\n" + "-" * 60)
        print(f"SKIPPED ({len(skipped)}):")
        print("-" * 60)
        by_reason = {}
        for s in skipped:
            by_reason.setdefault(s["reason"], []).append(s)
        for reason, items in by_reason.items():
            print(f"\n  {reason}")
            for item in items[:5]:
                print(f"    - {item['name']} ({item.get('country', '')})")
            if len(items) > 5:
                print(f"    ... and {len(items) - 5} more")

    unmatched_roots = [s for s in skipped if "no bridge match" in s.get("reason", "")]
    if unmatched_roots:
        print("\n" + "-" * 60)
        print("UNMATCHED CONTAINMENT ROOTS (need bridge entries):")
        print("-" * 60)
        root_set = set()
        for s in unmatched_roots:
            import re as _re
            match = _re.search(r"root: '(.+?)' / (.+?)\)", s["reason"])
            if match:
                root_set.add(f"{s.get('country', '')} | {match.group(1)} ({match.group(2)})")
        for r in sorted(root_set):
            print(f"  - {r}")

    untouched = total_catch_all - len(attributions) - len(skipped)
    print("\n" + "-" * 60)
    print(f"TOTALS: {len(attributions)} attributed, {len(skipped)} skipped, {untouched} untouched")
    print("-" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Appellation -> Region Attribution")
    parser.add_argument("--pass", type=int, required=True, dest="pass_num", choices=[1, 2, 3])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    pass_num = args.pass_num
    apply_mode = args.apply
    sb = get_supabase()

    print(f"\n{'=' * 60}")
    print(f"APPELLATION -> REGION ATTRIBUTION -- Pass {pass_num}")
    print(f"Mode: {'APPLY' if apply_mode else 'DRY-RUN'}")
    print(f"{'=' * 60}\n")

    data_config = json.loads(DATA_FILE.read_text(encoding="utf-8"))

    catch_all_apps = load_catch_all_appellations(sb)
    print(f"Loaded {len(catch_all_apps)} catch-all appellations\n")

    region_by_slug = load_regions(sb)
    print(f"Loaded {len(region_by_slug)} named regions\n")

    attributions = []
    skipped = []

    if pass_num == 1:
        result = run_pass1(catch_all_apps, region_by_slug, data_config, sb)
        attributions = result["attributions"]
        skipped = result["skipped"]
    elif pass_num == 2:
        result = run_pass2(catch_all_apps, region_by_slug, data_config, set())
        attributions = result["attributions"]
        skipped = result["skipped"]
    elif pass_num == 3:
        result = run_pass3(catch_all_apps, region_by_slug, data_config, set())
        attributions = result["attributions"]
        skipped = result["skipped"]

    print_report(attributions, skipped, len(catch_all_apps))

    if apply_mode and attributions:
        apply_attributions(attributions, sb)
    elif apply_mode and not attributions:
        print("Nothing to apply.")
    else:
        print("Dry-run complete. Use --apply to execute changes.")


if __name__ == "__main__":
    main()
