"""
Attribute appellations from catch-all regions to their proper named regions.

Three-pass strategy:
  Pass 1: Containment hierarchy trace -> bridge table lookup
  Pass 2: Name-pattern matching (regex)
  Pass 3: Direct slug-to-region lookup

Usage:
    python -m pipeline.reference.attribute_appellations_to_regions --pass 1
    python -m pipeline.reference.attribute_appellations_to_regions --pass 1 --apply
    python -m pipeline.reference.attribute_appellations_to_regions --pass 2
    python -m pipeline.reference.attribute_appellations_to_regions --pass 3
"""

import sys
import re
import json
import argparse
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all


# ── Load DB state ───────────────────────────────────────────────────

def load_catch_all_appellations(sb) -> list[dict]:
    """Load all appellations currently assigned to catch-all regions."""
    all_rows: list[dict] = []
    offset = 0
    page_size = 1000
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


def load_regions(sb) -> dict[str, dict]:
    """Load all regions and return slug->info dict for non-catch-all regions."""
    all_rows: list[dict] = []
    offset = 0
    page_size = 1000
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

    region_by_slug: dict[str, dict] = {}
    for r in all_rows:
        if not r["is_catch_all"]:
            region_by_slug[r["slug"]] = {
                "id": r["id"],
                "name": r["name"],
                "country_slug": r["countries"]["slug"],
            }
    return region_by_slug


# ── Trace containment hierarchy for all catch-all appellations ──────

def trace_catch_all_ancestors(sb) -> dict[str, list[dict]]:
    """Build BFS ancestor chains for all appellations with containment data."""
    # Get all containment rows
    all_containment: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        result = (
            sb.table("appellation_containment")
            .select("child_id, parent_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_containment.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size
    print(f"  Loaded {len(all_containment)} containment rows")

    # Get all appellation slugs/names for lookup
    app_lookup: dict[str, dict] = {}
    offset = 0
    while True:
        result = (
            sb.table("appellations")
            .select("id, name, slug")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        for a in result.data:
            app_lookup[a["id"]] = {"name": a["name"], "slug": a["slug"]}
        if len(result.data) < page_size:
            break
        offset += page_size
    print(f"  Loaded {len(app_lookup)} appellation lookups")

    # Build parent map: child_id -> [parent_id, ...]
    parent_map: dict[str, list[str]] = defaultdict(list)
    for row in all_containment:
        parent_map[row["child_id"]].append(row["parent_id"])

    # BFS: for each appellation, trace upward (closest first)
    result_map: dict[str, list[dict]] = {}
    for child_id, parent_ids in parent_map.items():
        ancestors: list[dict] = []
        visited: set[str] = {child_id}
        frontier = [{"id": pid, "depth": 1} for pid in parent_ids]

        while frontier:
            next_frontier: list[dict] = []
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
        result_map[child_id] = ancestors

    print(f"  Built ancestor chains for {len(result_map)} appellations\n")
    return result_map


# ── Pass 1: Containment Trace ───────────────────────────────────────

def run_pass1(
    sb, catch_all_apps: list[dict], region_by_slug: dict, data: dict
) -> tuple[list[dict], list[dict]]:
    """Pass 1: Containment hierarchy trace -> bridge table lookup."""
    print("Pass 1: Containment Trace -> Bridge Lookup\n")

    bridge_map: dict[str, str | None] = {}
    for entry in data["containment_bridges"]["entries"]:
        bridge_map[entry["appellation_slug"]] = entry["region_slug"]

    ancestors = trace_catch_all_ancestors(sb)

    attributions: list[dict] = []
    skipped: list[dict] = []

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

    return attributions, skipped


# ── Pass 2: Name Patterns ───────────────────────────────────────────

def run_pass2(
    catch_all_apps: list[dict], region_by_slug: dict,
    data: dict, already_attributed_ids: set[str],
) -> tuple[list[dict], list[dict]]:
    """Pass 2: Name-pattern matching."""
    print("Pass 2: Name-Pattern Matching\n")

    patterns = []
    for e in data.get("name_patterns", {}).get("entries", []):
        patterns.append({
            "country": e["country"],
            "regex": re.compile(e["pattern"], re.IGNORECASE),
            "region_slug": e["region_slug"],
            "source": e.get("source", ""),
        })

    if not patterns:
        print("  No patterns defined yet. Add entries to name_patterns in the data file.\n")
        return [], []

    remaining = [a for a in catch_all_apps if a["id"] not in already_attributed_ids]
    attributions: list[dict] = []

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

    return attributions, []


# ── Pass 3: Direct Lookup ───────────────────────────────────────────

def run_pass3(
    catch_all_apps: list[dict], region_by_slug: dict,
    data: dict, already_attributed_ids: set[str],
) -> tuple[list[dict], list[dict]]:
    """Pass 3: Direct slug-to-region lookup."""
    print("Pass 3: Direct Slug-to-Region Lookup\n")

    direct_map: dict[str, str | None] = {}
    for entry in data.get("direct_attributions", {}).get("entries", []):
        direct_map[entry["appellation_slug"]] = entry["region_slug"]

    if not direct_map:
        print("  No direct attributions defined yet. Add entries to direct_attributions in the data file.\n")
        return [], []

    remaining = [a for a in catch_all_apps if a["id"] not in already_attributed_ids]
    attributions: list[dict] = []

    for app in remaining:
        if app["slug"] in direct_map:
            region_slug = direct_map[app["slug"]]
            if region_slug is None:
                continue  # Explicitly stays catch-all
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

    return attributions, []


# ── Apply attributions ──────────────────────────────────────────────

def apply_attributions(sb, attributions: list[dict]) -> None:
    """Apply region_id updates to appellations."""
    print(f"\nApplying {len(attributions)} attributions...")
    applied = 0
    for attr in attributions:
        try:
            sb.table("appellations").update(
                {"region_id": attr["region_id"]}
            ).eq("id", attr["appellation_id"]).execute()
            applied += 1
        except Exception as e:
            print(f"  ERROR updating {attr['appellation_name']}: {e}")
        if applied % 100 == 0 and applied > 0:
            print(f"  {applied}/{len(attributions)}...")
    print(f"  Applied {applied}/{len(attributions)} attributions")


# ── Report ──────────────────────────────────────────────────────────

def print_report(
    attributions: list[dict], skipped: list[dict], total_catch_all: int
) -> None:
    """Print attribution summary grouped by country and region."""
    by_country: dict[str, list[dict]] = defaultdict(list)
    for attr in attributions:
        by_country[attr["country"]].append(attr)

    print("\n" + "-" * 60)
    print("ATTRIBUTION SUMMARY")
    print("-" * 60)

    for country, attrs in sorted(by_country.items(), key=lambda x: -len(x[1])):
        print(f"\n  {country}: {len(attrs)} attributions")
        by_region: dict[str, int] = defaultdict(int)
        for a in attrs:
            key = f"{a['region_name']} ({a['region_slug']})"
            by_region[key] += 1
        for region, count in sorted(by_region.items(), key=lambda x: -x[1]):
            print(f"    -> {region}: {count}")

    if skipped:
        print("\n" + "-" * 60)
        print(f"SKIPPED ({len(skipped)}):")
        print("-" * 60)
        by_reason: dict[str, list[dict]] = defaultdict(list)
        for s in skipped:
            by_reason[s["reason"]].append(s)
        for reason, items in by_reason.items():
            print(f"\n  {reason}")
            for item in items[:5]:
                print(f"    - {item['name']} ({item['country']})")
            if len(items) > 5:
                print(f"    ... and {len(items) - 5} more")

    # Unmatched roots
    unmatched_roots = [s for s in skipped if "no bridge match" in s.get("reason", "")]
    if unmatched_roots:
        print("\n" + "-" * 60)
        print("UNMATCHED CONTAINMENT ROOTS (need bridge entries):")
        print("-" * 60)
        root_set: set[str] = set()
        for s in unmatched_roots:
            m = re.search(r"root: '(.+?)' / (.+?)\)", s["reason"])
            if m:
                root_set.add(f"{s['country']} | {m.group(1)} ({m.group(2)})")
        for r in sorted(root_set):
            print(f"  - {r}")

    print("\n" + "-" * 60)
    untouched = total_catch_all - len(attributions) - len(skipped)
    print(f"TOTALS: {len(attributions)} attributed, {len(skipped)} skipped, {untouched} untouched")
    print("-" * 60 + "\n")


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Attribute appellations to regions")
    parser.add_argument("--pass", dest="pass_num", type=int, required=True,
                        choices=[1, 2, 3], help="Pass number (1, 2, or 3)")
    parser.add_argument("--apply", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--file", default="data/appellation_region_attributions.json",
                        help="Path to attributions JSON data file")
    args = parser.parse_args()

    pass_num = args.pass_num
    apply_mode = args.apply

    print(f"\n{'=' * 60}")
    print(f"APPELLATION -> REGION ATTRIBUTION -- Pass {pass_num}")
    print(f"Mode: {'APPLY' if apply_mode else 'DRY-RUN'}")
    print(f"{'=' * 60}\n")

    # Load data file
    filepath = Path(args.file)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath
    if not filepath.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)
    data = json.loads(filepath.read_text(encoding="utf-8"))

    sb = get_supabase()

    catch_all_apps = load_catch_all_appellations(sb)
    print(f"Loaded {len(catch_all_apps)} catch-all appellations\n")

    region_by_slug = load_regions(sb)
    print(f"Loaded {len(region_by_slug)} named regions\n")

    attributions: list[dict] = []
    skipped: list[dict] = []

    if pass_num == 1:
        attributions, skipped = run_pass1(sb, catch_all_apps, region_by_slug, data)
    elif pass_num == 2:
        attributions, skipped = run_pass2(catch_all_apps, region_by_slug, data, set())
    elif pass_num == 3:
        attributions, skipped = run_pass3(catch_all_apps, region_by_slug, data, set())

    print_report(attributions, skipped, len(catch_all_apps))

    if apply_mode and attributions:
        apply_attributions(sb, attributions)
    elif apply_mode:
        print("Nothing to apply.")
    else:
        print("Dry-run complete. Use --apply to execute changes.")


if __name__ == "__main__":
    main()
