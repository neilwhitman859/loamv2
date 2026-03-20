#!/usr/bin/env python3
"""
Parse region data from Claude tool output JSON.

Usage:
    python -m pipeline.analyze.parse_regions <json-file>
"""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m pipeline.analyze.parse_regions <json-file>")
        sys.exit(1)

    raw = Path(sys.argv[1]).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    text = parsed[0]["text"]
    match = re.search(r"\[\{.*\}\]", text, re.DOTALL)
    if not match:
        print("No JSON array found in text")
        sys.exit(1)

    inner = json.loads(match.group(0))

    by_country: dict[str, list] = {}
    for r in inner:
        k = r.get("country", "Unknown")
        by_country.setdefault(k, []).append(r)

    for country in sorted(by_country):
        rows = by_country[country]
        catch_all = next((r for r in rows if r.get("is_catch_all")), None)
        real = [r for r in rows if not r.get("is_catch_all")]
        total_apps = sum(r.get("app_count", 0) for r in rows)

        print(f"\n## {country} ({len(real)} regions, {total_apps} appellations)")
        if catch_all and catch_all.get("app_count", 0) > 0:
            print(f"  WARNING: {catch_all['app_count']} appellations on catch-all")

        for r in sorted(real, key=lambda x: x.get("parent_region") or ""):
            indent = "    " if r.get("parent_region") else "  "
            parent = f" (under {r['parent_region']})" if r.get("parent_region") else ""
            print(f"{indent}{r.get('region', '?')}{parent} -- {r.get('app_count', 0)} apps, {r.get('child_count', 0)} children")


if __name__ == "__main__":
    main()
