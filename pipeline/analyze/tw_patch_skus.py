#!/usr/bin/env python3
"""
Patches pages 35-43 in totalwine_lexington_green.jsonl with
SKU, size, and URL data from the browser localStorage export.

Usage:
    python -m pipeline.analyze.tw_patch_skus
"""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

JSONL_FILE = Path(__file__).resolve().parents[2] / "totalwine_lexington_green.jsonl"
PATCH_FILE = Path("C:/Users/neilw/Downloads/tw_full_data.txt")


def parse_categories_from_url(url: str) -> list[str]:
    categories = []
    if not url:
        return categories
    cleaned = re.sub(r"\?.*$", "", url).replace("/wine/", "", 1)
    parts = cleaned.strip("/").split("/")
    skip = {"deals", "gift-center", "p"}
    for i, part in enumerate(parts):
        if part in skip or re.match(r"^\d+$", part):
            continue
        if i == len(parts) - 1 or (i == len(parts) - 3 and parts[-2] == "p"):
            continue
        readable = " ".join(w.capitalize() for w in part.split("-"))
        categories.append(readable)
    return categories


def main():
    # Build lookup from patch file
    patch_map: dict[str, dict] = {}
    if PATCH_FILE.exists():
        for line in PATCH_FILE.read_text(encoding="utf-8").strip().split("\n"):
            parts = line.split("|")
            if len(parts) >= 4 and parts[0]:
                patch_map[parts[0].strip()] = {
                    "sku": parts[1] or "", "size": parts[2] or "", "url": parts[3] or "",
                }
    else:
        print(f"Patch file not found: {PATCH_FILE}")
        sys.exit(1)

    print(f"Loaded {len(patch_map)} patch entries")

    lines = JSONL_FILE.read_text(encoding="utf-8").strip().split("\n")
    patched = 0
    sku_added = 0
    size_added = 0
    url_added = 0

    updated = []
    for line in lines:
        obj = json.loads(line)
        if obj.get("page", 0) < 35:
            updated.append(line)
            continue

        patch = patch_map.get(obj.get("name"))
        if not patch:
            updated.append(line)
            continue

        patched += 1
        if patch["sku"] and not obj.get("sku"):
            obj["sku"] = patch["sku"]
            sku_added += 1
        elif patch["sku"] and obj.get("sku") != patch["sku"]:
            obj["sku"] = patch["sku"]
        if patch["size"] and not obj.get("size"):
            obj["size"] = patch["size"]
            size_added += 1
        if patch["url"] and not obj.get("url"):
            obj["url"] = patch["url"]
            url_added += 1
        if patch["url"] and not obj.get("categories"):
            obj["categories"] = parse_categories_from_url(patch["url"])

        updated.append(json.dumps(obj))

    JSONL_FILE.write_text("\n".join(updated) + "\n", encoding="utf-8")
    print(f"Patched {patched} entries:")
    print(f"  SKUs added/updated: {sku_added}")
    print(f"  Sizes added: {size_added}")
    print(f"  URLs added: {url_added}")


if __name__ == "__main__":
    main()
