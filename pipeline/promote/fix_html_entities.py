#!/usr/bin/env python3
"""
Fix HTML entities in vinification_notes and other text fields.
KL data contained &ldquo; &rdquo; &ocirc; etc.

Usage:
    python -m pipeline.promote.fix_html_entities
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, fetch_all

ENTITIES = {
    "&ldquo;": "\u201C",
    "&rdquo;": "\u201D",
    "&lsquo;": "\u2018",
    "&rsquo;": "\u2019",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&nbsp;": " ",
    "&eacute;": "\u00e9",
    "&egrave;": "\u00e8",
    "&ocirc;": "\u00f4",
    "&agrave;": "\u00e0",
    "&uuml;": "\u00fc",
    "&ouml;": "\u00f6",
    "&aacute;": "\u00e1",
    "&iacute;": "\u00ed",
    "&ntilde;": "\u00f1",
    "&ccedil;": "\u00e7",
    "&mdash;": "\u2014",
    "&ndash;": "\u2013",
    "&hellip;": "\u2026",
    "&deg;": "\u00b0",
    "<br>": " ",
    "<br/>": " ",
    "<br />": " ",
}


def decode_entities(text: str | None) -> str | None:
    if not text:
        return text
    result = text
    for entity, replacement in ENTITIES.items():
        result = result.replace(entity, replacement)
    # Numeric entities like &#8217;
    result = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), result)
    # Clean double spaces
    result = re.sub(r"\s{2,}", " ", result).strip()
    return result


def main():
    sb = get_supabase()
    print("Fixing HTML entities...\n")

    # Fix wines.vinification_notes
    wines = fetch_all("wines", "id,name,vinification_notes")
    wine_fixed = 0
    for w in wines:
        if not w.get("vinification_notes"):
            continue
        fixed = decode_entities(w["vinification_notes"])
        if fixed == w["vinification_notes"]:
            continue

        result = sb.table("wines").update({"vinification_notes": fixed}).eq("id", w["id"]).execute()
        if result.data is not None:
            wine_fixed += 1
        else:
            print(f"  Warning: {w['name']}: update may have failed")

    print(f"Wines vinification_notes fixed: {wine_fixed}")

    # Fix wines.name (some KL wine names have HTML entities)
    name_fixed = 0
    for w in wines:
        fixed = decode_entities(w["name"])
        if fixed == w["name"]:
            continue

        result = sb.table("wines").update({"name": fixed}).eq("id", w["id"]).execute()
        if result.data is not None:
            print(f"  Fixed name: \"{w['name']}\" -> \"{fixed}\"")
            name_fixed += 1
        else:
            print(f"  Warning: name fix {w['name']}: update may have failed")

    print(f"Wine names fixed: {name_fixed}")


if __name__ == "__main__":
    main()
