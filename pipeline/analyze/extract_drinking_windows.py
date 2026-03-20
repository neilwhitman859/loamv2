#!/usr/bin/env python3
"""
Extracts drinking window data from Ridge vintage notes in JSONL,
then updates wine_vintages with producer_drinking_window_start/end.

Usage:
    python -m pipeline.analyze.extract_drinking_windows          # Dry run
    python -m pipeline.analyze.extract_drinking_windows --apply  # Update DB
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

JSONL_FILE = Path(__file__).resolve().parents[2] / "ridge_wines.jsonl"

NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11,
    "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19,
    "twenty": 20, "twenty-one": 21, "twenty-two": 22, "twenty-three": 23,
    "twenty-four": 24, "twenty-five": 25, "thirty": 30, "thirty-five": 35,
    "forty": 40, "fifty": 50,
}


def parse_word(w: str) -> int | None:
    n = NUMBER_WORDS.get(w.lower())
    if n:
        return n
    try:
        return int(w)
    except ValueError:
        return None


def extract_drinking_window(notes: str, vintage: int) -> dict | None:
    if not notes or not vintage:
        return None

    patterns = [
        re.compile(r"(?:over|for)\s+the\s+next\s+([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?", re.I),
        re.compile(r"(?:develop|improve|evolve|age)\s+(?:over|for)\s+(?:the\s+next\s+)?([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?", re.I),
        re.compile(r"enjoy\s+(?:now\s+and\s+)?(?:over|for)\s+(?:the\s+next\s+)?([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?", re.I),
        re.compile(r"drink\s+now\s+and\s+(?:over|for)\s+(?:the\s+next\s+)?([\w-]+)\s+(?:to\s+([\w-]+)\s+)?years?", re.I),
    ]
    for pat in patterns:
        m = pat.search(notes)
        if m:
            hi = parse_word(m.group(2)) if m.group(2) else parse_word(m.group(1))
            if hi:
                return {"start": vintage, "end": vintage + hi, "source": m.group(0)}

    # "best from YYYY"
    m = re.search(r"best\s+(?:from|after)\s+(\d{4})", notes, re.I)
    if m:
        return {"start": int(m.group(1)), "end": int(m.group(1)) + 15, "source": m.group(0)}

    # "YYYY-YYYY" range
    m = re.search(r"(\d{4})\s*[-\u2013]\s*(\d{4})", notes)
    if m:
        s, e = int(m.group(1)), int(m.group(2))
        if 1990 <= s <= 2050 and e >= s and e <= 2100:
            return {"start": s, "end": e, "source": m.group(0)}

    return None


def main():
    parser = argparse.ArgumentParser(description="Extract drinking windows from Ridge JSONL")
    parser.add_argument("--apply", action="store_true", help="Actually update DB")
    args = parser.parse_args()

    sb = get_supabase()
    print(f"Mode: {'APPLY (will update DB)' if args.apply else 'DRY RUN (preview only)'}\n")

    lines = JSONL_FILE.read_text(encoding="utf-8").strip().split("\n")
    wines = [json.loads(l) for l in lines]
    print(f"Loaded {len(wines)} entries from JSONL\n")

    result = sb.table("producers").select("id").eq("slug", "ridge-vineyards").execute()
    if not result.data:
        print("Ridge producer not found")
        sys.exit(1)

    db_wines = sb.table("wines").select("id,name").eq("producer_id", result.data[0]["id"]).execute()
    wine_name_to_id = {w["name"]: w["id"] for w in (db_wines.data or [])}

    extracted = 0
    updated = 0
    skipped = 0

    for w in wines:
        if not w.get("vintage") or not w.get("wineName"):
            continue
        notes = f"{w.get('vintageNotes', '')} {w.get('winemakerNotes', '')}"
        dw = extract_drinking_window(notes, w["vintage"])
        if not dw:
            continue
        extracted += 1

        wine_id = wine_name_to_id.get(w["wineName"])
        if not wine_id:
            print(f"  No DB wine for \"{w['wineName']}\"")
            skipped += 1
            continue

        print(f"  {w['vintage']} {w['wineName']}: {dw['start']}-{dw['end']} (from: \"{dw['source']}\")")

        if args.apply:
            result = sb.table("wine_vintages").update({
                "producer_drinking_window_start": dw["start"],
                "producer_drinking_window_end": dw["end"],
            }).eq("wine_id", wine_id).eq("vintage_year", w["vintage"]).execute()
            if result.data is not None:
                updated += 1
            else:
                print("    UPDATE ERROR")

    print(f"\n{'=' * 40}")
    print("  Drinking Window Extraction")
    print("=" * 40)
    print(f"  Scanned: {len(wines)} entries")
    print(f"  Extracted: {extracted}")
    print(f"  Skipped: {skipped}")
    if args.apply:
        print(f"  Updated in DB: {updated}")
    else:
        print("  (Dry run -- use --apply to update DB)")


if __name__ == "__main__":
    main()
