"""
Extract drinking windows from Ridge vintage notes JSONL file.

Regex-based extraction of drinking window dates from tasting notes.
Six patterns: 'over the next X years', 'develop over X years',
'enjoy now and over', 'drink now and for', 'best from YYYY', 'YYYY-YYYY' range.

Usage:
    python -m pipeline.enrich.extract_drinking_windows
    python -m pipeline.enrich.extract_drinking_windows --input data/ridge_notes.jsonl
    python -m pipeline.enrich.extract_drinking_windows --dry-run
"""

import sys
import re
import json
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

# ── Number words for text-to-number conversion ──────────────────────
NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
    "nineteen": 19, "twenty": 20, "twenty-five": 25, "thirty": 30,
}


def parse_number(text: str) -> int | None:
    """Parse a number from text (digit or word)."""
    text = text.strip().lower()
    if text.isdigit():
        return int(text)
    return NUMBER_WORDS.get(text)


def extract_drinking_window(note: str, vintage_year: int | None) -> dict | None:
    """
    Extract a drinking window from a tasting note.

    Returns dict with start_year and end_year, or None if not found.
    """
    if not note:
        return None

    note_lower = note.lower()
    current_year = datetime.now().year

    # Pattern 1: "over the next X years" / "over the next X-Y years"
    m = re.search(r"over the next (\w+(?:-\w+)?)\s*(?:to\s*(\w+))?\s*years?", note_lower)
    if m:
        n1 = parse_number(m.group(1))
        n2 = parse_number(m.group(2)) if m.group(2) else None
        if n1:
            base_year = vintage_year or current_year
            return {
                "start_year": current_year,
                "end_year": base_year + (n2 or n1),
            }

    # Pattern 2: "develop over X years" / "develop over the next X years"
    m = re.search(r"develop\s+(?:over\s+)?(?:the\s+next\s+)?(\w+(?:-\w+)?)\s*(?:to\s*(\w+))?\s*years?", note_lower)
    if m:
        n1 = parse_number(m.group(1))
        n2 = parse_number(m.group(2)) if m.group(2) else None
        if n1:
            base_year = vintage_year or current_year
            return {
                "start_year": base_year + max(1, (n1 // 3)),
                "end_year": base_year + (n2 or n1),
            }

    # Pattern 3: "enjoy now and over the next X years"
    m = re.search(r"enjoy\s+now\s+and\s+(?:over\s+)?(?:the\s+next\s+)?(\w+(?:-\w+)?)\s*(?:to\s*(\w+))?\s*years?", note_lower)
    if m:
        n1 = parse_number(m.group(1))
        n2 = parse_number(m.group(2)) if m.group(2) else None
        if n1:
            return {
                "start_year": current_year,
                "end_year": current_year + (n2 or n1),
            }

    # Pattern 4: "drink now and for X more years"
    m = re.search(r"drink\s+now\s+and\s+for\s+(\w+(?:-\w+)?)\s*(?:to\s*(\w+))?\s*(?:more\s+)?years?", note_lower)
    if m:
        n1 = parse_number(m.group(1))
        n2 = parse_number(m.group(2)) if m.group(2) else None
        if n1:
            return {
                "start_year": current_year,
                "end_year": current_year + (n2 or n1),
            }

    # Pattern 5: "best from YYYY" / "best from YYYY to YYYY"
    m = re.search(r"best\s+(?:from|starting)\s+((?:19|20)\d{2})\s*(?:(?:to|through|-)\s*((?:19|20)\d{2}))?", note_lower)
    if m:
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else start + 10
        return {"start_year": start, "end_year": end}

    # Pattern 6: "YYYY-YYYY" drinking window range (at end of note or standalone)
    m = re.search(r"\b((?:19|20)\d{2})\s*[-–]\s*((?:19|20)\d{2})\b", note)
    if m:
        start = int(m.group(1))
        end = int(m.group(2))
        if start < end and start >= 1980 and end <= 2080:
            return {"start_year": start, "end_year": end}

    return None


def main():
    parser = argparse.ArgumentParser(description="Extract drinking windows from tasting notes")
    parser.add_argument("--input", default="data/ridge_vintage_notes.jsonl",
                        help="Path to JSONL file with vintage notes")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()

    filepath = Path(args.input)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath

    if not filepath.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    # Load notes
    notes = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                notes.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    print(f"Loaded {len(notes)} notes\n")

    extracted = 0
    not_found = 0
    updates = []

    for entry in notes:
        note_text = entry.get("note") or entry.get("tasting_note") or entry.get("text") or ""
        vintage_year = entry.get("vintage_year")
        wine_id = entry.get("wine_id")
        wine_vintage_id = entry.get("wine_vintage_id")

        result = extract_drinking_window(note_text, vintage_year)

        if result:
            extracted += 1
            updates.append({
                "wine_id": wine_id,
                "wine_vintage_id": wine_vintage_id,
                "vintage_year": vintage_year,
                "start_year": result["start_year"],
                "end_year": result["end_year"],
                "note_preview": note_text[:80],
            })
            if extracted <= 20:
                print(f"  {vintage_year or 'NV'}: {result['start_year']}-{result['end_year']}  ({note_text[:60]}...)")
        else:
            not_found += 1

    print(f"\nExtracted: {extracted}")
    print(f"No window found: {not_found}")

    if args.dry_run or not updates:
        print("\n[DRY RUN] No DB writes")
        return

    # Write to DB
    sb = get_supabase()
    updated = 0

    for u in updates:
        if not u.get("wine_vintage_id") and not (u.get("wine_id") and u.get("vintage_year")):
            continue
        try:
            data_update = {
                "critic_drinking_window_start": u["start_year"],
                "critic_drinking_window_end": u["end_year"],
            }
            if u.get("wine_vintage_id"):
                sb.table("wine_vintage_scores").update(data_update).eq(
                    "wine_vintage_id", u["wine_vintage_id"]
                ).execute()
            else:
                sb.table("wine_vintage_scores").update(data_update).eq(
                    "wine_id", u["wine_id"]
                ).eq("vintage_year", u["vintage_year"]).execute()
            updated += 1
        except Exception as e:
            print(f"  Error updating: {e}")

    print(f"\nDB updated: {updated} scores with drinking windows")


if __name__ == "__main__":
    main()
