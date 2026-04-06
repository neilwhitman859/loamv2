"""
Haiku-powered repair of remaining U+FFFD characters in wine names.

Uses Claude Haiku to recognize corrupted accented characters in wine names
that the dictionary-based approach in name_cleanup.py couldn't fix.

This is OCR-style correction, not inference — each corrupted character has
exactly one correct answer based on the wine/language term.

Usage:
    python -m pipeline.analyze.name_cleanup_haiku                # dry-run
    python -m pipeline.analyze.name_cleanup_haiku --execute       # apply
"""

import argparse
import json
import time

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.normalize import normalize, slugify

FFFD = "\ufffd"
BATCH_SIZE = 30  # names per API call


def fetch_fffd_names(conn, table="wines"):
    """Fetch all rows with remaining U+FFFD in the name."""
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, name FROM {table}
            WHERE name LIKE '%' || chr(65533) || '%'
            ORDER BY name
        """)
        return cur.fetchall()


def repair_batch_with_haiku(client, names: list[str]) -> dict[str, str]:
    """Ask Haiku to correct a batch of corrupted wine names.

    Returns dict mapping original name -> corrected name.
    """
    name_list = "\n".join(f"{i+1}. {name}" for i, name in enumerate(names))

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{
            "role": "user",
            "content": f"""These wine names contain corrupted characters shown as \ufffd (the Unicode replacement character). Each \ufffd replaces exactly one accented letter (like e\u0301, e\u0300, e\u0302, n\u0303, u\u0308, o\u0302, a\u0302, a\u0300, etc.) or a special character (like \u00b0, \u00a1, \u0153).

For each name, return the corrected version with the proper accented character restored. Use your knowledge of French, Spanish, German, Italian, and Portuguese wine terminology.

Rules:
- Replace each \ufffd with exactly ONE correct character
- If a \ufffd could be an apostrophe (between letters like d\ufffdA or l\ufffdE), use a straight apostrophe '
- If you truly cannot determine the correct character, return the name unchanged
- Return ONLY a JSON array of corrected names, in the same order, no explanations

Names:
{name_list}"""
        }],
    )

    # Parse the response
    text = response.content[0].text.strip()
    # Handle markdown code blocks
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    try:
        corrected = json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON array from response
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            corrected = json.loads(text[start:end])
        else:
            print(f"  WARNING: Could not parse Haiku response: {text[:200]}")
            return {}

    if len(corrected) != len(names):
        print(f"  WARNING: Expected {len(names)} results, got {len(corrected)}")
        # Try to use what we got
        corrected = corrected[:len(names)]

    result = {}
    for orig, fixed in zip(names, corrected):
        if isinstance(fixed, str) and fixed != orig and FFFD not in fixed:
            result[orig] = fixed

    return result


def main():
    parser = argparse.ArgumentParser(description="Haiku-powered U+FFFD name repair")
    parser.add_argument("--execute", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--table", choices=["wines", "producers"],
                        default="wines", help="Which table to clean")
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    try:
        rows = fetch_fffd_names(conn, args.table)
        print(f"\nFound {len(rows)} {args.table} with U+FFFD characters.")

        if not rows:
            print("Nothing to fix.")
            return

        # Batch and process
        all_fixes = {}  # name -> corrected_name
        id_map = {}     # name -> [list of row IDs]
        for row_id, name in rows:
            id_map.setdefault(name, []).append(row_id)

        unique_names = list(id_map.keys())
        print(f"  {len(unique_names)} unique names to process.")

        total_batches = (len(unique_names) + BATCH_SIZE - 1) // BATCH_SIZE
        input_tokens = 0
        output_tokens = 0

        for i in range(0, len(unique_names), BATCH_SIZE):
            batch = unique_names[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            print(f"  Batch {batch_num}/{total_batches} ({len(batch)} names)...", end=" ")

            try:
                fixes = repair_batch_with_haiku(client, batch)
                all_fixes.update(fixes)
                print(f"fixed {len(fixes)}/{len(batch)}")
            except Exception as e:
                print(f"ERROR: {e}")

            # Brief pause to avoid rate limits
            if i + BATCH_SIZE < len(unique_names):
                time.sleep(0.5)

        # Summary
        total_rows_affected = sum(len(id_map[name]) for name in all_fixes)
        print(f"\n{'=' * 60}")
        print(f"  HAIKU REPAIR SUMMARY {'(DRY RUN)' if not args.execute else ''}")
        print(f"{'=' * 60}")
        print(f"  Unique names fixed: {len(all_fixes)} / {len(unique_names)}")
        print(f"  Total rows affected: {total_rows_affected} / {len(rows)}")

        # Show sample fixes
        print(f"\n  Sample fixes (first 30):")
        for i, (orig, fixed) in enumerate(list(all_fixes.items())[:30]):
            try:
                print(f"    {orig}")
                print(f"      -> {fixed}")
            except UnicodeEncodeError:
                print(f"    (encoding error in display)")

        # Show unfixed
        unfixed = [n for n in unique_names if n not in all_fixes]
        if unfixed:
            print(f"\n  Still unfixed: {len(unfixed)}")
            for n in unfixed[:10]:
                try:
                    print(f"    {n}")
                except UnicodeEncodeError:
                    print(f"    (encoding error)")

        if args.execute and all_fixes:
            print(f"\n  Applying {total_rows_affected} changes...")
            updated = 0
            slug_conflicts = 0
            with conn.cursor() as cur:
                for orig_name, new_name in all_fixes.items():
                    new_norm = normalize(new_name)
                    new_slug = slugify(new_name)
                    for row_id in id_map[orig_name]:
                        cur.execute("SAVEPOINT row_sp")
                        try:
                            cur.execute(f"""
                                UPDATE {args.table}
                                SET name = %s, name_normalized = %s,
                                    slug = %s, updated_at = now()
                                WHERE id = %s
                            """, (new_name, new_norm, new_slug, str(row_id)))
                            cur.execute("RELEASE SAVEPOINT row_sp")
                            updated += 1
                        except Exception:
                            cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                            cur.execute(f"""
                                UPDATE {args.table}
                                SET name = %s, name_normalized = %s,
                                    updated_at = now()
                                WHERE id = %s
                            """, (new_name, new_norm, str(row_id)))
                            updated += 1
                            slug_conflicts += 1

                conn.commit()
            print(f"  Done. {updated} rows updated.")
            if slug_conflicts:
                print(f"  {slug_conflicts} slug conflicts (slug kept as-is).")
        elif not args.execute and all_fixes:
            print(f"\n  Re-run with --execute to apply {total_rows_affected} changes.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
