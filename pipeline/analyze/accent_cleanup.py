#!/usr/bin/env python3
"""
Deterministic accent restoration for producer and wine names.

Fixes systematically wrong ASCII names that should have diacritics:
- "Chateau" -> "Château", "Cuvee" -> "Cuvée", "Cotes" -> "Côtes", etc.
- Applies word-boundary-safe regex replacements
- Regenerates name_normalized and slug after fixes
- Detects slug collisions (potential duplicates)

No AI needed — these are unambiguous French/Spanish/Portuguese/German accent rules.

Usage:
    python -m pipeline.analyze.accent_cleanup                      # dry-run, full report
    python -m pipeline.analyze.accent_cleanup --execute             # apply fixes
    python -m pipeline.analyze.accent_cleanup --table producers     # producers only
    python -m pipeline.analyze.accent_cleanup --table wines         # wines only
"""

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify

# ── Accent Rules ──────────────────────────────────────────────────────────────
# Format: (pattern, replacement, flags, description)
# Patterns use word boundaries to avoid false positives.
# Applied in order — more specific patterns first.

ACCENT_RULES = [
    # === French (most common) ===
    # Château / Châteaux
    (r'\bChateau\b', 'Château', 0, 'French: Chateau -> Château'),
    (r'\bChateaux\b', 'Châteaux', 0, 'French: Chateaux -> Châteaux'),
    # Côte / Côtes
    (r'\bCotes\s+d', 'Côtes d', 0, 'French: Cotes d -> Côtes d'),
    (r'\bCote\s+d', 'Côte d', 0, 'French: Cote d -> Côte d'),
    (r'\bCotes\s+du\b', 'Côtes du', 0, 'French: Cotes du -> Côtes du'),
    (r'\bCote\s+du\b', 'Côte du', 0, 'French: Cote du -> Côte du'),
    (r'\bCote-Rotie\b', 'Côte-Rôtie', 0, 'French: Cote-Rotie -> Côte-Rôtie'),
    (r'\bCote Rotie\b', 'Côte Rôtie', 0, 'French: Cote Rotie -> Côte Rôtie'),
    # Cuvée
    (r'\bCuvee\b', 'Cuvée', 0, 'French: Cuvee -> Cuvée'),
    # Père / Frère
    (r'\bPere\b', 'Père', 0, 'French: Pere -> Père'),
    (r'\bFreres\b', 'Frères', 0, 'French: Freres -> Frères'),
    (r'\bFrere\b', 'Frère', 0, 'French: Frere -> Frère'),
    # Crémant
    (r'\bCremant\b', 'Crémant', 0, 'French: Cremant -> Crémant'),
    # Réserve / Réservé
    (r'\bReserve\b', 'Réserve', 0, 'French: Reserve -> Réserve'),
    # Première / Premier — skip: Premier is valid English too
    # Clos / Domaine — no accent needed, already correct
    # Élevé / Élevage
    (r'\bElevage\b', 'Élevage', 0, 'French: Elevage -> Élevage'),
    (r'\bEleve\b', 'Élevé', 0, 'French: Eleve -> Élevé'),
    # Pré
    (r'\bPre-\b', 'Pré-', 0, 'French: Pre- -> Pré-'),
    # Négociant
    (r'\bNegociant\b', 'Négociant', 0, 'French: Negociant -> Négociant'),
    # Sélection
    (r'\bSelection\b', 'Sélection', 0, 'French: Selection -> Sélection'),

    # === Spanish ===
    # Señorío
    (r'\bSenorio\b', 'Señorío', 0, 'Spanish: Senorio -> Señorío'),
    # Viña
    (r'\bVina\b', 'Viña', 0, 'Spanish: Vina -> Viña'),
    # Año
    (r'\bAno\b', 'Año', 0, 'Spanish: Ano -> Año'),
    # Crianza / Añejo — no accent needed

    # === Portuguese ===
    # São
    (r'\bSao\b', 'São', 0, 'Portuguese: Sao -> São'),

    # === German ===
    # Spätburgunder / Spätlese
    (r'\bSpatburgunder\b', 'Spätburgunder', 0, 'German: Spatburgunder -> Spätburgunder'),
    (r'\bSpatlese\b', 'Spätlese', 0, 'German: Spatlese -> Spätlese'),
    # Grüner Veltliner
    (r'\bGruner\b', 'Grüner', 0, 'German: Gruner -> Grüner'),
    # Müller-Thurgau
    (r'\bMuller-Thurgau\b', 'Müller-Thurgau', 0, 'German: Muller-Thurgau -> Müller-Thurgau'),
    (r'\bMuller Thurgau\b', 'Müller Thurgau', 0, 'German: Muller Thurgau -> Müller Thurgau'),
    # Würtemberg is handled by region references
]

# Rules that should NOT be applied (too ambiguous or wrong context):
# - "Reserve" in English context (e.g., "Reserve Chardonnay" is valid English)
# - "Premier" / "Premiere" — valid English words
# - "Rose" — could be the flower, the color, or rosé wine
# - "Selection" in English context


def should_skip_reserve(name):
    """Check if 'Reserve' is likely English (not French Réserve)."""
    # French context indicators: starts with Château, Domaine, Clos, etc.
    # Or has French words nearby
    french_markers = ['Château', 'Chateau', 'Domaine', 'Clos', 'Maison',
                      'Blanc', 'Rouge', 'Rosé', 'Brut', 'Cuvée', 'Cuvee',
                      'Côtes', 'Cotes', 'Saint-', 'Crémant', 'Cremant',
                      'Grande', 'Vieilles', 'Vignes']
    return not any(m in name for m in french_markers)


def should_skip_selection(name):
    """Check if 'Selection' is likely English."""
    french_markers = ['Château', 'Chateau', 'Domaine', 'Clos', 'Maison',
                      'Blanc', 'Rouge', 'Cuvée', 'Cuvee']
    return not any(m in name for m in french_markers)


def apply_rules(name):
    """Apply accent rules to a name. Returns (new_name, list_of_applied_rules)."""
    result = name
    applied = []
    for pattern, replacement, flags, desc in ACCENT_RULES:
        # Skip Reserve/Selection in English context
        if 'Reserve' in desc and should_skip_reserve(result):
            continue
        if 'Selection' in desc and should_skip_selection(result):
            continue

        new = re.sub(pattern, replacement, result, flags=flags)
        if new != result:
            applied.append(desc)
            result = new
    return result, applied


def main():
    parser = argparse.ArgumentParser(description="Deterministic accent restoration")
    parser.add_argument("--execute", action="store_true", help="Apply fixes (default: dry-run)")
    parser.add_argument("--table", choices=["producers", "wines", "both"], default="both",
                        help="Which table(s) to fix")
    parser.add_argument("--limit", type=int, help="Max rows to fix per table")
    args = parser.parse_args()

    print(f"Accent Cleanup — {'EXECUTE' if args.execute else 'DRY RUN'}")
    print(f"Table: {args.table}")
    print(f"Rules: {len(ACCENT_RULES)}")
    print()

    conn = get_conn()
    tables = []
    if args.table in ("producers", "both"):
        tables.append("producers")
    if args.table in ("wines", "both"):
        tables.append("wines")

    grand_total = 0

    for table in tables:
        print(f"\n{'=' * 65}")
        print(f"  TABLE: {table}")
        print(f"{'=' * 65}")

        # Fetch all names
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {table}")
            rows = cur.fetchall()
        print(f"  Total rows: {len(rows):,}")

        # Apply rules
        fixes = []  # (id, old_name, new_name, rules_applied)
        rule_counts = Counter()

        for row_id, name in rows:
            if not name:
                continue
            new_name, applied = apply_rules(name)
            if new_name != name:
                fixes.append((row_id, name, new_name, applied))
                for r in applied:
                    rule_counts[r] += 1

        print(f"  Fixes needed: {len(fixes):,}")
        print()

        # Show rule breakdown
        print(f"  Rule breakdown:")
        for rule, count in rule_counts.most_common():
            print(f"    {count:>6,}  {rule}")
        print()

        # Show samples
        print(f"  Samples (first 20):")
        for _, old, new, rules in fixes[:20]:
            print(f"    '{old}' -> '{new}'")
        if len(fixes) > 20:
            print(f"    ... and {len(fixes) - 20} more")
        print()

        # Check for slug collisions (batch query for speed)
        if fixes:
            new_slugs = defaultdict(list)
            for row_id, old, new, _ in fixes:
                new_slug = slugify(new)
                new_slugs[new_slug].append((row_id, old, new))

            # Batch check: find slugs that already exist in DB
            all_new_slugs = list(new_slugs.keys())
            collisions = []
            with conn.cursor() as cur:
                # Check in batches of 500
                for i in range(0, len(all_new_slugs), 500):
                    batch = all_new_slugs[i:i+500]
                    placeholders = ",".join(["%s"] * len(batch))
                    cur.execute(f"SELECT id, name, slug FROM {table} WHERE slug IN ({placeholders})", batch)
                    for eid, ename, eslug in cur.fetchall():
                        entries = new_slugs[eslug]
                        if eid not in {e[0] for e in entries}:
                            collisions.append((eslug, entries, (eid, ename)))

            if collisions:
                print(f"  !! SLUG COLLISIONS: {len(collisions)} (potential duplicates!)")
                for slug, entries, existing in collisions[:10]:
                    for _, old, new in entries:
                        print(f"    '{old}' -> '{new}' (slug '{slug}') COLLIDES with '{existing[1]}'")
                if len(collisions) > 10:
                    print(f"    ... and {len(collisions) - 10} more collisions")
                print()

        # Apply in batches to avoid deadlocks and long transactions
        if args.execute and fixes:
            to_apply = fixes[:args.limit] if args.limit else fixes
            print(f"  Applying {len(to_apply):,} fixes in batches of 500...")

            applied_count = 0
            slug_conflict_count = 0
            BATCH_SIZE = 500
            for batch_start in range(0, len(to_apply), BATCH_SIZE):
                batch = to_apply[batch_start:batch_start + BATCH_SIZE]
                with conn.cursor() as cur:
                    for row_id, old_name, new_name, _ in batch:
                        new_norm = normalize(new_name)
                        new_slug = slugify(new_name)

                        # Check slug conflict before updating
                        cur.execute(f"SELECT id FROM {table} WHERE slug = %s AND id != %s",
                                    (new_slug, row_id))
                        if cur.fetchone():
                            slug_conflict_count += 1
                            continue  # skip — would create duplicate slug

                        cur.execute(f"""
                            UPDATE {table}
                            SET name = %s, name_normalized = %s, slug = %s
                            WHERE id = %s
                        """, (new_name, new_norm, new_slug, row_id))
                        applied_count += 1
                conn.commit()
                if (batch_start // BATCH_SIZE) % 10 == 0:
                    print(f"    ... {applied_count:,} applied so far")

            print(f"  Applied: {applied_count:,} fixes")
            if slug_conflict_count:
                print(f"  Skipped: {slug_conflict_count:,} (slug conflicts — likely duplicates)")
            grand_total += applied_count
        else:
            grand_total += len(fixes)

    print(f"\n{'=' * 65}")
    if args.execute:
        print(f"TOTAL APPLIED: {grand_total:,}")
    else:
        print(f"TOTAL WOULD FIX: {grand_total:,}")
        print("Run with --execute to apply.")

    conn.close()


if __name__ == "__main__":
    main()
