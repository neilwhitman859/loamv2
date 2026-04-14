#!/usr/bin/env python3
"""B5.2: Build test data for the 3-tier AI model bake-off.

Outputs:
  bakeoff/data/task1/pairs.json       — 200 labeled dedup pairs
  bakeoff/data/task3/contexts.json    — 30 wine context packages

Task 2 (web scraping) is handled separately in build_task2.py.

Usage:
    python -m bakeoff.build_test_data [--task1] [--task3] [--all]
"""

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

os.environ["PYTHONUNBUFFERED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.lib.db import get_conn

# Demo producer IDs — exclude from Task 3 to avoid contamination
DEMO_PRODUCER_IDS = [
    "531a3920-ccab-4f14-801f-df8f1ece51f0",  # Stag's Leap Wine Cellars
    "9dba92bb-432a-4645-b359-97c604d81e4c",  # Fort Ross
    "a645cffc-65ec-41dc-a603-7fa55106694c",  # López de Heredia
    "be0b2418-e978-4c73-85f0-6d49c9940935",  # López de Heredia (merged)
    "8316beee-c06a-4149-bdfa-ca0fc70eb143",  # CIRQ
    "5af1f66d-ca37-4c7a-83e7-8ea741e069f8",  # Ridge Vineyards
    "ceebb2c1-3121-4067-bbfa-71e9e2e02b5c",  # Domaine Tempier
    "1271aa01-0f8e-4abb-b8fb-6ae2da03a1b6",  # E. Guigal
    "9691ac1f-68c7-47b8-9ae1-d84db9a7d28d",  # Trimbach
    "1801910f-375e-4e6b-962f-49f558897bf9",  # Domaine Huet
    "9507f989-a73c-4caa-b6a4-4f596ac6a0f2",  # DRC
    "95560c9b-e4c1-423b-8524-0e79f8f6bb54",  # Krug
    "9859dc71-c199-45c0-a1e3-0a73b95ff3f0",  # Giacomo Conterno
    "2c863ed3-98cb-4474-8954-6259aa2793f8",  # Château Margaux
    "1ceb6035-6a13-4698-880f-156e72726ab6",  # Château Latour
]


def build_task1(cur):
    """Build 200 labeled dedup pairs across 4 strata."""
    print("\n=== TASK 1: Building dedup pairs ===\n")
    pairs = []

    # ── Stratum A: Clear SAME — varietal suffix pattern (50 pairs) ──
    print("Stratum A: Clear SAME (varietal suffix)...")
    cur.execute("""
        WITH grape_names AS (
            SELECT DISTINCT lower(name) as gname FROM grapes
            WHERE name IS NOT NULL AND length(name) > 2
        ),
        pairs AS (
            SELECT
                w1.id as id_a, w1.name as name_a, w1.display_name as display_a,
                w2.id as id_b, w2.name as name_b, w2.display_name as display_b,
                p.name as producer_name, p.id as producer_id,
                w1.color as color_a, w2.color as color_b,
                similarity(w1.name, w2.name) as sim
            FROM wines w1
            JOIN wines w2 ON w1.producer_id = w2.producer_id
                AND w1.id < w2.id
                AND w1.deleted_at IS NULL AND w2.deleted_at IS NULL
            JOIN producers p ON w1.producer_id = p.id AND p.deleted_at IS NULL
            WHERE length(w1.name) < length(w2.name)
              AND similarity(w1.name, w2.name) > 0.5
              AND (w1.color IS NULL OR w2.color IS NULL OR w1.color = w2.color)
              -- Longer name starts with shorter name
              AND w2.name ILIKE w1.name || ' %'
              -- Suffix is a grape name
              AND EXISTS (
                  SELECT 1 FROM grape_names
                  WHERE lower(
                      trim(substring(w2.name FROM length(w1.name) + 2))
                  ) = gname
              )
              -- Exclude German lot numbers
              AND w2.name !~ E'Nr[0-9]+'
              AND w1.name !~ E'Nr[0-9]+'
        )
        SELECT DISTINCT ON (id_a)
            id_a, name_a, display_a, id_b, name_b, display_b,
            producer_name, producer_id, color_a, color_b, sim
        FROM pairs
        ORDER BY id_a, sim DESC
        LIMIT 55
    """)
    stratum_a_raw = cur.fetchall()
    cols = [d[0] for d in cur.description]

    # Also get pairs where the name IS a grape but formatted differently
    cur.execute("""
        WITH pairs AS (
            SELECT
                w1.id as id_a, w1.name as name_a, w1.display_name as display_a,
                w2.id as id_b, w2.name as name_b, w2.display_name as display_b,
                p.name as producer_name, p.id as producer_id,
                w1.color as color_a, w2.color as color_b,
                similarity(w1.name, w2.name) as sim
            FROM wines w1
            JOIN wines w2 ON w1.producer_id = w2.producer_id
                AND w1.id < w2.id
                AND w1.deleted_at IS NULL AND w2.deleted_at IS NULL
            JOIN producers p ON w1.producer_id = p.id AND p.deleted_at IS NULL
            WHERE similarity(w1.name, w2.name) > 0.7
              AND (w1.color IS NULL OR w2.color IS NULL OR w1.color = w2.color)
              -- Name containment either direction
              AND (w2.name ILIKE '%' || w1.name || '%'
                   OR w1.name ILIKE '%' || w2.name || '%')
              AND length(w1.name) != length(w2.name)
              AND abs(length(w1.name) - length(w2.name)) BETWEEN 3 AND 25
              -- Same grapes
              AND EXISTS (
                  SELECT 1 FROM wine_grapes wg1
                  JOIN wine_grapes wg2 ON wg1.grape_id = wg2.grape_id
                  WHERE wg1.wine_id = w1.id AND wg2.wine_id = w2.id
              )
              -- Exclude German lot numbers
              AND w2.name !~ E'Nr[0-9]+'
              AND w1.name !~ E'Nr[0-9]+'
        )
        SELECT DISTINCT ON (id_a)
            id_a, name_a, display_a, id_b, name_b, display_b,
            producer_name, producer_id, color_a, color_b, sim
        FROM pairs
        ORDER BY id_a, sim DESC
        LIMIT 55
    """)
    stratum_a_raw2 = cur.fetchall()

    # Merge and deduplicate
    seen_ids = set()
    stratum_a = []
    for source in [stratum_a_raw, stratum_a_raw2]:
        for row in source:
            d = dict(zip(cols, row))
            pair_key = tuple(sorted([str(d["id_a"]), str(d["id_b"])]))
            if pair_key not in seen_ids and len(stratum_a) < 50:
                seen_ids.add(pair_key)
                stratum_a.append({
                    "pair_id": f"A{len(stratum_a)+1:03d}",
                    "stratum": "A",
                    "ground_truth": "SAME",
                    "wine_a": {"id": str(d["id_a"]), "name": d["name_a"],
                               "display_name": d["display_a"], "color": d["color_a"]},
                    "wine_b": {"id": str(d["id_b"]), "name": d["name_b"],
                               "display_name": d["display_b"], "color": d["color_b"]},
                    "producer": {"id": str(d["producer_id"]), "name": d["producer_name"]},
                    "similarity": float(d["sim"]) if d["sim"] else None,
                    "rationale": "Varietal suffix pattern — same wine, one name appends grape variety"
                })
    print(f"  Stratum A: {len(stratum_a)} pairs")

    # ── Stratum B: Clear DIFFERENT — same producer, different grapes/colors (50 pairs) ──
    print("Stratum B: Clear DIFFERENT (different grapes)...")
    cur.execute("""
        WITH wine_grapes_agg AS (
            SELECT wg.wine_id,
                   string_agg(DISTINCT g.name, ', ' ORDER BY g.name) as grape_names
            FROM wine_grapes wg
            JOIN grapes g ON wg.grape_id = g.id
            GROUP BY wg.wine_id
        ),
        pairs AS (
            SELECT
                w1.id as id_a, w1.name as name_a, w1.display_name as display_a,
                w2.id as id_b, w2.name as name_b, w2.display_name as display_b,
                p.name as producer_name, p.id as producer_id,
                w1.color as color_a, w2.color as color_b,
                g1.grape_names as grapes_a, g2.grape_names as grapes_b,
                similarity(w1.name, w2.name) as sim
            FROM wines w1
            JOIN wines w2 ON w1.producer_id = w2.producer_id
                AND w1.id < w2.id
                AND w1.deleted_at IS NULL AND w2.deleted_at IS NULL
            JOIN producers p ON w1.producer_id = p.id AND p.deleted_at IS NULL
            LEFT JOIN wine_grapes_agg g1 ON g1.wine_id = w1.id
            LEFT JOIN wine_grapes_agg g2 ON g2.wine_id = w2.id
            WHERE similarity(w1.name, w2.name) > 0.6
              AND (
                  (w1.color IS NOT NULL AND w2.color IS NOT NULL AND w1.color != w2.color)
                  OR (g1.grape_names IS NOT NULL AND g2.grape_names IS NOT NULL
                      AND g1.grape_names != g2.grape_names)
              )
              AND w1.name !~ E'Nr[0-9]+' AND w2.name !~ E'Nr[0-9]+'
        )
        SELECT DISTINCT ON (producer_id, name_a)
            id_a, name_a, display_a, id_b, name_b, display_b,
            producer_name, producer_id, color_a, color_b,
            grapes_a, grapes_b, sim
        FROM pairs
        ORDER BY producer_id, name_a, sim DESC
        LIMIT 200
    """)
    stratum_b_raw = cur.fetchall()
    b_cols = [d[0] for d in cur.description]

    # Diversify: max 2 from same producer
    producer_count = {}
    stratum_b = []
    for row in stratum_b_raw:
        d = dict(zip(b_cols, row))
        pid = str(d["producer_id"])
        if producer_count.get(pid, 0) >= 2:
            continue
        producer_count[pid] = producer_count.get(pid, 0) + 1
        pair_key = tuple(sorted([str(d["id_a"]), str(d["id_b"])]))
        if pair_key not in seen_ids and len(stratum_b) < 50:
            seen_ids.add(pair_key)
            reason = []
            if d["color_a"] and d["color_b"] and d["color_a"] != d["color_b"]:
                reason.append(f"Different color ({d['color_a']} vs {d['color_b']})")
            if d["grapes_a"] and d["grapes_b"] and d["grapes_a"] != d["grapes_b"]:
                reason.append(f"Different grapes")
            stratum_b.append({
                "pair_id": f"B{len(stratum_b)+1:03d}",
                "stratum": "B",
                "ground_truth": "DIFFERENT",
                "wine_a": {"id": str(d["id_a"]), "name": d["name_a"],
                           "display_name": d["display_a"], "color": d["color_a"],
                           "grapes": d["grapes_a"]},
                "wine_b": {"id": str(d["id_b"]), "name": d["name_b"],
                           "display_name": d["display_b"], "color": d["color_b"],
                           "grapes": d["grapes_b"]},
                "producer": {"id": str(d["producer_id"]), "name": d["producer_name"]},
                "similarity": float(d["sim"]) if d["sim"] else None,
                "rationale": "; ".join(reason) if reason else "Different wines"
            })
    print(f"  Stratum B: {len(stratum_b)} pairs")

    # ── Stratum C: Classification/tier differences (50 pairs) ──
    print("Stratum C: Classification differences...")
    cur.execute("""
        WITH pairs AS (
            SELECT
                w1.id as id_a, w1.name as name_a, w1.display_name as display_a,
                w2.id as id_b, w2.name as name_b, w2.display_name as display_b,
                p.name as producer_name, p.id as producer_id,
                w1.color as color_a, w2.color as color_b,
                a1.name as app_a, a2.name as app_b,
                similarity(w1.name, w2.name) as sim
            FROM wines w1
            JOIN wines w2 ON w1.producer_id = w2.producer_id
                AND w1.id < w2.id
                AND w1.deleted_at IS NULL AND w2.deleted_at IS NULL
            JOIN producers p ON w1.producer_id = p.id AND p.deleted_at IS NULL
            LEFT JOIN appellations a1 ON w1.appellation_id = a1.id
            LEFT JOIN appellations a2 ON w2.appellation_id = a2.id
            WHERE similarity(w1.name, w2.name) > 0.7
              AND w1.name != w2.name
              AND (
                  -- Reserva vs Gran Reserva / Crianza
                  (w1.name ~* '(reserva|gran reserva|crianza|riserva)'
                   AND w2.name ~* '(reserva|gran reserva|crianza|riserva)')
                  -- German Prädikat differences
                  OR (w1.name ~* '(kabinett|spatlese|auslese|beerenauslese|trockenbeerenauslese|eiswein)'
                      AND w2.name ~* '(kabinett|spatlese|auslese|beerenauslese|trockenbeerenauslese|eiswein)'
                      AND w1.name NOT ILIKE '%' || w2.name || '%'
                      AND w2.name NOT ILIKE '%' || w1.name || '%')
                  -- Champagne styles
                  OR (w1.name ~* '(brut|extra brut|brut nature|blanc de blancs|blanc de noirs|demi-sec)'
                      AND w2.name ~* '(brut|extra brut|brut nature|blanc de blancs|blanc de noirs|demi-sec)'
                      AND w1.name != w2.name)
                  -- Grand Cru vs Premier Cru
                  OR (w1.name ~* 'grand cru' AND w2.name ~* 'premier cru')
                  OR (w1.name ~* 'premier cru' AND w2.name ~* 'grand cru')
                  -- Echezeaux vs Grands-Echezeaux
                  OR (w1.name ~* 'echezeaux' AND w2.name ~* 'grands.echezeaux')
                  -- Moelleux vs Sec (Vouvray style)
                  OR (w1.name ~* '(sec|moelleux|demi.sec)'
                      AND w2.name ~* '(sec|moelleux|demi.sec)'
                      AND w1.name != w2.name)
                  -- German star ratings (1* vs 2* vs 3*)
                  OR (w1.name ~ E'[0-9][*]' AND w2.name ~ E'[0-9][*]'
                      AND w1.name != w2.name)
              )
        )
        SELECT DISTINCT ON (producer_id, substring(name_a from 1 for 20))
            id_a, name_a, display_a, id_b, name_b, display_b,
            producer_name, producer_id, color_a, color_b,
            app_a, app_b, sim
        FROM pairs
        ORDER BY producer_id, substring(name_a from 1 for 20), sim DESC
        LIMIT 200
    """)
    stratum_c_raw = cur.fetchall()
    c_cols = [d[0] for d in cur.description]

    producer_count = {}
    stratum_c = []
    for row in stratum_c_raw:
        d = dict(zip(c_cols, row))
        pid = str(d["producer_id"])
        if producer_count.get(pid, 0) >= 3:
            continue
        producer_count[pid] = producer_count.get(pid, 0) + 1
        pair_key = tuple(sorted([str(d["id_a"]), str(d["id_b"])]))
        if pair_key not in seen_ids and len(stratum_c) < 50:
            seen_ids.add(pair_key)
            # Determine the classification difference
            na, nb = d["name_a"].lower(), d["name_b"].lower()
            reason = "Classification/tier difference"
            if "echezeaux" in na and "grands" in nb:
                reason = "Echezeaux vs Grands-Echezeaux — different Grand Cru vineyards"
            elif "gran reserva" in na or "gran reserva" in nb:
                reason = "Different Rioja classification tier"
            elif "riserva" in na or "riserva" in nb:
                reason = "Riserva vs non-Riserva — different quality tier"
            elif any(p in na for p in ["auslese", "spatlese", "kabinett"]):
                if any(p in nb for p in ["auslese", "spatlese", "kabinett"]):
                    reason = "Different German Prädikat levels — distinct bottlings"
            elif "brut" in na and "brut" in nb:
                reason = "Different sparkling wine dosage levels"
            elif "moelleux" in na or "moelleux" in nb or "sec" in na:
                reason = "Different sweetness levels — distinct bottlings"
            elif "*" in d["name_a"] or "*" in d["name_b"]:
                reason = "Different star ratings — distinct quality tiers"

            stratum_c.append({
                "pair_id": f"C{len(stratum_c)+1:03d}",
                "stratum": "C",
                "ground_truth": "DIFFERENT",
                "wine_a": {"id": str(d["id_a"]), "name": d["name_a"],
                           "display_name": d["display_a"], "color": d["color_a"],
                           "appellation": d["app_a"]},
                "wine_b": {"id": str(d["id_b"]), "name": d["name_b"],
                           "display_name": d["display_b"], "color": d["color_b"],
                           "appellation": d["app_b"]},
                "producer": {"id": str(d["producer_id"]), "name": d["producer_name"]},
                "similarity": float(d["sim"]) if d["sim"] else None,
                "rationale": reason
            })
    print(f"  Stratum C: {len(stratum_c)} pairs")

    # ── Stratum D: Ambiguous / hard cases (50 pairs) ──
    print("Stratum D: Ambiguous / hard cases...")
    cur.execute("""
        WITH pairs AS (
            SELECT
                w1.id as id_a, w1.name as name_a, w1.display_name as display_a,
                w2.id as id_b, w2.name as name_b, w2.display_name as display_b,
                p.name as producer_name, p.id as producer_id,
                w1.color as color_a, w2.color as color_b,
                e1.external_id as lwin_a, e2.external_id as lwin_b,
                similarity(w1.name, w2.name) as sim
            FROM wines w1
            JOIN wines w2 ON w1.producer_id = w2.producer_id
                AND w1.id < w2.id
                AND w1.deleted_at IS NULL AND w2.deleted_at IS NULL
            JOIN producers p ON w1.producer_id = p.id AND p.deleted_at IS NULL
            LEFT JOIN external_ids e1 ON e1.entity_id = w1.id AND e1.entity_type = 'wine' AND e1.system = 'lwin'
            LEFT JOIN external_ids e2 ON e2.entity_id = w2.id AND e2.entity_type = 'wine' AND e2.system = 'lwin'
            WHERE similarity(w1.name, w2.name) BETWEEN 0.6 AND 0.95
              AND w1.name != w2.name
              AND (w1.color IS NULL OR w2.color IS NULL OR w1.color = w2.color)
              -- Exclude simple Nr patterns
              AND NOT (w1.name ~ E'Nr[0-9]+' AND w2.name ~ E'Nr[0-9]+')
              -- Include interesting ambiguous patterns
              AND (
                  -- Spelling variants: very similar names, close in length
                  (similarity(w1.name, w2.name) > 0.8
                   AND abs(length(w1.name) - length(w2.name)) BETWEEN 0 AND 3)
                  -- Name containment with short suffix
                  OR (length(w1.name) != length(w2.name)
                      AND abs(length(w1.name) - length(w2.name)) BETWEEN 2 AND 8)
              )
        )
        SELECT DISTINCT ON (producer_id, left(name_a, 15))
            id_a, name_a, display_a, id_b, name_b, display_b,
            producer_name, producer_id, color_a, color_b,
            lwin_a, lwin_b, sim
        FROM pairs
        ORDER BY producer_id, left(name_a, 15), sim DESC
        LIMIT 200
    """)
    stratum_d_raw = cur.fetchall()
    d_cols = [d[0] for d in cur.description]

    producer_count = {}
    stratum_d = []
    for row in stratum_d_raw:
        d = dict(zip(d_cols, row))
        pid = str(d["producer_id"])
        if producer_count.get(pid, 0) >= 2:
            continue
        producer_count[pid] = producer_count.get(pid, 0) + 1
        pair_key = tuple(sorted([str(d["id_a"]), str(d["id_b"])]))
        if pair_key not in seen_ids and len(stratum_d) < 50:
            seen_ids.add(pair_key)
            # Determine ground truth using LWIN evidence
            lwin_a, lwin_b = d.get("lwin_a"), d.get("lwin_b")
            if lwin_a and lwin_b:
                gt = "SAME" if lwin_a == lwin_b else "DIFFERENT"
                rationale = f"LWIN evidence: {'same LWIN' if gt == 'SAME' else 'different LWINs'}"
            else:
                # Heuristic: spelling variant = likely SAME, word addition = likely DIFFERENT
                na, nb = d["name_a"], d["name_b"]
                edit_dist = sum(1 for a, b in zip(na, nb) if a != b) + abs(len(na) - len(nb))
                if edit_dist <= 2 and abs(len(na) - len(nb)) <= 1:
                    gt = "SAME"
                    rationale = "Spelling variant (edit distance ≤ 2, same length)"
                elif abs(len(na) - len(nb)) > 5:
                    gt = "DIFFERENT"
                    rationale = "Significant name difference (additional word/qualifier)"
                else:
                    gt = "AMBIGUOUS"
                    rationale = "Requires domain knowledge — manual review needed"

            stratum_d.append({
                "pair_id": f"D{len(stratum_d)+1:03d}",
                "stratum": "D",
                "ground_truth": gt,
                "wine_a": {"id": str(d["id_a"]), "name": d["name_a"],
                           "display_name": d["display_a"], "color": d["color_a"],
                           "lwin": d.get("lwin_a")},
                "wine_b": {"id": str(d["id_b"]), "name": d["name_b"],
                           "display_name": d["display_b"], "color": d["color_b"],
                           "lwin": d.get("lwin_b")},
                "producer": {"id": str(d["producer_id"]), "name": d["producer_name"]},
                "similarity": float(d["sim"]) if d["sim"] else None,
                "rationale": rationale
            })
    print(f"  Stratum D: {len(stratum_d)} pairs")

    # ── Enrich all pairs with metadata ──
    print("\nEnriching pairs with grape/LWIN metadata...")
    all_pairs = stratum_a + stratum_b + stratum_c + stratum_d
    all_wine_ids = []
    for p in all_pairs:
        all_wine_ids.extend([p["wine_a"]["id"], p["wine_b"]["id"]])

    # Batch fetch grapes for all wines
    if all_wine_ids:
        placeholders = ",".join(["%s"] * len(all_wine_ids))
        cur.execute(f"""
            SELECT wg.wine_id, g.name, wg.percentage
            FROM wine_grapes wg
            JOIN grapes g ON wg.grape_id = g.id
            WHERE wg.wine_id IN ({placeholders})
            ORDER BY wg.wine_id, wg.percentage DESC NULLS LAST
        """, all_wine_ids)
        grape_map = {}
        for wid, gname, pct in cur.fetchall():
            wid = str(wid)
            grape_map.setdefault(wid, []).append(
                {"name": gname, "percentage": float(pct) if pct else None}
            )

        # Batch fetch LWINs
        cur.execute(f"""
            SELECT entity_id, external_id, system
            FROM external_ids
            WHERE entity_id IN ({placeholders})
              AND entity_type = 'wine'
              AND system = 'lwin'
        """, all_wine_ids)
        lwin_map = {}
        for wid, eid, sys in cur.fetchall():
            wid = str(wid)
            lwin_map.setdefault(wid, []).append(eid)

        # Attach metadata
        for p in all_pairs:
            for side in ["wine_a", "wine_b"]:
                wid = p[side]["id"]
                p[side]["grapes"] = grape_map.get(wid, [])
                p[side]["lwins"] = lwin_map.get(wid, [])

    # Build metadata_block for each pair (what the model will see)
    for p in all_pairs:
        wa, wb = p["wine_a"], p["wine_b"]
        lines = [f"Producer: {p['producer']['name']}"]
        if wa.get("color"):
            lines.append(f"Wine A color: {wa['color']}")
        if wb.get("color"):
            lines.append(f"Wine B color: {wb['color']}")
        if wa.get("grapes"):
            g_str = ", ".join(f"{g['name']} ({g['percentage']}%)" if g['percentage']
                              else g['name'] for g in wa['grapes'])
            lines.append(f"Wine A grapes: {g_str}")
        if wb.get("grapes"):
            g_str = ", ".join(f"{g['name']} ({g['percentage']}%)" if g['percentage']
                              else g['name'] for g in wb['grapes'])
            lines.append(f"Wine B grapes: {g_str}")
        if wa.get("lwins"):
            lines.append(f"Wine A LWIN: {', '.join(wa['lwins'])}")
        if wb.get("lwins"):
            lines.append(f"Wine B LWIN: {', '.join(wb['lwins'])}")
        if wa.get("appellation"):
            lines.append(f"Wine A appellation: {wa['appellation']}")
        if wb.get("appellation"):
            lines.append(f"Wine B appellation: {wb['appellation']}")
        p["metadata_block"] = "\n".join(lines)

    # Summary
    gt_counts = {}
    for p in all_pairs:
        gt = p["ground_truth"]
        gt_counts[gt] = gt_counts.get(gt, 0) + 1

    total = len(all_pairs)
    print(f"\n  Total pairs: {total}")
    print(f"  Ground truth: {gt_counts}")
    print(f"  Strata: A={len(stratum_a)}, B={len(stratum_b)}, C={len(stratum_c)}, D={len(stratum_d)}")

    return all_pairs


def build_task3(cur):
    """Build 30 wine context packages stratified across 3 difficulty tiers.

    Uses curated wine selections from DESIGN.md — verified to exist in DB
    and not already enriched (no wine_insights).
    """
    print("\n=== TASK 3: Building wine context packages ===\n")

    # Curated wine IDs from DESIGN.md candidates, verified in DB
    # All have no wine_insights (uncontaminated)

    # Tier A: Famous wines with rich data
    tier_a_ids = [
        "1231e371-af5f-4bde-9d2d-be094605cc21",  # Shafer One Point Five (10 scores, 21 vints)
        "3f0c9f34-d466-4f33-8c57-5d2c740bf5e8",  # Oyster Bay Chardonnay (18 scores, 10 vints)
        "ba7c6cc5-ed69-424d-90ee-9b83ae510c36",  # Penfolds Bin 2 (2 scores, 15 vints, 2 grapes)
        "6020edf8-3f23-4853-9591-a10c843f5a70",  # Catena Zapata High Mountain Vines (4 scores, 12 vints, 3 grapes)
        "dffc0909-1d13-4b9f-9de7-98fd212c6b5f",  # Cakebread Cuttings Wharf (6 scores, 8 vints)
        "14823b73-f705-4b4d-8a11-e0079c33ab99",  # Tyrrell's Vat 1 (4 scores, 7 vints)
        "4647b290-7c71-4b76-9913-8e334d110814",  # Paul Hobbs Cuvee Agustina Chard (10 scores, 6 vints)
        "7e9b8b66-560f-45df-b566-5419d21b395c",  # d'Arenberg The High Trellis (2 scores, 11 vints)
        "7ab3daf6-3b5f-41c7-9507-d57dee47e905",  # Vasse Felix Filius Cabernet (3 scores, 4 vints, 4 grapes)
        "0aec2002-afb8-4d0f-a7ba-cc6fb3d8452f",  # Fritz Haag Brauneberger Juffer Sonnenuhr (4 scores, 11 vints)
    ]

    # Tier B: Mid-obscurity with moderate data
    tier_b_ids = [
        "46a46e99-c081-41ce-abcc-bf993469fc0e",  # Kir Yianni Ramnista, Naoussa (10 scores)
        "e998b872-3ce8-419e-ba0c-9904a4638629",  # Terroir Al Limit Les Tosses (6 scores)
        "c815d359-5c41-4ee0-ab03-2afaf1ab5f81",  # Comando G El Reventon (6 scores, 2 grapes)
        "1a19854d-d7fb-4046-a2ba-9c2391b6026a",  # Terre del Barolo Arnaldo Rivera (9 scores)
        "a46855f7-2916-434c-91c3-3847c981f0d0",  # Leo Hillinger Blaufrankisch (5 scores)
        "29c755be-e57c-4d38-8f28-3ec6d54d5268",  # des Bosquets Les Routes, Gigondas (4 scores)
        "1bc9b5ef-3d2d-4f0d-924b-fbfcd741e8d3",  # Montes Alpha M (2 scores, 10 vints)
        "21aa403d-a5ad-43f2-99d9-560dc9e66587",  # Lafage Tessellae Vieilles Vignes (2 scores, 4 grapes)
        "5f36a577-de6e-459c-b69c-ebce3fd3ca94",  # Howard Park Leston Cab Sauv (3 scores, 14 vints, 2 grapes)
        "1c2f0e8d-4a39-42f4-b008-c5d02bd4025d",  # Atamisque Chardonnay (7 scores, 5 vints)
    ]

    # Tier C: Deep cuts with thin data
    tier_c_ids = [
        "49a78f8d-0332-4925-b8b1-c1cb40dcb33b",  # Wind Gap Woodruff Pinot Noir (2 scores, 2 vints)
        "10c89e05-ecdd-4254-9c24-5a0b67894da5",  # Black Box Deep & Dark (4 scores — slightly richer)
        "4cd8b76a-b4fc-43cf-83ae-8a0c84e7673e",  # Barefoot Buttery Chardonnay (3 scores)
        "b8dc12b0-1820-4edb-8108-0701aee0860a",  # Tolosa 1772 Grenache (12 scores — richer than planned)
        "9934442d-7786-489b-aca3-e329b1c12da5",  # Plantagenet Omrah Pinot Noir (2 scores)
        "272c4c47-7ca7-4053-b022-86524690242b",  # Lafage Tessellae Old Vines (3 scores)
        "dc9e4353-ddf9-4e48-854e-1a3d6404b7b3",  # Shafer One Point Five Cab (diff record, 5 scores)
        "ebde8e00-350b-40fc-a762-0987259419ac",  # Howard Park Leston Shiraz (5 scores)
        "0aba350e-fffb-49bd-ad8b-9d2ec1f32fe9",  # Vasse Felix Filius Cab Sauv (4 scores)
        "4504d94f-9c51-4f71-a052-4c6bc770ed24",  # Cakebread Cuttings Wharf Chard (3 scores)
    ]

    print(f"  Tier A: {len(tier_a_ids)} curated famous wines")
    print(f"  Tier B: {len(tier_b_ids)} curated mid-obscurity wines")
    print(f"  Tier C: {len(tier_c_ids)} curated thin-data wines")

    # ── Build full context packages using batch_enrich pattern ──
    all_ids = tier_a_ids + tier_b_ids + tier_c_ids
    print(f"\nBuilding context packages for {len(all_ids)} wines...")

    # Import the batch_enrich context builder
    from pipeline.enrich.batch_enrich import bulk_preload_context

    contexts = bulk_preload_context(cur, all_ids)

    # Assemble final output
    wine_packages = []
    tier_labels = {**{wid: "A" for wid in tier_a_ids},
                   **{wid: "B" for wid in tier_b_ids},
                   **{wid: "C" for wid in tier_c_ids}}

    for wid in all_ids:
        ctx = contexts.get(wid)
        if not ctx:
            print(f"  WARNING: No context for {wid}")
            continue

        # Clean up non-serializable fields
        clean_ctx = {}
        for k, v in ctx.items():
            if v is None:
                clean_ctx[k] = None
            elif isinstance(v, (str, int, float, bool, list, dict)):
                clean_ctx[k] = v
            else:
                clean_ctx[k] = str(v)

        wine_packages.append({
            "wine_id": wid,
            "tier": tier_labels[wid],
            "display_name": ctx.get("display_name") or ctx.get("name"),
            "producer_name": ctx.get("producer_name"),
            "country": ctx.get("country_name"),
            "region": ctx.get("region_name"),
            "appellation": ctx.get("appellation_name"),
            "context": clean_ctx
        })

    # Summary
    tier_counts = {}
    for wp in wine_packages:
        t = wp["tier"]
        tier_counts[t] = tier_counts.get(t, 0) + 1

    print(f"\n  Total wines: {len(wine_packages)}")
    print(f"  Tiers: {tier_counts}")
    for wp in wine_packages:
        ctx = wp["context"]
        scores = len(ctx.get("scores", []))
        grapes = len(ctx.get("grapes", []))
        vints = len(ctx.get("vintages", []))
        print(f"    [{wp['tier']}] {wp['display_name']} — "
              f"grapes:{grapes} scores:{scores} vintages:{vints}")

    return wine_packages


def main():
    parser = argparse.ArgumentParser(description="Build bake-off test data")
    parser.add_argument("--task1", action="store_true", help="Build Task 1 dedup pairs")
    parser.add_argument("--task3", action="store_true", help="Build Task 3 wine contexts")
    parser.add_argument("--all", action="store_true", help="Build all tasks")
    args = parser.parse_args()

    if not (args.task1 or args.task3 or args.all):
        args.all = True

    conn = get_conn()
    cur = conn.cursor()

    out_dir = Path(__file__).parent / "data"

    if args.task1 or args.all:
        pairs = build_task1(cur)
        out_file = out_dir / "task1" / "pairs.json"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(pairs, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n  Saved {len(pairs)} pairs to {out_file}")

    if args.task3 or args.all:
        packages = build_task3(cur)
        out_file = out_dir / "task3" / "contexts.json"
        out_file.parent.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(packages, f, indent=2, ensure_ascii=False, default=str)
        print(f"\n  Saved {len(packages)} contexts to {out_file}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
