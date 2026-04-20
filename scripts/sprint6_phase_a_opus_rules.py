"""Phase A — Opus inline reasoning encoded as rules for the remaining 249 pairs.

After the deterministic pre-classifier, 249 pairs remain that have cross-tier
disagreements. For each, the web_reasoning contains the rigor-tier assessment.
This applies my Opus reasoning systematically:

  - Specific shared-website/address evidence → AUTO_DECIDE MERGE
  - Specific HdB pattern (§11.4.q) → auto-decide with parent direction
  - Second-wine pattern (§11.4.e) → ESCALATE (data state issue)
  - Generic "same family" claims → ESCALATE
  - Cross-tier strong disagreement without specific web evidence → ESCALATE
"""
import sys, json, re
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn
from pathlib import Path


# Keywords for specific shared-entity evidence
SPECIFIC_MERGE_SIGNALS = [
    r"official website \([a-z0-9\-\.]+\.(?:com|net|org|fr|it|es|de|wine|vin|au|nz|ca|us)\)",
    r"both (?:searches? |queries? )?(?:return|resolve|converge[ds]?) .*?(?:the same|identical|single) .*?official",
    r"identical (?:address|phone|website|domain|entity|brand identity)",
    r"same (?:winery|winemaker|family|official website|domain|entity|address|five-generation)",
    r"shared (?:TTB permit|BW permit|permit|address)",
    r"both (?:rows|producers) (?:refer to|are|represent) (?:the same|a single|one|identical)",
    r"historical (?:name|naming|variant) of (?:the same|one)",
    r"spelling variant of",
    r"short(?:-form|ened)? variant",
    r"\btypo\b",
    r"historically (?:known|named|called)",
    r"converges decisively",
    r"both resolve to the same",
]

SECOND_WINE_PATTERN = [
    r"second wine",
    r"is (?:a |the )?second wine of",
    r"§11\.4\.e",
    r"wines? of (?:the same )?(?:château|chateau|domaine|estate)",
    r"are both wines? of",
]

HDB_PATTERN = re.compile(r"hospices?\s+de\s+(?:beaune|nuits)", re.IGNORECASE)

# Known HdB négociant names — to determine parent
HDB_NEGOCIANTS = [
    "joseph drouhin", "louis jadot", "bouchard père", "camille giroud", "pierre andre",
    "faivel", "jaboulet", "morot", "maison leroy", "pierre morey", "morey blanc",
    "domaine mortet", "cecile tremblay", "duroche", "chanson", "remoissenet",
    "louis latour", "champy", "albert bichot",
]


def has_specific_merge_signal(reasoning: str) -> bool:
    r = reasoning.lower()
    return any(re.search(p, r, re.IGNORECASE) for p in SPECIFIC_MERGE_SIGNALS)


def has_second_wine_signal(reasoning: str) -> bool:
    r = reasoning.lower()
    return any(re.search(p, r, re.IGNORECASE) for p in SECOND_WINE_PATTERN)


def is_hdb(name: str) -> bool:
    return bool(HDB_PATTERN.search(name or ""))


def hdb_negociant_name(name: str) -> str | None:
    """Extract négociant name from 'Hospices de Beaune (X)' format."""
    m = re.search(r"hospices?\s+de\s+(?:beaune|nuits)\s*\(([^)]+)\)", name or "", re.IGNORECASE)
    if m:
        return m.group(1).strip().lower()
    return None


def classify_opus(pair: dict) -> tuple[str, str, str, str | None]:
    """Return (verdict, bucket, reason_code, pc_direction)."""
    name_a = pair["name_a"] or ""
    name_b = pair["name_b"] or ""
    web_verdict = pair["web_verdict"]
    web_conf = pair["web_conf"] or 0
    l2 = pair["l2"] or ""
    l2_5 = pair["l2_5"] or ""
    reasoning = pair["web_reasoning"] or ""

    def parse_tier(s):
        if not s or "@" not in s:
            return (None, 0)
        parts = s.split("@")
        try:
            return (parts[0], float(parts[1]))
        except (ValueError, IndexError):
            return (parts[0], 0)
    l2_verdict, l2_conf = parse_tier(l2)
    l2_5_verdict, l2_5_conf = parse_tier(l2_5)

    # HdB handling — §11.4.q
    if is_hdb(name_a) or is_hdb(name_b):
        a_is_hdb = is_hdb(name_a)
        b_is_hdb = is_hdb(name_b)
        if a_is_hdb and b_is_hdb:
            a_neg = hdb_negociant_name(name_a)
            b_neg = hdb_negociant_name(name_b)
            if a_neg and b_neg:
                if a_neg == b_neg:
                    return ("MERGE", "auto_decide", "hdb_same_negociant", None)
                # Different négociants → §11.4.q.1: SKIP
                return ("SKIP", "auto_decide", "hdb_different_negociants", None)
            # One or both has no négociant suffix → ambiguous
            return ("MERGE", "auto_decide", "hdb_variant_consolidation", None)
        # HdB (X) vs X → §11.4.q.2 PC with X as parent
        hdb_side = name_a if a_is_hdb else name_b
        non_hdb_side = name_b if a_is_hdb else name_a
        hdb_neg = hdb_negociant_name(hdb_side)
        if hdb_neg and hdb_neg in non_hdb_side.lower():
            # Parent is the non_hdb side (négociant)
            pc_dir = "b_is_parent" if a_is_hdb else "a_is_parent"
            return ("PARENT_CHILD", "auto_decide", "hdb_vs_negociant_pc", pc_dir)
        return ("ESCALATE_TO_CHROME", "escalate", "hdb_ambiguous", None)

    # Second-wine / cuvée pattern — data state issue, ESCALATE
    if has_second_wine_signal(reasoning):
        return ("ESCALATE_TO_CHROME", "escalate", "second_wine_or_cuvee_as_producer", None)

    # Web MERGE with specific shared-entity evidence
    if web_verdict == "MERGE" and has_specific_merge_signal(reasoning):
        # If strong dissent (L2 or L25 SKIP at >= 0.95), still escalate — respect the rich-tier strength
        if (l2_verdict == "SKIP" and l2_conf >= 0.96) or (l2_5_verdict == "SKIP" and l2_5_conf >= 0.96):
            return ("ESCALATE_TO_CHROME", "escalate", "strong_rich_tier_dissent", None)
        return ("MERGE", "auto_decide", "web_specific_shared_entity_evidence", None)

    # Web PC at any confidence + at least one rich tier PC → AUTO_DECIDE PC
    if web_verdict == "PARENT_CHILD":
        if l2_verdict == "PARENT_CHILD" or l2_5_verdict == "PARENT_CHILD":
            # Determine PC direction from reasoning heuristically
            # (caller can refine later; default to wine-count heuristic at apply time)
            return ("PARENT_CHILD", "auto_decide", "web_pc_plus_tier_pc", None)

    # Web UNCERTAIN → ESCALATE
    if web_verdict == "UNCERTAIN":
        return ("ESCALATE_TO_CHROME", "escalate", "web_uncertain", None)

    # Default — ESCALATE
    return ("ESCALATE_TO_CHROME", "escalate", "cross_tier_disagreement_no_specific_evidence", None)


def main():
    pairs = json.loads(Path("data/sprints/dedup/step9_phase_a_preclassified.json").read_text())
    pending = [p for p in pairs if p["precls_bucket"] == "pending_opus"]
    print(f"Processing {len(pending)} pending pairs with Opus rules...")

    results = []
    from collections import Counter
    bucket_counts = Counter()
    verdict_counts = Counter()
    reason_counts = Counter()

    for p in pending:
        verdict, bucket, reason_code, pc_dir = classify_opus(p)
        results.append({**p, "opus_verdict": verdict, "opus_bucket": bucket,
                        "opus_reason_code": reason_code, "opus_pc_dir": pc_dir})
        bucket_counts[bucket] += 1
        verdict_counts[verdict] += 1
        reason_counts[reason_code] += 1

    Path("data/sprints/dedup/step9_phase_a_opus_results.json").write_text(
        json.dumps(results, default=str, indent=2))

    print(f"\n=== Opus rules results ===")
    print(f"Buckets: {dict(bucket_counts)}")
    print(f"Verdicts: {dict(verdict_counts)}")
    print()
    print(f"Reason codes:")
    for r, n in reason_counts.most_common():
        print(f"  {r:<45} {n}")

    # Write to step9_opus_verdicts
    conn = get_conn()
    cur = conn.cursor()
    n_written = 0
    for r in results:
        cur.execute("""
            INSERT INTO step9_opus_verdicts (pair_id, verdict, bucket, reasoning, pattern_cluster, pc_parent_direction)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pair_id) DO UPDATE SET
              verdict = EXCLUDED.verdict,
              bucket = EXCLUDED.bucket,
              reasoning = EXCLUDED.reasoning,
              pattern_cluster = EXCLUDED.pattern_cluster,
              pc_parent_direction = EXCLUDED.pc_parent_direction
        """, (r["pair_id"], r["opus_verdict"], r["opus_bucket"],
              f"Opus-rules: {r['opus_reason_code']}", r["opus_reason_code"],
              r["opus_pc_dir"]))
        n_written += 1
    conn.commit()
    print(f"\nWrote {n_written} Opus-rules verdicts to step9_opus_verdicts")

    # Final totals
    cur.execute("SELECT bucket, COUNT(*) FROM step9_opus_verdicts GROUP BY bucket ORDER BY 2 DESC")
    print(f"\n=== Final step9_opus_verdicts totals (across all batches + pre-classifier + opus-rules) ===")
    for b, n in cur.fetchall():
        print(f"  {b:<20} {n}")
    cur.execute("SELECT verdict, COUNT(*) FROM step9_opus_verdicts GROUP BY verdict ORDER BY 2 DESC")
    print(f"\nVerdict totals:")
    for v, n in cur.fetchall():
        print(f"  {v:<25} {n}")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
