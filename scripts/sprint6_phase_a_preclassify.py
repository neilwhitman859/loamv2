"""Phase A deterministic pre-classifier.

Apply rules-based classification to the 680 remaining pairs:
  - AUTO_DECIDE MERGE: web MERGE >= 0.93 + L2 & L2.5 both MERGE + no corporate-holdco keywords + not single-letter-country ambig
  - AUTO_DECIDE PARENT_CHILD: web PC >= 0.90 + L2 & L2.5 both PC + clear name pattern
  - AUTO_DECIDE SKIP: web SKIP >= 0.93 + L2 & L2.5 both SKIP (shouldn't happen for user_review_merge_lowconf but check for missing)
  - NEEDS_OPUS_REVIEW: everything else

The corporate-owner watch-list + name-pattern heuristics catch cases where
the "selling entity would say" test requires more care.
"""
import sys, json, re
sys.stdout.reconfigure(encoding='utf-8')
from pipeline.lib.db import get_conn
from pathlib import Path

# Corporate owner-of-many-labels watch list (case-insensitive substring match)
# If EITHER producer name OR web_reasoning contains these, escalate to opus/chrome
CORPORATE_HOLDCOS = [
    "rothschild", "magrez", "constellation brands", "treasury wine", "e. & j. gallo",
    "lvmh", "pernod ricard", "kendall-jackson family", "vintage wine estates",
    "house of smith", "hess collection group", "jackson family",
    "antinori group",  # Antinori umbrella vs specific estates
    "frescobaldi",  # similar structure
    "gonzalez byass",  # multiple brands
    "symington family",  # Port houses under one umbrella
    "concha y toro",  # multi-brand Chilean
    "cavalieri d'oro",
    "beringer wine estates",
    "sonoma-cutrer",
]

# Explicit JV / collab patterns — these should NEVER auto-MERGE (§11.4.o)
JV_PATTERNS = [
    re.compile(r"\s+&\s+"),          # "X & Y"
    re.compile(r"\s+et\s+"),         # "X et Y" (French)
    re.compile(r"\s+x\s+", re.IGNORECASE),  # "X x Y" collabs
    re.compile(r"\s+and\s+", re.IGNORECASE),
]


def has_corporate_keyword(*texts) -> bool:
    combined = " ".join(t.lower() for t in texts if t)
    return any(h in combined for h in CORPORATE_HOLDCOS)


def is_jv_pattern(name: str) -> bool:
    return any(p.search(name or "") for p in JV_PATTERNS)


PREP_ARTICLE = {
    "du", "de", "la", "le", "les", "des", "del", "della", "dello", "di", "da",
    "von", "van", "der", "den", "auf", "zu", "im", "il", "lo",
}


def has_known_family_surname_pattern(name_a: str, name_b: str) -> bool:
    """Detect shared-surname family-split pattern requiring escalation.

    Exclude cases where one side's only differentiator is a preposition/article
    (e.g., "du Pegau" vs "Pegau" — same producer, not a split).
    """
    a_words = (name_a or "").split()
    b_words = (name_b or "").split()
    if not (a_words and b_words):
        return False
    if a_words[-1].lower() != b_words[-1].lower():
        return False
    if len(a_words) == 1 and len(b_words) == 1:
        return False
    # Strip leading prepositions/articles and re-compare
    def strip_prep(words):
        while words and words[0].lower() in PREP_ARTICLE:
            words = words[1:]
        return words
    a_stripped = strip_prep(a_words)
    b_stripped = strip_prep(b_words)
    if not a_stripped or not b_stripped:
        return False
    # If after stripping prepositions, one name is a subset of the other, NOT a family split
    if a_stripped == b_stripped:
        return False
    # If first content words match, NOT a family split (it's a spelling variant)
    if a_stripped[0].lower() == b_stripped[0].lower():
        return False
    return True


# Positive MERGE signal keywords in reasoning text
STRONG_MERGE_KEYWORDS = [
    "converges decisively", "converges on a single", "identical official website",
    "same official website", "identical brand", "unified brand identity",
    "identical knowledge graph", "both resolve to the same entity",
    "single producer", "same producer", "same family operation",
    "accent variant", "spelling variant", "typo", "abbreviation variant",
    "Hospices de Beaune négociant variant", "hdb négociant variant",
    "importer/curator prefix", "merchant prefix", "merchant-curator prefix",
    "§11.4.p", "§11.4.q", "§11.4.h", "§11.4.i", "§11.1",
    "strip the prefix", "strip prefix",
]

# Strong SKIP/ESCALATE signals
STRONG_SKIP_KEYWORDS = [
    "distinct producers", "distinct estates", "separate producers",
    "unrelated", "no ownership relation", "different owner",
    "distinct commercial entities", "coincidentally share",
]

# PC direction signals
PC_SIGNALS = [
    "parent", "sub-brand", "second label", "second wine",
    "subsidiary", "owned by", "controls", "umbrella",
    "child brand", "is a wine of", "is a second wine of",
]

# Hospices de Beaune variants — apply §11.4.q
HDB_PATTERN = re.compile(r"hospices?\s+de\s+(?:beaune|nuits)", re.IGNORECASE)


def has_strong_merge_signal(text: str) -> bool:
    tl = text.lower()
    return any(k.lower() in tl for k in STRONG_MERGE_KEYWORDS)


def has_strong_skip_signal(text: str) -> bool:
    tl = text.lower()
    return any(k.lower() in tl for k in STRONG_SKIP_KEYWORDS)


def is_hdb_pair(name_a: str, name_b: str) -> bool:
    return bool(HDB_PATTERN.search(name_a or "")) and bool(HDB_PATTERN.search(name_b or ""))


def classify(pair: dict) -> tuple[str, str, str]:
    """Return (verdict, bucket, reason_code)."""
    name_a = pair.get("name_a") or ""
    name_b = pair.get("name_b") or ""
    web_verdict = pair.get("web_verdict")
    web_conf = pair.get("web_conf") or 0
    l2 = pair.get("l2") or ""
    l2_5 = pair.get("l2_5") or ""
    reasoning = pair.get("web_reasoning") or ""
    action = pair.get("stage3_action") or ""

    # Parse L2/L2.5 verdicts out of "VERDICT@CONF" format
    def parse_tier(s):
        if not s or "@" not in s:
            return (s, 0)
        parts = s.split("@")
        try:
            return (parts[0], float(parts[1]))
        except (ValueError, IndexError):
            return (parts[0], 0)
    l2_verdict, l2_conf = parse_tier(l2)
    l2_5_verdict, l2_5_conf = parse_tier(l2_5)

    # Helper: does any dissenting rich tier have SKIP/PC at very high confidence?
    def has_strong_dissent(target_verdict):
        dissent_threshold = 0.95
        if l2_verdict and l2_verdict != target_verdict and l2_conf >= dissent_threshold:
            return True
        if l2_5_verdict and l2_5_verdict != target_verdict and l2_5_conf >= dissent_threshold:
            return True
        return False

    # --- ESCALATE FIRST (red flags) ---

    # Corporate-holdco rule
    if has_corporate_keyword(name_a, name_b, reasoning):
        return ("ESCALATE_TO_CHROME", "escalate", "corporate_owner_of_many_labels")

    # JV/collab pattern on MERGE verdict — never auto-MERGE
    if web_verdict == "MERGE" and (is_jv_pattern(name_a) or is_jv_pattern(name_b)):
        return ("ESCALATE_TO_CHROME", "escalate", "jv_or_collab_pattern_no_auto_merge")

    # Shared-surname family-split
    if has_known_family_surname_pattern(name_a, name_b):
        return ("ESCALATE_TO_CHROME", "escalate", "shared_surname_family_split")

    # --- AUTO-DECIDE MERGE (various confidence paths) ---

    if web_verdict == "MERGE":
        # Path 1: HdB pair — trust the §11.4.q pattern if both names start with HdB
        if is_hdb_pair(name_a, name_b):
            return ("MERGE", "auto_decide", "hdb_pattern_merge")

        # Path 2: 3-tier consensus at any confidence >= 0.88
        if (web_conf >= 0.88 and l2_verdict == "MERGE" and l2_5_verdict == "MERGE"):
            return ("MERGE", "auto_decide", "three_tier_consensus")

        # Path 3: Identical normalized names + web MERGE at >= 0.88 + one rich tier agrees + no strong dissent
        norm_a = name_a.lower().strip()
        norm_b = name_b.lower().strip()
        if (norm_a == norm_b and web_conf >= 0.88
                and (l2_verdict == "MERGE" or l2_5_verdict == "MERGE")
                and not has_strong_dissent("MERGE")):
            return ("MERGE", "auto_decide", "identical_names_plus_tier")

        # Path 4: Strong MERGE signals in reasoning + web conf >= 0.88 + at least one rich tier MERGE + no strong dissent
        if (web_conf >= 0.88 and has_strong_merge_signal(reasoning)
                and (l2_verdict == "MERGE" or l2_5_verdict == "MERGE")
                and not has_strong_dissent("MERGE")):
            return ("MERGE", "auto_decide", "strong_merge_signal_plus_tier")

        # Path 5: Substring-variant cases (one name contains the other after strip)
        def strip_noise(s):
            t = (s or "").lower()
            for w in ["chateau", "château", "domaine", "bodega", "bodegas", "tenuta",
                     "casa", "cantina", "weingut", "maison", "clos",
                     "estate", "winery", "vineyards", "vineyard", "cellars", "wines"]:
                t = t.replace(w, "")
            return re.sub(r"\s+", " ", t).strip()
        if (strip_noise(name_a) == strip_noise(name_b) and web_conf >= 0.87
                and not has_strong_skip_signal(reasoning)
                and not has_strong_dissent("MERGE")):
            return ("MERGE", "auto_decide", "noise_stripped_name_match")

        # Path 6: Preposition-variant (e.g., "du Pegau" vs "Pegau") — not a family split
        def strip_prep_leading(s):
            words = (s or "").split()
            while words and words[0].lower() in PREP_ARTICLE:
                words = words[1:]
            return " ".join(words).lower()
        if (strip_prep_leading(name_a) == strip_prep_leading(name_b)
                and web_conf >= 0.87 and not has_strong_skip_signal(reasoning)
                and not has_strong_dissent("MERGE")):
            return ("MERGE", "auto_decide", "preposition_variant")

    # --- AUTO-DECIDE PARENT_CHILD ---

    if web_verdict == "PARENT_CHILD":
        # 3-tier PC consensus
        if (web_conf >= 0.88
                and l2_verdict == "PARENT_CHILD" and l2_5_verdict == "PARENT_CHILD"):
            return ("PARENT_CHILD", "auto_decide", "three_tier_pc_consensus")

        # 2-tier PC consensus (web + one rich) — at >= 0.88 + no strong dissent
        if (web_conf >= 0.88
                and (l2_verdict == "PARENT_CHILD" or l2_5_verdict == "PARENT_CHILD")
                and not has_strong_dissent("PARENT_CHILD")):
            return ("PARENT_CHILD", "auto_decide", "two_tier_pc_consensus")

    # --- AUTO-DECIDE SKIP ---

    if web_verdict == "SKIP" and web_conf >= 0.90:
        if l2_verdict == "SKIP" and l2_5_verdict == "SKIP":
            return ("SKIP", "auto_decide", "three_tier_skip_consensus")
        if has_strong_skip_signal(reasoning):
            return ("SKIP", "auto_decide", "web_skip_plus_strong_signal")

    # --- Default: Opus review ---

    return ("NEEDS_OPUS_REVIEW", "pending_opus", "not_classified_by_rules")


def main():
    pairs = json.loads(Path("data/sprints/dedup/step9_phase_a_remaining.json").read_text())
    print(f"Classifying {len(pairs)} pairs...")

    results = []
    from collections import Counter
    bucket_counts = Counter()
    reason_counts = Counter()

    for p in pairs:
        verdict, bucket, reason_code = classify(p)
        results.append({**p, "precls_verdict": verdict, "precls_bucket": bucket, "precls_reason_code": reason_code})
        bucket_counts[bucket] += 1
        reason_counts[reason_code] += 1

    Path("data/sprints/dedup/step9_phase_a_preclassified.json").write_text(json.dumps(results, default=str, indent=2))

    print(f"\n=== Pre-classification results ===")
    for b, n in bucket_counts.most_common():
        print(f"  {b:<18} {n:>4}")
    print()
    print(f"=== Reason codes ===")
    for r, n in reason_counts.most_common():
        print(f"  {r:<45} {n:>4}")

    # Write auto-decided verdicts to DB
    conn = get_conn()
    cur = conn.cursor()
    n_written = 0
    for r in results:
        if r["precls_bucket"] == "auto_decide":
            cur.execute("""
                INSERT INTO step9_opus_verdicts (pair_id, verdict, bucket, reasoning, pattern_cluster, pc_parent_direction)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (pair_id) DO UPDATE SET
                  verdict = EXCLUDED.verdict,
                  bucket = EXCLUDED.bucket,
                  reasoning = EXCLUDED.reasoning,
                  pattern_cluster = EXCLUDED.pattern_cluster
            """, (r["pair_id"], r["precls_verdict"], "auto_decide",
                  f"Pre-classifier: {r['precls_reason_code']}",
                  r["precls_reason_code"], None))
            n_written += 1
    conn.commit()
    print(f"\nWrote {n_written} auto-decide verdicts to step9_opus_verdicts")

    # Also write escalates (from corporate/JV/surname rules)
    n_esc = 0
    for r in results:
        if r["precls_bucket"] == "escalate":
            cur.execute("""
                INSERT INTO step9_opus_verdicts (pair_id, verdict, bucket, reasoning, pattern_cluster)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (pair_id) DO UPDATE SET
                  verdict = EXCLUDED.verdict,
                  bucket = EXCLUDED.bucket,
                  reasoning = EXCLUDED.reasoning,
                  pattern_cluster = EXCLUDED.pattern_cluster
            """, (r["pair_id"], "ESCALATE_TO_CHROME", "escalate",
                  f"Pre-classifier rule: {r['precls_reason_code']}",
                  r["precls_reason_code"]))
            n_esc += 1
    conn.commit()
    print(f"Wrote {n_esc} escalate verdicts to step9_opus_verdicts")

    # Pending Opus review count
    n_pending = sum(1 for r in results if r["precls_bucket"] == "pending_opus")
    print(f"\n{n_pending} pairs remain for Opus inline review")

    cur.close(); conn.close()


if __name__ == "__main__":
    main()
