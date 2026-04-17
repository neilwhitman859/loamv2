"""
B6.4 Phase A — Gold-label calibration pairs via Sonnet 4.6 + web_search_20250305.

The "oracle" for the B6.4 calibration set. Takes pairs whose gold_verdict is None
(Tiers T3, T4, T5 by default), sends Sonnet + Anthropic native web search on each,
and writes gold_verdict + gold_confidence + gold_reasoning + gold_web_evidence back
to calibration_set.json.

Schema of each pair after oracle:
  gold_verdict     "MERGE" | "PARENT_CHILD" | "SKIP" | "UNCERTAIN"
  gold_confidence  float 0.0-1.0
  gold_reasoning   str
  gold_web_evidence list[{url, title, query, snippet}]
  gold_source      "sonnet_4_6_web_search_20250305"

Run:
    # dry-run one pair for a prompt preview
    python -m pipeline.identity.calibration_oracle --dry-run

    # run the oracle, budget cap at $15, resume from prior run
    python -m pipeline.identity.calibration_oracle --execute --budget 15

    # limit to tier(s), useful for debugging
    python -m pipeline.identity.calibration_oracle --execute --tiers T5 --limit 5

Cost: Sonnet 4.6 = $3/MTok input + $15/MTok output; web_search_20250305 = $10/1K queries.
Per pair est: ~5K input cached (0.3/MTok cache read) + ~800 out + 2 searches => $0.03-0.05.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import SONNET_MODEL


CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"
IDENTITY_RULES_PATH = Path(__file__).resolve().parents[2] / "docs" / "IDENTITY_RULES.md"


# Sonnet 4.6 pricing
PRICING = {
    "input":          3.00,   # $/MTok
    "output":        15.00,   # $/MTok
    "cache_read":     0.30,   # $/MTok
    "cache_write":    3.75,   # $/MTok
    "web_search":    10.00,   # $/1K queries
}


def load_section_11() -> str:
    """Load IDENTITY_RULES.md section 11 for embedding."""
    text = IDENTITY_RULES_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"(## 11\. Producer Identity Rules.*?)(?=\n## Appendix|\n---\s*\n## )",
        text, re.DOTALL
    )
    if not m:
        raise RuntimeError("Could not locate section 11 in IDENTITY_RULES.md")
    return m.group(1).strip()


ORACLE_SCHEMA = """OUTPUT FORMAT — at the end of your response, return a single JSON object (no markdown fences, no preamble before or after):

{
  "verdict":    "MERGE" | "PARENT_CHILD" | "SKIP" | "UNCERTAIN",
  "confidence": <float 0.0-1.0>,
  "reasoning":  "<2-4 sentences, cite the specific web evidence you used>",
  "web_evidence": [
    {"url": "<URL>", "title": "<page title>", "why": "<what this proved>"},
    ...
  ]
}

The JSON object is MANDATORY and MUST be the last thing in your response.
"""


ORACLE_INSTRUCTIONS = """You are the GOLD-LABEL oracle for a producer-dedup calibration set.
You have access to the web_search tool. Use it (up to 5 searches per pair) to verify
producer identity before classifying. Prefer producer's own website, Wikipedia,
Wine-Searcher, Liv-ex/LWIN records, importer sites.

Classify the producer pair per the IDENTITY_RULES Section 11 embedded in the
system prompt. Do NOT rely solely on the names or L1 verdict we provide — the
whole point of this oracle call is that L1 may be wrong and we need an external
check to ground-truth it.

Verdict guidance for the gold-label task:
- Prefer MERGE if evidence shows the two rows refer to the same producer (same
  brand on label, same physical address, same owner+brand, name is a known
  abbreviation/translation/typo of the other, different spelling of one legal
  entity). "Near-certain" cases should have confidence >=0.90.
- Prefer PARENT_CHILD if distinct brands with documented ownership relation
  (Silver Oak / Twomey; négociant + estate labels from the same house;
  corporate holdco with a label-appearing brand).
- Prefer SKIP if web evidence shows two distinct producers with independent
  identities (different owners, different addresses, different brand portfolios).
  "Near-certain" cases should have confidence >=0.90.
- Use UNCERTAIN ONLY if web searches return no conclusive evidence after 3+
  queries. Do not use UNCERTAIN as a convenience bucket — prefer SKIP when
  no evidence supports MERGE.

Always cite evidence via web_evidence. If you did not search, say so in reasoning
and set confidence below 0.70.
"""


def build_system_preamble(section_11: str) -> str:
    """Static preamble that gets cached across calls."""
    return f"""You are a wine-data identity-verification oracle. Your job is to
assign gold labels to candidate producer-pair duplicates by consulting the web.

{section_11}

{ORACLE_SCHEMA}

{ORACLE_INSTRUCTIONS}
"""


def _cap_list(items, n):
    return items[:n] if items and len(items) > n else (items or [])


def load_context(cur, pair: dict) -> dict:
    """Pull extra context from DB for one pair: top wines + TTB fingerprint + LWIN."""
    pa = pair["producer_id_a"]
    pb = pair["producer_id_b"]

    # Top wines per producer
    cur.execute("""
      SELECT producer_id, COALESCE(display_name, name)
      FROM (
        SELECT producer_id, display_name, name,
               ROW_NUMBER() OVER (
                 PARTITION BY producer_id
                 ORDER BY length(COALESCE(display_name, name, '')) DESC
               ) AS rn
        FROM wines
        WHERE producer_id IN (%s, %s) AND deleted_at IS NULL
      ) r
      WHERE rn <= 8
    """, (pa, pb))
    wines = {}
    for pid, nm in cur.fetchall():
        wines.setdefault(str(pid), []).append(nm)

    # TTB fingerprint
    cur.execute("""
      SELECT canonical_producer_id,
             COALESCE(permit_no, permit_number) AS permit,
             applicant_name, applicant_address, applicant_city, applicant_state,
             brand_name
      FROM source_ttb_colas
      WHERE canonical_producer_id IN (%s, %s)
        AND (permit_no IS NOT NULL OR permit_number IS NOT NULL)
      LIMIT 5000
    """, (pa, pb))
    ttb: dict[str, dict] = {}
    for pid, permit, nm, addr, city, state, brand in cur.fetchall():
        d = ttb.setdefault(str(pid), {
            "bw_permits": set(),
            "permittees": set(),
            "addresses": set(),
            "brand_names": set(),
            "cola_count": 0,
        })
        d["cola_count"] += 1
        if permit and (permit.startswith("BW-") or re.match(r"^[A-Z]{2,3}-BW", permit)):
            d["bw_permits"].add(permit)
        if nm:
            d["permittees"].add(nm)
        if addr:
            full_addr = ", ".join(x for x in [addr, city, state] if x)
            if full_addr:
                d["addresses"].add(full_addr)
        if brand:
            d["brand_names"].add(brand.upper())
    for pid, d in ttb.items():
        d["bw_permits"] = sorted(d["bw_permits"])[:5]
        d["permittees"] = sorted(d["permittees"])[:3]
        d["addresses"] = sorted(d["addresses"])[:3]
        d["brand_names"] = sorted(d["brand_names"])[:10]

    # LWIN link count
    cur.execute("""
      SELECT w.producer_id, COUNT(DISTINCT ei.external_id)
      FROM wines w
      JOIN external_ids ei
        ON ei.entity_type='wine' AND ei.entity_id=w.id AND ei.system='lwin_7'
      WHERE w.producer_id IN (%s, %s) AND w.deleted_at IS NULL
      GROUP BY w.producer_id
    """, (pa, pb))
    lwin = {str(pid): n for pid, n in cur.fetchall()}

    # Producer metadata
    cur.execute("""
      SELECT id, metadata, region_id, year_established, country_id
      FROM producers
      WHERE id IN (%s, %s)
    """, (pa, pb))
    meta = {}
    for pid, md, rid, year, cid in cur.fetchall():
        website = None
        if md and isinstance(md, dict):
            website = md.get("website") or md.get("url") or md.get("homepage")
        meta[str(pid)] = {
            "website": website,
            "region_id": str(rid) if rid else None,
            "year_established": year,
            "country_id": str(cid) if cid else None,
        }

    return {
        "wines": wines,
        "ttb": ttb,
        "lwin": lwin,
        "meta": meta,
    }


def format_pair_prompt(pair: dict, ctx: dict) -> str:
    """Build the per-pair user message."""
    pa = pair["producer_id_a"]
    pb = pair["producer_id_b"]
    wines_a = _cap_list(ctx["wines"].get(pa, []), 8)
    wines_b = _cap_list(ctx["wines"].get(pb, []), 8)
    ttb_a = ctx["ttb"].get(pa, {})
    ttb_b = ctx["ttb"].get(pb, {})
    lwin_a = ctx["lwin"].get(pa, 0)
    lwin_b = ctx["lwin"].get(pb, 0)
    meta_a = ctx["meta"].get(pa, {})
    meta_b = ctx["meta"].get(pb, {})

    def ttb_line(t):
        if not t:
            return "(no TTB COLA records)"
        bits = []
        if t.get("bw_permits"):
            bits.append(f"BW permits: {t['bw_permits']}")
        if t.get("permittees"):
            bits.append(f"Permittees: {t['permittees']}")
        if t.get("addresses"):
            bits.append(f"Address: {t['addresses'][0]}")
        if t.get("brand_names"):
            bits.append(f"Brand names on COLAs: {t['brand_names']}")
        bits.append(f"COLA count: {t.get('cola_count', 0)}")
        return "; ".join(bits)

    def meta_line(m):
        bits = []
        if m.get("website"):
            bits.append(f"website: {m['website']}")
        if m.get("year_established"):
            bits.append(f"established: {m['year_established']}")
        return "; ".join(bits) if bits else "(no producer metadata)"

    return f"""PAIR to verify (tier={pair['tier']}, country={pair['country']}):

Producer A: {pair['name_a']!r}  ({pair['wines_a']} wines, {lwin_a} LWIN-linked)
  Sample wines: {wines_a}
  TTB: {ttb_line(ttb_a)}
  Meta: {meta_line(meta_a)}

Producer B: {pair['name_b']!r}  ({pair['wines_b']} wines, {lwin_b} LWIN-linked)
  Sample wines: {wines_b}
  TTB: {ttb_line(ttb_b)}
  Meta: {meta_line(meta_b)}

Blocking signals: {json.dumps(pair.get('signals') or {}, default=str)}
L1 Haiku verdict: {pair.get('l1_verdict')} (confidence {pair.get('l1_confidence')})
L1 reasoning: {pair.get('l1_reasoning')}

Use the web_search tool to verify whether these two rows refer to the same
producer. Return the gold JSON at the end of your response.
"""


def parse_response(content_blocks: list) -> tuple[dict, list]:
    """Extract the last JSON object from text blocks + collect web_search results."""
    text = ""
    evidence: list[dict] = []
    for block in content_blocks:
        t = getattr(block, "type", None)
        if t == "text":
            text += block.text
        elif t == "web_search_tool_result":
            # block.content is a list of {type: web_search_result, url, title, ...}
            for r in (block.content or []):
                if getattr(r, "type", None) == "web_search_result":
                    evidence.append({
                        "url": getattr(r, "url", None),
                        "title": getattr(r, "title", None),
                        "page_age": getattr(r, "page_age", None),
                    })
    if not text.strip():
        return {"verdict": "UNCERTAIN", "confidence": 0.0, "reasoning": "No text response"}, evidence

    # Find last JSON object
    m = re.findall(r"\{[^{}]*\}(?:\s*$|\s*\Z)", text, re.DOTALL)
    json_obj = None
    for candidate in reversed(list(re.finditer(r"\{(?:[^{}]|\{[^{}]*\})*\}", text, re.DOTALL))):
        try:
            json_obj = json.loads(candidate.group(0))
            if "verdict" in json_obj:
                break
        except json.JSONDecodeError:
            continue
    if not json_obj:
        return {"verdict": "UNCERTAIN", "confidence": 0.0, "reasoning": f"No JSON parsed from response. First 400 chars: {text[:400]}"}, evidence
    return json_obj, evidence


def compute_cost_cents(usage, n_web_searches: int) -> float:
    """Cost in cents for one Sonnet + web_search call."""
    in_t = usage.input_tokens
    out_t = usage.output_tokens
    cache_r = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_w = getattr(usage, "cache_creation_input_tokens", 0) or 0
    regular_input = max(0, in_t - cache_r - cache_w)

    cost = (
        regular_input * PRICING["input"] / 1_000_000
        + out_t        * PRICING["output"] / 1_000_000
        + cache_r      * PRICING["cache_read"] / 1_000_000
        + cache_w      * PRICING["cache_write"] / 1_000_000
        + n_web_searches * PRICING["web_search"] / 1_000
    )
    return cost * 100


def call_oracle(client: anthropic.Anthropic, preamble: str,
                pair_prompt: str, max_searches: int = 5, dry_run: bool = False):
    """Single Sonnet call with web_search tool. Returns (json_obj, evidence, usage, n_searches, err)."""
    if dry_run:
        return {"verdict": "UNCERTAIN", "confidence": 0.0}, [], None, 0, None

    try:
        resp = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=4000,
            system=[{
                "type": "text",
                "text": preamble,
                "cache_control": {"type": "ephemeral"},
            }],
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": max_searches,
            }],
            messages=[{"role": "user", "content": pair_prompt}],
        )
        json_obj, evidence = parse_response(resp.content)
        # Count server_tool_use blocks to estimate number of searches
        n_searches = sum(
            1 for b in resp.content if getattr(b, "type", None) == "server_tool_use"
        )
        return json_obj, evidence, resp.usage, n_searches, None
    except Exception as e:
        return {"verdict": "UNCERTAIN", "confidence": 0.0,
                "reasoning": f"API ERROR: {type(e).__name__}: {str(e)[:200]}"}, [], None, 0, str(e)[:300]


def main() -> int:
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--budget", type=float, default=15.0, help="budget cap in dollars")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--tiers", default="T3,T4,T5", help="comma-separated tiers")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--max-searches", type=int, default=5)
    ap.add_argument("--resume", action="store_true", default=True,
                    help="skip pairs that already have gold_verdict (default on)")
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    args = ap.parse_args()

    if not (args.execute or args.dry_run):
        print("Specify --execute or --dry-run", file=sys.stderr)
        return 2

    if not CALIBRATION_PATH.exists():
        print(f"Missing {CALIBRATION_PATH}. Run build_calibration_set.py first.", file=sys.stderr)
        return 1

    doc = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
    section_11 = load_section_11()
    preamble = build_system_preamble(section_11)
    print(f"Preamble: {len(preamble)} chars (~{len(preamble)//4} tokens est.)")

    tier_set = set(args.tiers.split(","))
    targets = []
    for tier_name, pairs in doc["tiers"].items():
        if tier_name not in tier_set:
            continue
        for p in pairs:
            if args.resume and p.get("gold_verdict"):
                continue
            targets.append(p)
    if args.limit:
        targets = targets[: args.limit]
    print(f"Target {len(targets)} pairs across tiers {sorted(tier_set)}")

    if args.dry_run:
        conn = get_conn()
        cur = conn.cursor()
        sample = targets[:1]
        if sample:
            ctx = load_context(cur, sample[0])
            prompt = format_pair_prompt(sample[0], ctx)
            print("\n--- SYSTEM PREAMBLE (truncated) ---")
            print(preamble[:1500] + "\n...[truncated]...")
            print("\n--- USER MESSAGE ---")
            print(prompt)
            print("\n--- END DRY RUN ---")
        cur.close()
        conn.close()
        return 0

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    conn = get_conn()
    cur = conn.cursor()

    total_cost_cents = 0.0
    state_lock = Lock()
    abort_flag = {"abort": False}
    completed = 0

    # Save progress periodically
    def persist():
        CALIBRATION_PATH.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")

    def process(idx: int, pair: dict):
        if abort_flag["abort"]:
            return idx, pair, {"verdict": "UNCERTAIN"}, [], 0.0, "aborted"
        ctx = load_context(cur, pair)
        prompt = format_pair_prompt(pair, ctx)
        json_obj, evidence, usage, n_searches, err = call_oracle(
            client, preamble, prompt, max_searches=args.max_searches
        )
        cost_cents = compute_cost_cents(usage, n_searches) if usage else 0.0
        return idx, pair, json_obj, evidence, cost_cents, err

    t_start = time.time()

    # Run serially when workers=1 to share cursor safely; parallel uses separate conns
    if args.workers <= 1:
        for i, pair in enumerate(targets):
            idx, pair, verdict, evidence, cost_cents, err = process(i, pair)
            if err:
                print(f"  [{i+1}/{len(targets)}] ERROR {pair['name_a']!r}/{pair['name_b']!r}: {err[:200]}", flush=True)
            with state_lock:
                total_cost_cents += cost_cents
                completed += 1
                if total_cost_cents / 100 > args.budget:
                    print(f"\n!! Budget exceeded (${total_cost_cents/100:.2f} > ${args.budget:.2f}). Aborting remaining.", flush=True)
                    abort_flag["abort"] = True
                pair["gold_verdict"] = verdict.get("verdict")
                pair["gold_confidence"] = verdict.get("confidence")
                pair["gold_reasoning"] = verdict.get("reasoning")
                pair["gold_web_evidence"] = verdict.get("web_evidence") or [
                    {"url": e.get("url"), "title": e.get("title")} for e in evidence
                ]
                pair["gold_source"] = "sonnet_4_6_web_search_20250305"
                if completed % 10 == 0:
                    persist()
            elapsed = time.time() - t_start
            eta = (elapsed / max(1, completed)) * (len(targets) - completed)
            print(f"  [{completed}/{len(targets)}] {pair['name_a'][:30]!r:<32}/"
                  f"{pair['name_b'][:30]!r:<32} "
                  f"-> {verdict.get('verdict'):<14} {verdict.get('confidence')} "
                  f"| {cost_cents:.2f}c | ${total_cost_cents/100:.2f} | eta={eta/60:.0f}m",
                  flush=True)
    else:
        # Parallel workers: each uses its own DB cursor
        def worker_process(idx: int, pair: dict):
            if abort_flag["abort"]:
                return idx, pair, {"verdict": "UNCERTAIN"}, [], 0.0, "aborted"
            local_conn = get_conn()
            local_cur = local_conn.cursor()
            try:
                ctx = load_context(local_cur, pair)
                prompt = format_pair_prompt(pair, ctx)
                json_obj, evidence, usage, n_searches, err = call_oracle(
                    client, preamble, prompt, max_searches=args.max_searches
                )
                cost_cents = compute_cost_cents(usage, n_searches) if usage else 0.0
                return idx, pair, json_obj, evidence, cost_cents, err
            finally:
                local_cur.close()
                local_conn.close()

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(worker_process, i, p): i for i, p in enumerate(targets)}
            for fut in as_completed(futures):
                idx, pair, verdict, evidence, cost_cents, err = fut.result()
                if err and err != "aborted":
                    print(f"  [{completed+1}] ERROR {pair['name_a']!r}/{pair['name_b']!r}: {err[:200]}", flush=True)
                with state_lock:
                    total_cost_cents += cost_cents
                    completed += 1
                    if total_cost_cents / 100 > args.budget:
                        print(f"\n!! Budget exceeded. Aborting.", flush=True)
                        abort_flag["abort"] = True
                    pair["gold_verdict"] = verdict.get("verdict")
                    pair["gold_confidence"] = verdict.get("confidence")
                    pair["gold_reasoning"] = verdict.get("reasoning")
                    pair["gold_web_evidence"] = verdict.get("web_evidence") or [
                        {"url": e.get("url"), "title": e.get("title")} for e in evidence
                    ]
                    pair["gold_source"] = "sonnet_4_6_web_search_20250305"
                    if completed % 10 == 0:
                        persist()
                elapsed = time.time() - t_start
                eta = (elapsed / max(1, completed)) * (len(targets) - completed)
                print(f"  [{completed}/{len(targets)}] {pair['name_a'][:30]!r:<32}/"
                      f"{pair['name_b'][:30]!r:<32} "
                      f"-> {verdict.get('verdict'):<14} {verdict.get('confidence')} "
                      f"| {cost_cents:.2f}c | ${total_cost_cents/100:.2f} | eta={eta/60:.0f}m",
                      flush=True)

    # Final persist
    persist()

    # Report
    print("\n" + "=" * 60)
    print("ORACLE RESULTS")
    print("=" * 60)
    print(f"Pairs labeled: {completed}")
    print(f"Total cost:    ${total_cost_cents/100:.3f}")
    if completed:
        print(f"Per-pair:      {total_cost_cents/completed:.2f}c avg")

    # Distribution
    from collections import Counter
    verdicts = Counter(
        p.get("gold_verdict")
        for tier, pairs in doc["tiers"].items()
        for p in pairs
        if tier in tier_set and p.get("gold_verdict")
    )
    print("\nGold verdict distribution:")
    for v, n in verdicts.most_common():
        print(f"  {v:<15} {n}")

    # Compare against L1
    print("\nL1 vs Gold agreement:")
    from collections import defaultdict
    agree_counts = defaultdict(lambda: defaultdict(int))
    for tier, pairs in doc["tiers"].items():
        if tier not in tier_set:
            continue
        for p in pairs:
            gv = p.get("gold_verdict")
            lv = p.get("l1_verdict")
            if gv and lv:
                agree_counts[lv][gv] += 1
    for l1_v in sorted(agree_counts):
        row_total = sum(agree_counts[l1_v].values())
        print(f"  L1={l1_v:<14} ({row_total}): " +
              ", ".join(f"{g}={n}" for g, n in sorted(agree_counts[l1_v].items())))

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
