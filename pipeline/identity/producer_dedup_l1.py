"""
B6.3 Part E — L1 Haiku batched producer-pair classifier.

Reads pairs from `producer_dedup_pairs` with method_name='blocking',
batches 10/call, asks Haiku 4.5 (direct Anthropic SDK) to classify each
pair as MERGE / PARENT_CHILD / SKIP / UNCERTAIN per IDENTITY_RULES §11.
Writes results back to producer_dedup_pairs with method_name='l1_haiku_batch'.

Prompt caching: IDENTITY_RULES §11 + output schema + few-shot examples
are marked cache_control='ephemeral' (1-hour TTL with extended-cache beta).
Variable per-batch input = 10 pair context blocks.

Run:
    # 200-pair stratified pilot
    python -m pipeline.identity.producer_dedup_l1 --pilot --execute

    # 200-pair pilot dry-run (prints prompt, no API call)
    python -m pipeline.identity.producer_dedup_l1 --pilot --dry-run

    # Full run (post-pilot approval)
    python -m pipeline.identity.producer_dedup_l1 --execute --budget 150

    # Subset by strategy or size
    python -m pipeline.identity.producer_dedup_l1 --limit 50 --execute

Model: HAIKU_MODEL (claude-haiku-4-5). Direct Anthropic, not OpenRouter.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL


# ── Pricing (Haiku 4.5) ───────────────────────────────────────────

PRICING = {
    "input":           0.80,   # $/1M tokens
    "output":          4.00,   # $/1M tokens
    "cache_write":     1.00,   # $/1M tokens (1.25x base input; extended 1h = 2x)
    "cache_write_1h":  1.60,   # $/1M tokens (2x base)
    "cache_read":      0.08,   # $/1M tokens (0.1x base)
}


# ── Section 11 loader ─────────────────────────────────────────────

def load_section_11() -> str:
    """Load IDENTITY_RULES.md §11 verbatim for prompt embedding."""
    path = Path(__file__).resolve().parents[2] / "docs" / "IDENTITY_RULES.md"
    text = path.read_text(encoding="utf-8")
    # Extract from '## 11. Producer Identity Rules' to before '## Appendix'
    m = re.search(
        r"(## 11\. Producer Identity Rules.*?)(?=\n## Appendix|\n---\s*\n## )",
        text, re.DOTALL
    )
    if not m:
        raise RuntimeError(
            "Could not locate Section 11 in IDENTITY_RULES.md — check headers"
        )
    return m.group(1).strip()


# ── Static prompt preamble (cacheable) ────────────────────────────

OUTPUT_SCHEMA = """OUTPUT FORMAT — return ONLY a JSON array, no markdown fences, no preamble:

[
  {
    "pair_id": <integer, the pair_id provided in the input>,
    "verdict": "MERGE" | "PARENT_CHILD" | "SKIP" | "UNCERTAIN",
    "confidence": <float 0.0-1.0>,
    "reasoning": "<one or two sentences citing specific evidence you used>"
  },
  ...
]

Rules for verdict assignment:
- MERGE: same brand identity on the label per §11.1. High confidence when multiple signals converge (shared permit + substring + shared wine) or when the names are near-identical across a country split.
- PARENT_CHILD: distinct brands with a clear ownership relation per §11.2. Use when two rows share a BW permit but display different brands (e.g., Silver Oak + Twomey).
- SKIP: unrelated producers per §11.3. Use for commune-name collisions (Latour vs Latour-Martillac), family-name collisions (Conterno vs Conterno Fantino), different wineries in the Cape Winemakers Guild, distinct holdings in a portfolio, etc.
- UNCERTAIN: genuinely mixed signals — use sparingly. An UNCERTAIN verdict will escalate to L2/L3 review. Prefer SKIP when you can't find positive evidence for MERGE.

For confidence:
- >=0.90: unambiguous (multiple converging signals, clearly same/different identity)
- 0.70-0.89: strong single signal or consistent but not-converging signals
- 0.50-0.69: leaning but real uncertainty
- <0.50: prefer UNCERTAIN over a low-confidence verdict

For reasoning:
- Cite the concrete evidence. Name specific brands, permit numbers, shared wines, countries. Do NOT paraphrase or hedge.
- Short and direct. 1-2 sentences.
"""


FEW_SHOT = """Examples demonstrating the rules:

Example 1 — obvious MERGE with multi-strategy agreement:
Input pair: {pair_id: 1, producer_a_name: "Ridge Vineyards", producer_a_country: "US", producer_a_wines: 110,
            producer_b_name: "Ridge", producer_b_country: "US", producer_b_wines: 2,
            signals: {"s2_trigram": 0.375, "s9_substring": {"kind": "word_contain"}, "s6_ttb_permit": {"permits": ["BW-4488","BW-CA-4488","BW-CA-4798"]}},
            ttb_fingerprint_a: {bw_permits: ["BW-CA-4488","BW-CA-4798","BW-4488"], brand_names: ["RIDGE","RIDGE VINEYARDS","MONTE BELLO"]},
            ttb_fingerprint_b: {bw_permits: ["BW-CA-4488"], brand_names: ["RIDGE VINEYARDS"]}}
Output: {"pair_id": 1, "verdict": "MERGE", "confidence": 0.98, "reasoning": "Three converging signals: shared BW permit BW-CA-4488, substring containment, trigram similarity. Both rows produce 'Ridge' brand wines from the same Santa Cruz Mountains BW facility. The 'Ridge' row is a B6.2 LWIN-import variant of the canonical 'Ridge Vineyards'."}

Example 2 — SKIP despite high trigram (famous distinct-producers case):
Input pair: {pair_id: 2, producer_a_name: "Stag's Leap Wine Cellars", producer_a_country: "US", producer_a_wines: 49,
            producer_b_name: "Stags' Leap Winery", producer_b_country: "US", producer_b_wines: 26,
            signals: {"s2_trigram": 0.72}}
Output: {"pair_id": 2, "verdict": "SKIP", "confidence": 0.97, "reasoning": "Two distinct historical producers in the Stag's Leap District, Napa — Warren Winiarski's Stag's Leap Wine Cellars and the Doumani family's Stags' Leap Winery. Apostrophe-position difference on label reflects genuinely different brand identities; famous case often cited in wine trade literature."}

Example 3 — PARENT_CHILD at shared BW permit:
Input pair: {pair_id: 3, producer_a_name: "Silver Oak", producer_a_country: "US", producer_a_wines: 4,
            producer_b_name: "Twomey", producer_b_country: "US", producer_b_wines: 13,
            signals: {"s6_ttb_permit": {"permits": ["BW-CA-5455"]}},
            ttb_fingerprint_a: {bw_permits: ["BW-CA-5455"], brand_names: ["SILVER OAK","SILVER OAK CELLARS"]},
            ttb_fingerprint_b: {bw_permits: ["BW-CA-5455"], brand_names: ["TWOMEY","TWOMEY CELLARS"]}}
Output: {"pair_id": 3, "verdict": "PARENT_CHILD", "confidence": 0.92, "reasoning": "Silver Oak and Twomey are distinct brand identities (different bottles, different labels, different wines — Silver Oak makes Cabernet; Twomey makes Pinot/Sauv Blanc/Merlot) owned by the same Duncan family at the same BW-CA-5455 facility. Classic PARENT-CHILD per §11.2."}

Example 4 — cross-country same name, UNCERTAIN (needs L2/L3):
Input pair: {pair_id: 4, producer_a_name: "Jordan", producer_a_country: "US", producer_a_wines: 25,
            producer_b_name: "Jordan", producer_b_country: "ZA", producer_b_wines: 24,
            signals: {"s7_cross_country": {"trigram": 1.0, "country_a": "US", "country_b": "ZA"}}}
Output: {"pair_id": 4, "verdict": "SKIP", "confidence": 0.95, "reasoning": "Jordan Vineyard & Winery in Sonoma (US) and Jordan Wine Estate in Stellenbosch (South Africa) are distinct unrelated producers with independently-developed brand identities. The South African estate was started by Gary Jordan; the US winery by Tom Jordan — no ownership connection."}

Example 5 — lose-lose split becomes MERGE via shared rare wine:
Input pair: {pair_id: 5, producer_a_name: "DRC", producer_a_country: "FR", producer_a_wines: 4,
            producer_b_name: "de la Romanee-Conti", producer_b_country: "FR", producer_b_wines: 16,
            signals: {"s10_shared_rare_wine": {"shared_wines": ["marey monge"]}}}
Output: {"pair_id": 5, "verdict": "MERGE", "confidence": 0.98, "reasoning": "DRC is the universally-used abbreviation for Domaine de la Romanée-Conti. The shared wine 'Marey-Monge' (a specific Vosne-Romanée lieu-dit bottled only by DRC) is the tie-breaker — no other producer makes a Marey-Monge. Two rows of the same producer from different import paths."}
"""


def build_static_preamble(section_11: str) -> str:
    return f"""You are classifying candidate producer-pair duplicates in a wine database. For each pair, decide whether the two producer rows represent the same brand identity (MERGE), distinct brands with an ownership relation (PARENT_CHILD), unrelated producers (SKIP), or are genuinely uncertain (UNCERTAIN).

Follow the rules below strictly. They are the product's identity policy — do not reinterpret or add your own criteria.

{section_11}

{OUTPUT_SCHEMA}

{FEW_SHOT}
"""


# ── Per-pair context ──────────────────────────────────────────────

def load_ttb_fingerprints(cur, producer_ids: list) -> dict:
    """Pre-compute TTB fingerprints for all producers in batch.

    Returns dict of producer_id -> {bw_permits, permittees, addresses, brand_names, cola_count}
    """
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT canonical_producer_id,
               COALESCE(permit_no, permit_number) AS permit,
               applicant_name, applicant_address, applicant_city, applicant_state,
               brand_name
        FROM source_ttb_colas
        WHERE canonical_producer_id IN ({placeholders})
          AND (permit_no IS NOT NULL OR permit_number IS NOT NULL)
        LIMIT 50000
    """, producer_ids)
    per_producer = {}
    for pid, permit, name, addr, city, state, brand in cur.fetchall():
        if pid not in per_producer:
            per_producer[pid] = {
                "bw_permits": set(),
                "permittees": set(),
                "addresses": set(),
                "brand_names": set(),
                "cola_count": 0,
            }
        d = per_producer[pid]
        d["cola_count"] += 1
        if permit and (permit.startswith("BW-") or re.match(r"^[A-Z]{2,3}-BW", permit)):
            d["bw_permits"].add(permit)
        if name:
            d["permittees"].add(name)
        if addr:
            full_addr = ", ".join(x for x in [addr, city, state] if x).strip()
            if full_addr:
                d["addresses"].add(full_addr)
        if brand:
            d["brand_names"].add(brand.upper())

    # Cap and serialize
    result = {}
    for pid, d in per_producer.items():
        result[pid] = {
            "bw_permits": sorted(d["bw_permits"])[:5],
            "permittees": sorted(d["permittees"])[:3],
            "addresses": sorted(d["addresses"])[:3],
            "brand_names": sorted(d["brand_names"])[:10],
            "cola_count": d["cola_count"],
        }
    return result


def load_wine_samples(cur, producer_ids: list) -> dict:
    """Top 5 wine names per producer (by vintage count or wine count)."""
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        WITH ranked AS (
          SELECT producer_id, display_name, name,
                 ROW_NUMBER() OVER (
                   PARTITION BY producer_id
                   ORDER BY length(COALESCE(display_name, name, '')) DESC
                 ) AS rn
          FROM wines
          WHERE producer_id IN ({placeholders}) AND deleted_at IS NULL
        )
        SELECT producer_id, COALESCE(display_name, name) FROM ranked
        WHERE rn <= 5
    """, producer_ids)
    per_producer = {}
    for pid, name in cur.fetchall():
        per_producer.setdefault(pid, []).append(name)
    return per_producer


def load_lwin_presence(cur, producer_ids: list) -> dict:
    """Whether each producer has LWIN-linked wines."""
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT w.producer_id, COUNT(DISTINCT ei.external_id) AS n_lwins
        FROM wines w
        JOIN external_ids ei
          ON ei.entity_type='wine' AND ei.entity_id = w.id AND ei.system='lwin_7'
        WHERE w.producer_id IN ({placeholders}) AND w.deleted_at IS NULL
        GROUP BY w.producer_id
    """, producer_ids)
    return {pid: n for pid, n in cur.fetchall()}


# ── Pair sampling ─────────────────────────────────────────────────

def sample_pilot_pairs(cur, n: int = 200) -> list[dict]:
    """Pull ~n pairs stratified across strategies, plus anchor cases."""
    anchors = [
        # obvious MERGE — expect MERGE verdict
        ("Ridge Vineyards", "Ridge"),
        ("Duckhorn Vineyards", "Duckhorn"),
        ("Caymus Vineyards", "Caymus"),
        ("DRC", "de la Romanee-Conti"),
        ("Silver Oak Cellars", "Silver Oak"),
        # obvious SKIP — expect SKIP verdict
        ("Stag's Leap Wine Cellars", "Stags' Leap Winery"),
        # PARENT_CHILD — expect PARENT_CHILD or MERGE
        ("Silver Oak", "Twomey"),
        # cross-country same name
        ("Cupcake Vineyards", "Cupcake Vineyards"),
    ]

    pairs = []
    seen_pair_ids = set()

    # Load anchor pairs
    for a_name, b_name in anchors:
        cur.execute("""
          SELECT ddp.id, ddp.producer_id_a, ddp.producer_id_b, ddp.name_a, ddp.name_b,
                 ddp.country, ddp.similarity, ddp.wines_a, ddp.wines_b, ddp.signals
          FROM producer_dedup_pairs ddp
          WHERE ddp.method_name='blocking'
            AND (
              (ddp.name_a = %s AND ddp.name_b = %s)
              OR (ddp.name_a = %s AND ddp.name_b = %s)
            )
          LIMIT 5
        """, (a_name, b_name, b_name, a_name))
        for r in cur.fetchall():
            if r[0] not in seen_pair_ids:
                pairs.append(_row_to_pair(r))
                seen_pair_ids.add(r[0])

    # Stratified random across strategies
    remaining = n - len(pairs)
    strategies = ['s2_trigram', 's6_ttb_permit', 's7_cross_country',
                  's8_catalog_overlap', 's9_substring',
                  's10_shared_rare_wine', 's11_cross_word_subset']
    per_strategy = max(1, remaining // len(strategies))

    for key in strategies:
        cur.execute(f"""
          SELECT id, producer_id_a, producer_id_b, name_a, name_b,
                 country, similarity, wines_a, wines_b, signals
          FROM producer_dedup_pairs
          WHERE method_name='blocking'
            AND signals ? '{key}'
            AND id <> ALL(%s::int[])
          ORDER BY random()
          LIMIT %s
        """, (list(seen_pair_ids), per_strategy))
        for r in cur.fetchall():
            if r[0] not in seen_pair_ids:
                pairs.append(_row_to_pair(r))
                seen_pair_ids.add(r[0])
                if len(pairs) >= n:
                    break
        if len(pairs) >= n:
            break

    return pairs[:n]


def _row_to_pair(row) -> dict:
    return {
        "pair_id": row[0],
        "producer_id_a": str(row[1]),
        "producer_id_b": str(row[2]),
        "name_a": row[3],
        "name_b": row[4],
        "country": row[5],
        "similarity": float(row[6]) if row[6] is not None else None,
        "wines_a": row[7],
        "wines_b": row[8],
        "signals": row[9],
    }


# ── Per-batch prompt construction ─────────────────────────────────

def format_pair_context(pair: dict, ttb: dict, wines: dict,
                         lwin_presence: dict) -> str:
    """Format one pair as a compact text block for the LLM."""
    pid = pair["pair_id"]
    id_a = pair["producer_id_a"]
    id_b = pair["producer_id_b"]

    a_ttb = ttb.get(id_a, {})
    b_ttb = ttb.get(id_b, {})
    a_wines = wines.get(id_a, [])[:5]
    b_wines = wines.get(id_b, [])[:5]

    def ttb_blob(t):
        if not t:
            return "  (no TTB)"
        parts = []
        if t.get("bw_permits"): parts.append(f"    BW permits: {t['bw_permits']}")
        if t.get("permittees"): parts.append(f"    Permittees: {t['permittees']}")
        if t.get("addresses"):  parts.append(f"    Address: {t['addresses'][0]}")
        if t.get("brand_names"): parts.append(f"    Brand names on COLAs: {t['brand_names']}")
        parts.append(f"    COLA count: {t.get('cola_count', 0)}")
        return "\n".join(parts)

    a_lwin_n = lwin_presence.get(id_a, 0)
    b_lwin_n = lwin_presence.get(id_b, 0)

    signals_summary = json.dumps(pair.get("signals") or {}, default=str)

    return f"""--- PAIR {pid} ---
Producer A: {pair['name_a']!r}  [{pair['country'].split('/')[0] if '/' in pair['country'] else pair['country']}]  wines={pair['wines_a']} ({a_lwin_n} LWIN-linked)
  Sample wines: {a_wines[:5]}
{ttb_blob(a_ttb)}

Producer B: {pair['name_b']!r}  [{pair['country'].split('/')[-1] if '/' in pair['country'] else pair['country']}]  wines={pair['wines_b']} ({b_lwin_n} LWIN-linked)
  Sample wines: {b_wines[:5]}
{ttb_blob(b_ttb)}

Blocking signals fired: {signals_summary}
"""


# ── API call with caching ─────────────────────────────────────────

def call_l1(client: anthropic.Anthropic, static_preamble: str,
            batch_block: str, dry_run: bool = False) -> tuple[list, dict]:
    """One Haiku call for a batch of 10 pairs. Returns (results_list, usage_info)."""
    if dry_run:
        return [], {
            "input_tokens": 0, "output_tokens": 0,
            "cache_read_tokens": 0, "cache_write_tokens": 0,
        }

    system = [
        {
            "type": "text",
            "text": static_preamble,
            "cache_control": {"type": "ephemeral"},
        }
    ]
    user = f"""Classify the {len(re.findall(r'--- PAIR', batch_block))} pairs below. Return ONLY a JSON array as specified.

{batch_block}"""

    resp = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=3000,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        # Try to salvage: find the array
        m = re.search(r"\[\s*\{.*\}\s*\]", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
        else:
            raise RuntimeError(f"L1 JSON parse failed: {e!r}. First 500 chars: {raw[:500]}")

    u = resp.usage
    usage = {
        "input_tokens": u.input_tokens,
        "output_tokens": u.output_tokens,
        "cache_read_tokens": getattr(u, "cache_read_input_tokens", 0) or 0,
        "cache_write_tokens": getattr(u, "cache_creation_input_tokens", 0) or 0,
    }
    return parsed, usage


# ── DB write ──────────────────────────────────────────────────────

def write_l1_verdicts(cur, pair_verdicts: list[tuple[dict, dict, float]]):
    """Write L1 results to producer_dedup_pairs (method_name='l1_haiku_batch').

    Each entry: (pair_dict, verdict_dict, cost_cents)
    """
    for pair, verdict, cost_cents in pair_verdicts:
        cur.execute("""
          INSERT INTO producer_dedup_pairs
            (producer_id_a, producer_id_b, name_a, name_b, country, similarity,
             wines_a, wines_b, method_name, verdict, confidence, reasoning,
             cost_cents, signals, created_at, updated_at)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'l1_haiku_batch',
                  %s, %s, %s, %s, %s, now(), now())
          ON CONFLICT (producer_id_a, producer_id_b, method_name)
          DO UPDATE SET
            verdict = EXCLUDED.verdict,
            confidence = EXCLUDED.confidence,
            reasoning = EXCLUDED.reasoning,
            cost_cents = EXCLUDED.cost_cents,
            signals = EXCLUDED.signals,
            updated_at = now()
        """, (
            pair["producer_id_a"], pair["producer_id_b"],
            pair["name_a"], pair["name_b"], pair["country"], pair["similarity"],
            pair["wines_a"], pair["wines_b"],
            verdict.get("verdict"),
            verdict.get("confidence"),
            verdict.get("reasoning"),
            cost_cents,
            json.dumps({
                "blocking_signals": pair.get("signals"),
                "l1_model": HAIKU_MODEL,
            }, default=str),
        ))


# ── Cost calc ─────────────────────────────────────────────────────

def compute_cost_cents(usage: dict) -> float:
    """Total cost in cents for one Haiku call."""
    in_t = usage["input_tokens"]
    out_t = usage["output_tokens"]
    cache_r = usage.get("cache_read_tokens", 0)
    cache_w = usage.get("cache_write_tokens", 0)

    # input_tokens INCLUDES cache_read_tokens + cache_write_tokens,
    # so regular_input = input_tokens - cache_read - cache_write
    regular_input = max(0, in_t - cache_r - cache_w)

    cost_dollars = (
        regular_input * PRICING["input"] / 1_000_000
        + out_t        * PRICING["output"] / 1_000_000
        + cache_r      * PRICING["cache_read"] / 1_000_000
        + cache_w      * PRICING["cache_write"] / 1_000_000
    )
    return cost_dollars * 100


# ── Main ──────────────────────────────────────────────────────────

def main() -> int:
    # Windows console: force UTF-8 so Section 11 unicode doesn't crash stdout
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--pilot", action="store_true",
                    help="run on 200 stratified pairs (pilot mode)")
    ap.add_argument("--limit", type=int, default=None,
                    help="max pairs to process")
    ap.add_argument("--dry-run", action="store_true",
                    help="print prompt, no API calls, no DB writes")
    ap.add_argument("--execute", action="store_true",
                    help="do API calls AND DB writes")
    ap.add_argument("--budget", type=float, default=2.0,
                    help="budget in dollars; abort if exceeded")
    ap.add_argument("--batch-size", type=int, default=10,
                    help="pairs per API call (default 10)")
    ap.add_argument("--workers", type=int, default=1,
                    help="concurrent API workers (default 1 = serial)")
    ap.add_argument("--print-sample", type=int, default=10,
                    help="print N sample verdicts after run")
    ap.add_argument("--resume", action="store_true",
                    help="skip pairs already written with method_name='l1_haiku_batch'")
    args = ap.parse_args()

    if not (args.pilot or args.limit or args.execute):
        print("Must specify --pilot, --limit, or --execute (full run)", file=sys.stderr)
        return 2

    section_11 = load_section_11()
    static_preamble = build_static_preamble(section_11)
    preamble_chars = len(static_preamble)
    print(f"Static preamble: {preamble_chars} chars (~{preamble_chars//4} tokens est.)")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    # Sample pairs
    if args.pilot:
        pairs = sample_pilot_pairs(cur, n=args.limit or 200)
    else:
        # Full run: every blocking pair not yet processed by L1
        cur.execute("""
          SELECT id, producer_id_a, producer_id_b, name_a, name_b,
                 country, similarity, wines_a, wines_b, signals
          FROM producer_dedup_pairs
          WHERE method_name='blocking'
            AND NOT EXISTS (
              SELECT 1 FROM producer_dedup_pairs ddp2
              WHERE ddp2.producer_id_a = producer_dedup_pairs.producer_id_a
                AND ddp2.producer_id_b = producer_dedup_pairs.producer_id_b
                AND ddp2.method_name = 'l1_haiku_batch'
            )
          ORDER BY id
          LIMIT %s
        """, (args.limit or 10**9,))
        pairs = [_row_to_pair(r) for r in cur.fetchall()]

    print(f"Sampled {len(pairs)} pairs.")

    # Preload TTB + wines + LWIN presence for all producers
    all_ids = list({p["producer_id_a"] for p in pairs} | {p["producer_id_b"] for p in pairs})
    print(f"Preloading context for {len(all_ids)} distinct producers...")
    t0 = time.time()
    ttb = load_ttb_fingerprints(cur, all_ids)
    wines = load_wine_samples(cur, all_ids)
    lwin_pres = load_lwin_presence(cur, all_ids)
    print(f"  TTB: {len(ttb)} / wines: {len(wines)} / lwin: {len(lwin_pres)} / {time.time()-t0:.1f}s")

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    total_cost_cents = 0.0
    total_calls = 0
    verdict_counts = {"MERGE": 0, "PARENT_CHILD": 0, "SKIP": 0, "UNCERTAIN": 0, "UNKNOWN": 0}
    confidence_sum = {"MERGE": 0.0, "PARENT_CHILD": 0.0, "SKIP": 0.0, "UNCERTAIN": 0.0}
    sample_results = []

    # Build batches
    batches = []
    for i in range(0, len(pairs), args.batch_size):
        batches.append(pairs[i:i + args.batch_size])

    state_lock = Lock()
    abort_flag = {"abort": False}

    def process_batch(batch_idx: int, batch: list):
        """Call L1 for one batch and return (batch_idx, batch, verdicts, usage, call_cost, error)."""
        if abort_flag["abort"]:
            return batch_idx, batch, [], {}, 0.0, None, "aborted"
        blocks = "\n".join(
            format_pair_context(p, ttb, wines, lwin_pres) for p in batch
        )
        try:
            verdicts, usage = call_l1(client, static_preamble, blocks,
                                       dry_run=args.dry_run)
            call_cost = compute_cost_cents(usage)
            return batch_idx, batch, verdicts, usage, call_cost, blocks, None
        except Exception as e:
            return batch_idx, batch, [], {}, 0.0, blocks, str(e)[:300]

    if args.dry_run and batches:
        print("\n--- PROMPT PREVIEW (first batch only) ---")
        print(static_preamble[:2000] + "\n...[truncated]...")
        blocks_preview = "\n".join(
            format_pair_context(p, ttb, wines, lwin_pres) for p in batches[0]
        )
        print("\n--- USER CONTENT ---")
        print(blocks_preview[:4000])
        print("\n--- END PREVIEW ---\n")
        return 0

    t_start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_batch, i, b): i for i, b in enumerate(batches)}
        for fut in as_completed(futures):
            batch_idx, batch, verdicts, usage, call_cost, blocks, err = fut.result()
            if abort_flag["abort"]:
                continue
            if err:
                print(f"  batch {batch_idx+1}: ERROR: {err[:200]}")
                continue

            with state_lock:
                total_cost_cents += call_cost
                total_calls += 1
                if total_cost_cents / 100 > args.budget:
                    print(f"\n!! Budget exceeded ({total_cost_cents/100:.2f} > {args.budget:.2f}). Aborting remaining batches.")
                    abort_flag["abort"] = True

            verdicts_by_id = {int(v["pair_id"]): v for v in verdicts if "pair_id" in v}
            pair_verdicts_for_write = []
            for p in batch:
                v = verdicts_by_id.get(p["pair_id"])
                if not v:
                    v = {
                        "verdict": "UNCERTAIN",
                        "confidence": 0.0,
                        "reasoning": "MISSING_VERDICT in model output",
                    }
                vk = v.get("verdict", "UNKNOWN")
                with state_lock:
                    if vk not in verdict_counts:
                        verdict_counts["UNKNOWN"] += 1
                    else:
                        verdict_counts[vk] += 1
                        try:
                            confidence_sum[vk] += float(v.get("confidence") or 0)
                        except (TypeError, ValueError):
                            pass
                per_pair_cost = call_cost / max(1, len(batch))
                pair_verdicts_for_write.append((p, v, per_pair_cost))
                with state_lock:
                    if len(sample_results) < args.print_sample:
                        sample_results.append((p, v))

            if args.execute and not args.dry_run:
                with state_lock:
                    write_l1_verdicts(cur, pair_verdicts_for_write)
                    conn.commit()

            processed = sum(verdict_counts.values())
            elapsed_total = time.time() - t_start
            eta_s = (elapsed_total / max(1, processed)) * (len(pairs) - processed)
            print(f"  b{batch_idx+1:>5}/{len(batches)}: {len(batch)} pairs "
                  f"in={usage.get('input_tokens',0):>5} "
                  f"(cr={usage.get('cache_read_tokens', 0):>5} "
                  f"cw={usage.get('cache_write_tokens', 0):>4}) "
                  f"out={usage.get('output_tokens',0):>4} | "
                  f"{call_cost:.2f}¢ | processed={processed}/{len(pairs)} | "
                  f"${total_cost_cents/100:.2f} | eta={eta_s/60:.0f}m", flush=True)

    # Report
    print("\n" + "=" * 60)
    print("L1 PILOT RESULTS")
    print("=" * 60)
    print(f"Pairs processed:  {sum(verdict_counts.values())}")
    print(f"Total cost:       ${total_cost_cents/100:.3f}")
    print(f"Avg cost/pair:    {total_cost_cents/max(1, sum(verdict_counts.values())):.3f}¢")
    print(f"Total calls:      {total_calls}")
    print(f"Budget used:      ${total_cost_cents/100:.3f} / ${args.budget:.2f}")
    print()
    print("Verdict distribution:")
    for v, n in verdict_counts.items():
        if n > 0:
            avg_conf = confidence_sum.get(v, 0) / n if v in confidence_sum else 0
            print(f"  {v:<15} {n:>4}  (avg confidence: {avg_conf:.2f})")

    # Extrapolate full-corpus cost
    cur.execute("SELECT COUNT(*) FROM producer_dedup_pairs WHERE method_name='blocking'")
    corpus_size = cur.fetchone()[0]
    n_processed = sum(verdict_counts.values())
    if n_processed > 0:
        extrapolated = total_cost_cents / 100 * (corpus_size / n_processed)
        print(f"\nExtrapolated full-corpus cost: ${extrapolated:.2f} for {corpus_size:,} pairs")

    # Sample reasoning
    print(f"\nSample verdicts ({min(len(sample_results), args.print_sample)}):")
    for p, v in sample_results[:args.print_sample]:
        print(f"\n  [{p['pair_id']}] {p['name_a']!r:<40} / {p['name_b']!r:<40}")
        print(f"       verdict: {v.get('verdict')}, confidence: {v.get('confidence')}")
        print(f"       reasoning: {v.get('reasoning', '')[:250]}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
