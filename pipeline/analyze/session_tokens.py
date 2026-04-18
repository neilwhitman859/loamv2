"""Compute session token usage for the current Claude Code transcript + active pipeline jobs.

Emits JSON to stdout:
  {
    "claude": {input, cache_write_1h, cache_write_5m, cache_read, output, cost_usd, entries},
    "pipeline": {label: {pairs, cost_usd, input_tokens, output_tokens, errors}, ...},
    "totals": {cost_usd, input_tokens, output_tokens}
  }

Usage:
    python -m pipeline.analyze.session_tokens --transcript PATH --log PATH [--log PATH ...]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Opus 4.7 pricing in $/MTok
OPUS_IN, OPUS_OUT = 15.0, 75.0
OPUS_CW_1H, OPUS_CW_5M, OPUS_CR = 30.0, 18.75, 1.5


def parse_transcript(path: Path) -> dict:
    in_t = out_t = cache_w_1h = cache_w_5m = cache_r = n = 0
    if not path.exists():
        return {"input": 0, "cache_write_1h": 0, "cache_write_5m": 0,
                "cache_read": 0, "output": 0, "cost_usd": 0.0, "entries": 0}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            u = d.get("message", {}).get("usage") if "message" in d else d.get("usage")
            if not u or not isinstance(u, dict):
                continue
            in_t += u.get("input_tokens", 0) or 0
            out_t += u.get("output_tokens", 0) or 0
            cw = u.get("cache_creation", {}) or {}
            cache_w_1h += cw.get("ephemeral_1h_input_tokens", 0) or 0
            cache_w_5m += cw.get("ephemeral_5m_input_tokens", 0) or 0
            cache_r += u.get("cache_read_input_tokens", 0) or 0
            n += 1
    cost = (
        in_t * OPUS_IN
        + out_t * OPUS_OUT
        + cache_w_1h * OPUS_CW_1H
        + cache_w_5m * OPUS_CW_5M
        + cache_r * OPUS_CR
    ) / 1_000_000
    return {
        "input": in_t,
        "cache_write_1h": cache_w_1h,
        "cache_write_5m": cache_w_5m,
        "cache_read": cache_r,
        "output": out_t,
        "cost_usd": cost,
        "entries": n,
    }


LOG_LINE_RE = re.compile(
    r"b\s*\d+/\d+:\s*(\d+)\s*pairs\s*in=\s*(\d+)\s*"
    r"(?:\(cr=\s*(\d+)\s*cw=\s*(\d+)\)\s*)?out=\s*(\d+)\s*.*?\|\s*([\d.]+)c\s*\|"
)
ERR_RE = re.compile(r":\s*ERR\s+")
RESULTS_RE = re.compile(r"Total cost:\s*\$([\d.]+)")
PROCESSED_TAIL_RE = re.compile(r"processed=(\d+)/(\d+)\s*\|\s*\$([\d.]+)\s*\|\s*eta=(\d+)m")


def parse_log(path: Path) -> dict:
    out = {"pairs": 0, "cost_usd": 0.0, "input_tokens": 0, "output_tokens": 0,
           "cache_read": 0, "cache_write": 0, "errors": 0,
           "processed": None, "total": None, "eta_min": None}
    if not path.exists():
        return out
    batch_sum_total = 0.0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if ERR_RE.search(line):
                out["errors"] += 1
                continue
            m = LOG_LINE_RE.search(line)
            if m:
                n_pairs, in_t, cr, cw, out_t, cents = m.groups()
                out["pairs"] += int(n_pairs)
                out["input_tokens"] += int(in_t)
                out["output_tokens"] += int(out_t)
                if cr:
                    out["cache_read"] += int(cr)
                if cw:
                    out["cache_write"] += int(cw)
                batch_sum_total += float(cents) / 100.0
            m2 = PROCESSED_TAIL_RE.search(line)
            if m2:
                out["processed"] = int(m2.group(1))
                out["total"] = int(m2.group(2))
                out["eta_min"] = int(m2.group(4))
    # Use batch sum (per-call totals) — accurate across resumed runs, unlike the RESULTS completion-line which only reflects the last run segment.
    out["cost_usd"] = batch_sum_total
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--transcript", default=None)
    ap.add_argument("--log", action="append", default=[], help="label=path pairs; repeatable")
    args = ap.parse_args()

    claude = parse_transcript(Path(args.transcript)) if args.transcript else {}
    pipeline = {}
    for entry in args.log:
        if "=" in entry:
            label, pth = entry.split("=", 1)
        else:
            label, pth = Path(entry).stem, entry
        pipeline[label] = parse_log(Path(pth))

    total_cost = (claude.get("cost_usd", 0) +
                  sum(v.get("cost_usd", 0) for v in pipeline.values()))
    result = {
        "claude": claude,
        "pipeline": pipeline,
        "totals": {"cost_usd": total_cost},
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
